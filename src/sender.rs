//! Streams a [`Ring`] to QuestDB over WebSocket. Records leave the ring only when QuestDB acks the
//! message that carried them; on reconnect everything unacked is re-sent.

use core::cell::RefCell;
use core::fmt::Write as _;
use core::mem::MaybeUninit;
use core::net::SocketAddrV4;
use core::sync::atomic::{AtomicU32, Ordering::Relaxed};

use embassy_net::{Stack, tcp::TcpSocket};
use embassy_sync::blocking_mutex::{Mutex, raw::CriticalSectionRawMutex};
use embassy_time::{Duration, Instant, Timer};
use embedded_io_async::{Read as _, Write as _};
use log::{error, info, warn};

use crate::*;

/// Values of `Stats::last_error`: why the last connection ended.
pub const ERR_CONNECT: u32 = 1;
pub const ERR_READ: u32 = 2;
pub const ERR_WRITE: u32 = 3;
pub const ERR_SERVER: u32 = 4;
pub const ERR_PROTOCOL: u32 = 5;

const MAX_INFLIGHT: usize = 8;
/// Retriable rejections of the same message before it's dropped as poison, as in the reference
/// client's `max_frame_rejections`. A device has no disk to park it on, so it's dropped and counted
/// in `Stats::rejected` instead of halting.
const MAX_FRAME_REJECTIONS: u32 = 4;
const FRAME_HEADER_MAX: usize = 14;

pub type SharedRing<R> = Mutex<CriticalSectionRawMutex, RefCell<Ring<R>>>;

pub struct Config<'a> {
    pub addr: SocketAddrV4,
    /// Sent as `X-QWP-Client-Id`.
    pub client_id: &'a str,
    /// The symbol dictionary: a `SYMBOL` column's values are ids from `Symbols::intern`. Each
    /// message carries the names added since the last one; the whole table again after a
    /// reconnect. The application may intern more while the sender runs.
    pub symbols: &'a Symbols,
    /// Longest a record waits for a batch to fill.
    pub batch_interval: Duration,
    pub stats: &'a Stats,
}

/// Counters for the application's own telemetry. All but `last_error` are cumulative, except that
/// the application may `swap(0)` `tx_bytes` and `encode_us` for per-interval values.
pub struct Stats {
    pub tx_bytes: AtomicU32,
    pub encode_us: AtomicU32,
    pub reconnects: AtomicU32,
    pub acks: AtomicU32,
    pub rejected: AtomicU32,
    pub last_error: AtomicU32,
}

impl Stats {
    pub const fn new() -> Self {
        Self {
            tx_bytes: AtomicU32::new(0),
            encode_us: AtomicU32::new(0),
            reconnects: AtomicU32::new(0),
            acks: AtomicU32::new(0),
            rejected: AtomicU32::new(0),
            last_error: AtomicU32::new(0),
        }
    }
}

impl Default for Stats {
    fn default() -> Self {
        Self::new()
    }
}

/// The sender's memory. Size it to the board and the data rate: `body` bounds a message, `tcp_tx`
/// bounds what's on the wire unacked, `scratch` bounds the records in a message.
pub struct Buffers<R: 'static> {
    pub body: &'static mut [u8],
    pub tcp_tx: &'static mut [u8],
    pub tcp_rx: &'static mut [u8],
    pub scratch: &'static mut [MaybeUninit<R>],
}

/// Never returns: call it from the application's task (embassy tasks can't be generic).
///
/// `random` feeds the WebSocket masks and key. `priority` yields records that must get out even
/// when the ring is full; they aren't kept for re-sending. `encode` writes records to tables.
pub async fn run<R: Copy>(
    stack: Stack<'static>,
    ring: &SharedRing<R>,
    buffers: Buffers<R>,
    config: &Config<'_>,
    random: impl Fn() -> u32,
    mut priority: impl FnMut() -> Option<R>,
    encode: impl Fn(&mut Encoder<'_>, &[R]) -> Result<(), Full>,
) -> ! {
    let Buffers {
        body,
        tcp_tx,
        tcp_rx,
        scratch,
    } = buffers;
    assert!(!scratch.is_empty(), "scratch must hold at least one record");
    let stats = config.stats;
    let mut resp = [0u8; 512];
    let mut strikes = 0; // consecutive retriable rejections of the oldest unacked message
    stack.wait_config_up().await;
    loop {
        let mut sock = TcpSocket::new(stack, &mut *tcp_rx, &mut *tcp_tx);
        sock.set_timeout(Some(Duration::from_secs(10)));
        let result: Result<(), &str> = async {
            sock.connect((*config.addr.ip(), config.addr.port()))
                .await
                .map_err(|_| "connect")?;
            let max_payload = upgrade(&mut sock, &mut resp, config.client_id, &random)
                .await?
                .min(body.len() - FRAME_HEADER_MAX);
            info!("qwp connected, max payload {max_payload} B");

            let mut inflight = Inflight::<MAX_INFLIGHT>::new();
            let mut symbols_sent = 0;
            let mut last_send = Instant::now();
            loop {
                if !inflight.is_full()
                    && let Some(r) = priority()
                {
                    let len;
                    (len, symbols_sent) = message(
                        &mut body[FRAME_HEADER_MAX..FRAME_HEADER_MAX + max_payload],
                        config.symbols,
                        symbols_sent,
                        &[r],
                        &encode,
                    )
                    .map_err(|_| "record exceeds max payload")?;
                    let start = ws_frame(body, len, &random);
                    sock.write_all(&body[start..FRAME_HEADER_MAX + len])
                        .await
                        .map_err(|_| "write")?;
                    stats
                        .tx_bytes
                        .fetch_add((FRAME_HEADER_MAX + len - start) as u32, Relaxed);
                    inflight.push(0); // not from the ring, so its ack pops nothing
                    continue;
                }
                let sent = inflight.records();
                let unsent = ring.lock(|r| r.borrow().len()) - sent;
                let due = unsent > 0
                    && (unsent >= (scratch.len() / 4).max(1)
                        || last_send.elapsed() >= config.batch_interval);
                if inflight.len() > 0 && (inflight.is_full() || !due || sock.can_recv()) {
                    let mut h = [0u8; 10];
                    sock.read_exact(&mut h[..2]).await.map_err(|_| "read")?;
                    let ext = match h[1] & 0x7F {
                        126 => 2,
                        127 => 8,
                        _ => 0,
                    };
                    sock.read_exact(&mut h[2..2 + ext])
                        .await
                        .map_err(|_| "read")?;
                    let (op, _, len) = ws_parse_header(&h).ok_or("frame header")?;
                    let take = len.min(resp.len());
                    sock.read_exact(&mut resp[..take])
                        .await
                        .map_err(|_| "read")?;
                    for _ in take..len {
                        sock.read_exact(&mut [0u8]).await.map_err(|_| "read")?; // never for QWP responses
                    }
                    match op {
                        WS_BINARY => match parse_response(&resp[..take]) {
                            Some((STATUS_DURABLE_ACK, ..)) => {} // never requested
                            Some((STATUS_OK, Some(seq), _)) => {
                                // cumulative: acks every message up to seq
                                let Some(n) = inflight.ok(seq) else {
                                    error!(
                                        "qwp ack seq {seq}, in flight {}..{}",
                                        inflight.next_seq(),
                                        inflight.next_seq() + inflight.len() as i64
                                    );
                                    return Err("seq");
                                };
                                if n > 0 {
                                    strikes = 0;
                                }
                                stats.acks.fetch_add(1, Relaxed);
                                ring.lock(|r| r.borrow_mut().pop(n));
                            }
                            Some((status, Some(seq), msg)) => {
                                let msg = core::str::from_utf8(msg).unwrap_or("");
                                error!("qwp status {status} on seq {seq}: {msg}");
                                let Some(n) = inflight.error(seq) else {
                                    error!("qwp error seq {seq}, expected {}", inflight.next_seq());
                                    return Err("seq");
                                };
                                strikes += 1;
                                if !retriable(status) || strikes >= MAX_FRAME_REJECTIONS {
                                    // it would fail the same way forever: drop it rather than
                                    // wedge the ring. QWP rejects whole messages, so this drops
                                    // every table in it, not just the one at fault.
                                    warn!(
                                        "qwp: dropping {n} records rejected with status {status}"
                                    );
                                    ring.lock(|r| r.borrow_mut().pop(n));
                                    stats.rejected.fetch_add(1, Relaxed);
                                    strikes = 0;
                                }
                                // the server refuses every later frame on this connection
                                return Err("server error");
                            }
                            _ => return Err("bad response"),
                        },
                        WS_PING => {
                            let mask = random().to_le_bytes();
                            let mut f = [0u8; FRAME_HEADER_MAX];
                            let hl = ws_header(&mut f, WS_PONG, take, mask);
                            ws_mask(&mut resp[..take], mask);
                            sock.write_all(&f[..hl]).await.map_err(|_| "write")?;
                            sock.write_all(&resp[..take]).await.map_err(|_| "write")?;
                        }
                        WS_CLOSE => return Err("server closed"),
                        _ => {}
                    }
                    continue;
                }
                if !due {
                    Timer::after_millis(10).await;
                    continue;
                }

                let mut k = unsent.min(scratch.len());
                for (i, slot) in scratch[..k].iter_mut().enumerate() {
                    let r = ring.lock(|r| r.borrow().get(sent + i)).ok_or("ring")?;
                    slot.write(r);
                }
                // SAFETY: the first k slots were just written
                let recs = unsafe { core::slice::from_raw_parts(scratch.as_ptr().cast::<R>(), k) };
                let t0 = Instant::now();
                let len;
                (len, symbols_sent) = loop {
                    match message(
                        &mut body[FRAME_HEADER_MAX..FRAME_HEADER_MAX + max_payload],
                        config.symbols,
                        symbols_sent,
                        &recs[..k],
                        &encode,
                    ) {
                        Ok(r) => break r,
                        Err(_) if k > 1 => k /= 2,
                        Err(_) => return Err("record exceeds max payload"),
                    }
                };
                let start = ws_frame(body, len, &random);
                stats
                    .encode_us
                    .fetch_add(t0.elapsed().as_micros() as u32, Relaxed);
                sock.write_all(&body[start..FRAME_HEADER_MAX + len])
                    .await
                    .map_err(|_| "write")?;
                stats
                    .tx_bytes
                    .fetch_add((FRAME_HEADER_MAX + len - start) as u32, Relaxed);
                inflight.push(k);
                last_send = Instant::now();
            }
        }
        .await;
        if let Err(e) = result {
            stats.reconnects.fetch_add(1, Relaxed);
            stats.last_error.store(err_code(e), Relaxed);
            warn!("qwp: {e}, reconnecting");
        }
        sock.abort();
        let _ = sock.flush().await;
        Timer::after_secs(1).await;
    }
}

fn err_code(e: &str) -> u32 {
    match e {
        "connect" => ERR_CONNECT,
        "read" => ERR_READ,
        "write" => ERR_WRITE,
        "server error" | "server closed" => ERR_SERVER,
        _ => ERR_PROTOCOL,
    }
}

/// One message: the symbols QuestDB hasn't seen on this connection, then the records. Returns its
/// length and the symbols sent so far. The table is read after `recs` were taken from the ring, and
/// a record only holds ids interned before it was pushed, so every id in it is covered.
fn message<R>(
    out: &mut [u8],
    symbols: &Symbols,
    symbols_sent: usize,
    recs: &[R],
    encode: &impl Fn(&mut Encoder<'_>, &[R]) -> Result<(), Full>,
) -> Result<(usize, usize), Full> {
    symbols.with(|names| {
        let mut e = Encoder::with_symbols(out, symbols_sent as u64, &names[symbols_sent..])?;
        encode(&mut e, recs)?;
        Ok((e.finish(), names.len()))
    })
}

/// Masks the payload at `body[FRAME_HEADER_MAX..][..len]` and writes a binary frame header just
/// before it; returns where the frame starts.
fn ws_frame(body: &mut [u8], len: usize, random: &impl Fn() -> u32) -> usize {
    let mask = random().to_le_bytes();
    ws_mask(&mut body[FRAME_HEADER_MAX..FRAME_HEADER_MAX + len], mask);
    let mut f = [0u8; FRAME_HEADER_MAX];
    let hl = ws_header(&mut f, WS_BINARY, len, mask);
    body[FRAME_HEADER_MAX - hl..FRAME_HEADER_MAX].copy_from_slice(&f[..hl]);
    FRAME_HEADER_MAX - hl
}

struct SliceWriter<'a> {
    buf: &'a mut [u8],
    len: usize,
}

impl core::fmt::Write for SliceWriter<'_> {
    fn write_str(&mut self, s: &str) -> core::fmt::Result {
        let end = self.len + s.len();
        self.buf
            .get_mut(self.len..end)
            .ok_or(core::fmt::Error)?
            .copy_from_slice(s.as_bytes());
        self.len = end;
        Ok(())
    }
}

/// Reads an HTTP/1.1 response head into `buf`: `(status code, head length)`.
async fn read_head(
    sock: &mut TcpSocket<'_>,
    buf: &mut [u8],
) -> Result<(u16, usize), embassy_net::tcp::Error> {
    let mut n = 0;
    loop {
        if n == buf.len() {
            return Err(embassy_net::tcp::Error::ConnectionReset);
        }
        let r = sock.read(&mut buf[n..]).await?;
        if r == 0 {
            return Err(embassy_net::tcp::Error::ConnectionReset);
        }
        n += r;
        if let Some(i) = buf[..n].windows(4).position(|w| w == b"\r\n\r\n") {
            let code = core::str::from_utf8(&buf[9..12])
                .ok()
                .and_then(|c| c.parse().ok())
                .unwrap_or(0);
            return Ok((code, i + 4));
        }
    }
}

fn header<'a>(head: &'a [u8], name: &str) -> Option<&'a str> {
    core::str::from_utf8(head).ok()?.lines().find_map(|l| {
        let (k, v) = l.split_once(':')?;
        k.eq_ignore_ascii_case(name).then(|| v.trim())
    })
}

/// WebSocket upgrade to QWP; returns the payload budget (90% of the server's advertised max).
async fn upgrade(
    sock: &mut TcpSocket<'_>,
    buf: &mut [u8],
    client_id: &str,
    random: &impl Fn() -> u32,
) -> Result<usize, &'static str> {
    let mut nonce = [0u8; 16];
    nonce
        .chunks_mut(4)
        .for_each(|c| c.copy_from_slice(&random().to_le_bytes()));
    let key = ws_key(nonce);
    let mut req = [0u8; 256];
    let mut w = SliceWriter {
        buf: &mut req,
        len: 0,
    };
    write!(
        w,
        "GET /write/v4 HTTP/1.1\r\nHost: questdb\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\
         Sec-WebSocket-Key: {}\r\nSec-WebSocket-Version: 13\r\nX-QWP-Max-Version: 1\r\n\
         X-QWP-Client-Id: {client_id}\r\n\r\n",
        core::str::from_utf8(&key).unwrap(),
    )
    .map_err(|_| "request too long")?;
    let n = w.len;
    sock.write_all(&req[..n]).await.map_err(|_| "write")?;
    // ponytail: Sec-WebSocket-Accept isn't verified (needs SHA-1); the server is a known QuestDB
    let (code, head_end) = read_head(sock, buf).await.map_err(|_| "read")?;
    let head = &buf[..head_end];
    if code != 101 {
        warn!("qwp upgrade: HTTP {code}");
        return Err("upgrade refused");
    }
    if header(head, "x-qwp-version") != Some("1") {
        return Err("unsupported QWP version");
    }
    let max = header(head, "x-qwp-max-batch-size")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(1_900_000);
    Ok(max / 10 * 9)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A name interned mid-connection goes out, alone, in the next message's delta.
    #[test]
    fn message_sends_new_symbols_only() {
        let syms = Symbols::new();
        syms.intern("dev-a");
        let enc = |_: &mut Encoder<'_>, _: &[u8]| -> Result<(), Full> { Ok(()) };
        let mut out = [0u8; 64];
        let (_, sent) = message(&mut out, &syms, 0, &[0u8], &enc).unwrap();
        assert_eq!(sent, 1);
        syms.intern("dev-b");
        let (n, sent) = message(&mut out, &syms, sent, &[0u8], &enc).unwrap();
        assert_eq!(sent, 2);
        assert_eq!(&out[12..n], &[1, 1, 5, b'd', b'e', b'v', b'-', b'b']);
    }
}
