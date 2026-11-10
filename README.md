# qwp-embedded

`no_std`, allocation-free [QuestDB Wire Protocol](https://questdb.com/docs/connect/wire-protocols/qwp-ingress-websocket/)
client for microcontrollers. Extracted from
[s3-sensors](https://github.com/sklarsa/s3-sensors), where it streams ~5,500 rows/s from an
ESP32-S3, and also runs on an ESP32-C6. Any target with embassy-net should work.

- **`Encoder`**: builds QWP messages into your buffer. Several tables per message, BYTE, SHORT,
  LONG, FLOAT, DOUBLE and SYMBOL columns without nulls, Gorilla designated timestamps. No
  dependencies.
- **`Ring`**: drop-newest record buffer. Records stay until QuestDB acks them.
- **`sender::run`** (feature `sender`): streams a ring over WebSocket, pipelining up to 8 messages
  and re-sending anything unacked after a reconnect (see [Acks and errors](#acks-and-errors)).

## Sizing to the board

You own all the memory, so the same sender runs on a board with PSRAM or without:

| | ESP32-S3, 5,500 rows/s | ESP32-C6, 300 rows/s |
|---|---|---|
| `body` (max message) | 128 KB, PSRAM | 16 KB, internal |
| `tcp_tx` | 64 KB, PSRAM | 16 KB, internal |
| Ring | ~1.8 MB, PSRAM | ~120 KB, internal |

A record type is yours too: any `Copy` type, written to tables by your `encode` function.

```rust
static SYMBOLS: Symbols = Symbols::new();

#[embassy_executor::task]
async fn sender(stack: Stack<'static>, ring: &'static SharedRing<Rec>, buffers: Buffers<Rec>) {
    let device = SYMBOLS.intern("my-device").unwrap(); // more can be added while run() goes
    let config = Config {
        addr: "192.168.1.10:9000".parse().unwrap(),
        client_id: "my-device/1",
        symbols: &SYMBOLS,
        batch_interval: Duration::from_millis(100),
        stats: &STATS,
    };
    run(stack, ring, buffers, &config, || rng.random(), || None, |e, recs| {
        e.table("readings", recs.len(), &[("device", SYMBOL), ("celsius", FLOAT)])?;
        e.symbols(recs.iter().map(|_| device))?; // ids from SYMBOLS.intern
        e.f32s(recs.iter().map(|r| r.celsius))?;
        e.timestamps(recs.iter().map(|r| r.t_us))
    })
    .await
}
```

Timestamps are microseconds since the epoch, taken on the device. QWP creates missing tables with
wide column types and no TTL, so create yours first.

## Limits

- No TLS or authentication.
- `Sec-WebSocket-Accept` isn't verified.
- No nulls: to leave a column out, leave it out of the table block.
- The ring drops the newest records when full.
- QuestDB rejects a whole message, so one bad table drops the other tables' rows sent with it.

## Acks and errors

As QuestDB's server does it, which is not quite what the protocol page says:

- **OK acks are cumulative.** `OK seq=N` acks every message up to N on the connection. The server
  coalesces them (at least every 64 messages, and whenever a socket read ends), so one OK often
  covers several pipelined messages.
- **An error ends the connection's pipeline.** The server acks everything before the failed message
  first, then refuses every later one without answering. The sender reconnects and re-sends from
  the failed message.
- **What gets dropped.** Following the reference client's policy, only rejections that would repeat
  on an identical resend are permanent: schema mismatch, parse error, security error. Everything
  else, unknown statuses included, is retried. A message rejected 4 times in a row is treated as
  poison. Without a disk to park permanent failures on, the sender drops them and counts them in
  `Stats::rejected` rather than halting.

## Tests

```sh
cargo test                     # encoder and ring, on the host
cargo check --features sender
```
