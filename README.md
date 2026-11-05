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
  and re-sending anything unacked after a reconnect.

## Sizing to the board

You own all the memory, so the same sender runs on a board with PSRAM or without:

| | ESP32-S3, 5,500 rows/s | ESP32-C6, 300 rows/s |
|---|---|---|
| `body` (max message) | 128 KB, PSRAM | 16 KB, internal |
| `tcp_tx` | 64 KB, PSRAM | 16 KB, internal |
| Ring | ~1.8 MB, PSRAM | ~120 KB, internal |

A record type is yours too: any `Copy` type, written to tables by your `encode` function.

```rust
#[embassy_executor::task]
async fn sender(stack: Stack<'static>, ring: &'static SharedRing<Rec>, buffers: Buffers<Rec>) {
    let config = Config {
        addr: "192.168.1.10:9000".parse().unwrap(),
        client_id: "my-device/1",
        symbols: &["my-device"],
        batch_interval: Duration::from_millis(100),
        stats: &STATS,
    };
    run(stack, ring, buffers, &config, || rng.random(), || None, |e, recs| {
        e.table("readings", recs.len(), &[("device", SYMBOL), ("celsius", FLOAT)])?;
        e.symbols(recs.iter().map(|_| 0))?; // index into config.symbols
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

## Tests

```sh
cargo test                     # encoder and ring, on the host
cargo check --features sender
```
