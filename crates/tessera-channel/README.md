# tessera-channel

Non-lossy MPSC (multi-producer, single-consumer) shared-memory queue.
Credit-based backpressure (block / try / timeout), FIFO ordering,
caller-selected send mode per call.

**Status**: v0.0.1 — Rust core landing in [Stage 4c of the upstream
extraction plan](https://github.com/Indubitable-Industries/Bayence-Certus/blob/main/claudedocs/plans/mp_tools_open_source_extraction_2026-05-23.md).
CI wiring + crates.io / PyPI publish land in Stage 5.

## What it does

- **Non-lossy FIFO queue in shared memory** — fixed-slot ring with
  head + tail counters in SHM. Producers `fetch_add` the tail to
  claim the next slot; consumer advances head after dequeue.
- **MPSC by design**: multiple producer processes can `send()`
  concurrently to the same Channel; exactly one consumer (the role-
  Receiver Channel handle in the region's owner process) reads via
  `recv()`. The single-consumer constraint gives us linearizability
  for free without seqlock retry on the read side.
- **Credit-based backpressure** — caller picks the mode per `send`
  call: `send()` blocks until room is available; `try_send()` fails
  fast with `ChannelFull`; `send_timeout()` is bounded blocking.
  Writers never overwrite — that's Ring's job (cell #2 of the
  primitive matrix); Channel is cell #3 (non-lossy MPSC small
  typed-bytes).
- **BLAKE3-derived namespace** — same convention as Pool / Ring.
  Two peers with the same `description` derive the same SHM region
  name and attach without manual coordination.
- **Single-owner lifecycle** — one process creates the region
  (Receiver role); producers attach as Sender role. Mirrors §3.5.b
  of the upstream extraction plan.
- **Trusted IPC posture** — same as Pool / Ring (§3.5.e): the IPC
  namespace boundary IS the trust boundary; no in-library ACLs.

## Quick start

```rust
use tessera_channel::{Channel, ChannelConfig, ChannelRole};

let receiver = Channel::open(ChannelConfig {
    description: "my-app/control".into(),
    slot_count: 256,
    slot_size_bytes: 4096,
    role: ChannelRole::Receiver,
    force_recreate: false,
})?;

// In another process (or thread, attached to the same region):
let sender = Channel::open(ChannelConfig {
    description: "my-app/control".into(),
    slot_count: 256,
    slot_size_bytes: 4096,
    role: ChannelRole::Sender,
    force_recreate: false,
})?;

sender.send(b"hello channel")?;
let msg = receiver.recv()?;
# Ok::<(), tessera_channel::TesseraChannelError>(())
```

For Python ergonomics, install the
[`tessera-channel`](../../python/py-tessera-channel/) Python facade and
use `from tessera_channel import Channel` with the same API.

## Use cases

Channel is the right shape when:

- You're building an RPC plane on top of SHM and dropping a request
  or response is not OK.
- You're coordinating a worker pool (e.g., Tessera Sink uses Channel
  for both control-plane WriteJob descriptors and ack-plane responses).
- You need a queue that one consumer drains in order — telemetry-
  shaped multi-reader fanout belongs in Ring instead.

If you want **lossy multi-reader broadcast**, use Tessera Ring.
If you want **lease-based bulk-bytes transfer**, use Tessera Pool.
The three primitives cover the three useful cells of the
lossiness × reader-topology × payload-shape matrix; see
`docs/concept_landscape.md` in the workspace root for the full view.

## Roadmap

- **v0.1.0 (Stage 5)**: publish to crates.io with the current public
  surface (MPSC, bytes-only payloads, three send modes).
- **Future**:
  - MPMC variant (multiple consumers) once a real use case surfaces.
  - Typed message support at the facade layer (serialize via
    bincode/pickle before send; deserialize after recv).
  - Zero-copy `Receiver::recv_view()` returning a slot-borrowed
    slice instead of a copied Vec (v0.2).
