# tessera-sink

Atomic-write worker pool to disk. The first *composite service* in
Tessera, built over the primitives:

- [`tessera-pool`](../tessera-pool/) — zero-copy payload handoff. Payloads
  larger than one slot are split into chunks.
- [`tessera-channel`](../tessera-channel/) — control plane (owner → worker:
  `ChunkDescriptor` / `Commit` / `Cancel`) and ack plane (worker → owner:
  `ChunkAck` / `ChunkFailed` / `CancelAck` / `JobComplete`).

N worker subprocesses (the [`tessera-sink-worker`](../tessera-sink-worker/)
bin) stream chunks to a temp file in the target's directory, verify chunk
count + a BLAKE3 hash on commit, and atomically rename into place — so a
reader never observes a partially written file.

## Region ownership

Channel's rule is *the Receiver creates the region*; the consistent rule
across the Sink is **the reader owns its region**:

| Region            | Reader   | Creator (owns lifecycle) |
|-------------------|----------|--------------------------|
| ack channel       | owner    | owner (Receiver)         |
| control channel i | worker i | worker i (Receiver)      |
| pool              | workers  | owner (lease authority)  |

The Pool is the exception: its owner is the single writer
(single-writer-lease), not a reader.

## Status

v0.0.1, Stage 4d implemented. Single-threaded owner (Pool/Channel are
`!Send`): the owner cooperatively drains the ack plane and renews leases
inside `submit` / `flush` rather than on background threads. Worker
subprocesses are spawned via `std::process::Command` (true multiprocess,
fault-isolated, exercising the real cross-process Pool + Channel path).

See the [workspace README](../../README.md) and
[`docs/concept_landscape.md`](../../docs/concept_landscape.md) for design
context.
