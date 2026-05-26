# Tessera concept landscape

This document maps the conceptual surface Tessera covers — what's a
primitive, what's a composite service, what's deliberately out of
scope, and what's a future candidate. It's a living map; treat it as
authoritative for "what category does X belong to?" questions but not
as a roadmap commitment.

**Status**: tracked reference doc, but not part of the side-doc
authority chain (`mp_tools_open_source_extraction_2026-05-23.md` is
still the locked spec for decisions); this is the higher-altitude
mental model that frames where each spec'd piece sits.

## The categories

- **Primitive** — a byte-level SHM IPC contract. One state machine,
  one on-disk layout, one principal contract (lossy vs non-lossy ×
  reader topology × payload shape). Independently useful; doesn't
  depend on other Tessera primitives.
- **Service** — a composite built from primitives. Wraps a use case
  (atomic-write-to-disk, cross-process barrier, KV cache, …). May
  pull in OS / disk concerns the primitive layer doesn't.
- **Sub-primitive** — too low-level to ship as a top-level concept.
  Usually folded into primitives' internals (atomic counters, lease
  generation, namespace handles).
- **Adjacent / out of scope** — real concepts at a different layer
  (durability, cross-machine, language-level concurrency primitives).
  Tessera deliberately doesn't address them.

## Tessera v0.1 — primitives + services

| Concept | Layer | Contract (one-line) | Use case | Built on | Status |
|---|---|---|---|---|---|
| **Pool** | Primitive | Non-lossy, lease/return, large opaque payloads, single-writer-per-slot, point-to-point handoff via descriptor | Hand off 64 MB training batch / parquet chunk / model snapshot between owner and worker | raw SHM | Shipped (v0.0.1) |
| **Ring** | Primitive | Lossy, multi-writer, multi-reader broadcast, small-to-medium payloads, fire-and-forget, FIFO per section | Telemetry / metrics / log fan-out to multiple independent consumers (TUI + Prometheus + archiver) | raw SHM | Shipped (v0.0.1) |
| **Channel** | Primitive | Non-lossy, multi-producer / single-consumer, small typed messages, FIFO, blocking-or-fail-fast backpressure | Reliable queue: control plane, ack plane, RPC messages, anything where dropping is not OK | raw SHM | Shipped (v0.0.1) |
| **Sink** | Service | Atomic-write-to-disk worker pool with chunking + hash integrity + optional fsync | Persist large artifacts (batches, snapshots, logs) durably from a hot-path producer without blocking it | Pool (payload) + Channel (control + ack) | In flight (PR #10) |

### Tid-bits on each

**Pool** — the "big-bytes lifetime" primitive. If you need a worker
to read 64 MB without copying through pickle or a socket, this is
the answer. Single-owner lifecycle, single-writer-per-slot. Workers
attach by description (BLAKE3) and read by descriptor handoff.
Doesn't broadcast; one descriptor = one (or few) readers. Stale
descriptors fail validation via slot-generation counter rather than
corrupt a re-leased slot.

**Ring** — the "every consumer sees everything, ok to lose old
events" primitive. Per-slot seqlock; writer fetch_add the section's
writer_position then odd-then-even the slot sequence. Readers
maintain process-local cursors and detect lapping via
`oldest_available = latest - slot_count`. Lossy by design — no
backpressure on writers — which is exactly what you want for
telemetry but exactly wrong for control/RPC. Multi-section: one Ring
region can carry independent `logs` + `metrics` + `errors` streams
without coordination.

**Channel** — the "non-lossy queue" primitive. Fills the gap between
Pool (too heavy / blob-shaped) and Ring (too lossy) for small typed
messages where dropping is not acceptable. Credit-based backpressure:
producers fail-fast / block / time out when the queue is full,
they don't overwrite. Required by Sink's control + ack planes.
Without it, Sink either lives in-process-only or papers over the gap
with OS pipes external to Tessera's primitive set.

**Sink** — the first composite service. Reads payload bytes from a
Pool slot, streams them to disk atomically (temp-file + rename),
verifies via blake3. Owner spawns N worker OS processes; control via
per-worker Channel; ack via shared MPSC Channel. Owner-held leases
with renewal timer per §3.5.d. Chunked streaming with worker affinity
when a payload exceeds slot_size. fsync per-submit (default off).

## Future candidates — not in v0.1, may land in v0.2+

| Concept | Layer | Contract / shape | Use case | Why not v0.1 |
|---|---|---|---|---|
| **Heartbeat / liveness** | Service or sub-primitive | Atomic timestamps in shared bytes; staleness = peer death | Worker-pool health monitoring | Composable from Pool + atomic ops; build when a real composite (Sink, others) needs it as an external API |
| **Barrier / latch** | Service | Wait until N peers signal a rendezvous point | Coordinated startup / shutdown of worker groups, ML training all-reduce sync | Compose from Channel + counter; ship if multiple Tessera consumers request it |
| **Shared KV / map** | Service | Concurrent hashmap over SHM | Cross-process feature cache, model-version lookup | Compose from Pool (values) + Channel (invalidations); large design surface; defer until clear demand |
| **Journal / append-only log** | Service | Durable ordered append + replay, time-bounded retention | Audit trails, event sourcing, replayable test fixtures | Crosses into durability concerns; likely warrants a separate library rather than living inside Tessera |
| **Lossy SPSC small** | Primitive (matrix cell #1) | Lossy single-consumer queue, small payloads | "Best-effort task queue; ok to drop tasks" | Niche; no concrete demand surfaced |
| **Lossy big-blob broadcast** | Primitive (matrix cell #6) | Lossy multi-reader, large payloads | Streaming video / large telemetry blobs to multiple lossy consumers | Combines Ring's drop semantics with Pool's slot cost; very niche; defer until concrete demand |
| **Reliable broadcast / durable bus** | Adjacent (matrix cell #4) | Non-lossy multi-reader, replay across restarts | Kafka-style event bus | Usually disk-backed and cross-machine; belongs in a different layer/library |

### Tid-bits on each

**Heartbeat** — Bayence ships one (`HeartbeatShm` in
`bayence/telemetry/writer_pool.py`, ~50 LOC: a small SHM of u64
timestamps per writer; the pool's monitor thread reads them every
second and restarts stale writers). The pattern fits inside a Sink
implementation; promoting it to a top-level Tessera primitive would
be premature — most consumers don't need it.

**Barrier / latch** — useful for ML training start-of-step sync,
coordinated rolling restart of worker groups. The Bayence training
loop has a barrier-like construct over multiprocessing.Event. Easy to
build atop Channel once Channel exists.

**Shared KV / map** — concurrent hashmap over SHM, with eviction.
The Tessera Pool roadmap (§5.5) already mentions "typed slot views"
and "eviction-aware lease shapes" as forward-compatible additions —
that's the direction this composite would go.

**Journal / append-only log** — durability concerns push this out
of pure SHM territory. A real journal needs WAL semantics, log
compaction, replay-from-position. Probably belongs in a sibling
library that uses Tessera Channel for hot-path append and disk for
the cold path.

**Lossy SPSC small** (matrix cell #1) — degenerate case of Ring
with single reader. Niche. If demand surfaces, easier as a Ring mode
flag than as a new primitive.

**Lossy big-blob broadcast** (matrix cell #6) — combines Ring's
lossy-drop drawback with Pool's per-slot cost. Hard to think of a
real use case that wouldn't be better served by either disk-backed
journal (durability) or a smaller-payload Ring (telemetry-shaped).

**Reliable broadcast / durable bus** (matrix cell #4) — this is
Kafka territory. Cross-machine, disk-backed, with replay-from-offset
semantics. A different design point; doesn't fit the SHM-primitive
abstraction Tessera targets.

## Deliberately out of scope

| Concept | Why out of scope |
|---|---|
| **Cross-process mutex / semaphore** | OS already provides pthread_mutex with PTHREAD_PROCESS_SHARED; futex on Linux. Wrapping these adds little value over POSIX |
| **Persistent / disk-backed bytes** | Durability ≠ IPC. Different concern; belongs in a different library (e.g. a Tessera-Journal future sibling) |
| **Cross-machine messaging** | Network is a different layer with different failure modes (partition, latency, reordering). Out of scope for shared-memory IPC |
| **Language-level concurrency primitives** | std::sync::Mutex, crossbeam channels, tokio — in-process Rust concerns, not Tessera's job |
| **Container orchestration / service discovery** | Higher up the stack; Tessera consumers handle this themselves (Docker `ipc:` namespace sharing, k8s pod IPC config, etc.) |

## The matrix view (for completeness)

Principal axes: lossiness × reader topology × payload shape.

| # | Lossiness | Readers | Payload | Tessera coverage |
|---|---|---|---|---|
| 1 | Lossy | 1 | Small | Future (likely Ring mode flag if demanded) |
| 2 | Lossy | N (broadcast) | Small/medium | **Ring** ✓ |
| 3 | Non-lossy | 1 (queue) | Small/medium | **Channel** (Stage 4c) |
| 4 | Non-lossy | N (broadcast) | Small/medium | Out of scope (usually disk-backed) |
| 5 | Non-lossy | 1 or N | Large blob | **Pool** ✓ |
| 6 | Lossy | N | Large blob | Future (very niche) |

Three filled cells from day one (Pool / Ring / Channel) = the
complete useful primitive surface for byte-level SHM IPC. Three
unfilled cells are either future modes / primitives if demand
surfaces, or deliberate out-of-scope.

## Prior art for the trio

- **Boost.Interprocess** — `shared_memory_object` + `mapped_region`
  (Pool-ish) + `message_queue` (Channel-ish). No Ring equivalent in
  the stock library.
- **iceoryx2** (Eclipse, Rust) — pub/sub (Ring-ish with optional
  history depth), event (sub-primitive signal), request/response
  (composite over channels). Fuses Ring + Channel under one pub/sub
  umbrella with a mode knob.
- **shmipc** (Bytedance) — MPSC ring buffer (Channel) + memory pool
  (Pool). Two primitives; no broadcast / Ring.
- **Aeron** (Real Logic, Java) — broadcast-only with subscription
  cursors + reliable subscriptions (NAK retransmit). One primitive
  with two modes.

Tessera's three-primitive split keeps state machines distinct rather
than unifying behind a mode flag — explicit at the cost of one extra
crate.
