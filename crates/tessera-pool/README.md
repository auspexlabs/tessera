# tessera-pool

Non-lossy, lease-backed shared-memory pool for large opaque payloads.
Pool gives one authority process fixed-size slots, descriptor handoff,
stale-lease reclamation, and generation checks that reject stale
handles instead of corrupting a reused slot.

**Status:** v0.0.1. Rust core and PyO3 facade are implemented; the
crate is still pre-v0.1 and not yet published.

## What it does

- **Fixed slots in shared memory** — one SHM region per Pool, sized at
  construction. Layout is `[Header][SlotMeta x N][PayloadArea]`.
- **BLAKE3-derived namespace** — `Pool::new(config)` hashes the
  caller-supplied `description` and uses the digest as both the POSIX
  SHM segment name and a verification token in the header.
- **Single-owner lifecycle** — one process creates the region
  (`is_owner: true`). Attachers use the same description with
  `is_owner: false`. If the owner tries to create a region that already
  exists, construction fails unless `force_recreate: true` is set.
- **Single-writer lease semantics** — only the owner acquires, writes,
  releases, renews, and reclaims slots. Attachers consume payload bytes
  by descriptor handoff.
- **Generation invalidation** — every slot carries a generation counter.
  Stale descriptors fail validation when a slot is released, reclaimed,
  or re-leased.
- **POSIX SHM through `shared_memory`** — cross-process and
  cross-container use works when the peers share an IPC namespace.

Pool is deliberately byte-oriented. Serialize at the boundary, hand off
owned bytes, and deserialize on the receiving side.

## Quick Start

```rust
use std::time::Duration;
use tessera_pool::{Pool, PoolConfig};

let mut pool = Pool::new(PoolConfig {
    description: "my-app/training-batches".into(),
    slot_count: 8,
    slot_size_bytes: 64 * 1024 * 1024,
    is_owner: true,
    ttl_micros: 60_000_000,
    force_recreate: false,
})?;

let lease = pool.acquire(Duration::from_secs(1))?;
let descriptor = pool.write(&lease, &payload_bytes)?;

// Hand `descriptor` to a worker over Channel, a process pipe, etc.
let read_back = pool.read_payload(&descriptor)?;

pool.release(&lease)?;
# Ok::<(), tessera_pool::TesseraPoolError>(())
```

An attached reader uses the same geometry and `is_owner: false`:

```rust
let pool = Pool::new(PoolConfig {
    description: "my-app/training-batches".into(),
    slot_count: 8,
    slot_size_bytes: 64 * 1024 * 1024,
    is_owner: false,
    ttl_micros: 60_000_000,
    force_recreate: false,
})?;

let bytes = pool.read_payload(&descriptor)?;
# Ok::<(), tessera_pool::TesseraPoolError>(())
```

## Threading Contract

The current Rust core inherits `!Send` / `!Sync` constraints from the
underlying mmap owner, and the Python facade is marked `unsendable`.
Use one handle from one thread today. Cross-process use is the supported
sharing model.

The intended cross-thread contract is being designed in
[`docs/issue_facade_thread_safety.md`](../../docs/issue_facade_thread_safety.md).
Pool is the hardest case because `acquire`/`release` concurrency and the
`read_payload` validate-then-copy path both need internal synchronization.

## Tests

```bash
cargo test -p tessera-pool
```

The tests cover header layout, namespace derivation, create/attach
lifecycle, acquire/write/read/release/renew/reclaim flows, stale-handle
rejection, oversized payloads, and attacher restrictions.

## Roadmap

- v0.1: publish the current byte-oriented API once the public docs,
  packaging, and thread-safety contract are locked.
- Later candidates: typed slot views, zero-copy borrowed views with
  explicit lifetimes, eviction-aware lease shapes, and peer/multi-owner
  modes if a concrete use case justifies the extra protocol surface.
