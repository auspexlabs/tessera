# tessera-pool (Python)

Python facade for [`tessera-pool`](../../crates/tessera-pool/): a
lease-backed shared-memory pool for large opaque byte payloads.

**Status:** v0.0.1. Functional in development builds; not yet
published to PyPI.

## Install

```bash
cd python/py-tessera-pool
maturin develop --release
```

## Quick Start

```python
from tessera_pool import Pool

with Pool(
    description="my-app/training-batches",
    slot_count=8,
    slot_size_bytes=64 * 1024 * 1024,
    ttl_seconds=60.0,
) as pool:
    lease = pool.acquire(timeout_seconds=1.0)
    descriptor = pool.write(lease, payload_bytes)

    # Send `descriptor` to a worker process by Channel, Pipe, Queue, etc.
    copy = pool.read_payload(descriptor)

    pool.release(lease)
```

The owner creates the region. Readers attach with the same description
and geometry:

```python
with Pool(
    description="my-app/training-batches",
    slot_count=8,
    slot_size_bytes=64 * 1024 * 1024,
    is_owner=False,
) as pool:
    copy = pool.read_payload(descriptor)
```

If an owner tries to create a region that already exists, construction
fails unless `force_recreate=True` is passed. Use that option only when
the caller intentionally owns cleanup of a stale region.

## Public Surface

- `Pool(...)` — context-manager-friendly constructor.
- `acquire(timeout_seconds=30.0)` — owner-only; returns a lease.
- `write(lease, bytes)` — owner-only; writes payload bytes and returns a
  descriptor.
- `read_payload(descriptor)` — reads and validates a descriptor, returning
  owned `bytes`.
- `release(lease)`, `renew(lease)`, `reclaim_stale()` — owner-only lease
  management.
- `in_use_count()` — diagnostic count of leased slots.
- `close()` — releases the facade's handle.
- `TesseraPoolError` — base exception class.

## Threading

`Pool` is `Send + Sync` — a handle may be moved between threads and used
concurrently. `acquire`/`write`/`release`/`renew`/`reclaim_stale` and the
`read_payload` read path are serialized per slot internally; blocking
`acquire` releases the GIL and holds no lock across its wait, so a
blocked `acquire` never prevents a `release` on another thread, and
`close()` wakes a blocked `acquire`.

`read_payload` is correct-or-`TesseraPoolError` (`StaleHandle`) under the
single-writer-lease protocol (owner holds the lease until the reader has
consumed the payload; reclaim is crash-recovery only). A fully race-free
copy under a *protocol violation* — rewriting a slot in one process while
another reads it cross-process — is a v0.2 item. See
[`docs/issue_facade_thread_safety.md`](../../docs/issue_facade_thread_safety.md).
