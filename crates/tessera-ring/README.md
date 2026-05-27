# tessera-ring

Lossy mmap-backed multi-writer / multi-reader ring buffer for small to
medium byte events. Writers never block on readers; slow readers detect
and count gaps.

**Status:** v0.0.1. Rust core and PyO3 facade are functional; the crate
is still pre-v0.1 and not yet published.

## What it does

- **Sections in shared memory** — one SHM region per Ring; callers
  define integer `section_id` values, each with its own `slot_count` and
  `slot_size_bytes`. Layout is
  `[GlobalHeader][SectionHeader x N][per-section slot arrays]`.
- **BLAKE3-derived namespace** — `Ring::open(config)` hashes the
  caller-supplied `description` and uses the digest as both the POSIX
  SHM segment name and a verification token in the header.
- **Multi-writer publish** — `Writer::publish(section_id, bytes)` claims
  the next slot through an atomic section writer cursor.
- **Multi-reader broadcast** — Reader cursors live in process-local
  memory, not in SHM. Independent readers over the same section each see
  the stream from their own cursor and account their own drops.
- **Per-slot seqlock** — writers stamp odd-then-even around the payload
  copy; readers check the sequence and slot position before accepting a
  payload.
- **Fresh-reader semantics** — `Ring::reader(section_id)` starts at the
  current writer position. New readers see future events, not historical
  ring contents.
- **Lossy by design** — if writers lap a reader, the oldest unread entry
  is overwritten and the reader's `stats().dropped` increases.

## Quick Start

```rust
use tessera_ring::{Ring, RingConfig, SectionConfig};

let ring = Ring::open(RingConfig {
    description: "my-app/telemetry".into(),
    sections: vec![SectionConfig::new(0, 4096, 2048)],
    is_owner: true,
    force_recreate: false,
})?;

// Additional peer processes attach with RingConfig { is_owner: false, ... }.
let writer = ring.writer();
let mut reader = ring.reader(0)?;

writer.publish(0, b"hello tessera ring")?;
for event in reader.poll()? {
    // event.section_id, event.position, event.timestamp_nanos, event.payload
}
# Ok::<(), tessera_ring::TesseraRingError>(())
```

For Python ergonomics, install the
[`tessera-ring`](../../python/py-tessera-ring/) Python facade and use
`from tessera_ring import Ring` with the same concepts.

## Threading Contract

Ring is `Send + Sync` (the seqlock protocol is concurrent by design), so
handles move between threads and the facade is no longer `unsendable`.
The contract is role-specific:

- **Writer** is concurrently callable — multiple Writer handles, on
  multiple threads or processes, publish via the seqlock without external
  locking.
- **Reader** keeps a process-local cursor and `poll(&mut self)`, so a
  single Reader handle is one-caller-at-a-time (the facade serializes it
  internally). Independent Reader handles remain independent.

See [`docs/issue_facade_thread_safety.md`](../../docs/issue_facade_thread_safety.md).

## Tests

```bash
cargo test -p tessera-ring
```

The tests cover header layout, namespace derivation, create/attach
lifecycle, section geometry, publish/poll behavior, ordered delivery,
lap accounting, fresh-reader-at-now, multi-reader broadcast,
multi-section isolation, oversized/unknown section rejection, stats,
and cross-process attach.

## Roadmap

- v0.1: publish the current byte-oriented API once the public docs,
  packaging, and thread-safety contract are locked.
- Later candidates: zero-copy `Reader::poll_view()`, typed slot views,
  and in-SHM drop counters for process-external observability.
