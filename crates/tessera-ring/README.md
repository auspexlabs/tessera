# tessera-ring

Lossy mmap-backed multi-writer / multi-reader ring buffer. Per-section
write cursors with per-slot seqlock counters, fresh-readers-start-at-now
semantics, reader-side gap accounting.

**Status**: v0.0.1 — Rust core and PyO3 facade functional. CI wiring +
docs polish + crates.io / PyPI publish land in Stage 5 of the upstream
extraction plan.

## What it does

- **Sections in shared memory** — one SHM region per Ring; caller
  defines a list of named sections, each with its own `slot_count` and
  `slot_size_bytes`. Layout is `[GlobalHeader][SectionHeader × N][per-section slot arrays]`,
  documented in `src/header.rs`. Sections are addressed by caller-supplied
  `section_id`, not by ordinal; the library does not classify event
  bytes.
- **BLAKE3-derived namespace** — `Ring::open(config)` hashes the
  caller-supplied `description` and uses the digest both as the POSIX
  SHM segment name (`/tessera-ring-<hex>`) and as a cross-verification
  token in the header. Peers attaching with the same description
  automatically share the same region.
- **Multi-writer broadcast** — `Writer::publish(section_id, bytes)`
  claims the next slot via atomic `fetch_add` on the section's
  `writer_position` counter. Multiple writers in different processes
  can publish concurrently to the same section without coordination.
- **Multi-reader, each consumer sees everything** — `Reader` cursors
  live in process-local memory, not in SHM. A TUI, a log archiver,
  and a Prometheus exporter can all read the same Ring section
  independently; lapped readers detect the gap and account it
  (`stats().dropped`).
- **Per-slot seqlock** — each `SlotHeader` carries its own atomic
  `sequence` counter. Writers stamp odd-then-even around the payload
  copy; readers spin briefly on odd, otherwise check
  before/after-equality plus a position cross-check to confirm the
  slot wasn't overwritten mid-read.
- **Fresh-reader semantics** — `Ring::reader(section_id)` opens the
  reader at the current writer position. Fresh readers see only NEW
  events, not historical ring contents (per §4b lock).
- **Lossy by design** — writers never block on readers. If the ring
  is full, the writer claims the next slot, the oldest entry is
  overwritten, and the lap shows up in slow readers'
  `ReaderStats.dropped`.

## Quick start

```rust
use tessera_ring::{Ring, RingConfig, SectionConfig};

let ring = Ring::open(RingConfig {
    description: "my-app/telemetry".into(),
    sections: vec![SectionConfig::new(/*section_id=*/0, /*slot_count=*/4096, /*slot_size_bytes=*/2048)],
    is_owner: true,
    force_recreate: false,
})?;

// Concurrent writers & readers in the same process — or in any
// attached peer process via Ring::open(RingConfig { is_owner: false, … }).
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
`from tessera_ring import Ring` with the same API.

## Tests

`cargo test -p tessera-ring` — 54 tests covering:

- Header layout invariants + Pod round-trips (15)
- BLAKE3-derived namespace derivation + Pool/Ring name disjointness (5)
- Region create/attach lifecycle + section-geometry validation + slot
  accessors + cross-attacher visibility (23)
- Ring state machine: publish + poll happy path, ordered delivery,
  lap accounting, fresh-reader-at-now, multi-reader broadcast,
  multi-section isolation, oversized/unknown rejection, ReaderStats,
  cross-process attach (11)

Plus 26 Python end-to-end tests in `python/py-tessera-ring/tests/` and
a runnable cross-process example at `examples/ring_intra_container.py`.

## Roadmap

- **v0.1.0 (Stage 5)**: publish to crates.io with the current public
  surface.
- **Forward-compatible additions** (no API break needed):
  - zero-copy `Reader::poll_view()` returning slot-borrowed slices
    (v0.2)
  - typed slot views (Arrow / NumPy backed) atop the per-slot payload
  - in-SHM drop counters for cross-process observability without
    process-local cursors

The v0.1 surface anticipates all of these via opaque types and a
section-list config that already supports per-section geometry.
