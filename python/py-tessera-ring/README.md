# tessera-ring (Python)

Python facade for [`tessera-ring`](../../crates/tessera-ring/): a lossy
multi-writer / multi-reader shared-memory ring for byte events.

**Status:** v0.0.1. Functional in development builds; not yet
published to PyPI.

## Install

```bash
cd python/py-tessera-ring
maturin develop --release
```

## Quick Start

```python
from tessera_ring import Ring

with Ring(
    description="my-app/telemetry",
    sections=[(0, 4096, 2048)],
) as ring:
    writer = ring.writer()
    reader = ring.reader(0)        # fresh reader starts at "now"

    writer.publish(0, b"hello")

    for event in reader.poll():
        print(event.position, event.payload)

    print(reader.stats())          # ReaderStats(cursor=..., latest=..., dropped=...)
```

`sections` is a list of `(section_id, slot_count, slot_size_bytes)`
tuples. The library does not classify event bytes; callers map their own
logical stream names to integer `section_id` values.

See [`examples/ring_intra_container.py`](../../examples/ring_intra_container.py)
for a cross-process owner + subprocess-consumer demo.

## Public Surface

- `Ring` — context-manager-friendly; constructs or attaches the SHM
  region.
- `writer()` — returns a Writer.
- `reader(section_id)` — returns a fresh Reader for one section.
- `Writer.publish(section_id, bytes)` — publishes one event.
- `Reader.poll() -> list[Event]` — returns owned event copies.
- `Reader.stats() -> ReaderStats` — returns cursor/latest/drop
  diagnostics.
- `Event`, `ReaderStats`, `TesseraRingError`.

## Threading Limitation

The Python classes are currently `unsendable`; use each Ring, Writer,
and Reader object from the thread that created it. Cross-process sharing
is supported, and each process should open its own handle. The planned
thread-safe contract is role-specific: Writer can become concurrently
callable, while one Reader handle must stay one-caller-at-a-time or be
internally serialized.
