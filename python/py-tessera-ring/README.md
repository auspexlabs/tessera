# tessera-ring (Python)

Python facade for [`tessera-ring`](../../crates/tessera-ring/) — lossy
mmap-backed multi-writer / multi-reader ring buffer.

**Status**: v0.0.1 — functional. CI / PyPI publish lands in Stage 5.

## Install (development)

```bash
maturin develop --release   # builds the native extension into the active venv
```

## Quick start

```python
from tessera_ring import Ring

with Ring(description="my-app/telemetry",
          sections=[(0, 4096, 2048)]) as ring:
    writer = ring.writer()
    reader = ring.reader(0)        # fresh reader starts at "now"
    writer.publish(0, b"hello")
    for event in reader.poll():
        print(event.position, event.payload)
    print(reader.stats())          # ReaderStats(cursor=…, latest=…, dropped=…)
```

`sections` is a list of `(section_id, slot_count, slot_size_bytes)`
3-tuples. The library does not classify event bytes — sections are
caller-named logical streams inside one Ring region.

See [`examples/ring_intra_container.py`](../../examples/ring_intra_container.py)
for a cross-process owner + subprocess-consumer demo, and the workspace
[`tessera-ring`](../../crates/tessera-ring/) README for the full design
notes.

## Public surface

- `Ring` — context-manager-friendly; constructs / attaches the SHM region.
- `Writer` — `publish(section_id, bytes)`.
- `Reader` — `poll() -> list[Event]`, `stats() -> ReaderStats`,
  `cursor` / `dropped` getters.
- `Event` — frozen, picklable result of `poll()`.
- `ReaderStats` — frozen result of `stats()`.
- `TesseraRingError` — base exception class.
