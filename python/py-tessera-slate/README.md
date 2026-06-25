# tessera-slate (Python)

Python facade for [`tessera-slate`](../../crates/tessera-slate/): a
seqlock-protected latest-value snapshot slot table in shared memory.

**Status:** v0.0.1. Functional in development builds; not yet
published to PyPI.

## Install

```bash
cd python/py-tessera-slate
maturin develop --release
```

## Quick Start

```python
from tessera_slate import Slate

with Slate(
    description="my-app/snapshots",
    slot_count=8,
    slot_size_bytes=64,
) as slate:
    reader = slate.reader()           # shares the writer's mapping

    slate.write_slot(2, b"hi")        # overwrite slot 2 in place

    read = reader.read_slot(2)
    if read.is_slot:
        print(read.sequence, read.value)   # 2 b'hi'

    print(reader.header())            # Header(writer_seq=..., last_update_ns=...)
```

Slate has no history: each slot holds one latest value that a writer
overwrites in place and readers poll for. Slots are statically addressed
by integer index; callers map their own logical names to slot indices.

A reader may also attach directly to an existing region:

```python
from tessera_slate import SlateReader

with SlateReader("my-app/snapshots", slot_count=8, slot_size_bytes=64) as reader:
    read = reader.read_slot(2)
```

## Public Surface

- `Slate` — writer / owner; context-manager-friendly; constructs or
  attaches the SHM region.
- `Slate.write_slot(index, data)` — overwrites one slot.
- `Slate.reader()` — returns a `SlateReader` sharing the mapping.
- `Slate.unlink()` — owner-only; unlinks the SHM name (requires no other
  live handle).
- `SlateReader` — read-only handle; context-manager-friendly.
- `SlateReader.read_slot(index) -> SlotRead` — latest coherent snapshot.
- `SlateReader.header() -> Header` — region-global write counters.
- `SlotRead`, `Header`, `TesseraSlateError`.

## Read outcomes

`read_slot` returns a `SlotRead` whose `state` is one of:

- `"slot"` — a coherent value; `value` holds the payload bytes,
  `sequence` is the (even) seqlock value, `timestamp_nanos` is the write
  time. `is_slot` is `True`.
- `"empty"` — the slot has never been written. `value` is `None`;
  `sequence` / `timestamp_nanos` are `0`. `is_empty` is `True`.
- `"torn"` — every retry collided with a concurrent write. Keep the
  previous value and poll again. `value` is `None`. `is_torn` is `True`.

## Threading

One writer per slot is the protocol: distinct slots may be written from
distinct threads / processes concurrently, but two writers on the *same*
slot is a protocol violation. Readers are lock-free and torn-read-
tolerant — any number may poll concurrently, including from other
processes. Reads and writes never block, so the facade does not release
the GIL around them.
