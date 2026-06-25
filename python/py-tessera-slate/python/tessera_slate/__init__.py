"""Tessera Slate — seqlock-protected latest-value snapshot slot table.

Thin Python facade over the Rust core in ``tessera-slate``. The native
extension module (``tessera_slate._native``) provides the implementation;
this package re-exports the public surface for ergonomic import.

```python
from tessera_slate import Slate

with Slate(description="my-app/snapshots",
           slot_count=8,
           slot_size_bytes=64) as slate:
    slate.write_slot(2, b"hi")
    reader = slate.reader()
    read = reader.read_slot(2)
    if read.is_slot:
        print(read.sequence, read.value)
```

Public symbols:

- ``Slate``: the writer / owner; context-manager-friendly.
- ``SlateReader``: read-only handle; also returned by ``Slate.reader()``.
- ``Header``: frozen result of ``SlateReader.header()``.
- ``SlotRead``: frozen result of ``SlateReader.read_slot()``.
- ``TesseraSlateError``: base exception class for all slate errors.
"""

from tessera_slate._native import (
    Header,
    Slate,
    SlateReader,
    SlotRead,
    TesseraSlateError,
    _slot_read_from_parts,
)

__version__ = "0.0.1"
__all__ = ["Header", "Slate", "SlateReader", "SlotRead", "TesseraSlateError"]
