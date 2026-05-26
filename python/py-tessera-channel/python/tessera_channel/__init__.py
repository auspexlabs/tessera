"""Tessera Channel — non-lossy MPSC shared-memory queue.

Thin Python facade over the Rust core in ``tessera-channel``. The
native extension module (``tessera_channel._native``) provides the
implementation; this package re-exports the public surface for
ergonomic import.

```python
from tessera_channel import Channel

# Receiver creates the region:
with Channel(description="my-app/control",
             slot_count=256,
             slot_size_bytes=4096,
             role="receiver") as chan:
    msg = chan.recv()

# Sender (in another process) attaches:
with Channel(description="my-app/control",
             slot_count=256,
             slot_size_bytes=4096,
             role="sender") as chan:
    chan.send(b"hello channel")
```

Public symbols:

- ``Channel``: context-manager-friendly Channel class.
- ``TesseraChannelError``: base exception class for all Channel errors.
"""

from tessera_channel._native import Channel, TesseraChannelError

__version__ = "0.0.1"
__all__ = ["Channel", "TesseraChannelError"]
