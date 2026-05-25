"""Tessera Pool — non-lossy lease-backed shared-memory pool primitive.

Thin Python facade over the Rust core in ``tessera-pool``. The native
extension module (``tessera_pool._native``) provides the implementation;
this package re-exports the public surface for ergonomic import.

```python
from tessera_pool import Pool

with Pool(description="my-app/batches",
          slot_count=8,
          slot_size_bytes=64 * 1024 * 1024) as pool:
    lease = pool.acquire(timeout_seconds=1.0)
    descriptor = pool.write(lease, payload_bytes)
    # hand descriptor across IPC to a worker; the worker calls
    # pool.read_payload(descriptor)
    pool.release(lease)
```

Public symbols:

- ``Pool``: the pool itself.
- ``Lease``: owner-side lease handle.
- ``Descriptor``: read-only IPC token.
- ``TesseraPoolError``: base exception class for all pool errors.
"""

from tessera_pool._native import Descriptor, Lease, Pool, TesseraPoolError

__version__ = "0.0.1"
__all__ = ["Descriptor", "Lease", "Pool", "TesseraPoolError"]
