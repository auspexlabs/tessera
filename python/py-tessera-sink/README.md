# tessera-sink (Python)

Python facade for [`tessera-sink`](../../crates/tessera-sink/) — an
atomic-write worker pool to disk.

**Status**: v0.0.1, Stage 4d implemented.

```python
from tessera_sink import Sink

with Sink(description="my-app/artifacts",
          worker_count=4,
          pool_slot_count=8,
          pool_slot_size_bytes=64 * 1024 * 1024) as sink:
    sink.submit("/data/out.parquet", payload_bytes, fsync=True)
    sink.flush()
```

You hand `submit` pre-serialized `bytes`; chunking, BLAKE3 hashing, and
atomic temp+rename all happen in the Rust core and worker subprocesses
(no serialization in Python). The worker executable is discovered via
the `worker_bin_path` kwarg, the `TESSERA_SINK_WORKER_BIN` env var, a
sibling of the current executable, then `PATH`.

The class is `unsendable` and blocks while holding the GIL (the owner is
single-threaded); parallelism comes from the worker subprocesses. Drive
a Sink from one Python thread.
