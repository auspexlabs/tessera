# Tessera examples

Runnable cross-component demos. Each prints what it's doing and asserts
the expected outcome, so a clean exit means the demo passed.

Run from the workspace root after installing the relevant package
(`maturin develop` from the matching `python/py-tessera-*/` directory).
The Sink examples additionally need the worker binary:
`cargo build -p tessera-sink-worker`.

| File | Demonstrates |
|---|---|
| `pool_intra_container.py` | Single-container Pool: owner process + worker subprocess sharing one SHM region by BLAKE3-derived description; lease → write → descriptor handoff → read → release. |
| `ring_intra_container.py` | Single-container Ring: one writer, one reader, per-reader cursor + drop accounting. |
| `ring_broadcast.py` | Multi-reader broadcast — one writer, several concurrent readers, each tracking its own drop count. |
| `channel_intra_container.py` | Non-lossy MPSC Channel: receiver (parent) + sender (subprocess), FIFO drain. |
| `channel_mpsc.py` | N producer subprocesses → one consumer, exercising multi-producer `tail` contention. |
| `sink_atomic_write.py` | Sink basics: submit several `(path, bytes)` jobs across worker subprocesses, flush, verify each file is byte-exact on disk. |
| `sink_chunked_streaming.py` | One large payload streamed as many chunks through few Pool slots — the owner recycles slots via worker acks; worker reassembles + BLAKE3-verifies before the atomic rename. |
