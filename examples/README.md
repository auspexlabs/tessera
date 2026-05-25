# Tessera examples

**Status**: v0.0.1 scaffold. The example files below are placeholders;
they're populated in Stage 4 alongside the corresponding component
implementations.

| File | Demonstrates | Lands in |
|---|---|---|
| `pool_intra_container.py` | Single-container Pool usage: owner process + child workers sharing one SHM region by fork inheritance. | Stage 4a |
| `pool_paired_containers/` | Two-container Pool deployment with compose `ipc:` namespace sharing. `docker-compose.yml` + `producer.py` + `consumer.py` + `README.md`. | Stage 4a |
| `ring_broadcast.py` | Multi-reader broadcast — one writer, three concurrent readers, each tracking its own drop count. | Stage 4b |
| `sink_atomic_write.py` | Basic Sink usage: submit `(path, bytes)`; verify atomic on-disk rename. | Stage 4c |
| `sink_with_pool_handoff.py` | Cross-component demo: Sink producer acquires Pool leases, hands descriptors to worker subprocesses, owner-side lease renewal during long writes. | Stage 4c |
