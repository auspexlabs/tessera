"""Atomic-write Sink demo: submit several payloads, flush, verify.

Topology "Intra-container" per the Tessera design — the Sink owner
(this process) and its worker subprocesses share the SHM regions
(Pool + control/ack Channels) inside one container.

Sink's contract for this demo:
  - You hand ``submit(path, bytes)`` a target path and pre-serialized
    bytes. The library chunks the payload across Pool slots, streams
    each chunk to a worker subprocess, verifies a BLAKE3 hash, and
    atomically renames a temp file into place — so a reader never sees
    a partially written file.
  - ``submit`` returns once the work is *queued*; ``flush`` waits for
    every job to land on disk and raises on the first failure.
  - Parallelism comes from the worker subprocesses, not threads.

Run from the workspace root:
    cargo build -p tessera-sink-worker
    python examples/sink_atomic_write.py

Requires ``tessera-sink`` installed in the active venv
(``maturin develop`` from ``python/py-tessera-sink/``).
"""

from __future__ import annotations

import os
import pathlib
import tempfile

from tessera_sink import Sink

# examples/ -> repo root
_REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]


def _worker_bin() -> str:
    for profile in ("debug", "release"):
        candidate = _REPO_ROOT / "target" / profile / "tessera-sink-worker"
        if candidate.exists():
            return str(candidate)
    raise SystemExit(
        "worker binary not found — run `cargo build -p tessera-sink-worker` first"
    )


def main() -> None:
    worker_bin = _worker_bin()
    description = f"tessera-example/sink-atomic/{os.getpid()}"

    with tempfile.TemporaryDirectory() as outdir:
        with Sink(
            description=description,
            worker_count=3,
            pool_slot_count=8,
            pool_slot_size_bytes=64 * 1024,
            worker_bin_path=worker_bin,
        ) as sink:
            print(
                f"owner[{os.getpid()}]: started Sink with "
                f"{sink.worker_count} worker subprocesses"
            )

            expected = {}
            for i in range(6):
                path = os.path.join(outdir, f"artifact-{i}.bin")
                payload = (f"artifact {i}: ".encode()) + bytes((j % 256) for j in range(1000 * (i + 1)))
                job_id = sink.submit(path, payload, fsync=(i % 2 == 0))
                expected[path] = payload
                print(f"  submitted artifact-{i}.bin ({len(payload)} bytes), job_id={job_id:#034x}")

            sink.flush()
            print("owner: flush complete — all jobs landed on disk")

        # Verify every file is present and byte-exact.
        for path, payload in expected.items():
            on_disk = pathlib.Path(path).read_bytes()
            assert on_disk == payload, f"mismatch for {path}"
        print(f"verified {len(expected)} files written atomically and byte-exact")


if __name__ == "__main__":
    main()
