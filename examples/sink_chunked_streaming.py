"""Chunked-streaming Sink demo: one large payload, few Pool slots.

Shows how Sink streams a payload larger than a single Pool slot: the
owner splits it into slot-sized chunks, and because there are fewer
slots than chunks, the owner must drain worker acks (which release
leases) to recycle slots mid-job. The worker reassembles the chunks
in order and verifies the BLAKE3 hash before the atomic rename.

Run from the workspace root:
    cargo build -p tessera-sink-worker
    python examples/sink_chunked_streaming.py

Requires ``tessera-sink`` installed in the active venv
(``maturin develop`` from ``python/py-tessera-sink/``).
"""

from __future__ import annotations

import hashlib
import os
import pathlib
import tempfile

from tessera_sink import Sink

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]

# 4 KiB slots, only 4 of them, but a ~256 KiB payload → ~64 chunks.
# The owner recycles the 4 slots ~16 times, driven by worker acks.
SLOT_SIZE = 4 * 1024
SLOT_COUNT = 4
PAYLOAD_BYTES = 256 * 1024


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
    description = f"tessera-example/sink-chunked/{os.getpid()}"
    payload = bytes((i * 31 + 7) % 256 for i in range(PAYLOAD_BYTES))
    expected_digest = hashlib.blake3(payload).hexdigest() if hasattr(hashlib, "blake3") else None

    with tempfile.TemporaryDirectory() as outdir:
        path = os.path.join(outdir, "large.bin")
        with Sink(
            description=description,
            worker_count=2,
            pool_slot_count=SLOT_COUNT,
            pool_slot_size_bytes=SLOT_SIZE,
            worker_bin_path=worker_bin,
        ) as sink:
            n_chunks = (PAYLOAD_BYTES + SLOT_SIZE - 1) // SLOT_SIZE
            print(
                f"owner[{os.getpid()}]: submitting {PAYLOAD_BYTES} bytes as "
                f"~{n_chunks} chunks across {SLOT_COUNT} pool slots"
            )
            sink.submit(path, payload)
            sink.flush()
            print("owner: flush complete")

        on_disk = pathlib.Path(path).read_bytes()
        assert on_disk == payload, "reassembled file does not match payload"
        msg = f"verified {len(on_disk)} bytes reassembled byte-exact"
        if expected_digest is not None:
            assert hashlib.blake3(on_disk).hexdigest() == expected_digest
            msg += " (blake3 digest matches)"
        print(msg)


if __name__ == "__main__":
    main()
