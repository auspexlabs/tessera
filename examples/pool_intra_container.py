"""Intra-container Pool demo: producer + worker subprocess sharing one SHM region.

Topology T1 ("Intra-container") per the Tessera design — owner process
+ worker subprocesses inside the same container. The owner creates a
Pool, hands each chunk's Descriptor to a worker through an IPC
channel, and the worker reads the payload bytes from shared memory
(no copy through pickle of the payload itself; only the small
Descriptor crosses the queue).

Important pattern note ("owner-held lease"):
The owner MUST NOT release a lease until the worker has finished
reading the descriptor's payload. Releasing the lease frees the slot;
a subsequent `acquire` bumps the slot's generation, which invalidates
any in-flight descriptor still on its way to a worker. This example
sends all descriptors first, then waits for the worker to ack each
one, and only then releases the corresponding lease.

Run from the workspace root:
    python examples/pool_intra_container.py

Requires ``tessera-pool`` installed in the active venv (``maturin
develop`` from ``python/py-tessera-pool/``).
"""

from __future__ import annotations

import multiprocessing as mp
import os
import time

from tessera_pool import Descriptor, Pool


def worker_main(
    description: str,
    descriptors_in: "mp.Queue[tuple[int, Descriptor] | None]",
    acks_out: "mp.Queue[int]",
) -> None:
    """Subprocess: attach to the SHM region, read each descriptor's payload,
    ack the chunk back to the owner, exit on receiving the sentinel ``None``."""
    pool = Pool(
        description=description,
        slot_count=N_SLOTS,
        slot_size_bytes=SLOT_SIZE,
        is_owner=False,
    )
    received = 0
    total_bytes = 0
    while True:
        item = descriptors_in.get()
        if item is None:
            break
        chunk_id, descriptor = item
        payload = pool.read_payload(descriptor)
        received += 1
        total_bytes += len(payload)
        acks_out.put(chunk_id)
    print(f"  worker[{os.getpid()}]: drained {received} descriptors, {total_bytes:,} bytes")


N_SLOTS = 6
SLOT_SIZE = 1024 * 1024
N_CHUNKS = 6  # ≤ N_SLOTS so we don't have to recycle leases mid-flight


def main() -> None:
    description = f"tessera-example/pool-intra/{os.getpid()}"
    descriptors_q: "mp.Queue[tuple[int, Descriptor] | None]" = mp.Queue()
    acks_q: "mp.Queue[int]" = mp.Queue()

    with Pool(
        description=description,
        slot_count=N_SLOTS,
        slot_size_bytes=SLOT_SIZE,
        ttl_seconds=30.0,
    ) as pool:
        print(f"owner[{os.getpid()}]: created {pool}")

        worker = mp.Process(
            target=worker_main,
            args=(description, descriptors_q, acks_q),
            daemon=True,
        )
        worker.start()
        # Give the worker a moment to attach. In real code, an
        # explicit "ready" handshake would replace this sleep.
        time.sleep(0.1)

        # Send all N_CHUNKS, holding each lease until the worker ack's.
        outstanding: dict[int, "object"] = {}
        for i in range(N_CHUNKS):
            payload = f"batch-{i:03d}".encode() * 4096  # ~32 KB each
            lease = pool.acquire(timeout_seconds=2.0)
            descriptor = pool.write(lease, payload)
            outstanding[i] = lease
            descriptors_q.put((i, descriptor))
            print(f"  owner: handed off batch-{i:03d} ({len(payload):,} bytes)")

        # Tell the worker we're done sending.
        descriptors_q.put(None)

        # Wait for each chunk's ack, then release the lease. Order
        # doesn't matter — acks come in the order the worker drains.
        for _ in range(N_CHUNKS):
            chunk_id = acks_q.get(timeout=5.0)
            lease = outstanding.pop(chunk_id)
            pool.release(lease)
            print(f"  owner: released lease for batch-{chunk_id:03d}")

        worker.join(timeout=5.0)
        if worker.is_alive():
            worker.terminate()
            raise SystemExit("worker did not exit cleanly")

        print(f"owner: all chunks acked + released. in_use_count={pool.in_use_count()}")


if __name__ == "__main__":
    main()
