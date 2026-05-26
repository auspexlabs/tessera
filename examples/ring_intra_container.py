"""Intra-container Ring demo: owner publishes, subprocess consumer drains.

Topology T1 ("Intra-container") per the Tessera design — owner process
+ consumer subprocesses inside the same container, sharing the SHM
region via BLAKE3-derived namespace handle.

The Ring's contract differs from Pool's:
  - Ring is **lossy**: writers never block on readers. If the consumer
    is slow, older events are overwritten and the consumer's
    ``stats().dropped`` counter accounts the gap.
  - Ring is **broadcast**: every reader handle starts at the current
    writer position and sees all subsequent events independently of
    other readers (multi-reader broadcast — not work-distribution).
  - Ring has no lease lifecycle: ``Writer.publish(section_id, bytes)``
    is fire-and-forget.

Run from the workspace root:
    python examples/ring_intra_container.py

Requires ``tessera-ring`` installed in the active venv (``maturin
develop`` from ``python/py-tessera-ring/``).
"""

from __future__ import annotations

import multiprocessing as mp
import os
import time

from tessera_ring import Ring


# Ring geometry. Intentionally small + intentionally faster owner than
# consumer so the demo also illustrates lossy semantics (the consumer's
# stats().dropped > 0).
N_SLOTS = 8
SLOT_SIZE = 1024
N_PUBLISHES = 25
SECTION_ID = 0


def consumer_main(description: str, ready: "mp.Event", done: "mp.Event") -> None:
    """Subprocess: attach to the SHM region and drain Section 0 until
    the owner signals 'done' AND no more new events are arriving."""
    with Ring(
        description=description,
        sections=[(SECTION_ID, N_SLOTS, SLOT_SIZE)],
        is_owner=False,
    ) as ring:
        reader = ring.reader(SECTION_ID)
        ready.set()

        seen = 0
        while True:
            events = reader.poll()
            if events:
                seen += len(events)
                first, last = events[0], events[-1]
                if first.position == last.position:
                    print(
                        f"  consumer[{os.getpid()}]: drained 1 event "
                        f"at position={first.position} payload={first.payload!r}"
                    )
                else:
                    print(
                        f"  consumer[{os.getpid()}]: drained {len(events)} events "
                        f"(positions {first.position}..{last.position})"
                    )
            elif done.is_set():
                # Owner stopped publishing AND we have nothing new — exit.
                break
            else:
                # Idle: short poll interval. In production this would
                # be driven by an event loop or a select() on multiple
                # sources.
                time.sleep(0.005)

        stats = reader.stats()
        print(
            f"  consumer[{os.getpid()}]: total drained={seen}; "
            f"final stats: cursor={stats.cursor} latest={stats.latest} dropped={stats.dropped}"
        )


def main() -> None:
    description = f"tessera-example/ring-intra/{os.getpid()}"
    ready = mp.Event()
    done = mp.Event()

    with Ring(
        description=description,
        sections=[(SECTION_ID, N_SLOTS, SLOT_SIZE)],
    ) as ring:
        print(f"owner[{os.getpid()}]: created Ring (slots={N_SLOTS}, size={SLOT_SIZE})")

        consumer = mp.Process(
            target=consumer_main,
            args=(description, ready, done),
            daemon=True,
        )
        consumer.start()

        # Wait for the consumer to attach + open its reader. Without
        # this barrier, fresh-reader-starts-at-current-writer-position
        # would mean the consumer misses events published before it
        # gets there.
        if not ready.wait(timeout=5.0):
            raise SystemExit("consumer did not attach within 5s")

        writer = ring.writer()
        for i in range(N_PUBLISHES):
            payload = f"event-{i:03d}".encode()
            writer.publish(SECTION_ID, payload)
            # Faster than the consumer's 5ms poll interval, so the
            # consumer falls behind and demonstrates lossy behavior.
            time.sleep(0.001)

        # Give the consumer a final chance to catch up, then signal done.
        time.sleep(0.05)
        done.set()
        consumer.join(timeout=5.0)
        if consumer.is_alive():
            consumer.terminate()
            raise SystemExit("consumer did not exit cleanly")

        print(f"owner: published {N_PUBLISHES} events; consumer joined cleanly")


if __name__ == "__main__":
    main()
