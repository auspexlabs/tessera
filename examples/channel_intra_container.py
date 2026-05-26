"""Intra-container Channel demo: receiver (parent) + sender (subprocess).

Topology T1 ("Intra-container") per the Tessera design — receiver
process + sender subprocess inside the same container, sharing the
SHM region via BLAKE3-derived namespace handle.

Channel's contract for this demo:
  - Non-lossy MPSC: messages are queued, not dropped, even if the
    receiver is slower than the sender. Sender blocks (or fails-fast
    with try_send, depending on call) when the queue fills.
  - Single Receiver: the parent process owns the region's lifecycle.
    Multiple Senders may attach (multi-producer); v0.1 has one in
    this example, ``channel_mpsc.py`` exercises N producers.
  - Bytes-only payload: callers serialize before send if they need
    typed messages.

Run from the workspace root:
    python examples/channel_intra_container.py

Requires ``tessera-channel`` installed in the active venv
(``maturin develop`` from ``python/py-tessera-channel/``).
"""

from __future__ import annotations

import multiprocessing as mp
import os
import time

from tessera_channel import Channel


SLOT_COUNT = 16
SLOT_SIZE = 64
N_MESSAGES = 20


def sender_main(description: str, ready: "mp.Event", done: "mp.Event") -> None:
    """Subprocess: attach to the Channel region as Sender, publish
    N messages, signal done, exit."""
    with Channel(
        description=description,
        slot_count=SLOT_COUNT,
        slot_size_bytes=SLOT_SIZE,
        role="sender",
    ) as chan:
        ready.set()
        for i in range(N_MESSAGES):
            payload = f"event-{i:03d}".encode()
            chan.send(payload)  # Blocks if queue full — non-lossy.
            # Modest pacing so the receiver can interleave drains.
            if i % 4 == 3:
                time.sleep(0.002)
        print(f"  sender[{os.getpid()}]: published {N_MESSAGES} messages")
    done.set()


def main() -> None:
    description = f"tessera-example/channel-intra/{os.getpid()}"
    ready = mp.Event()
    done = mp.Event()

    with Channel(
        description=description,
        slot_count=SLOT_COUNT,
        slot_size_bytes=SLOT_SIZE,
        role="receiver",
    ) as chan:
        print(
            f"receiver[{os.getpid()}]: created Channel "
            f"(slot_count={SLOT_COUNT}, slot_size={SLOT_SIZE})"
        )

        sender = mp.Process(
            target=sender_main,
            args=(description, ready, done),
            daemon=True,
        )
        sender.start()

        # Wait for the sender to attach. Without this barrier the
        # receiver might call recv() before the sender exists — recv
        # would block forever waiting for a producer that never
        # connects (in this demo the sender is the only producer).
        if not ready.wait(timeout=5.0):
            raise SystemExit("sender did not attach within 5s")

        received = 0
        while received < N_MESSAGES:
            msg = chan.recv()  # Blocks until a message is available.
            received += 1
            if received % 5 == 0 or received == N_MESSAGES:
                print(
                    f"  receiver: drained {received}/{N_MESSAGES} messages; "
                    f"latest payload={msg!r}"
                )

        sender.join(timeout=5.0)
        if sender.is_alive():
            sender.terminate()
            raise SystemExit("sender did not exit cleanly")

        head, tail = chan.positions()
        print(
            f"receiver: final positions head={head} tail={tail} "
            f"(head==tail means queue fully drained)"
        )
        assert head == tail == N_MESSAGES


if __name__ == "__main__":
    main()
