"""MPSC demo: N subprocess senders, single receiver, every message arrives.

This example exercises Channel's distinctive property — non-lossy
multi-producer / single-consumer over SHM. N subprocess senders
each open their own Sender handle (attached by description) and
publish K messages; the receiver (parent process) drains all N*K
messages.

Because Channel is non-lossy, EVERY message must arrive (no drops,
unlike Ring where slow readers can be lapped). The example asserts
this at the end by checking that every (sender_id, sequence) pair
shows up exactly once in the receiver's drained set.

Why subprocesses (not threads): the Python `Channel` class is
currently `unsendable` and blocking `send()` / `recv()` calls hold
the GIL while spinning. Cross-thread MPSC via `threading.Thread`
would deadlock. Subprocesses sidestep this because each has its own
GIL. The planned thread-safe contract is role-specific: Sender can
become concurrently callable, while one Receiver handle must stay
one-caller-at-a-time or be internally serialized.

Run from the workspace root:
    python examples/channel_mpsc.py

Requires ``tessera-channel`` installed in the active venv
(``maturin develop`` from ``python/py-tessera-channel/``).
"""

from __future__ import annotations

import multiprocessing as mp
import os

from tessera_channel import Channel


N_SENDERS = 4
N_PER_SENDER = 50
SLOT_COUNT = 16
SLOT_SIZE = 16


def sender_main(
    sender_id: int,
    description: str,
    ready: "mp.Event",
    barrier: "mp.Barrier",
) -> None:
    """Subprocess: open a Sender handle, wait for the barrier so all
    senders start together, then publish K messages."""
    with Channel(
        description=description,
        slot_count=SLOT_COUNT,
        slot_size_bytes=SLOT_SIZE,
        role="sender",
    ) as chan:
        ready.set()
        # Wait for all senders to be attached before any of them
        # starts publishing. Makes the demo's traffic pattern
        # representative of real concurrent producer load.
        barrier.wait(timeout=10.0)
        for i in range(N_PER_SENDER):
            payload = sender_id.to_bytes(4, "little") + i.to_bytes(4, "little")
            chan.send(payload)  # Blocks if queue full — non-lossy.
    print(f"  sender[{os.getpid()}, id={sender_id}]: published {N_PER_SENDER} messages")


def main() -> None:
    description = f"tessera-example/channel-mpsc/{os.getpid()}"
    total = N_SENDERS * N_PER_SENDER

    ready_events = [mp.Event() for _ in range(N_SENDERS)]
    barrier = mp.Barrier(N_SENDERS + 1)  # senders + parent (the receiver)

    with Channel(
        description=description,
        slot_count=SLOT_COUNT,
        slot_size_bytes=SLOT_SIZE,
        role="receiver",
    ) as chan:
        print(
            f"receiver[{os.getpid()}]: created Channel "
            f"(slot_count={SLOT_COUNT}, slot_size={SLOT_SIZE}, "
            f"expecting {total} messages)"
        )

        senders = [
            mp.Process(
                target=sender_main,
                args=(sid, description, ready_events[sid], barrier),
                daemon=True,
                name=f"sender-{sid}",
            )
            for sid in range(N_SENDERS)
        ]
        for s in senders:
            s.start()

        # Wait for every sender to attach before crossing the barrier.
        for sid, ev in enumerate(ready_events):
            if not ev.wait(timeout=10.0):
                raise SystemExit(f"sender {sid} did not attach within 10s")

        # Cross the barrier so all senders start publishing
        # concurrently.
        barrier.wait(timeout=10.0)

        seen: set[tuple[int, int]] = set()
        while len(seen) < total:
            msg = chan.recv()
            assert len(msg) == 8
            sid = int.from_bytes(msg[:4], "little")
            seq = int.from_bytes(msg[4:], "little")
            assert 0 <= sid < N_SENDERS, f"sender_id {sid} out of range"
            assert 0 <= seq < N_PER_SENDER, f"seq {seq} out of range"
            assert (sid, seq) not in seen, f"duplicate message ({sid}, {seq})"
            seen.add((sid, seq))
            if len(seen) % (total // 4) == 0 or len(seen) == total:
                print(f"  receiver: drained {len(seen)}/{total} messages")

        for s in senders:
            s.join(timeout=10.0)
            if s.is_alive():
                s.terminate()
                raise SystemExit(f"{s.name} did not exit cleanly")

        head, tail = chan.positions()
        assert head == tail == total, f"expected head==tail=={total}, got ({head}, {tail})"
        print(
            f"receiver: MPSC verified — {total} messages from {N_SENDERS} senders, "
            f"every (sender_id, seq) pair delivered exactly once. "
            f"Final positions head={head} tail={tail}."
        )


if __name__ == "__main__":
    main()
