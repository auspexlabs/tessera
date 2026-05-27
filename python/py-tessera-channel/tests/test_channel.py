"""End-to-end tests for tessera_channel's PyO3 facade.

Each test constructs its own Channel(s) with a uniquely-keyed
description so parallel test execution doesn't collide on the same
SHM region. The SHM region is unlinked when the Channel's Rust Drop
fires (i.e. when the Python object is garbage-collected at scope
exit, or explicitly via ``close()`` / ``__exit__``).

Run with: ``pytest python/py-tessera-channel/tests/`` from the
workspace root, after ``maturin develop`` has installed the wheel
into the active venv.
"""

from __future__ import annotations

import os
import threading
import time
import uuid

import pytest

from tessera_channel import Channel, TesseraChannelError


def _unique_description(tag: str) -> str:
    """A description string unique to this test invocation."""
    return f"tessera-channel-test/{tag}/{os.getpid()}/{uuid.uuid4().hex}"


def _pair(tag: str, slot_count: int = 8, slot_size: int = 64) -> tuple[Channel, Channel]:
    """Open a (receiver, sender) pair by shared description."""
    desc = _unique_description(tag)
    receiver = Channel(
        description=desc,
        slot_count=slot_count,
        slot_size_bytes=slot_size,
        role="receiver",
    )
    sender = Channel(
        description=desc,
        slot_count=slot_count,
        slot_size_bytes=slot_size,
        role="sender",
    )
    return receiver, sender


# ---------------------------------------------------------------- construction


def test_channel_construct_and_metadata():
    desc = _unique_description("construct")
    with Channel(
        description=desc,
        slot_count=8,
        slot_size_bytes=64,
        role="receiver",
    ) as recv:
        assert recv.is_owner is True
        assert recv.role == "receiver"
        assert recv.slot_count == 8
        assert recv.slot_size_bytes == 64
        assert recv.is_closed is False


def test_channel_construct_sender_role():
    desc = _unique_description("sender-construct")
    with Channel(
        description=desc,
        slot_count=4,
        slot_size_bytes=32,
        role="receiver",
    ) as _recv:
        with Channel(
            description=desc,
            slot_count=4,
            slot_size_bytes=32,
            role="sender",
        ) as send:
            assert send.is_owner is False
            assert send.role == "sender"


@pytest.mark.parametrize("alias", ["receiver", "RECEIVER", "recv", "consumer"])
def test_receiver_role_aliases_accepted(alias):
    desc = _unique_description(f"role-alias-{alias}")
    with Channel(
        description=desc,
        slot_count=4,
        slot_size_bytes=32,
        role=alias,
    ) as chan:
        assert chan.role == "receiver"


@pytest.mark.parametrize("alias", ["sender", "SENDER", "send", "producer"])
def test_sender_role_aliases_accepted(alias):
    desc = _unique_description(f"sender-alias-{alias}")
    # Need a receiver to exist first for the sender to attach.
    with Channel(
        description=desc,
        slot_count=4,
        slot_size_bytes=32,
        role="receiver",
    ) as _r:
        with Channel(
            description=desc,
            slot_count=4,
            slot_size_bytes=32,
            role=alias,
        ) as chan:
            assert chan.role == "sender"


def test_invalid_role_rejected():
    desc = _unique_description("invalid-role")
    with pytest.raises(TesseraChannelError, match="invalid role"):
        Channel(
            description=desc,
            slot_count=4,
            slot_size_bytes=32,
            role="bogus",
        )


@pytest.mark.parametrize(
    "kwargs",
    [
        # zero slot count
        dict(slot_count=0, slot_size_bytes=64, role="receiver"),
        # zero slot size
        dict(slot_count=4, slot_size_bytes=0, role="receiver"),
        # slot size not multiple of 8
        dict(slot_count=4, slot_size_bytes=17, role="receiver"),
    ],
)
def test_channel_construct_rejects_invalid_config(kwargs):
    kwargs["description"] = _unique_description("invalid-config")
    with pytest.raises(TesseraChannelError):
        Channel(**kwargs)


# ---------------------------------------------------------------- send / recv happy path


def test_send_then_recv_returns_payload():
    recv, send = _pair("send-recv")
    try:
        send.send(b"hello channel")
        got = recv.recv()
        assert got == b"hello channel"
        head, tail = recv.positions()
        assert head == 1
        assert tail == 1
    finally:
        send.close()
        recv.close()


def test_multiple_messages_arrive_in_order():
    recv, send = _pair("ordered", slot_count=16, slot_size=8)
    try:
        for i in range(10):
            send.send(i.to_bytes(4, "little"))
        for i in range(10):
            got = recv.recv()
            assert got == i.to_bytes(4, "little")
    finally:
        send.close()
        recv.close()


def test_ring_wraps_correctly_after_full_drain():
    recv, send = _pair("wraparound", slot_count=4, slot_size=16)
    try:
        for cycle in range(3):
            for i in range(4):
                send.send(f"c{cycle}-i{i}".encode())
            for i in range(4):
                got = recv.recv()
                assert got == f"c{cycle}-i{i}".encode()
        head, tail = recv.positions()
        assert head == 12
        assert tail == 12
    finally:
        send.close()
        recv.close()


# ---------------------------------------------------------------- non-blocking variants


def test_try_send_fails_fast_when_full():
    recv, send = _pair("try-send-full", slot_count=2, slot_size=16)
    try:
        send.try_send(b"a")
        send.try_send(b"b")
        with pytest.raises(TesseraChannelError, match="full"):
            send.try_send(b"c")
        # Drain one, then try_send succeeds again.
        recv.recv()
        send.try_send(b"c")
    finally:
        send.close()
        recv.close()


def test_try_recv_fails_fast_when_empty():
    recv, send = _pair("try-recv-empty", slot_count=4, slot_size=16)
    try:
        with pytest.raises(TesseraChannelError, match="empty"):
            recv.try_recv()
    finally:
        send.close()
        recv.close()


# ---------------------------------------------------------------- bounded blocking


def test_send_timeout_returns_timeout_on_full():
    recv, send = _pair("send-timeout", slot_count=1, slot_size=16)
    try:
        send.send(b"a")
        # Second send blocks; short timeout expects Timeout error.
        with pytest.raises(TesseraChannelError, match="timed out"):
            send.send_timeout(b"b", timeout_seconds=0.05)
    finally:
        send.close()
        recv.close()


def test_recv_timeout_returns_timeout_on_empty():
    recv, send = _pair("recv-timeout", slot_count=4, slot_size=16)
    try:
        with pytest.raises(TesseraChannelError, match="timed out"):
            recv.recv_timeout(timeout_seconds=0.05)
    finally:
        send.close()
        recv.close()


def test_timeout_validates_seconds():
    recv, send = _pair("timeout-validation", slot_count=2, slot_size=16)
    try:
        with pytest.raises(TesseraChannelError, match="must be"):
            send.send_timeout(b"x", timeout_seconds=-1.0)
        with pytest.raises(TesseraChannelError, match="finite"):
            send.send_timeout(b"x", timeout_seconds=float("inf"))
        with pytest.raises(TesseraChannelError, match="unreasonably large"):
            send.send_timeout(b"x", timeout_seconds=1e15)
    finally:
        send.close()
        recv.close()


# ---------------------------------------------------------------- error paths


def test_send_rejects_oversized_payload():
    recv, send = _pair("oversized", slot_count=4, slot_size=16)
    try:
        with pytest.raises(TesseraChannelError, match="exceeds"):
            send.send(b"x" * 17)
    finally:
        send.close()
        recv.close()


def test_role_mismatch_send_on_receiver():
    recv, send = _pair("role-mismatch-1", slot_count=4, slot_size=16)
    try:
        with pytest.raises(TesseraChannelError, match="role"):
            recv.send(b"x")
    finally:
        send.close()
        recv.close()


def test_role_mismatch_recv_on_sender():
    recv, send = _pair("role-mismatch-2", slot_count=4, slot_size=16)
    try:
        with pytest.raises(TesseraChannelError, match="role"):
            send.try_recv()
    finally:
        send.close()
        recv.close()


def test_operations_on_closed_channel_raise():
    desc = _unique_description("closed")
    chan = Channel(
        description=desc,
        slot_count=4,
        slot_size_bytes=16,
        role="receiver",
    )
    chan.close()
    assert chan.is_closed is True
    with pytest.raises(TesseraChannelError, match="closed"):
        chan.try_recv()
    with pytest.raises(TesseraChannelError, match="closed"):
        chan.positions()
    # Idempotent close.
    chan.close()


def test_context_manager_closes_on_exit():
    desc = _unique_description("ctx-exit")
    chan = Channel(
        description=desc,
        slot_count=4,
        slot_size_bytes=16,
        role="receiver",
    )
    with chan as inside:
        assert inside.is_closed is False
    assert chan.is_closed is True


# ---------------------------------------------------------------- positions


def test_positions_reflects_send_recv_progression():
    recv, send = _pair("positions", slot_count=8, slot_size=8)
    try:
        assert recv.positions() == (0, 0)
        send.send(b"\x00" * 4)
        assert recv.positions() == (0, 1)
        send.send(b"\x00" * 4)
        assert recv.positions() == (0, 2)
        recv.recv()
        assert recv.positions() == (1, 2)
        recv.recv()
        assert recv.positions() == (2, 2)
    finally:
        send.close()
        recv.close()


# ---------------------------------------------------------------- concurrent MPSC


def test_serial_multiple_producers_simulation_via_separate_handles():
    # MPSC correctness IS validated at the Rust core level — see
    # `concurrent_multiple_producers_single_consumer_preserves_all_messages`
    # in `crates/tessera-channel/src/channel.rs` which spawns 4
    # native threads, each opening its own RustChannel, and drains
    # 400 messages without loss.
    #
    # On the Python facade in v0.1 we can't currently demonstrate
    # the same with threading.Thread — Channel is `!Send` (because
    # Region's Shmem is `!Send`) so the Python class is marked
    # `unsendable` in the PyO3 facade. Blocking `send()` / `recv()`
    # calls spin inside Rust while holding the GIL, which deadlocks
    # any cross-thread Python pattern (one thread blocks holding the
    # GIL, another can't acquire it to make progress).
    #
    # The planned thread-safe facade contract is role-specific:
    # Sender can become concurrently callable, while one Receiver
    # handle must stay one-caller-at-a-time or be internally serialized.
    # For now Python users wanting multi-producer should use
    # `multiprocessing` — each subprocess gets its own GIL.
    #
    # This test exercises the equivalent code path serially: open
    # multiple Sender handles in sequence, interleaved with recvs,
    # to confirm the facade correctly handles multiple Sender
    # objects against the same Channel region.
    desc = _unique_description("serial-multi-sender")
    N_SENDERS = 3
    N_PER_SENDER = 10

    receiver = Channel(
        description=desc,
        slot_count=8,
        slot_size_bytes=16,
        role="receiver",
    )

    senders = [
        Channel(
            description=desc,
            slot_count=8,
            slot_size_bytes=16,
            role="sender",
        )
        for _ in range(N_SENDERS)
    ]
    try:
        # Interleave sends from different sender handles. After each
        # round of sends, drain. This validates that the Channel
        # region correctly handles multiple Sender handles attaching
        # and sending in sequence.
        for round_idx in range(3):
            for sid, sender in enumerate(senders):
                for i in range(N_PER_SENDER):
                    payload = sid.to_bytes(4, "little") + (round_idx * N_PER_SENDER + i).to_bytes(
                        4, "little"
                    )
                    # try_send so we stay non-blocking (and confirm
                    # 8-slot ring can absorb 30 sends interleaved
                    # with drains).
                    try:
                        sender.try_send(payload)
                    except TesseraChannelError:
                        # Queue full — drain one and retry.
                        _ = receiver.recv()
                        sender.try_send(payload)
        # Final drain.
        while True:
            try:
                _ = receiver.try_recv()
            except TesseraChannelError:
                break
        assert receiver.positions()[0] == receiver.positions()[1]
    finally:
        for s in senders:
            s.close()
        receiver.close()


# ---------------------------------------------------------------- force_recreate


def test_force_recreate_clobbers_existing_region():
    desc = _unique_description("force-recreate")
    first = Channel(
        description=desc,
        slot_count=2,
        slot_size_bytes=16,
        role="receiver",
    )
    # Without force_recreate, second create on same description refuses.
    with pytest.raises(TesseraChannelError, match="already exists"):
        Channel(
            description=desc,
            slot_count=2,
            slot_size_bytes=16,
            role="receiver",
        )
    # With force_recreate, succeeds.
    second = Channel(
        description=desc,
        slot_count=2,
        slot_size_bytes=16,
        role="receiver",
        force_recreate=True,
    )
    second.close()
    first.close()


# ---------------------------------------------------------------- repr


def test_channel_repr_includes_state():
    recv, send = _pair("repr", slot_count=4, slot_size=16)
    try:
        r_repr = repr(recv)
        s_repr = repr(send)
        assert "Channel" in r_repr
        assert "receiver" in r_repr
        assert "sender" in s_repr
        assert "slot_count=4" in r_repr
        assert "slot_size_bytes=16" in r_repr
    finally:
        send.close()
        recv.close()
