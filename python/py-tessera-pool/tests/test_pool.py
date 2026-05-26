"""End-to-end tests for tessera_pool's PyO3 facade.

Each test constructs its own Pool with a uniquely-keyed description
so parallel test execution doesn't collide on the same SHM region.
The SHM region is unlinked when the Pool's Rust ``Drop`` fires
(i.e. when the Python object is garbage-collected at scope exit).

Run with: ``pytest python/py-tessera-pool/tests/`` from the workspace
root, after ``maturin develop`` has installed the wheel into the
active venv.
"""

from __future__ import annotations

import os
import time
import uuid

import pytest

from tessera_pool import Descriptor, Lease, Pool, TesseraPoolError


def _unique_description(tag: str) -> str:
    """A description string unique to this test invocation.

    Combines the test tag + PID + a fresh uuid4 so parallel test
    runs (xdist, repeats, CI sharding) don't collide on the underlying
    SHM segment name.
    """
    return f"tessera-pool-test/{tag}/{os.getpid()}/{uuid.uuid4().hex}"


# ---------------------------------------------------------------- construction


def test_pool_construct_and_metadata():
    desc = _unique_description("construct")
    with Pool(
        description=desc,
        slot_count=4,
        slot_size_bytes=1024,
        ttl_seconds=10.0,
    ) as pool:
        assert pool.is_owner is True
        assert pool.slot_count == 4
        assert pool.slot_size_bytes == 1024
        assert pool.ttl_seconds == pytest.approx(10.0, rel=1e-6)
        assert pool.in_use_count() == 0


@pytest.mark.parametrize(
    "kwargs",
    [
        # missing required field
        dict(slot_count=4, slot_size_bytes=64, ttl_seconds=10.0),
        # zero slot count
        dict(description="x", slot_count=0, slot_size_bytes=64, ttl_seconds=10.0),
        # zero slot size
        dict(description="x", slot_count=4, slot_size_bytes=0, ttl_seconds=10.0),
    ],
)
def test_pool_construct_rejects_invalid_config(kwargs):
    with pytest.raises((TypeError, TesseraPoolError)):
        Pool(**kwargs)


def test_pool_construct_rejects_overflowing_region_size():
    """Additional from sequential review: u32::MAX slots of u32::MAX
    bytes would overflow usize on region size math. Region::create
    surfaces this as a Config error before any shm_open."""
    with pytest.raises(TesseraPoolError, match="overflow|exceeds usize"):
        Pool(
            description=_unique_description("region-overflow"),
            slot_count=0xFFFFFFFF,
            slot_size_bytes=0xFFFFFFFF,
            ttl_seconds=10.0,
        )


# ---------------------------------------------------------------- acquire / release


def test_acquire_release_roundtrip():
    desc = _unique_description("acq-rel")
    with Pool(description=desc, slot_count=3, slot_size_bytes=128, ttl_seconds=10.0) as pool:
        leases = [pool.acquire(timeout_seconds=1.0) for _ in range(3)]
        assert pool.in_use_count() == 3
        # All distinct slot indices, distinct lease IDs.
        assert len({lease.slot_index for lease in leases}) == 3
        assert len({lease.lease_id_hex for lease in leases}) == 3
        for lease in leases:
            pool.release(lease)
        assert pool.in_use_count() == 0


def test_acquire_times_out_when_exhausted():
    desc = _unique_description("exhausted")
    with Pool(description=desc, slot_count=1, slot_size_bytes=64, ttl_seconds=10.0) as pool:
        _ = pool.acquire(timeout_seconds=1.0)
        # Second acquire on a single-slot pool with no release: must time out fast.
        with pytest.raises(TesseraPoolError):
            pool.acquire(timeout_seconds=0.05)


def test_release_returns_slot_to_pool():
    desc = _unique_description("release-returns")
    with Pool(description=desc, slot_count=1, slot_size_bytes=64, ttl_seconds=10.0) as pool:
        lease_a = pool.acquire(timeout_seconds=1.0)
        # Pool is now exhausted.
        with pytest.raises(TesseraPoolError):
            pool.acquire(timeout_seconds=0.02)
        pool.release(lease_a)
        # After release, acquire works again. Same slot, different lease.
        lease_b = pool.acquire(timeout_seconds=1.0)
        assert lease_b.slot_index == lease_a.slot_index
        assert lease_b.lease_id_hex != lease_a.lease_id_hex
        assert lease_b.generation != lease_a.generation
        pool.release(lease_b)


# ---------------------------------------------------------------- write / read


def test_write_read_payload_roundtrip():
    desc = _unique_description("write-read")
    payload = b"the rain in spain falls mainly in the plain"
    with Pool(description=desc, slot_count=2, slot_size_bytes=512, ttl_seconds=10.0) as pool:
        lease = pool.acquire(timeout_seconds=1.0)
        descriptor = pool.write(lease, payload)
        assert isinstance(descriptor, Descriptor)
        assert descriptor.slot_index == lease.slot_index
        assert descriptor.size_bytes == len(payload)
        assert descriptor.lease_id_hex == lease.lease_id_hex

        read_back = pool.read_payload(descriptor)
        assert isinstance(read_back, bytes)
        assert read_back == payload

        pool.release(lease)


def test_write_rejects_oversized_payload():
    desc = _unique_description("oversized")
    with Pool(description=desc, slot_count=1, slot_size_bytes=16, ttl_seconds=10.0) as pool:
        lease = pool.acquire(timeout_seconds=1.0)
        with pytest.raises(TesseraPoolError, match="exceeds slot_size_bytes"):
            pool.write(lease, b"x" * 32)
        pool.release(lease)


def test_double_write_on_same_lease_rejected():
    desc = _unique_description("double-write")
    with Pool(description=desc, slot_count=1, slot_size_bytes=64, ttl_seconds=10.0) as pool:
        lease = pool.acquire(timeout_seconds=1.0)
        pool.write(lease, b"first")
        with pytest.raises(TesseraPoolError, match="write_after_finalize"):
            pool.write(lease, b"second")
        pool.release(lease)


@pytest.mark.parametrize(
    "size_bytes",
    [0, 1, 256, 1024, 4 * 1024 * 1024],
)
def test_write_read_handles_edge_sizes(size_bytes):
    desc = _unique_description(f"sizes-{size_bytes}")
    with Pool(
        description=desc,
        slot_count=1,
        slot_size_bytes=4 * 1024 * 1024,
        ttl_seconds=10.0,
    ) as pool:
        lease = pool.acquire(timeout_seconds=1.0)
        # Deterministic payload: repeat the size as a marker byte.
        marker = (size_bytes & 0xFF).to_bytes(1, "little") if size_bytes > 0 else b""
        payload = marker * size_bytes
        descriptor = pool.write(lease, payload)
        assert descriptor.size_bytes == size_bytes
        read_back = pool.read_payload(descriptor)
        assert len(read_back) == size_bytes
        assert read_back == payload
        pool.release(lease)


# ---------------------------------------------------------------- reclaim / renew


def test_reclaim_stale_bumps_generation_and_invalidates_descriptor():
    desc = _unique_description("reclaim-stale")
    with Pool(
        description=desc,
        slot_count=1,
        slot_size_bytes=64,
        # Microsecond TTL so reclaim fires after a tiny sleep.
        ttl_seconds=0.000001,
    ) as pool:
        lease = pool.acquire(timeout_seconds=1.0)
        descriptor = pool.write(lease, b"ephemeral")
        time.sleep(0.01)

        reclaimed = pool.reclaim_stale()
        assert reclaimed == 1
        assert pool.in_use_count() == 0

        # Original descriptor is now stale.
        with pytest.raises(TesseraPoolError, match="stale handle"):
            pool.read_payload(descriptor)

        # Original lease release also fails.
        with pytest.raises(TesseraPoolError, match="stale handle"):
            pool.release(lease)


def test_renew_keeps_lease_alive():
    desc = _unique_description("renew")
    # 50ms TTL.
    with Pool(description=desc, slot_count=1, slot_size_bytes=64, ttl_seconds=0.05) as pool:
        lease = pool.acquire(timeout_seconds=1.0)
        time.sleep(0.03)
        pool.renew(lease)
        time.sleep(0.03)
        # Total elapsed > TTL; without renew, reclaim would fire.
        reclaimed = pool.reclaim_stale()
        assert reclaimed == 0
        assert pool.in_use_count() == 1
        pool.release(lease)


# ---------------------------------------------------------------- attacher


def test_attacher_can_read_descriptor_handoff():
    """Single-process simulation of the owner→worker handoff: the
    owner Pool creates the region, writes a payload, hands the
    Descriptor to an attacher Pool (constructed with is_owner=False)
    which reads it back."""
    desc = _unique_description("attach-handoff")
    owner = Pool(description=desc, slot_count=2, slot_size_bytes=128, ttl_seconds=10.0)
    try:
        attacher = Pool(
            description=desc,
            slot_count=2,
            slot_size_bytes=128,
            is_owner=False,
            # ttl_seconds is ignored for attachers (inherited from header).
        )
        try:
            assert attacher.is_owner is False
            # Attacher inherits ttl from header.
            assert attacher.ttl_seconds == pytest.approx(10.0, rel=1e-6)

            lease = owner.acquire(timeout_seconds=1.0)
            descriptor = owner.write(lease, b"handed across IPC")

            read_back = attacher.read_payload(descriptor)
            assert read_back == b"handed across IPC"
        finally:
            del attacher
    finally:
        # Release before drop so the slot is clean.
        # (Not strictly required — Drop will unlink the region either way.)
        del owner


def test_attacher_cannot_mutate():
    desc = _unique_description("attach-readonly")
    owner = Pool(description=desc, slot_count=1, slot_size_bytes=64, ttl_seconds=10.0)
    try:
        attacher = Pool(
            description=desc,
            slot_count=1,
            slot_size_bytes=64,
            is_owner=False,
        )
        try:
            with pytest.raises(TesseraPoolError, match="owner"):
                attacher.acquire(timeout_seconds=0.05)
            with pytest.raises(TesseraPoolError, match="owner"):
                attacher.reclaim_stale()
        finally:
            del attacher
    finally:
        del owner


# ---------------------------------------------------------------- context mgr


def test_context_manager_releases_on_exit():
    """`__exit__` deterministically closes the Pool (drops the RustPool,
    unlinks the SHM region). After the `with` block exits, a fresh
    Pool with the same description can be created without any GC dance.
    """
    desc = _unique_description("ctxmgr-recycle")
    with Pool(description=desc, slot_count=1, slot_size_bytes=64, ttl_seconds=10.0) as p1:
        lease = p1.acquire(timeout_seconds=1.0)
        p1.release(lease)
        assert p1.is_closed is False
    # After __exit__: deterministic close, no gc.collect() needed.
    assert p1.is_closed is True

    with Pool(description=desc, slot_count=1, slot_size_bytes=64, ttl_seconds=10.0) as p2:
        assert p2.is_owner is True


@pytest.mark.parametrize("bad_value", [float("inf"), float("-inf"), float("nan")])
def test_acquire_rejects_nonfinite_timeout(bad_value):
    """Codex iter-4 P2 regression: Duration::from_secs_f64 panics on
    inf/NaN. Validate before conversion."""
    desc = _unique_description("nonfinite-timeout")
    with Pool(description=desc, slot_count=1, slot_size_bytes=64, ttl_seconds=10.0) as pool:
        with pytest.raises(TesseraPoolError, match="finite|>= 0"):
            pool.acquire(timeout_seconds=bad_value)


def test_acquire_rejects_unreasonably_large_timeout():
    """200 years in seconds — almost certainly a unit-conversion bug
    (caller meant micros). Reject explicitly so the error message
    points at the actual mistake instead of panicking later."""
    desc = _unique_description("huge-timeout")
    with Pool(description=desc, slot_count=1, slot_size_bytes=64, ttl_seconds=10.0) as pool:
        with pytest.raises(TesseraPoolError, match="unreasonably large|unit conversion"):
            pool.acquire(timeout_seconds=200 * 365.25 * 86400)


@pytest.mark.parametrize("bad_value", [float("inf"), float("-inf"), float("nan"), 0.0, -1.0])
def test_pool_new_rejects_invalid_ttl(bad_value):
    """Codex iter-4 P2 generalized: ttl_seconds must be finite + > 0."""
    with pytest.raises(TesseraPoolError, match="finite|> 0"):
        Pool(
            description=_unique_description(f"bad-ttl-{bad_value}"),
            slot_count=1,
            slot_size_bytes=64,
            ttl_seconds=bad_value,
        )


def test_operations_on_closed_pool_raise():
    """All Pool operations except `close()` and the `is_closed` getter
    must raise `TesseraPoolError("Pool is closed")` after close.
    `close()` itself must be idempotent."""
    desc = _unique_description("closed-ops")
    pool = Pool(description=desc, slot_count=1, slot_size_bytes=64, ttl_seconds=10.0)
    pool.close()
    assert pool.is_closed is True

    with pytest.raises(TesseraPoolError, match="closed"):
        pool.acquire(timeout_seconds=0.05)
    with pytest.raises(TesseraPoolError, match="closed"):
        pool.in_use_count()
    with pytest.raises(TesseraPoolError, match="closed"):
        pool.reclaim_stale()

    # close() is idempotent.
    pool.close()
    pool.close()


def test_concurrent_create_with_live_owner_refuses_to_clobber():
    """Codex P1-1 regression: a second `Pool(is_owner=True)` against
    the same description as a live first owner must error rather than
    silently unlinking the live segment."""
    desc = _unique_description("concurrent-create")
    p1 = Pool(description=desc, slot_count=2, slot_size_bytes=64, ttl_seconds=10.0)
    try:
        with pytest.raises(TesseraPoolError, match="already exists|Refusing to clobber"):
            Pool(description=desc, slot_count=2, slot_size_bytes=64, ttl_seconds=10.0)
        # The first owner is still alive and usable.
        lease = p1.acquire(timeout_seconds=1.0)
        p1.release(lease)
    finally:
        del p1


def test_read_payload_rejects_tampered_descriptor_size():
    """Codex P1-2 regression: read_payload must validate
    descriptor.size_bytes against the slot's stored payload_len
    (and capacity) before copying."""
    from tessera_pool import _descriptor_from_bytes

    desc = _unique_description("size-mismatch")
    with Pool(description=desc, slot_count=1, slot_size_bytes=1024, ttl_seconds=10.0) as pool:
        lease = pool.acquire(timeout_seconds=1.0)
        descriptor = pool.write(lease, b"hello")

        # Tamper: reconstruct a Descriptor with the same identity but
        # an inflated size_bytes (claim 999 bytes when only 5 were written).
        # Uses the same pickle factory the binding exposes.
        # First, get the lease_id bytes back from the descriptor.
        lease_id_bytes = bytes.fromhex(descriptor.lease_id_hex)
        tampered = _descriptor_from_bytes(
            descriptor.slot_index,
            descriptor.generation,
            lease_id_bytes,
            999,  # over-stated size
        )
        with pytest.raises(TesseraPoolError, match="does not match|payload_len|exceeds"):
            pool.read_payload(tampered)

        # The legitimate descriptor still works.
        bytes_back = pool.read_payload(descriptor)
        assert bytes_back == b"hello"

        pool.release(lease)
