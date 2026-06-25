"""End-to-end tests for tessera_slate's PyO3 facade.

Each test constructs its own Slate with a uniquely-keyed description
so parallel test execution doesn't collide on the same SHM region.
The SHM region is unlinked when the Slate's Rust ``Drop`` fires
(i.e. when the Python object is garbage-collected at scope exit, or
explicitly via ``close()`` / ``__exit__``).

Run with: ``pytest python/py-tessera-slate/tests/`` from the workspace
root, after ``maturin develop`` has installed the wheel into the
active venv.
"""

from __future__ import annotations

import os
import pickle
import uuid

import pytest

from tessera_slate import Header, Slate, SlateReader, SlotRead, TesseraSlateError


def _unique_description(tag: str) -> str:
    """A description string unique to this test invocation.

    Combines the test tag + PID + a fresh uuid4 so parallel test runs
    (xdist, repeats, CI sharding) don't collide on the underlying SHM
    segment name.
    """
    return f"tessera-slate-test/{tag}/{os.getpid()}/{uuid.uuid4().hex}"


# ---------------------------------------------------------------- construction


def test_slate_construct_and_metadata():
    desc = _unique_description("construct")
    with Slate(description=desc, slot_count=8, slot_size_bytes=64) as slate:
        assert slate.is_owner is True
        assert slate.is_closed is False
        assert slate.slot_count == 8
        assert slate.slot_size_bytes == 64


def test_slate_construct_positional():
    desc = _unique_description("positional")
    with Slate(desc, 4, 32) as slate:
        assert slate.is_owner is True
        assert slate.slot_count == 4
        assert slate.slot_size_bytes == 32


# ---------------------------------------------------------------- write / read


def test_write_then_read_round_trip():
    desc = _unique_description("roundtrip")
    with Slate(description=desc, slot_count=4, slot_size_bytes=64) as slate:
        slate.write_slot(1, b"hello slate")
        reader = slate.reader()
        read = reader.read_slot(1)
        assert isinstance(read, SlotRead)
        assert read.state == "slot"
        assert read.is_slot is True
        assert read.value == b"hello slate"
        # First write to a slot publishes seqlock value 2 (0 -> 1 -> 2).
        assert read.sequence == 2
        assert read.timestamp_nanos > 0


def test_unwritten_slot_reads_empty():
    desc = _unique_description("empty")
    with Slate(description=desc, slot_count=4, slot_size_bytes=64) as slate:
        reader = slate.reader()
        read = reader.read_slot(0)
        assert read.state == "empty"
        assert read.is_empty is True
        assert read.is_slot is False
        assert read.value is None
        assert read.sequence == 0
        assert read.timestamp_nanos == 0


def test_overwrite_returns_latest_value():
    desc = _unique_description("overwrite")
    with Slate(description=desc, slot_count=2, slot_size_bytes=64) as slate:
        slate.write_slot(0, b"first")
        slate.write_slot(0, b"second value")
        read = slate.reader().read_slot(0)
        assert read.state == "slot"
        assert read.value == b"second value"
        # Two writes to the same slot: seqlock advances by 2 each write.
        assert read.sequence == 4


def test_shorter_rewrite_leaves_no_stale_tail():
    desc = _unique_description("lengths")
    with Slate(description=desc, slot_count=1, slot_size_bytes=64) as slate:
        reader = slate.reader()
        slate.write_slot(0, b"running-long-value")
        slate.write_slot(0, b"ok")
        read = reader.read_slot(0)
        assert read.state == "slot"
        assert read.value == b"ok"


def test_write_slot_returns_none():
    desc = _unique_description("write-returns-none")
    with Slate(description=desc, slot_count=2, slot_size_bytes=64) as slate:
        result = slate.write_slot(0, b"x")
        assert result is None


# ---------------------------------------------------------------- error paths


def test_write_rejects_oversized_payload():
    desc = _unique_description("oversized")
    with Slate(description=desc, slot_count=1, slot_size_bytes=8) as slate:
        with pytest.raises(TesseraSlateError, match="exceeds"):
            slate.write_slot(0, b"this is way too long")
        # The seqlock was never taken: the slot still reads empty.
        assert slate.reader().read_slot(0).state == "empty"


def test_write_rejects_out_of_range_index():
    desc = _unique_description("oob-write")
    with Slate(description=desc, slot_count=2, slot_size_bytes=8) as slate:
        with pytest.raises(TesseraSlateError, match="out of range"):
            slate.write_slot(2, b"x")


def test_read_rejects_out_of_range_index():
    desc = _unique_description("oob-read")
    with Slate(description=desc, slot_count=2, slot_size_bytes=8) as slate:
        reader = slate.reader()
        with pytest.raises(TesseraSlateError, match="out of range"):
            reader.read_slot(9)


def test_attach_rejects_geometry_mismatch():
    desc = _unique_description("geometry")
    with Slate(description=desc, slot_count=8, slot_size_bytes=64) as slate:
        # Reader claiming a smaller slot_count than the creator: bounds
        # check passes, semantic geometry check fires.
        with pytest.raises(TesseraSlateError, match="geometry mismatch"):
            SlateReader(desc, 4, 64)


def test_attach_rejects_schema_hash_mismatch():
    desc = _unique_description("schemahash")
    with Slate(
        description=desc, slot_count=4, slot_size_bytes=64, schema_hash=0xABCD
    ) as slate:
        # Same hash attaches fine.
        with SlateReader(desc, 4, 64, schema_hash=0xABCD) as ok:
            assert ok.slot_count == 4
        # A drifted layout hash is refused.
        with pytest.raises(TesseraSlateError, match="schema hash mismatch"):
            SlateReader(desc, 4, 64, schema_hash=0x1234)


def test_refuses_to_clobber_without_force():
    desc = _unique_description("clobber")
    first = Slate(description=desc, slot_count=2, slot_size_bytes=8)
    try:
        # Without force_recreate, a second owner-side create refuses.
        with pytest.raises(TesseraSlateError):
            Slate(description=desc, slot_count=2, slot_size_bytes=8, is_owner=True)
        # With force_recreate it succeeds.
        forced = Slate(
            description=desc,
            slot_count=2,
            slot_size_bytes=8,
            is_owner=True,
            force_recreate=True,
        )
        forced.close()
    finally:
        first.close()


# ---------------------------------------------------------------- second writer


def test_second_writer_owns_its_own_slot():
    desc = _unique_description("twowriters")
    sections = dict(slot_count=4, slot_size_bytes=64)
    with Slate(description=desc, is_owner=True, **sections) as owner:
        # A second writer attaches (is_owner=False) and writes its own slot.
        with Slate(description=desc, is_owner=False, **sections) as worker:
            assert worker.is_owner is False
            owner.write_slot(0, b"owner")
            worker.write_slot(1, b"worker")

            reader = owner.reader()
            r0 = reader.read_slot(0)
            r1 = reader.read_slot(1)
            assert r0.state == "slot" and r0.value == b"owner"
            assert r1.state == "slot" and r1.value == b"worker"
            # writer_seq counts writes across all slots.
            assert reader.header().writer_seq == 2


# ---------------------------------------------------------------- header


def test_header_counts_writes():
    desc = _unique_description("header")
    with Slate(description=desc, slot_count=4, slot_size_bytes=64) as slate:
        reader = slate.reader()
        header = reader.header()
        assert isinstance(header, Header)
        assert header.writer_seq == 0
        assert header.last_update_ns == 0

        for i in range(6):
            slate.write_slot(i % 4, i.to_bytes(8, "little"))
        header = reader.header()
        assert header.writer_seq == 6
        assert header.last_update_ns > 0


# ---------------------------------------------------------------- reader attach


def test_external_reader_attaches_and_reads():
    desc = _unique_description("external-reader")
    with Slate(description=desc, slot_count=4, slot_size_bytes=64) as slate:
        slate.write_slot(2, b"snapshot")
        # A reader constructed directly (not via slate.reader()) attaches
        # to the same region.
        with SlateReader(desc, 4, 64) as reader:
            assert reader.slot_count == 4
            assert reader.slot_size_bytes == 64
            read = reader.read_slot(2)
            assert read.state == "slot"
            assert read.value == b"snapshot"


def test_reader_survives_writer_close():
    desc = _unique_description("reader-survives")
    slate = Slate(description=desc, slot_count=2, slot_size_bytes=64)
    slate.write_slot(0, b"persisted")
    reader = slate.reader()
    # Closing the writer drops its handle; the reader holds its own region
    # clone and stays usable.
    slate.close()
    assert slate.is_closed is True
    read = reader.read_slot(0)
    assert read.state == "slot"
    assert read.value == b"persisted"
    reader.close()


# ---------------------------------------------------------------- close paths


def test_operations_on_closed_slate_raise():
    desc = _unique_description("closed-slate")
    slate = Slate(description=desc, slot_count=2, slot_size_bytes=16)
    slate.close()
    assert slate.is_closed is True
    with pytest.raises(TesseraSlateError, match="closed"):
        slate.write_slot(0, b"x")
    with pytest.raises(TesseraSlateError, match="closed"):
        slate.reader()
    with pytest.raises(TesseraSlateError, match="closed"):
        slate.unlink()
    # Idempotent.
    slate.close()


def test_operations_on_closed_reader_raise():
    desc = _unique_description("closed-reader")
    with Slate(description=desc, slot_count=2, slot_size_bytes=16) as slate:
        reader = slate.reader()
        reader.close()
        assert reader.is_closed is True
        with pytest.raises(TesseraSlateError, match="closed"):
            reader.read_slot(0)
        with pytest.raises(TesseraSlateError, match="closed"):
            reader.header()
        # Idempotent.
        reader.close()


def test_context_manager_closes_on_exit():
    desc = _unique_description("ctx-exit")
    slate = Slate(description=desc, slot_count=2, slot_size_bytes=16)
    with slate as inside:
        assert inside.is_closed is False
    assert slate.is_closed is True


# ---------------------------------------------------------------- torn API


def test_torn_state_api_exists():
    # Forcing an actual torn read from Python is a race we don't try to
    # provoke; just assert the API surface for the "torn" outcome exists
    # and reads a clean value as not-torn.
    desc = _unique_description("torn-api")
    with Slate(description=desc, slot_count=1, slot_size_bytes=64) as slate:
        slate.write_slot(0, b"clean")
        read = slate.reader().read_slot(0)
        assert hasattr(read, "is_torn")
        assert read.is_torn is False
        assert read.state in ("slot", "empty", "torn")


# ---------------------------------------------------------------- pickle


def test_slot_read_pickle_roundtrips_slot():
    desc = _unique_description("slot-pickle")
    with Slate(description=desc, slot_count=2, slot_size_bytes=64) as slate:
        slate.write_slot(0, b"picklable slot")
        read = slate.reader().read_slot(0)
        assert read.state == "slot"
        restored = pickle.loads(pickle.dumps(read))
        assert restored.state == read.state
        assert restored.value == read.value
        assert restored.sequence == read.sequence
        assert restored.timestamp_nanos == read.timestamp_nanos


def test_slot_read_pickle_roundtrips_empty():
    desc = _unique_description("empty-pickle")
    with Slate(description=desc, slot_count=2, slot_size_bytes=64) as slate:
        read = slate.reader().read_slot(0)
        assert read.state == "empty"
        restored = pickle.loads(pickle.dumps(read))
        assert restored.state == "empty"
        assert restored.value is None
        assert restored.sequence == 0
        assert restored.timestamp_nanos == 0


# ---------------------------------------------------------------- types


def test_slot_read_repr_includes_fields():
    desc = _unique_description("slot-repr")
    with Slate(description=desc, slot_count=2, slot_size_bytes=64) as slate:
        slate.write_slot(0, b"abc")
        read = slate.reader().read_slot(0)
        repr_str = repr(read)
        assert "SlotRead" in repr_str
        assert "state=" in repr_str
        assert "sequence=" in repr_str


def test_header_repr_includes_fields():
    desc = _unique_description("header-repr")
    with Slate(description=desc, slot_count=2, slot_size_bytes=64) as slate:
        header = slate.reader().header()
        repr_str = repr(header)
        assert "Header" in repr_str
        assert "writer_seq=" in repr_str


def test_slate_repr_includes_state():
    desc = _unique_description("slate-repr")
    with Slate(description=desc, slot_count=2, slot_size_bytes=64) as slate:
        repr_str = repr(slate)
        assert "Slate" in repr_str
        assert "is_owner=" in repr_str
        assert "slot_count=" in repr_str


def test_reader_repr_includes_state():
    desc = _unique_description("reader-repr")
    with Slate(description=desc, slot_count=2, slot_size_bytes=64) as slate:
        reader = slate.reader()
        repr_str = repr(reader)
        assert "SlateReader" in repr_str
        assert "slot_count=" in repr_str
