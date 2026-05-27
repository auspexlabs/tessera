"""End-to-end tests for tessera_ring's PyO3 facade.

Each test constructs its own Ring with a uniquely-keyed description
so parallel test execution doesn't collide on the same SHM region.
The SHM region is unlinked when the Ring's Rust ``Drop`` fires
(i.e. when the Python object is garbage-collected at scope exit, or
explicitly via ``close()`` / ``__exit__``).

Run with: ``pytest python/py-tessera-ring/tests/`` from the workspace
root, after ``maturin develop`` has installed the wheel into the
active venv.
"""

from __future__ import annotations

import os
import pickle
import uuid

import pytest

from tessera_ring import Event, Reader, ReaderStats, Ring, TesseraRingError, Writer


def _unique_description(tag: str) -> str:
    """A description string unique to this test invocation.

    Combines the test tag + PID + a fresh uuid4 so parallel test runs
    (xdist, repeats, CI sharding) don't collide on the underlying SHM
    segment name.
    """
    return f"tessera-ring-test/{tag}/{os.getpid()}/{uuid.uuid4().hex}"


# ---------------------------------------------------------------- construction


def test_ring_construct_and_metadata():
    desc = _unique_description("construct")
    with Ring(description=desc, sections=[(0, 8, 64)]) as ring:
        assert ring.is_owner is True
        assert ring.is_closed is False


def test_ring_construct_multiple_sections():
    desc = _unique_description("multi-section-construct")
    with Ring(description=desc, sections=[(0, 4, 64), (7, 8, 128)]) as ring:
        assert ring.is_owner is True
        # Both sections addressable.
        w = ring.writer()
        w.publish(0, b"to-zero")
        w.publish(7, b"to-seven")


@pytest.mark.parametrize(
    "sections",
    [
        [],  # empty section list rejected at Rust layer
        [(0, 0, 64)],  # zero slot count
        [(0, 4, 0)],  # zero slot size
        [(0, 4, 64), (0, 8, 128)],  # duplicate section_id
    ],
)
def test_ring_construct_rejects_invalid_sections(sections):
    desc = _unique_description("invalid-sections")
    with pytest.raises(TesseraRingError):
        Ring(description=desc, sections=sections)


def test_ring_construct_rejects_bad_tuple_shape():
    desc = _unique_description("bad-tuple")
    # 2-tuple instead of 3-tuple
    with pytest.raises(TesseraRingError, match="3-tuple"):
        Ring(description=desc, sections=[(0, 64)])


def test_ring_attach_to_existing_region():
    desc = _unique_description("attach")
    sections = [(0, 8, 32)]
    with Ring(description=desc, sections=sections, is_owner=True) as owner:
        owner.writer().publish(0, b"from owner before attach")
        # Open an attacher to the same description.
        with Ring(description=desc, sections=sections, is_owner=False) as attacher:
            assert attacher.is_owner is False
            # Fresh reader on the attacher side starts at the current
            # writer position — sees only NEW events, not historical.
            r = attacher.reader(0)
            assert len(r.poll()) == 0
            owner.writer().publish(0, b"after attach")
            events = r.poll()
            assert len(events) == 1
            assert events[0].payload == b"after attach"


# ---------------------------------------------------------------- publish / poll


def test_publish_then_poll_returns_event_payload():
    desc = _unique_description("publish-poll")
    with Ring(description=desc, sections=[(0, 8, 256)]) as ring:
        w = ring.writer()
        r = ring.reader(0)
        w.publish(0, b"hello tessera ring")
        events = r.poll()
        assert len(events) == 1
        e = events[0]
        assert isinstance(e, Event)
        assert e.section_id == 0
        assert e.position == 0
        assert e.payload == b"hello tessera ring"
        assert e.timestamp_nanos > 0


def test_multiple_publishes_arrive_in_order():
    desc = _unique_description("ordered")
    with Ring(description=desc, sections=[(0, 16, 32)]) as ring:
        w = ring.writer()
        r = ring.reader(0)
        for i in range(10):
            w.publish(0, f"event-{i}".encode())
        events = r.poll()
        assert len(events) == 10
        for i, e in enumerate(events):
            assert e.position == i
            assert e.payload == f"event-{i}".encode()
        assert r.dropped == 0


def test_reader_lapped_accounts_dropped_events():
    # Ring with 4 slots; publish 10 events; reader catches up to
    # oldest_available = 10 - 4 = 6, so 6 events were dropped.
    desc = _unique_description("lapped")
    with Ring(description=desc, sections=[(0, 4, 16)]) as ring:
        w = ring.writer()
        r = ring.reader(0)
        for i in range(10):
            w.publish(0, i.to_bytes(4, "little"))
        events = r.poll()
        assert len(events) == 4
        assert r.dropped == 6
        # The 4 delivered events should be positions 6, 7, 8, 9.
        for i, e in enumerate(events):
            assert e.position == 6 + i


def test_fresh_reader_starts_at_current_writer_position():
    # Fresh readers see only NEW events, not historical ring contents.
    desc = _unique_description("fresh-reader-now")
    with Ring(description=desc, sections=[(0, 8, 16)]) as ring:
        w = ring.writer()
        for i in range(3):
            w.publish(0, i.to_bytes(4, "little"))
        # Reader opens AFTER the pre-publishes.
        r = ring.reader(0)
        assert r.cursor == 3
        idle = r.poll()
        assert idle == []
        assert r.dropped == 0
        w.publish(0, b"new")
        events = r.poll()
        assert len(events) == 1
        assert events[0].position == 3
        assert events[0].payload == b"new"


def test_multiple_readers_each_see_full_stream():
    # Multi-reader broadcast: each reader maintains its own cursor.
    desc = _unique_description("broadcast")
    with Ring(description=desc, sections=[(0, 16, 16)]) as ring:
        w = ring.writer()
        r1 = ring.reader(0)
        r2 = ring.reader(0)
        for i in range(5):
            w.publish(0, i.to_bytes(4, "little"))
        e1 = r1.poll()
        e2 = r2.poll()
        assert len(e1) == 5
        assert len(e2) == 5
        for a, b in zip(e1, e2):
            assert a.position == b.position
            assert a.payload == b.payload


def test_multi_section_publish_is_isolated():
    desc = _unique_description("multi-section-publish")
    with Ring(description=desc, sections=[(0, 8, 16), (1, 8, 32)]) as ring:
        w = ring.writer()
        r0 = ring.reader(0)
        r1 = ring.reader(1)
        w.publish(0, b"a-section-0")
        w.publish(1, b"b-section-1")
        w.publish(0, b"c-section-0")
        e0 = r0.poll()
        e1 = r1.poll()
        assert len(e0) == 2
        assert len(e1) == 1
        assert [e.payload for e in e0] == [b"a-section-0", b"c-section-0"]
        assert e1[0].payload == b"b-section-1"
        # Per-section writer_position is independent.
        assert e0[0].position == 0
        assert e0[1].position == 1
        assert e1[0].position == 0


# ---------------------------------------------------------------- error paths


def test_publish_rejects_oversized_event():
    desc = _unique_description("oversized")
    with Ring(description=desc, sections=[(0, 4, 16)]) as ring:
        w = ring.writer()
        with pytest.raises(TesseraRingError, match="exceeds"):
            w.publish(0, b"x" * 17)


def test_publish_rejects_unknown_section():
    desc = _unique_description("unknown-section")
    with Ring(description=desc, sections=[(0, 4, 16)]) as ring:
        w = ring.writer()
        with pytest.raises(TesseraRingError, match="unknown"):
            w.publish(99, b"x")


def test_reader_rejects_unknown_section():
    desc = _unique_description("unknown-reader-section")
    with Ring(description=desc, sections=[(0, 4, 16)]) as ring:
        with pytest.raises(TesseraRingError, match="unknown"):
            ring.reader(99)


def test_closing_ring_invalidates_existing_writer_and_reader():
    # Codex P2 fix on PR #2 (`d467b14`): Ring.close() must invalidate
    # previously-issued Writer / Reader handles. Without the closed-flag
    # propagation, those handles kept cloned Arc<Region>s alive and
    # continued to function past close() — violating the documented
    # "deterministic close" semantic.
    desc = _unique_description("close-invalidates-children")
    ring = Ring(description=desc, sections=[(0, 4, 64)])
    writer = ring.writer()
    reader = ring.reader(0)
    # Pre-close: child handles work normally.
    writer.publish(0, b"before close")
    events = reader.poll()
    assert len(events) == 1
    assert events[0].payload == b"before close"

    ring.close()
    assert ring.is_closed

    # Post-close: all API operations on the child handles raise.
    with pytest.raises(TesseraRingError, match="closed"):
        writer.publish(0, b"after close")
    with pytest.raises(TesseraRingError, match="closed"):
        reader.poll()
    with pytest.raises(TesseraRingError, match="closed"):
        reader.stats()

    # Cursor / dropped getters remain readable so consumers can inspect
    # final state (per the close() docstring's explicit carve-out).
    _ = reader.cursor
    _ = reader.dropped


def test_operations_on_closed_ring_raise():
    desc = _unique_description("closed-ring")
    ring = Ring(description=desc, sections=[(0, 4, 16)])
    ring.close()
    assert ring.is_closed is True
    with pytest.raises(TesseraRingError, match="closed"):
        ring.writer()
    with pytest.raises(TesseraRingError, match="closed"):
        ring.reader(0)
    # Idempotent.
    ring.close()


def test_context_manager_closes_on_exit():
    desc = _unique_description("ctx-exit")
    ring = Ring(description=desc, sections=[(0, 4, 16)])
    with ring as inside:
        assert inside.is_closed is False
    assert ring.is_closed is True


# ---------------------------------------------------------------- stats


def test_reader_stats_reports_cursor_latest_dropped():
    desc = _unique_description("stats")
    with Ring(description=desc, sections=[(0, 4, 16)]) as ring:
        w = ring.writer()
        r = ring.reader(0)
        for i in range(6):
            w.publish(0, i.to_bytes(4, "little"))
        _ = r.poll()
        stats = r.stats()
        assert isinstance(stats, ReaderStats)
        assert stats.section_id == 0
        assert stats.latest == 6
        assert stats.cursor == 6
        assert stats.dropped == 2  # 6 events, 4-slot ring → 2 dropped


def test_idle_poll_is_cheap_and_empty():
    desc = _unique_description("idle-poll")
    with Ring(description=desc, sections=[(0, 4, 16)]) as ring:
        r = ring.reader(0)
        for _ in range(5):
            assert r.poll() == []


# ---------------------------------------------------------------- pickle


def test_event_pickle_roundtrips():
    desc = _unique_description("event-pickle")
    with Ring(description=desc, sections=[(0, 4, 64)]) as ring:
        w = ring.writer()
        r = ring.reader(0)
        w.publish(0, b"picklable event")
        events = r.poll()
        assert len(events) == 1
        e = events[0]
        blob = pickle.dumps(e)
        restored = pickle.loads(blob)
        assert restored.section_id == e.section_id
        assert restored.position == e.position
        assert restored.timestamp_nanos == e.timestamp_nanos
        assert restored.payload == e.payload


# ---------------------------------------------------------------- force_recreate


def test_force_recreate_clobbers_existing_region():
    desc = _unique_description("force-recreate")
    sections = [(0, 4, 64)]
    first = Ring(description=desc, sections=sections, is_owner=True)
    # Without force_recreate, a second owner-side create on the same
    # description refuses.
    with pytest.raises(TesseraRingError, match="already exists"):
        Ring(description=desc, sections=sections, is_owner=True)
    # With force_recreate it succeeds.
    second = Ring(
        description=desc,
        sections=sections,
        is_owner=True,
        force_recreate=True,
    )
    second.close()
    first.close()


# ---------------------------------------------------------------- types


def test_event_repr_includes_fields():
    desc = _unique_description("event-repr")
    with Ring(description=desc, sections=[(0, 4, 64)]) as ring:
        # Reader before publish: fresh readers start at the current
        # writer position, so the order matters.
        r = ring.reader(0)
        ring.writer().publish(0, b"abc")
        e = r.poll()[0]
        repr_str = repr(e)
        assert "Event" in repr_str
        assert "position=" in repr_str
        assert "section_id=" in repr_str


def test_reader_repr_includes_state():
    desc = _unique_description("reader-repr")
    with Ring(description=desc, sections=[(0, 4, 64)]) as ring:
        r = ring.reader(0)
        repr_str = repr(r)
        assert "Reader" in repr_str
        assert "section_id=" in repr_str


def test_writer_publish_returns_none():
    desc = _unique_description("publish-returns-none")
    with Ring(description=desc, sections=[(0, 4, 64)]) as ring:
        w = ring.writer()
        result = w.publish(0, b"x")
        assert result is None
