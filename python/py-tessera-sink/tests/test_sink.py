"""End-to-end tests for tessera_sink's PyO3 facade.

Each test starts its own Sink with a uniquely-keyed description so
parallel runs don't collide on a SHM region. The Sink spawns the real
``tessera-sink-worker`` subprocesses; we point it at the Cargo-built
debug binary via the ``worker_bin_path`` kwarg.

Run with: ``pytest python/py-tessera-sink/tests/`` from the workspace
root, after ``maturin develop`` has installed the wheel and
``cargo build -p tessera-sink-worker`` has built the worker.
"""

from __future__ import annotations

import os
import pathlib
import uuid

import pytest

from tessera_sink import Sink, TesseraSinkError

# tests/ -> py-tessera-sink/ -> python/ -> tessera repo root
_REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]
_WORKER_BIN = _REPO_ROOT / "target" / "debug" / "tessera-sink-worker"


def _unique_description(tag: str) -> str:
    return f"tessera-sink-test/{tag}/{os.getpid()}/{uuid.uuid4().hex}"


def _sink(
    tag: str,
    *,
    worker_count: int = 1,
    pool_slot_count: int = 4,
    pool_slot_size_bytes: int = 4096,
) -> Sink:
    return Sink(
        description=_unique_description(tag),
        worker_count=worker_count,
        pool_slot_count=pool_slot_count,
        pool_slot_size_bytes=pool_slot_size_bytes,
        worker_bin_path=str(_WORKER_BIN),
    )


@pytest.fixture(autouse=True)
def _require_worker_bin():
    if not _WORKER_BIN.exists():
        pytest.skip(
            f"worker binary not built at {_WORKER_BIN}; "
            "run `cargo build -p tessera-sink-worker`"
        )


def test_submit_single_file(tmp_path):
    target = tmp_path / "hello.bin"
    payload = b"hello tessera sink from python"
    with _sink("single") as sink:
        sink.submit(str(target), payload)
        sink.flush()
    assert target.read_bytes() == payload


def test_submit_multi_chunk(tmp_path):
    target = tmp_path / "multi.bin"
    payload = bytes((i % 251) for i in range(500))
    with _sink("multi", pool_slot_count=8, pool_slot_size_bytes=64) as sink:
        sink.submit(str(target), payload)
        sink.flush()
    assert target.read_bytes() == payload


def test_submit_multiple_files_across_workers(tmp_path):
    files = {}
    with _sink("many", worker_count=3, pool_slot_count=8) as sink:
        for i in range(9):
            target = tmp_path / f"file-{i}.bin"
            payload = f"file {i} payload".encode() * (i + 1)
            sink.submit(str(target), payload)
            files[target] = payload
        sink.flush()
    for target, payload in files.items():
        assert target.read_bytes() == payload


def test_empty_payload_writes_empty_file(tmp_path):
    target = tmp_path / "empty.bin"
    with _sink("empty") as sink:
        sink.submit(str(target), b"")
        sink.flush()
    assert target.exists()
    assert target.read_bytes() == b""


def test_fsync_path(tmp_path):
    target = tmp_path / "durable.bin"
    payload = b"durable via fsync"
    with _sink("fsync") as sink:
        sink.submit(str(target), payload, fsync=True)
        sink.flush()
    assert target.read_bytes() == payload


def test_submit_returns_job_id(tmp_path):
    target = tmp_path / "job.bin"
    with _sink("jobid") as sink:
        job_id = sink.submit(str(target), b"abc")
        assert isinstance(job_id, int)
        assert job_id >= 0
        sink.flush()


def test_context_manager_closes():
    sink = _sink("ctx")
    assert sink.is_closed is False
    with sink:
        assert sink.worker_count == 1
    assert sink.is_closed is True


def test_submit_after_close_raises(tmp_path):
    sink = _sink("closed")
    sink.close()
    with pytest.raises(TesseraSinkError):
        sink.submit(str(tmp_path / "x.bin"), b"data")


def test_no_temp_files_left_behind(tmp_path):
    target = tmp_path / "clean.bin"
    payload = bytes(range(256)) * 4
    with _sink("clean", worker_count=2, pool_slot_count=4, pool_slot_size_bytes=256) as sink:
        sink.submit(str(target), payload)
        sink.flush()
    entries = sorted(p.name for p in tmp_path.iterdir())
    assert entries == ["clean.bin"], f"stray files: {entries}"


def test_repr_mentions_description():
    with _sink("repr") as sink:
        r = repr(sink)
        assert "Sink(" in r
        assert "worker_count=1" in r
