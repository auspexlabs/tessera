"""Pool parity baseline — pins Tessera Pool's byte-faithful round-trip.

Drives ``Pool.acquire`` → ``Pool.write`` → ``Pool.read_payload`` over a
deterministic seed and snapshots:

  - per-payload (name, size, content sha256) of bytes read back
  - in_use_count() and slot_count after explicit release of all leases

The canonical reduction is sorted by payload ``name`` so test ordering
and any future reorderings of the seed tuple don't perturb the hash.

CONFIDENCE: HIGH. Pool is non-lossy and bytes-in == bytes-out is the
explicit contract; the test exists to lock that contract over time and
to give the downstream Certus parity test a peer baseline to compare
against once Stage 9 (shadow-consumption PR) lands.

How to (re-)generate the snapshot:

    pytest python/py-tessera-pool/tests/parity/ --snapshot-update

After re-generation, ``git diff`` should show exactly what changed.
A non-empty diff that wasn't intended indicates non-determinism that
needs to be controlled before accepting the new baseline.
"""

from __future__ import annotations

import hashlib
import os
import uuid

import pytest
from syrupy.extensions.json import JSONSnapshotExtension

from tessera_pool import Pool

from .seed.payloads import PAYLOADS, POOL_CONFIG


@pytest.fixture
def snapshot_json(snapshot):
    """syrupy fixture with a JSON extension so snapshots are diff-able."""
    return snapshot.use_extension(JSONSnapshotExtension)


def _unique_description() -> str:
    """Description string unique to this invocation.

    Combines a static tag + PID + a fresh uuid4 so parallel test runs
    (xdist, repeats, CI sharding) don't collide on the SHM segment name.
    The description is *not* part of the snapshot — only round-tripped
    bytes are.
    """
    return f"tessera-pool-parity/{os.getpid()}/{uuid.uuid4().hex}"


def _drive_pool_workload() -> dict:
    """Run the deterministic workload through Pool.

    For each seed payload: acquire a lease, write the payload, read it
    back via the returned descriptor, and record (name, size, sha256 of
    bytes read). Then release all leases and capture final counters.

    The reduction is sorted by ``name`` to produce a canonical form
    independent of seed-tuple ordering.
    """
    description = _unique_description()
    per_payload: list[dict] = []

    with Pool(description=description, **POOL_CONFIG) as pool:
        leases = []
        try:
            for spec in PAYLOADS:
                lease = pool.acquire(timeout_seconds=1.0)
                leases.append(lease)
                descriptor = pool.write(lease, spec.payload)
                read_back = pool.read_payload(descriptor)
                per_payload.append(
                    {
                        "name": spec.name,
                        "size": len(read_back),
                        "content_sha256": hashlib.sha256(read_back).hexdigest(),
                    }
                )

            in_use_after_writes = pool.in_use_count()
        finally:
            for lease in leases:
                pool.release(lease)

        in_use_after_release = pool.in_use_count()
        slot_count = pool.slot_count
        slot_size_bytes = pool.slot_size_bytes

    return {
        "pool_config": {
            "slot_count": slot_count,
            "slot_size_bytes": slot_size_bytes,
        },
        "in_use_after_writes": in_use_after_writes,
        "in_use_after_release": in_use_after_release,
        "payloads": sorted(per_payload, key=lambda r: r["name"]),
    }


def test_pool_parity_baseline(snapshot_json) -> None:
    """Capture deterministic byte-round-trip output of Tessera Pool.

    The snapshot pins:
      - per-payload (name, size, content sha256) of bytes read back via
        the descriptor — proves byte-faithful round-trip
      - slot-occupancy counters after writes (== len(PAYLOADS)) and
        after explicit release (== 0)
      - pool config echo (slot_count, slot_size_bytes) so a config drift
        produces a visible snapshot diff

    The snapshot does NOT pin: descriptions (carry PID/uuid for parallel
    isolation), lease IDs (opaque internal handles), or any timing /
    wall-clock data.
    """
    captured = _drive_pool_workload()
    assert captured == snapshot_json
