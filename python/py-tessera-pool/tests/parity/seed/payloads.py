"""Deterministic byte payloads for the Tessera Pool parity baseline.

Drives ``Pool.acquire`` → ``Pool.write`` → ``Pool.read_payload`` with a
fixed set of payloads at edge sizes. Each payload is constructed
deterministically from a seed string + a known length so the bytes are
byte-identical across runs without any random-state dependency.

This is the *primitive-level* baseline. Downstream consumers (e.g. the
Certus ``ParquetWorkerPool`` that sits on top of Pool) have their own
parity tests in the consumer repo with a different seed shape (Arrow
records → Parquet). Both layers pinning their own canonical hash is
the equivalence gate for the eventual in-tree → tessera swap.

Edge sizes:
  - 1 byte                  (smallest legal payload)
  - 256 bytes               (single cache line / small)
  - 4 KiB                   (page-aligned medium)
  - 16 KiB                  (exact slot-size match — fills the slot)
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class PayloadSpec:
    """One deterministic Pool write submission."""

    name: str       # stable identifier used as the snapshot key
    payload: bytes


def _seeded_bytes(seed: str, length: int) -> bytes:
    """Construct a deterministic byte string of exactly ``length`` bytes.

    Repeats ``seed`` (encoded UTF-8) and truncates to ``length``. Pure
    function of (seed, length) — identical across runs and platforms.
    """
    if length == 0:
        return b""
    seed_bytes = seed.encode("utf-8")
    n_repeats = (length // len(seed_bytes)) + 1
    return (seed_bytes * n_repeats)[:length]


PAYLOADS: tuple[PayloadSpec, ...] = (
    PayloadSpec(name="alpha_1B",      payload=_seeded_bytes("alpha", 1)),
    PayloadSpec(name="bravo_256B",    payload=_seeded_bytes("bravo", 256)),
    PayloadSpec(name="charlie_4KiB",  payload=_seeded_bytes("charlie-medium-payload-marker-", 4 * 1024)),
    PayloadSpec(name="delta_16KiB",   payload=_seeded_bytes("delta-large-payload-block-", 16 * 1024)),
)


# Pool configuration — locked. Changing any of these would invalidate
# the snapshot and require a documented re-baseline.
POOL_CONFIG = {
    "slot_count": 4,
    "slot_size_bytes": 16 * 1024,   # tightest size that still fits the largest payload
    "ttl_seconds": 30.0,
}
