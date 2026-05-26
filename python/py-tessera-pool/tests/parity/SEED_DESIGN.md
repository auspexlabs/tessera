# Pool parity baseline — seed-data design

Deterministic byte-payload seed + a syrupy snapshot pinning Tessera
Pool's byte-faithful round-trip behavior over time.

Pattern mirrors `tests/unit/tessera_port_baseline/` in
[`Indubitable-Industries/Bayence-Certus`](https://github.com/Indubitable-Industries/Bayence-Certus):
small literal seed committed to git, syrupy JSON snapshot committed
under `__snapshots__/`, regenerate with `pytest --snapshot-update`.

## Files

| File | Role |
|---|---|
| `seed/payloads.py` | Deterministic byte payloads + Pool config. |
| `test_pool_parity.py` | Drives the seed through Pool, hashes output, asserts against snapshot. |
| `__snapshots__/test_pool_parity/test_pool_parity_baseline.json` | Committed baseline (generated on first run with `--snapshot-update`). |

## Determinism strategy

| Concern | Approach |
|---|---|
| Random state | Not relied on — every seed value is a literal. |
| Wall-clock | None in the snapshot. |
| SHM segment names | PID + uuid4 in the description for parallel isolation; description is **not** in the snapshot. |
| Lease IDs | Opaque internal handles; **not** in the snapshot. |
| Slot scheduling | Snapshot pins only `(name, size, sha256(read_back_bytes))`, sorted by `name`; lease-to-slot assignment order does not matter. |

## How to regenerate

```sh
pytest python/py-tessera-pool/tests/parity/ --snapshot-update
```

After regeneration, `git diff` shows exactly what changed. A non-empty
diff that wasn't intended indicates non-determinism that needs to be
controlled before accepting the new baseline.

## Relationship to the upstream Certus baseline

Certus's `tests/unit/test_tessera_pool_parity.py` pins the *consumer*
(`ParquetWorkerPool`) output (parquet files, Arrow-IPC-canonical hash).
The seed there is Arrow records.

This baseline pins the *primitive* (`tessera_pool.Pool`) output (bytes
round-trip). The seed here is raw bytes at edge sizes.

The two baselines test complementary contracts. When the Certus
consumer is swapped from its in-tree `SharedMemoryPool` to Tessera
Pool, the Certus parquet-content hash must remain identical — that
swap relies on the byte-faithfulness this baseline pins.
