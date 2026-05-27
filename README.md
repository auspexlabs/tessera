# Tessera

Tessera is a Rust workspace with thin Python facades for shared-memory IPC:

- **Pool**: fixed-size shared-memory slots for large payload handoff.
- **Ring**: lossy mmap-backed broadcast for telemetry-shaped streams.
- **Channel**: non-lossy MPSC shared-memory queue for control and ack planes.
- **Sink**: atomic disk-write worker pool composed from Pool and Channel.

> **Status: pre-v0.1.** The Rust cores, PyO3 facades, examples, and Sink worker
> binary are implemented, but Tessera is not published yet. The v0.1.0 release
> is gated on re-importing these packages into Certus and validating them in the
> production pipeline. Until that gate passes, expect API and contract churn.

A *tessera* is a small tile in a mosaic. In this library, the "tiles" are
shared-memory slots and descriptors: producers put bytes in a region, pass a
small token to another process, and keep lifecycle ownership explicit.

## Why This Exists

Python multiprocessing works well for small messages. It gets expensive when
the thing crossing the process boundary is a 100 MB Arrow batch, a telemetry
stream that several consumers need to observe independently, or a disk-write
job that should be queued without forcing the hot path to block on I/O.

Tessera is not a general IPC framework. It provides a small set of opinionated
byte-level primitives:

- one owner creates a shared-memory region;
- peers attach by a BLAKE3-derived description;
- payloads are caller-owned bytes, not library-chosen serialization formats;
- lifecycle and crash-recovery choices are explicit.

## Components

| Crate | Python module | Lossy? | Shape |
|---|---|---:|---|
| [`tessera-pool`](crates/tessera-pool/) | `tessera_pool` | No | Lease-backed fixed slots. Owner acquires/writes/releases; attachers read by descriptor. |
| [`tessera-ring`](crates/tessera-ring/) | `tessera_ring` | Yes | Multi-writer, multi-reader broadcast. Per-section writer positions, per-reader local cursors, gap accounting. |
| [`tessera-channel`](crates/tessera-channel/) | `tessera_channel` | No | Multi-producer, single-consumer FIFO queue with blocking, try, and timeout send/recv modes. |
| [`tessera-sink`](crates/tessera-sink/) | `tessera_sink` | No | Worker-subprocess disk writer with chunking, BLAKE3 integrity, and atomic temp-file rename. |

Each Rust crate is usable directly from Rust. Each Python package in
[`python/`](python/) exposes the same core capability through PyO3.

## Current Concurrency Contract

The current Python facades are `#[pyclass(unsendable)]`, so Python objects are
thread-affine and PyO3 raises if a facade instance is touched from a different
OS thread. Cross-process use is supported and covered by examples; cross-thread
Python use is under active design before v0.1.

The intended contract is tracked in
[`docs/issue_facade_thread_safety.md`](docs/issue_facade_thread_safety.md):
separate `Send` (move a handle between threads) from `Sync` (concurrent calls
on the same handle), make soundness claims at the Rust-core protocol boundary,
and ensure blocking Python methods release the GIL without holding lifecycle
locks that other threads need for progress.

## Quick Start

The examples below match the current Python facades.

```python
from tessera_pool import Pool
from tessera_ring import Ring
from tessera_channel import Channel
from tessera_sink import Sink

payload_bytes = b"payload"

# Pool: owner writes bytes, another process can attach and read by Descriptor.
with Pool(
    description="my-app/batches",
    slot_count=8,
    slot_size_bytes=64 * 1024 * 1024,
    ttl_seconds=60.0,
) as pool:
    lease = pool.acquire(timeout_seconds=1.0)
    descriptor = pool.write(lease, payload_bytes)
    read_back = pool.read_payload(descriptor)
    pool.release(lease)

# Ring: publish through a Writer; each Reader has its own cursor.
with Ring(
    description="my-app/telemetry",
    sections=[(0, 4096, 2048)],
) as ring:
    writer = ring.writer()
    reader = ring.reader(0)
    writer.publish(0, b"event")
    for event in reader.poll():
        print(event.position, event.payload)

# Channel: receiver creates the region; senders attach to it.
with Channel(
    description="my-app/control",
    slot_count=256,
    slot_size_bytes=4096,
    role="receiver",
) as receiver:
    with Channel(
        description="my-app/control",
        slot_count=256,
        slot_size_bytes=4096,
        role="sender",
    ) as sender:
        sender.send(b"hello")
    assert receiver.recv() == b"hello"

# Sink: queue atomic file writes to worker subprocesses.
with Sink(
    description="my-app/artifacts",
    worker_count=4,
    pool_slot_count=8,
    pool_slot_size_bytes=64 * 1024 * 1024,
) as sink:
    sink.submit("/path/to/output.bin", payload_bytes, fsync=True)
    sink.flush()
```

For complete runnable demos, see [`examples/`](examples/).

## Design Highlights

- **BLAKE3-derived names.** Peers using the same human-readable description
  derive the same internal shared-memory name.
- **Refuse-to-clobber lifecycle.** Region creators fail if a name already
  exists unless `force_recreate=true` is explicitly set as an operator recovery
  action.
- **Owner-held leases.** Pool and Sink keep lease release authority with the
  owner, so workers cannot accidentally free or recycle slots they only read.
- **Bytes-only boundary.** v0.1 surfaces accept and return bytes. Callers choose
  Arrow, pickle, bincode, JSON, or any other serialization above Tessera.
- **Explicit topology.** Pool is large-payload handoff, Ring is lossy broadcast,
  Channel is reliable single-consumer queue, and Sink is a composite service.

## Release Readiness

| Area | Status |
|---|---|
| Pool Rust core + PyO3 facade | implemented |
| Ring Rust core + PyO3 facade | implemented |
| Channel Rust core + PyO3 facade | implemented |
| Sink Rust core + PyO3 facade + `tessera-sink-worker` | implemented |
| Certus re-import and production validation | next gate |
| crates.io / PyPI release | deferred until the Certus gate passes |

## Workspace Layout

```text
tessera/
├── Cargo.toml
├── README.md
├── crates/
│   ├── tessera-pool/
│   ├── tessera-ring/
│   ├── tessera-channel/
│   ├── tessera-sink/
│   └── tessera-sink-worker/
├── python/
│   ├── py-tessera-pool/
│   ├── py-tessera-ring/
│   ├── py-tessera-channel/
│   └── py-tessera-sink/
├── examples/
└── docs/
```

PyPI distribution names are hyphenated (`tessera-pool`); Python import modules
are underscored (`tessera_pool`).

## Building Locally

Rust cores:

```sh
cargo check --workspace
cargo test --workspace
```

Python facades, from an activated Python 3.10+ environment:

```sh
pip install maturin
cd python/py-tessera-pool      # or py-tessera-ring / py-tessera-channel / py-tessera-sink
maturin develop --release
```

Sink examples also need the worker executable:

```sh
cargo build -p tessera-sink-worker
```

## Documentation Map

- [`docs/concept_landscape.md`](docs/concept_landscape.md): primitive/service
  taxonomy and non-goals.
- [`docs/issue_facade_thread_safety.md`](docs/issue_facade_thread_safety.md):
  active thread-safety design issue before v0.1.
- Component READMEs under [`crates/`](crates/) and [`python/`](python/) describe
  Rust and Python surfaces separately.

## Licensing

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your
option.

## Acknowledgments

Tessera was extracted from the Certus multi-process and telemetry tooling. The
single-owner lifecycle, lease-generation validation, owner-held Sink leases,
caller-supplied Ring sections, and parity-test approach all came from that
production extraction path.
