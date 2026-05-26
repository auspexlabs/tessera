# Tessera

> Open-source multi-process primitives for Python and Rust:
> **Pool** (lease-backed shared-memory slots), **Ring** (lossy mmap-backed broadcast),
> **Channel** (non-lossy MPSC queue), and **Sink** (atomic-write worker pool
> composed over Pool + Channel).

> **Status (pre-v0.1):** Tessera is being extracted from a set of
> multi-process tools we've been running in production. All four
> components have now landed — the three primitives (Pool, Ring,
> Channel) and the Sink composite service over them — and we'll remove
> this banner once everything is integrated back into our own
> environment and validated end-to-end. Until then, expect rapid
> iteration and occasional API churn.

A *tessera* is the small tile that fills a slot in a mosaic. Each component
in this library hands out tesserae — typed slot-tokens — backed by shared
memory or memory-mapped regions. The result: producer and worker
processes hand large payloads back and forth without copying through
serialization layers, and external containers can join the same SHM
region via shared IPC namespaces.

---

## Why another shared-memory library?

Several libraries already cover parts of this space well:

- **Python stdlib [`multiprocessing.shared_memory`](https://docs.python.org/3/library/multiprocessing.shared_memory.html)** —
  a raw named byte buffer. No slot management, no lifecycle, no lease
  coordination; you build those yourself.
- **[`posix_ipc`](https://pypi.org/project/posix_ipc/), [`sysv_ipc`](https://pypi.org/project/sysv_ipc/)** —
  thin wrappers around the POSIX / SysV syscalls. Building blocks, not
  policy.
- **[iceoryx](https://iceoryx.io/) / [iceoryx2](https://github.com/eclipse-iceoryx/iceoryx2)** —
  lock-free zero-copy pub/sub aimed at HPC and robotics. Large, capable
  API surface; C++-first with Rust bindings.
- **Apache Arrow Plasma** — deprecated.
- **Aeron, ZeroMQ inproc, nng** — message-transport-oriented; a
  different model than slot-and-descriptor handoff.

Tessera is shaped differently in three ways:

1. **Three opinionated primitives, not a general framework.** Pool
   (transactional bytes), Ring (lossy event broadcast), Sink (atomic
   I/O over Pool). Each does one thing. They share design language —
   BLAKE3-derived region handles, descriptor handoff, owner-held leases
   — so composing them stays predictable.
2. **Extracted from a running ML system,** not designed in the
   abstract. The lifecycle decisions (single-owner regions, lease
   renewal under slow consumers, gap detection for late-attaching
   readers, refuse-to-clobber semantics on create) are answers to
   incidents that actually happened.
3. **Rust core, thin PyO3 facade.** Production code paths live in
   Rust; Python is the front-of-house. Type and lifetime safety come
   from the host language, not from contract docs.

We open-sourced it because the extracted shape was small, self-contained,
and seemed generally useful for anyone moving large payloads between
Python processes — train/eval loops, inference pipelines, telemetry
drainers, artifact stagers. If your problem is "I want to hand 100 MB of
bytes to a worker without re-pickling," this might fit. If you want
something more battle-tested for production today, iceoryx2 and Aeron
are excellent.

## Why

Most Python multiprocessing primitives optimize for small messages and
implicit pickling. That model breaks down when you want to:

- Hand 100 MB Arrow batches between producer and worker without two
  serialization round-trips.
- Broadcast a stream of telemetry events to several readers
  simultaneously, where each reader sees its own per-section
  drop-counter and the writers never block.
- Coordinate atomic disk writes across a worker pool that shares one SHM
  region as a zero-copy staging area.
- Do any of the above across a container boundary (`docker compose`
  `ipc:` namespaces) without rolling a custom IPC layer.

Tessera packages those three primitives — Pool, Ring, Sink — with thin
Python facades on top of Rust cores.

## Components

| Crate | PyPI | Lossy? | What it is |
|---|---|---|---|
| [`tessera-pool`](crates/tessera-pool/) | [`tessera-pool`](https://pypi.org/project/tessera-pool/) | No | Non-lossy lease-backed shared-memory pool. Fixed slots, single-owner lifecycle, single-writer-lease, timeout reclaim with slot-generation invalidation of stale handles. For transactional large payloads. |
| [`tessera-ring`](crates/tessera-ring/) | [`tessera-ring`](https://pypi.org/project/tessera-ring/) | Yes | Lossy mmap-backed multi-writer / multi-reader ring buffer. Per-section write cursors with seqlock counters, caller-supplied sections, per-reader local cursors with gap detection. For telemetry-shaped streams. |
| [`tessera-channel`](crates/tessera-channel/) | [`tessera-channel`](https://pypi.org/project/tessera-channel/) | No | Non-lossy MPSC shared-memory queue. Multiple senders (CAS-claim on `tail`), one receiver, FIFO ordering, blocking / try / timeout modes. For control / RPC / ack planes. |
| [`tessera-sink`](crates/tessera-sink/) | [`tessera-sink`](https://pypi.org/project/tessera-sink/) | No | Atomic-write worker pool to disk — a *composite service* over `tessera-pool` (zero-copy chunk handoff) and `tessera-channel` (control + ack planes). Owner-held leases with worker ack/cancel, chunked streaming with worker affinity, atomic temp+rename, BLAKE3 integrity. Worker subprocesses spawned via the `tessera-sink-worker` bin. |

Each Rust crate has a thin Python facade in [`python/`](python/) built
with [PyO3](https://pyo3.rs) / [maturin](https://www.maturin.rs).

## Design highlights

- **BLAKE3-derived namespace handles.** Two peers with the same
  human-readable description derive the same internal handle and attach
  to the same SHM region — no manual coordination of magic strings.
- **Owner-held leases.** Sink's lease-renewal model keeps lifecycle
  state with the producer, so slow worker subprocesses can't be
  reclaimed mid-read (avoids a classic SHM stale-handle race).
- **Section-aware Ring API.** Writers and readers index into named
  sections; the library never inspects payloads or runs a classifier
  on the caller's behalf.
- **Cross-container IPC as a first-class capability.** Pool's
  single-owner-multi-attacher design works inside one container today
  and across containers via shared `ipc:` namespaces. Lifecycle, lease
  coordination, crash recovery, and security posture are all explicit
  design dimensions, not bolted-on opaque modes.
- **Forward-compatible API.** v0.1 surfaces are bytes-only with strict
  explicit-release lifecycle, but the lease/descriptor shape leaves room
  for typed slot views (Arrow / NumPy zero-copy) and eviction-aware
  policies as additive capabilities.

## Status

**v0.0.1 — all components implemented; pre-v0.1 hardening.** The Rust
cores and PyO3 facades have landed per the staged plan:

| Stage | What | Status |
|---|---|---|
| 4a | Pool — Rust core + PyO3 facade | done |
| 4b | Ring — Rust core + PyO3 facade | done |
| 4c | Channel — Rust core + PyO3 facade | done |
| 4d | Sink — Rust core + PyO3 facade + `tessera-sink-worker` bin (composite over Pool + Channel) | done |
| 5 | Open-source posture pass (API isolation audit, README docs, examples) and v0.1.0 release to crates.io + PyPI | pending |

Track the plan in the upstream Certus repo:
`claudedocs/plans/mp_tools_open_source_extraction_2026-05-23.md` in
[Indubitable-Industries/Bayence-Certus](https://github.com/Indubitable-Industries/Bayence-Certus).

## Quick start (API preview; installable at v0.1.0)

```python
from tessera_pool import Pool
from tessera_ring import Ring
from tessera_channel import Channel
from tessera_sink import Sink

# Non-lossy SHM pool — transactional large payloads
with Pool(slot_count=8, slot_size_bytes=64 * 1024 * 1024, description="my-app/batches") as pool:
    lease = pool.acquire()
    descriptor = pool.write(lease, payload_bytes)
    # ... hand `descriptor` across IPC to a worker ...
    pool.release(lease)

# Lossy multi-reader ring — telemetry-shaped streams
with Ring(sections={"logs": {"slot_count": 4096, "slot_size_bytes": 2048}},
          description="my-app/telemetry") as ring:
    ring.publish("logs", event_bytes)
    for event in ring.reader("log-drainer", section="logs").poll():
        ...

# Non-lossy MPSC queue — control / RPC / ack planes
with Channel(slot_count=256, slot_size_bytes=4096,
             description="my-app/control", role="receiver") as chan:
    msg = chan.recv()

# Atomic-write worker pool — chunked Pool-backed disk writes
with Sink(description="my-app/artifacts", worker_count=4,
          pool_slot_count=8, pool_slot_size_bytes=64 * 1024 * 1024) as sink:
    sink.submit("/path/to/output.bin", payload_bytes, fsync=True)
    sink.flush()
```

## Workspace layout

```
tessera/
├── Cargo.toml                # workspace manifest
├── README.md                 # this file
├── LICENSE-MIT / LICENSE-APACHE
├── crates/                   # pure Rust cores (usable from Rust without Python)
│   ├── tessera-pool/
│   ├── tessera-ring/
│   ├── tessera-channel/
│   ├── tessera-sink/
│   └── tessera-sink-worker/  # worker executable spawned by tessera-sink
├── python/                   # PyO3 facades (one per primitive + Sink)
│   ├── py-tessera-pool/
│   ├── py-tessera-ring/
│   ├── py-tessera-channel/
│   └── py-tessera-sink/
├── examples/                 # cross-component demos (populated in Stage 4+)
└── .github/workflows/        # CI: cargo check / cargo test / maturin build
```

The PyPI distribution name is hyphenated (`tessera-pool`); the Python
import module is underscored (`tessera_pool`):

```python
# pyproject.toml:
tessera-pool = "^0.1.0"

# in code:
from tessera_pool import Pool
```

## Building locally

Rust workspace (no Python toolchain required for core crates):

```sh
cargo check --workspace
cargo test  --workspace
```

Python facades (requires Python ≥3.10 and [maturin](https://www.maturin.rs)):

```sh
pip install maturin
cd python/py-tessera-pool   # or py-tessera-ring / py-tessera-sink
maturin develop             # builds + installs into the active venv as editable
```

## Licensing

Dual-licensed under [MIT](LICENSE-MIT) **OR** [Apache-2.0](LICENSE-APACHE),
at your option. This matches the prevailing convention for open-source
Rust crates and is friendly to use in proprietary, GPL, and permissively
licensed downstream projects alike.

## Acknowledgments

Tessera was extracted from the [Certus](https://github.com/Indubitable-Industries/Bayence-Certus)
project's `certus/mp/` and `certus/telemetry/` packages. The architectural
decisions (single-owner lifecycle, single-writer-lease, owner-held lease
renewal for Sink, caller-supplied Ring sections, etc.) and the
verification framework all originated there.
