# Issue: Tessera's concurrency contract for multi-threaded host processes

**Status:** RESOLVED for v0.1 — implemented on branch `concurrency/send-sync-contract`
(PR #13). One residual item consciously deferred to v0.2; see "Resolution" below.
**Affects:** all four primitives' Python facades and their Rust cores.
**Date:** 2026-05-26

---

## Resolution (v0.1 decision)

Option C was implemented per the per-role table: `&self` + per-slot locks for
Pool, `recv_lock` for the Channel receiver, seqlock `Send + Sync` for Ring,
`Arc` facades + atomic `closed` flag + `py.allow_threads` for close/cancel,
Sink owner kept thread-affine.

**One item is deferred to v0.2, by decision:** a fully race-free payload copy
under *arbitrary* concurrent writer + reader on the **same slot, cross-process**
(i.e. a caller that violates the single-writer-lease protocol — reclaiming /
reusing + rewriting a slot in one process while another is mid-`read_payload`).
`read_payload` detects it via a generation re-check (returns `StaleHandle`), but
the unsynchronized cross-process `memcpy` itself is a data race that a
process-private lock cannot prevent.

Rationale for deferring:
- **Parity-plus.** This is exactly the exposure the prior in-tree
  `certus/mp/shared_memory_pool.py` had — with *less* protection (it had no
  generation check at all). Certus shipped on it for the whole project because
  its protocol (owner holds the lease until the worker acks; reclaim is
  crash-recovery only, for dead readers) means the race never occurs.
- The contract `read_payload` documents (single-writer-lease) forbids the
  triggering scenario, so v0.1 is sound *for the use Tessera is built for*.
- A genuinely race-free arbitrary-concurrent cross-process copy needs an
  **in-SHM robust per-slot lock** (atomic lock word in the shared region with
  crashed-holder recovery, à la `PTHREAD_MUTEX_ROBUST`). That's a real,
  recovery-laden chunk of work whose only beneficiary is a use-case outside
  Tessera's own — appropriate as a **v0.2 portability item**, disproportionate
  for v0.1.

### v0.2 roadmap

- **In-SHM robust per-slot lock for `Pool`** so `read_payload` is race-free
  under arbitrary concurrent writer/reader across processes (broadens the
  library beyond the single-writer-lease protocol). Tracked here.

---

> **Revision-history note:** the body below is the pre-resolution design write-up
> (v1–v4) kept as the rationale of record.

> **Revision history.**
> - v1: claimed a uniform fix ("facades serialize via a `parking_lot::Mutex`,
>   add `unsafe impl Send + Sync`"). Wrong — not uniform, and deadlocks.
> - v2: reframed around soundness + progress constraints and a per-primitive
>   contract.
> - v3: made the **Send-vs-Sync distinction explicit per role** — "move a
>   handle to another thread" (`Send`) ≠ "call the same handle concurrently"
>   (`Sync`). Added the Pool-core refactor prerequisite and the close/drop-race
>   design.
> - **v4 (this):** strengthens the Pool `Sync` requirement to cover the **read
>   path** (metadata validation + payload copy + `in_use_count` scan), not just
>   mutation; and adds **cancellation semantics** to the close/drop design (a
>   blocked op must be woken with a clean error, not hang to timeout).

## The core distinction (read this first)

Two independent properties, often conflated:

- **`Send` — move between threads.** Create a handle on thread A, hand it to
  thread B, only ever one thread touches it at a time. The `unsendable` panic
  we hit is purely a `Send` failure (a `ThreadId` check), nothing to do with
  concurrency.
- **`Sync` — concurrent calls on the *same* handle.** Two threads inside
  `&self` methods of the same object at once.

Crucial GIL nuance: while the GIL is held, Python-level calls are serialized,
so even a `Send`-not-`Sync` handle can be *called* from several Python threads
safely (only one is in Rust at a time). The moment a blocking op releases the
GIL (`py.allow_threads`, which Constraint 2 forces), concurrency becomes real
and `Sync` correctness must actually hold. So: **`Send` is the baseline ask for
portable primitive handles; `Sync` is an additional claim that's only true for
the genuinely concurrent protocols.** Sink owner is the explicit exception under
consideration because it is a composite, single-threaded orchestrator rather
than a byte-level primitive handle.

## Context

Each facade is `#[pyclass(unsendable)]`, so an instance is pinned to its
creating thread and PyO3 panics if touched from another:

```
PanicException: tessera_pool::PyPool is unsendable, but sent to another thread
```

The first consumer (Certus' `ParquetWorkerPool`) is an ordinary async service:
Pool **created** on the event-loop thread, **used** from a `run_in_executor`
thread, **stopped** from an `asyncio.to_thread` thread, leases **released** from
an internal collector thread. A drop-in swap panics. For owner/authority roles
there is only one instance, so "one handle per thread" is not available there.
The current test suites drive every object single-threaded, so none of this is
covered today.

## Two hard constraints

**Constraint 1 — Soundness.** `Send`/`Sync` are `unsafe` assertions justified by
each Rust core's protocol, per primitive:

- Ring **writer**: lock-free seqlock, N concurrent writers by design → `Sync`.
- Ring **reader**: `poll(&mut self)` plus per-reader local cursor + drop
  counters (`crates/tessera-ring/src/ring.rs:347`). *Independent* readers are
  concurrent with each other, but the **same** reader handle is **not** safe to
  `poll`/read-stats from two threads at once → `Send`, not concurrently `Sync`
  (today `PyReader` serializes it with a facade mutex).
- Channel **sender**: MPSC, CAS on `tail` → `Sync`.
- Channel **receiver**: single-receiver path loads `head`, copies the slot,
  clears `ready`, stores `head + 1` (`crates/tessera-channel/src/channel.rs:374`).
  Two concurrent `recv()` on the same receiver race — one can free a slot while
  the other is still copying, letting a sender reuse it → `Send`, **not**
  concurrently `Sync` (unless a receiver-side serialization lock is added).
- Pool **owner**: needs *genuinely* concurrent `acquire` (one thread) and
  `release` (another) → must be `Sync` — see the prerequisite below. Note this
  is not only a *mutation* concern: `in_use_count()` scans every slot's
  `SlotMeta` (`crates/tessera-pool/src/pool.rs:379`) and would run concurrently
  with `acquire`/`release` mutating that same metadata.
- Pool **attacher/reader**: `read_payload(&self)` *validates `SlotMeta` then
  copies the payload* (`crates/tessera-pool/src/pool.rs:257`) — it is **not** a
  trivially-`Sync` read. Today `Region::write_slot_meta` takes `&mut self`
  (`crates/tessera-pool/src/region.rs:325`), so the type system guarantees no
  in-process reader runs concurrently with a writer of the same slot. Removing
  the facade lock breaks that guarantee: a reader's validate-then-copy could
  race a writer's `SlotMeta` mutation / slot reclaim and tear. So `Sync` for the
  read path requires the **same internal protocol** as the write path (a
  slot-level lock, or moving `SlotMeta` to atomics/seqlock so the reader gets a
  consistent `(generation, payload)` snapshot and retries on a concurrent bump).
- Sink **owner**: single-threaded by design (cooperative ack-drain + lease
  renewal orchestrating subprocesses) → `Send` at best; realistically
  thread-affine.

Hard invariant (true today — keep it a documented rule): **no method returns or
retains a reference into the mmap region past the op.** Data methods return
owned copies; handle types carry plain data. This directly conflicts with the
roadmap's **zero-copy typed views** — a returned view borrows the region and
cannot be `Send` without a lifetime bound. If zero-copy views land, they are
inherently thread/lifetime-affine even when the handle is `Sync`.

**Constraint 2 — Progress.** A blocking op must not hold a lock (or the GIL)
that another thread needs to unblock it. `Pool.acquire()` blocks until a slot
frees; the slot frees only when `Pool.release()` runs on another thread. If the
facade holds its `Mutex` across `acquire` (today `with_inner_mut(|p|
p.acquire())` does — `python/py-tessera-pool/src/lib.rs:250`), `release` can
never take the lock → hang. Releasing the GIL is necessary but **not
sufficient**; the facade must also not hold a coarse lock across the wait.

## Per-role contract (proposed)

| Primitive · role | `Send` (move) | `Sync` (concurrent same handle) | Notes |
|---|---|---|---|
| Ring · writer | yes | **yes** | seqlock, multi-writer by design |
| Ring · reader | yes | **no** | independent readers concurrent; one reader handle = serialized `poll`/stats |
| Channel · sender | yes | **yes** | MPSC, CAS on `tail` |
| Channel · receiver | yes | **no** | single-receiver copy path races under concurrent `recv` |
| Pool · owner | yes | **yes** (after refactor) | needs concurrent acquire/release; see prerequisite |
| Pool · attacher | yes | **yes** (after read-path protocol) | API-read-only, but `read_payload` must snapshot metadata/payload consistently |
| Sink · owner | yes* | **no** | single-threaded by design; *or explicitly remains thread-affine |

For the **`Send`-not-`Sync`** rows (Ring reader, Channel receiver, Sink owner),
the handle may be moved across threads but concurrent calls must be prevented —
either by the host using it from one thread at a time, or by a documented
internal serialization lock on that handle.

## Option C (recommended) — thread-safe Rust cores, thin facades

Concrete work items, per the reviews:

1. **Pool core synchronization refactor (prerequisite).** The owner mutation
   path is still `&mut self` (`acquire` / `write` / `release` / `renew` /
   `reclaim_stale`, `crates/tessera-pool/src/pool.rs:164,207,294,320,339`),
   while the read path is already `&self` (`read_payload` `:256`,
   `in_use_count` `:377`). The Python facade still serializes both paths behind
   its `Mutex` (`python/py-tessera-pool/src/lib.rs:250,262`), which prevents
   in-process progress (`acquire` can block while holding the only lock
   `release` needs) and masks the fact that the Rust read path has no internal
   synchronization against owner metadata mutation.

   For Constraint 2, the mutation methods must move to **`&self` + internal
   synchronization**, and the existing `&self` read methods must use that same
   synchronization/snapshot protocol. It is not enough to remove the facade
   lock from `read_payload`: `Region::write_slot_meta` is `&mut self`
   (`region.rs:325`) today, and that exclusivity is exactly what gets lost once
   one Pool handle can be called from several host threads. Two viable shapes:
   - a **per-slot lock** held briefly for both meta-mutation and the
     validate-then-copy read (never across a wait), or
   - move `SlotMeta` to **atomics/seqlock** so reads snapshot
     `(generation, payload)` consistently and retry on a concurrent bump.
   The free-list is already a lock-free `SegQueue`. An `Arc`/in-flight-guard
   facade is an alternative, but `&self` core methods are the clean form.
2. **`unsafe impl` lives on the core handle** closest to the raw mmap owner
   (`Region` / `Writer` / core structs), with the soundness comment tied to
   that type's protocol — not as a blanket impl on the pyclass.
3. **Facade lock guards lifecycle/closed-state only**, never wraps a blocking
   op.
4. **Blocking ops** (`acquire`/`recv`/`send`/`flush`) release the GIL
   (`py.allow_threads`) and hold no lock across the wait.
5. **`Send`-not-`Sync` handles** get a documented one-caller rule or an explicit
   per-handle serialization lock (Ring reader, Channel receiver).

### Close/drop racing — a design requirement, not just a test

Once blocking ops no longer hold the lifecycle mutex, `close()` cannot simply
drop the inner object while another thread is mid-op. Required design, two
parts:

1. **No use-after-unmap.** Put the core and closed bit in shared facade state
   (for example `Arc<FacadeState { core: Arc<Core>, closed: AtomicBool }>`).
   An op clones the state/core before releasing lifecycle state; `close()` marks
   the handle closed and drops only its owner reference; the actual
   `munmap`/`shm_unlink` happens when the last core `Arc` reference drops
   (after in-flight ops finish).
2. **Cancellation (don't just prevent UAF — wake the blocked op).** The `Arc`
   scheme stops a crash but does not, by itself, unblock a thread parked in
   `recv()`, `send()` (full channel), or `acquire()` (full pool). Those wait
   loops must poll the shared **atomic `closed` flag** and return a clean
   `Closed`/`Cancelled` error promptly when `close()` sets it — otherwise a
   `close()` leaves peers blocking until their timeout (or forever, for the
   un-timed blocking variants). So: `close()` = set `closed` (Release) + drop
   owner `Arc` ref; every wait loop checks `closed` (Acquire) each iteration.

This is local-handle cancellation. It is separate from remote peer death across
processes, which would require an in-SHM closed/epoch/liveness signal and is not
part of the minimum fix for the PyO3 thread-affinity panic.

Specify both per primitive.

## Option B (alternative) — per-consumer owner-thread marshaling, zero `unsafe`

Keep facades `unsendable`; each consumer spawns one dedicated owner thread and
marshals every op through a queue. No `unsafe`, no Tessera change, but a
per-consumer thread-bridge (×3) with a queue-hop of latency, re-paid by every
future host. Not recommended for a portable library.

## Acceptance tests (none exist yet)

1. Pool: create on A, `acquire`/`write` on B, `release` on C, **with the pool
   full** so `acquire` blocks until the cross-thread `release` runs.
2. `Send` check — create on A, use on B, **drop on C** — for every role marked
   `Send`. **Excludes Sink owner** if it stays thread-affine; in that case Sink
   must raise a **clean documented error**, not a PyO3 panic.
3. `Sync` check — true concurrent calls on one handle (with the GIL released):
   Ring multi-writer, Channel multi-sender, Pool concurrent acquire/release.
   And the negative: concurrent `recv` on one Channel receiver / `poll` on one
   Ring reader must be either prevented or serialized, never UB.
4. **Pool read/write meta race** — one thread `read_payload`/`in_use_count`
   (both owner handle and attacher handle variants) while another
   `acquire`/`release`/reclaims the same slots; assert no torn read and no UB
   (the case the current facade lock and `&mut self` mutation path hide today).
5. **Close cancels, not just protects** — block a thread in `recv` / full
   `send` / full `acquire`, call `close()` from another thread, assert the
   blocked op returns a clean `Closed` error **promptly** (well under its
   timeout), and that no `munmap` happens until that op has returned.
6. `close()` / parent drop racing an in-flight blocking op, per primitive
   (the use-after-unmap case).

## Questions for the reviewer

1. Is the per-role `Send`/`Sync` table now correct — especially Pool owner
   `Sync` *after* the `&mut self → &self` refactor, and the `Send`-not-`Sync`
   classification of Ring reader, Channel receiver, and Sink owner?
2. For the Pool read path, which is preferable — a **per-slot lock** covering
   validate-then-copy, or moving `SlotMeta` to **atomics/seqlock** with reader
   retry? (The latter is more code but keeps reads lock-free.)
3. Is `Arc` + "mark-closed, unmap-on-last-ref" + an **atomic `closed` flag
   checked in every wait loop** the right close/drop + cancellation design for
   all primitives, or are there ones where a simpler scheme is safe?
4. For the `Send`-not-`Sync` handles, do you prefer a documented one-caller
   contract (cheaper, pushes discipline to the host) or a built-in per-handle
   serialization lock (safer, hides a subtle footgun)?
5. Any deadlock/soundness hazard from `py.allow_threads` while a core lock is
   held during the refactored Pool ops?

## Appendix — minimal repro

```python
import threading
from tessera_pool import Pool
p = Pool(description="x", slot_count=4, slot_size_bytes=4096)   # thread MAIN
t = threading.Thread(target=p.in_use_count); t.start(); t.join() # -> PanicException
```
