# tessera-channel (Python)

Python facade for [`tessera-channel`](../../crates/tessera-channel/) —
non-lossy MPSC shared-memory queue.

**Status**: v0.0.1. Functional in development builds; not yet
published to PyPI.

## Install (development)

```bash
cd python/py-tessera-channel
maturin develop --release   # builds the native extension into the active venv
```

## Quick start

```python
from tessera_channel import Channel

# Receiver (process A) creates the region:
with Channel(description="my-app/control",
             slot_count=256,
             slot_size_bytes=4096,
             role="receiver") as chan:
    msg = chan.recv()      # blocks until a message arrives
    print(repr(msg))

# Sender (process B) attaches:
with Channel(description="my-app/control",
             slot_count=256,
             slot_size_bytes=4096,
             role="sender") as chan:
    chan.send(b"hello channel")     # blocks if queue full (non-lossy)
    chan.try_send(b"or fail-fast")  # raises ChannelFull if full
    chan.send_timeout(b"or bounded", timeout_seconds=0.5)
```

`slot_size_bytes` must be a multiple of 8 (AtomicU64 alignment for
per-slot fields). MPSC: exactly one Receiver per region; multiple
Senders may coexist.

See [`examples/channel_intra_container.py`](../../examples/channel_intra_container.py)
for a cross-process receiver + subprocess sender demo, and
[`examples/channel_mpsc.py`](../../examples/channel_mpsc.py) for an
N-producer single-consumer pattern.

## Public surface

- `Channel` — context-manager-friendly; constructs / attaches the SHM region.
  - `send(bytes)` — blocking; non-lossy
  - `try_send(bytes)` — non-blocking; raises `TesseraChannelError` with "full"
  - `send_timeout(bytes, timeout_seconds)` — bounded blocking
  - `recv()` — blocking
  - `try_recv()` — non-blocking; raises with "empty"
  - `recv_timeout(timeout_seconds)` — bounded blocking
  - `positions() -> (head, tail)` — diagnostics
  - `is_owner` / `role` / `slot_count` / `slot_size_bytes` properties
  - `close()` / `__enter__` / `__exit__`
- `TesseraChannelError` — base exception class.

## v0.1 limitation: cross-thread Python use

The Python `Channel` class is currently `unsendable` (Rust `Channel`
is `!Send` due to `Shmem`'s `!Send`), and blocking `send()` / `recv()`
calls spin inside Rust while holding the GIL. As a result, doing
cross-thread MPSC via `threading.Thread` will deadlock (one thread
blocks holding the GIL, another can't proceed). The Rust core IS
MPSC-safe (validated by the `concurrent_multiple_producers_…` test
in the Rust crate); the limitation is purely at the Python facade
layer.

For now, Python users wanting multi-producer should use
`multiprocessing.Process` — each subprocess has its own GIL. The planned
thread-safe contract is role-specific: Sender can become concurrently
callable, while a single Receiver handle must stay one-caller-at-a-time
or be internally serialized. The design is tracked in
[`docs/issue_facade_thread_safety.md`](../../docs/issue_facade_thread_safety.md).
