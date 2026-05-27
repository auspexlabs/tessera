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

## Threading

`Channel` is `Send + Sync` — handles move between threads and are used
concurrently. Blocking `send()` / `recv()` release the GIL, so
cross-thread MPSC via `threading.Thread` works (no deadlock). The
contract is role-specific:

- **Sender** is concurrently callable — multiple senders (threads or
  processes) publish via CAS-MPSC.
- **Receiver** is single-consumer: the dequeue is serialized by an
  internal lock, so concurrent `recv()` on a shared/cloned receiver is
  safe (one at a time). `try_recv()` stays non-blocking, and
  `recv_timeout()`'s deadline bounds the total wait.

`close()` wakes a blocked `send`/`recv` with a `TesseraChannelError`.
`multiprocessing.Process` also works (each subprocess opens its own
handle by description). Design notes:
[`docs/issue_facade_thread_safety.md`](../../docs/issue_facade_thread_safety.md).
