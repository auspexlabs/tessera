//! Tessera Channel state machine — send / try_send / send_timeout /
//! recv / try_recv.
//!
//! MPSC FIFO ring over SHM. Multiple Senders (`fetch_add(tail)`)
//! compete for the next slot; exactly one Receiver (`load(head)` →
//! advance `head`) drains in order. Non-lossy: senders never
//! overwrite a slot the receiver hasn't dequeued.
//!
//! Linearizability comes from the head/tail discipline plus per-slot
//! `sequence` cross-check; no seqlock retry is needed on the read side
//! because only one Receiver consumes.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::error::{Result, TesseraChannelError};
use crate::namespace::NamespaceHandle;
use crate::region::Region;
use crate::{ChannelConfig, ChannelRole};

/// How many bounded-spin iterations the Sender / Receiver attempt
/// before yielding to the OS scheduler. Spin is cheap on contention
/// that resolves in microseconds (e.g. multiple senders racing on
/// the same CAS window); yielding is appropriate when the
/// counterparty isn't making forward progress.
const SPIN_BUDGET_BEFORE_YIELD: u32 = 64;

/// Classification of a single receive attempt, so the public recv
/// family can choose blocking vs non-blocking behavior without the
/// shared helper spinning internally.
enum RecvOutcome {
    /// A message was dequeued; head advanced past it.
    Got(Vec<u8>),
    /// `head == tail` — the queue has no claimed slots.
    Empty,
    /// `head < tail` (the head slot has been claimed by a sender)
    /// but the slot's `ready` flag isn't set yet — sender is
    /// mid-write or stalled. Returned WITHOUT spinning so `try_recv`
    /// fails fast; `recv` / `recv_timeout` drive their own wait loop.
    NotReady,
}

/// Tessera Channel handle. Wraps a shared `Region` and exposes the
/// MPSC queue API. The handle's role (Receiver vs Sender) is fixed
/// at `open` time and controls which operations succeed.
#[derive(Clone)]
pub struct Channel {
    region: Arc<Region>,
    role: ChannelRole,
}

impl Channel {
    /// Open a Channel per the config: BLAKE3-derive the namespace
    /// handle, then either create (Receiver) or attach (Sender) the
    /// SHM region.
    pub fn open(config: ChannelConfig) -> Result<Self> {
        let handle = NamespaceHandle::derive(&config.description);
        let region = match config.role {
            ChannelRole::Receiver => Region::create(
                &handle,
                config.slot_count,
                config.slot_size_bytes,
                config.force_recreate,
            )?,
            ChannelRole::Sender => Region::attach(
                &handle,
                config.slot_count,
                config.slot_size_bytes,
            )?,
        };
        Ok(Self {
            region: Arc::new(region),
            role: config.role,
        })
    }

    /// Role this Channel handle was opened with.
    pub fn role(&self) -> ChannelRole {
        self.role
    }

    /// True iff this handle was opened as the region creator (Receiver).
    pub fn is_owner(&self) -> bool {
        self.region.is_owner()
    }

    fn require_role(&self, required: ChannelRole) -> Result<()> {
        if self.role != required {
            return Err(TesseraChannelError::RoleMismatch {
                actual: self.role.snapshot(),
                required: required.snapshot(),
            });
        }
        Ok(())
    }

    fn validate_payload_len(&self, bytes_len: usize) -> Result<()> {
        let cap = self.region.slot_size_bytes() as usize;
        if bytes_len > cap {
            return Err(TesseraChannelError::OversizedPayload {
                payload_size: bytes_len,
                slot_size: cap,
            });
        }
        Ok(())
    }

    /// Publish one message. Blocks until room is available. Lossless:
    /// if the Channel is full, the call waits for the Receiver to
    /// drain rather than overwriting.
    ///
    /// Role: must be opened as `Sender`.
    pub fn send(&self, bytes: &[u8]) -> Result<()> {
        self.require_role(ChannelRole::Sender)?;
        self.validate_payload_len(bytes.len())?;
        loop {
            if self.try_send_inner(bytes)? {
                return Ok(());
            }
            // Full — spin briefly, then yield. The Receiver advancing
            // head is the only thing that can free a slot, so we have
            // to wait.
            for _ in 0..SPIN_BUDGET_BEFORE_YIELD {
                if !self.is_full() {
                    break;
                }
                core::hint::spin_loop();
            }
            if self.is_full() {
                std::thread::yield_now();
            }
        }
    }

    /// Non-blocking publish. Returns `Err(ChannelFull)` if the queue
    /// is full at the moment of the call; the caller is responsible
    /// for retrying or accepting the failure.
    ///
    /// Role: must be opened as `Sender`.
    pub fn try_send(&self, bytes: &[u8]) -> Result<()> {
        self.require_role(ChannelRole::Sender)?;
        self.validate_payload_len(bytes.len())?;
        if self.try_send_inner(bytes)? {
            Ok(())
        } else {
            Err(TesseraChannelError::ChannelFull {
                slot_count: self.region.slot_count(),
            })
        }
    }

    /// Bounded-blocking publish. Returns `Ok(())` on enqueue,
    /// `Err(Timeout)` if the budget expires before room is available.
    ///
    /// Role: must be opened as `Sender`.
    pub fn send_timeout(&self, bytes: &[u8], timeout: Duration) -> Result<()> {
        self.require_role(ChannelRole::Sender)?;
        self.validate_payload_len(bytes.len())?;
        let deadline = Instant::now() + timeout;
        loop {
            if self.try_send_inner(bytes)? {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(TesseraChannelError::Timeout {
                    timeout_micros: timeout.as_micros() as u64,
                    head: self.region.head_atomic().load(Ordering::Acquire),
                    tail: self.region.tail_atomic().load(Ordering::Acquire),
                });
            }
            for _ in 0..SPIN_BUDGET_BEFORE_YIELD {
                if !self.is_full() {
                    break;
                }
                core::hint::spin_loop();
            }
            if self.is_full() {
                std::thread::yield_now();
            }
        }
    }

    /// Attempt one send. Returns `Ok(true)` on success, `Ok(false)`
    /// if the queue was full (caller decides whether to wait / retry
    /// / return error).
    ///
    /// Internal helper shared by `send`, `try_send`, and
    /// `send_timeout`.
    ///
    /// Codex PR #8 P1 fix (channel.rs:190): the previous version did
    /// an unconditional `fetch_add(tail)` AFTER a separate fullness
    /// pre-check. Two concurrent senders could both pass the check
    /// with only one slot free; the loser claimed a wrapped slot and
    /// either blocked (violating try_send's non-blocking contract)
    /// or raced an in-flight writer. The fix is a CAS loop that
    /// re-checks capacity against `head` and only commits the claim
    /// via `compare_exchange` — so capacity reservation and the tail
    /// increment are atomic together.
    fn try_send_inner(&self, bytes: &[u8]) -> Result<bool> {
        let slot_count = self.region.slot_count() as u64;
        let tail_atomic = self.region.tail_atomic();
        let head_atomic = self.region.head_atomic();

        // CAS-claim: atomically reserve capacity + increment tail.
        // Only the sender that wins the CAS for a given `tail` value
        // owns that position; losers retry with a fresh load. A
        // sender NEVER claims beyond `slot_count` outstanding slots.
        let claimed = loop {
            let tail = tail_atomic.load(Ordering::Acquire);
            let head = head_atomic.load(Ordering::Acquire);
            if tail.wrapping_sub(head) >= slot_count {
                // Genuinely full at this instant — no claim made, so
                // try_send can fail-fast and send/send_timeout can
                // wait + retry. Non-blocking contract preserved.
                return Ok(false);
            }
            match tail_atomic.compare_exchange_weak(
                tail,
                tail.wrapping_add(1),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break tail,
                Err(_) => {
                    // Another sender advanced tail between our load
                    // and CAS. Retry with a fresh capacity check.
                    core::hint::spin_loop();
                    continue;
                }
            }
        };

        let slot_index = (claimed % slot_count) as u32;
        let seq = self.region.slot_sequence_atomic(slot_index)?;
        let ready = self.region.slot_ready_atomic(slot_index)?;

        // No ready-spin needed. The CAS capacity check guarantees
        // `claimed - head < slot_count`, i.e. `claimed - slot_count
        // < head`. The slot at `claimed % slot_count` was last
        // written at position `claimed - slot_count`; the Receiver
        // clears that slot's `ready` BEFORE advancing `head` past
        // `claimed - slot_count`. Our Acquire-load of `head` in the
        // CAS loop synchronizes-with the Receiver's Release-store of
        // `head`, so the cleared `ready` (and consumed payload) are
        // visible to us. The slot is reusable.

        // Write payload + metadata. SAFETY: we hold the slot
        // exclusively — the CAS guarantees a unique outstanding
        // claim per slot (no two senders own the same slot at once),
        // and the capacity argument above guarantees no Receiver is
        // mid-read on this slot from a prior cycle.
        let timestamp = current_nanos();
        unsafe {
            let dst = self.region.slot_payload_ptr_mut(slot_index)?;
            core::ptr::copy_nonoverlapping(bytes.as_ptr(), dst, bytes.len());
            self.region
                .write_slot_metadata(slot_index, bytes.len() as u32, timestamp)?;
        }
        // Stamp sequence FIRST (Release), then ready (Release).
        // Ordering: a Receiver observing ready==1 must also observe
        // the matching sequence (and the payload). The ready store
        // is the linearization point that publishes the slot.
        seq.store(claimed, Ordering::Release);
        ready.store(1, Ordering::Release);
        Ok(true)
    }

    fn is_full(&self) -> bool {
        let slot_count = self.region.slot_count() as u64;
        let head = self.region.head_atomic().load(Ordering::Acquire);
        let tail = self.region.tail_atomic().load(Ordering::Acquire);
        tail.wrapping_sub(head) >= slot_count
    }

    fn is_empty(&self) -> bool {
        let head = self.region.head_atomic().load(Ordering::Acquire);
        let tail = self.region.tail_atomic().load(Ordering::Acquire);
        head >= tail
    }

    /// Dequeue one message. Blocks until a Sender enqueues if the
    /// Channel is empty (or the head slot's producer is mid-write).
    ///
    /// Role: must be opened as `Receiver`.
    pub fn recv(&self) -> Result<Vec<u8>> {
        self.require_role(ChannelRole::Receiver)?;
        loop {
            match self.try_recv_inner()? {
                RecvOutcome::Got(msg) => return Ok(msg),
                // Empty (head == tail) OR NotReady (head < tail but
                // producer hasn't published the slot yet): block.
                RecvOutcome::Empty | RecvOutcome::NotReady => {
                    for _ in 0..SPIN_BUDGET_BEFORE_YIELD {
                        if !self.is_empty() {
                            break;
                        }
                        core::hint::spin_loop();
                    }
                    std::thread::yield_now();
                }
            }
        }
    }

    /// Non-blocking dequeue. Returns `Err(ChannelEmpty)` if no
    /// message is currently receivable — either the queue is truly
    /// empty (`head == tail`) OR the head slot has been claimed by a
    /// sender that hasn't finished publishing yet (`head < tail` but
    /// `ready == 0`). In both cases the call returns promptly without
    /// blocking.
    ///
    /// Codex PR #8 P1 fix (channel.rs:342): the previous shared
    /// helper spun unboundedly waiting for `ready`, so a sender that
    /// stalled after `fetch_add(tail)` but before publishing would
    /// make `try_recv` block forever — breaking the non-blocking
    /// contract. `try_recv` now treats an unready head slot as
    /// not-currently-receivable and returns immediately.
    ///
    /// Role: must be opened as `Receiver`.
    pub fn try_recv(&self) -> Result<Vec<u8>> {
        self.require_role(ChannelRole::Receiver)?;
        match self.try_recv_inner()? {
            RecvOutcome::Got(msg) => Ok(msg),
            RecvOutcome::Empty | RecvOutcome::NotReady => {
                let head = self.region.head_atomic().load(Ordering::Acquire);
                Err(TesseraChannelError::ChannelEmpty { head })
            }
        }
    }

    /// Bounded-blocking dequeue.
    ///
    /// Role: must be opened as `Receiver`.
    pub fn recv_timeout(&self, timeout: Duration) -> Result<Vec<u8>> {
        self.require_role(ChannelRole::Receiver)?;
        let deadline = Instant::now() + timeout;
        loop {
            match self.try_recv_inner()? {
                RecvOutcome::Got(msg) => return Ok(msg),
                RecvOutcome::Empty | RecvOutcome::NotReady => {
                    if Instant::now() >= deadline {
                        return Err(TesseraChannelError::Timeout {
                            timeout_micros: timeout.as_micros() as u64,
                            head: self.region.head_atomic().load(Ordering::Acquire),
                            tail: self.region.tail_atomic().load(Ordering::Acquire),
                        });
                    }
                    for _ in 0..SPIN_BUDGET_BEFORE_YIELD {
                        if !self.is_empty() {
                            break;
                        }
                        core::hint::spin_loop();
                    }
                    std::thread::yield_now();
                }
            }
        }
    }

    /// Attempt one receive, classifying the result so callers can
    /// choose blocking vs non-blocking behavior:
    ///   - `Got(bytes)` — a message was dequeued; head advanced.
    ///   - `Empty` — `head == tail`; the queue has no claimed slots.
    ///   - `NotReady` — `head < tail` (a sender claimed the head
    ///     slot) but the slot's `ready` flag isn't set yet (sender
    ///     is mid-write or stalled). Crucially this returns WITHOUT
    ///     spinning, so `try_recv` can fail-fast.
    ///
    /// FIFO is preserved: the Receiver only ever looks at the slot
    /// at `head % slot_count`; it never skips ahead to a later slot
    /// that happens to be ready while the head slot isn't.
    fn try_recv_inner(&self) -> Result<RecvOutcome> {
        let slot_count = self.region.slot_count() as u64;
        let head = self.region.head_atomic().load(Ordering::Acquire);
        let tail = self.region.tail_atomic().load(Ordering::Acquire);
        if head >= tail {
            return Ok(RecvOutcome::Empty);
        }
        let slot_index = (head % slot_count) as u32;
        let ready = self.region.slot_ready_atomic(slot_index)?;
        if ready.load(Ordering::Acquire) == 0 {
            // A sender claimed this slot (head < tail) but hasn't
            // published it yet. Do NOT spin here — return NotReady so
            // try_recv fails fast and recv / recv_timeout drive the
            // wait loop at their own level.
            return Ok(RecvOutcome::NotReady);
        }

        // Sequence sanity check: the slot's stamped sequence must
        // match our expected head. A mismatch indicates a protocol
        // violation (a wrapped sender that didn't observe ready=0,
        // or memory corruption). Fail fast rather than deliver wrong
        // data. With the CAS-based sender claim this should never
        // occur.
        let seq = self.region.slot_sequence_atomic(slot_index)?;
        let observed_seq = seq.load(Ordering::Acquire);
        if observed_seq != head {
            return Err(TesseraChannelError::Region(format!(
                "slot sequence mismatch at head {head}: slot[{slot_index}].sequence \
                = {observed_seq}; expected {head}. Indicates a stale write from a \
                wrapped sender that didn't observe ready=0 — should not occur \
                with the current CAS-based Sender protocol. Failing fast."
            )));
        }

        // SAFETY: ready == 1 and sequence == head confirmed; the
        // producer's writes (payload + metadata) are visible via the
        // ready Release → Acquire pairing.
        let (length, _ts) = unsafe { self.region.read_slot_metadata(slot_index)? };
        let cap = self.region.slot_size_bytes();
        let copy_len = length.min(cap) as usize;
        let mut payload = vec![0u8; copy_len];
        unsafe {
            let src = self.region.slot_payload_ptr(slot_index)?;
            core::ptr::copy_nonoverlapping(src, payload.as_mut_ptr(), copy_len);
        }

        // Clear ready (Release: pairs with the next-cycle Sender's
        // capacity-check Acquire on head). Then advance head so
        // subsequent recv calls move forward.
        ready.store(0, Ordering::Release);
        self.region
            .head_atomic()
            .store(head + 1, Ordering::Release);
        Ok(RecvOutcome::Got(payload))
    }

    /// Snapshot of `(head, tail)` for diagnostics. Not part of the
    /// happy path; intended for tests, examples, observability.
    pub fn positions(&self) -> (u64, u64) {
        (
            self.region.head_atomic().load(Ordering::Acquire),
            self.region.tail_atomic().load(Ordering::Acquire),
        )
    }
}

fn current_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unique_description(tag: &str) -> String {
        let pid = std::process::id();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        format!("tessera-channel-state-test/{tag}/{pid}/{nanos}")
    }

    fn open_receiver_and_sender(tag: &str, slot_count: u32, slot_size: u32) -> (Channel, Channel) {
        let desc = unique_description(tag);
        let receiver = Channel::open(ChannelConfig {
            description: desc.clone(),
            slot_count,
            slot_size_bytes: slot_size,
            role: ChannelRole::Receiver,
            force_recreate: false,
        })
        .expect("receiver open");
        let sender = Channel::open(ChannelConfig {
            description: desc,
            slot_count,
            slot_size_bytes: slot_size,
            role: ChannelRole::Sender,
            force_recreate: false,
        })
        .expect("sender open");
        (receiver, sender)
    }

    #[test]
    fn try_recv_returns_promptly_when_head_slot_claimed_but_not_ready() {
        // Codex PR #8 P1 fix (channel.rs:342) regression: simulate a
        // sender that claimed the head slot (incremented tail) but
        // stalled before publishing (ready stays 0). try_recv must
        // return ChannelEmpty PROMPTLY, not spin forever.
        use crate::namespace::NamespaceHandle;
        use crate::region::Region;
        use std::sync::atomic::Ordering;

        let desc = unique_description("try-recv-not-ready");
        let receiver = Channel::open(ChannelConfig {
            description: desc.clone(),
            slot_count: 4,
            slot_size_bytes: 16,
            role: ChannelRole::Receiver,
            force_recreate: false,
        })
        .expect("receiver open");

        // Reach the same SHM via a directly-attached Region and
        // increment tail without publishing — exactly the
        // "sender claimed but stalled" state.
        let handle = NamespaceHandle::derive(&desc);
        let region = Region::attach(&handle, 4, 16).expect("attach region");
        region.tail_atomic().fetch_add(1, Ordering::SeqCst);

        // head == 0, tail == 1: head < tail but slot 0 ready == 0.
        let start = std::time::Instant::now();
        let err = receiver.try_recv().unwrap_err();
        let elapsed = start.elapsed();
        assert!(
            matches!(err, TesseraChannelError::ChannelEmpty { .. }),
            "expected ChannelEmpty, got {err:?}"
        );
        assert!(
            elapsed.as_secs() < 1,
            "try_recv must return promptly on not-ready head slot; took {elapsed:?}"
        );

        drop(region);
    }

    #[test]
    fn tight_ring_concurrent_senders_no_overcommit_or_corruption() {
        // Codex PR #8 P1 fix (channel.rs:190) regression: a tight
        // ring (slot_count=2) with many concurrent senders maximizes
        // capacity-contention. With the old unconditional fetch_add,
        // two senders could both pass the fullness pre-check and
        // overcommit, producing sequence mismatches / lost messages.
        // With the CAS-based claim, every message arrives intact.
        use std::thread;

        const N_PRODUCERS: u32 = 6;
        const N_PER_PRODUCER: u32 = 200;

        let desc = unique_description("tight-ring-mpsc");
        let slot_count = 2; // brutally tight: only 2 outstanding at once
        let slot_size = 16;

        let receiver = Channel::open(ChannelConfig {
            description: desc.clone(),
            slot_count,
            slot_size_bytes: slot_size,
            role: ChannelRole::Receiver,
            force_recreate: false,
        })
        .expect("receiver open");

        let producers: Vec<_> = (0..N_PRODUCERS)
            .map(|pid| {
                let desc = desc.clone();
                thread::spawn(move || {
                    let sender = Channel::open(ChannelConfig {
                        description: desc,
                        slot_count,
                        slot_size_bytes: slot_size,
                        role: ChannelRole::Sender,
                        force_recreate: false,
                    })
                    .expect("sender open");
                    for i in 0..N_PER_PRODUCER {
                        let mut p = [0u8; 8];
                        p[..4].copy_from_slice(&pid.to_le_bytes());
                        p[4..].copy_from_slice(&i.to_le_bytes());
                        sender.send(&p).expect("send");
                    }
                })
            })
            .collect();

        let total = (N_PRODUCERS * N_PER_PRODUCER) as usize;
        let mut seen = std::collections::HashSet::new();
        while seen.len() < total {
            let msg = receiver.recv().expect("recv");
            assert_eq!(msg.len(), 8);
            let pid = u32::from_le_bytes(msg[..4].try_into().unwrap());
            let seq = u32::from_le_bytes(msg[4..].try_into().unwrap());
            assert!(pid < N_PRODUCERS, "pid {pid} out of range — overcommit corruption?");
            assert!(seq < N_PER_PRODUCER, "seq {seq} out of range — overcommit corruption?");
            assert!(seen.insert((pid, seq)), "duplicate ({pid}, {seq})");
        }
        for h in producers {
            h.join().expect("producer join");
        }
        assert_eq!(seen.len(), total, "every message must arrive exactly once");
    }

    #[test]
    fn send_then_recv_returns_payload() {
        let (recv, send) = open_receiver_and_sender("simple", 4, 64);
        send.send(b"hello channel").expect("send");
        let got = recv.recv().expect("recv");
        assert_eq!(got, b"hello channel");
        let (h, t) = recv.positions();
        assert_eq!(h, 1);
        assert_eq!(t, 1);
    }

    #[test]
    fn multiple_messages_arrive_in_order() {
        let (recv, send) = open_receiver_and_sender("ordered", 8, 32);
        for i in 0..5u32 {
            send.send(&i.to_le_bytes()).expect("send");
        }
        for i in 0..5u32 {
            let got = recv.recv().expect("recv");
            assert_eq!(got, i.to_le_bytes());
        }
    }

    #[test]
    fn try_send_fails_fast_when_full() {
        let (_recv, send) = open_receiver_and_sender("try-send-full", 2, 16);
        send.try_send(b"a").expect("first");
        send.try_send(b"b").expect("second");
        // Queue is full; third try_send fails.
        let err = send.try_send(b"c").unwrap_err();
        assert!(matches!(err, TesseraChannelError::ChannelFull { slot_count: 2 }));
    }

    #[test]
    fn try_recv_fails_fast_when_empty() {
        let (recv, _send) = open_receiver_and_sender("try-recv-empty", 4, 16);
        let err = recv.try_recv().unwrap_err();
        assert!(matches!(err, TesseraChannelError::ChannelEmpty { head: 0 }));
    }

    #[test]
    fn send_timeout_returns_timeout_on_full() {
        let (_recv, send) = open_receiver_and_sender("send-timeout", 1, 16);
        send.send(b"a").expect("first");
        // Second send blocks; with a short timeout we expect Timeout.
        let err = send.send_timeout(b"b", Duration::from_millis(10)).unwrap_err();
        assert!(matches!(err, TesseraChannelError::Timeout { .. }));
    }

    #[test]
    fn recv_timeout_returns_timeout_on_empty() {
        let (recv, _send) = open_receiver_and_sender("recv-timeout", 4, 16);
        let err = recv.recv_timeout(Duration::from_millis(10)).unwrap_err();
        assert!(matches!(err, TesseraChannelError::Timeout { .. }));
    }

    #[test]
    fn role_mismatch_errors() {
        let (recv, send) = open_receiver_and_sender("role-mismatch", 4, 16);
        // Receiver-role handle can't send.
        let err = recv.send(b"x").unwrap_err();
        assert!(matches!(err, TesseraChannelError::RoleMismatch { .. }));
        // Sender-role handle can't recv.
        let err = send.recv().unwrap_err();
        assert!(matches!(err, TesseraChannelError::RoleMismatch { .. }));
    }

    #[test]
    fn oversized_payload_rejected() {
        let (_recv, send) = open_receiver_and_sender("oversized", 4, 16);
        let big = vec![0u8; 17];
        let err = send.try_send(&big).unwrap_err();
        match err {
            TesseraChannelError::OversizedPayload {
                payload_size,
                slot_size,
            } => {
                assert_eq!(payload_size, 17);
                assert_eq!(slot_size, 16);
            }
            other => panic!("expected OversizedPayload, got {other:?}"),
        }
    }

    #[test]
    fn ring_wraps_correctly_after_full_drain() {
        let (recv, send) = open_receiver_and_sender("wraparound", 4, 16);
        // Fill, drain, fill again to exercise the wrap.
        for cycle in 0..3 {
            for i in 0..4u32 {
                let msg = format!("c{cycle}-i{i}");
                send.send(msg.as_bytes()).expect("send");
            }
            for i in 0..4u32 {
                let msg = format!("c{cycle}-i{i}");
                let got = recv.recv().expect("recv");
                assert_eq!(got, msg.as_bytes());
            }
        }
        let (h, t) = recv.positions();
        assert_eq!(h, 12);
        assert_eq!(t, 12);
    }

    #[test]
    fn concurrent_multiple_producers_single_consumer_preserves_all_messages() {
        // MPSC happy path: 4 producer threads each send 100 messages
        // through a 16-slot Channel; single consumer drains all 400.
        // Channel is non-lossy, so EVERY message must arrive.
        use std::thread;

        const N_PRODUCERS: u32 = 4;
        const N_PER_PRODUCER: u32 = 100;

        let desc = unique_description("mpsc-stress");
        let slot_count = 16;
        let slot_size = 16;

        let receiver = Channel::open(ChannelConfig {
            description: desc.clone(),
            slot_count,
            slot_size_bytes: slot_size,
            role: ChannelRole::Receiver,
            force_recreate: false,
        })
        .expect("receiver open");

        let producer_threads: Vec<_> = (0..N_PRODUCERS)
            .map(|pid| {
                let desc = desc.clone();
                thread::spawn(move || {
                    let sender = Channel::open(ChannelConfig {
                        description: desc,
                        slot_count,
                        slot_size_bytes: slot_size,
                        role: ChannelRole::Sender,
                        force_recreate: false,
                    })
                    .expect("sender open");
                    for i in 0..N_PER_PRODUCER {
                        let mut payload = [0u8; 8];
                        payload[..4].copy_from_slice(&pid.to_le_bytes());
                        payload[4..].copy_from_slice(&i.to_le_bytes());
                        sender.send(&payload).expect("send");
                    }
                })
            })
            .collect();

        let total = (N_PRODUCERS * N_PER_PRODUCER) as usize;
        let mut received = Vec::with_capacity(total);
        // Read until all producers' messages have been observed.
        // Producers may still be in flight; recv blocks until at
        // least one arrives.
        while received.len() < total {
            let msg = receiver.recv().expect("recv");
            received.push(msg);
        }
        for h in producer_threads {
            h.join().expect("producer join");
        }

        // Every (producer_id, sequence) pair must appear exactly once.
        let mut seen = std::collections::HashSet::new();
        for msg in &received {
            assert_eq!(msg.len(), 8);
            let pid = u32::from_le_bytes(msg[..4].try_into().unwrap());
            let seq = u32::from_le_bytes(msg[4..].try_into().unwrap());
            assert!(pid < N_PRODUCERS);
            assert!(seq < N_PER_PRODUCER);
            assert!(
                seen.insert((pid, seq)),
                "duplicate message (pid={pid}, seq={seq})"
            );
        }
        assert_eq!(seen.len(), total, "every message must arrive exactly once");
    }
}
