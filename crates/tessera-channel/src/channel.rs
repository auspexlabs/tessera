//! Tessera Channel state machine — send / try_send / send_timeout /
//! recv / try_recv.
//!
//! MPSC FIFO ring over SHM. Multiple Senders (`fetch_add(tail)`)
//! compete for the next slot; exactly one Receiver (`load(head)` →
//! advance `head`) drains in order. Non-lossy: senders never
//! overwrite a slot the receiver hasn't dequeued.
//!
//! Per side-doc §4c pseudocode. Linearizability comes from the
//! head/tail discipline + per-slot `sequence` cross-check; no
//! seqlock retry on the read side because only one Receiver
//! consumes.

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
/// the same fetch_add window); yielding is appropriate when the
/// counterparty isn't making forward progress.
const SPIN_BUDGET_BEFORE_YIELD: u32 = 64;

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
    fn try_send_inner(&self, bytes: &[u8]) -> Result<bool> {
        let slot_count = self.region.slot_count() as u64;
        let head = self.region.head_atomic().load(Ordering::Acquire);
        let tail = self.region.tail_atomic().load(Ordering::Acquire);
        if tail.wrapping_sub(head) >= slot_count {
            return Ok(false);
        }
        // Claim the slot via CAS-equivalent fetch_add. If multiple
        // senders are racing, they each get a unique position. A
        // pathological case: between our head/tail read above and
        // the fetch_add, the queue fills up — fetch_add ALWAYS
        // succeeds (it's unconditional) so we could end up
        // overwriting a slot the receiver hasn't drained yet. Guard
        // against that with a post-claim check.
        let claimed = self.region.tail_atomic().fetch_add(1, Ordering::AcqRel);
        // Re-check: did we just claim a slot that's still occupied?
        // If so, give it back by NOT marking it ready — the Receiver
        // won't dequeue an unready slot. Callers retry.
        //
        // Actually a more aggressive defense: spin until the slot's
        // `ready` is clear (Receiver has dequeued the prior cycle).
        // This is correct under MPSC because:
        //   - the slot at `claimed % slot_count` was last used at
        //     position `claimed - slot_count` (the previous wrap)
        //   - the Receiver clears `ready` AFTER advancing head past
        //     that position
        //   - so if head > claimed - slot_count, the prior writer's
        //     payload has been consumed and the slot is reusable
        let slot_index = (claimed % slot_count) as u32;
        let ready = self.region.slot_ready_atomic(slot_index)?;
        let seq = self.region.slot_sequence_atomic(slot_index)?;
        // Wait for the slot to be reusable: the previous cycle's
        // Receiver must have cleared `ready`. Under normal flow this
        // is immediate (the head/tail check above already proves
        // it). The wait protects against the edge case where head
        // advanced just past the gate but the Receiver hasn't yet
        // cleared the slot's ready flag.
        let mut spin = 0u32;
        while ready.load(Ordering::Acquire) != 0 {
            spin += 1;
            if spin > SPIN_BUDGET_BEFORE_YIELD {
                std::thread::yield_now();
                spin = 0;
            } else {
                core::hint::spin_loop();
            }
        }

        // Write payload + metadata. SAFETY: we hold the slot
        // exclusively — claimed via unique fetch_add result, and the
        // `ready == 0` check above guarantees no Receiver is mid-read
        // on this slot from a prior cycle.
        let timestamp = current_nanos();
        unsafe {
            let dst = self.region.slot_payload_ptr_mut(slot_index)?;
            core::ptr::copy_nonoverlapping(bytes.as_ptr(), dst, bytes.len());
            self.region
                .write_slot_metadata(slot_index, bytes.len() as u32, timestamp)?;
        }
        // Stamp sequence FIRST (Release: pairs with Receiver's
        // Acquire on sequence), then ready (Release: pairs with
        // Receiver's Acquire on ready). Ordering matters: if
        // Receiver observes ready==1 but sequence still 0 (stale),
        // it would treat the slot as belonging to position 0
        // instead of our `claimed`. The two stores below ensure
        // sequence is durable before ready is set.
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
    /// Channel is empty.
    ///
    /// Role: must be opened as `Receiver`.
    pub fn recv(&self) -> Result<Vec<u8>> {
        self.require_role(ChannelRole::Receiver)?;
        loop {
            if let Some(msg) = self.try_recv_inner()? {
                return Ok(msg);
            }
            for _ in 0..SPIN_BUDGET_BEFORE_YIELD {
                if !self.is_empty() {
                    break;
                }
                core::hint::spin_loop();
            }
            if self.is_empty() {
                std::thread::yield_now();
            }
        }
    }

    /// Non-blocking dequeue. Returns `Err(ChannelEmpty)` if no
    /// message is available at the moment of the call.
    ///
    /// Role: must be opened as `Receiver`.
    pub fn try_recv(&self) -> Result<Vec<u8>> {
        self.require_role(ChannelRole::Receiver)?;
        if let Some(msg) = self.try_recv_inner()? {
            Ok(msg)
        } else {
            let head = self.region.head_atomic().load(Ordering::Acquire);
            Err(TesseraChannelError::ChannelEmpty { head })
        }
    }

    /// Bounded-blocking dequeue.
    ///
    /// Role: must be opened as `Receiver`.
    pub fn recv_timeout(&self, timeout: Duration) -> Result<Vec<u8>> {
        self.require_role(ChannelRole::Receiver)?;
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(msg) = self.try_recv_inner()? {
                return Ok(msg);
            }
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
            if self.is_empty() {
                std::thread::yield_now();
            }
        }
    }

    /// Attempt one receive. Returns `Ok(Some(bytes))` on dequeue,
    /// `Ok(None)` if the queue was empty (caller decides what to do).
    fn try_recv_inner(&self) -> Result<Option<Vec<u8>>> {
        let slot_count = self.region.slot_count() as u64;
        let head = self.region.head_atomic().load(Ordering::Acquire);
        let tail = self.region.tail_atomic().load(Ordering::Acquire);
        if head >= tail {
            return Ok(None);
        }
        let slot_index = (head % slot_count) as u32;
        let ready = self.region.slot_ready_atomic(slot_index)?;
        let seq = self.region.slot_sequence_atomic(slot_index)?;

        // Spin briefly waiting for the producer to set `ready`. A
        // claiming sender may have fetch_add'd tail past our head
        // but not yet completed the payload copy + ready store.
        let mut spin = 0u32;
        while ready.load(Ordering::Acquire) == 0 {
            spin += 1;
            if spin > SPIN_BUDGET_BEFORE_YIELD {
                // Producer is slow / stalled. Yield and come back
                // — but DON'T treat the slot as empty (head < tail
                // means a sender claimed it; we just need to wait).
                std::thread::yield_now();
                spin = 0;
                // If something changed (e.g., the slot got reclaimed
                // by a force-flush in v0.2), re-check head/tail. For
                // v0.1 we just keep waiting.
            } else {
                core::hint::spin_loop();
            }
        }

        // Sequence sanity check: the slot's stamped sequence must
        // match our expected head. If it doesn't, something has
        // gone wrong (stale slot from a wrapped writer that didn't
        // wait for ready==0, or memory corruption). Return an error
        // surface — better to surface than to deliver wrong data.
        let observed_seq = seq.load(Ordering::Acquire);
        if observed_seq != head {
            return Err(TesseraChannelError::Region(format!(
                "slot sequence mismatch at head {head}: slot[{slot_index}].sequence \
                = {observed_seq}; expected {head}. Indicates a stale write from a \
                wrapped sender that didn't observe ready=0 — should not occur \
                with the current Sender protocol. Failing fast."
            )));
        }

        // SAFETY: ready == 1 and sequence == head confirmed; the
        // producer's writes (payload + metadata) are visible to us
        // via the ready Release → Acquire pairing.
        let (length, _ts) = unsafe { self.region.read_slot_metadata(slot_index)? };
        let cap = self.region.slot_size_bytes();
        let copy_len = length.min(cap) as usize;
        let mut payload = vec![0u8; copy_len];
        unsafe {
            let src = self.region.slot_payload_ptr(slot_index)?;
            core::ptr::copy_nonoverlapping(src, payload.as_mut_ptr(), copy_len);
        }

        // Clear ready (Release: pairs with Sender's Acquire on the
        // next cycle's `ready == 0` check). Then advance head so
        // subsequent recv calls move forward.
        ready.store(0, Ordering::Release);
        self.region
            .head_atomic()
            .store(head + 1, Ordering::Release);
        Ok(Some(payload))
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
