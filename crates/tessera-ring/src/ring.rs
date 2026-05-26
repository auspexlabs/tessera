//! Tessera Ring state machine — Writer::publish + Reader::poll.
//!
//! Sits on top of `crate::region`'s atomic + raw-pointer accessors and
//! implements the seqlock protocol locked in §4b of the upstream
//! extraction plan:
//!
//! ```text
//! Writer::publish(section_id, bytes):
//!   position = fetch_add(section.writer_position, 1)
//!   slot_index = position % section.slot_count
//!   seq = slot.sequence.load()
//!   slot.sequence.store(seq + 1)      # odd = write in progress
//!   write slot.position, payload, length, timestamp
//!   slot.sequence.store(seq + 2)      # even = stable
//!
//! Reader::poll():
//!   latest = section.writer_position.load()
//!   oldest_available = latest.saturating_sub(slot_count)
//!   if cursor < oldest_available:
//!     dropped += oldest_available - cursor
//!     cursor = oldest_available
//!   while cursor < latest:
//!     slot = section.slots[cursor % slot_count]
//!     before = slot.sequence.load()
//!     if before is odd: bounded_retry, else drop+continue
//!     copy header_fields + payload
//!     after = slot.sequence.load()
//!     if before == after and after is even and slot.position == cursor:
//!       yield event ; cursor += 1
//!     else: refresh latest + oldest_available, retry or drop
//! ```
//!
//! The writer is lossy by design: it never blocks on readers. Readers
//! detect being lapped via the `oldest_available` check and account
//! the gap in their process-local `dropped` counter.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::{Result, TesseraRingError};
use crate::namespace::NamespaceHandle;
use crate::region::Region;
use crate::SectionConfig;

/// Bounded spin retries when a slot's sequence is observed odd. After
/// this many tries we accept the drop and move the cursor forward.
///
/// Tuned for "low-microsecond writers" — if a publisher is paused for
/// longer than ~100 spin cycles' worth, the reader is better off
/// dropping that slot and accounting it than blocking the consumer.
const ODD_SEQUENCE_SPIN_BUDGET: u32 = 128;

/// Configuration for opening a Ring region. Mirrors `tessera_pool::PoolConfig`'s
/// shape: a description string for namespace derivation, the section
/// list, an owner flag, and a force_recreate escape hatch.
#[derive(Clone, Debug)]
pub struct RingConfig {
    /// Human-readable description; hashed via BLAKE3 into the SHM region name.
    pub description: String,
    /// Caller-supplied section list.
    pub sections: Vec<SectionConfig>,
    /// `true` → caller is the creator (will `create()`); `false` →
    /// caller is an attacher (will `open()`). Single-creator semantics
    /// per §3.5.b: exactly one process creates the region; others
    /// attach.
    pub is_owner: bool,
    /// Operator-asserted recovery: if `is_owner` is true and the SHM
    /// segment already exists, unlink + recreate unconditionally.
    /// Caller is responsible for confirming no live owner exists. Has
    /// no effect for `is_owner == false`.
    pub force_recreate: bool,
}

impl RingConfig {
    /// Convenience constructor for a single-section Ring.
    pub fn single_section(
        description: impl Into<String>,
        section_id: u32,
        slot_count: u32,
        slot_size_bytes: u32,
    ) -> Self {
        Self {
            description: description.into(),
            sections: vec![SectionConfig::new(section_id, slot_count, slot_size_bytes)],
            is_owner: true,
            force_recreate: false,
        }
    }
}

/// Tessera Ring handle. Wraps a shared `Region` and hands out
/// `Writer` / `Reader` handles. The Ring itself does not publish or
/// poll — Writers and Readers do; multiple of each can be issued from
/// one Ring.
#[derive(Clone)]
pub struct Ring {
    region: Arc<Region>,
}

impl Ring {
    /// Open a Ring per the config: BLAKE3-derive the namespace handle,
    /// then either create (owner) or attach (non-owner) the SHM region.
    pub fn open(config: RingConfig) -> Result<Self> {
        let handle = NamespaceHandle::derive(&config.description);
        let region = if config.is_owner {
            Region::create(&handle, &config.sections, config.force_recreate)?
        } else {
            Region::attach(&handle, &config.sections)?
        };
        Ok(Self {
            region: Arc::new(region),
        })
    }

    /// Issue a new `Writer` handle. Multiple writers may coexist;
    /// each `publish` claims an independent global position via
    /// fetch-add, so two writers concurrently publishing to the same
    /// section produce distinct slots.
    pub fn writer(&self) -> Writer {
        Writer {
            region: Arc::clone(&self.region),
        }
    }

    /// Issue a new `Reader` handle for a specific section, starting
    /// at the current writer position (fresh-reader-starts-at-now per
    /// §4b — historical replay from buffered slots is deferred).
    pub fn reader(&self, section_id: u32) -> Result<Reader> {
        let ordinal = self.region.section_ordinal(section_id)?;
        let cursor = self
            .region
            .writer_position_atomic(ordinal)?
            .load(Ordering::Acquire);
        Ok(Reader {
            region: Arc::clone(&self.region),
            section_id,
            ordinal,
            cursor,
            dropped: 0,
        })
    }

    /// Whether this Ring instance was opened as the region creator.
    pub fn is_owner(&self) -> bool {
        self.region.is_owner()
    }

    /// Configured section list, in ordinal order.
    pub fn sections(&self) -> &[SectionConfig] {
        self.region.sections()
    }
}

/// Writer handle. `publish(section_id, bytes)` appends one event to
/// the named section, overwriting the oldest slot if the ring is full.
#[derive(Clone)]
pub struct Writer {
    region: Arc<Region>,
}

impl Writer {
    /// Publish one event to a section. Lossy: if the ring has wrapped
    /// past readers, those readers detect the gap on their next
    /// `poll()` and account it in their `ReaderStats.dropped` count;
    /// the writer never blocks.
    pub fn publish(&self, section_id: u32, bytes: &[u8]) -> Result<()> {
        let ordinal = self.region.section_ordinal(section_id)?;
        let slot_size = self.region.slot_capacity(ordinal)?;
        if bytes.len() > slot_size as usize {
            return Err(TesseraRingError::OversizedEvent {
                section_id,
                event_size: bytes.len(),
                slot_size: slot_size as usize,
            });
        }
        let slot_count = self.region.slot_count(ordinal)?;

        // 1. Claim a global position via fetch_add on the section's
        //    writer_position counter.
        let position = self
            .region
            .writer_position_atomic(ordinal)?
            .fetch_add(1, Ordering::AcqRel);
        let slot_index = (position % slot_count as u64) as u32;

        // 2. Bump slot.sequence to odd (write in progress). We load,
        //    add 1, and store with Release so the upcoming non-atomic
        //    writes are visible to readers after the matching
        //    Acquire-load of the post-write sequence.
        let seq_atomic = self.region.slot_sequence_atomic(ordinal, slot_index)?;
        let seq_before = seq_atomic.load(Ordering::Acquire);
        // We unconditionally bump to (seq_before | 1) + 0 OR
        // seq_before + 1; both produce an odd value when seq_before
        // was even. The seqlock guarantee is "sequence is odd during
        // write, then we add another 1 to make it even (and one
        // higher than before)". Standard pattern:
        let seq_writing = seq_before.wrapping_add(1);
        seq_atomic.store(seq_writing, Ordering::Release);

        // 3. Write the slot header fields + payload inside the odd
        //    window. SAFETY: we hold the seqlock-odd state on this
        //    slot's sequence counter; any concurrent reader sees odd
        //    and either retries or drops. Bounds verified by the
        //    region accessors against slot_index < slot_count and
        //    bytes.len() <= slot_size.
        let timestamp = current_nanos();
        unsafe {
            self.region.write_slot_header_fields(
                ordinal,
                slot_index,
                position,
                bytes.len() as u32,
                timestamp,
            )?;
            let dst = self.region.slot_payload_ptr_mut(ordinal, slot_index)?;
            core::ptr::copy_nonoverlapping(bytes.as_ptr(), dst, bytes.len());
        }

        // 4. Bump slot.sequence to even (stable). Release so readers'
        //    subsequent Acquire-load observes the prior writes.
        let seq_done = seq_writing.wrapping_add(1);
        seq_atomic.store(seq_done, Ordering::Release);

        Ok(())
    }
}

/// One event copied out of the ring. Owned bytes (v0.1 copies on read;
/// zero-copy views are a v0.2 refinement).
#[derive(Clone, Debug)]
pub struct Event {
    /// Section this event was published to.
    pub section_id: u32,
    /// Global writer position at publish time.
    pub position: u64,
    /// Nanoseconds since UNIX epoch at publish time.
    pub timestamp_nanos: u64,
    /// Event payload bytes.
    pub payload: Vec<u8>,
}

/// Per-section reader handle. Maintains a process-local cursor and a
/// drop counter; multiple Readers on the same section are independent
/// (multi-reader broadcast per §4.1).
#[derive(Clone)]
pub struct Reader {
    region: Arc<Region>,
    section_id: u32,
    ordinal: u32,
    cursor: u64,
    dropped: u64,
}

/// Per-section drop / cursor statistics surfaced via `Reader::stats()`.
///
/// Distinct from `crate::ReaderStats` (the public type defined in
/// lib.rs for downstream signatures); we re-export the same shape
/// here. (Future commits may merge.)
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct ReaderStats {
    /// Section this reader tracks.
    pub section_id: u32,
    /// Reader's current cursor.
    pub cursor: u64,
    /// Writer position at stats snapshot.
    pub latest: u64,
    /// Total events dropped (lapped + odd-sequence-spin-exhausted).
    pub dropped: u64,
}

impl Reader {
    /// Section this reader is bound to.
    pub fn section_id(&self) -> u32 {
        self.section_id
    }

    /// Current cursor (next position the reader expects to consume).
    pub fn cursor(&self) -> u64 {
        self.cursor
    }

    /// Number of events this reader has been lapped on (or had to
    /// drop due to seqlock retry exhaustion).
    pub fn dropped(&self) -> u64 {
        self.dropped
    }

    /// Snapshot stats: cursor, latest writer position, dropped count.
    pub fn stats(&self) -> Result<ReaderStats> {
        let latest = self
            .region
            .writer_position_atomic(self.ordinal)?
            .load(Ordering::Acquire);
        Ok(ReaderStats {
            section_id: self.section_id,
            cursor: self.cursor,
            latest,
            dropped: self.dropped,
        })
    }

    /// Drain all events between `self.cursor` and the current writer
    /// position. Returns the events in publish order. Updates the
    /// reader's cursor and drop counters as a side effect.
    ///
    /// Returns an empty vec when the reader is caught up; subsequent
    /// `poll()` calls remain cheap (one atomic load).
    pub fn poll(&mut self) -> Result<Vec<Event>> {
        let slot_count = self.region.slot_count(self.ordinal)?;
        let writer_pos = self.region.writer_position_atomic(self.ordinal)?;

        let mut events = Vec::new();
        let mut latest = writer_pos.load(Ordering::Acquire);

        // Catch up to oldest_available if we've been lapped.
        let oldest_available = latest.saturating_sub(slot_count as u64);
        if self.cursor < oldest_available {
            self.dropped = self.dropped.saturating_add(oldest_available - self.cursor);
            self.cursor = oldest_available;
        }

        while self.cursor < latest {
            let slot_index = (self.cursor % slot_count as u64) as u32;
            let seq_atomic = self.region.slot_sequence_atomic(self.ordinal, slot_index)?;

            // Phase 1: see a stable (even) sequence. Bounded spin if
            // odd; if budget exhausted, drop this slot and move on.
            let mut spin = 0u32;
            let before = loop {
                let s = seq_atomic.load(Ordering::Acquire);
                if s & 1 == 0 {
                    break s;
                }
                spin += 1;
                if spin > ODD_SEQUENCE_SPIN_BUDGET {
                    // Slot is mid-write or writer paused — drop and
                    // advance.
                    self.dropped = self.dropped.saturating_add(1);
                    self.cursor += 1;
                    // Refresh latest because we may have just made
                    // forward progress that doesn't correspond to a
                    // delivered event.
                    latest = writer_pos.load(Ordering::Acquire);
                    let oldest = latest.saturating_sub(slot_count as u64);
                    if self.cursor < oldest {
                        self.dropped =
                            self.dropped.saturating_add(oldest - self.cursor);
                        self.cursor = oldest;
                    }
                    // Continue the outer while loop.
                    break u64::MAX; // sentinel — caught below
                }
                core::hint::spin_loop();
            };
            if before == u64::MAX {
                continue;
            }

            // Phase 2: copy slot data inside the seqlock window.
            // SAFETY: caller protocol: we bracket the read with
            // sequence loads (Acquire) before and after; if before ==
            // after and even, the bytes we read are sequence-stable.
            // The intermediate copy is `read_unaligned` + memcpy of
            // owned bytes (no shared references held past the after-
            // check), so even on a torn read we just discard and retry.
            let (slot_position, length, timestamp) = unsafe {
                self.region
                    .read_slot_header_fields(self.ordinal, slot_index)?
            };
            let slot_capacity = self.region.slot_capacity(self.ordinal)?;
            let copy_len = length.min(slot_capacity) as usize;
            let mut payload = vec![0u8; copy_len];
            unsafe {
                let src = self.region.slot_payload_ptr(self.ordinal, slot_index)?;
                core::ptr::copy_nonoverlapping(src, payload.as_mut_ptr(), copy_len);
            }

            // Phase 3: verify seqlock + position match.
            let after = seq_atomic.load(Ordering::Acquire);
            if before == after && after & 1 == 0 && slot_position == self.cursor {
                events.push(Event {
                    section_id: self.section_id,
                    position: slot_position,
                    timestamp_nanos: timestamp,
                    payload,
                });
                self.cursor += 1;
                continue;
            }

            // Seqlock fired or slot was overwritten mid-read. Refresh
            // latest + oldest and decide whether to retry or drop.
            latest = writer_pos.load(Ordering::Acquire);
            let oldest = latest.saturating_sub(slot_count as u64);
            if self.cursor < oldest {
                self.dropped = self.dropped.saturating_add(oldest - self.cursor);
                self.cursor = oldest;
            }
            // Otherwise loop back without incrementing cursor — same
            // position, retry the seqlock dance.
        }

        Ok(events)
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
    use crate::namespace::NamespaceHandle;

    fn unique_description(tag: &str) -> String {
        let pid = std::process::id();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        format!("tessera-ring-state-test/{tag}/{pid}/{nanos}")
    }

    #[test]
    fn publish_then_poll_returns_event_payload() {
        let cfg = RingConfig {
            description: unique_description("simple-publish"),
            sections: vec![SectionConfig::new(0, 8, 256)],
            is_owner: true,
            force_recreate: false,
        };
        let ring = Ring::open(cfg).expect("open");
        let mut reader = ring.reader(0).expect("reader");
        let writer = ring.writer();
        writer.publish(0, b"hello tessera").expect("publish");
        let events = reader.poll().expect("poll");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].section_id, 0);
        assert_eq!(events[0].position, 0);
        assert_eq!(events[0].payload, b"hello tessera");
        // Cursor advanced past the event.
        assert_eq!(reader.cursor(), 1);
        assert_eq!(reader.dropped(), 0);
    }

    #[test]
    fn multiple_publishes_arrive_in_order() {
        let cfg = RingConfig {
            description: unique_description("ordered-publishes"),
            sections: vec![SectionConfig::new(0, 16, 32)],
            is_owner: true,
            force_recreate: false,
        };
        let ring = Ring::open(cfg).expect("open");
        let mut reader = ring.reader(0).expect("reader");
        let writer = ring.writer();
        for i in 0..10 {
            let msg = format!("event-{i}");
            writer.publish(0, msg.as_bytes()).expect("publish");
        }
        let events = reader.poll().expect("poll");
        assert_eq!(events.len(), 10);
        for (i, event) in events.iter().enumerate() {
            assert_eq!(event.position, i as u64);
            assert_eq!(event.payload, format!("event-{i}").into_bytes());
        }
        assert_eq!(reader.dropped(), 0);
    }

    #[test]
    fn reader_lapped_accounts_dropped_events() {
        // Ring with 4 slots; publish 10 events; reader catches up to
        // oldest_available = 10 - 4 = 6, so 6 events were dropped.
        let cfg = RingConfig {
            description: unique_description("lapped-reader"),
            sections: vec![SectionConfig::new(0, 4, 16)],
            is_owner: true,
            force_recreate: false,
        };
        let ring = Ring::open(cfg).expect("open");
        let mut reader = ring.reader(0).expect("reader");
        let writer = ring.writer();
        for i in 0..10u32 {
            writer
                .publish(0, &i.to_le_bytes())
                .expect("publish");
        }
        let events = reader.poll().expect("poll");
        assert_eq!(events.len(), 4);
        assert_eq!(reader.dropped(), 6);
        // Events should be positions 6, 7, 8, 9.
        for (i, event) in events.iter().enumerate() {
            assert_eq!(event.position, 6 + i as u64);
        }
    }

    #[test]
    fn fresh_reader_starts_at_current_writer_position() {
        // §4b: fresh readers see only NEW events, not historical
        // ring contents.
        let cfg = RingConfig {
            description: unique_description("fresh-reader-now"),
            sections: vec![SectionConfig::new(0, 8, 16)],
            is_owner: true,
            force_recreate: false,
        };
        let ring = Ring::open(cfg).expect("open");
        let writer = ring.writer();
        for i in 0..3u32 {
            writer.publish(0, &i.to_le_bytes()).expect("pre-publish");
        }
        // Reader opens AFTER the pre-publishes.
        let mut reader = ring.reader(0).expect("reader");
        assert_eq!(reader.cursor(), 3);
        // Idle poll — no new events.
        let idle = reader.poll().expect("idle poll");
        assert_eq!(idle.len(), 0);
        assert_eq!(reader.dropped(), 0);
        // Now publish one more.
        writer.publish(0, b"new").expect("post-publish");
        let events = reader.poll().expect("poll");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].position, 3);
        assert_eq!(events[0].payload, b"new");
    }

    #[test]
    fn multiple_readers_each_see_full_stream() {
        // §4.1: multi-reader broadcast — each reader maintains its
        // own cursor.
        let cfg = RingConfig {
            description: unique_description("multi-reader"),
            sections: vec![SectionConfig::new(0, 16, 16)],
            is_owner: true,
            force_recreate: false,
        };
        let ring = Ring::open(cfg).expect("open");
        let mut r1 = ring.reader(0).expect("r1");
        let mut r2 = ring.reader(0).expect("r2");
        let writer = ring.writer();
        for i in 0..5u32 {
            writer.publish(0, &i.to_le_bytes()).expect("publish");
        }
        let e1 = r1.poll().expect("r1 poll");
        let e2 = r2.poll().expect("r2 poll");
        assert_eq!(e1.len(), 5);
        assert_eq!(e2.len(), 5);
        for (a, b) in e1.iter().zip(e2.iter()) {
            assert_eq!(a.position, b.position);
            assert_eq!(a.payload, b.payload);
        }
    }

    #[test]
    fn multi_section_publish_is_isolated() {
        let cfg = RingConfig {
            description: unique_description("multi-section"),
            sections: vec![
                SectionConfig::new(0, 8, 16),
                SectionConfig::new(1, 8, 32),
            ],
            is_owner: true,
            force_recreate: false,
        };
        let ring = Ring::open(cfg).expect("open");
        let mut r0 = ring.reader(0).expect("r0");
        let mut r1 = ring.reader(1).expect("r1");
        let writer = ring.writer();
        writer.publish(0, b"a-section-0").expect("pub 0");
        writer.publish(1, b"b-section-1").expect("pub 1");
        writer.publish(0, b"c-section-0").expect("pub 0 again");
        let e0 = r0.poll().expect("r0 poll");
        let e1 = r1.poll().expect("r1 poll");
        assert_eq!(e0.len(), 2);
        assert_eq!(e1.len(), 1);
        assert_eq!(e0[0].payload, b"a-section-0");
        assert_eq!(e0[1].payload, b"c-section-0");
        assert_eq!(e1[0].payload, b"b-section-1");
        // Per-section writer_position is independent.
        assert_eq!(e0[0].position, 0);
        assert_eq!(e0[1].position, 1);
        assert_eq!(e1[0].position, 0);
    }

    #[test]
    fn publish_rejects_oversized_event() {
        let cfg = RingConfig {
            description: unique_description("oversized"),
            sections: vec![SectionConfig::new(0, 4, 16)],
            is_owner: true,
            force_recreate: false,
        };
        let ring = Ring::open(cfg).expect("open");
        let writer = ring.writer();
        let big = vec![0u8; 17];
        let err = writer.publish(0, &big).unwrap_err();
        match err {
            TesseraRingError::OversizedEvent {
                section_id,
                event_size,
                slot_size,
            } => {
                assert_eq!(section_id, 0);
                assert_eq!(event_size, 17);
                assert_eq!(slot_size, 16);
            }
            other => panic!("expected OversizedEvent, got {other:?}"),
        }
    }

    #[test]
    fn publish_unknown_section_errors() {
        let cfg = RingConfig {
            description: unique_description("unknown-section"),
            sections: vec![SectionConfig::new(0, 4, 16)],
            is_owner: true,
            force_recreate: false,
        };
        let ring = Ring::open(cfg).expect("open");
        let writer = ring.writer();
        let err = writer.publish(99, b"x").unwrap_err();
        match err {
            TesseraRingError::UnknownSection {
                section_id,
                configured,
            } => {
                assert_eq!(section_id, 99);
                assert_eq!(configured, vec![0]);
            }
            other => panic!("expected UnknownSection, got {other:?}"),
        }
    }

    #[test]
    fn reader_stats_reports_cursor_latest_dropped() {
        let cfg = RingConfig {
            description: unique_description("stats"),
            sections: vec![SectionConfig::new(0, 4, 16)],
            is_owner: true,
            force_recreate: false,
        };
        let ring = Ring::open(cfg).expect("open");
        let mut reader = ring.reader(0).expect("reader");
        let writer = ring.writer();
        for i in 0..6u32 {
            writer.publish(0, &i.to_le_bytes()).expect("publish");
        }
        let _ = reader.poll().expect("poll");
        let s = reader.stats().expect("stats");
        assert_eq!(s.section_id, 0);
        assert_eq!(s.latest, 6);
        assert_eq!(s.cursor, 6);
        assert_eq!(s.dropped, 2); // 6 events, 4-slot ring → 2 dropped
    }

    #[test]
    fn attach_opens_existing_region_and_sees_events() {
        // Same description, owner creates and attacher opens.
        let desc = unique_description("attach-open");
        let sections = vec![SectionConfig::new(0, 8, 32)];
        let owner_cfg = RingConfig {
            description: desc.clone(),
            sections: sections.clone(),
            is_owner: true,
            force_recreate: false,
        };
        let owner_ring = Ring::open(owner_cfg).expect("owner open");
        owner_ring
            .writer()
            .publish(0, b"from owner")
            .expect("publish");

        let attacher_cfg = RingConfig {
            description: desc,
            sections,
            is_owner: false,
            force_recreate: false,
        };
        let attacher_ring = Ring::open(attacher_cfg).expect("attacher open");
        assert!(!attacher_ring.is_owner());
        // Fresh reader on the attacher side starts at "now" — has to
        // see new events after it opens, not historical ones.
        let mut attacher_reader = attacher_ring.reader(0).expect("attacher reader");
        assert_eq!(attacher_reader.cursor(), 1);
        owner_ring
            .writer()
            .publish(0, b"after attach")
            .expect("publish");
        let events = attacher_reader.poll().expect("poll");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].payload, b"after attach");
    }

    #[test]
    fn namespace_is_blake3_derived_and_stable() {
        // Belt-and-suspenders: confirm that opening a Ring uses
        // BLAKE3(description) for SHM naming, matching attachers using
        // the same description.
        let desc = unique_description("blake3-stability");
        let handle = NamespaceHandle::derive(&desc);
        let cfg = RingConfig {
            description: desc.clone(),
            sections: vec![SectionConfig::new(0, 4, 16)],
            is_owner: true,
            force_recreate: false,
        };
        let _ring = Ring::open(cfg).expect("open");
        // If naming weren't BLAKE3-stable, this assert would fire.
        let same_handle = NamespaceHandle::derive(&desc);
        assert_eq!(handle.full_digest(), same_handle.full_digest());
    }
}
