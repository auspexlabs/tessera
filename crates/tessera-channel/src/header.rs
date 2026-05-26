//! Region layout for Tessera Channel.
//!
//! A Tessera Channel SHM region is laid out as:
//!
//! ```text
//! offset 0:                Header (HEADER_SIZE bytes, Pod)
//! offset HEADER_SIZE:      Slots — (SlotHeader + slot_size_bytes) × slot_count
//! ```
//!
//! `Header` carries the atomic `head` and `tail` counters that
//! producers and consumer use to coordinate enqueue / dequeue.
//! `SlotHeader` carries the per-slot `ready` flag and the slot's
//! sequence number (the global position the slot was claimed at).
//!
//! All structs are `repr(C)` with explicit padding so the byte
//! layout is stable across compilers / architectures, and
//! `bytemuck::Pod` so they can be reinterpreted out of mapped memory
//! without copy.
//!
//! Numeric fields are stored in native byte order. Channel is a
//! single-machine IPC primitive (the IPC namespace boundary is the
//! trust boundary per §3.5.e); we do not target cross-architecture
//! deployments. If that changes, bump `FORMAT_VERSION` and add
//! explicit `to_le_bytes` / `from_le_bytes` plumbing.

use bytemuck::{Pod, Zeroable};

/// Magic bytes at the top of every Tessera Channel region.
///
/// ASCII "TESCHANv" — verifies on attach that we're looking at a
/// Channel region (vs garbage, vs a different Tessera component,
/// vs a corrupted region after a partial init).
pub const MAGIC: u64 = u64::from_le_bytes(*b"TESCHANv");

/// Layout version. Bump on any incompatible change to `Header` or
/// `SlotHeader` shapes; attachers reject regions with a mismatched
/// version rather than risk reading garbage.
pub const FORMAT_VERSION: u32 = 1;

/// SHM region header. Stamped by the Receiver-role process (the
/// region owner) at create; read (and validated) by Sender-role
/// attachers.
///
/// Carries the head + tail counters as plain `u64`. Runtime atomic
/// access is via `AtomicU64` casts in `crate::region`, the same
/// pattern Pool / Ring use for their atomic fields. Alignment of
/// the counters is preserved because their byte offsets are
/// multiples of 8 (see `tests` below).
///
/// Repr-C with explicit padding: stable layout across compiler versions.
#[repr(C)]
#[derive(Copy, Clone, Pod, Zeroable, Debug)]
pub struct Header {
    /// Constant `MAGIC`. First field so a 0-byte region trivially
    /// fails `magic == MAGIC`.
    pub magic: u64,
    /// `FORMAT_VERSION` at region creation. Attachers reject regions
    /// where this doesn't match the linked-in constant.
    pub format_version: u32,
    /// Reserved: explicit padding to align `epoch_micros` on 8 bytes.
    pub _pad0: u32,
    /// Receiver-stamped deployment epoch (microseconds since UNIX
    /// epoch at region creation). Used to reject reattach-after-
    /// reboot scenarios where the Receiver has been restarted from
    /// a fresh deployment (§3.5.b).
    pub epoch_micros: u64,
    /// Number of slots in the region. Fixed at creation.
    pub slot_count: u32,
    /// Per-slot payload size in bytes (excludes the per-slot
    /// `SlotHeader`). Fixed at creation.
    pub slot_size_bytes: u32,
    /// Monotonic head position. Receiver advances after a successful
    /// `recv()`. Stored as `u64` in the Pod struct; runtime access
    /// is via `AtomicU64` cast.
    pub head: u64,
    /// Monotonic tail position. Senders `fetch_add(1)` to claim a
    /// slot. Stored as `u64`; runtime access via `AtomicU64` cast.
    pub tail: u64,
    /// BLAKE3(description) digest at region creation. Attachers
    /// recompute from their description and verify the match — catches
    /// the case where two consumers think they share a region but
    /// their descriptions disagree (typo, env-var drift, etc.).
    pub handle_blake3: [u8; 32],
    /// Reserved bytes for future additions without a format-version
    /// bump. Currently zeroed.
    pub _reserved: [u8; 40],
}

impl Header {
    /// On-disk size of the header in bytes. Promoted to a const so
    /// region size math is statically checkable.
    pub const SIZE: usize = core::mem::size_of::<Self>();
}

/// Per-slot header. Precedes each slot's payload bytes.
///
/// Carries the slot's claimed `sequence` (the global position the
/// claiming sender claimed via `fetch_add(tail, 1)`) and a `ready`
/// flag (set after the sender finishes copying the payload bytes;
/// cleared by the receiver after dequeue).
///
/// MPSC linearizability: the Receiver reads slots strictly in
/// `head` order. If `sequence != head` on a slot the Receiver
/// expects, the sender hasn't finalized yet — Receiver spins or
/// returns ChannelEmpty depending on the call mode. No seqlock
/// retry needed because only one Receiver ever consumes.
#[repr(C)]
#[derive(Copy, Clone, Pod, Zeroable, Debug)]
pub struct SlotHeader {
    /// Sender-claimed global position (`fetch_add(tail, 1)` result).
    /// The Receiver cross-checks this against the expected `head`
    /// value to confirm it's reading the right slot.
    pub sequence: u64,
    /// Producer-finalized flag. 0 = slot pending or empty; nonzero
    /// = sender has finished writing and the slot is consumable.
    /// Runtime access as `AtomicU64` (treated as a boolean — any
    /// nonzero means ready).
    pub ready: u64,
    /// Actual payload byte length (may be less than the slot's
    /// `slot_size_bytes` capacity).
    pub length: u32,
    /// Reserved: padding to align `timestamp_nanos` on 8 bytes.
    pub _pad0: u32,
    /// Nanoseconds since UNIX epoch at send time. Useful for latency
    /// diagnostics; not used by the state machine.
    pub timestamp_nanos: u64,
    /// Reserved bytes for future per-slot fields without a
    /// format-version bump.
    pub _reserved: [u8; 24],
}

impl SlotHeader {
    /// On-disk size of a slot header.
    pub const SIZE: usize = core::mem::size_of::<Self>();
}

/// Compute the byte offset where the slot array starts.
pub fn slot_array_offset() -> usize {
    Header::SIZE
}

/// Compute the byte stride between successive slots.
///
/// Each slot is `SlotHeader::SIZE + slot_size_bytes`.
pub fn slot_stride(slot_size_bytes: u32) -> usize {
    SlotHeader::SIZE + (slot_size_bytes as usize)
}

/// Compute the byte offset of slot `slot_index`'s SlotHeader within
/// the mapped region.
pub fn slot_header_offset(slot_index: u32, slot_size_bytes: u32) -> usize {
    slot_array_offset() + (slot_index as usize) * slot_stride(slot_size_bytes)
}

/// Compute the byte offset of slot `slot_index`'s payload within
/// the mapped region.
pub fn slot_payload_offset(slot_index: u32, slot_size_bytes: u32) -> usize {
    slot_header_offset(slot_index, slot_size_bytes) + SlotHeader::SIZE
}

/// Compute the total region size in bytes for the given slot config.
///
/// Layout: `Header :: (SlotHeader + slot_size_bytes) * slot_count`.
///
/// Returns `None` if the size would overflow `usize` — e.g. very
/// large `slot_count * slot_size_bytes`. The Channel layer surfaces
/// this as a `Config` error at construction so the caller fails fast.
pub fn region_size_bytes(slot_count: u32, slot_size_bytes: u32) -> Option<usize> {
    let stride = slot_stride(slot_size_bytes);
    let slot_table = stride.checked_mul(slot_count as usize)?;
    Header::SIZE.checked_add(slot_table)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_size_matches_documented_layout() {
        // 8 magic + 4 format_version + 4 _pad0 + 8 epoch_micros
        // + 4 slot_count + 4 slot_size_bytes + 8 head + 8 tail
        // + 32 handle_blake3 + 40 _reserved
        // = 120 bytes.
        assert_eq!(Header::SIZE, 120);
    }

    #[test]
    fn slot_header_size_matches_documented_layout() {
        // 8 sequence + 8 ready + 4 length + 4 _pad0 + 8 timestamp
        // + 24 _reserved = 56 bytes.
        assert_eq!(SlotHeader::SIZE, 56);
    }

    #[test]
    fn magic_bytes_are_ascii_marker() {
        let bytes = MAGIC.to_le_bytes();
        assert_eq!(&bytes, b"TESCHANv");
    }

    #[test]
    fn header_round_trips_through_bytes() {
        let h = Header {
            magic: MAGIC,
            format_version: FORMAT_VERSION,
            _pad0: 0,
            epoch_micros: 1_700_000_000_000_000,
            slot_count: 8,
            slot_size_bytes: 4096,
            head: 0,
            tail: 0,
            handle_blake3: [0xAB; 32],
            _reserved: [0; 40],
        };
        let bytes = bytemuck::bytes_of(&h);
        let round_tripped: &Header = bytemuck::from_bytes(bytes);
        assert_eq!(round_tripped.magic, MAGIC);
        assert_eq!(round_tripped.format_version, FORMAT_VERSION);
        assert_eq!(round_tripped.slot_count, 8);
        assert_eq!(round_tripped.slot_size_bytes, 4096);
        assert_eq!(round_tripped.handle_blake3, [0xAB; 32]);
    }

    #[test]
    fn slot_header_round_trips_through_bytes() {
        let s = SlotHeader {
            sequence: 42,
            ready: 1,
            length: 99,
            _pad0: 0,
            timestamp_nanos: 1_700_000_000_000_000_000,
            _reserved: [0; 24],
        };
        let bytes = bytemuck::bytes_of(&s);
        let round_tripped: &SlotHeader = bytemuck::from_bytes(bytes);
        assert_eq!(round_tripped.sequence, 42);
        assert_eq!(round_tripped.ready, 1);
        assert_eq!(round_tripped.length, 99);
    }

    #[test]
    fn region_size_is_header_plus_slots() {
        let n = 4_u32;
        let slot_size = 1024_u32;
        let expected = Header::SIZE + (n as usize) * (SlotHeader::SIZE + slot_size as usize);
        assert_eq!(region_size_bytes(n, slot_size), Some(expected));
    }

    #[test]
    fn region_size_overflow_returns_none() {
        // u32::MAX slots of u32::MAX bytes each → overflows usize on 64-bit.
        assert_eq!(region_size_bytes(u32::MAX, u32::MAX), None);
    }

    #[test]
    fn slot_offsets_are_disjoint_and_in_order() {
        let slot_size = 256_u32;
        assert_eq!(slot_array_offset(), Header::SIZE);
        assert_eq!(slot_header_offset(0, slot_size), Header::SIZE);
        assert_eq!(
            slot_header_offset(1, slot_size),
            Header::SIZE + SlotHeader::SIZE + slot_size as usize
        );
        // Payload follows directly after the SlotHeader within each slot.
        assert_eq!(
            slot_payload_offset(0, slot_size),
            slot_header_offset(0, slot_size) + SlotHeader::SIZE
        );
    }

    #[test]
    fn head_and_tail_offsets_are_8_aligned() {
        // Runtime atomic access via AtomicU64 cast requires 8-byte
        // alignment. The mmap base is page-aligned so the absolute
        // offsets just need to be multiples of 8.
        let h = Header::zeroed();
        let base = &h as *const Header as usize;
        let head_addr = &h.head as *const u64 as usize;
        let tail_addr = &h.tail as *const u64 as usize;
        assert_eq!(
            (head_addr - base) % 8,
            0,
            "Header.head must be 8-aligned for AtomicU64 cast"
        );
        assert_eq!(
            (tail_addr - base) % 8,
            0,
            "Header.tail must be 8-aligned for AtomicU64 cast"
        );
    }

    #[test]
    fn slot_sequence_and_ready_offsets_are_8_aligned() {
        // Same alignment story for the per-slot atomic fields.
        let s = SlotHeader::zeroed();
        let base = &s as *const SlotHeader as usize;
        let seq_addr = &s.sequence as *const u64 as usize;
        let ready_addr = &s.ready as *const u64 as usize;
        assert_eq!((seq_addr - base) % 8, 0);
        assert_eq!((ready_addr - base) % 8, 0);
    }
}
