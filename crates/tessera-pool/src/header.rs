//! Region layout.
//!
//! A Tessera Pool SHM region is laid out as:
//!
//! ```text
//! offset 0:              Header (HEADER_SIZE bytes, Plain-Old-Data)
//! offset HEADER_SIZE:    SlotMeta * slot_count
//! offset payload_start:  raw bytes; slot i lives at i * slot_size_bytes
//! ```
//!
//! All structs are `repr(C)` with explicit padding so the byte layout is
//! stable across compilers / architectures, and `bytemuck::Pod` so they
//! can be reinterpreted out of the mapped memory without copy.
//!
//! Numeric fields are stored in native byte order. Tessera Pool is a
//! single-machine IPC primitive (the IPC namespace boundary is the trust
//! boundary per §3.5.e); we do not target cross-architecture
//! deployments. If that changes, bump `FORMAT_VERSION` and add
//! explicit `to_le_bytes` / `from_le_bytes` plumbing.

use bytemuck::{Pod, Zeroable};

/// Magic bytes at the top of every Tessera Pool region.
///
/// ASCII "TESPOOLv" — verifies on attach that we're looking at a Pool
/// region (vs garbage, vs a different Tessera component, vs a corrupted
/// region after a partial init).
pub const MAGIC: u64 = u64::from_le_bytes(*b"TESPOOLv");

/// Layout version. Bump on any incompatible change to `Header` or
/// `SlotMeta` shapes; attachers reject regions with a mismatched
/// version rather than risk reading garbage.
pub const FORMAT_VERSION: u32 = 1;

/// Slot flag bits.
pub mod flags {
    /// Slot currently leased (between acquire and release/reclaim).
    pub const IN_USE: u32 = 1 << 0;
    /// `Pool::write` has been called on this lease (one-shot per §3.4 lock).
    pub const PAYLOAD_FINALIZED: u32 = 1 << 1;
}

/// SHM region header. Stamped by the owner at region creation; read
/// (and validated) by non-owner attachers.
///
/// Repr-C with explicit padding: stable layout across compiler versions.
#[repr(C)]
#[derive(Copy, Clone, Pod, Zeroable, Debug)]
pub struct Header {
    /// Constant `MAGIC`. First field so a 0-byte region trivially fails
    /// `magic == MAGIC`.
    pub magic: u64,
    /// `FORMAT_VERSION` at region creation. Attachers reject regions
    /// where this doesn't match the linked-in constant.
    pub format_version: u32,
    /// Reserved: explicit padding to align `epoch` on 8 bytes.
    pub _pad0: u32,
    /// Owner-stamped deployment epoch (microseconds since UNIX epoch at
    /// region creation). Used to reject reattach-after-reboot scenarios
    /// where the owner has been restarted from a fresh deployment (§3.5.b).
    pub epoch_micros: u64,
    /// Number of slots in the region. Fixed at creation.
    pub slot_count: u32,
    /// Per-slot payload size in bytes. Fixed at creation.
    pub slot_size_bytes: u32,
    /// Time-to-live for in-flight leases, in microseconds. Owner stamps
    /// this at creation; non-owner attachers inherit it from the header
    /// and cannot override locally (§3.5.d).
    pub ttl_micros: u64,
    /// BLAKE3(description) digest at region creation. Attachers
    /// recompute from their description and verify the match — catches
    /// the case where two consumers think they share a region but their
    /// descriptions disagree (typo, env-var drift, etc.).
    pub handle_blake3: [u8; 32],
    /// Reserved bytes for future additions without a format-version
    /// bump. Currently zeroed.
    pub _reserved: [u8; 56],
}

impl Header {
    /// On-disk size of the header in bytes. Promoted to a const so
    /// region size math is statically checkable.
    pub const SIZE: usize = core::mem::size_of::<Self>();
}

/// Per-slot metadata. One entry per slot, packed after the header.
///
/// Lives in SHM so cross-container attachers can observe lease state
/// (validation only — only the owner mutates per §3.5.c single-writer-lease).
#[repr(C)]
#[derive(Copy, Clone, Pod, Zeroable, Debug)]
pub struct SlotMeta {
    /// Lease identifier, high 64 bits. Combined with `lease_id_low`
    /// forms a 128-bit ID; descriptor validation requires both halves
    /// to match.
    pub lease_id_high: u64,
    /// Lease identifier, low 64 bits.
    pub lease_id_low: u64,
    /// Generation counter. Incremented on `Pool::acquire` and on
    /// `Pool::reclaim_stale`. Stale descriptors (held by a worker that
    /// missed a reclaim) carry an out-of-date generation and fail
    /// validation — preventing them from corrupting a re-leased slot.
    pub generation: u64,
    /// Owner-side monotonic time at acquire, in microseconds. Used by
    /// the reclaim sweep to identify TTL-expired leases.
    pub acquired_at_micros: u64,
    /// Payload size in bytes after `Pool::write`. Zero before write.
    pub payload_len: u32,
    /// `flags::IN_USE` and `flags::PAYLOAD_FINALIZED` bits.
    pub flags: u32,
    /// Reserved bytes for future per-slot fields (e.g. eviction priority,
    /// typed-view marker) without a format-version bump.
    pub _reserved: [u8; 32],
}

impl SlotMeta {
    /// On-disk size of a slot metadata entry.
    pub const SIZE: usize = core::mem::size_of::<Self>();

    /// True if the slot currently holds an active lease.
    pub fn in_use(&self) -> bool {
        (self.flags & flags::IN_USE) != 0
    }

    /// True if `Pool::write` has been called on the current lease.
    pub fn payload_finalized(&self) -> bool {
        (self.flags & flags::PAYLOAD_FINALIZED) != 0
    }
}

/// Compute the total region size in bytes for the given slot config.
///
/// Layout: `Header :: SlotMeta * slot_count :: payload(slot_size_bytes * slot_count)`.
///
/// Returns `None` if the size would overflow `usize` — e.g. very large
/// `slot_count * slot_size_bytes`. The Pool layer surfaces this as a
/// `Config` error at construction so the caller fails fast rather than
/// getting a confusing OS-level shm_open failure on a usize-wrapped
/// size.
pub fn region_size_bytes(slot_count: u32, slot_size_bytes: u32) -> Option<usize> {
    let n = slot_count as usize;
    let slot_size = slot_size_bytes as usize;
    let slot_table = SlotMeta::SIZE.checked_mul(n)?;
    let payload = slot_size.checked_mul(n)?;
    Header::SIZE.checked_add(slot_table)?.checked_add(payload)
}

/// Offset (from region start) where the slot-metadata table begins.
pub fn slot_table_offset() -> usize {
    Header::SIZE
}

/// Offset where slot `i`'s metadata entry starts.
pub fn slot_meta_offset(slot_index: u32) -> usize {
    slot_table_offset() + (slot_index as usize) * SlotMeta::SIZE
}

/// Offset where the payload area begins, given a slot count.
pub fn payload_area_offset(slot_count: u32) -> usize {
    slot_table_offset() + (slot_count as usize) * SlotMeta::SIZE
}

/// Offset where slot `i`'s payload bytes start.
pub fn slot_payload_offset(slot_index: u32, slot_count: u32, slot_size_bytes: u32) -> usize {
    payload_area_offset(slot_count) + (slot_index as usize) * (slot_size_bytes as usize)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_size_matches_documented_layout() {
        // Header is sized so on-disk layout is stable. If you change
        // it deliberately, bump FORMAT_VERSION. If you change it by
        // accident, this test catches the drift before it ships.
        assert_eq!(Header::SIZE, 128);
    }

    #[test]
    fn slot_meta_size_matches_documented_layout() {
        // 16 (lease_id) + 8 (generation) + 8 (acquired_at) + 4 (payload_len)
        // + 4 (flags) + 32 (_reserved) = 72.
        assert_eq!(SlotMeta::SIZE, 72);
    }

    #[test]
    fn magic_bytes_are_ascii_marker() {
        // Decodes back to "TESPOOLv" for human inspection of crash dumps.
        let bytes = MAGIC.to_le_bytes();
        assert_eq!(&bytes, b"TESPOOLv");
    }

    #[test]
    fn header_round_trips_through_bytes() {
        let h = Header {
            magic: MAGIC,
            format_version: FORMAT_VERSION,
            _pad0: 0,
            epoch_micros: 1_700_000_000_000_000,
            slot_count: 8,
            slot_size_bytes: 64 * 1024 * 1024,
            ttl_micros: 60_000_000,
            handle_blake3: [0xAB; 32],
            _reserved: [0; 56],
        };
        let bytes = bytemuck::bytes_of(&h);
        let round_tripped: &Header = bytemuck::from_bytes(bytes);
        assert_eq!(round_tripped.magic, MAGIC);
        assert_eq!(round_tripped.format_version, FORMAT_VERSION);
        assert_eq!(round_tripped.slot_count, 8);
        assert_eq!(round_tripped.slot_size_bytes, 64 * 1024 * 1024);
        assert_eq!(round_tripped.ttl_micros, 60_000_000);
        assert_eq!(round_tripped.handle_blake3, [0xAB; 32]);
    }

    #[test]
    fn region_size_is_header_plus_slot_table_plus_payload() {
        let n = 4_u32;
        let slot_size = 1024_u32;
        let expected = Header::SIZE + (n as usize) * SlotMeta::SIZE + (n as usize) * (slot_size as usize);
        assert_eq!(region_size_bytes(n, slot_size), Some(expected));
    }

    #[test]
    fn region_size_overflow_returns_none() {
        // Pathological config: u32::MAX slots of u32::MAX bytes each.
        // The multiplication overflows usize on 64-bit; checked path
        // returns None instead of wrapping into a tiny value that
        // would silently allocate a too-small SHM segment.
        assert_eq!(region_size_bytes(u32::MAX, u32::MAX), None);
    }

    #[test]
    fn slot_offsets_are_disjoint_and_in_order() {
        let n = 3_u32;
        let slot_size = 256_u32;
        // Slot metas are contiguous after the header.
        assert_eq!(slot_meta_offset(0), Header::SIZE);
        assert_eq!(slot_meta_offset(1), Header::SIZE + SlotMeta::SIZE);
        assert_eq!(slot_meta_offset(2), Header::SIZE + SlotMeta::SIZE * 2);
        // Payload area starts after the slot table.
        assert_eq!(payload_area_offset(n), Header::SIZE + (n as usize) * SlotMeta::SIZE);
        // Per-slot payload offsets are stride * index from the payload area start.
        assert_eq!(slot_payload_offset(0, n, slot_size), payload_area_offset(n));
        assert_eq!(
            slot_payload_offset(1, n, slot_size),
            payload_area_offset(n) + slot_size as usize
        );
    }

    #[test]
    fn slot_meta_flag_helpers_match_bit_definitions() {
        let mut meta = SlotMeta::zeroed();
        assert!(!meta.in_use());
        assert!(!meta.payload_finalized());
        meta.flags = flags::IN_USE;
        assert!(meta.in_use());
        assert!(!meta.payload_finalized());
        meta.flags = flags::IN_USE | flags::PAYLOAD_FINALIZED;
        assert!(meta.in_use());
        assert!(meta.payload_finalized());
    }
}
