//! Region layout for Tessera Ring.
//!
//! A Tessera Ring SHM region is laid out as:
//!
//! ```text
//! offset 0:                    GlobalHeader (HEADER_SIZE bytes, Plain-Old-Data)
//! offset HEADER_SIZE:          SectionHeader * section_count
//! offset sections_data_offset: per-section slot arrays, in section_id order;
//!                              each slot is SlotHeader + slot_size_bytes payload.
//! ```
//!
//! Per-section geometry (slot_count, slot_size_bytes) is stamped into
//! each `SectionHeader` at region creation. Attachers verify their
//! configured geometry matches the stamped values per section, not
//! globally — different sections may have different shapes.
//!
//! All structs are `repr(C)` with explicit padding so the byte layout is
//! stable across compilers / architectures, and `bytemuck::Pod` so they
//! can be reinterpreted out of the mapped memory without copy.
//!
//! Numeric fields are stored in native byte order. Tessera Ring is a
//! single-machine IPC primitive; the IPC namespace boundary is the
//! trust boundary. We do not target cross-architecture deployments. If
//! that changes, bump `FORMAT_VERSION` and add explicit `to_le_bytes` /
//! `from_le_bytes` plumbing.
//!
//! ### Seqlock model
//!
//! Tessera Ring uses **per-slot seqlocks**. Each `SlotHeader` carries
//! its own `sequence` counter; writers stamp odd-then-even to publish;
//! readers spin until they see the same even sequence before and after
//! copy.

use bytemuck::{Pod, Zeroable};

/// Magic bytes at the top of every Tessera Ring region.
///
/// ASCII "TESRINGv" — verifies on attach that we're looking at a Ring
/// region (vs garbage, vs a different Tessera component, vs a corrupted
/// region after a partial init).
pub const MAGIC: u64 = u64::from_le_bytes(*b"TESRINGv");

/// Layout version. Bump on any incompatible change to `GlobalHeader`,
/// `SectionHeader`, or `SlotHeader` shapes; attachers reject regions
/// with a mismatched version rather than risk reading garbage.
pub const FORMAT_VERSION: u32 = 1;

/// Global region header. Stamped by the region creator; read (and
/// validated) by attachers.
///
/// Repr-C with explicit padding: stable layout across compiler versions.
#[repr(C)]
#[derive(Copy, Clone, Pod, Zeroable, Debug)]
pub struct GlobalHeader {
    /// Constant `MAGIC`. First field so a 0-byte region trivially fails
    /// `magic == MAGIC`.
    pub magic: u64,
    /// `FORMAT_VERSION` at region creation. Attachers reject regions
    /// where this doesn't match the linked-in constant.
    pub format_version: u32,
    /// Reserved: explicit padding to align `epoch_micros` on 8 bytes.
    pub _pad0: u32,
    /// Region creator's deployment epoch (microseconds since UNIX epoch
    /// at region creation). Used to reject reattach-after-reboot
    /// scenarios where the creator has been restarted from a fresh
    /// deployment.
    pub epoch_micros: u64,
    /// Number of sections in the region. Fixed at creation.
    pub section_count: u32,
    /// Reserved: explicit padding to align `handle_blake3` on 8 bytes
    /// (32-byte array alignment doesn't require it, but explicit is
    /// clearer than implicit).
    pub _pad1: u32,
    /// BLAKE3(description) digest at region creation. Attachers
    /// recompute from their description and verify the match — catches
    /// the case where two consumers think they share a region but their
    /// descriptions disagree (typo, env-var drift, etc.).
    pub handle_blake3: [u8; 32],
    /// Reserved bytes for future additions without a format-version
    /// bump. Currently zeroed.
    pub _reserved: [u8; 64],
}

impl GlobalHeader {
    /// On-disk size of the global header in bytes. Promoted to a const
    /// so region size math is statically checkable.
    pub const SIZE: usize = core::mem::size_of::<Self>();
}

/// Per-section header. One entry per section, packed after the
/// `GlobalHeader` in section_id order matching the caller's
/// configuration.
///
/// Lives in SHM so writers / readers can observe per-section geometry
/// and the writer position without coordination. `writer_position` is
/// the atomic monotonic counter used by `Writer::publish` to claim the
/// next slot index via fetch-add; readers consult it to detect new
/// events and gap-detect lapped state.
#[repr(C)]
#[derive(Copy, Clone, Pod, Zeroable, Debug)]
pub struct SectionHeader {
    /// Caller-supplied section identifier. Sections are addressed by id
    /// at the public API, not by ordinal position.
    pub section_id: u32,
    /// Slot count for this section. Fixed at creation.
    pub slot_count: u32,
    /// Per-slot payload size in bytes for this section. Fixed at
    /// creation. Does NOT include the `SlotHeader`.
    pub slot_size_bytes: u32,
    /// Reserved: padding to align `writer_position` on 8 bytes.
    pub _pad0: u32,
    /// Atomic monotonic writer position. Each `Writer::publish` does
    /// `fetch_add(1)` on this counter; the resulting position modulo
    /// `slot_count` is the slot index. Readers track their own cursor
    /// against this value (process-local; not in SHM).
    pub writer_position: u64,
    /// Reserved bytes for future per-section fields (e.g. drop counters
    /// observable across the IPC boundary, eviction policy markers)
    /// without a format-version bump.
    pub _reserved: [u8; 40],
}

impl SectionHeader {
    /// On-disk size of a section header.
    pub const SIZE: usize = core::mem::size_of::<Self>();
}

/// Per-slot header. Precedes each slot's payload bytes.
///
/// Per-slot seqlock: writers stamp `sequence` odd before
/// modifying payload, then even after — readers spin until they see
/// the same even sequence before-and-after their copy. `position`
/// carries the global writer position at write time so readers can
/// confirm they read the slot they intended (vs a wrapped overwrite
/// during their read).
#[repr(C)]
#[derive(Copy, Clone, Pod, Zeroable, Debug)]
pub struct SlotHeader {
    /// Seqlock counter. Even = stable / readable; odd = write in
    /// progress. Atomic in concurrent contexts; declared here as a
    /// plain u64 because `bytemuck::Pod` doesn't admit atomics — the
    /// access layer uses `AtomicU64` views into the same bytes.
    pub sequence: u64,
    /// Global writer position at the time this slot was published.
    /// Readers cross-check this against their expected position to
    /// detect mid-read overwrite.
    pub position: u64,
    /// Actual payload byte length. May be less than the section's
    /// `slot_size_bytes`.
    pub length: u32,
    /// Reserved: padding to align `timestamp_nanos` on 8 bytes.
    pub _pad0: u32,
    /// Nanoseconds since UNIX epoch at publish time.
    pub timestamp_nanos: u64,
    /// Reserved bytes for future per-slot fields (e.g. event-type tag,
    /// truncation flag, sampling metadata) without a format-version bump.
    pub _reserved: [u8; 32],
}

impl SlotHeader {
    /// On-disk size of a slot header.
    pub const SIZE: usize = core::mem::size_of::<Self>();
}

/// Compute the byte offset where the section-header table starts.
pub fn section_table_offset() -> usize {
    GlobalHeader::SIZE
}

/// Compute the byte offset where the entry for section ordinal `i`
/// lives within the section-header table.
///
/// "Ordinal" here is the position in the caller's configured section
/// list, not the section_id value. Section_id-to-ordinal mapping is
/// the access-layer's responsibility; the on-disk table is dense.
pub fn section_header_offset(ordinal: u32) -> usize {
    section_table_offset() + (ordinal as usize) * SectionHeader::SIZE
}

/// Compute the byte offset where per-section slot data starts, given
/// the total number of configured sections.
pub fn sections_data_offset(section_count: u32) -> usize {
    section_table_offset() + (section_count as usize) * SectionHeader::SIZE
}

/// Compute the byte stride between successive slots within a section.
///
/// Each slot is `SlotHeader::SIZE + slot_size_bytes`.
pub fn slot_stride(slot_size_bytes: u32) -> usize {
    SlotHeader::SIZE + (slot_size_bytes as usize)
}

/// Compute the byte size of one section's slot array: stride * slot_count.
///
/// Returns `None` on overflow.
pub fn section_data_size(slot_count: u32, slot_size_bytes: u32) -> Option<usize> {
    slot_stride(slot_size_bytes).checked_mul(slot_count as usize)
}

/// Compute the total region size in bytes for the given list of
/// per-section (slot_count, slot_size_bytes) configurations.
///
/// Layout:
/// `GlobalHeader :: SectionHeader * section_count :: sum(section_data_size for each section)`.
///
/// Returns `None` if any intermediate multiplication or sum would
/// overflow `usize`. The Ring layer surfaces this as a `Config` error
/// at construction so the caller fails fast rather than getting a
/// confusing OS-level shm_open failure on a usize-wrapped size.
pub fn region_size_bytes(sections: &[(u32, u32)]) -> Option<usize> {
    let section_count = u32::try_from(sections.len()).ok()?;
    let mut total = sections_data_offset(section_count);
    for &(slot_count, slot_size_bytes) in sections {
        let section_bytes = section_data_size(slot_count, slot_size_bytes)?;
        total = total.checked_add(section_bytes)?;
    }
    Some(total)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn global_header_size_matches_documented_layout() {
        // 8 (magic) + 4 (format_version) + 4 (_pad0) + 8 (epoch_micros)
        // + 4 (section_count) + 4 (_pad1) + 32 (handle_blake3) + 64 (_reserved)
        // = 128 bytes. If you change it deliberately, bump FORMAT_VERSION.
        // If you change it by accident, this test catches the drift.
        assert_eq!(GlobalHeader::SIZE, 128);
    }

    #[test]
    fn section_header_size_matches_documented_layout() {
        // 4 (section_id) + 4 (slot_count) + 4 (slot_size_bytes) + 4 (_pad0)
        // + 8 (writer_position) + 40 (_reserved) = 64 bytes.
        assert_eq!(SectionHeader::SIZE, 64);
    }

    #[test]
    fn slot_header_size_matches_documented_layout() {
        // 8 (sequence) + 8 (position) + 4 (length) + 4 (_pad0)
        // + 8 (timestamp_nanos) + 32 (_reserved) = 64 bytes.
        assert_eq!(SlotHeader::SIZE, 64);
    }

    #[test]
    fn magic_bytes_are_ascii_marker() {
        // Decodes back to "TESRINGv" for human inspection of crash dumps.
        let bytes = MAGIC.to_le_bytes();
        assert_eq!(&bytes, b"TESRINGv");
    }

    #[test]
    fn global_header_round_trips_through_bytes() {
        let h = GlobalHeader {
            magic: MAGIC,
            format_version: FORMAT_VERSION,
            _pad0: 0,
            epoch_micros: 1_700_000_000_000_000,
            section_count: 3,
            _pad1: 0,
            handle_blake3: [0xAB; 32],
            _reserved: [0; 64],
        };
        let bytes = bytemuck::bytes_of(&h);
        let round_tripped: &GlobalHeader = bytemuck::from_bytes(bytes);
        assert_eq!(round_tripped.magic, MAGIC);
        assert_eq!(round_tripped.format_version, FORMAT_VERSION);
        assert_eq!(round_tripped.section_count, 3);
        assert_eq!(round_tripped.handle_blake3, [0xAB; 32]);
        assert_eq!(round_tripped.epoch_micros, 1_700_000_000_000_000);
    }

    #[test]
    fn section_header_round_trips_through_bytes() {
        let s = SectionHeader {
            section_id: 7,
            slot_count: 4096,
            slot_size_bytes: 2048,
            _pad0: 0,
            writer_position: 0,
            _reserved: [0; 40],
        };
        let bytes = bytemuck::bytes_of(&s);
        let round_tripped: &SectionHeader = bytemuck::from_bytes(bytes);
        assert_eq!(round_tripped.section_id, 7);
        assert_eq!(round_tripped.slot_count, 4096);
        assert_eq!(round_tripped.slot_size_bytes, 2048);
        assert_eq!(round_tripped.writer_position, 0);
    }

    #[test]
    fn slot_header_round_trips_through_bytes() {
        let s = SlotHeader {
            sequence: 42,
            position: 1234,
            length: 99,
            _pad0: 0,
            timestamp_nanos: 1_700_000_000_000_000_000,
            _reserved: [0; 32],
        };
        let bytes = bytemuck::bytes_of(&s);
        let round_tripped: &SlotHeader = bytemuck::from_bytes(bytes);
        assert_eq!(round_tripped.sequence, 42);
        assert_eq!(round_tripped.position, 1234);
        assert_eq!(round_tripped.length, 99);
        assert_eq!(round_tripped.timestamp_nanos, 1_700_000_000_000_000_000);
    }

    #[test]
    fn section_table_layout_is_dense_and_in_order() {
        // Section ordinals 0..N are contiguous, packed after the global header.
        assert_eq!(section_table_offset(), GlobalHeader::SIZE);
        assert_eq!(section_header_offset(0), GlobalHeader::SIZE);
        assert_eq!(
            section_header_offset(1),
            GlobalHeader::SIZE + SectionHeader::SIZE
        );
        assert_eq!(
            section_header_offset(2),
            GlobalHeader::SIZE + SectionHeader::SIZE * 2
        );
    }

    #[test]
    fn sections_data_offset_is_after_table() {
        assert_eq!(sections_data_offset(0), GlobalHeader::SIZE);
        assert_eq!(
            sections_data_offset(3),
            GlobalHeader::SIZE + 3 * SectionHeader::SIZE
        );
    }

    #[test]
    fn slot_stride_includes_header_plus_payload() {
        assert_eq!(slot_stride(0), SlotHeader::SIZE);
        assert_eq!(slot_stride(2048), SlotHeader::SIZE + 2048);
        assert_eq!(slot_stride(64 * 1024), SlotHeader::SIZE + 64 * 1024);
    }

    #[test]
    fn section_data_size_is_stride_times_count() {
        assert_eq!(section_data_size(0, 2048), Some(0));
        assert_eq!(
            section_data_size(4, 2048),
            Some(4 * (SlotHeader::SIZE + 2048))
        );
    }

    #[test]
    fn section_data_size_overflow_returns_none() {
        // Pathological: u32::MAX slots of u32::MAX bytes each.
        assert_eq!(section_data_size(u32::MAX, u32::MAX), None);
    }

    #[test]
    fn region_size_is_header_plus_section_table_plus_per_section_data() {
        let sections = [(4_u32, 1024_u32), (8_u32, 512_u32)];
        let expected = GlobalHeader::SIZE
            + 2 * SectionHeader::SIZE
            + 4 * (SlotHeader::SIZE + 1024)
            + 8 * (SlotHeader::SIZE + 512);
        assert_eq!(region_size_bytes(&sections), Some(expected));
    }

    #[test]
    fn region_size_empty_sections_is_header_only() {
        assert_eq!(region_size_bytes(&[]), Some(GlobalHeader::SIZE));
    }

    #[test]
    fn region_size_overflow_returns_none() {
        // Pathological: u32::MAX slots of u32::MAX bytes each, in a single section.
        let sections = [(u32::MAX, u32::MAX)];
        assert_eq!(region_size_bytes(&sections), None);
    }
}
