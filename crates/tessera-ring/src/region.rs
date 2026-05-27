//! SHM region lifecycle: create / attach / unlink, plus safe accessors
//! for the global header, per-section headers, and per-slot data.
//!
//! The region is laid out per `crate::header` documentation: the
//! global header at offset 0, then a dense section-header table, then
//! per-section slot arrays in section-ordinal order. This module owns
//! the raw mapped bytes; all `unsafe` for byte-slice → typed-pointer
//! reinterpretation lives here.
//!
//! Atomic / seqlock plumbing for `writer_position` and per-slot
//! `sequence` lands in a follow-up commit (commit 3) — this commit
//! exposes the byte-copy accessors that the seqlock layer will use as
//! its sub-primitives.

use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::time::{SystemTime, UNIX_EPOCH};

use bytemuck::Zeroable;
use shared_memory::{Shmem, ShmemConf, ShmemError};

use crate::error::{Result, TesseraRingError};
use crate::header::{
    region_size_bytes, section_data_size, section_header_offset, sections_data_offset,
    slot_stride, GlobalHeader, SectionHeader, SlotHeader, FORMAT_VERSION, MAGIC,
};
use crate::namespace::NamespaceHandle;
use crate::SectionConfig;

/// One mapped Tessera Ring region. Owns the `Shmem` handle so the
/// region stays mapped until this struct is dropped.
///
/// Holds the caller-supplied section configuration plus a precomputed
/// `section_data_offsets` table so slot accessors are O(1) instead of
/// O(section_count) per call.
///
/// **Canonical section order**: regardless of the order the caller
/// passes sections to `create` / `attach`, the library stores them
/// sorted ascending by `section_id` internally. The on-disk
/// `SectionHeader` table is stamped in this canonical order too. This
/// makes two peers with the same sections-by-id interoperable even if
/// they pass the list in different orders (e.g., one passes
/// `[(0, ...), (7, ...)]` and the other passes `[(7, ...), (0, ...)]`).
/// Codex P1 fix on PR #2 commit 9d7817b.
pub struct Region {
    shmem: Shmem,
    /// Canonical (sorted-by-section_id) section list. Distinct from
    /// whatever order the caller supplied.
    sections: Vec<SectionConfig>,
    /// Maps caller-supplied `section_id` → ordinal index into the
    /// canonical `sections` Vec.
    section_id_to_ordinal: HashMap<u32, u32>,
    /// `section_data_offsets[ordinal]` is the byte offset where this
    /// section's slot array starts inside the mapped region, in
    /// canonical order.
    section_data_offsets: Vec<usize>,
    /// POSIX SHM segment name (e.g. `/tessera-ring-<hex>`). Stored so
    /// `Region::unlink` can call `shm_unlink` without re-deriving from
    /// the namespace handle. Codex P2 fix on PR #2 commit 9d7817b.
    shm_name: String,
    is_owner: bool,
    /// True once `Region::unlink()` has been called successfully.
    /// Used to short-circuit subsequent `unlink()` calls so a stale
    /// owner can't race a successor's freshly-created region with the
    /// same name. Mirrors the three-part Pool fix from PR #4 (iter-1
    /// commit b18b95a, iter-3 commit 3987833, iter-4 commit 3baf46d).
    /// Without this flag — and the matching `Shmem::set_owner(false)`
    /// + return-code-gated state mutation — A's second unlink or
    /// drop-time unlink would clobber B's name after a handoff.
    manually_unlinked: bool,
}

// SAFETY: `Region` is `!Send + !Sync` by default because `Shmem` holds a
// raw pointer. The pointer addresses a process-global mmap valid from any
// thread. Ring's slot accessors use the seqlock + atomic-position
// protocol (`crate::region` read/write helpers) that is correct under N
// concurrent writers and M concurrent readers by design — that is the
// whole point of the lossy broadcast ring. Per-reader cursor/drop state
// lives in `Reader` (not here) and is `&mut self`-guarded, so the facade
// serializes a single reader handle. Drop is a thread-agnostic
// `munmap`/`shm_unlink`. Sharing `&Region` and moving `Region` across
// threads are therefore sound; `Ring` / `Writer` / `Reader` inherit
// `Send + Sync` through their `Arc<Region>`.
unsafe impl Send for Region {}
unsafe impl Sync for Region {}

impl core::fmt::Debug for Region {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Region")
            .field("section_count", &self.sections.len())
            .field("is_owner", &self.is_owner)
            .field("len", &self.shmem.len())
            .finish()
    }
}

impl Region {
    /// Owner path: create a fresh region, stamp the global header +
    /// per-section headers, zero the slot data area.
    ///
    /// If the SHM segment already exists:
    /// - Default (`force_recreate == false`): return an error. We do
    ///   NOT inspect the existing segment, because a "looks invalid"
    ///   verdict is racy — another owner may be mid-init, having
    ///   created the segment but not yet stamped headers. Treating a
    ///   zeroed-header window as "stale" would clobber a live segment.
    ///   Operators recovering from a crashed prior owner must
    ///   explicitly pass `force_recreate=true`.
    /// - `force_recreate == true`: caller asserts no live owner.
    ///   Unconditionally unlink + recreate. Misuse is on the caller.
    pub fn create(
        handle: &NamespaceHandle,
        sections: &[SectionConfig],
        force_recreate: bool,
    ) -> Result<Self> {
        validate_section_config(sections)?;

        // Codex P1 fix on PR #2 / commit 9d7817b: canonicalize section
        // order by section_id so two peers passing the same sections in
        // different list orders still interoperate. The on-disk header
        // table is stamped in canonical order; the caller's input order
        // doesn't matter.
        let mut canonical: Vec<SectionConfig> = sections.to_vec();
        canonical.sort_by_key(|s| s.section_id());

        let pairs: Vec<(u32, u32)> = canonical
            .iter()
            .map(|s| (s.slot_count(), s.slot_size_bytes()))
            .collect();
        let size = region_size_bytes(&pairs).ok_or_else(|| {
            TesseraRingError::Config(format!(
                "region size overflow across {} sections (per-section \
                slot_count * slot_size_bytes exceeds usize::MAX). Reduce \
                slot_count or slot_size_bytes.",
                canonical.len()
            ))
        })?;
        let name = handle.shm_name();

        let shmem = match ShmemConf::new().size(size).os_id(&name).create() {
            Ok(shmem) => shmem,
            Err(ShmemError::LinkExists) | Err(ShmemError::MappingIdExists) => {
                if force_recreate {
                    // Operator-asserted recovery: no live owner exists.
                    // Unlink + recreate unconditionally. We do NOT
                    // attach-validate first — that would re-introduce
                    // the startup-race vulnerability where a fresh
                    // segment in the mid-init window looks "invalid"
                    // because its header isn't stamped yet.
                    let _ = unlink_named_region(&name);
                    ShmemConf::new()
                        .size(size)
                        .os_id(&name)
                        .create()
                        .map_err(|e| {
                            TesseraRingError::Region(format!(
                                "create after force_recreate unlink: {e}"
                            ))
                        })?
                } else {
                    return Err(TesseraRingError::Region(format!(
                        "Ring region '{name}' already exists. Refusing to clobber. \
                        Possible causes: another owner is alive (do not create a \
                        second), OR a prior owner crashed without unlinking. For \
                        recovery from a crashed owner, retry with \
                        `force_recreate=true` — but only after confirming no live \
                        owner exists, since `force_recreate` will unconditionally \
                        unlink the existing segment."
                    )));
                }
            }
            Err(e) => return Err(TesseraRingError::Region(format!("create: {e}"))),
        };

        let (section_id_to_ordinal, section_data_offsets) =
            build_section_lookup_tables(&canonical);

        let epoch_micros = current_epoch_micros();
        let mut region = Region {
            shmem,
            sections: canonical.clone(),
            section_id_to_ordinal,
            section_data_offsets,
            shm_name: name,
            is_owner: true,
            manually_unlinked: false,
        };
        region.write_global_header(handle, epoch_micros);
        for (ordinal, cfg) in canonical.iter().enumerate() {
            let sh = SectionHeader {
                section_id: cfg.section_id(),
                slot_count: cfg.slot_count(),
                slot_size_bytes: cfg.slot_size_bytes(),
                _pad0: 0,
                writer_position: 0,
                _reserved: [0; 40],
            };
            region.write_section_header(ordinal as u32, sh)?;
            // Slot data starts zeroed (Shmem create zeroes on Linux).
            // Being explicit about per-slot init lands with the
            // state-machine commit; here we trust the zero-initialized
            // mapping until then.
        }
        Ok(region)
    }

    /// Non-owner path: attach to an existing region, validate the
    /// global header magic / version / handle digest, and confirm
    /// every caller-supplied section matches the on-disk section
    /// header (by section_id and geometry).
    pub fn attach(handle: &NamespaceHandle, sections: &[SectionConfig]) -> Result<Self> {
        validate_section_config(sections)?;

        // Codex P1 fix on PR #2 / commit 9d7817b: canonicalize section
        // order by section_id. The on-disk header table is in
        // canonical order; sorting the caller's input makes the
        // ordinal-by-ordinal validation succeed regardless of input
        // ordering.
        let mut canonical: Vec<SectionConfig> = sections.to_vec();
        canonical.sort_by_key(|s| s.section_id());

        let name = handle.shm_name();
        let shmem = ShmemConf::new()
            .os_id(&name)
            .open()
            .map_err(|e| TesseraRingError::Region(format!("attach: {e}")))?;

        // Codex P1 fix on PR #2 / commit d467b14: before any raw copy
        // out of the mapped bytes, verify the attached region is at
        // least as large as our expected layout requires. Without this
        // check, attaching to a stale / corrupt / wrong-producer SHM
        // segment of the same name (e.g. a 128-byte truncated leftover)
        // would let read_global_header copy 128 bytes from a region
        // that doesn't have them — UB.
        //
        // Expected size is computed from the CALLER's section config
        // (not the on-disk header, which we haven't safely read yet).
        // If the on-disk region has a different config, the subsequent
        // global-header + section-header validation will catch the
        // semantic mismatch; this length check is purely about
        // bounds-safety before the first unsafe copy.
        let pairs: Vec<(u32, u32)> = canonical
            .iter()
            .map(|s| (s.slot_count(), s.slot_size_bytes()))
            .collect();
        let expected_size = region_size_bytes(&pairs).ok_or_else(|| {
            TesseraRingError::Config(format!(
                "region size overflow across {} sections (caller's per-section \
                slot_count * slot_size_bytes exceeds usize::MAX)",
                canonical.len()
            ))
        })?;
        if shmem.len() < expected_size {
            return Err(TesseraRingError::Region(format!(
                "attached SHM region '{name}' is smaller than expected: caller's section \
                config requires at least {expected_size} bytes (GlobalHeader {} + section \
                table + per-section slot data for {} sections), but the mapped region is \
                only {} bytes. Possible causes: stale SHM segment from a prior crashed \
                peer, wrong namespace handle, or caller's section config doesn't match the \
                creator's. Bailing out before any raw byte access (Codex P1 #2).",
                GlobalHeader::SIZE,
                canonical.len(),
                shmem.len()
            )));
        }

        let (section_id_to_ordinal, section_data_offsets) =
            build_section_lookup_tables(&canonical);
        let region = Region {
            shmem,
            sections: canonical,
            section_id_to_ordinal,
            section_data_offsets,
            shm_name: name,
            is_owner: false,
            manually_unlinked: false,
        };
        region.validate_attached_global_header(handle)?;
        region.validate_attached_section_headers()?;
        Ok(region)
    }

    /// Whether this region was opened by the creator (create) vs an
    /// attacher (open). Drop-time unlink behavior keys off this.
    pub fn is_owner(&self) -> bool {
        self.is_owner
    }

    /// Total section count.
    pub fn section_count(&self) -> u32 {
        self.sections.len() as u32
    }

    /// Caller-supplied section configuration list, in ordinal order.
    pub fn sections(&self) -> &[SectionConfig] {
        &self.sections
    }

    /// Section configuration by ordinal.
    ///
    /// Returns `Region` error if `ordinal >= section_count`.
    pub fn section_config(&self, ordinal: u32) -> Result<SectionConfig> {
        self.sections
            .get(ordinal as usize)
            .copied()
            .ok_or_else(|| {
                TesseraRingError::Region(format!(
                    "section ordinal {ordinal} out of range (section_count={})",
                    self.sections.len()
                ))
            })
    }

    /// Resolve a caller-supplied `section_id` to its ordinal.
    ///
    /// Returns `UnknownSection` if the id was not in the config used
    /// to open this Region.
    pub fn section_ordinal(&self, section_id: u32) -> Result<u32> {
        self.section_id_to_ordinal
            .get(&section_id)
            .copied()
            .ok_or_else(|| TesseraRingError::UnknownSection {
                section_id,
                configured: self.sections.iter().map(|s| s.section_id()).collect(),
            })
    }

    /// Global header epoch (microseconds since UNIX epoch at
    /// owner-side `Region::create`).
    pub fn epoch_micros(&self) -> u64 {
        self.read_global_header().epoch_micros
    }

    // --- Global header accessors -----------------------------------

    fn write_global_header(&mut self, handle: &NamespaceHandle, epoch_micros: u64) {
        let header = GlobalHeader {
            magic: MAGIC,
            format_version: FORMAT_VERSION,
            _pad0: 0,
            epoch_micros,
            section_count: self.sections.len() as u32,
            _pad1: 0,
            handle_blake3: handle.full_digest(),
            _reserved: [0; 64],
        };
        let header_bytes = bytemuck::bytes_of(&header);
        // SAFETY: we own the mapping (just created) and offset 0 +
        // GlobalHeader::SIZE is in bounds (region_size_bytes includes
        // GlobalHeader::SIZE).
        unsafe {
            let dst = self.shmem.as_ptr();
            core::ptr::copy_nonoverlapping(header_bytes.as_ptr(), dst, GlobalHeader::SIZE);
        }
    }

    pub(crate) fn read_global_header(&self) -> GlobalHeader {
        // SAFETY: offset 0 + SIZE is in bounds (region_size includes
        // GlobalHeader::SIZE); GlobalHeader is Pod so any byte pattern
        // is a valid GlobalHeader (the magic / version checks happen at
        // attach time before any of this is consulted).
        let mut header = GlobalHeader::zeroed();
        let header_bytes = bytemuck::bytes_of_mut(&mut header);
        unsafe {
            let src = self.shmem.as_ptr();
            core::ptr::copy_nonoverlapping(src, header_bytes.as_mut_ptr(), GlobalHeader::SIZE);
        }
        header
    }

    fn validate_attached_global_header(&self, handle: &NamespaceHandle) -> Result<()> {
        let header = self.read_global_header();
        if header.magic != MAGIC {
            return Err(TesseraRingError::Region(format!(
                "magic mismatch: expected {:#x}, found {:#x} (not a Tessera Ring region?)",
                MAGIC, header.magic
            )));
        }
        if header.format_version != FORMAT_VERSION {
            return Err(TesseraRingError::HeaderMismatch {
                message: "format_version mismatch".into(),
                expected_format: FORMAT_VERSION,
                found_format: header.format_version,
            });
        }
        if header.section_count != self.sections.len() as u32 {
            return Err(TesseraRingError::HeaderMismatch {
                message: format!(
                    "section_count mismatch: expected {}, found {}",
                    self.sections.len(),
                    header.section_count
                ),
                expected_format: FORMAT_VERSION,
                found_format: header.format_version,
            });
        }
        if header.handle_blake3 != handle.full_digest() {
            return Err(TesseraRingError::Region(format!(
                "handle digest mismatch on attach — your description \
                derives a different handle than the creator's; verify \
                the description string matches across processes (header_blake3 \
                in SHM differs from BLAKE3({:?}))",
                handle.shm_name()
            )));
        }
        Ok(())
    }

    // --- Section header accessors ----------------------------------

    fn check_ordinal(&self, ordinal: u32) -> Result<()> {
        if ordinal as usize >= self.sections.len() {
            return Err(TesseraRingError::Region(format!(
                "section ordinal {ordinal} out of range (section_count={})",
                self.sections.len()
            )));
        }
        Ok(())
    }

    /// Read the section header at ordinal `ordinal`.
    pub fn read_section_header(&self, ordinal: u32) -> Result<SectionHeader> {
        self.check_ordinal(ordinal)?;
        let offset = section_header_offset(ordinal);
        let mut sh = SectionHeader::zeroed();
        let sh_bytes = bytemuck::bytes_of_mut(&mut sh);
        // SAFETY: ordinal is verified in range; section_header_offset
        // is < sections_data_offset(section_count), which is < region_size.
        unsafe {
            let src = self.shmem.as_ptr().add(offset);
            core::ptr::copy_nonoverlapping(src, sh_bytes.as_mut_ptr(), SectionHeader::SIZE);
        }
        Ok(sh)
    }

    /// Write the section header at ordinal `ordinal`. Owner-only at
    /// create time; runtime `writer_position` updates go through the
    /// atomic view introduced in commit 3.
    pub fn write_section_header(&mut self, ordinal: u32, sh: SectionHeader) -> Result<()> {
        self.check_ordinal(ordinal)?;
        let offset = section_header_offset(ordinal);
        let sh_bytes = bytemuck::bytes_of(&sh);
        // SAFETY: ordinal verified; we hold &mut self so no racing
        // accessor in this process.
        unsafe {
            let dst = self.shmem.as_ptr().add(offset);
            core::ptr::copy_nonoverlapping(sh_bytes.as_ptr(), dst, SectionHeader::SIZE);
        }
        Ok(())
    }

    fn validate_attached_section_headers(&self) -> Result<()> {
        for (ordinal, cfg) in self.sections.iter().enumerate() {
            let on_disk = self.read_section_header(ordinal as u32)?;
            if on_disk.section_id != cfg.section_id()
                || on_disk.slot_count != cfg.slot_count()
                || on_disk.slot_size_bytes != cfg.slot_size_bytes()
            {
                return Err(TesseraRingError::SectionConfigMismatch {
                    section_id: cfg.section_id(),
                    expected_count: cfg.slot_count(),
                    found_count: on_disk.slot_count,
                    expected_size: cfg.slot_size_bytes(),
                    found_size: on_disk.slot_size_bytes,
                });
            }
        }
        Ok(())
    }

    // --- Slot accessors --------------------------------------------

    fn check_slot_index(&self, ordinal: u32, slot_index: u32) -> Result<()> {
        self.check_ordinal(ordinal)?;
        let cfg = self.sections[ordinal as usize];
        if slot_index >= cfg.slot_count() {
            return Err(TesseraRingError::Region(format!(
                "slot_index {slot_index} out of range for section ordinal \
                {ordinal} (slot_count={})",
                cfg.slot_count()
            )));
        }
        Ok(())
    }

    fn slot_offset(&self, ordinal: u32, slot_index: u32) -> usize {
        let cfg = self.sections[ordinal as usize];
        let section_start = self.section_data_offsets[ordinal as usize];
        section_start + (slot_index as usize) * slot_stride(cfg.slot_size_bytes())
    }

    fn slot_payload_offset(&self, ordinal: u32, slot_index: u32) -> usize {
        self.slot_offset(ordinal, slot_index) + SlotHeader::SIZE
    }

    /// Read a slot header by (ordinal, slot_index). O(1).
    pub fn read_slot_header(&self, ordinal: u32, slot_index: u32) -> Result<SlotHeader> {
        self.check_slot_index(ordinal, slot_index)?;
        let offset = self.slot_offset(ordinal, slot_index);
        let mut sh = SlotHeader::zeroed();
        let sh_bytes = bytemuck::bytes_of_mut(&mut sh);
        // SAFETY: bounds verified by check_slot_index; slot_offset is
        // within the section's data area which is < region_size.
        unsafe {
            let src = self.shmem.as_ptr().add(offset);
            core::ptr::copy_nonoverlapping(src, sh_bytes.as_mut_ptr(), SlotHeader::SIZE);
        }
        Ok(sh)
    }

    /// Write a slot header by (ordinal, slot_index). Used at
    /// owner-side init and (in commit 3) inside the seqlock-protected
    /// publish window.
    pub fn write_slot_header(
        &mut self,
        ordinal: u32,
        slot_index: u32,
        sh: SlotHeader,
    ) -> Result<()> {
        self.check_slot_index(ordinal, slot_index)?;
        let offset = self.slot_offset(ordinal, slot_index);
        let sh_bytes = bytemuck::bytes_of(&sh);
        // SAFETY: bounds verified; we hold &mut self.
        unsafe {
            let dst = self.shmem.as_ptr().add(offset);
            core::ptr::copy_nonoverlapping(sh_bytes.as_ptr(), dst, SlotHeader::SIZE);
        }
        Ok(())
    }

    /// Copy `bytes` into the payload area of slot (ordinal, slot_index).
    pub fn write_slot_payload(
        &mut self,
        ordinal: u32,
        slot_index: u32,
        bytes: &[u8],
    ) -> Result<()> {
        self.check_slot_index(ordinal, slot_index)?;
        let cfg = self.sections[ordinal as usize];
        if bytes.len() > cfg.slot_size_bytes() as usize {
            return Err(TesseraRingError::Region(format!(
                "payload size {} exceeds slot capacity {} for section ordinal {}",
                bytes.len(),
                cfg.slot_size_bytes(),
                ordinal
            )));
        }
        let offset = self.slot_payload_offset(ordinal, slot_index);
        // SAFETY: bounds verified; we hold &mut self.
        unsafe {
            let dst = self.shmem.as_ptr().add(offset);
            core::ptr::copy_nonoverlapping(bytes.as_ptr(), dst, bytes.len());
        }
        Ok(())
    }

    /// Read a copy of the first `payload_len` bytes of slot's payload.
    pub fn read_slot_payload(
        &self,
        ordinal: u32,
        slot_index: u32,
        payload_len: u32,
    ) -> Result<Vec<u8>> {
        self.check_slot_index(ordinal, slot_index)?;
        let cfg = self.sections[ordinal as usize];
        if payload_len > cfg.slot_size_bytes() {
            return Err(TesseraRingError::Region(format!(
                "payload_len {payload_len} exceeds slot capacity {} for section ordinal {ordinal}",
                cfg.slot_size_bytes()
            )));
        }
        let offset = self.slot_payload_offset(ordinal, slot_index);
        let mut out = vec![0u8; payload_len as usize];
        // SAFETY: bounds verified.
        unsafe {
            let src = self.shmem.as_ptr().add(offset);
            core::ptr::copy_nonoverlapping(src, out.as_mut_ptr(), payload_len as usize);
        }
        Ok(out)
    }

    // --- Runtime atomic / raw-pointer accessors --------------------
    //
    // The seqlock state machine (see `crate::ring`) needs:
    //   - atomic ops on `writer_position` (per section)
    //   - atomic ops on per-slot `sequence`
    //   - unsafe raw-pointer writes to slot payload + non-atomic
    //     SlotHeader fields inside the seqlock-protected window
    //
    // All of these are `&self` (not `&mut self`) because the runtime
    // path operates through `Arc<Region>` shared across threads / over
    // the Writer/Reader handles. Memory safety is provided by:
    //   * AtomicU64 for cross-thread synchronization on the sequence
    //     and writer_position counters
    //   * the seqlock protocol (odd-then-even sequence flips) ensuring
    //     readers either see a complete slot or detect mid-write and
    //     retry
    //   * verified alignment: the mmap base is page-aligned and our
    //     field offsets are 8-byte-aligned, so `&AtomicU64` casts are
    //     well-formed

    /// Byte offset of `writer_position` within `SectionHeader`.
    ///
    /// Layout: section_id(4) + slot_count(4) + slot_size_bytes(4)
    /// + _pad0(4) = 16 bytes. The test
    /// `writer_position_field_offset_matches_layout` locks this in.
    const WRITER_POSITION_FIELD_OFFSET: usize = 16;

    /// Byte offset of `sequence` within `SlotHeader`. First field, so 0.
    const SEQUENCE_FIELD_OFFSET: usize = 0;

    /// Byte offset of `position` within `SlotHeader`. After `sequence`.
    const POSITION_FIELD_OFFSET: usize = 8;

    /// Byte offset of `length` within `SlotHeader`.
    /// sequence(8) + position(8) = 16.
    const LENGTH_FIELD_OFFSET: usize = 16;

    /// Byte offset of `timestamp_nanos` within `SlotHeader`.
    /// sequence(8) + position(8) + length(4) + _pad0(4) = 24.
    const TIMESTAMP_FIELD_OFFSET: usize = 24;

    /// Atomic view of a section's `writer_position` counter.
    ///
    /// Used by `Writer::publish` to claim the next slot via fetch_add,
    /// and by `Reader::poll` to observe the latest published position.
    pub fn writer_position_atomic(&self, ordinal: u32) -> Result<&AtomicU64> {
        self.check_ordinal(ordinal)?;
        let offset = section_header_offset(ordinal) + Self::WRITER_POSITION_FIELD_OFFSET;
        // SAFETY: offset is within bounds (section_header_offset(ordinal)
        // is < sections_data_offset(section_count) by check_ordinal +
        // dense table layout; adding 16 stays inside SectionHeader::SIZE
        // == 64). Alignment: mmap base is page-aligned and offset is
        // 8-aligned (section_header_offset is 64-aligned via
        // GlobalHeader::SIZE == 128 + 64*k; adding 16 keeps 8-alignment).
        // AtomicU64 has the same layout as u64.
        unsafe {
            let ptr = self.shmem.as_ptr().add(offset) as *const AtomicU64;
            Ok(&*ptr)
        }
    }

    /// Atomic view of a slot's `sequence` counter.
    ///
    /// Used by `Writer::publish` to stamp odd-then-even, and by
    /// `Reader::poll` to check the seqlock state before / after copy.
    pub fn slot_sequence_atomic(&self, ordinal: u32, slot_index: u32) -> Result<&AtomicU64> {
        self.check_slot_index(ordinal, slot_index)?;
        let offset = self.slot_offset(ordinal, slot_index) + Self::SEQUENCE_FIELD_OFFSET;
        // SAFETY: slot_offset(ordinal, slot_index) lies inside this
        // section's data area (verified by check_slot_index).
        // Alignment guarantee chain:
        //   * mmap base is page-aligned (4096), so the segment start
        //     is way past 8-aligned.
        //   * sections_data_offset = GlobalHeader::SIZE (128) +
        //     section_count * SectionHeader::SIZE (64); both 128 and
        //     64 are multiples of 8, so sections_data_offset is
        //     8-aligned regardless of section_count.
        //   * Per-section section_data_offsets are cumulative sums of
        //     (slot_count * slot_stride). slot_stride = SlotHeader::SIZE
        //     (64) + slot_size_bytes. validate_section_config enforces
        //     slot_size_bytes % 8 == 0 (added as a Codex P1 fix on
        //     PR #2 / commit 9577c0d), so every section's data offset
        //     stays 8-aligned.
        //   * Inside a section, slot k starts at section_data_offset +
        //     k * stride; both are 8-aligned, so all slot starts are
        //     8-aligned. The sequence field is at slot offset 0, so it
        //     too is 8-aligned. AtomicU64 has the same layout as u64
        //     (size 8, alignment 8); the cast is well-formed.
        unsafe {
            let ptr = self.shmem.as_ptr().add(offset) as *const AtomicU64;
            Ok(&*ptr)
        }
    }

    /// Raw mutable pointer to the start of slot's payload area. Used
    /// by `Writer::publish` inside the seqlock-odd window to copy
    /// caller bytes.
    ///
    /// # Safety
    ///
    /// Caller must hold the seqlock-odd state on this slot's `sequence`
    /// counter so no reader sees mid-write data. `len` bytes from the
    /// returned pointer must not exceed the section's `slot_size_bytes`.
    pub unsafe fn slot_payload_ptr_mut(
        &self,
        ordinal: u32,
        slot_index: u32,
    ) -> Result<*mut u8> {
        self.check_slot_index(ordinal, slot_index)?;
        let offset = self.slot_payload_offset(ordinal, slot_index);
        // SAFETY: caller-asserted seqlock protection; offset bounds
        // verified by check_slot_index above.
        Ok(unsafe { self.shmem.as_ptr().add(offset) })
    }

    /// Raw const pointer to the start of slot's payload area. Used by
    /// `Reader::poll` inside the seqlock-check window to copy bytes
    /// out.
    ///
    /// # Safety
    ///
    /// Caller must verify (via seqlock before/after sequence check)
    /// that the slot's sequence is stable and even around the copy.
    pub unsafe fn slot_payload_ptr(
        &self,
        ordinal: u32,
        slot_index: u32,
    ) -> Result<*const u8> {
        self.check_slot_index(ordinal, slot_index)?;
        let offset = self.slot_payload_offset(ordinal, slot_index);
        Ok(unsafe { self.shmem.as_ptr().add(offset) as *const u8 })
    }

    /// Write the per-slot non-atomic header fields (`position`,
    /// `length`, `timestamp_nanos`) inside the seqlock-odd window.
    ///
    /// `sequence` is NOT touched here — the caller manages the seqlock
    /// counter via `slot_sequence_atomic` directly.
    ///
    /// # Safety
    ///
    /// Caller must hold the seqlock-odd state on the target slot.
    pub unsafe fn write_slot_header_fields(
        &self,
        ordinal: u32,
        slot_index: u32,
        position: u64,
        length: u32,
        timestamp_nanos: u64,
    ) -> Result<()> {
        self.check_slot_index(ordinal, slot_index)?;
        let slot_base = self.slot_offset(ordinal, slot_index);
        // SAFETY: slot bounds verified; caller-asserted seqlock protection.
        unsafe {
            let base = self.shmem.as_ptr().add(slot_base);
            core::ptr::write_unaligned(
                base.add(Self::POSITION_FIELD_OFFSET) as *mut u64,
                position,
            );
            core::ptr::write_unaligned(
                base.add(Self::LENGTH_FIELD_OFFSET) as *mut u32,
                length,
            );
            core::ptr::write_unaligned(
                base.add(Self::TIMESTAMP_FIELD_OFFSET) as *mut u64,
                timestamp_nanos,
            );
        }
        Ok(())
    }

    /// Read the per-slot non-atomic header fields. Used by
    /// `Reader::poll` between the before/after sequence checks.
    ///
    /// # Safety
    ///
    /// Caller must verify (via seqlock before/after sequence check)
    /// that the read window is sequence-stable.
    pub unsafe fn read_slot_header_fields(
        &self,
        ordinal: u32,
        slot_index: u32,
    ) -> Result<(u64, u32, u64)> {
        self.check_slot_index(ordinal, slot_index)?;
        let slot_base = self.slot_offset(ordinal, slot_index);
        // SAFETY: slot bounds verified; caller-asserted seqlock check
        // brackets the read window.
        unsafe {
            let base = self.shmem.as_ptr().add(slot_base);
            let position = core::ptr::read_unaligned(
                base.add(Self::POSITION_FIELD_OFFSET) as *const u64,
            );
            let length = core::ptr::read_unaligned(
                base.add(Self::LENGTH_FIELD_OFFSET) as *const u32,
            );
            let timestamp = core::ptr::read_unaligned(
                base.add(Self::TIMESTAMP_FIELD_OFFSET) as *const u64,
            );
            Ok((position, length, timestamp))
        }
    }

    /// Return the slot's configured payload capacity (for the section
    /// ordinal). Used by Writer to bounds-check caller bytes.
    pub fn slot_capacity(&self, ordinal: u32) -> Result<u32> {
        Ok(self.section_config(ordinal)?.slot_size_bytes())
    }

    /// Return the slot count for a section ordinal. Used by Writer to
    /// compute `position % slot_count` and by Reader for gap math.
    pub fn slot_count(&self, ordinal: u32) -> Result<u32> {
        Ok(self.section_config(ordinal)?.slot_count())
    }

    // --- Cleanup ---------------------------------------------------

    /// Explicit owner-side unlink of the SHM segment by name.
    ///
    /// Calling this removes the POSIX SHM name from the system; any
    /// subsequent `Region::attach` from another process will fail.
    /// Existing mappings (this `Region` and any concurrent attached
    /// peers) remain valid until they drop, because POSIX `shm_unlink`
    /// removes the name without unmapping live mappings.
    ///
    /// Idempotent: calling unlink twice (or on a name that's already
    /// gone) is safe; OS errors are swallowed because this is
    /// best-effort cleanup.
    ///
    /// Codex P2 fix on PR #2 (commit 9d7817b) — previously this was a
    /// no-op despite the "should be called by the owner at clean
    /// shutdown" docstring, which was misleading.
    ///
    /// # Restrictions
    ///
    /// Non-owners (attachers) MUST NOT call this; doing so removes the
    /// name out from under the creating owner. Returns `OwnerOnly`-shaped
    /// error... actually Ring's TesseraRingError doesn't have OwnerOnly
    /// (Ring is symmetric multi-writer/multi-reader), but we still
    /// reject attacher-side calls here as a Region error to enforce the
    /// lifecycle contract.
    pub fn unlink(&mut self) -> Result<()> {
        // Idempotent short-circuit: once we've already unlinked, do
        // nothing further. Mirrors Pool PR #4 iter-3 (commit 3987833,
        // Codex comment 3304957184) — without this short-circuit, a
        // stale owner A who calls unlink() a SECOND time after a
        // successor B has recreated the same name would call
        // libc::shm_unlink again and remove B's freshly-created name.
        if self.manually_unlinked {
            return Ok(());
        }
        if !self.is_owner {
            return Err(TesseraRingError::Region(
                "Region::unlink called by an attacher (is_owner=false). Only the \
                creator may unlink the shared-memory name. Drop this Region to \
                release the attacher's mapping; the creator decides when to unlink."
                    .into(),
            ));
        }
        #[cfg(unix)]
        {
            let cname = std::ffi::CString::new(self.shm_name.as_str()).map_err(|_| {
                TesseraRingError::Region(
                    "stored shm_name contains an interior NUL byte (cannot happen \
                    in practice — namespace handles produce hex-only names)"
                        .into(),
                )
            })?;
            // SAFETY: cname is a valid NUL-terminated C string;
            // shm_unlink is thread-safe POSIX.
            //
            // Mirrors Pool PR #4 iter-4 (commit 3baf46d, Codex
            // comment 3305006943): check the return value and only
            // flip state on success (rc == 0 or errno == ENOENT).
            // On real failure (e.g. EACCES from a uid change
            // mid-operation), return TesseraRingError::Region without
            // flipping state — caller can retry, and Drop-time
            // unlink remains active as a fallback.
            let rc = unsafe { libc::shm_unlink(cname.as_ptr()) };
            if rc != 0 {
                let err = std::io::Error::last_os_error();
                if err.raw_os_error() != Some(libc::ENOENT) {
                    return Err(TesseraRingError::Region(format!(
                        "shm_unlink('{}') failed: {} (errno={:?}). Region state \
                        flags NOT updated; caller may retry unlink(), or drop \
                        the Region to let Shmem's drop-time unlink attempt \
                        cleanup.",
                        self.shm_name,
                        err,
                        err.raw_os_error(),
                    )));
                }
                // ENOENT: name was already gone. Falls through to
                // the state-flip below as a successful unlink.
            }
        }
        // Codex P2 on PR #5 (comment 3305574604): on non-Unix
        // platforms, POSIX shm_unlink is unavailable. Without an
        // early return here, the state flip below would run
        // unconditionally — unlink() would claim success without
        // removing the OS name AND disable drop-time cleanup AND
        // block future retries, leaking the name forever. Bail out
        // explicitly so the caller knows unlink isn't supported on
        // this platform and the Region remains in a recoverable
        // state (Shmem's drop-time cleanup still active).
        #[cfg(not(unix))]
        {
            return Err(TesseraRingError::Region(
                "Region::unlink is not supported on non-Unix platforms (POSIX \
                shm_unlink unavailable). Drop the Region to let the underlying \
                shared_memory crate's drop-time cleanup attempt removal."
                    .into(),
            ));
        }
        // Below this point we're guaranteed to be on Unix AND the
        // shm_unlink call succeeded (or returned ENOENT). State flip
        // only happens once both gates are clear.
        //
        // Mirrors Pool PR #4 iter-1 (commit b18b95a, Codex comment
        // 3304769711): suppress the Shmem's drop-time unlink so a
        // handoff/restart sequence (owner A unlinks, owner B creates
        // fresh region with same name, then A finally drops) does
        // NOT have A's drop call shm_unlink AGAIN and remove B's
        // freshly-created name.
        #[cfg(unix)]
        {
            self.shmem.set_owner(false);
            // Block any future unlink() call from this Region (iter-3
            // pattern). Subsequent calls hit the early-return above.
            self.manually_unlinked = true;
        }
        Ok(())
    }
}

fn validate_section_config(sections: &[SectionConfig]) -> Result<()> {
    if sections.is_empty() {
        return Err(TesseraRingError::Config(
            "Ring requires at least one section; got empty section list".into(),
        ));
    }
    let mut seen = HashMap::new();
    for cfg in sections {
        if cfg.slot_count() == 0 {
            return Err(TesseraRingError::Config(format!(
                "section {} has slot_count == 0",
                cfg.section_id()
            )));
        }
        if cfg.slot_size_bytes() == 0 {
            return Err(TesseraRingError::Config(format!(
                "section {} has slot_size_bytes == 0",
                cfg.section_id()
            )));
        }
        // slot_size_bytes must be a multiple of 8 so that successive
        // slot starts remain 8-byte-aligned (slot_stride = 64 + size).
        // The Region layer takes `&AtomicU64` references into the
        // mapped bytes at slot.sequence offsets — misaligned references
        // are UB on strict-alignment targets and can fault on x86.
        // (Codex P1 on PR #2: enforce here rather than at the
        // unsafe cast site so the failure mode is fail-at-construction,
        // not fail-at-first-fetch-add.)
        if cfg.slot_size_bytes() % 8 != 0 {
            return Err(TesseraRingError::Config(format!(
                "section {} has slot_size_bytes={} which is not a multiple of 8; \
                slot stride must be 8-byte-aligned for AtomicU64 access on slot.sequence \
                (round up to {})",
                cfg.section_id(),
                cfg.slot_size_bytes(),
                (cfg.slot_size_bytes() + 7) & !7,
            )));
        }
        if seen.insert(cfg.section_id(), ()).is_some() {
            return Err(TesseraRingError::Config(format!(
                "duplicate section_id {} in config list",
                cfg.section_id()
            )));
        }
    }
    Ok(())
}

fn build_section_lookup_tables(
    sections: &[SectionConfig],
) -> (HashMap<u32, u32>, Vec<usize>) {
    let mut id_to_ordinal = HashMap::with_capacity(sections.len());
    let mut offsets = Vec::with_capacity(sections.len());
    let mut cursor = sections_data_offset(sections.len() as u32);
    for (ordinal, cfg) in sections.iter().enumerate() {
        id_to_ordinal.insert(cfg.section_id(), ordinal as u32);
        offsets.push(cursor);
        // section_data_size returned None in validate-overflow paths
        // earlier (via region_size_bytes); reaching here means it fits.
        cursor += section_data_size(cfg.slot_count(), cfg.slot_size_bytes())
            .expect("region_size_bytes succeeded earlier; per-section size fits");
    }
    (id_to_ordinal, offsets)
}

fn current_epoch_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}

/// Unlink a stale SHM region by name. Best-effort cleanup used when a
/// `force_recreate` create finds a leftover from a crashed prior owner.
fn unlink_named_region(name: &str) -> Result<()> {
    if let Ok(shmem) = ShmemConf::new().os_id(name).open() {
        drop(shmem);
        #[cfg(unix)]
        {
            let cname = std::ffi::CString::new(name).map_err(|_| {
                TesseraRingError::Region("region name contains NUL byte".into())
            })?;
            // SAFETY: cname is a valid NUL-terminated C string;
            // shm_unlink is thread-safe. Return value ignored on
            // purpose: best-effort cleanup.
            unsafe {
                libc::shm_unlink(cname.as_ptr());
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unique_handle(tag: &str) -> NamespaceHandle {
        let pid = std::process::id();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        NamespaceHandle::derive(&format!("tessera-ring-test/{tag}/{pid}/{nanos}"))
    }

    fn single_section(slot_count: u32, slot_size_bytes: u32) -> Vec<SectionConfig> {
        vec![SectionConfig::new(0, slot_count, slot_size_bytes)]
    }

    #[test]
    fn create_writes_valid_global_header() {
        let handle = unique_handle("global-header");
        let sections = single_section(4, 1024);
        let region = Region::create(&handle, &sections, false).expect("create");
        let h = region.read_global_header();
        assert_eq!(h.magic, MAGIC);
        assert_eq!(h.format_version, FORMAT_VERSION);
        assert_eq!(h.section_count, 1);
        assert_eq!(h.handle_blake3, handle.full_digest());
        assert!(h.epoch_micros > 0);
        assert!(region.is_owner());
    }

    #[test]
    fn create_writes_per_section_headers() {
        let handle = unique_handle("section-headers");
        let sections = vec![
            SectionConfig::new(0, 4, 1024),
            SectionConfig::new(7, 8, 512),
        ];
        let region = Region::create(&handle, &sections, false).expect("create");
        let s0 = region.read_section_header(0).expect("read 0");
        let s1 = region.read_section_header(1).expect("read 1");
        assert_eq!(s0.section_id, 0);
        assert_eq!(s0.slot_count, 4);
        assert_eq!(s0.slot_size_bytes, 1024);
        assert_eq!(s0.writer_position, 0);
        assert_eq!(s1.section_id, 7);
        assert_eq!(s1.slot_count, 8);
        assert_eq!(s1.slot_size_bytes, 512);
    }

    #[test]
    fn empty_section_list_is_rejected() {
        let handle = unique_handle("empty-sections");
        let err = Region::create(&handle, &[], false).unwrap_err();
        match err {
            TesseraRingError::Config(msg) => assert!(msg.contains("at least one section")),
            other => panic!("expected Config, got {other:?}"),
        }
    }

    #[test]
    fn duplicate_section_id_is_rejected() {
        let handle = unique_handle("duplicate-id");
        let sections = vec![
            SectionConfig::new(5, 4, 256),
            SectionConfig::new(5, 8, 512),
        ];
        let err = Region::create(&handle, &sections, false).unwrap_err();
        match err {
            TesseraRingError::Config(msg) => assert!(msg.contains("duplicate")),
            other => panic!("expected Config, got {other:?}"),
        }
    }

    #[test]
    fn zero_slot_count_is_rejected() {
        let handle = unique_handle("zero-slots");
        let sections = vec![SectionConfig::new(0, 0, 256)];
        let err = Region::create(&handle, &sections, false).unwrap_err();
        match err {
            TesseraRingError::Config(msg) => assert!(msg.contains("slot_count == 0")),
            other => panic!("expected Config, got {other:?}"),
        }
    }

    #[test]
    fn zero_slot_size_is_rejected() {
        let handle = unique_handle("zero-size");
        let sections = vec![SectionConfig::new(0, 4, 0)];
        let err = Region::create(&handle, &sections, false).unwrap_err();
        match err {
            TesseraRingError::Config(msg) => assert!(msg.contains("slot_size_bytes == 0")),
            other => panic!("expected Config, got {other:?}"),
        }
    }

    #[test]
    fn slot_size_not_multiple_of_8_is_rejected() {
        // Codex P1 fix: slot_size_bytes must be a multiple of 8 so
        // successive slot starts (and therefore slot.sequence offsets)
        // stay 8-byte-aligned for the AtomicU64 cast in
        // slot_sequence_atomic. Test the rejection path.
        for bad_size in [1u32, 7, 17, 100, 1023, 2049] {
            let handle = unique_handle(&format!("misalign-{bad_size}"));
            let sections = vec![SectionConfig::new(0, 4, bad_size)];
            let err = Region::create(&handle, &sections, false).unwrap_err();
            match err {
                TesseraRingError::Config(msg) => {
                    assert!(
                        msg.contains("not a multiple of 8"),
                        "expected alignment error for size {bad_size}, got: {msg}"
                    );
                }
                other => panic!("expected Config error for size {bad_size}, got {other:?}"),
            }
        }
        // And conversely, multiples of 8 pass: 8, 16, 64, 1024, 2048.
        for good_size in [8u32, 16, 64, 1024, 2048] {
            let handle = unique_handle(&format!("aligned-{good_size}"));
            let sections = vec![SectionConfig::new(0, 4, good_size)];
            let _ok = Region::create(&handle, &sections, false)
                .unwrap_or_else(|e| panic!("expected ok for size {good_size}, got {e:?}"));
        }
    }

    #[test]
    fn section_ordinal_maps_id_to_dense_position() {
        let handle = unique_handle("ordinal-map");
        let sections = vec![
            SectionConfig::new(10, 4, 256),
            SectionConfig::new(20, 4, 256),
            SectionConfig::new(30, 4, 256),
        ];
        let region = Region::create(&handle, &sections, false).expect("create");
        assert_eq!(region.section_ordinal(10).unwrap(), 0);
        assert_eq!(region.section_ordinal(20).unwrap(), 1);
        assert_eq!(region.section_ordinal(30).unwrap(), 2);
        match region.section_ordinal(99).unwrap_err() {
            TesseraRingError::UnknownSection {
                section_id,
                configured,
            } => {
                assert_eq!(section_id, 99);
                assert_eq!(configured, vec![10, 20, 30]);
            }
            other => panic!("expected UnknownSection, got {other:?}"),
        }
    }

    #[test]
    fn attach_reads_creators_headers() {
        let handle = unique_handle("attach-roundtrip");
        let sections = vec![
            SectionConfig::new(0, 4, 256),
            SectionConfig::new(1, 8, 128),
        ];
        let creator = Region::create(&handle, &sections, false).expect("create");
        let attacher = Region::attach(&handle, &sections).expect("attach");
        assert!(!attacher.is_owner());
        assert_eq!(attacher.epoch_micros(), creator.epoch_micros());
        let s0 = attacher.read_section_header(0).expect("read 0");
        assert_eq!(s0.section_id, 0);
        assert_eq!(s0.slot_count, 4);
        assert_eq!(s0.slot_size_bytes, 256);
        drop(attacher);
        drop(creator);
    }

    #[test]
    fn attach_rejects_section_geometry_mismatch() {
        // After Codex P1 fix on PR #2 added a bounds check that fires
        // BEFORE the semantic geometry check, this test now has to be
        // careful: the attacher's expected region size must be ≤ the
        // creator's actual region size so the bounds check passes and
        // the semantic check is what fires. Use a creator with a LARGER
        // geometry than the attacher claims, so:
        //   - creator's actual region is large enough that
        //     shmem.len() >= attacher's expected_size (bounds passes)
        //   - but on-disk SectionHeader.slot_count != attacher's slot_count
        //     (SectionConfigMismatch fires).
        let handle = unique_handle("section-geometry");
        let creator_sections = vec![SectionConfig::new(0, 8, 1024)];
        let _creator = Region::create(&handle, &creator_sections, false).expect("create");
        let attacher_sections = vec![SectionConfig::new(0, 4, 1024)];
        let err = Region::attach(&handle, &attacher_sections).unwrap_err();
        match err {
            TesseraRingError::SectionConfigMismatch {
                section_id,
                expected_count,
                found_count,
                ..
            } => {
                assert_eq!(section_id, 0);
                assert_eq!(expected_count, 4);
                assert_eq!(found_count, 8);
            }
            other => panic!("expected SectionConfigMismatch, got {other:?}"),
        }
    }

    #[test]
    fn attach_rejects_section_count_mismatch() {
        let handle = unique_handle("section-count");
        let creator_sections = vec![
            SectionConfig::new(0, 4, 256),
            SectionConfig::new(1, 4, 256),
        ];
        let _creator = Region::create(&handle, &creator_sections, false).expect("create");
        let attacher_sections = vec![SectionConfig::new(0, 4, 256)];
        let err = Region::attach(&handle, &attacher_sections).unwrap_err();
        match err {
            TesseraRingError::HeaderMismatch { message, .. } => {
                assert!(message.contains("section_count"));
            }
            other => panic!("expected HeaderMismatch, got {other:?}"),
        }
    }

    #[test]
    fn write_then_read_section_header_roundtrips() {
        let handle = unique_handle("section-header-roundtrip");
        let sections = vec![SectionConfig::new(7, 4, 256)];
        let mut region = Region::create(&handle, &sections, false).expect("create");
        let sh = SectionHeader {
            section_id: 7,
            slot_count: 4,
            slot_size_bytes: 256,
            _pad0: 0,
            writer_position: 1_234_567,
            _reserved: [0; 40],
        };
        region.write_section_header(0, sh).expect("write");
        let read = region.read_section_header(0).expect("read");
        assert_eq!(read.section_id, 7);
        assert_eq!(read.writer_position, 1_234_567);
    }

    #[test]
    fn write_then_read_slot_header_roundtrips() {
        let handle = unique_handle("slot-header-roundtrip");
        let sections = vec![SectionConfig::new(0, 4, 256)];
        let mut region = Region::create(&handle, &sections, false).expect("create");
        let sh = SlotHeader {
            sequence: 42,
            position: 1024,
            length: 99,
            _pad0: 0,
            timestamp_nanos: 1_700_000_000_000_000_000,
            _reserved: [0; 32],
        };
        region.write_slot_header(0, 2, sh).expect("write");
        let read = region.read_slot_header(0, 2).expect("read");
        assert_eq!(read.sequence, 42);
        assert_eq!(read.position, 1024);
        assert_eq!(read.length, 99);
        assert_eq!(read.timestamp_nanos, 1_700_000_000_000_000_000);
    }

    #[test]
    fn write_then_read_slot_payload_roundtrips() {
        let handle = unique_handle("slot-payload-roundtrip");
        let sections = vec![SectionConfig::new(0, 2, 64)];
        let mut region = Region::create(&handle, &sections, false).expect("create");
        let payload: Vec<u8> = (0..32).collect();
        region.write_slot_payload(0, 0, &payload).expect("write");
        let read = region.read_slot_payload(0, 0, 32).expect("read");
        assert_eq!(read, payload);
        let other = region.read_slot_payload(0, 1, 32).expect("read other");
        assert_eq!(other, vec![0u8; 32]);
    }

    #[test]
    fn slot_index_out_of_range_errors() {
        let handle = unique_handle("slot-oor");
        let sections = vec![SectionConfig::new(0, 2, 64)];
        let region = Region::create(&handle, &sections, false).expect("create");
        let err = region.read_slot_header(0, 99).unwrap_err();
        match err {
            TesseraRingError::Region(msg) => assert!(msg.contains("out of range")),
            other => panic!("expected Region error, got {other:?}"),
        }
    }

    #[test]
    fn multi_section_different_geometry_isolated_in_memory() {
        // Writes to section 0 must not bleed into section 1's slots
        // when their sizes differ. Belt-and-suspenders for the
        // section_data_offsets math.
        let handle = unique_handle("multi-section-isolation");
        let sections = vec![
            SectionConfig::new(0, 4, 64),
            SectionConfig::new(1, 4, 128),
        ];
        let mut region = Region::create(&handle, &sections, false).expect("create");
        let pat_a = vec![0xAAu8; 64];
        let pat_b = vec![0xBBu8; 128];
        region.write_slot_payload(0, 0, &pat_a).expect("write a");
        region.write_slot_payload(1, 0, &pat_b).expect("write b");
        let read_a = region.read_slot_payload(0, 0, 64).expect("read a");
        let read_b = region.read_slot_payload(1, 0, 128).expect("read b");
        assert_eq!(read_a, pat_a);
        assert_eq!(read_b, pat_b);
        // Also: section 0's slot 1 was untouched.
        let untouched = region.read_slot_payload(0, 1, 64).expect("read slot1");
        assert_eq!(untouched, vec![0u8; 64]);
    }

    #[test]
    fn cross_process_attach_via_shared_handle() {
        // Single-process simulation: creator and attacher are both in
        // this process, but the attacher only knows the handle.
        let handle = unique_handle("cross-attach");
        let sections = vec![SectionConfig::new(0, 4, 128)];
        let mut creator = Region::create(&handle, &sections, false).expect("create");
        creator
            .write_slot_payload(0, 2, b"hello attacher")
            .expect("write");

        let attacher = Region::attach(&handle, &sections).expect("attach");
        let read = attacher
            .read_slot_payload(0, 2, b"hello attacher".len() as u32)
            .expect("read");
        assert_eq!(read.as_slice(), b"hello attacher");
    }

    #[test]
    fn writer_position_field_offset_matches_layout() {
        // Locks in WRITER_POSITION_FIELD_OFFSET against any accidental
        // SectionHeader field shuffle. If you reorder SectionHeader's
        // fields, this fails first — fix the constant or the layout
        // before moving on.
        let sh = SectionHeader::zeroed();
        let base = &sh as *const SectionHeader as usize;
        let field = &sh.writer_position as *const u64 as usize;
        assert_eq!(field - base, Region::WRITER_POSITION_FIELD_OFFSET);
    }

    #[test]
    fn slot_header_field_offsets_match_layout() {
        let s = SlotHeader::zeroed();
        let base = &s as *const SlotHeader as usize;
        let sequence = &s.sequence as *const u64 as usize;
        let position = &s.position as *const u64 as usize;
        let length = &s.length as *const u32 as usize;
        let timestamp = &s.timestamp_nanos as *const u64 as usize;
        assert_eq!(sequence - base, Region::SEQUENCE_FIELD_OFFSET);
        assert_eq!(position - base, Region::POSITION_FIELD_OFFSET);
        assert_eq!(length - base, Region::LENGTH_FIELD_OFFSET);
        assert_eq!(timestamp - base, Region::TIMESTAMP_FIELD_OFFSET);
    }

    #[test]
    fn writer_position_atomic_starts_at_zero_and_supports_fetch_add() {
        use std::sync::atomic::Ordering;
        let handle = unique_handle("writer-pos-atomic");
        let sections = vec![SectionConfig::new(0, 4, 64)];
        let region = Region::create(&handle, &sections, false).expect("create");
        let pos = region.writer_position_atomic(0).expect("atomic view");
        assert_eq!(pos.load(Ordering::SeqCst), 0);
        let first = pos.fetch_add(1, Ordering::SeqCst);
        let second = pos.fetch_add(1, Ordering::SeqCst);
        assert_eq!(first, 0);
        assert_eq!(second, 1);
        assert_eq!(pos.load(Ordering::SeqCst), 2);
        // Visible to a separate attach to the same region.
        let attacher = Region::attach(&handle, &sections).expect("attach");
        assert_eq!(
            attacher
                .writer_position_atomic(0)
                .expect("attacher view")
                .load(Ordering::SeqCst),
            2
        );
    }

    #[test]
    fn slot_sequence_atomic_starts_at_zero() {
        use std::sync::atomic::Ordering;
        let handle = unique_handle("slot-seq-atomic");
        let sections = vec![SectionConfig::new(0, 4, 64)];
        let region = Region::create(&handle, &sections, false).expect("create");
        for i in 0..4 {
            let seq = region.slot_sequence_atomic(0, i).expect("seq view");
            assert_eq!(seq.load(Ordering::SeqCst), 0);
        }
    }

    #[test]
    fn unsafe_payload_ptr_writes_visible_via_read_slot_payload() {
        let handle = unique_handle("payload-ptr");
        let sections = vec![SectionConfig::new(0, 2, 32)];
        let region = Region::create(&handle, &sections, false).expect("create");
        let data = b"hello via raw ptr";
        // SAFETY: single-threaded test; no concurrent reader.
        unsafe {
            let dst = region.slot_payload_ptr_mut(0, 1).expect("ptr");
            core::ptr::copy_nonoverlapping(data.as_ptr(), dst, data.len());
        }
        let read = region
            .read_slot_payload(0, 1, data.len() as u32)
            .expect("read");
        assert_eq!(read.as_slice(), data);
    }

    #[test]
    fn write_then_read_slot_header_fields_via_unsafe_path() {
        let handle = unique_handle("header-fields-unsafe");
        let sections = vec![SectionConfig::new(0, 2, 64)];
        let region = Region::create(&handle, &sections, false).expect("create");
        // SAFETY: single-threaded test; caller-asserted seqlock window.
        unsafe {
            region
                .write_slot_header_fields(0, 1, 42, 7, 1_700_000_000_000_000_000)
                .expect("write");
            let (pos, len, ts) = region.read_slot_header_fields(0, 1).expect("read");
            assert_eq!(pos, 42);
            assert_eq!(len, 7);
            assert_eq!(ts, 1_700_000_000_000_000_000);
        }
    }

    #[test]
    fn force_recreate_clobbers_existing_region() {
        let handle = unique_handle("force-recreate");
        let sections = vec![SectionConfig::new(0, 4, 64)];
        let _first = Region::create(&handle, &sections, false).expect("create 1");
        // Without force_recreate, a second create on the same handle
        // refuses.
        let refused = Region::create(&handle, &sections, false);
        assert!(matches!(refused, Err(TesseraRingError::Region(_))));
        // With force_recreate it succeeds.
        let _second = Region::create(&handle, &sections, true).expect("force_recreate");
    }

    #[test]
    fn creator_and_attacher_with_different_list_order_interoperate() {
        // Codex P1 fix on PR #2 commit 9d7817b: section order in the
        // caller's input list must not affect interop. Two peers using
        // the same (section_id, slot_count, slot_size) tuples but
        // passing them in different orders should attach successfully.
        let handle = unique_handle("section-order-interop");
        let creator_sections = vec![
            SectionConfig::new(0, 4, 256),
            SectionConfig::new(7, 8, 128),
            SectionConfig::new(3, 4, 64),
        ];
        let mut creator = Region::create(&handle, &creator_sections, false).expect("create");

        // Attacher passes the same sections in REVERSED order.
        let attacher_sections = vec![
            SectionConfig::new(3, 4, 64),
            SectionConfig::new(7, 8, 128),
            SectionConfig::new(0, 4, 256),
        ];
        let attacher = Region::attach(&handle, &attacher_sections).expect("attach");

        // Both should resolve section_id 7 to the same canonical
        // ordinal (the one stamped on disk), and both should be able
        // to read/write the same slots.
        let creator_ord = creator.section_ordinal(7).unwrap();
        let attacher_ord = attacher.section_ordinal(7).unwrap();
        assert_eq!(
            creator_ord, attacher_ord,
            "section_id 7 must map to the same canonical ordinal on both sides"
        );

        // Cross-process round-trip: creator writes section 7, attacher reads it back.
        creator.write_slot_payload(creator_ord, 0, b"hello via reordered config").unwrap();
        let read = attacher
            .read_slot_payload(attacher_ord, 0, b"hello via reordered config".len() as u32)
            .unwrap();
        assert_eq!(read.as_slice(), b"hello via reordered config");
    }

    #[test]
    fn unlink_removes_shm_name_for_owner() {
        // Codex P2 fix on PR #2 commit 9d7817b: Region::unlink must
        // actually call shm_unlink for owners (not be a documented
        // no-op). Verify by calling unlink, then trying to attach from
        // a fresh attacher — the attempt must fail because the SHM
        // name is gone.
        let handle = unique_handle("explicit-unlink");
        let sections = vec![SectionConfig::new(0, 4, 64)];
        let mut owner = Region::create(&handle, &sections, false).expect("create");
        // Before unlink: attach succeeds (region is reachable by name).
        let _attacher = Region::attach(&handle, &sections).expect("attach pre-unlink");
        // Explicit unlink.
        owner.unlink().expect("unlink");
        // After unlink: a fresh attacher cannot find the name.
        let post_attach = Region::attach(&handle, &sections);
        assert!(
            post_attach.is_err(),
            "expected attach to fail after explicit unlink, got Ok"
        );
        // Idempotent: second unlink is safe.
        owner.unlink().expect("second unlink");
    }

    #[test]
    fn unlink_disables_drop_time_unlink_for_handoff_safety() {
        // Mirrors Pool PR #4 iter-1 fix (commit b18b95a, Codex
        // comment 3304769711) applied here to Ring.
        //
        // Sequence under test:
        //   1. A creates region X (name N).
        //   2. A unlinks N (name removed; A's drop suppressed via set_owner(false)).
        //   3. B creates a fresh region with name N (succeeds — A's
        //      unlink cleared the name in step 2).
        //   4. Drop A. With the fix, A's drop does NOT shm_unlink, so
        //      B's name survives.
        //   5. Attach to B by name — must succeed.
        let handle = unique_handle("handoff-no-clobber");
        let sections_a = vec![SectionConfig::new(0, 2, 64)];
        let sections_b = vec![SectionConfig::new(0, 4, 128)];
        let mut owner_a = Region::create(&handle, &sections_a, false).expect("A create");
        owner_a.unlink().expect("A unlink");

        let owner_b =
            Region::create(&handle, &sections_b, false).expect("B create after A unlink");

        drop(owner_a);

        let attacher = Region::attach(&handle, &sections_b).expect("attach to B after A's drop");
        assert_eq!(attacher.section_count(), 1);
        assert_eq!(attacher.section_config(0).unwrap().slot_count(), 4);
        drop(attacher);
        drop(owner_b);
    }

    #[test]
    fn stale_owners_second_unlink_does_not_clobber_successor() {
        // Mirrors Pool PR #4 iter-3 fix (commit 3987833, Codex
        // comment 3304957184) applied here to Ring.
        //
        // The set_owner(false) fix protects against A's *drop*
        // clobbering B's name. This test verifies the complementary
        // `manually_unlinked` short-circuit: A's *explicit second
        // unlink()* must also be a no-op so it can't race a
        // successor's freshly-created region.
        //
        // Sequence:
        //   1. A creates region (name N).
        //   2. A.unlink() — name N removed; flag set.
        //   3. B creates fresh region with name N.
        //   4. A.unlink() AGAIN — must be a true no-op (no
        //      libc::shm_unlink, no Shmem mutation).
        //   5. Attach to B — must succeed.
        let handle = unique_handle("stale-double-unlink");
        let sections_a = vec![SectionConfig::new(0, 2, 64)];
        let sections_b = vec![SectionConfig::new(0, 4, 128)];
        let mut owner_a = Region::create(&handle, &sections_a, false).expect("A create");
        owner_a.unlink().expect("A first unlink");

        let owner_b =
            Region::create(&handle, &sections_b, false).expect("B create after A unlink");

        owner_a
            .unlink()
            .expect("A second unlink should be a no-op");

        let attacher =
            Region::attach(&handle, &sections_b).expect("attach B after A's stale 2nd unlink");
        assert_eq!(attacher.section_config(0).unwrap().slot_count(), 4);
        drop(attacher);

        drop(owner_a);
        let attacher2 =
            Region::attach(&handle, &sections_b).expect("attach B after A's drop");
        drop(attacher2);
        drop(owner_b);
    }

    #[test]
    fn unlink_rejects_attacher_calls() {
        // Only the creator may unlink. Attacher-side unlink would yank
        // the name out from under the live creator — that's an API
        // misuse the lifecycle contract forbids.
        let handle = unique_handle("attacher-unlink-rejected");
        let sections = vec![SectionConfig::new(0, 4, 64)];
        let _creator = Region::create(&handle, &sections, false).expect("create");
        let mut attacher = Region::attach(&handle, &sections).expect("attach");
        let err = attacher.unlink().unwrap_err();
        match err {
            TesseraRingError::Region(msg) => {
                assert!(
                    msg.contains("attacher") || msg.contains("Only the creator"),
                    "expected attacher-rejection error, got: {msg}"
                );
            }
            other => panic!("expected Region error, got {other:?}"),
        }
    }

    #[test]
    fn attach_rejects_undersized_region() {
        // Codex P1 fix on PR #2 / commit d467b14: attach must check
        // shmem.len() against the caller's expected region size before
        // any raw byte copy. Verify by creating a region with one
        // section config (small expected size) and attaching with a
        // section config requiring more bytes — the mapping is the
        // SAME size, so the attached length is too small for the
        // attacher's expectation, and attach must fail with a clear
        // bounds error rather than reading past the end.
        let handle = unique_handle("undersized");
        let creator_sections = vec![SectionConfig::new(0, 1, 8)];
        let _creator = Region::create(&handle, &creator_sections, false).expect("create");
        // Attacher requires section config 100x larger.
        let attacher_sections = vec![SectionConfig::new(0, 100, 1024)];
        let err = Region::attach(&handle, &attacher_sections).unwrap_err();
        match err {
            TesseraRingError::Region(msg) => {
                assert!(
                    msg.contains("smaller than expected") || msg.contains("Bailing out"),
                    "expected size-mismatch bounds error, got: {msg}"
                );
            }
            other => panic!("expected Region bounds error, got {other:?}"),
        }
    }
}
