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
pub struct Region {
    shmem: Shmem,
    sections: Vec<SectionConfig>,
    /// Maps caller-supplied `section_id` → ordinal index into `sections`.
    /// `section_id` need not be dense (0..N); the on-disk table is.
    section_id_to_ordinal: HashMap<u32, u32>,
    /// `section_data_offsets[ordinal]` is the byte offset where this
    /// section's slot array starts inside the mapped region.
    section_data_offsets: Vec<usize>,
    is_owner: bool,
}

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

        let pairs: Vec<(u32, u32)> = sections
            .iter()
            .map(|s| (s.slot_count(), s.slot_size_bytes()))
            .collect();
        let size = region_size_bytes(&pairs).ok_or_else(|| {
            TesseraRingError::Config(format!(
                "region size overflow across {} sections (per-section \
                slot_count * slot_size_bytes exceeds usize::MAX). Reduce \
                slot_count or slot_size_bytes.",
                sections.len()
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
            build_section_lookup_tables(sections);

        let epoch_micros = current_epoch_micros();
        let mut region = Region {
            shmem,
            sections: sections.to_vec(),
            section_id_to_ordinal,
            section_data_offsets,
            is_owner: true,
        };
        region.write_global_header(handle, epoch_micros);
        for (ordinal, cfg) in sections.iter().enumerate() {
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

        let name = handle.shm_name();
        let shmem = ShmemConf::new()
            .os_id(&name)
            .open()
            .map_err(|e| TesseraRingError::Region(format!("attach: {e}")))?;

        let (section_id_to_ordinal, section_data_offsets) =
            build_section_lookup_tables(sections);
        let region = Region {
            shmem,
            sections: sections.to_vec(),
            section_id_to_ordinal,
            section_data_offsets,
            is_owner: false,
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

    // --- Cleanup ---------------------------------------------------

    /// Unlink the underlying SHM segment. Should be called by the
    /// owner at clean shutdown; attachers must NOT call this. Drop
    /// also unlinks owner-side automatically via `shared_memory`'s
    /// default ownership model.
    pub fn unlink(&mut self) -> Result<()> {
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
        let handle = unique_handle("section-geometry");
        let creator_sections = vec![SectionConfig::new(0, 4, 1024)];
        let _creator = Region::create(&handle, &creator_sections, false).expect("create");
        let attacher_sections = vec![SectionConfig::new(0, 8, 1024)];
        let err = Region::attach(&handle, &attacher_sections).unwrap_err();
        match err {
            TesseraRingError::SectionConfigMismatch {
                section_id,
                expected_count,
                found_count,
                ..
            } => {
                assert_eq!(section_id, 0);
                assert_eq!(expected_count, 8);
                assert_eq!(found_count, 4);
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
}
