//! SHM region lifecycle: create / attach / unlink, plus safe
//! accessors for the header (with atomic views on `head` and `tail`)
//! and per-slot data.
//!
//! Mirrors Pool / Ring's region.rs structure. Pre-bakes the
//! hard-won fixes from the Pool PR #4 + Ring PR #5 Codex review
//! loops:
//!   - bounds-check on attach before any raw byte access
//!   - `manually_unlinked` flag short-circuits repeat unlink()
//!   - `Shmem::set_owner(false)` after manual unlink suppresses
//!     drop-time unlink
//!   - `libc::shm_unlink` return-code gated on success before state
//!     flip
//!   - `#[cfg(not(unix))]` arm in unlink to avoid silent state-flip
//!     without OS effect
//!   - `slot_size_bytes % 8 == 0` validation for AtomicU64 alignment
//!     on per-slot fields

use std::sync::atomic::AtomicU64;
use std::time::{SystemTime, UNIX_EPOCH};

use bytemuck::Zeroable;
use shared_memory::{Shmem, ShmemConf, ShmemError};

use crate::error::{Result, TesseraChannelError};
use crate::header::{
    region_size_bytes, slot_header_offset, slot_payload_offset, Header, FORMAT_VERSION, MAGIC,
};
#[cfg(test)]
use crate::header::SlotHeader;
use crate::namespace::NamespaceHandle;

/// One mapped Tessera Channel region. Owns the `Shmem` handle so
/// the region stays mapped until this struct is dropped.
pub struct Region {
    shmem: Shmem,
    slot_count: u32,
    slot_size_bytes: u32,
    /// Cached POSIX SHM segment name (e.g. `/tessera-channel-<hex>`).
    /// Used by `unlink()` so we don't need to re-derive from a
    /// NamespaceHandle every time.
    shm_name: String,
    /// True iff this Region was opened by the Receiver (region
    /// creator). Drop-time and unlink behavior key off this.
    is_owner: bool,
    /// True once `unlink()` has been called successfully. Short-
    /// circuits subsequent unlink() calls so a stale owner can't
    /// race a successor's freshly-created region with the same name
    /// (Codex Pool PR #4 iter-3 lesson).
    manually_unlinked: bool,
}

impl core::fmt::Debug for Region {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Region")
            .field("slot_count", &self.slot_count)
            .field("slot_size_bytes", &self.slot_size_bytes)
            .field("is_owner", &self.is_owner)
            .field("manually_unlinked", &self.manually_unlinked)
            .field("len", &self.shmem.len())
            .finish()
    }
}

impl Region {
    /// Receiver-side: create a fresh region, stamp the header, zero
    /// the slot table.
    ///
    /// If the SHM segment already exists:
    /// - Default (`force_recreate == false`): return an error. We
    ///   don't inspect the existing segment because a "looks
    ///   invalid" verdict is racy — another Receiver may be mid-
    ///   init. Operators recovering from a crashed prior Receiver
    ///   must explicitly pass `force_recreate=true`.
    /// - `force_recreate == true`: caller asserts no live Receiver.
    ///   Unconditionally unlink + recreate.
    pub fn create(
        handle: &NamespaceHandle,
        slot_count: u32,
        slot_size_bytes: u32,
        force_recreate: bool,
    ) -> Result<Self> {
        if slot_count == 0 {
            return Err(TesseraChannelError::Config(
                "slot_count must be > 0".into(),
            ));
        }
        if slot_size_bytes == 0 {
            return Err(TesseraChannelError::Config(
                "slot_size_bytes must be > 0".into(),
            ));
        }
        // AtomicU64 alignment: slot_stride must be 8-aligned so
        // successive slots' `sequence` and `ready` fields are
        // 8-aligned. SlotHeader::SIZE is 56 (already 8-aligned);
        // slot_size_bytes contributes to stride directly. Require it
        // to be a multiple of 8.
        if slot_size_bytes % 8 != 0 {
            return Err(TesseraChannelError::Config(format!(
                "slot_size_bytes={slot_size_bytes} is not a multiple of 8; \
                slot stride must be 8-byte-aligned for AtomicU64 access on \
                per-slot `sequence` and `ready` fields (round up to {})",
                (slot_size_bytes + 7) & !7
            )));
        }

        let size = region_size_bytes(slot_count, slot_size_bytes).ok_or_else(|| {
            TesseraChannelError::Config(format!(
                "region size overflow: slot_count={slot_count} * slot_size_bytes={slot_size_bytes} \
                exceeds usize::MAX. Reduce one or both."
            ))
        })?;
        let name = handle.shm_name();

        let shmem = match ShmemConf::new().size(size).os_id(&name).create() {
            Ok(shmem) => shmem,
            Err(ShmemError::LinkExists) | Err(ShmemError::MappingIdExists) => {
                if force_recreate {
                    let _ = unlink_named_region(&name);
                    ShmemConf::new()
                        .size(size)
                        .os_id(&name)
                        .create()
                        .map_err(|e| {
                            TesseraChannelError::Region(format!(
                                "create after force_recreate unlink: {e}"
                            ))
                        })?
                } else {
                    return Err(TesseraChannelError::Region(format!(
                        "Channel region '{name}' already exists. Refusing to clobber. \
                        Possible causes: another Receiver is alive (do not create a \
                        second), OR a prior Receiver crashed without unlinking. For \
                        recovery from a crashed Receiver, retry with \
                        `force_recreate=true` — but only after confirming no live \
                        Receiver exists."
                    )));
                }
            }
            Err(e) => return Err(TesseraChannelError::Region(format!("create: {e}"))),
        };

        let epoch_micros = current_epoch_micros();
        let mut region = Region {
            shmem,
            slot_count,
            slot_size_bytes,
            shm_name: name,
            is_owner: true,
            manually_unlinked: false,
        };
        region.write_header(handle, epoch_micros, slot_count, slot_size_bytes);
        // Slot table starts zeroed (Shmem create zeroes on Linux),
        // which gives every slot sequence=0 and ready=0 — both fine
        // initial states for an empty queue.
        Ok(region)
    }

    /// Sender-side: attach to an existing region, validate the
    /// header against the caller's expected geometry + handle digest.
    pub fn attach(
        handle: &NamespaceHandle,
        expected_slot_count: u32,
        expected_slot_size_bytes: u32,
    ) -> Result<Self> {
        if expected_slot_count == 0 {
            return Err(TesseraChannelError::Config(
                "expected_slot_count must be > 0".into(),
            ));
        }
        if expected_slot_size_bytes == 0 {
            return Err(TesseraChannelError::Config(
                "expected_slot_size_bytes must be > 0".into(),
            ));
        }
        if expected_slot_size_bytes % 8 != 0 {
            return Err(TesseraChannelError::Config(format!(
                "expected_slot_size_bytes={expected_slot_size_bytes} is not a \
                multiple of 8; senders must match the receiver's 8-aligned \
                geometry"
            )));
        }

        let name = handle.shm_name();
        let shmem = ShmemConf::new()
            .os_id(&name)
            .open()
            .map_err(|e| TesseraChannelError::Region(format!("attach: {e}")))?;

        // Pre-baked Ring iter-2 lesson: bounds-check before any raw
        // copy. A stale / corrupt / wrong-size SHM segment of the
        // same name would otherwise let `read_header()` copy past
        // the end.
        let expected_size = region_size_bytes(expected_slot_count, expected_slot_size_bytes)
            .ok_or_else(|| {
                TesseraChannelError::Config(format!(
                    "region size overflow: slot_count={expected_slot_count} * \
                    slot_size_bytes={expected_slot_size_bytes} exceeds usize::MAX"
                ))
            })?;
        if shmem.len() < expected_size {
            return Err(TesseraChannelError::Region(format!(
                "attached SHM region '{name}' is smaller than expected: caller's \
                config requires at least {expected_size} bytes, but the mapped \
                region is only {} bytes. Possible causes: stale segment from a \
                crashed prior Receiver, wrong namespace handle, or config doesn't \
                match the Receiver's.",
                shmem.len()
            )));
        }

        let region = Region {
            shmem,
            slot_count: expected_slot_count,
            slot_size_bytes: expected_slot_size_bytes,
            shm_name: name,
            is_owner: false,
            manually_unlinked: false,
        };
        region.validate_attached_header(handle, expected_slot_count, expected_slot_size_bytes)?;
        Ok(region)
    }

    /// Whether this region was opened by the Receiver (create) vs a
    /// Sender (attach). Receiver-only operations (e.g. recv) and
    /// owner-side lifecycle (unlink) check this.
    pub fn is_owner(&self) -> bool {
        self.is_owner
    }

    /// Number of slots configured in the region.
    pub fn slot_count(&self) -> u32 {
        self.slot_count
    }

    /// Per-slot payload size.
    pub fn slot_size_bytes(&self) -> u32 {
        self.slot_size_bytes
    }

    /// Header epoch (microseconds since UNIX epoch at Receiver-side
    /// `Region::create`). Used to detect cross-deployment reuse.
    pub fn epoch_micros(&self) -> u64 {
        self.read_header().epoch_micros
    }

    // --- Header accessors -----------------------------------------

    fn write_header(
        &mut self,
        handle: &NamespaceHandle,
        epoch_micros: u64,
        slot_count: u32,
        slot_size_bytes: u32,
    ) {
        let header = Header {
            magic: MAGIC,
            format_version: FORMAT_VERSION,
            _pad0: 0,
            epoch_micros,
            slot_count,
            slot_size_bytes,
            head: 0,
            tail: 0,
            handle_blake3: handle.full_digest(),
            _reserved: [0; 40],
        };
        let header_bytes = bytemuck::bytes_of(&header);
        // SAFETY: we own the mapping (just created) and the
        // destination range is within bounds (region_size_bytes
        // includes Header::SIZE at offset 0).
        unsafe {
            let dst = self.shmem.as_ptr();
            core::ptr::copy_nonoverlapping(header_bytes.as_ptr(), dst, Header::SIZE);
        }
    }

    pub(crate) fn read_header(&self) -> Header {
        let mut header = Header::zeroed();
        let header_bytes = bytemuck::bytes_of_mut(&mut header);
        // SAFETY: offset 0 + SIZE is in bounds; Header is Pod so
        // any byte pattern is a valid Header (magic + version
        // checks happen at attach time before any of this is read).
        unsafe {
            let src = self.shmem.as_ptr();
            core::ptr::copy_nonoverlapping(src, header_bytes.as_mut_ptr(), Header::SIZE);
        }
        header
    }

    fn validate_attached_header(
        &self,
        handle: &NamespaceHandle,
        expected_slot_count: u32,
        expected_slot_size_bytes: u32,
    ) -> Result<()> {
        let header = self.read_header();
        if header.magic != MAGIC {
            return Err(TesseraChannelError::Region(format!(
                "magic mismatch: expected {:#x}, found {:#x} (not a Tessera Channel region?)",
                MAGIC, header.magic
            )));
        }
        if header.format_version != FORMAT_VERSION
            || header.slot_count != expected_slot_count
            || header.slot_size_bytes != expected_slot_size_bytes
        {
            return Err(TesseraChannelError::HeaderMismatch {
                message: "format / geometry mismatch".into(),
                expected_format: FORMAT_VERSION,
                found_format: header.format_version,
                expected_count: expected_slot_count,
                found_count: header.slot_count,
                expected_size: expected_slot_size_bytes,
                found_size: header.slot_size_bytes,
            });
        }
        if header.handle_blake3 != handle.full_digest() {
            return Err(TesseraChannelError::Region(format!(
                "handle digest mismatch on attach — your description derives a \
                different handle than the creator's; verify the description \
                string matches across processes (header_blake3 in SHM differs \
                from BLAKE3({:?}))",
                handle.shm_name()
            )));
        }
        Ok(())
    }

    // --- Runtime atomic accessors ---------------------------------

    /// Byte offset of `Header.head` within Header.
    ///
    /// Layout: magic(8) + format_version(4) + _pad0(4) +
    /// epoch_micros(8) + slot_count(4) + slot_size_bytes(4) = 32.
    /// The test `head_field_offset_matches_layout` locks this in.
    const HEAD_FIELD_OFFSET: usize = 32;

    /// Byte offset of `Header.tail` within Header.
    ///
    /// Layout: HEAD_FIELD_OFFSET + 8 (head u64) = 40.
    const TAIL_FIELD_OFFSET: usize = 40;

    /// Byte offset of `SlotHeader.sequence` within SlotHeader.
    /// First field, so 0.
    const SLOT_SEQUENCE_FIELD_OFFSET: usize = 0;

    /// Byte offset of `SlotHeader.ready` within SlotHeader.
    /// After `sequence` (8 bytes).
    const SLOT_READY_FIELD_OFFSET: usize = 8;

    /// Byte offset of `SlotHeader.length` within SlotHeader.
    /// sequence(8) + ready(8) = 16.
    const SLOT_LENGTH_FIELD_OFFSET: usize = 16;

    /// Byte offset of `SlotHeader.timestamp_nanos` within SlotHeader.
    /// sequence(8) + ready(8) + length(4) + _pad0(4) = 24.
    const SLOT_TIMESTAMP_FIELD_OFFSET: usize = 24;

    /// Atomic view of the queue's `head` counter (Receiver-side
    /// dequeue position).
    pub fn head_atomic(&self) -> &AtomicU64 {
        // SAFETY: head field offset (32) is within Header (size 120);
        // alignment: mmap base is page-aligned; HEAD_FIELD_OFFSET (32)
        // is 8-aligned. AtomicU64 has the same layout as u64.
        unsafe {
            let ptr = self.shmem.as_ptr().add(Self::HEAD_FIELD_OFFSET) as *const AtomicU64;
            &*ptr
        }
    }

    /// Atomic view of the queue's `tail` counter (Sender-side claim
    /// position).
    pub fn tail_atomic(&self) -> &AtomicU64 {
        // SAFETY: tail field offset (40) is within Header; 8-aligned.
        unsafe {
            let ptr = self.shmem.as_ptr().add(Self::TAIL_FIELD_OFFSET) as *const AtomicU64;
            &*ptr
        }
    }

    fn check_slot_index(&self, slot_index: u32) -> Result<()> {
        if slot_index >= self.slot_count {
            return Err(TesseraChannelError::Region(format!(
                "slot_index {slot_index} out of range (slot_count={})",
                self.slot_count
            )));
        }
        Ok(())
    }

    /// Atomic view of a slot's `sequence` field (Sender stamps when
    /// claiming; Receiver checks).
    pub fn slot_sequence_atomic(&self, slot_index: u32) -> Result<&AtomicU64> {
        self.check_slot_index(slot_index)?;
        let offset = slot_header_offset(slot_index, self.slot_size_bytes)
            + Self::SLOT_SEQUENCE_FIELD_OFFSET;
        // SAFETY: slot bounds verified; alignment: slot_array_offset
        // is 8-aligned (Header::SIZE = 120; 120 % 8 == 0); slot_stride
        // = 56 + slot_size_bytes, 8-aligned by validate-multiple-of-8
        // check at create/attach time.
        unsafe {
            let ptr = self.shmem.as_ptr().add(offset) as *const AtomicU64;
            Ok(&*ptr)
        }
    }

    /// Atomic view of a slot's `ready` flag (Sender sets to 1 after
    /// write; Receiver reads and clears to 0 after dequeue).
    pub fn slot_ready_atomic(&self, slot_index: u32) -> Result<&AtomicU64> {
        self.check_slot_index(slot_index)?;
        let offset = slot_header_offset(slot_index, self.slot_size_bytes)
            + Self::SLOT_READY_FIELD_OFFSET;
        // SAFETY: same chain as slot_sequence_atomic; ready field is
        // 8 bytes after sequence, both 8-aligned.
        unsafe {
            let ptr = self.shmem.as_ptr().add(offset) as *const AtomicU64;
            Ok(&*ptr)
        }
    }

    /// Write the non-atomic SlotHeader fields (`length` and
    /// `timestamp_nanos`) inside the producer's write window.
    ///
    /// `sequence` and `ready` are managed separately via the atomic
    /// accessors; this helper just covers the metadata fields.
    ///
    /// # Safety
    ///
    /// Caller must hold the slot in a producer-owned state (claimed
    /// via fetch_add but not yet marked ready).
    pub unsafe fn write_slot_metadata(
        &self,
        slot_index: u32,
        length: u32,
        timestamp_nanos: u64,
    ) -> Result<()> {
        self.check_slot_index(slot_index)?;
        let slot_base = slot_header_offset(slot_index, self.slot_size_bytes);
        // SAFETY: caller-asserted producer ownership; bounds verified.
        unsafe {
            let base = self.shmem.as_ptr().add(slot_base);
            core::ptr::write_unaligned(
                base.add(Self::SLOT_LENGTH_FIELD_OFFSET) as *mut u32,
                length,
            );
            core::ptr::write_unaligned(
                base.add(Self::SLOT_TIMESTAMP_FIELD_OFFSET) as *mut u64,
                timestamp_nanos,
            );
        }
        Ok(())
    }

    /// Read the non-atomic SlotHeader fields (length, timestamp).
    /// Used by Receiver after observing `ready == 1` and validating
    /// `sequence == head`.
    ///
    /// # Safety
    ///
    /// Caller must have confirmed `slot.ready == 1` and
    /// `slot.sequence == expected_head` before calling.
    pub unsafe fn read_slot_metadata(&self, slot_index: u32) -> Result<(u32, u64)> {
        self.check_slot_index(slot_index)?;
        let slot_base = slot_header_offset(slot_index, self.slot_size_bytes);
        // SAFETY: caller-asserted ready+sequence checks; bounds verified.
        unsafe {
            let base = self.shmem.as_ptr().add(slot_base);
            let length = core::ptr::read_unaligned(
                base.add(Self::SLOT_LENGTH_FIELD_OFFSET) as *const u32,
            );
            let timestamp = core::ptr::read_unaligned(
                base.add(Self::SLOT_TIMESTAMP_FIELD_OFFSET) as *const u64,
            );
            Ok((length, timestamp))
        }
    }

    /// Raw mutable pointer to the start of slot's payload area.
    /// Used by Sender inside the claim window to copy caller bytes.
    ///
    /// # Safety
    ///
    /// Caller must hold the slot as the unique writer (claimed via
    /// `fetch_add(tail, 1)` and not yet marked ready).
    pub unsafe fn slot_payload_ptr_mut(&self, slot_index: u32) -> Result<*mut u8> {
        self.check_slot_index(slot_index)?;
        let offset = slot_payload_offset(slot_index, self.slot_size_bytes);
        // SAFETY: caller-asserted; bounds verified.
        Ok(unsafe { self.shmem.as_ptr().add(offset) })
    }

    /// Raw const pointer to the start of slot's payload area. Used
    /// by Receiver inside the read window.
    ///
    /// # Safety
    ///
    /// Caller must have confirmed slot.ready == 1 + slot.sequence
    /// matches expected head.
    pub unsafe fn slot_payload_ptr(&self, slot_index: u32) -> Result<*const u8> {
        self.check_slot_index(slot_index)?;
        let offset = slot_payload_offset(slot_index, self.slot_size_bytes);
        Ok(unsafe { self.shmem.as_ptr().add(offset) as *const u8 })
    }

    // --- Cleanup ---------------------------------------------------

    /// Explicit Receiver-side unlink of the SHM segment by name.
    ///
    /// Same discipline as Pool / Ring after the Codex iterations:
    /// - `manually_unlinked` short-circuit prevents stale repeat
    ///   calls from clobbering a successor's freshly-created region.
    /// - Non-Receiver (Sender) calls return an error to enforce the
    ///   lifecycle contract.
    /// - `libc::shm_unlink` return code is checked; state flip only
    ///   on success (or ENOENT, treated as success).
    /// - `Shmem::set_owner(false)` suppresses drop-time unlink so
    ///   the Receiver's eventual drop doesn't shm_unlink again.
    /// - `#[cfg(not(unix))]` arm returns error so the state flip
    ///   doesn't silently leak the name on non-Unix builds.
    pub fn unlink(&mut self) -> Result<()> {
        if self.manually_unlinked {
            return Ok(());
        }
        if !self.is_owner {
            return Err(TesseraChannelError::Region(
                "Region::unlink called by a Sender (is_owner=false). Only the \
                Receiver may unlink the shared-memory name. Drop this Region to \
                release the Sender's mapping; the Receiver decides when to unlink."
                    .into(),
            ));
        }
        #[cfg(unix)]
        {
            let cname = std::ffi::CString::new(self.shm_name.as_str()).map_err(|_| {
                TesseraChannelError::Region(
                    "stored shm_name contains an interior NUL byte (cannot happen \
                    in practice — namespace handles produce hex-only names)"
                        .into(),
                )
            })?;
            // SAFETY: cname is a valid NUL-terminated C string;
            // shm_unlink is thread-safe POSIX. Check return code +
            // errno; only flip state on success.
            let rc = unsafe { libc::shm_unlink(cname.as_ptr()) };
            if rc != 0 {
                let err = std::io::Error::last_os_error();
                if err.raw_os_error() != Some(libc::ENOENT) {
                    return Err(TesseraChannelError::Region(format!(
                        "shm_unlink('{}') failed: {} (errno={:?}). Region state \
                        flags NOT updated; caller may retry unlink(), or drop \
                        the Region to let Shmem's drop-time unlink attempt \
                        cleanup.",
                        self.shm_name,
                        err,
                        err.raw_os_error(),
                    )));
                }
                // ENOENT: name was already gone; treat as success.
            }
        }
        #[cfg(not(unix))]
        {
            return Err(TesseraChannelError::Region(
                "Region::unlink is not supported on non-Unix platforms (POSIX \
                shm_unlink unavailable). Drop the Region to let the underlying \
                shared_memory crate's drop-time cleanup attempt removal."
                    .into(),
            ));
        }
        #[cfg(unix)]
        {
            self.shmem.set_owner(false);
            self.manually_unlinked = true;
        }
        Ok(())
    }
}

fn current_epoch_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}

/// Best-effort unlink of a stale SHM region by name. Used by
/// `Region::create` when `force_recreate=true` finds a leftover.
fn unlink_named_region(name: &str) -> Result<()> {
    #[cfg(unix)]
    {
        let cname = std::ffi::CString::new(name).map_err(|_| {
            TesseraChannelError::Region("region name contains NUL byte".into())
        })?;
        // SAFETY: cname is valid; shm_unlink is thread-safe POSIX;
        // return value ignored (best-effort).
        unsafe {
            libc::shm_unlink(cname.as_ptr());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    fn unique_handle(tag: &str) -> NamespaceHandle {
        let pid = std::process::id();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        NamespaceHandle::derive(&format!("tessera-channel-test/{tag}/{pid}/{nanos}"))
    }

    #[test]
    fn head_field_offset_matches_layout() {
        let h = Header::zeroed();
        let base = &h as *const Header as usize;
        let field = &h.head as *const u64 as usize;
        assert_eq!(field - base, Region::HEAD_FIELD_OFFSET);
    }

    #[test]
    fn tail_field_offset_matches_layout() {
        let h = Header::zeroed();
        let base = &h as *const Header as usize;
        let field = &h.tail as *const u64 as usize;
        assert_eq!(field - base, Region::TAIL_FIELD_OFFSET);
    }

    #[test]
    fn slot_header_field_offsets_match_layout() {
        let s = SlotHeader::zeroed();
        let base = &s as *const SlotHeader as usize;
        assert_eq!(
            &s.sequence as *const u64 as usize - base,
            Region::SLOT_SEQUENCE_FIELD_OFFSET
        );
        assert_eq!(
            &s.ready as *const u64 as usize - base,
            Region::SLOT_READY_FIELD_OFFSET
        );
        assert_eq!(
            &s.length as *const u32 as usize - base,
            Region::SLOT_LENGTH_FIELD_OFFSET
        );
        assert_eq!(
            &s.timestamp_nanos as *const u64 as usize - base,
            Region::SLOT_TIMESTAMP_FIELD_OFFSET
        );
    }

    #[test]
    fn create_writes_valid_header() {
        let handle = unique_handle("create-header");
        let region = Region::create(&handle, 4, 1024, false).expect("create");
        let h = region.read_header();
        assert_eq!(h.magic, MAGIC);
        assert_eq!(h.format_version, FORMAT_VERSION);
        assert_eq!(h.slot_count, 4);
        assert_eq!(h.slot_size_bytes, 1024);
        assert_eq!(h.head, 0);
        assert_eq!(h.tail, 0);
        assert_eq!(h.handle_blake3, handle.full_digest());
        assert!(h.epoch_micros > 0);
        assert!(region.is_owner());
    }

    #[test]
    fn zero_slot_count_is_rejected() {
        let handle = unique_handle("zero-slots");
        let err = Region::create(&handle, 0, 64, false).unwrap_err();
        assert!(matches!(err, TesseraChannelError::Config(_)));
    }

    #[test]
    fn zero_slot_size_is_rejected() {
        let handle = unique_handle("zero-size");
        let err = Region::create(&handle, 4, 0, false).unwrap_err();
        assert!(matches!(err, TesseraChannelError::Config(_)));
    }

    #[test]
    fn slot_size_not_multiple_of_8_is_rejected() {
        // Per-slot atomic alignment requires slot_size_bytes to be
        // a multiple of 8.
        for bad in [1u32, 7, 17, 100, 1023] {
            let handle = unique_handle(&format!("misalign-{bad}"));
            let err = Region::create(&handle, 4, bad, false).unwrap_err();
            match err {
                TesseraChannelError::Config(msg) => {
                    assert!(
                        msg.contains("not a multiple of 8"),
                        "expected alignment error for {bad}, got: {msg}"
                    );
                }
                other => panic!("expected Config, got {other:?}"),
            }
        }
        for good in [8u32, 16, 64, 1024, 2048] {
            let handle = unique_handle(&format!("aligned-{good}"));
            let _ = Region::create(&handle, 4, good, false)
                .unwrap_or_else(|e| panic!("expected ok for {good}, got {e:?}"));
        }
    }

    #[test]
    fn attach_reads_creators_header() {
        let handle = unique_handle("attach-roundtrip");
        let creator = Region::create(&handle, 4, 256, false).expect("create");
        let attacher = Region::attach(&handle, 4, 256).expect("attach");
        assert!(!attacher.is_owner());
        assert_eq!(attacher.epoch_micros(), creator.epoch_micros());
        drop(attacher);
        drop(creator);
    }

    #[test]
    fn attach_rejects_geometry_mismatch() {
        let handle = unique_handle("geometry-mismatch");
        // Make creator's region LARGER than what the attacher expects
        // so the bounds-check passes and the semantic check fires.
        let _creator = Region::create(&handle, 8, 1024, false).expect("create");
        let err = Region::attach(&handle, 4, 1024).unwrap_err();
        match err {
            TesseraChannelError::HeaderMismatch {
                expected_count,
                found_count,
                ..
            } => {
                assert_eq!(expected_count, 4);
                assert_eq!(found_count, 8);
            }
            other => panic!("expected HeaderMismatch, got {other:?}"),
        }
    }

    #[test]
    fn attach_rejects_undersized_region() {
        // Creator small; attacher's expected size huge.
        let handle = unique_handle("undersized");
        let _creator = Region::create(&handle, 1, 8, false).expect("create");
        let err = Region::attach(&handle, 100, 1024).unwrap_err();
        match err {
            TesseraChannelError::Region(msg) => {
                assert!(
                    msg.contains("smaller than expected"),
                    "expected bounds error, got: {msg}"
                );
            }
            other => panic!("expected Region, got {other:?}"),
        }
    }

    #[test]
    fn head_tail_atomics_start_at_zero_and_support_fetch_add() {
        let handle = unique_handle("head-tail-atomic");
        let region = Region::create(&handle, 4, 64, false).expect("create");
        assert_eq!(region.head_atomic().load(Ordering::SeqCst), 0);
        assert_eq!(region.tail_atomic().load(Ordering::SeqCst), 0);
        let prev = region.tail_atomic().fetch_add(1, Ordering::SeqCst);
        assert_eq!(prev, 0);
        assert_eq!(region.tail_atomic().load(Ordering::SeqCst), 1);

        // Visible to an attacher.
        let attacher = Region::attach(&handle, 4, 64).expect("attach");
        assert_eq!(attacher.tail_atomic().load(Ordering::SeqCst), 1);
    }

    #[test]
    fn slot_sequence_and_ready_atomics_start_at_zero() {
        let handle = unique_handle("slot-atomics-zero");
        let region = Region::create(&handle, 4, 64, false).expect("create");
        for i in 0..4 {
            assert_eq!(
                region
                    .slot_sequence_atomic(i)
                    .unwrap()
                    .load(Ordering::SeqCst),
                0
            );
            assert_eq!(
                region.slot_ready_atomic(i).unwrap().load(Ordering::SeqCst),
                0
            );
        }
    }

    #[test]
    fn slot_metadata_round_trips_via_unsafe_path() {
        let handle = unique_handle("slot-metadata");
        let region = Region::create(&handle, 2, 64, false).expect("create");
        // SAFETY: single-threaded test; caller-asserted producer ownership.
        unsafe {
            region.write_slot_metadata(1, 42, 1_700_000_000_000_000_000).unwrap();
            let (length, ts) = region.read_slot_metadata(1).unwrap();
            assert_eq!(length, 42);
            assert_eq!(ts, 1_700_000_000_000_000_000);
        }
    }

    #[test]
    fn slot_payload_ptr_writes_visible_via_attacher_read() {
        let handle = unique_handle("payload-ptr");
        let region = Region::create(&handle, 2, 64, false).expect("create");
        let data = b"hello channel via raw ptr";
        // SAFETY: single-threaded test; producer ownership.
        unsafe {
            let dst = region.slot_payload_ptr_mut(0).unwrap();
            core::ptr::copy_nonoverlapping(data.as_ptr(), dst, data.len());
        }
        // Attacher reads same bytes.
        let attacher = Region::attach(&handle, 2, 64).expect("attach");
        // SAFETY: caller-asserted sequencing; single-threaded test.
        unsafe {
            let src = attacher.slot_payload_ptr(0).unwrap();
            let mut buf = vec![0u8; data.len()];
            core::ptr::copy_nonoverlapping(src, buf.as_mut_ptr(), data.len());
            assert_eq!(buf.as_slice(), data);
        }
    }

    #[test]
    fn unlink_removes_shm_name_for_receiver() {
        let handle = unique_handle("explicit-unlink");
        let mut creator = Region::create(&handle, 4, 64, false).expect("create");
        let _attacher_pre = Region::attach(&handle, 4, 64).expect("attach pre-unlink");
        creator.unlink().expect("unlink");
        let post = Region::attach(&handle, 4, 64);
        assert!(post.is_err(), "attach should fail after unlink");
        // Idempotent: second unlink is a no-op.
        creator.unlink().expect("second unlink");
    }

    #[test]
    fn unlink_rejects_sender_calls() {
        let handle = unique_handle("sender-unlink-rejected");
        let _creator = Region::create(&handle, 4, 64, false).expect("create");
        let mut attacher = Region::attach(&handle, 4, 64).expect("attach");
        let err = attacher.unlink().unwrap_err();
        match err {
            TesseraChannelError::Region(msg) => {
                assert!(
                    msg.contains("Sender") || msg.contains("Only the Receiver"),
                    "expected sender-rejection error, got: {msg}"
                );
            }
            other => panic!("expected Region, got {other:?}"),
        }
    }

    #[test]
    fn unlink_handoff_safety_drop_does_not_clobber_successor() {
        // Pre-bakes Pool PR #4 iter-1 lesson.
        let handle = unique_handle("handoff-no-clobber");
        let mut a = Region::create(&handle, 2, 64, false).expect("A create");
        a.unlink().expect("A unlink");
        let b = Region::create(&handle, 4, 128, false).expect("B create after A unlink");
        drop(a);
        let attacher = Region::attach(&handle, 4, 128).expect("attach to B after A drop");
        assert_eq!(attacher.slot_count(), 4);
        drop(attacher);
        drop(b);
    }

    #[test]
    fn stale_double_unlink_does_not_clobber_successor() {
        // Pre-bakes Pool PR #4 iter-3 lesson.
        let handle = unique_handle("stale-double-unlink");
        let mut a = Region::create(&handle, 2, 64, false).expect("A create");
        a.unlink().expect("A first unlink");
        let b = Region::create(&handle, 4, 128, false).expect("B create after A unlink");
        a.unlink().expect("A second unlink (should be no-op)");
        let attacher = Region::attach(&handle, 4, 128).expect("attach B after stale 2nd unlink");
        drop(attacher);
        drop(a);
        let attacher2 = Region::attach(&handle, 4, 128).expect("attach B after A drop");
        drop(attacher2);
        drop(b);
    }

    #[test]
    fn force_recreate_clobbers_existing_region() {
        let handle = unique_handle("force-recreate");
        let _first = Region::create(&handle, 2, 64, false).expect("create 1");
        let refused = Region::create(&handle, 2, 64, false);
        assert!(matches!(refused, Err(TesseraChannelError::Region(_))));
        let _second = Region::create(&handle, 2, 64, true).expect("force_recreate");
    }

    #[test]
    fn cross_process_attach_via_shared_handle() {
        let handle = unique_handle("cross-attach");
        let creator = Region::create(&handle, 4, 128, false).expect("create");
        // Producer side: claim a slot, write metadata + payload, mark ready.
        let claimed = creator.tail_atomic().fetch_add(1, Ordering::SeqCst);
        let slot_index = (claimed % creator.slot_count() as u64) as u32;
        let msg = b"hello attacher";
        // SAFETY: single-process test; producer ownership.
        unsafe {
            let dst = creator.slot_payload_ptr_mut(slot_index).unwrap();
            core::ptr::copy_nonoverlapping(msg.as_ptr(), dst, msg.len());
            creator
                .write_slot_metadata(slot_index, msg.len() as u32, 0)
                .unwrap();
        }
        creator
            .slot_sequence_atomic(slot_index)
            .unwrap()
            .store(claimed, Ordering::Release);
        creator
            .slot_ready_atomic(slot_index)
            .unwrap()
            .store(1, Ordering::Release);

        // Attacher reads it back.
        let attacher = Region::attach(&handle, 4, 128).expect("attach");
        assert_eq!(
            attacher.tail_atomic().load(Ordering::Acquire),
            1,
            "attacher sees claimed slot"
        );
        let ready = attacher
            .slot_ready_atomic(slot_index)
            .unwrap()
            .load(Ordering::Acquire);
        assert_eq!(ready, 1);
        // SAFETY: ready check confirmed; single-process test.
        unsafe {
            let (length, _) = attacher.read_slot_metadata(slot_index).unwrap();
            assert_eq!(length, msg.len() as u32);
            let src = attacher.slot_payload_ptr(slot_index).unwrap();
            let mut buf = vec![0u8; length as usize];
            core::ptr::copy_nonoverlapping(src, buf.as_mut_ptr(), length as usize);
            assert_eq!(buf.as_slice(), msg);
        }
    }
}
