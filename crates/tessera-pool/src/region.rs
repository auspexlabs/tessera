//! SHM region lifecycle: create / attach / unlink, plus safe accessors
//! for the header, slot-metadata table, and per-slot payload slices.
//!
//! The region is laid out per `crate::header` documentation: a fixed
//! header at offset 0, then `slot_count` SlotMeta entries, then the
//! contiguous payload area. This module owns the raw mapped bytes; all
//! `unsafe` for byte-slice → typed-pointer reinterpretation lives here.

use std::time::{SystemTime, UNIX_EPOCH};

use bytemuck::Zeroable;
use shared_memory::{Shmem, ShmemConf, ShmemError};

use crate::error::{Result, TesseraPoolError};
use crate::header::{
    flags, region_size_bytes, slot_meta_offset, slot_payload_offset, Header, SlotMeta,
    FORMAT_VERSION, MAGIC,
};
use crate::namespace::NamespaceHandle;

/// One mapped Tessera Pool region. Owns the `Shmem` handle so the
/// region stays mapped until this struct is dropped.
pub struct Region {
    shmem: Shmem,
    slot_count: u32,
    slot_size_bytes: u32,
    /// POSIX SHM segment name (e.g. `/tessera-pool-<hex>`). Stored so
    /// `Region::unlink` can call `shm_unlink` without re-deriving from
    /// the namespace handle. Mirrors the Ring iter-3 fix on tessera-ring
    /// commit 0e39176.
    shm_name: String,
    is_owner: bool,
    /// True once `Region::unlink()` has been called successfully.
    /// Used to short-circuit subsequent `unlink()` calls so a stale
    /// owner can't race a successor's freshly-created region with
    /// the same name (Codex PR #4 iter-3 P1 — comment 3304957184).
    /// Without this flag, A's second unlink() call after B has
    /// recreated the same name would remove B's name.
    manually_unlinked: bool,
}

impl core::fmt::Debug for Region {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Region")
            .field("slot_count", &self.slot_count)
            .field("slot_size_bytes", &self.slot_size_bytes)
            .field("is_owner", &self.is_owner)
            .field("len", &self.shmem.len())
            .finish()
    }
}

impl Region {
    /// Owner path: create a fresh region, stamp the header with the
    /// caller's geometry + epoch + TTL + handle digest, zero the slot
    /// table.
    ///
    /// If the SHM segment already exists:
    /// - Default (`force_recreate == false`): return an error. We do
    ///   NOT try to inspect the existing segment, because a "looks
    ///   invalid" verdict is racy — another owner may be mid-init,
    ///   having created the segment but not yet stamped the header.
    ///   Treating a zeroed-header window as "stale" would clobber a
    ///   live segment. Operators recovering from a crashed prior
    ///   owner must explicitly pass `force_recreate=true`.
    /// - `force_recreate == true`: caller asserts no live owner.
    ///   Unconditionally unlink + recreate. Misuse of this flag is
    ///   the caller's responsibility.
    pub fn create(
        handle: &NamespaceHandle,
        slot_count: u32,
        slot_size_bytes: u32,
        ttl_micros: u64,
        force_recreate: bool,
    ) -> Result<Self> {
        if slot_count == 0 {
            return Err(TesseraPoolError::Config("slot_count must be > 0".into()));
        }
        if slot_size_bytes == 0 {
            return Err(TesseraPoolError::Config(
                "slot_size_bytes must be > 0".into(),
            ));
        }

        let size = region_size_bytes(slot_count, slot_size_bytes).ok_or_else(|| {
            TesseraPoolError::Config(format!(
                "region size overflow: slot_count={slot_count} * slot_size_bytes={slot_size_bytes} \
                exceeds usize::MAX. Reduce one or both."
            ))
        })?;
        let name = handle.shm_name();

        let shmem = match ShmemConf::new().size(size).os_id(&name).create() {
            Ok(shmem) => shmem,
            Err(ShmemError::LinkExists) | Err(ShmemError::MappingIdExists) => {
                if force_recreate {
                    // Operator-asserted recovery: no live owner exists,
                    // unlink + recreate unconditionally. We do NOT
                    // attach-validate first — that would re-introduce
                    // the startup-race vulnerability where a brand-new
                    // segment in the mid-init window looks "invalid"
                    // because its header isn't stamped yet.
                    let _ = unlink_named_region(&name);
                    ShmemConf::new()
                        .size(size)
                        .os_id(&name)
                        .create()
                        .map_err(|e| {
                            TesseraPoolError::Region(format!(
                                "create after force_recreate unlink: {e}"
                            ))
                        })?
                } else {
                    return Err(TesseraPoolError::Region(format!(
                        "Pool region '{name}' already exists. Refusing to clobber. \
                        Possible causes: another owner is alive (do not create a \
                        second), OR a prior owner crashed without unlinking. For \
                        recovery from a crashed owner, retry with \
                        `force_recreate=true` — but only after confirming no live \
                        owner exists, since `force_recreate` will unconditionally \
                        unlink the existing segment."
                    )));
                }
            }
            Err(e) => return Err(TesseraPoolError::Region(format!("create: {e}"))),
        };

        // Initialize the header in place.
        let epoch_micros = current_epoch_micros();
        let mut region = Region {
            shmem,
            slot_count,
            slot_size_bytes,
            shm_name: name,
            is_owner: true,
            manually_unlinked: false,
        };
        region.write_header(handle, epoch_micros, slot_count, slot_size_bytes, ttl_micros);
        // Slot table starts zeroed (Shmem create zeroes the mapping on
        // Linux), but be explicit about it: zero every SlotMeta entry.
        // The `?` here propagates a bounds error; not reachable in
        // practice because `i` is < slot_count by construction, but
        // satisfies the new Result-returning signature.
        for i in 0..slot_count {
            region.write_slot_meta(i, SlotMeta::zeroed())?;
        }
        Ok(region)
    }

    /// Non-owner path: attach to an existing region, validate the
    /// header against the caller's expected geometry + handle digest.
    pub fn attach(
        handle: &NamespaceHandle,
        expected_slot_count: u32,
        expected_slot_size_bytes: u32,
    ) -> Result<Self> {
        let name = handle.shm_name();
        let shmem = ShmemConf::new()
            .os_id(&name)
            .open()
            .map_err(|e| TesseraPoolError::Region(format!("attach: {e}")))?;

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

    /// Whether this region was opened by the owner (create) vs an
    /// attacher (open). Owner-only operations check this.
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

    /// TTL read from the region header. Non-owners use this to
    /// inherit the owner-stamped TTL (§3.5.d).
    pub fn ttl_micros(&self) -> u64 {
        self.read_header().ttl_micros
    }

    /// Header epoch (microseconds since UNIX epoch at owner-side
    /// `Region::create`). Used to detect cross-deployment reuse.
    pub fn epoch_micros(&self) -> u64 {
        self.read_header().epoch_micros
    }

    // --- Header accessors (private; the Pool wraps these) -----------

    fn write_header(
        &mut self,
        handle: &NamespaceHandle,
        epoch_micros: u64,
        slot_count: u32,
        slot_size_bytes: u32,
        ttl_micros: u64,
    ) {
        let header = Header {
            magic: MAGIC,
            format_version: FORMAT_VERSION,
            _pad0: 0,
            epoch_micros,
            slot_count,
            slot_size_bytes,
            ttl_micros,
            handle_blake3: handle.full_digest(),
            _reserved: [0; 56],
        };
        let header_bytes = bytemuck::bytes_of(&header);
        // SAFETY: we own the mapping (just created or attached) and
        // the destination range is within bounds (region_size_bytes
        // includes Header::SIZE at offset 0).
        unsafe {
            let dst = self.shmem.as_ptr();
            core::ptr::copy_nonoverlapping(header_bytes.as_ptr(), dst, Header::SIZE);
        }
    }

    pub(crate) fn read_header(&self) -> Header {
        // SAFETY: same as write_header — we own the mapping, offset 0
        // is in bounds, Header is Pod so any byte pattern is a valid
        // Header (the MAGIC + format_version checks happen at attach time
        // before any of this is read).
        let mut header = Header::zeroed();
        let header_bytes = bytemuck::bytes_of_mut(&mut header);
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
            return Err(TesseraPoolError::Region(format!(
                "magic mismatch: expected {:#x}, found {:#x} (not a Tessera Pool region?)",
                MAGIC, header.magic
            )));
        }
        if header.format_version != FORMAT_VERSION
            || header.slot_count != expected_slot_count
            || header.slot_size_bytes != expected_slot_size_bytes
        {
            return Err(TesseraPoolError::HeaderMismatch {
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
            return Err(TesseraPoolError::Region(format!(
                "handle digest mismatch on attach — your description \
                derives a different handle than the creator's; verify \
                the description string matches across processes (header_blake3 \
                in SHM differs from BLAKE3({:?}))",
                handle.shm_name()
            )));
        }
        Ok(())
    }

    // --- Slot metadata accessors -----------------------------------

    /// Validate `slot_index < slot_count`. Used by every accessor
    /// that performs `unsafe` pointer arithmetic so the bounds check
    /// survives release builds (debug_assert is stripped at opt
    /// levels 1+).
    fn check_slot_index(&self, slot_index: u32) -> Result<()> {
        if slot_index >= self.slot_count {
            return Err(TesseraPoolError::Region(format!(
                "slot_index {slot_index} out of range (slot_count={})",
                self.slot_count
            )));
        }
        Ok(())
    }

    /// Read a slot's metadata by index. O(1). Returns `Region` error
    /// if `slot_index` is out of range.
    pub fn read_slot_meta(&self, slot_index: u32) -> Result<SlotMeta> {
        self.check_slot_index(slot_index)?;
        let offset = slot_meta_offset(slot_index);
        let mut meta = SlotMeta::zeroed();
        let meta_bytes = bytemuck::bytes_of_mut(&mut meta);
        // SAFETY: offset + SIZE <= region size — slot_index is
        // verified < slot_count above, and slot_meta_offset(i)
        // == HEADER_SIZE + i * SlotMeta::SIZE which is < region_size
        // for any valid i.
        unsafe {
            let src = self.shmem.as_ptr().add(offset);
            core::ptr::copy_nonoverlapping(src, meta_bytes.as_mut_ptr(), SlotMeta::SIZE);
        }
        Ok(meta)
    }

    /// Write a slot's metadata. Returns `Region` error if
    /// `slot_index` is out of range. (Owner-only logically; not
    /// enforced here because the Pool layer enforces single-writer-
    /// lease before calling in.)
    pub fn write_slot_meta(&mut self, slot_index: u32, meta: SlotMeta) -> Result<()> {
        self.check_slot_index(slot_index)?;
        let offset = slot_meta_offset(slot_index);
        let meta_bytes = bytemuck::bytes_of(&meta);
        // SAFETY: offset + SIZE <= region size (see read_slot_meta);
        // we hold &mut self so no other reader/writer is racing in
        // this process.
        unsafe {
            let dst = self.shmem.as_ptr().add(offset);
            core::ptr::copy_nonoverlapping(meta_bytes.as_ptr(), dst, SlotMeta::SIZE);
        }
        Ok(())
    }

    // --- Slot payload accessors ------------------------------------

    /// Copy `bytes` into slot `slot_index`'s payload area. Returns
    /// `Region` error if `slot_index` is out of range or `bytes`
    /// exceeds the slot capacity (the Pool layer rejects oversized
    /// payloads with `OversizedPayload` earlier in the chain; this
    /// is defense in depth at the unsafe boundary).
    pub fn write_slot_payload(&mut self, slot_index: u32, bytes: &[u8]) -> Result<()> {
        self.check_slot_index(slot_index)?;
        if bytes.len() > self.slot_size_bytes as usize {
            return Err(TesseraPoolError::Region(format!(
                "payload size {} exceeds slot capacity {}",
                bytes.len(),
                self.slot_size_bytes
            )));
        }
        let offset = slot_payload_offset(slot_index, self.slot_count, self.slot_size_bytes);
        // SAFETY: slot_index verified in range, bytes.len() verified
        // ≤ slot_size_bytes; we hold &mut self so no concurrent
        // access in-process.
        unsafe {
            let dst = self.shmem.as_ptr().add(offset);
            core::ptr::copy_nonoverlapping(bytes.as_ptr(), dst, bytes.len());
        }
        Ok(())
    }

    /// Read a copy of slot `slot_index`'s payload bytes (first
    /// `payload_len` bytes). Used by attached readers consuming a
    /// descriptor.
    ///
    /// Returns an error if `slot_index` is out of range or
    /// `payload_len` exceeds the slot capacity. Callers in this crate
    /// should also clamp `payload_len` against the slot's current
    /// `payload_len` metadata before calling (see `Pool::read_payload`).
    pub fn read_slot_payload(&self, slot_index: u32, payload_len: u32) -> Result<Vec<u8>> {
        if slot_index >= self.slot_count {
            return Err(TesseraPoolError::Region(format!(
                "slot_index {slot_index} out of range (slot_count={})",
                self.slot_count
            )));
        }
        if payload_len > self.slot_size_bytes {
            return Err(TesseraPoolError::Region(format!(
                "payload_len {payload_len} exceeds slot capacity {}",
                self.slot_size_bytes
            )));
        }
        let offset = slot_payload_offset(slot_index, self.slot_count, self.slot_size_bytes);
        let mut out = vec![0u8; payload_len as usize];
        // SAFETY: bounds verified above; we hold &self so no in-process
        // writer is racing this region (owner-side writes go through
        // &mut self elsewhere).
        unsafe {
            let src = self.shmem.as_ptr().add(offset);
            core::ptr::copy_nonoverlapping(src, out.as_mut_ptr(), payload_len as usize);
        }
        Ok(out)
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
    /// Mirrors the Ring iter-3 P2 fix on tessera-ring commit 0e39176
    /// — previously this was a no-op despite the "should be called by
    /// the owner at clean shutdown" docstring, which was misleading.
    ///
    /// # Restrictions
    ///
    /// Non-owners (attachers) MUST NOT call this; doing so removes
    /// the name out from under the creating owner. Returns a `Region`
    /// error in that case to enforce the lifecycle contract.
    pub fn unlink(&mut self) -> Result<()> {
        // Idempotent short-circuit: once we've already unlinked, do
        // nothing further. This is the Codex iter-3 fix (PR #4
        // comment 3304957184) — without this short-circuit, a stale
        // owner A who calls unlink() a SECOND time after a successor
        // B has recreated the name would call libc::shm_unlink again
        // and remove B's freshly-created name. The first unlink
        // already released our claim on the OS name; we must not
        // touch it again.
        if self.manually_unlinked {
            return Ok(());
        }
        if !self.is_owner {
            return Err(TesseraPoolError::Region(
                "Region::unlink called by an attacher (is_owner=false). Only the \
                creator may unlink the shared-memory name. Drop this Region to \
                release the attacher's mapping; the creator decides when to unlink."
                    .into(),
            ));
        }
        #[cfg(unix)]
        {
            let cname = std::ffi::CString::new(self.shm_name.as_str()).map_err(|_| {
                TesseraPoolError::Region(
                    "stored shm_name contains an interior NUL byte (cannot happen \
                    in practice — namespace handles produce hex-only names)"
                        .into(),
                )
            })?;
            // SAFETY: cname is a valid NUL-terminated C string;
            // shm_unlink is thread-safe POSIX.
            //
            // Codex iter-4 P1 on PR #4 (comment 3305006943): we now
            // check the return value. If shm_unlink fails for a real
            // reason (e.g. EACCES from a uid change mid-operation),
            // we MUST NOT flip the state flags below — otherwise the
            // caller has no way to retry, and Drop-time unlink is
            // also suppressed, leaving the OS name live and breaking
            // the cleanup/handoff guarantees this method advertises.
            //
            // ENOENT is treated as success: "name already gone" is
            // the desired post-condition of unlink, regardless of
            // who removed it.
            let rc = unsafe { libc::shm_unlink(cname.as_ptr()) };
            if rc != 0 {
                let err = std::io::Error::last_os_error();
                if err.raw_os_error() != Some(libc::ENOENT) {
                    return Err(TesseraPoolError::Region(format!(
                        "shm_unlink('{}') failed: {} (errno={:?}). Region state \
                        flags NOT updated; caller may retry unlink(), or drop \
                        the Region to let Shmem's drop-time unlink attempt \
                        cleanup.",
                        self.shm_name,
                        err,
                        err.raw_os_error(),
                    )));
                }
                // ENOENT: name was already gone (some other peer
                // removed it, or this is a retry after a transient
                // failure). Falls through to the state-flip below as
                // a successful unlink.
            }
        }
        // Codex P1 on PR #4 iter-1 (comment 3304769711): suppress
        // the Shmem's drop-time unlink. Without this, a
        // handoff/restart sequence (owner A unlinks, owner B creates
        // a fresh region with the same name, then A finally drops)
        // would have A's drop call shm_unlink AGAIN, racily removing
        // B's freshly created name.
        //
        // Codex iter-4 (comment 3305006943): only reached after
        // shm_unlink succeeded (or returned ENOENT). On real
        // failure we early-returned above without touching state.
        self.shmem.set_owner(false);
        // Block any future unlink() call from this Region (Codex
        // iter-3 fix). Subsequent calls hit the early-return above.
        self.manually_unlinked = true;
        Ok(())
    }
}

fn current_epoch_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}

/// Unlink a stale SHM region by name (used when create finds a leftover).
fn unlink_named_region(name: &str) -> Result<()> {
    // shared_memory crate doesn't expose a free-standing unlink; the
    // typical path is to open the segment with `force_create_flink(false)`
    // and drop it as the owner. For now, attempt an open+drop:
    if let Ok(shmem) = ShmemConf::new().os_id(name).open() {
        // set_owner is not exposed through the public API for opened
        // segments; on Linux dropping a non-owned attachment does NOT
        // unlink. The fallback is to delegate to the OS via the libc
        // shm_unlink call directly. Implemented inline rather than via
        // an extra dependency.
        drop(shmem);
        #[cfg(unix)]
        {
            // POSIX shm_unlink takes the same name passed to shm_open.
            let cname = std::ffi::CString::new(name).map_err(|_| {
                TesseraPoolError::Region("region name contains NUL byte".into())
            })?;
            // SAFETY: cname is a valid NUL-terminated C string; shm_unlink
            // is a thread-safe POSIX call. We ignore the return value
            // because the caller is using this as a best-effort cleanup.
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
        // Each test gets a unique description so parallel test execution
        // doesn't collide on the same SHM name.
        let pid = std::process::id();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        NamespaceHandle::derive(&format!("tessera-pool-test/{tag}/{pid}/{nanos}"))
    }

    #[test]
    fn create_writes_valid_header() {
        let handle = unique_handle("create-header");
        let region = Region::create(&handle, 4, 1024, 60_000_000, false).expect("create");
        let h = region.read_header();
        assert_eq!(h.magic, MAGIC);
        assert_eq!(h.format_version, FORMAT_VERSION);
        assert_eq!(h.slot_count, 4);
        assert_eq!(h.slot_size_bytes, 1024);
        assert_eq!(h.ttl_micros, 60_000_000);
        assert_eq!(h.handle_blake3, handle.full_digest());
        assert!(h.epoch_micros > 0);
        assert!(region.is_owner());
    }

    #[test]
    fn create_initializes_slot_table_to_zero() {
        let handle = unique_handle("zero-slots");
        let region = Region::create(&handle, 3, 256, 30_000_000, false).expect("create");
        for i in 0..3 {
            let meta = region.read_slot_meta(i).expect("read");
            assert_eq!(meta.lease_id_high, 0);
            assert_eq!(meta.lease_id_low, 0);
            assert_eq!(meta.generation, 0);
            assert_eq!(meta.flags, 0);
            assert!(!meta.in_use());
        }
    }

    #[test]
    fn attach_reads_creators_header() {
        let handle = unique_handle("attach-roundtrip");
        let creator = Region::create(&handle, 2, 512, 45_000_000, false).expect("create");
        let attacher = Region::attach(&handle, 2, 512).expect("attach");
        assert!(!attacher.is_owner());
        assert_eq!(attacher.ttl_micros(), 45_000_000);
        assert_eq!(attacher.epoch_micros(), creator.epoch_micros());
        drop(attacher);
        drop(creator);
    }

    #[test]
    fn attach_rejects_geometry_mismatch() {
        let handle = unique_handle("geometry-mismatch");
        let _creator = Region::create(&handle, 4, 1024, 60_000_000, false).expect("create");
        let err = Region::attach(&handle, 8, 1024).unwrap_err();
        match err {
            TesseraPoolError::HeaderMismatch {
                expected_count,
                found_count,
                ..
            } => {
                assert_eq!(expected_count, 8);
                assert_eq!(found_count, 4);
            }
            other => panic!("expected HeaderMismatch, got {other:?}"),
        }
    }

    #[test]
    fn attach_rejects_handle_mismatch() {
        let handle_a = unique_handle("handle-a");
        let _creator = Region::create(&handle_a, 2, 256, 30_000_000, false).expect("create");
        // Attach with a different description → different handle →
        // different POSIX SHM name → ShmemError::MapOpenFailed, NOT a
        // handle-digest mismatch. (Handle-digest mismatch is the
        // failure mode where two consumers use the SAME shm_name but
        // disagree on header content — much rarer; covered by the
        // direct write_header round-trip test below.)
        let handle_b = unique_handle("handle-b");
        let err = Region::attach(&handle_b, 2, 256).unwrap_err();
        match err {
            TesseraPoolError::Region(_) => {}
            other => panic!("expected Region error, got {other:?}"),
        }
    }

    #[test]
    fn write_then_read_slot_meta_roundtrips() {
        let handle = unique_handle("meta-roundtrip");
        let mut region = Region::create(&handle, 2, 256, 30_000_000, false).expect("create");
        let meta = SlotMeta {
            lease_id_high: 0x1122_3344_5566_7788,
            lease_id_low: 0x99AA_BBCC_DDEE_FF00,
            generation: 7,
            acquired_at_micros: 1_234_567_890,
            payload_len: 42,
            flags: flags::IN_USE | flags::PAYLOAD_FINALIZED,
            _reserved: [0; 32],
        };
        region.write_slot_meta(1, meta).expect("write meta");
        let read = region.read_slot_meta(1).expect("read meta");
        assert_eq!(read.lease_id_high, 0x1122_3344_5566_7788);
        assert_eq!(read.lease_id_low, 0x99AA_BBCC_DDEE_FF00);
        assert_eq!(read.generation, 7);
        assert_eq!(read.acquired_at_micros, 1_234_567_890);
        assert_eq!(read.payload_len, 42);
        assert!(read.in_use());
        assert!(read.payload_finalized());
    }

    #[test]
    fn write_then_read_slot_payload_roundtrips() {
        let handle = unique_handle("payload-roundtrip");
        let mut region = Region::create(&handle, 2, 64, 30_000_000, false).expect("create");
        let payload: Vec<u8> = (0..32).collect();
        region.write_slot_payload(0, &payload).expect("write payload");
        let read = region.read_slot_payload(0, 32).expect("read");
        assert_eq!(read, payload);
        // Slot 1 was untouched.
        let other = region.read_slot_payload(1, 32).expect("read other");
        assert_eq!(other, vec![0u8; 32]);
    }

    #[test]
    fn cross_process_attach_via_shared_handle() {
        // Single-process simulation of attach: creator and attacher are
        // both in this process, but the attacher only knows the handle
        // (not the creator's Region object). Validates that name
        // derivation alone is sufficient to coordinate.
        let handle = unique_handle("cross-attach");
        let mut creator = Region::create(&handle, 4, 128, 60_000_000, false).expect("create");
        creator.write_slot_payload(2, b"hello attacher").expect("write");

        let attacher = Region::attach(&handle, 4, 128).expect("attach");
        let read = attacher
            .read_slot_payload(2, b"hello attacher".len() as u32)
            .expect("read");
        assert_eq!(read.as_slice(), b"hello attacher");
    }

    #[test]
    fn unlink_removes_shm_name_for_owner() {
        // Mirrors the Ring iter-3 P2 fix. Region::unlink must actually
        // call shm_unlink for owners; verify by calling unlink, then
        // attempting a fresh attach — must fail because the SHM name
        // is gone.
        let handle = unique_handle("explicit-unlink");
        let mut owner = Region::create(&handle, 4, 64, 60_000_000, false).expect("create");
        // Before unlink: attach succeeds.
        let _attacher = Region::attach(&handle, 4, 64).expect("attach pre-unlink");
        // Explicit unlink.
        owner.unlink().expect("unlink");
        // After unlink: fresh attacher cannot find the name.
        let post_attach = Region::attach(&handle, 4, 64);
        assert!(
            post_attach.is_err(),
            "expected attach to fail after explicit unlink, got Ok"
        );
        // Idempotent: second unlink is safe.
        owner.unlink().expect("second unlink");
    }

    #[test]
    fn unlink_disables_drop_time_unlink_for_handoff_safety() {
        // Codex P1 on PR #4 (comment 3304769711): after explicit
        // unlink(), the Region must NOT re-unlink the name when it
        // eventually drops. Otherwise a handoff/restart sequence
        // (owner A unlinks, owner B creates a fresh region with the
        // same name, then A drops) would have A's drop-time unlink
        // clobber B's freshly-created name.
        //
        // Direct test:
        //   1. A creates region X.
        //   2. A unlinks the name.
        //   3. B creates a fresh region with the same name (succeeds
        //      because A's unlink removed the OS name).
        //   4. Drop A. With the fix, A's drop does NOT call
        //      shm_unlink, so B's name survives.
        //   5. Attach to B by name — must succeed.
        let handle = unique_handle("handoff-no-clobber");
        let mut owner_a = Region::create(&handle, 2, 64, 60_000_000, false).expect("A create");
        owner_a.unlink().expect("A unlink");

        // B creates with the same handle. Without force_recreate this
        // would normally fail "already exists", but A's unlink cleared
        // the OS name so the create succeeds clean.
        let owner_b =
            Region::create(&handle, 4, 128, 60_000_000, false).expect("B create after A unlink");

        // Drop A. The fix (set_owner(false) inside unlink) ensures A's
        // drop does NOT call shm_unlink on the now-B-owned name.
        drop(owner_a);

        // B's name must still resolve.
        let attacher = Region::attach(&handle, 4, 128).expect("attach to B after A's drop");
        // Sanity: B's geometry, not A's.
        assert_eq!(attacher.slot_count(), 4);
        assert_eq!(attacher.slot_size_bytes(), 128);
        drop(attacher);
        drop(owner_b);
    }

    #[test]
    fn stale_owners_second_unlink_does_not_clobber_successor() {
        // Codex iter-3 P1 on PR #4 (comment 3304957184): the
        // drop-time unlink fix in iter-1 wasn't enough — a stale
        // owner A who explicitly calls unlink() a SECOND time after
        // successor B has recreated the same name would still call
        // libc::shm_unlink and remove B's name.
        //
        // Sequence under test:
        //   1. A creates region X (name N).
        //   2. A calls A.unlink() — name N removed; A's drop suppressed.
        //   3. B creates a fresh region with name N (succeeds — name
        //      was cleared in step 2).
        //   4. A calls A.unlink() AGAIN (stale call from a process
        //      that doesn't realize B has taken over).
        //   5. Verify B's name still resolves — A's second unlink
        //      MUST be a no-op, not a stray libc::shm_unlink.
        let handle = unique_handle("stale-double-unlink");
        let mut owner_a = Region::create(&handle, 2, 64, 60_000_000, false).expect("A create");
        owner_a.unlink().expect("A first unlink");

        // B creates fresh region with the same name.
        let owner_b =
            Region::create(&handle, 4, 128, 60_000_000, false).expect("B create after A unlink");

        // A's STALE second unlink must not touch B's name.
        owner_a.unlink().expect("A second unlink should be a no-op");

        // B's name must still resolve.
        let attacher = Region::attach(&handle, 4, 128).expect("attach B after A's stale 2nd unlink");
        assert_eq!(attacher.slot_count(), 4);
        assert_eq!(attacher.slot_size_bytes(), 128);
        drop(attacher);

        // Drop A — also must not touch the OS name (covered by the
        // sibling handoff-safety test, repeated here for thoroughness).
        drop(owner_a);
        let attacher2 = Region::attach(&handle, 4, 128).expect("attach B after A's drop");
        drop(attacher2);
        drop(owner_b);
    }

    #[test]
    fn unlink_rejects_attacher_calls() {
        // Only the creator may unlink. Attacher-side unlink would yank
        // the name out from under the live creator — that's an API
        // misuse the lifecycle contract forbids.
        let handle = unique_handle("attacher-unlink-rejected");
        let _creator = Region::create(&handle, 4, 64, 60_000_000, false).expect("create");
        let mut attacher = Region::attach(&handle, 4, 64).expect("attach");
        let err = attacher.unlink().unwrap_err();
        match err {
            TesseraPoolError::Region(msg) => {
                assert!(
                    msg.contains("attacher") || msg.contains("Only the creator"),
                    "expected attacher-rejection error, got: {msg}"
                );
            }
            other => panic!("expected Region error, got {other:?}"),
        }
    }
}
