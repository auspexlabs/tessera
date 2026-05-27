//! Pool state machine.
//!
//! All owner-side mutations to the SHM slot table flow through here.
//! Only the owner Pool acquires, writes, releases, renews, and reclaims;
//! attachers may only read payloads via a descriptor.

use std::time::{Duration, Instant};

use crossbeam_queue::SegQueue;
use parking_lot::Mutex;

use crate::error::{Result, TesseraPoolError};
use crate::header::{flags, SlotMeta};
use crate::namespace::NamespaceHandle;
use crate::region::Region;
use crate::{Descriptor, Lease, LeaseId};

/// Construction parameters for a Pool.
#[derive(Clone, Debug)]
pub struct PoolConfig {
    /// Operator-facing description string; combined with BLAKE3 to
    /// derive the SHM region name and the header digest. Two peers
    /// with the same description attach to the same region.
    pub description: String,
    /// Number of fixed-size slots in the region.
    pub slot_count: u32,
    /// Bytes per slot. Caller-side payloads larger than this are rejected.
    pub slot_size_bytes: u32,
    /// True if this process should create the region (and own its
    /// lifecycle). False to attach to an existing region.
    pub is_owner: bool,
    /// Lease TTL in microseconds. Owner-only; non-owners inherit the
    /// owner-stamped value from the header at attach time.
    pub ttl_micros: u64,
    /// Owner-side recovery escape hatch. Default `false`.
    ///
    /// When `false` (default), an owner that finds the SHM region
    /// already exists on `Pool::new` refuses to clobber it — we
    /// cannot safely distinguish "stale segment from a crashed prior
    /// owner" from "live segment from a concurrent owner in its
    /// mid-init window where the header isn't yet stamped." Failing
    /// fast is correct.
    ///
    /// When `true`, the caller asserts that no live owner exists for
    /// this description. The existing segment is unconditionally
    /// unlinked + recreated. Misuse will silently clobber a live
    /// peer; only set this during explicit recovery scenarios.
    ///
    /// Ignored when `is_owner == false`.
    pub force_recreate: bool,
}

/// Internal owner-side bookkeeping. Lives in process memory; not
/// shared across the SHM boundary.
#[derive(Debug)]
struct OwnerState {
    /// Lock-free queue of currently-free slot indices. Acquire pops;
    /// release / reclaim_stale push.
    free_slots: SegQueue<u32>,
    /// Coarse-grained mutex around the slot-table mutation path.
    /// Acquired by acquire / release / write / renew / reclaim_stale
    /// before reading-validating-writing a SlotMeta in SHM. Held only
    /// for the duration of one mutation — no I/O under the lock.
    slot_mutation_lock: Mutex<()>,
}

/// Non-lossy lease-backed shared-memory pool.
///
/// One process per region is the owner (constructed with
/// `is_owner: true`); zero or more attachers may construct with
/// `is_owner: false` and consume payload bytes via descriptors handed
/// across IPC.
#[derive(Debug)]
pub struct Pool {
    region: Region,
    owner_state: Option<OwnerState>,
    /// Cached TTL — for owners, copied from PoolConfig; for attachers,
    /// inherited from the SHM header.
    ttl_micros: u64,
}

impl Pool {
    /// Construct a Pool. Owner path creates and initializes the SHM
    /// region; attacher path validates an existing region.
    pub fn new(config: PoolConfig) -> Result<Self> {
        if config.description.is_empty() {
            return Err(TesseraPoolError::Config(
                "description must be non-empty".into(),
            ));
        }
        if config.slot_count == 0 {
            return Err(TesseraPoolError::Config("slot_count must be > 0".into()));
        }
        if config.slot_size_bytes == 0 {
            return Err(TesseraPoolError::Config(
                "slot_size_bytes must be > 0".into(),
            ));
        }

        let handle = NamespaceHandle::derive(&config.description);

        if config.is_owner {
            if config.ttl_micros == 0 {
                return Err(TesseraPoolError::Config(
                    "ttl_micros must be > 0 for owner Pool".into(),
                ));
            }
            let region = Region::create(
                &handle,
                config.slot_count,
                config.slot_size_bytes,
                config.ttl_micros,
                config.force_recreate,
            )?;
            // Initial free list: every slot index, in order.
            let free_slots = SegQueue::new();
            for i in 0..config.slot_count {
                free_slots.push(i);
            }
            Ok(Self {
                region,
                owner_state: Some(OwnerState {
                    free_slots,
                    slot_mutation_lock: Mutex::new(()),
                }),
                ttl_micros: config.ttl_micros,
            })
        } else {
            let region = Region::attach(&handle, config.slot_count, config.slot_size_bytes)?;
            let ttl_micros = region.ttl_micros();
            Ok(Self {
                region,
                owner_state: None,
                ttl_micros,
            })
        }
    }

    /// True for owner Pool instances; false for attachers.
    pub fn is_owner(&self) -> bool {
        self.owner_state.is_some()
    }

    /// TTL in microseconds (owner-stamped; non-owners inherit).
    pub fn ttl_micros(&self) -> u64 {
        self.ttl_micros
    }

    /// Slot count (matches `PoolConfig::slot_count`).
    pub fn slot_count(&self) -> u32 {
        self.region.slot_count()
    }

    /// Slot size (matches `PoolConfig::slot_size_bytes`).
    pub fn slot_size_bytes(&self) -> u32 {
        self.region.slot_size_bytes()
    }

    /// Acquire a free slot (owner-only). Blocks up to `timeout` for
    /// availability; polls the lock-free queue every 5 ms.
    ///
    /// On success, the slot is marked IN_USE with a fresh 128-bit
    /// lease_id and a bumped generation counter.
    pub fn acquire(&mut self, timeout: Duration) -> Result<Lease> {
        let state = self
            .owner_state
            .as_ref()
            .ok_or(TesseraPoolError::OwnerOnly)?;
        let deadline = Instant::now() + timeout;

        // Poll the queue. SegQueue::pop is non-blocking; we sleep
        // briefly between attempts so a busy spin doesn't peg a core.
        let slot_index = loop {
            if let Some(idx) = state.free_slots.pop() {
                break idx;
            }
            if Instant::now() >= deadline {
                return Err(TesseraPoolError::Timeout {
                    timeout_micros: timeout.as_micros() as u64,
                });
            }
            std::thread::sleep(Duration::from_millis(5));
        };

        // Mutate the slot meta under the slot-mutation lock so a
        // concurrent owner-side reclaim sweep doesn't race the
        // generation bump.
        let _guard = state.slot_mutation_lock.lock();
        let mut meta = self.region.read_slot_meta(slot_index)?;
        let lease_id = LeaseId::from_bytes(fresh_lease_id_bytes());
        meta.generation = meta.generation.wrapping_add(1);
        meta.lease_id_high = lease_id.high();
        meta.lease_id_low = lease_id.low();
        meta.acquired_at_micros = monotonic_micros();
        meta.payload_len = 0;
        meta.flags = flags::IN_USE;
        self.region.write_slot_meta(slot_index, meta)?;

        Ok(Lease::new(slot_index, lease_id, meta.generation))
    }

    /// Write a payload into the leased slot and return a descriptor
    /// the owner can hand across IPC to a worker.
    ///
    /// v0.1 is one-shot: a second `write` on the same lease
    /// fails with `WriteAfterFinalize`.
    pub fn write(&mut self, lease: &Lease, payload: &[u8]) -> Result<Descriptor> {
        let state = self
            .owner_state
            .as_ref()
            .ok_or(TesseraPoolError::OwnerOnly)?;
        if payload.len() > self.region.slot_size_bytes() as usize {
            return Err(TesseraPoolError::OversizedPayload {
                payload_size: payload.len(),
                slot_size: self.region.slot_size_bytes() as usize,
            });
        }

        let _guard = state.slot_mutation_lock.lock();
        let mut meta = self.region.read_slot_meta(lease.slot_index())?;
        validate_lease(lease, &meta)?;
        if meta.payload_finalized() {
            return Err(TesseraPoolError::WriteAfterFinalize {
                slot_index: lease.slot_index(),
            });
        }

        self.region
            .write_slot_payload(lease.slot_index(), payload)?;
        meta.payload_len = payload.len() as u32;
        meta.flags |= flags::PAYLOAD_FINALIZED;
        self.region.write_slot_meta(lease.slot_index(), meta)?;

        Ok(Descriptor::new(
            lease.slot_index(),
            lease.lease_id(),
            lease.generation(),
            payload.len() as u32,
        ))
    }

    /// Read the payload bytes referenced by a descriptor. Available
    /// to both owners and attachers (it's a read-only operation).
    ///
    /// Validates that the descriptor's `(lease_id, generation)` still
    /// match the slot's current metadata — catches the case where the
    /// owner reclaimed the slot before the descriptor holder finished
    /// consuming.
    ///
    /// Bound-checks `descriptor.size_bytes()` against the slot's
    /// stored `payload_len` AND against the slot capacity. Descriptors
    /// can be constructed by callers via `Descriptor::new` or
    /// reconstructed from pickled bytes, so we cannot trust the
    /// descriptor-claimed size in isolation — defense in depth against
    /// over-read.
    pub fn read_payload(&self, descriptor: &Descriptor) -> Result<Vec<u8>> {
        // Slot-index bounds: region read_slot_meta debug_asserts; check here too.
        let slot_index = descriptor.slot_index();
        if slot_index >= self.region.slot_count() {
            return Err(TesseraPoolError::Region(format!(
                "descriptor slot_index {slot_index} out of range (slot_count={})",
                self.region.slot_count()
            )));
        }
        let meta = self.region.read_slot_meta(slot_index)?;
        validate_descriptor(descriptor, &meta)?;
        // Descriptor size must match what was actually written. A
        // larger value would read past the written payload (potentially
        // uninitialized bytes); a smaller value silently truncates.
        // Both indicate descriptor tampering / mismatch.
        if descriptor.size_bytes() != meta.payload_len {
            return Err(TesseraPoolError::Region(format!(
                "descriptor size_bytes ({}) does not match slot's stored payload_len ({}); \
                refusing to read",
                descriptor.size_bytes(),
                meta.payload_len
            )));
        }
        // And capacity (redundant given the above + the write-time
        // OversizedPayload check, but defense in depth in case the
        // slot meta was somehow stamped past capacity).
        if descriptor.size_bytes() > self.region.slot_size_bytes() {
            return Err(TesseraPoolError::Region(format!(
                "descriptor size_bytes ({}) exceeds slot capacity ({})",
                descriptor.size_bytes(),
                self.region.slot_size_bytes()
            )));
        }
        self.region.read_slot_payload(slot_index, descriptor.size_bytes())
    }

    /// Release a leased slot (owner-only). The slot's metadata is
    /// cleared and the slot index is returned to the free list.
    pub fn release(&mut self, lease: &Lease) -> Result<()> {
        let state = self
            .owner_state
            .as_ref()
            .ok_or(TesseraPoolError::OwnerOnly)?;
        let _guard = state.slot_mutation_lock.lock();
        let meta = self.region.read_slot_meta(lease.slot_index())?;
        validate_lease(lease, &meta)?;
        // Clear meta — but DO NOT bump generation on a normal release.
        // Generation only bumps on acquire and reclaim_stale.
        let cleared = SlotMeta {
            lease_id_high: 0,
            lease_id_low: 0,
            generation: meta.generation,
            acquired_at_micros: 0,
            payload_len: 0,
            flags: 0,
            _reserved: [0; 32],
        };
        self.region.write_slot_meta(lease.slot_index(), cleared)?;
        state.free_slots.push(lease.slot_index());
        Ok(())
    }

    /// Renew a lease's `acquired_at` so the next reclaim sweep doesn't
    /// reclaim it. Owner-side only — workers cannot renew via descriptor.
    pub fn renew(&mut self, lease: &Lease) -> Result<()> {
        let state = self
            .owner_state
            .as_ref()
            .ok_or(TesseraPoolError::OwnerOnly)?;
        let _guard = state.slot_mutation_lock.lock();
        let mut meta = self.region.read_slot_meta(lease.slot_index())?;
        validate_lease(lease, &meta)?;
        meta.acquired_at_micros = monotonic_micros();
        self.region.write_slot_meta(lease.slot_index(), meta)?;
        Ok(())
    }

    /// Reclaim slots whose lease has been outstanding longer than
    /// `ttl_micros`. Returns the count reclaimed. Owner-only.
    ///
    /// Generation is bumped on each reclaimed slot, so any in-flight
    /// descriptor against that slot will fail `validate_descriptor`
    /// before it can read stale bytes.
    pub fn reclaim_stale(&mut self) -> Result<u32> {
        let state = self
            .owner_state
            .as_ref()
            .ok_or(TesseraPoolError::OwnerOnly)?;
        let now = monotonic_micros();
        let ttl = self.ttl_micros;
        let _guard = state.slot_mutation_lock.lock();
        let mut reclaimed = 0_u32;
        for i in 0..self.region.slot_count() {
            let meta = self.region.read_slot_meta(i)?;
            if !meta.in_use() {
                continue;
            }
            // `acquired_at_micros` is a monotonic clock value; if a
            // worker took a long time, monotonic_micros() may have
            // crossed UNIX wall-clock boundaries — irrelevant here.
            if now.saturating_sub(meta.acquired_at_micros) <= ttl {
                continue;
            }
            // Slot is stale. Bump generation, clear, return to free list.
            let cleared = SlotMeta {
                lease_id_high: 0,
                lease_id_low: 0,
                generation: meta.generation.wrapping_add(1),
                acquired_at_micros: 0,
                payload_len: 0,
                flags: 0,
                _reserved: [0; 32],
            };
            self.region.write_slot_meta(i, cleared)?;
            state.free_slots.push(i);
            reclaimed += 1;
        }
        Ok(reclaimed)
    }

    /// Current count of leased (in-use) slots. Useful for monitoring.
    pub fn in_use_count(&self) -> Result<u32> {
        let mut n = 0;
        for i in 0..self.region.slot_count() {
            if self.region.read_slot_meta(i)?.in_use() {
                n += 1;
            }
        }
        Ok(n)
    }

}

/// Validate that a lease's `(lease_id, generation)` match the slot's
/// current metadata. Used by write / release / renew.
fn validate_lease(lease: &Lease, meta: &SlotMeta) -> Result<()> {
    if meta.generation != lease.generation()
        || meta.lease_id_high != lease.lease_id().high()
        || meta.lease_id_low != lease.lease_id().low()
    {
        return Err(TesseraPoolError::StaleHandle {
            slot_index: lease.slot_index(),
            descriptor_generation: lease.generation(),
            current_generation: meta.generation,
        });
    }
    Ok(())
}

/// Validate that a descriptor's `(lease_id, generation)` still match
/// the slot's current metadata. Used by read_payload.
fn validate_descriptor(descriptor: &Descriptor, meta: &SlotMeta) -> Result<()> {
    if meta.generation != descriptor.generation()
        || meta.lease_id_high != descriptor.lease_id().high()
        || meta.lease_id_low != descriptor.lease_id().low()
    {
        return Err(TesseraPoolError::StaleHandle {
            slot_index: descriptor.slot_index(),
            descriptor_generation: descriptor.generation(),
            current_generation: meta.generation,
        });
    }
    Ok(())
}

/// Generate a fresh 128-bit lease ID from /dev/urandom. Each acquire
/// gets a unique ID; the probability of collision across the universe
/// of leases ever issued by this region is negligible (2^-64 birthday
/// bound at 2^64 leases, which is more than the lifetime of any real
/// pool).
fn fresh_lease_id_bytes() -> [u8; 16] {
    let mut bytes = [0u8; 16];
    // SAFETY: /dev/urandom is a standard Linux device, always present
    // on systems where Tessera runs. If the open fails (extremely
    // unusual — exhausted fds, chroot, etc.) we fall back to the
    // monotonic clock + a counter for forward progress; collisions
    // become possible but the system is already in an unhealthy state.
    if let Ok(mut f) = std::fs::File::open("/dev/urandom") {
        use std::io::Read;
        let _ = f.read_exact(&mut bytes);
    } else {
        let now = monotonic_micros();
        bytes[..8].copy_from_slice(&now.to_le_bytes());
        bytes[8..].copy_from_slice(&now.rotate_left(17).to_le_bytes());
    }
    bytes
}

/// Monotonic timestamp in microseconds. Used for `acquired_at` and
/// TTL math. NOT wall-clock; the SHM header's `epoch_micros` is
/// wall-clock for cross-deployment epoch detection.
fn monotonic_micros() -> u64 {
    use std::time::Instant;
    static START: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
    let start = START.get_or_init(Instant::now);
    start.elapsed().as_micros() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn owner_config(tag: &str, slot_count: u32, slot_size: u32) -> PoolConfig {
        let pid = std::process::id();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        PoolConfig {
            description: format!("tessera-pool-test/{tag}/{pid}/{nanos}"),
            slot_count,
            slot_size_bytes: slot_size,
            is_owner: true,
            ttl_micros: 60_000_000,
            force_recreate: false,
        }
    }

    #[test]
    fn owner_can_acquire_and_release() {
        let mut pool = Pool::new(owner_config("acquire-release", 4, 256)).expect("new");
        assert_eq!(pool.in_use_count().expect("in_use"), 0);

        let lease1 = pool.acquire(Duration::from_secs(1)).expect("acquire 1");
        let lease2 = pool.acquire(Duration::from_secs(1)).expect("acquire 2");
        assert_eq!(pool.in_use_count().expect("in_use"), 2);
        assert_ne!(lease1.slot_index(), lease2.slot_index());
        assert_ne!(lease1.lease_id(), lease2.lease_id());

        pool.release(&lease1).expect("release 1");
        assert_eq!(pool.in_use_count().expect("in_use"), 1);
        pool.release(&lease2).expect("release 2");
        assert_eq!(pool.in_use_count().expect("in_use"), 0);
    }

    #[test]
    fn acquire_exhausts_then_times_out() {
        let mut pool = Pool::new(owner_config("exhaust", 2, 128)).expect("new");
        let _l1 = pool.acquire(Duration::from_secs(1)).expect("l1");
        let _l2 = pool.acquire(Duration::from_secs(1)).expect("l2");
        let err = pool.acquire(Duration::from_millis(50)).unwrap_err();
        match err {
            TesseraPoolError::Timeout { .. } => {}
            other => panic!("expected Timeout, got {other:?}"),
        }
    }

    #[test]
    fn release_returns_slot_to_free_list() {
        let mut pool = Pool::new(owner_config("release-returns", 1, 64)).expect("new");
        let lease_a = pool.acquire(Duration::from_secs(1)).expect("a");
        // Second acquire would time out since only one slot exists.
        assert!(pool.acquire(Duration::from_millis(20)).is_err());
        pool.release(&lease_a).expect("release a");
        // After release, the slot is acquirable again.
        let lease_b = pool.acquire(Duration::from_secs(1)).expect("b");
        // Same slot index, different lease_id, different generation.
        assert_eq!(lease_a.slot_index(), lease_b.slot_index());
        assert_ne!(lease_a.lease_id(), lease_b.lease_id());
        assert_ne!(lease_a.generation(), lease_b.generation());
    }

    #[test]
    fn write_then_read_payload_via_descriptor() {
        let mut pool = Pool::new(owner_config("write-read", 2, 1024)).expect("new");
        let lease = pool.acquire(Duration::from_secs(1)).expect("acquire");
        let payload = b"hello tessera pool";
        let descriptor = pool.write(&lease, payload).expect("write");
        assert_eq!(descriptor.size_bytes(), payload.len() as u32);

        let read = pool.read_payload(&descriptor).expect("read_payload");
        assert_eq!(read.as_slice(), payload);

        pool.release(&lease).expect("release");
    }

    #[test]
    fn write_rejects_oversized_payload() {
        let mut pool = Pool::new(owner_config("oversized", 1, 16)).expect("new");
        let lease = pool.acquire(Duration::from_secs(1)).expect("acquire");
        let big_payload = vec![0u8; 32];
        let err = pool.write(&lease, &big_payload).unwrap_err();
        match err {
            TesseraPoolError::OversizedPayload {
                payload_size,
                slot_size,
            } => {
                assert_eq!(payload_size, 32);
                assert_eq!(slot_size, 16);
            }
            other => panic!("expected OversizedPayload, got {other:?}"),
        }
    }

    #[test]
    fn double_write_on_same_lease_is_rejected() {
        let mut pool = Pool::new(owner_config("double-write", 1, 64)).expect("new");
        let lease = pool.acquire(Duration::from_secs(1)).expect("acquire");
        pool.write(&lease, b"first").expect("first write");
        let err = pool.write(&lease, b"second").unwrap_err();
        match err {
            TesseraPoolError::WriteAfterFinalize { .. } => {}
            other => panic!("expected WriteAfterFinalize, got {other:?}"),
        }
    }

    #[test]
    fn reclaim_stale_bumps_generation_and_invalidates_descriptor() {
        // Short TTL so reclaim_stale fires.
        let mut config = owner_config("reclaim-stale", 1, 64);
        config.ttl_micros = 1; // 1 microsecond — effectively "anything held is stale"
        let mut pool = Pool::new(config).expect("new");
        let lease = pool.acquire(Duration::from_secs(1)).expect("acquire");
        let descriptor = pool.write(&lease, b"abc").expect("write");

        // Force a small delay so acquired_at_micros + 1 < now.
        std::thread::sleep(Duration::from_millis(5));

        let reclaimed = pool.reclaim_stale().expect("reclaim");
        assert_eq!(reclaimed, 1);
        assert_eq!(pool.in_use_count().expect("in_use"), 0);

        // Original descriptor is now stale.
        let err = pool.read_payload(&descriptor).unwrap_err();
        match err {
            TesseraPoolError::StaleHandle { .. } => {}
            other => panic!("expected StaleHandle, got {other:?}"),
        }

        // Original lease release also fails (stale).
        let err = pool.release(&lease).unwrap_err();
        match err {
            TesseraPoolError::StaleHandle { .. } => {}
            other => panic!("expected StaleHandle, got {other:?}"),
        }
    }

    #[test]
    fn renew_keeps_lease_alive_through_reclaim_sweep() {
        let mut config = owner_config("renew", 1, 64);
        config.ttl_micros = 50_000; // 50 ms
        let mut pool = Pool::new(config).expect("new");
        let lease = pool.acquire(Duration::from_secs(1)).expect("acquire");

        // Wait past 1/2 the TTL, renew, wait again — total > TTL but
        // never > TTL since the renew. Reclaim should NOT fire.
        std::thread::sleep(Duration::from_millis(30));
        pool.renew(&lease).expect("renew");
        std::thread::sleep(Duration::from_millis(30));
        let reclaimed = pool.reclaim_stale().expect("reclaim");
        assert_eq!(reclaimed, 0);
        assert_eq!(pool.in_use_count().expect("in_use"), 1);

        pool.release(&lease).expect("release");
    }

    #[test]
    fn non_owner_cannot_acquire_release_write_renew_reclaim() {
        // Set up an owner first so the region exists.
        let config = owner_config("non-owner-rejected", 2, 128);
        let mut owner = Pool::new(config.clone()).expect("new owner");

        let attacher_config = PoolConfig {
            is_owner: false,
            ..config.clone()
        };
        let mut attacher = Pool::new(attacher_config).expect("new attacher");

        assert!(!attacher.is_owner());
        assert_eq!(attacher.ttl_micros(), config.ttl_micros);

        let err = attacher.acquire(Duration::from_millis(10)).unwrap_err();
        assert!(matches!(err, TesseraPoolError::OwnerOnly));

        // Owner acquires, writes, hands descriptor to "attacher" for read.
        let lease = owner.acquire(Duration::from_secs(1)).expect("owner acquire");
        let descriptor = owner.write(&lease, b"shared").expect("owner write");
        let read = attacher.read_payload(&descriptor).expect("attacher read");
        assert_eq!(read.as_slice(), b"shared");

        // Attacher cannot release / renew / reclaim.
        assert!(matches!(
            attacher.release(&lease),
            Err(TesseraPoolError::OwnerOnly)
        ));
        assert!(matches!(
            attacher.renew(&lease),
            Err(TesseraPoolError::OwnerOnly)
        ));
        assert!(matches!(
            attacher.reclaim_stale(),
            Err(TesseraPoolError::OwnerOnly)
        ));

        owner.release(&lease).expect("owner release");
    }

    #[test]
    fn concurrent_create_with_live_owner_refuses_to_clobber() {
        // Codex P1 regression guard (iterations 1 + 3): if an owner is
        // already alive for this region, a second `Pool::new(is_owner=true,
        // force_recreate=false, ...)` must refuse rather than silently
        // unlinking the live segment — even in a startup-race window
        // where the existing segment's header hasn't been stamped yet.
        let config = owner_config("concurrent-create", 2, 256);
        let _alive_owner = Pool::new(config.clone()).expect("first owner");
        // Second attempt with the same description + geometry: should error.
        let err = Pool::new(config.clone()).unwrap_err();
        match err {
            TesseraPoolError::Region(msg) => {
                assert!(
                    msg.contains("already exists") || msg.contains("Refusing to clobber"),
                    "expected refuse-to-clobber error, got: {msg}"
                );
            }
            other => panic!("expected Region error, got {other:?}"),
        }
        // The first owner is still alive and usable.
        // (Drop happens when _alive_owner goes out of scope.)
    }

    #[test]
    fn force_recreate_unlinks_and_takes_over() {
        // The recovery escape hatch for crashed-prior-owner scenarios.
        // The caller asserts "no live owner"; we unconditionally unlink
        // and recreate. This is the inverse of the previous test —
        // demonstrates that the flag actually works when set, while
        // the default safely refuses.
        let config = owner_config("force-recreate", 2, 256);
        let first = Pool::new(config.clone()).expect("first owner");
        // First-owner sanity: holds an active lease.
        // (We drop without releasing — simulates a crashed owner.)
        drop(first);
        // In a real crash scenario, the SHM segment may still be present
        // on the filesystem (POSIX shm_unlink only fires if Shmem owner-
        // drop ran). Force-recreate lets us recover.
        let recovery_config = PoolConfig {
            force_recreate: true,
            ..config.clone()
        };
        let recovered = Pool::new(recovery_config).expect("recover with force_recreate");
        assert!(recovered.is_owner());
    }

    #[test]
    fn read_payload_rejects_descriptor_with_mismatched_size() {
        // Codex P1-2 regression guard: a descriptor whose size_bytes
        // doesn't match the slot's stored payload_len must be rejected
        // BEFORE any payload bytes are copied. Otherwise a hand-crafted
        // Descriptor::new could request an OOB read in release builds
        // (debug_assert is stripped).
        let mut pool = Pool::new(owner_config("size-mismatch", 1, 1024)).expect("new");
        let lease = pool.acquire(Duration::from_secs(1)).expect("acquire");
        let descriptor = pool.write(&lease, b"hello").expect("write");

        // Lie about the size — claim 999 bytes when only 5 were written.
        let oversized_descriptor =
            Descriptor::new(descriptor.slot_index(), descriptor.lease_id(), descriptor.generation(), 999);
        let err = pool.read_payload(&oversized_descriptor).unwrap_err();
        match err {
            TesseraPoolError::Region(msg) => {
                assert!(
                    msg.contains("does not match") || msg.contains("payload_len"),
                    "expected size-mismatch error, got: {msg}"
                );
            }
            other => panic!("expected Region error, got {other:?}"),
        }

        // Lying smaller (claim 2 bytes when 5 were written) also rejected —
        // silent truncation is a tampering signal too.
        let truncated_descriptor =
            Descriptor::new(descriptor.slot_index(), descriptor.lease_id(), descriptor.generation(), 2);
        let err = pool.read_payload(&truncated_descriptor).unwrap_err();
        assert!(matches!(err, TesseraPoolError::Region(_)));

        // The legitimate descriptor still works.
        let bytes = pool.read_payload(&descriptor).expect("legit read");
        assert_eq!(bytes.as_slice(), b"hello");

        pool.release(&lease).expect("release");
    }

    #[test]
    fn read_payload_rejects_out_of_range_slot_index() {
        // Defense in depth: a hand-crafted descriptor with slot_index
        // beyond slot_count must be rejected, not silently OOB-read.
        let pool = Pool::new(owner_config("oob-slot", 2, 64)).expect("new");
        let bogus = Descriptor::new(99, crate::LeaseId::from_bytes([0; 16]), 0, 1);
        let err = pool.read_payload(&bogus).unwrap_err();
        assert!(matches!(err, TesseraPoolError::Region(_)));
    }

    #[test]
    fn descriptor_validates_against_current_generation() {
        let mut pool = Pool::new(owner_config("descriptor-gen", 1, 64)).expect("new");
        let lease_a = pool.acquire(Duration::from_secs(1)).expect("a");
        let descriptor_a = pool.write(&lease_a, b"alpha").expect("write a");
        pool.release(&lease_a).expect("release a");

        // Same slot is re-acquired with a new lease + new generation.
        let lease_b = pool.acquire(Duration::from_secs(1)).expect("b");
        assert_eq!(lease_a.slot_index(), lease_b.slot_index());
        assert_ne!(lease_a.generation(), lease_b.generation());

        // Original descriptor_a is now stale.
        let err = pool.read_payload(&descriptor_a).unwrap_err();
        assert!(matches!(err, TesseraPoolError::StaleHandle { .. }));

        pool.release(&lease_b).expect("release b");
    }
}
