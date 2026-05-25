//! Tessera Pool — non-lossy lease-backed shared-memory pool primitive.
//!
//! See the workspace README for the design summary; the per-section
//! references in this crate's source point at the upstream side-doc
//! `mp_tools_open_source_extraction_2026-05-23.md`.
//!
//! Stage 4a (in progress): public types + region layout land first;
//! state machine (`acquire` / `write` / `release` / `reclaim_stale` /
//! `renew`) lands in follow-up commits.

#![deny(missing_docs)]
#![deny(unsafe_op_in_unsafe_fn)]

pub mod error;
pub mod header;
pub mod namespace;
pub mod pool;
pub mod region;

pub use error::{Result, TesseraPoolError};
pub use namespace::NamespaceHandle;
pub use pool::{Pool, PoolConfig};

/// 128-bit lease identifier returned by `Pool::acquire`.
///
/// Stored as two `u64` halves so it Pods-cleanly into `SlotMeta`. The
/// `Display` impl renders as 32 hex chars; you can construct one from
/// any 16 bytes via `LeaseId::from_bytes`.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct LeaseId {
    high: u64,
    low: u64,
}

impl LeaseId {
    /// Construct from raw bytes (e.g. drawn from a secure RNG at
    /// `acquire` time).
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        let high = u64::from_le_bytes(bytes[..8].try_into().expect("8 bytes"));
        let low = u64::from_le_bytes(bytes[8..].try_into().expect("8 bytes"));
        Self { high, low }
    }

    /// High 64 bits — for SHM serialization into `SlotMeta`.
    pub fn high(self) -> u64 {
        self.high
    }

    /// Low 64 bits.
    pub fn low(self) -> u64 {
        self.low
    }

    /// 16-byte representation suitable for over-the-wire / in-SHM use.
    pub fn to_bytes(self) -> [u8; 16] {
        let mut out = [0u8; 16];
        out[..8].copy_from_slice(&self.high.to_le_bytes());
        out[8..].copy_from_slice(&self.low.to_le_bytes());
        out
    }
}

impl core::fmt::Display for LeaseId {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        for byte in self.to_bytes() {
            write!(f, "{:02x}", byte)?;
        }
        Ok(())
    }
}

/// Owner-side lease handle returned by `Pool::acquire`.
///
/// Carries everything the owner needs to validate later operations:
/// the slot index, the 128-bit lease ID stamped at acquire, and the
/// generation counter at acquire time. Owner-only; do not hand across
/// IPC — use `Descriptor` for that.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Lease {
    slot_index: u32,
    lease_id: LeaseId,
    generation: u64,
}

impl Lease {
    /// Construct a lease (internal use by `Pool::acquire`).
    pub fn new(slot_index: u32, lease_id: LeaseId, generation: u64) -> Self {
        Self {
            slot_index,
            lease_id,
            generation,
        }
    }

    /// Slot index this lease covers.
    pub fn slot_index(self) -> u32 {
        self.slot_index
    }

    /// 128-bit lease ID.
    pub fn lease_id(self) -> LeaseId {
        self.lease_id
    }

    /// Generation counter at acquire time. The owner-side write
    /// / release / renew APIs validate that this matches the slot's
    /// current generation in SHM.
    pub fn generation(self) -> u64 {
        self.generation
    }
}

/// Read-only handoff token passed across IPC channels to worker
/// subprocesses (or paired-container peers).
///
/// Carries the same identifying fields as `Lease` but does NOT entitle
/// the holder to release or renew — those operations stay with the
/// owner per §3.5.c single-writer-lease lock. A worker validates a
/// descriptor by attaching to the region, looking up `slot_index`'s
/// metadata, and confirming `(lease_id, generation)` still match.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Descriptor {
    slot_index: u32,
    lease_id: LeaseId,
    generation: u64,
    size_bytes: u32,
}

impl Descriptor {
    /// Construct (internal use by `Pool::write`).
    pub fn new(slot_index: u32, lease_id: LeaseId, generation: u64, size_bytes: u32) -> Self {
        Self {
            slot_index,
            lease_id,
            generation,
            size_bytes,
        }
    }

    /// Slot index referenced by this descriptor.
    pub fn slot_index(self) -> u32 {
        self.slot_index
    }

    /// Lease ID that wrote this payload.
    pub fn lease_id(self) -> LeaseId {
        self.lease_id
    }

    /// Generation at write time.
    pub fn generation(self) -> u64 {
        self.generation
    }

    /// Size of the written payload, in bytes.
    pub fn size_bytes(self) -> u32 {
        self.size_bytes
    }
}

