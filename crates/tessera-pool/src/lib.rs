//! Tessera Pool — non-lossy lease-backed shared-memory pool primitive.
//!
//! v0.0.1 SCAFFOLD ONLY. Implementations land in Stage 4a of the
//! upstream extraction plan; see the Tessera README for the planned
//! surface (single-owner lifecycle, single-writer-lease, timeout
//! reclaim, BLAKE3-derived namespace handles).

#![allow(dead_code)]

/// Pool of fixed-size shared-memory slots. Single owner; non-owner
/// processes attach by description.
pub struct Pool {
    _placeholder: (),
}

/// Lease handle returned by `Pool::acquire`. Carries
/// `(slot_index, lease_id, generation)` so the pool can validate
/// release operations against stale handles after a timeout reclaim.
pub struct Lease {
    _placeholder: (),
}

/// Descriptor returned by `Pool::write` for hand-off across IPC
/// channels. Read-only token; cannot be used to renew or release —
/// those operations stay with the owner-side Lease.
pub struct Descriptor {
    _placeholder: (),
}
