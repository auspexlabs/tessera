//! Tessera Sink — atomic-write worker pool to disk, built on tessera-pool.
//!
//! v0.0.1 SCAFFOLD ONLY. Implementations land in Stage 4c of the
//! upstream extraction plan; see the Tessera README for the planned
//! surface (owner-held leases with worker ack/cancel channels, chunked
//! streaming with worker affinity, atomic temp+rename, XXHash integrity).

#![allow(dead_code)]

// Re-export the underlying primitive so consumers can `use tessera_sink::Pool;`
// when they want both Sink and Pool from one import in v0.0.1. After v0.1.0
// the surface settles; this convenience re-export may or may not survive.
pub use tessera_pool;

/// Worker pool that streams payloads from Pool slots to disk atomically.
pub struct Sink {
    _placeholder: (),
}

/// Job descriptor passed across the producer → worker control channel.
pub struct WriteJob {
    _placeholder: (),
}
