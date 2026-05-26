//! Tessera Sink — atomic-write worker pool to disk.
//!
//! Sink is the first *composite service* in Tessera: it does not own
//! its own SHM wire format. Instead it wires together the three
//! primitives —
//!
//! - [`tessera_pool::Pool`] for zero-copy payload handoff (payloads
//!   larger than one slot are split into chunks),
//! - one [`tessera_channel::Channel`] per worker as the **control
//!   plane** (owner → worker `ChunkDescriptor` / `Commit` / `Cancel`),
//! - one shared [`tessera_channel::Channel`] as the **ack plane**
//!   (worker → owner `ChunkAck` / `ChunkFailed` / `CancelAck` /
//!   `JobComplete`),
//!
//! — plus N worker OS subprocesses that stream chunks to a temp file
//! and atomically rename into place on commit, with BLAKE3 integrity
//! verification.
//!
//! See the workspace README and `docs/concept_landscape.md` for where
//! Sink sits relative to the primitives; the design is specified in
//! the upstream side-doc `mp_tools_open_source_extraction_2026-05-23.md`
//! (§3.4 Rust-from-start, §3.5 cross-process SHM, §4d handoff
//! pseudocode) in the Bayence-Certus repo.
//!
//! ## Region ownership
//!
//! Channel's locked rule is *the Receiver creates the region*. That
//! maps the two planes cleanly, and the consistent rule across the
//! whole Sink is **the reader owns its region**:
//!
//! | Region            | Reader   | Creator (owns lifecycle) |
//! |-------------------|----------|--------------------------|
//! | ack channel       | owner    | owner (Receiver)         |
//! | control channel i | worker i | worker i (Receiver)      |
//! | pool              | workers  | owner (lease authority)  |
//!
//! The Pool is the one exception: its owner is the single writer
//! (§3.5.c single-writer-lease), not a reader — workers attach and
//! read payloads via descriptors handed across the control channel.
//!
//! The owner ([`Sink`]) is single-threaded by necessity (Pool/Channel
//! wrap a `!Send` `Shmem`): it cooperatively drains the ack plane and
//! renews leases inside `submit` / `flush`. Worker subprocesses are
//! spawned via [`spawn::build_worker_command`] (the `tessera-sink-worker`
//! bin) and run [`run_worker`].

#![deny(missing_docs)]
#![deny(unsafe_op_in_unsafe_fn)]
#![allow(dead_code)]

pub mod config;
pub mod error;
pub mod messages;
pub mod names;
pub mod sink;
pub mod spawn;
pub mod worker;

// Re-export the underlying primitives so a consumer can pull the
// descriptor / lease / role types from one import in v0.x.
pub use tessera_channel;
pub use tessera_pool;

pub use config::SinkConfig;
pub use error::{Result, TesseraSinkError};
pub use sink::Sink;
pub use worker::{run_worker, WorkerParams};
