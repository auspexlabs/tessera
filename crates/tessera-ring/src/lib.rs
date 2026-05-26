//! Tessera Ring — lossy mmap-backed multi-writer / multi-reader ring buffer.
//!
//! See the workspace README for the design summary; the per-section
//! references in this crate's source point at the upstream side-doc
//! `mp_tools_open_source_extraction_2026-05-23.md`.
//!
//! The public surface is small: open a `Ring` from a `RingConfig`,
//! issue `Writer` and `Reader` handles, and exchange `Event`s. The
//! seqlock state machine lives in `ring`; SHM lifecycle in `region`;
//! POD on-disk types in `header`; BLAKE3-derived namespace in
//! `namespace`.

#![deny(missing_docs)]
#![deny(unsafe_op_in_unsafe_fn)]

pub mod error;
pub mod header;
pub mod namespace;
pub mod region;
pub mod ring;

pub use error::{Result, TesseraRingError};
pub use namespace::NamespaceHandle;
pub use ring::{Event, Reader, ReaderStats, Ring, RingConfig, Writer};

/// Per-section configuration supplied by the caller at `Ring::open`.
///
/// Sections are caller-named logical streams inside a single Ring
/// region: a `logs` section and a `metrics` section can coexist with
/// independent slot counts and slot sizes. The library does not
/// inspect or classify event bytes — the caller addresses sections by
/// id at every `publish` / `poll` call.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct SectionConfig {
    section_id: u32,
    slot_count: u32,
    slot_size_bytes: u32,
}

impl SectionConfig {
    /// Construct a section config.
    ///
    /// The Ring layer validates `slot_count > 0` and
    /// `slot_size_bytes > 0` at `Ring::open` time and surfaces
    /// `TesseraRingError::Config` on violation; constructing the value
    /// itself is infallible so callers can build a config list before
    /// the Ring exists.
    pub fn new(section_id: u32, slot_count: u32, slot_size_bytes: u32) -> Self {
        Self {
            section_id,
            slot_count,
            slot_size_bytes,
        }
    }

    /// Section identifier supplied by the caller.
    pub fn section_id(self) -> u32 {
        self.section_id
    }

    /// Slot count configured for this section.
    pub fn slot_count(self) -> u32 {
        self.slot_count
    }

    /// Slot payload size for this section, in bytes (excludes the
    /// per-slot `SlotHeader`).
    pub fn slot_size_bytes(self) -> u32 {
        self.slot_size_bytes
    }
}
