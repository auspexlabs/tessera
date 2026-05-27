//! Tessera Ring — lossy mmap-backed multi-writer / multi-reader ring buffer.
//!
//! See the workspace README and `docs/concept_landscape.md` for the
//! design summary.
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
/// Sections are caller-defined logical streams inside a single Ring
/// region. Callers map names such as `logs` or `metrics` to integer
/// `section_id` values; the library only stores and routes by id.
/// Sections can have independent slot counts and slot sizes.
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
