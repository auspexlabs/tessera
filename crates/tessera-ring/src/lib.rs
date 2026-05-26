//! Tessera Ring — lossy mmap-backed multi-writer / multi-reader ring buffer.
//!
//! See the workspace README for the design summary; the per-section
//! references in this crate's source point at the upstream side-doc
//! `mp_tools_open_source_extraction_2026-05-23.md`.
//!
//! Stage 4b (in progress): public types + region layout land first;
//! state machine (`Writer::publish` / `Reader::poll` / namespace handle
//! / region management) lands in follow-up commits.

#![deny(missing_docs)]
#![deny(unsafe_op_in_unsafe_fn)]
#![allow(dead_code)]

pub mod error;
pub mod header;
pub mod namespace;
pub mod region;

pub use error::{Result, TesseraRingError};
pub use namespace::NamespaceHandle;

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

/// Reader-side cursor for one section. Process-local: lives in the
/// reader's memory, not in the SHM region. Each reader maintains its
/// own cursor so multiple consumers see the full event stream
/// independently (multi-reader broadcast per §4 of the upstream
/// extraction plan).
///
/// Implementations land in a follow-up commit; this is the public
/// shape so callers can refer to it.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct ReaderCursor {
    section_id: u32,
    position: u64,
}

impl ReaderCursor {
    /// Construct a cursor (internal use by `Reader::open`).
    pub fn new(section_id: u32, position: u64) -> Self {
        Self { section_id, position }
    }

    /// Section this cursor tracks.
    pub fn section_id(self) -> u32 {
        self.section_id
    }

    /// Current position (writer-equivalent monotonic index, NOT a slot
    /// index — the slot index is `position % slot_count`).
    pub fn position(self) -> u64 {
        self.position
    }
}

/// Per-section drop / cursor statistics surfaced to consumers via
/// `Reader::stats()`.
///
/// `dropped` counts how many events this reader was lapped on (writer
/// position advanced beyond what this reader had buffered). Reported
/// per-section because section traffic patterns can differ
/// drastically inside one Ring region (e.g. high-frequency `logs`
/// stream vs low-frequency `errors` stream).
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct ReaderStats {
    section_id: u32,
    cursor: u64,
    latest: u64,
    dropped: u64,
}

impl ReaderStats {
    /// Construct (internal use by `Reader::stats`).
    pub fn new(section_id: u32, cursor: u64, latest: u64, dropped: u64) -> Self {
        Self {
            section_id,
            cursor,
            latest,
            dropped,
        }
    }

    /// Section id these stats describe.
    pub fn section_id(self) -> u32 {
        self.section_id
    }

    /// Reader's current cursor position.
    pub fn cursor(self) -> u64 {
        self.cursor
    }

    /// Writer position at the moment of the stats snapshot.
    pub fn latest(self) -> u64 {
        self.latest
    }

    /// Number of events this reader was lapped on for this section.
    pub fn dropped(self) -> u64 {
        self.dropped
    }
}

/// Ring backed by a memory-mapped region. Multi-writer, multi-reader,
/// lossy: writers do not coordinate with readers; readers detect when
/// they have been lapped and account the gap in their local
/// `dropped` counter.
///
/// State machine and concrete fields land in a follow-up commit; this
/// is the public type name for downstream signatures.
pub struct Ring {
    _placeholder: (),
}

/// Writer handle for a Ring. Use `Writer::publish(section_id, bytes)`
/// to append; the section_id is caller-supplied — the library does
/// not classify event bytes.
///
/// Implementations land in a follow-up commit.
pub struct Writer {
    _placeholder: (),
}

/// Reader handle for a Ring. Each reader maintains its own cursor in
/// process-local memory (not in the shared region) and its own
/// drop-count accounting.
///
/// Implementations land in a follow-up commit.
pub struct Reader {
    _placeholder: (),
}
