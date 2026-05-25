//! Tessera Ring — lossy mmap-backed multi-writer / multi-reader ring buffer.
//!
//! v0.0.1 SCAFFOLD ONLY. Implementations land in Stage 4b of the
//! upstream extraction plan; see the Tessera README for the planned
//! surface (per-section write cursors with seqlock counters,
//! caller-supplied sections, per-reader local cursors with gap detection).

#![allow(dead_code)]

/// Ring backed by a memory-mapped region. Multi-writer, multi-reader,
/// lossy: writers do not coordinate with readers; readers detect when
/// they have been lapped and account the gap in their local
/// `dropped` counter.
pub struct Ring {
    _placeholder: (),
}

/// Writer handle for a Ring. Use `Writer::publish(section_id, bytes)`
/// to append; the section_id is caller-supplied — the library does
/// not classify event bytes.
pub struct Writer {
    _placeholder: (),
}

/// Reader handle for a Ring. Each reader maintains its own cursor in
/// process-local memory (not in the shared region) and its own
/// drop-count accounting.
pub struct Reader {
    _placeholder: (),
}
