//! Error types for the Tessera Ring crate.

use thiserror::Error;

/// All Ring errors flow through this enum.
///
/// Variants split by failure mode so consumers can react meaningfully
/// without parsing strings. Ring is intentionally symmetric (no owner /
/// non-owner asymmetry) and lossy (writers never block on readers), so
/// the variant surface is smaller than Tessera Pool's.
#[derive(Error, Debug)]
pub enum TesseraRingError {
    /// Caller-side configuration bug detected at construction
    /// (e.g. empty section list, `slot_count == 0`, `slot_size_bytes == 0`,
    /// duplicate section ids).
    #[error("invalid Ring config: {0}")]
    Config(/** Human-readable explanation of which field was invalid. */ String),

    /// SHM region creation / attach / unmap failed (OS resource issue,
    /// permissions, name collision, etc.).
    #[error("shared-memory region error: {0}")]
    Region(/** Underlying OS / library message. */ String),

    /// Attached region was created with a different magic or format
    /// version; reading would corrupt.
    #[error(
        "attached SHM region has incompatible global header: {message} \
        (expected format_version={expected_format}, found {found_format})"
    )]
    HeaderMismatch {
        /// Short description of which field disagrees.
        message: String,
        /// Format version this library was built with.
        expected_format: u32,
        /// Format version stamped in the attached region.
        found_format: u32,
    },

    /// Attached region's per-section geometry disagrees with the
    /// caller's config. Either the slot_count or slot_size_bytes
    /// differs from what the owner stamped at creation.
    #[error(
        "section {section_id} geometry mismatch: \
        expected slot_count={expected_count}, found {found_count}; \
        expected slot_size_bytes={expected_size}, found {found_size}"
    )]
    SectionConfigMismatch {
        /// Section id with the disagreement.
        section_id: u32,
        /// Slot count expected by the caller.
        expected_count: u32,
        /// Slot count stamped in the attached region.
        found_count: u32,
        /// Slot size expected by the caller.
        expected_size: u32,
        /// Slot size stamped in the attached region.
        found_size: u32,
    },

    /// `Writer::publish` or `Reader::poll` named a section_id that is
    /// not configured on this Ring.
    #[error("unknown section_id: {section_id} (configured sections: {configured:?})")]
    UnknownSection {
        /// Section id supplied by the caller.
        section_id: u32,
        /// Section ids known to this Ring instance, for diagnostics.
        configured: Vec<u32>,
    },

    /// `Writer::publish` was called with bytes longer than the section's
    /// slot capacity. v0.1 truncates only as an explicit caller choice;
    /// the default refuses to drop bytes silently.
    #[error("event size {event_size} exceeds section {section_id} slot_size_bytes {slot_size}")]
    OversizedEvent {
        /// Section id targeted by the publish.
        section_id: u32,
        /// Caller-supplied byte length.
        event_size: usize,
        /// Section-configured slot capacity.
        slot_size: usize,
    },

    /// `Writer::publish` observed a slot whose seqlock stayed stuck at
    /// the same odd value for the bounded recovery budget. The previous
    /// writer is either dead (crashed / killed mid-publish) or
    /// arbitrarily delayed.
    ///
    /// `Writer::publish` deliberately does NOT steal the slot from the
    /// stale writer: the seqlock-on-shared-slot model can't safely
    /// hand off when the original writer might still be alive, because
    /// a delayed wakeup would interleave writes and corrupt the slot
    /// (Codex iter-5 P1 on PR #2 / commit a559f2d).
    ///
    /// Caller behavior on `SlotStuck`:
    /// - The lost write's slot stays poisoned. Subsequent publishes
    ///   that wrap to the same slot will also fail.
    /// - Other slots in the section remain fully usable; calling
    ///   `publish` again will claim a *different* position (via the
    ///   atomic counter) and likely land on a different slot.
    /// - Persistent stuck slots indicate a Ring whose state is no
    ///   longer trustworthy. Owners should monitor for sustained
    ///   `SlotStuck` errors and rebuild the Ring (drop + recreate with
    ///   `force_recreate=true`) to clear poisoned slots.
    #[error(
        "slot stuck on section {section_id} slot_index {slot_index}: \
        sequence held at odd value {stuck_sequence} through the recovery \
        budget. Previous writer (position ≤ {position}) appears dead or \
        arbitrarily delayed; not safe to take over the slot. Subsequent \
        publishes wrapping to this slot will also fail. Recover by \
        rebuilding the Ring with force_recreate=true."
    )]
    SlotStuck {
        /// Section id where the stuck slot lives.
        section_id: u32,
        /// Slot index within the section (= position % slot_count).
        slot_index: u32,
        /// Position the failing writer was attempting to publish at.
        position: u64,
        /// The odd sequence value observed throughout the spin budget.
        stuck_sequence: u64,
    },
}

/// Result alias for `tessera-ring` operations.
pub type Result<T> = core::result::Result<T, TesseraRingError>;
