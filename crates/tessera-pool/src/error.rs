//! Error types for the Tessera Pool crate.

use thiserror::Error;

/// All Pool errors flow through this enum.
///
/// Variants split by failure mode so consumers can react meaningfully
/// without parsing strings.
#[derive(Error, Debug)]
pub enum TesseraPoolError {
    /// Caller-side configuration bug detected at construction
    /// (e.g. `slot_count == 0`, `slot_size_bytes == 0`).
    #[error("invalid Pool config: {0}")]
    Config(/** Human-readable explanation of which field was invalid. */ String),

    /// SHM region creation / attach / unmap failed (OS resource issue,
    /// permissions, name collision, etc.).
    #[error("shared-memory region error: {0}")]
    Region(/** Underlying OS / library message. */ String),

    /// Attached region was created with different geometry or by a
    /// different deployment epoch; reading would corrupt.
    #[error(
        "attached SHM region has incompatible header: {message} \
        (expected format_version={expected_format}, found {found_format}; \
        expected slot_count={expected_count}, found {found_count}; \
        expected slot_size_bytes={expected_size}, found {found_size})"
    )]
    HeaderMismatch {
        /// Short description of which field disagrees.
        message: String,
        /// Format version this library was built with.
        expected_format: u32,
        /// Format version stamped in the attached region.
        found_format: u32,
        /// Slot count expected by the caller.
        expected_count: u32,
        /// Slot count stamped in the attached region.
        found_count: u32,
        /// Slot size expected by the caller.
        expected_size: u32,
        /// Slot size stamped in the attached region.
        found_size: u32,
    },

    /// Non-owner attempted an owner-only operation (`acquire`, `release`,
    /// `renew`, `reclaim_stale`).
    #[error("operation requires owner-side Pool (constructed with is_owner=true)")]
    OwnerOnly,

    /// Descriptor / lease has a generation older than the slot's
    /// current generation. The slot was reclaimed and re-leased; the
    /// holder of this descriptor must abandon it.
    #[error(
        "stale handle for slot {slot_index}: descriptor generation {descriptor_generation} \
        != current slot generation {current_generation}"
    )]
    StaleHandle {
        /// Slot index referenced by the descriptor.
        slot_index: u32,
        /// Generation carried by the stale descriptor.
        descriptor_generation: u64,
        /// Current generation in the slot's `SlotMeta`.
        current_generation: u64,
    },

    /// `Pool::write` was called with bytes longer than the slot can hold.
    #[error("payload size {payload_size} exceeds slot_size_bytes {slot_size}")]
    OversizedPayload {
        /// Caller-supplied byte length.
        payload_size: usize,
        /// Pool-configured slot capacity.
        slot_size: usize,
    },

    /// `Pool::write` was called twice on the same lease. v0.1 is
    /// one-shot — acquire, write once, release.
    #[error("write_after_finalize: Pool::write already called on lease for slot {slot_index} (v0.1 one-shot)")]
    WriteAfterFinalize {
        /// Slot index that was already finalized.
        slot_index: u32,
    },

    /// `Pool::acquire` couldn't get a free slot within the timeout.
    #[error("Pool::acquire timed out after {timeout_micros} micros; no slot became free")]
    Timeout {
        /// Configured acquire timeout, for log / error context.
        timeout_micros: u64,
    },
}

/// Result alias for `tessera-pool` operations.
pub type Result<T> = core::result::Result<T, TesseraPoolError>;
