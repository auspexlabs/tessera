//! Error types for the Tessera Channel crate.

use thiserror::Error;

/// All Channel errors flow through this enum.
///
/// Variants split by failure mode so consumers can react meaningfully
/// without parsing strings. Channel is MPSC + non-lossy + bytes-only;
/// the variant surface is smaller than Pool's but larger than Ring's
/// (Ring is lossy, so most "would-block" cases turn into reader-side
/// drops rather than caller errors).
#[derive(Error, Debug)]
pub enum TesseraChannelError {
    /// Caller-side configuration bug detected at construction
    /// (e.g. `slot_count == 0`, `slot_size_bytes == 0`,
    /// `slot_size_bytes` not a multiple of 8, etc.).
    #[error("invalid Channel config: {0}")]
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

    /// `Channel::send` was called with bytes longer than the
    /// configured `slot_size_bytes`. Channel rejects rather than
    /// silently truncating; callers serialize-then-check before send.
    #[error("payload size {payload_size} exceeds slot_size_bytes {slot_size}")]
    OversizedPayload {
        /// Caller-supplied byte length.
        payload_size: usize,
        /// Channel-configured slot capacity.
        slot_size: usize,
    },

    /// `try_send` couldn't enqueue because the Channel is full
    /// (`tail - head == slot_count`). Non-blocking-mode-only;
    /// blocking `send()` waits for room.
    #[error("Channel is full (tail - head == slot_count == {slot_count}); use blocking send() or wait for the consumer to drain")]
    ChannelFull {
        /// Configured slot count, for caller diagnostic.
        slot_count: u32,
    },

    /// `try_recv` couldn't dequeue because the Channel is empty
    /// (`head == tail`). Non-blocking-mode-only; blocking `recv()`
    /// waits for a producer.
    #[error("Channel is empty (head == tail == {head}); use blocking recv() or wait for a producer to enqueue")]
    ChannelEmpty {
        /// Current head position, for caller diagnostic.
        head: u64,
    },

    /// `send_timeout` or `recv_timeout` exhausted its budget without
    /// progress. Distinct from `ChannelFull` / `ChannelEmpty` to give
    /// the caller a clear signal that they asked for bounded blocking
    /// and the bound was reached.
    #[error("Channel operation timed out after {timeout_micros} micros (state: head={head}, tail={tail})")]
    Timeout {
        /// Configured timeout in microseconds.
        timeout_micros: u64,
        /// Head position at timeout.
        head: u64,
        /// Tail position at timeout.
        tail: u64,
    },

    /// Sender-role / Receiver-role API misuse: e.g., `send()` called
    /// on a `Receiver`-role handle, or `recv()` called on a `Sender`.
    /// MPSC requires exactly one process to hold the Receiver role
    /// for any given region.
    #[error("Channel role mismatch: this handle is in role={actual:?}, but operation requires {required:?}")]
    RoleMismatch {
        /// The role this Channel handle was opened with.
        actual: ChannelRoleSnapshot,
        /// The role the called operation requires.
        required: ChannelRoleSnapshot,
    },
}

/// Snapshot of a `ChannelRole` value, suitable for embedding in
/// error variants. Carries no behavior; just a name for diagnostics.
///
/// Distinct from `crate::ChannelRole` to avoid importing the public
/// type into `error.rs` (circular concern); they map 1-to-1.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ChannelRoleSnapshot {
    /// Receiver-role handle (owns the region's lifecycle; calls recv).
    Receiver,
    /// Sender-role handle (attaches to an existing region; calls send).
    Sender,
}

/// Result alias for `tessera-channel` operations.
pub type Result<T> = core::result::Result<T, TesseraChannelError>;
