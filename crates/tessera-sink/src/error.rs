//! Error types for the Tessera Sink crate.

use thiserror::Error;

/// All Sink errors flow through this enum.
///
/// Sink is a *composite service* over Pool (shared-memory payload
/// handoff) and two Channels (control + ack planes), so its error
/// surface wraps both underlying primitives via `#[from]` and adds the
/// service-specific failure modes (worker spawn, chunk integrity,
/// job lifecycle).
#[derive(Error, Debug)]
pub enum TesseraSinkError {
    /// Caller-side configuration bug detected at construction
    /// (e.g. `worker_count == 0`, empty `description`, a control /
    /// ack slot too small to hold the largest control message).
    #[error("invalid Sink config: {0}")]
    Config(/** Human-readable explanation of which field was invalid. */ String),

    /// An operation on the underlying Pool failed.
    #[error("pool error: {0}")]
    Pool(#[from] tessera_pool::TesseraPoolError),

    /// An operation on one of the underlying Channels failed.
    #[error("channel error: {0}")]
    Channel(#[from] tessera_channel::TesseraChannelError),

    /// A filesystem operation (temp write, fsync, rename, unlink)
    /// failed on the worker side.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// The `tessera-sink-worker` executable could not be located. The
    /// `tried` list records every path probed (env override,
    /// sibling-of-current-exe, bare PATH lookup) for diagnosis.
    #[error("could not locate tessera-sink-worker binary; tried: {tried:?}")]
    WorkerBinaryNotFound {
        /// Every candidate path that was probed, in order.
        tried: Vec<String>,
    },

    /// Spawning a worker subprocess failed.
    #[error("failed to spawn worker {worker_id}: {message}")]
    WorkerSpawn {
        /// Index of the worker that failed to spawn.
        worker_id: u32,
        /// Underlying OS error message.
        message: String,
    },

    /// A submitted payload chunk exceeds the Pool slot capacity. The
    /// owner chunks payloads to `pool_slot_size_bytes`; this signals a
    /// chunking-math bug, not a caller error.
    #[error("chunk size {chunk_size} exceeds pool slot capacity {slot_size}")]
    ChunkTooLarge {
        /// Size of the offending chunk.
        chunk_size: usize,
        /// Pool slot capacity.
        slot_size: usize,
    },

    /// A control / ack message could not be decoded from its wire
    /// bytes (truncated, bad tag, length-prefix overrun). Indicates a
    /// version skew or corruption, not normal operation.
    #[error("protocol error: {0}")]
    Protocol(/** What was malformed in the message bytes. */ String),

    /// On `Commit`, the worker's reassembled chunk count did not match
    /// the count the owner declared. The temp file is discarded.
    #[error("job {job_id}: chunk count mismatch (expected {expected}, got {actual})")]
    ChunkCountMismatch {
        /// 128-bit job id, hex-rendered.
        job_id: String,
        /// Chunk count the owner declared in `Commit`.
        expected: u32,
        /// Chunk count the worker actually received.
        actual: u32,
    },

    /// On `Commit`, the worker's running BLAKE3 hash of the reassembled
    /// bytes did not match the owner-declared `expected_hash`. The temp
    /// file is discarded rather than renamed into place.
    #[error("job {job_id}: hash mismatch (expected {expected}, got {actual})")]
    HashMismatch {
        /// 128-bit job id, hex-rendered.
        job_id: String,
        /// Owner-declared BLAKE3 hex digest.
        expected: String,
        /// Worker-computed BLAKE3 hex digest.
        actual: String,
    },

    /// A job finished in a failed state (worker reported an error, a
    /// chunk failed, or integrity verification failed). Surfaced by
    /// `Sink::flush`.
    #[error("job {job_id} for {path} failed: {message}")]
    JobFailed {
        /// 128-bit job id, hex-rendered.
        job_id: String,
        /// Target path the job was writing.
        path: String,
        /// Failure detail.
        message: String,
    },

    /// An operation was attempted on a Sink that has already been
    /// closed / shut down.
    #[error("Sink is closed")]
    Closed,

    /// A bounded-blocking operation (control-Sender attach, flush)
    /// exhausted its budget without progress.
    #[error("Sink operation timed out after {timeout_micros} micros: {context}")]
    Timeout {
        /// Configured timeout in microseconds.
        timeout_micros: u64,
        /// What was being waited on.
        context: String,
    },
}

/// Result alias for `tessera-sink` operations.
pub type Result<T> = core::result::Result<T, TesseraSinkError>;
