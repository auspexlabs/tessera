//! Sink construction parameters.
//!
//! A Sink composes one Pool (payload plane) and two Channel families
//! (control + ack planes). Rather than make callers hand-wire three
//! sub-configs, `SinkConfig` carries the geometry for all three and
//! the Sink derives per-region descriptions from the single
//! `description` base (see `crate::names`).

use std::path::PathBuf;

use crate::error::{Result, TesseraSinkError};

/// Construction parameters for a [`crate::Sink`].
#[derive(Clone, Debug)]
pub struct SinkConfig {
    /// Operator-facing base description. The Pool and each Channel
    /// derive their own region description by suffixing this (e.g.
    /// `"<base>/pool"`, `"<base>/ack"`, `"<base>/control/<i>"`), so a
    /// single base string namespaces the whole Sink. Workers receive
    /// the derived descriptions over argv and re-derive the same
    /// BLAKE3 handles.
    pub description: String,

    /// Number of worker subprocesses to spawn.
    pub worker_count: u32,

    /// Pool slot count — the number of in-flight chunks the owner can
    /// have leased at once across all jobs.
    pub pool_slot_count: u32,

    /// Pool slot size in bytes. This is the maximum chunk payload;
    /// `submit` splits larger payloads into this many bytes per chunk.
    pub pool_slot_size_bytes: u32,

    /// Pool lease TTL in microseconds. Outstanding leases the owner
    /// holds for in-flight chunks are renewed on a timer at `ttl/2`;
    /// a crashed owner's leases are reclaimed after this TTL.
    pub ttl_micros: u64,

    /// How long `submit` waits to acquire a free Pool slot before
    /// returning a timeout error, in microseconds.
    pub acquire_timeout_micros: u64,

    /// Slot count for each per-worker control Channel (owner → worker).
    pub control_slot_count: u32,

    /// Slot size for each per-worker control Channel, in bytes. Must
    /// be large enough to hold the largest control message (a
    /// `ChunkDescriptor` or `Commit`, both of which carry the target
    /// path string). Validated at construction against a floor.
    pub control_slot_size_bytes: u32,

    /// Slot count for the shared ack Channel (workers → owner).
    pub ack_slot_count: u32,

    /// Slot size for the shared ack Channel, in bytes. Must hold the
    /// largest ack message (a `JobComplete` or `ChunkFailed`, both of
    /// which can carry a path / error string).
    pub ack_slot_size_bytes: u32,

    /// Optional explicit path to the `tessera-sink-worker` executable.
    /// When `None`, the Sink probes (in order): the
    /// `TESSERA_SINK_WORKER_BIN` env var, a sibling of the current
    /// executable, then a bare `tessera-sink-worker` PATH lookup.
    pub worker_bin_path: Option<PathBuf>,

    /// Owner-side recovery escape hatch, forwarded to the Pool and ack
    /// Channel: if `true`, unlink + recreate any pre-existing SHM
    /// regions for this description. Misuse clobbers a live Sink; only
    /// set during explicit recovery.
    pub force_recreate: bool,
}

/// Smallest control / ack slot we accept. The fixed-size portion of
/// the largest message is well under this; the remainder is headroom
/// for the path / error string. Paths longer than the slot capacity
/// are rejected at `submit` time with a clear error rather than here.
pub(crate) const MIN_CONTROL_ACK_SLOT_BYTES: u32 = 512;

impl SinkConfig {
    /// Validate the config, returning the first problem found. Called
    /// by `Sink::start` before any region is created.
    pub fn validate(&self) -> Result<()> {
        if self.description.is_empty() {
            return Err(TesseraSinkError::Config("description must be non-empty".into()));
        }
        if self.worker_count == 0 {
            return Err(TesseraSinkError::Config("worker_count must be > 0".into()));
        }
        if self.pool_slot_count == 0 {
            return Err(TesseraSinkError::Config("pool_slot_count must be > 0".into()));
        }
        if self.pool_slot_size_bytes == 0 {
            return Err(TesseraSinkError::Config(
                "pool_slot_size_bytes must be > 0".into(),
            ));
        }
        if self.ttl_micros == 0 {
            return Err(TesseraSinkError::Config("ttl_micros must be > 0".into()));
        }
        if self.control_slot_count == 0 {
            return Err(TesseraSinkError::Config("control_slot_count must be > 0".into()));
        }
        if self.ack_slot_count == 0 {
            return Err(TesseraSinkError::Config("ack_slot_count must be > 0".into()));
        }
        // Channel requires slot_size_bytes % 8 == 0 for AtomicU64
        // alignment; surface that here with Sink context rather than
        // letting the Channel constructor reject it later.
        if self.control_slot_size_bytes % 8 != 0 {
            return Err(TesseraSinkError::Config(
                "control_slot_size_bytes must be a multiple of 8".into(),
            ));
        }
        if self.ack_slot_size_bytes % 8 != 0 {
            return Err(TesseraSinkError::Config(
                "ack_slot_size_bytes must be a multiple of 8".into(),
            ));
        }
        if self.control_slot_size_bytes < MIN_CONTROL_ACK_SLOT_BYTES {
            return Err(TesseraSinkError::Config(format!(
                "control_slot_size_bytes ({}) below floor {MIN_CONTROL_ACK_SLOT_BYTES}",
                self.control_slot_size_bytes
            )));
        }
        if self.ack_slot_size_bytes < MIN_CONTROL_ACK_SLOT_BYTES {
            return Err(TesseraSinkError::Config(format!(
                "ack_slot_size_bytes ({}) below floor {MIN_CONTROL_ACK_SLOT_BYTES}",
                self.ack_slot_size_bytes
            )));
        }
        Ok(())
    }
}

/// A valid baseline config for use in tests across the crate.
#[cfg(test)]
pub(crate) fn tests_support_config() -> SinkConfig {
    SinkConfig {
        description: "tessera-sink-test/cfg".into(),
        worker_count: 2,
        pool_slot_count: 8,
        pool_slot_size_bytes: 64 * 1024,
        ttl_micros: 60_000_000,
        acquire_timeout_micros: 5_000_000,
        control_slot_count: 64,
        control_slot_size_bytes: 4096,
        ack_slot_count: 256,
        ack_slot_size_bytes: 4096,
        worker_bin_path: None,
        force_recreate: false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid() -> SinkConfig {
        tests_support_config()
    }

    #[test]
    fn valid_config_passes() {
        valid().validate().expect("valid config should pass");
    }

    #[test]
    fn rejects_zero_workers() {
        let mut c = valid();
        c.worker_count = 0;
        assert!(matches!(c.validate(), Err(TesseraSinkError::Config(_))));
    }

    #[test]
    fn rejects_unaligned_channel_slot() {
        let mut c = valid();
        c.control_slot_size_bytes = 4097; // not a multiple of 8
        assert!(matches!(c.validate(), Err(TesseraSinkError::Config(_))));
    }

    #[test]
    fn rejects_too_small_channel_slot() {
        let mut c = valid();
        c.ack_slot_size_bytes = 256; // below the 512 floor (and 8-aligned)
        assert!(matches!(c.validate(), Err(TesseraSinkError::Config(_))));
    }

    #[test]
    fn rejects_empty_description() {
        let mut c = valid();
        c.description = String::new();
        assert!(matches!(c.validate(), Err(TesseraSinkError::Config(_))));
    }
}
