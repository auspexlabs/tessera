//! Control- and ack-plane message types and their wire codec.
//!
//! These messages travel across [`tessera_channel::Channel`] as raw
//! bytes. Serialization lives in Rust (never the Python facade), so the
//! codec here is the single source of truth for the over-the-wire shape.
//!
//! ## Wire format
//!
//! Every message is `[tag: u8][fields…]`. Integers are little-endian
//! fixed-width. Strings are `[len: u32][utf-8 bytes]`. A
//! [`tessera_pool::Descriptor`] serializes as its four fields
//! (`slot_index u32`, `lease_id [u8; 16]`, `generation u64`,
//! `size_bytes u32`). Decoding validates the tag and bounds-checks
//! every field read, returning [`TesseraSinkError::Protocol`] on any
//! truncation or unknown tag rather than panicking.

use tessera_pool::{Descriptor, LeaseId};

use crate::error::{Result, TesseraSinkError};

// Control-plane tags (owner → worker).
const TAG_CHUNK_DESCRIPTOR: u8 = 1;
const TAG_COMMIT: u8 = 2;
const TAG_CANCEL: u8 = 3;
const TAG_SHUTDOWN: u8 = 4;

// Ack-plane tags (worker → owner).
const TAG_CHUNK_ACK: u8 = 16;
const TAG_CHUNK_FAILED: u8 = 17;
const TAG_CANCEL_ACK: u8 = 18;
const TAG_JOB_COMPLETE: u8 = 19;
const TAG_WORKER_READY: u8 = 20;

/// Owner → worker control-plane message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ControlMessage {
    /// Hand a single chunk's Pool descriptor to the worker. Carries
    /// the target path (so the worker can build / locate the temp
    /// file on the first chunk) and the chunk's ordinal index.
    ChunkDescriptor {
        /// 128-bit job identifier.
        job_id: u128,
        /// Final target path for the assembled file.
        path: String,
        /// Zero-based ordinal of this chunk within the job.
        chunk_index: u32,
        /// Pool descriptor pointing at the chunk's bytes in SHM.
        descriptor: Descriptor,
    },
    /// All chunks sent — the worker should verify count + hash, fsync
    /// (if requested), and atomically rename the temp file into place.
    Commit {
        /// 128-bit job identifier.
        job_id: u128,
        /// Final target path.
        path: String,
        /// Total number of chunks the worker should have received.
        chunk_count: u32,
        /// BLAKE3 digest of the fully reassembled payload.
        expected_hash: [u8; 32],
        /// Whether the worker should fsync before rename.
        fsync: bool,
    },
    /// Abort the job — stop accepting chunks, delete the temp file.
    Cancel {
        /// 128-bit job identifier.
        job_id: u128,
    },
    /// Graceful stop: the worker should clean up any in-flight temp
    /// files and exit its run loop. Sent by the owner on Sink shutdown
    /// after all jobs have drained.
    Shutdown,
}

/// Worker → owner ack-plane message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AckMessage {
    /// Startup handshake: the worker has attached its Pool, **created**
    /// its control region, and attached the ack plane. The owner waits
    /// for this before attaching its control Sender, so it never binds
    /// to a stale control region a crashed predecessor left behind
    /// (the worker's `force_recreate` has already unlinked + recreated
    /// it by the time this is sent).
    WorkerReady {
        /// Index of the worker that is ready.
        worker_id: u32,
    },
    /// A chunk was streamed to the temp file successfully; the owner
    /// may release the chunk's Pool lease.
    ChunkAck {
        /// 128-bit job identifier.
        job_id: u128,
        /// Chunk ordinal that was acked.
        chunk_index: u32,
    },
    /// A chunk failed to stream; the owner releases the lease, cancels
    /// the job, and marks it failed.
    ChunkFailed {
        /// 128-bit job identifier.
        job_id: u128,
        /// Chunk ordinal that failed.
        chunk_index: u32,
        /// Human-readable failure detail.
        error: String,
    },
    /// Acknowledges a `Cancel`; the worker has stopped and cleaned up.
    CancelAck {
        /// 128-bit job identifier.
        job_id: u128,
    },
    /// Terminal status for a job after `Commit`.
    JobComplete {
        /// 128-bit job identifier.
        job_id: u128,
        /// True on success (renamed into place), false on failure.
        success: bool,
        /// Final target path.
        path: String,
        /// Failure detail when `success == false`; empty otherwise.
        error: String,
    },
}

impl ControlMessage {
    /// Serialize to wire bytes.
    pub fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        match self {
            ControlMessage::ChunkDescriptor {
                job_id,
                path,
                chunk_index,
                descriptor,
            } => {
                w.put_u8(TAG_CHUNK_DESCRIPTOR);
                w.put_u128(*job_id);
                w.put_str(path);
                w.put_u32(*chunk_index);
                w.put_descriptor(descriptor);
            }
            ControlMessage::Commit {
                job_id,
                path,
                chunk_count,
                expected_hash,
                fsync,
            } => {
                w.put_u8(TAG_COMMIT);
                w.put_u128(*job_id);
                w.put_str(path);
                w.put_u32(*chunk_count);
                w.put_arr(expected_hash);
                w.put_bool(*fsync);
            }
            ControlMessage::Cancel { job_id } => {
                w.put_u8(TAG_CANCEL);
                w.put_u128(*job_id);
            }
            ControlMessage::Shutdown => {
                w.put_u8(TAG_SHUTDOWN);
            }
        }
        w.into_inner()
    }

    /// Decode from wire bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut r = Reader::new(bytes);
        let tag = r.get_u8()?;
        let msg = match tag {
            TAG_CHUNK_DESCRIPTOR => ControlMessage::ChunkDescriptor {
                job_id: r.get_u128()?,
                path: r.get_str()?,
                chunk_index: r.get_u32()?,
                descriptor: r.get_descriptor()?,
            },
            TAG_COMMIT => ControlMessage::Commit {
                job_id: r.get_u128()?,
                path: r.get_str()?,
                chunk_count: r.get_u32()?,
                expected_hash: r.get_arr::<32>()?,
                fsync: r.get_bool()?,
            },
            TAG_CANCEL => ControlMessage::Cancel {
                job_id: r.get_u128()?,
            },
            TAG_SHUTDOWN => ControlMessage::Shutdown,
            other => {
                return Err(TesseraSinkError::Protocol(format!(
                    "unknown control message tag {other}"
                )))
            }
        };
        r.expect_consumed()?;
        Ok(msg)
    }
}

impl AckMessage {
    /// Serialize to wire bytes.
    pub fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        match self {
            AckMessage::WorkerReady { worker_id } => {
                w.put_u8(TAG_WORKER_READY);
                w.put_u32(*worker_id);
            }
            AckMessage::ChunkAck {
                job_id,
                chunk_index,
            } => {
                w.put_u8(TAG_CHUNK_ACK);
                w.put_u128(*job_id);
                w.put_u32(*chunk_index);
            }
            AckMessage::ChunkFailed {
                job_id,
                chunk_index,
                error,
            } => {
                w.put_u8(TAG_CHUNK_FAILED);
                w.put_u128(*job_id);
                w.put_u32(*chunk_index);
                w.put_str(error);
            }
            AckMessage::CancelAck { job_id } => {
                w.put_u8(TAG_CANCEL_ACK);
                w.put_u128(*job_id);
            }
            AckMessage::JobComplete {
                job_id,
                success,
                path,
                error,
            } => {
                w.put_u8(TAG_JOB_COMPLETE);
                w.put_u128(*job_id);
                w.put_bool(*success);
                w.put_str(path);
                w.put_str(error);
            }
        }
        w.into_inner()
    }

    /// Decode from wire bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut r = Reader::new(bytes);
        let tag = r.get_u8()?;
        let msg = match tag {
            TAG_WORKER_READY => AckMessage::WorkerReady {
                worker_id: r.get_u32()?,
            },
            TAG_CHUNK_ACK => AckMessage::ChunkAck {
                job_id: r.get_u128()?,
                chunk_index: r.get_u32()?,
            },
            TAG_CHUNK_FAILED => AckMessage::ChunkFailed {
                job_id: r.get_u128()?,
                chunk_index: r.get_u32()?,
                error: r.get_str()?,
            },
            TAG_CANCEL_ACK => AckMessage::CancelAck {
                job_id: r.get_u128()?,
            },
            TAG_JOB_COMPLETE => AckMessage::JobComplete {
                job_id: r.get_u128()?,
                success: r.get_bool()?,
                path: r.get_str()?,
                error: r.get_str()?,
            },
            other => {
                return Err(TesseraSinkError::Protocol(format!(
                    "unknown ack message tag {other}"
                )))
            }
        };
        r.expect_consumed()?;
        Ok(msg)
    }
}

/// Render a `u128` job id as 32 lowercase hex chars (for error
/// messages / diagnostics; the wire form is the raw 16 bytes).
pub fn job_id_hex(job_id: u128) -> String {
    format!("{job_id:032x}")
}

// --- Wire helpers -------------------------------------------------

struct Writer {
    buf: Vec<u8>,
}

impl Writer {
    fn new() -> Self {
        Self { buf: Vec::new() }
    }
    fn into_inner(self) -> Vec<u8> {
        self.buf
    }
    fn put_u8(&mut self, v: u8) {
        self.buf.push(v);
    }
    fn put_bool(&mut self, v: bool) {
        self.buf.push(v as u8);
    }
    fn put_u32(&mut self, v: u32) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }
    fn put_u64(&mut self, v: u64) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }
    fn put_u128(&mut self, v: u128) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }
    fn put_arr<const N: usize>(&mut self, v: &[u8; N]) {
        self.buf.extend_from_slice(v);
    }
    fn put_str(&mut self, s: &str) {
        self.put_u32(s.len() as u32);
        self.buf.extend_from_slice(s.as_bytes());
    }
    fn put_descriptor(&mut self, d: &Descriptor) {
        self.put_u32(d.slot_index());
        self.put_arr(&d.lease_id().to_bytes());
        self.put_u64(d.generation());
        self.put_u32(d.size_bytes());
    }
}

struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self.pos.checked_add(n).ok_or_else(|| {
            TesseraSinkError::Protocol("length overflow while decoding".into())
        })?;
        if end > self.buf.len() {
            return Err(TesseraSinkError::Protocol(format!(
                "truncated message: need {n} bytes at offset {}, only {} remain",
                self.pos,
                self.buf.len().saturating_sub(self.pos)
            )));
        }
        let out = &self.buf[self.pos..end];
        self.pos = end;
        Ok(out)
    }

    fn get_u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    fn get_bool(&mut self) -> Result<bool> {
        Ok(self.get_u8()? != 0)
    }
    fn get_u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().expect("4 bytes")))
    }
    fn get_u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().expect("8 bytes")))
    }
    fn get_u128(&mut self) -> Result<u128> {
        Ok(u128::from_le_bytes(
            self.take(16)?.try_into().expect("16 bytes"),
        ))
    }
    fn get_arr<const N: usize>(&mut self) -> Result<[u8; N]> {
        Ok(self.take(N)?.try_into().expect("N bytes"))
    }
    fn get_str(&mut self) -> Result<String> {
        let len = self.get_u32()? as usize;
        let bytes = self.take(len)?;
        String::from_utf8(bytes.to_vec())
            .map_err(|e| TesseraSinkError::Protocol(format!("invalid utf-8 in string field: {e}")))
    }
    fn get_descriptor(&mut self) -> Result<Descriptor> {
        let slot_index = self.get_u32()?;
        let lease_id = LeaseId::from_bytes(self.get_arr::<16>()?);
        let generation = self.get_u64()?;
        let size_bytes = self.get_u32()?;
        Ok(Descriptor::new(slot_index, lease_id, generation, size_bytes))
    }

    /// Error if there are trailing bytes after the message — catches
    /// version skew / corruption that happened to parse a valid prefix.
    fn expect_consumed(&self) -> Result<()> {
        if self.pos != self.buf.len() {
            return Err(TesseraSinkError::Protocol(format!(
                "trailing bytes after message: consumed {}, total {}",
                self.pos,
                self.buf.len()
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn descriptor() -> Descriptor {
        Descriptor::new(7, LeaseId::from_bytes([0xAB; 16]), 42, 1234)
    }

    #[test]
    fn control_chunk_descriptor_round_trips() {
        let msg = ControlMessage::ChunkDescriptor {
            job_id: 0x1122_3344_5566_7788_99AA_BBCC_DDEE_FF00,
            path: "/data/output/file.parquet".into(),
            chunk_index: 3,
            descriptor: descriptor(),
        };
        let bytes = msg.encode();
        assert_eq!(ControlMessage::decode(&bytes).unwrap(), msg);
    }

    #[test]
    fn control_commit_round_trips() {
        let msg = ControlMessage::Commit {
            job_id: 9,
            path: "/tmp/x".into(),
            chunk_count: 5,
            expected_hash: [0x5A; 32],
            fsync: true,
        };
        let bytes = msg.encode();
        assert_eq!(ControlMessage::decode(&bytes).unwrap(), msg);
    }

    #[test]
    fn control_cancel_round_trips() {
        let msg = ControlMessage::Cancel { job_id: u128::MAX };
        let bytes = msg.encode();
        assert_eq!(ControlMessage::decode(&bytes).unwrap(), msg);
    }

    #[test]
    fn control_shutdown_round_trips() {
        let msg = ControlMessage::Shutdown;
        assert_eq!(ControlMessage::decode(&msg.encode()).unwrap(), msg);
    }

    #[test]
    fn ack_chunk_ack_round_trips() {
        let msg = AckMessage::ChunkAck {
            job_id: 1,
            chunk_index: 0,
        };
        assert_eq!(AckMessage::decode(&msg.encode()).unwrap(), msg);
    }

    #[test]
    fn ack_chunk_failed_round_trips() {
        let msg = AckMessage::ChunkFailed {
            job_id: 2,
            chunk_index: 1,
            error: "disk full".into(),
        };
        assert_eq!(AckMessage::decode(&msg.encode()).unwrap(), msg);
    }

    #[test]
    fn ack_job_complete_round_trips_both_states() {
        let ok = AckMessage::JobComplete {
            job_id: 3,
            success: true,
            path: "/done".into(),
            error: String::new(),
        };
        assert_eq!(AckMessage::decode(&ok.encode()).unwrap(), ok);

        let bad = AckMessage::JobComplete {
            job_id: 4,
            success: false,
            path: "/done".into(),
            error: "hash mismatch".into(),
        };
        assert_eq!(AckMessage::decode(&bad.encode()).unwrap(), bad);
    }

    #[test]
    fn ack_cancel_ack_round_trips() {
        let msg = AckMessage::CancelAck { job_id: 123 };
        assert_eq!(AckMessage::decode(&msg.encode()).unwrap(), msg);
    }

    #[test]
    fn ack_worker_ready_round_trips() {
        let msg = AckMessage::WorkerReady { worker_id: 5 };
        assert_eq!(AckMessage::decode(&msg.encode()).unwrap(), msg);
    }

    #[test]
    fn decode_rejects_unknown_tag() {
        let err = ControlMessage::decode(&[250, 0, 0]).unwrap_err();
        assert!(matches!(err, TesseraSinkError::Protocol(_)));
    }

    #[test]
    fn decode_rejects_truncated() {
        let full = ControlMessage::Cancel { job_id: 7 }.encode();
        // Drop the last byte → the u128 read runs past the end.
        let err = ControlMessage::decode(&full[..full.len() - 1]).unwrap_err();
        assert!(matches!(err, TesseraSinkError::Protocol(_)));
    }

    #[test]
    fn decode_rejects_trailing_bytes() {
        let mut full = AckMessage::CancelAck { job_id: 7 }.encode();
        full.push(0xFF); // junk byte
        let err = AckMessage::decode(&full).unwrap_err();
        assert!(matches!(err, TesseraSinkError::Protocol(_)));
    }

    #[test]
    fn decode_rejects_empty() {
        assert!(matches!(
            ControlMessage::decode(&[]),
            Err(TesseraSinkError::Protocol(_))
        ));
    }

    #[test]
    fn descriptor_fields_survive_round_trip() {
        let msg = ControlMessage::ChunkDescriptor {
            job_id: 1,
            path: "p".into(),
            chunk_index: 99,
            descriptor: descriptor(),
        };
        match ControlMessage::decode(&msg.encode()).unwrap() {
            ControlMessage::ChunkDescriptor { descriptor: d, .. } => {
                assert_eq!(d.slot_index(), 7);
                assert_eq!(d.lease_id().to_bytes(), [0xAB; 16]);
                assert_eq!(d.generation(), 42);
                assert_eq!(d.size_bytes(), 1234);
            }
            other => panic!("wrong variant: {other:?}"),
        }
    }
}
