//! Worker run loop — topology-agnostic entry point.
//!
//! [`run_worker`] is the heart of a Sink worker process. It is written
//! to know nothing about *how* it was launched: the `tessera-sink-worker`
//! bin crate is a thin `main()` that parses argv and calls this. A
//! future in-process / threaded host could call it identically.
//!
//! Each worker:
//! 1. attaches to the Pool (`is_owner = false`) to read chunk payloads,
//! 2. **creates** its control Channel as the `Receiver` (per the
//!    "reader owns its region" rule — the worker reads control msgs),
//! 3. attaches to the shared ack Channel as a `Sender`,
//! 4. drains control messages: streams each chunk to a temp file,
//!    verifies count + BLAKE3 hash on `Commit`, atomically renames into
//!    place, and reports terminal status on the ack plane.

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::PathBuf;
use std::time::Duration;

use tessera_channel::{Channel, ChannelConfig, ChannelRole, TesseraChannelError};
use tessera_pool::{Pool, PoolConfig};

use crate::error::{Result, TesseraSinkError};
use crate::messages::{job_id_hex, AckMessage, ControlMessage};

/// How long the control-channel `recv` blocks before the loop wakes to
/// check for owner-death (orphan detection). Short enough that an
/// orphaned worker exits promptly; long enough not to busy-spin.
const CONTROL_POLL: Duration = Duration::from_millis(250);

/// How long a worker waits to push an ack before giving up. Generous
/// because the owner drains continuously; a timeout here means the
/// owner has hung or died, so the worker exits.
const ACK_SEND_TIMEOUT: Duration = Duration::from_secs(30);

/// Everything a worker needs to attach to its regions. The owner
/// derives the three descriptions (via [`crate::names`]) and the
/// geometry, and passes them verbatim so both sides agree on the
/// BLAKE3-derived SHM handles.
#[derive(Clone, Debug)]
pub struct WorkerParams {
    /// Pool region description (`<base>/pool`).
    pub pool_description: String,
    /// This worker's control-channel description (`<base>/control/<id>`).
    pub control_description: String,
    /// Shared ack-channel description (`<base>/ack`).
    pub ack_description: String,
    /// Pool slot count (must match the owner's).
    pub pool_slot_count: u32,
    /// Pool slot size in bytes (must match the owner's).
    pub pool_slot_size_bytes: u32,
    /// Control-channel slot count.
    pub control_slot_count: u32,
    /// Control-channel slot size in bytes.
    pub control_slot_size_bytes: u32,
    /// Ack-channel slot count.
    pub ack_slot_count: u32,
    /// Ack-channel slot size in bytes.
    pub ack_slot_size_bytes: u32,
    /// This worker's index, for diagnostics.
    pub worker_id: u32,
    /// Recovery escape hatch forwarded from the owner: when `true`, the
    /// worker unlinks + recreates its control region if a stale one
    /// already exists (e.g. left by a SIGKILLed predecessor after a
    /// failed start). When `false` it fails loud on a pre-existing
    /// region, surfacing a same-description double-Sink misuse.
    pub force_recreate: bool,
}

/// Per-job streaming state held while chunks arrive.
struct WorkerJob {
    final_path: PathBuf,
    temp_path: PathBuf,
    file: File,
    hasher: blake3::Hasher,
    /// Ordinal the next chunk must carry; enforces in-order arrival.
    next_chunk_index: u32,
}

/// Run the worker until a `Shutdown` control message arrives, the
/// owner dies (orphan detection), or a fatal transport error occurs.
///
/// Returns `Ok(())` on graceful shutdown / orphan exit. Returns `Err`
/// only on a transport / setup failure the worker can't recover from
/// (the bin crate maps that to a non-zero exit code).
pub fn run_worker(params: WorkerParams) -> Result<()> {
    let pool = Pool::new(PoolConfig {
        description: params.pool_description.clone(),
        slot_count: params.pool_slot_count,
        slot_size_bytes: params.pool_slot_size_bytes,
        is_owner: false,
        ttl_micros: 0, // ignored for attachers; inherited from header
        force_recreate: false,
    })?;

    // The worker is the reader of its control channel → it creates it.
    let control = Channel::open(ChannelConfig {
        description: params.control_description.clone(),
        slot_count: params.control_slot_count,
        slot_size_bytes: params.control_slot_size_bytes,
        role: ChannelRole::Receiver,
        force_recreate: params.force_recreate,
    })?;

    // The owner created the ack region before spawning us; attach.
    let ack = Channel::open(ChannelConfig {
        description: params.ack_description.clone(),
        slot_count: params.ack_slot_count,
        slot_size_bytes: params.ack_slot_size_bytes,
        role: ChannelRole::Sender,
        force_recreate: false,
    })?;

    // Startup handshake: announce readiness only after the control
    // region exists (created + bound above). The owner waits for this
    // before attaching its control Sender, so it can never bind to a
    // stale control region from a crashed predecessor.
    send_ack(&ack, &AckMessage::WorkerReady {
        worker_id: params.worker_id,
    })?;

    let mut jobs: HashMap<u128, WorkerJob> = HashMap::new();

    loop {
        match control.recv_timeout(CONTROL_POLL) {
            Ok(bytes) => {
                let msg = ControlMessage::decode(&bytes)?;
                if matches!(msg, ControlMessage::Shutdown) {
                    cleanup_all(&mut jobs);
                    return Ok(());
                }
                dispatch(&pool, &ack, &mut jobs, msg)?;
            }
            Err(TesseraChannelError::Timeout { .. }) => {
                // No message this window. Bail if we've been orphaned
                // (owner died without sending Shutdown) so we don't
                // leak a worker process holding SHM mappings.
                if parent_is_gone() {
                    cleanup_all(&mut jobs);
                    return Ok(());
                }
            }
            Err(e) => return Err(e.into()),
        }
    }
}

/// Route one decoded control message to its handler. All worker-side
/// failures are converted into ack messages (never propagated), so a
/// single bad job never tears down the worker.
fn dispatch(
    pool: &Pool,
    ack: &Channel,
    jobs: &mut HashMap<u128, WorkerJob>,
    msg: ControlMessage,
) -> Result<()> {
    match msg {
        ControlMessage::ChunkDescriptor {
            job_id,
            path,
            chunk_index,
            descriptor,
        } => {
            let ack_msg = match handle_chunk(pool, jobs, job_id, &path, chunk_index, &descriptor) {
                Ok(()) => AckMessage::ChunkAck {
                    job_id,
                    chunk_index,
                },
                Err(e) => {
                    // Drop any partial state so a later Commit fails cleanly.
                    cleanup_job(jobs, job_id);
                    AckMessage::ChunkFailed {
                        job_id,
                        chunk_index,
                        error: e.to_string(),
                    }
                }
            };
            send_ack(ack, &ack_msg)
        }
        ControlMessage::Commit {
            job_id,
            path,
            chunk_count,
            expected_hash,
            fsync,
        } => {
            let ack_msg =
                match handle_commit(jobs, job_id, &path, chunk_count, &expected_hash, fsync) {
                    Ok(final_path) => AckMessage::JobComplete {
                        job_id,
                        success: true,
                        path: final_path,
                        error: String::new(),
                    },
                    Err(e) => AckMessage::JobComplete {
                        job_id,
                        success: false,
                        path,
                        error: e.to_string(),
                    },
                };
            send_ack(ack, &ack_msg)
        }
        ControlMessage::Cancel { job_id } => {
            cleanup_job(jobs, job_id);
            send_ack(ack, &AckMessage::CancelAck { job_id })
        }
        // Shutdown is handled in the run loop before dispatch.
        ControlMessage::Shutdown => Ok(()),
    }
}

/// Stream one chunk into the job's temp file. Creates the temp file on
/// the first (index 0) chunk; enforces in-order arrival thereafter.
fn handle_chunk(
    pool: &Pool,
    jobs: &mut HashMap<u128, WorkerJob>,
    job_id: u128,
    path: &str,
    chunk_index: u32,
    descriptor: &tessera_pool::Descriptor,
) -> Result<()> {
    let bytes = pool.read_payload(descriptor)?;

    if !jobs.contains_key(&job_id) {
        // First chunk of a job must be index 0. Anything else means we
        // already dropped this job's state after an earlier failure.
        if chunk_index != 0 {
            return Err(TesseraSinkError::Protocol(format!(
                "job {} first observed chunk has index {chunk_index}, expected 0",
                job_id_hex(job_id)
            )));
        }
        let final_path = PathBuf::from(path);
        let temp_path = temp_path_for(&final_path, job_id)?;
        let file = File::create(&temp_path)?;
        jobs.insert(
            job_id,
            WorkerJob {
                final_path,
                temp_path,
                file,
                hasher: blake3::Hasher::new(),
                next_chunk_index: 0,
            },
        );
    }

    let job = jobs.get_mut(&job_id).expect("inserted above");
    if chunk_index != job.next_chunk_index {
        return Err(TesseraSinkError::Protocol(format!(
            "job {} expected chunk {}, got {chunk_index}",
            job_id_hex(job_id),
            job.next_chunk_index
        )));
    }
    job.file.write_all(&bytes)?;
    job.hasher.update(&bytes);
    job.next_chunk_index += 1;
    Ok(())
}

/// Finalize a job: verify chunk count + hash, fsync if requested,
/// atomically rename the temp file into place. Returns the final path
/// string on success.
fn handle_commit(
    jobs: &mut HashMap<u128, WorkerJob>,
    job_id: u128,
    path: &str,
    chunk_count: u32,
    expected_hash: &[u8; 32],
    fsync: bool,
) -> Result<String> {
    let job = jobs.remove(&job_id).ok_or_else(|| TesseraSinkError::JobFailed {
        job_id: job_id_hex(job_id),
        path: path.to_string(),
        message: "no received chunks at commit (all chunks may have failed)".into(),
    })?;

    // From here, any failure must still delete the temp file.
    let result = (|| -> Result<()> {
        if job.next_chunk_index != chunk_count {
            return Err(TesseraSinkError::ChunkCountMismatch {
                job_id: job_id_hex(job_id),
                expected: chunk_count,
                actual: job.next_chunk_index,
            });
        }
        let actual = job.hasher.finalize();
        if actual.as_bytes() != expected_hash {
            return Err(TesseraSinkError::HashMismatch {
                job_id: job_id_hex(job_id),
                expected: hex32(expected_hash),
                actual: hex32(actual.as_bytes()),
            });
        }
        if fsync {
            job.file.sync_all()?;
        }
        Ok(())
    })();

    if let Err(e) = result {
        let _ = fs::remove_file(&job.temp_path);
        return Err(e);
    }

    // Drop the file handle before rename (correct on all platforms).
    let WorkerJob {
        final_path,
        temp_path,
        ..
    } = job;
    if let Err(e) = fs::rename(&temp_path, &final_path) {
        let _ = fs::remove_file(&temp_path);
        return Err(e.into());
    }
    Ok(final_path.to_string_lossy().into_owned())
}

/// Build the temp path: a dotfile sibling of the final path in the
/// same directory (same filesystem → rename is atomic).
fn temp_path_for(final_path: &std::path::Path, job_id: u128) -> Result<PathBuf> {
    let file_name = final_path
        .file_name()
        .ok_or_else(|| {
            TesseraSinkError::Config(format!(
                "target path {final_path:?} has no file name component"
            ))
        })?
        .to_string_lossy();
    let parent = final_path.parent().unwrap_or_else(|| std::path::Path::new("."));
    Ok(parent.join(format!(".{file_name}.{}.tmp", job_id_hex(job_id))))
}

/// Best-effort cleanup for one job: remove its temp file, drop state.
fn cleanup_job(jobs: &mut HashMap<u128, WorkerJob>, job_id: u128) {
    if let Some(job) = jobs.remove(&job_id) {
        let _ = fs::remove_file(&job.temp_path);
    }
}

/// Best-effort cleanup for every in-flight job (on shutdown / exit).
fn cleanup_all(jobs: &mut HashMap<u128, WorkerJob>) {
    for (_, job) in jobs.drain() {
        let _ = fs::remove_file(&job.temp_path);
    }
}

/// Push an ack with a bounded timeout. A timeout means the owner has
/// stopped draining (hung or died) → fatal for the worker.
fn send_ack(ack: &Channel, msg: &AckMessage) -> Result<()> {
    ack.send_timeout(&msg.encode(), ACK_SEND_TIMEOUT)
        .map_err(|e| match e {
            TesseraChannelError::Timeout { .. } => TesseraSinkError::Timeout {
                timeout_micros: ACK_SEND_TIMEOUT.as_micros() as u64,
                context: "ack send (owner not draining)".into(),
            },
            other => other.into(),
        })
}

fn hex32(bytes: &[u8; 32]) -> String {
    let mut s = String::with_capacity(64);
    for b in bytes {
        use std::fmt::Write;
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// True if this process has been reparented to init (owner died).
#[cfg(unix)]
fn parent_is_gone() -> bool {
    // SAFETY: getppid is always safe; it takes no args and only reads.
    unsafe { libc::getppid() == 1 }
}

#[cfg(not(unix))]
fn parent_is_gone() -> bool {
    false
}
