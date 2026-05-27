//! Sink owner state machine.
//!
//! Single-threaded by necessity: Pool and Channel wrap `shared_memory::Shmem`,
//! which is `!Send`, so the owner cannot hand them to background threads.
//! Instead the owner *cooperatively* drains the ack plane and renews
//! leases inside `submit` / `flush`. In practice workers ack within
//! milliseconds, so leases are released almost immediately; the renewal
//! timer is the safety net for in-flight chunks, and the Pool TTL plus
//! worker orphan-detection bound the worst case.

use std::collections::HashMap;
use std::process::Child;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tessera_channel::{Channel, ChannelConfig, ChannelRole, TesseraChannelError};
use tessera_pool::{Lease, Pool, PoolConfig};

use crate::config::SinkConfig;
use crate::error::{Result, TesseraSinkError};
use crate::messages::{job_id_hex, AckMessage, ControlMessage};
use crate::worker::WorkerParams;
use crate::{names, spawn};

/// How long the owner waits for a worker to create its control region
/// (and thus for the owner's Sender attach to succeed) at startup.
const CONTROL_ATTACH_TIMEOUT: Duration = Duration::from_secs(10);

/// How long a control-plane send blocks before failing (worker hung/dead).
const CONTROL_SEND_TIMEOUT: Duration = Duration::from_secs(30);

/// Grace period for a worker to exit after `Shutdown` before we kill it.
const WORKER_EXIT_GRACE: Duration = Duration::from_secs(2);

/// How long `Sink::start` waits for all workers to signal readiness.
const WORKER_READY_TIMEOUT: Duration = Duration::from_secs(10);

/// Terminal-or-pending status of a submitted job.
#[derive(Clone, Debug, PartialEq, Eq)]
enum JobStatus {
    Pending,
    Succeeded,
    Failed(String),
    Cancelled,
}

impl JobStatus {
    fn is_terminal(&self) -> bool {
        !matches!(self, JobStatus::Pending)
    }
}

struct JobState {
    path: String,
    worker_id: u32,
    status: JobStatus,
}

/// Atomic-write worker pool to disk. Owns a Pool, an ack Channel, N
/// per-worker control Channels, and N worker subprocesses.
pub struct Sink {
    config: SinkConfig,
    base: String,
    pool: Pool,
    ack: Channel,
    controls: Vec<Channel>,
    workers: Vec<Child>,
    /// Owner-held leases for chunks the workers haven't acked yet,
    /// keyed by `(job_id, chunk_index)`.
    outstanding: HashMap<(u128, u32), Lease>,
    jobs: HashMap<u128, JobState>,
    last_renew: Instant,
    closed: bool,
}

impl Sink {
    /// Start a Sink: create the Pool + ack Channel, spawn N workers,
    /// and attach a control Channel to each.
    pub fn start(config: SinkConfig) -> Result<Self> {
        config.validate()?;
        let base = config.description.clone();

        let pool = Pool::new(PoolConfig {
            description: names::pool(&base),
            slot_count: config.pool_slot_count,
            slot_size_bytes: config.pool_slot_size_bytes,
            is_owner: true,
            ttl_micros: config.ttl_micros,
            force_recreate: config.force_recreate,
        })?;

        // Owner reads the ack plane → owner creates it (Receiver).
        let ack = Channel::open(ChannelConfig {
            description: names::ack(&base),
            slot_count: config.ack_slot_count,
            slot_size_bytes: config.ack_slot_size_bytes,
            role: ChannelRole::Receiver,
            force_recreate: config.force_recreate,
        })?;

        let bin = spawn::resolve_worker_bin(&config)?;

        let mut workers: Vec<Child> = Vec::with_capacity(config.worker_count as usize);
        let mut controls: Vec<Channel> = Vec::with_capacity(config.worker_count as usize);

        // Startup is a three-step barrier. On any failure, kill every
        // child spawned so far: dropping `Child` does NOT terminate the
        // process, so survivors would become orphans holding SHM
        // mappings and blocking a retry.
        //
        // 1. Spawn all workers. Each creates its own control region
        //    (Receiver) and signals readiness on the ack plane.
        for worker_id in 0..config.worker_count {
            let params = Self::worker_params(&base, &config, worker_id);
            match spawn::build_worker_command(&bin, &params).spawn() {
                Ok(child) => workers.push(child),
                Err(e) => {
                    Self::kill_all(&mut workers);
                    return Err(TesseraSinkError::WorkerSpawn {
                        worker_id,
                        message: e.to_string(),
                    });
                }
            }
        }

        // 2. Wait for every worker's WorkerReady. This is the barrier
        //    that prevents binding to a stale control region: a worker
        //    only signals after it has created (force_recreate-clobbered,
        //    if needed) its control region, so by the time we attach in
        //    step 3 the region name resolves to the fresh segment.
        if let Err(e) = Self::await_all_ready(&ack, &mut workers, config.worker_count) {
            Self::kill_all(&mut workers);
            return Err(e);
        }

        // 3. Attach a control Sender to each worker. The region is
        //    guaranteed to exist now, so this succeeds promptly.
        for worker_id in 0..config.worker_count {
            let child = &mut workers[worker_id as usize];
            match Self::attach_control(&base, &config, worker_id, child) {
                Ok(control) => controls.push(control),
                Err(e) => {
                    Self::kill_all(&mut workers);
                    return Err(e);
                }
            }
        }

        Ok(Self {
            config,
            base,
            pool,
            ack,
            controls,
            workers,
            outstanding: HashMap::new(),
            jobs: HashMap::new(),
            last_renew: Instant::now(),
            closed: false,
        })
    }

    fn worker_params(base: &str, config: &SinkConfig, worker_id: u32) -> WorkerParams {
        WorkerParams {
            pool_description: names::pool(base),
            control_description: names::control(base, worker_id),
            ack_description: names::ack(base),
            pool_slot_count: config.pool_slot_count,
            pool_slot_size_bytes: config.pool_slot_size_bytes,
            control_slot_count: config.control_slot_count,
            control_slot_size_bytes: config.control_slot_size_bytes,
            ack_slot_count: config.ack_slot_count,
            ack_slot_size_bytes: config.ack_slot_size_bytes,
            worker_id,
            force_recreate: config.force_recreate,
        }
    }

    /// Kill and reap every spawned child. Best-effort; used on a failed
    /// start so no worker survives as an orphan holding SHM mappings.
    fn kill_all(workers: &mut Vec<Child>) {
        for mut child in workers.drain(..) {
            let _ = child.kill();
            let _ = child.wait();
        }
    }

    /// Drain the ack plane until every worker has sent `WorkerReady`.
    /// Bounded by `WORKER_READY_TIMEOUT`; fails fast if a child exits
    /// before signalling.
    fn await_all_ready(ack: &Channel, workers: &mut [Child], worker_count: u32) -> Result<()> {
        let deadline = Instant::now() + WORKER_READY_TIMEOUT;
        let mut ready = vec![false; worker_count as usize];
        let mut remaining = worker_count;
        while remaining > 0 {
            match ack.recv_timeout(Duration::from_millis(50)) {
                Ok(bytes) => {
                    if let AckMessage::WorkerReady { worker_id } = AckMessage::decode(&bytes)? {
                        if let Some(slot) = ready.get_mut(worker_id as usize) {
                            if !*slot {
                                *slot = true;
                                remaining -= 1;
                            }
                        }
                    }
                    // Only WorkerReady can arrive before startup completes.
                }
                Err(TesseraChannelError::Timeout { .. }) => {
                    for (i, child) in workers.iter_mut().enumerate() {
                        if let Ok(Some(status)) = child.try_wait() {
                            return Err(TesseraSinkError::WorkerSpawn {
                                worker_id: i as u32,
                                message: format!("worker exited during startup: {status}"),
                            });
                        }
                    }
                    if Instant::now() >= deadline {
                        return Err(TesseraSinkError::Timeout {
                            timeout_micros: WORKER_READY_TIMEOUT.as_micros() as u64,
                            context: format!(
                                "waiting for worker readiness ({} of {worker_count} ready)",
                                worker_count - remaining
                            ),
                        });
                    }
                }
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    }

    fn attach_control(
        base: &str,
        config: &SinkConfig,
        worker_id: u32,
        child: &mut Child,
    ) -> Result<Channel> {
        let deadline = Instant::now() + CONTROL_ATTACH_TIMEOUT;
        let cfg = ChannelConfig {
            description: names::control(base, worker_id),
            slot_count: config.control_slot_count,
            slot_size_bytes: config.control_slot_size_bytes,
            role: ChannelRole::Sender,
            force_recreate: false,
        };
        loop {
            match Channel::open(cfg.clone()) {
                Ok(c) => return Ok(c),
                Err(_) => {
                    // The worker signalled WorkerReady before we got here,
                    // so its control region exists; this retry is just
                    // belt-and-suspenders for a transient open failure.
                    // Fail fast if the worker died in the meantime.
                    if let Ok(Some(status)) = child.try_wait() {
                        return Err(TesseraSinkError::WorkerSpawn {
                            worker_id,
                            message: format!("worker exited during startup: {status}"),
                        });
                    }
                    if Instant::now() >= deadline {
                        return Err(TesseraSinkError::Timeout {
                            timeout_micros: CONTROL_ATTACH_TIMEOUT.as_micros() as u64,
                            context: format!("attaching control channel for worker {worker_id}"),
                        });
                    }
                    std::thread::sleep(Duration::from_millis(20));
                }
            }
        }
    }

    /// Submit a payload to be written atomically to `path`. Splits the
    /// payload into Pool-sized chunks, hands each to the affinity-chosen
    /// worker, and issues a `Commit`. Returns when all chunks + the
    /// commit have been *sent* (not when the file is on disk — call
    /// [`Sink::flush`] to await completion). Returns the 128-bit job id.
    pub fn submit(&mut self, path: &str, bytes: &[u8], fsync: bool) -> Result<u128> {
        if self.closed {
            return Err(TesseraSinkError::Closed);
        }
        let slot = self.config.pool_slot_size_bytes as usize;
        // Empty payload → one zero-length chunk so the worker still
        // creates + renames an (empty) file.
        let chunks: Vec<&[u8]> = if bytes.is_empty() {
            vec![&[]]
        } else {
            bytes.chunks(slot).collect()
        };
        let chunk_count = chunks.len() as u32;
        let expected_hash: [u8; 32] = *blake3::hash(bytes).as_bytes();

        let job_id = random_u128();
        let worker_id = (job_id % self.config.worker_count as u128) as u32;
        self.jobs.insert(
            job_id,
            JobState {
                path: path.to_string(),
                worker_id,
                status: JobStatus::Pending,
            },
        );

        let result = self.submit_inner(job_id, worker_id, path, &chunks, chunk_count, expected_hash, fsync);
        if let Err(e) = result {
            // Roll back: release this job's leases, tell the worker to
            // abort, and mark the job failed.
            self.release_job_leases(job_id);
            let _ = self.send_control(worker_id, &ControlMessage::Cancel { job_id });
            self.mark_failed(job_id, format!("submit failed: {e}"));
            return Err(e);
        }
        Ok(job_id)
    }

    #[allow(clippy::too_many_arguments)]
    fn submit_inner(
        &mut self,
        job_id: u128,
        worker_id: u32,
        path: &str,
        chunks: &[&[u8]],
        chunk_count: u32,
        expected_hash: [u8; 32],
        fsync: bool,
    ) -> Result<()> {
        for (idx, chunk) in chunks.iter().enumerate() {
            let chunk_index = idx as u32;
            if chunk.len() > self.config.pool_slot_size_bytes as usize {
                return Err(TesseraSinkError::ChunkTooLarge {
                    chunk_size: chunk.len(),
                    slot_size: self.config.pool_slot_size_bytes as usize,
                });
            }
            let lease = self.acquire_with_drain()?;
            let descriptor = self.pool.write(&lease, chunk)?;
            self.outstanding.insert((job_id, chunk_index), lease);
            self.send_control(
                worker_id,
                &ControlMessage::ChunkDescriptor {
                    job_id,
                    path: path.to_string(),
                    chunk_index,
                    descriptor,
                },
            )?;
            self.maybe_renew();
        }
        self.send_control(
            worker_id,
            &ControlMessage::Commit {
                job_id,
                path: path.to_string(),
                chunk_count,
                expected_hash,
                fsync,
            },
        )?;
        Ok(())
    }

    /// Wait until every submitted job reaches a terminal state. Returns
    /// the first failure encountered (if any), clearing terminal job
    /// records so a subsequent `flush` starts clean.
    pub fn flush(&mut self) -> Result<()> {
        if self.closed {
            return Err(TesseraSinkError::Closed);
        }
        loop {
            self.drain_acks()?;
            self.maybe_renew();
            self.detect_dead_workers();
            if self.jobs.values().all(|j| j.status.is_terminal()) {
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }

        // Collect the first failure, then clear all terminal records.
        let mut first_failure: Option<TesseraSinkError> = None;
        for (job_id, job) in self.jobs.iter() {
            let message = match &job.status {
                JobStatus::Failed(m) => m.clone(),
                JobStatus::Cancelled => "job cancelled".to_string(),
                _ => continue,
            };
            if first_failure.is_none() {
                first_failure = Some(TesseraSinkError::JobFailed {
                    job_id: job_id_hex(*job_id),
                    path: job.path.clone(),
                    message,
                });
            }
        }
        self.jobs.clear();
        match first_failure {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// Acquire a Pool slot, draining acks (which release leases and free
    /// slots) while we wait, bounded by `acquire_timeout_micros`.
    fn acquire_with_drain(&mut self) -> Result<Lease> {
        let deadline = Instant::now() + Duration::from_micros(self.config.acquire_timeout_micros);
        loop {
            self.drain_acks()?;
            match self.pool.acquire(Duration::from_millis(10)) {
                Ok(lease) => return Ok(lease),
                Err(tessera_pool::TesseraPoolError::Timeout { .. }) => {
                    if Instant::now() >= deadline {
                        return Err(TesseraSinkError::Timeout {
                            timeout_micros: self.config.acquire_timeout_micros,
                            context: "acquiring a pool slot (all slots in flight)".into(),
                        });
                    }
                }
                Err(e) => return Err(e.into()),
            }
            self.maybe_renew();
        }
    }

    /// Non-blocking drain of the ack plane.
    fn drain_acks(&mut self) -> Result<()> {
        loop {
            match self.ack.try_recv() {
                Ok(bytes) => {
                    let msg = AckMessage::decode(&bytes)?;
                    self.handle_ack(msg);
                }
                Err(TesseraChannelError::ChannelEmpty { .. }) => return Ok(()),
                Err(e) => return Err(e.into()),
            }
        }
    }

    fn handle_ack(&mut self, msg: AckMessage) {
        match msg {
            // Consumed during the startup barrier; ignore if it somehow
            // arrives later (e.g. a duplicate).
            AckMessage::WorkerReady { .. } => {}
            AckMessage::ChunkAck {
                job_id,
                chunk_index,
            } => {
                self.release_lease(job_id, chunk_index);
            }
            AckMessage::ChunkFailed {
                job_id,
                chunk_index,
                error,
            } => {
                self.release_lease(job_id, chunk_index);
                let worker_id = self.jobs.get(&job_id).map(|j| j.worker_id);
                if let Some(worker_id) = worker_id {
                    let _ = self.send_control(worker_id, &ControlMessage::Cancel { job_id });
                }
                self.mark_failed(job_id, format!("chunk {chunk_index} failed: {error}"));
            }
            AckMessage::CancelAck { job_id } => {
                self.release_job_leases(job_id);
                if let Some(job) = self.jobs.get_mut(&job_id) {
                    if !job.status.is_terminal() {
                        job.status = JobStatus::Cancelled;
                    }
                }
            }
            AckMessage::JobComplete {
                job_id,
                success,
                error,
                ..
            } => {
                if let Some(job) = self.jobs.get_mut(&job_id) {
                    job.status = if success {
                        JobStatus::Succeeded
                    } else {
                        JobStatus::Failed(error)
                    };
                }
            }
        }
    }

    /// Renew every outstanding lease if at least `ttl/2` has elapsed.
    fn maybe_renew(&mut self) {
        let half_ttl = Duration::from_micros(self.config.ttl_micros / 2);
        if self.last_renew.elapsed() < half_ttl {
            return;
        }
        let leases: Vec<Lease> = self.outstanding.values().copied().collect();
        for lease in leases {
            // A lease may already be gone (reclaimed) — ignore.
            let _ = self.pool.renew(&lease);
        }
        self.last_renew = Instant::now();
    }

    fn release_lease(&mut self, job_id: u128, chunk_index: u32) {
        if let Some(lease) = self.outstanding.remove(&(job_id, chunk_index)) {
            let _ = self.pool.release(&lease);
        }
    }

    fn release_job_leases(&mut self, job_id: u128) {
        let keys: Vec<(u128, u32)> = self
            .outstanding
            .keys()
            .filter(|(jid, _)| *jid == job_id)
            .copied()
            .collect();
        for key in keys {
            if let Some(lease) = self.outstanding.remove(&key) {
                let _ = self.pool.release(&lease);
            }
        }
    }

    fn mark_failed(&mut self, job_id: u128, message: String) {
        if let Some(job) = self.jobs.get_mut(&job_id) {
            if !job.status.is_terminal() {
                job.status = JobStatus::Failed(message);
            }
        }
    }

    /// Fail any still-pending job whose worker process has exited.
    fn detect_dead_workers(&mut self) {
        let mut dead: Vec<(u32, String)> = Vec::new();
        for (i, child) in self.workers.iter_mut().enumerate() {
            if let Ok(Some(status)) = child.try_wait() {
                dead.push((i as u32, format!("worker {i} exited: {status}")));
            }
        }
        if dead.is_empty() {
            return;
        }
        for (worker_id, message) in dead {
            let failed_jobs: Vec<u128> = self
                .jobs
                .iter()
                .filter(|(_, j)| j.worker_id == worker_id && !j.status.is_terminal())
                .map(|(id, _)| *id)
                .collect();
            for job_id in failed_jobs {
                self.release_job_leases(job_id);
                self.mark_failed(job_id, message.clone());
            }
        }
    }

    fn send_control(&self, worker_id: u32, msg: &ControlMessage) -> Result<()> {
        let control = self
            .controls
            .get(worker_id as usize)
            .ok_or_else(|| TesseraSinkError::Config(format!("no control channel for worker {worker_id}")))?;
        control
            .send_timeout(&msg.encode(), CONTROL_SEND_TIMEOUT)
            .map_err(|e| match e {
                TesseraChannelError::Timeout { .. } => TesseraSinkError::Timeout {
                    timeout_micros: CONTROL_SEND_TIMEOUT.as_micros() as u64,
                    context: format!("control send to worker {worker_id} (not draining)"),
                },
                other => other.into(),
            })
    }

    /// Number of worker subprocesses.
    pub fn worker_count(&self) -> u32 {
        self.config.worker_count
    }

    /// The base description this Sink namespaces its regions under.
    pub fn description(&self) -> &str {
        &self.base
    }

    /// True once `close` / drop has run.
    pub fn is_closed(&self) -> bool {
        self.closed
    }

    /// Gracefully shut down: signal every worker to stop, wait briefly,
    /// then kill stragglers. Idempotent. Called automatically on drop.
    pub fn close(&mut self) {
        if self.closed {
            return;
        }
        // Best-effort graceful stop.
        for control in &self.controls {
            let _ = control.send_timeout(
                &ControlMessage::Shutdown.encode(),
                Duration::from_millis(200),
            );
        }
        for child in &mut self.workers {
            let deadline = Instant::now() + WORKER_EXIT_GRACE;
            loop {
                match child.try_wait() {
                    Ok(Some(_)) => break,
                    Ok(None) => {
                        if Instant::now() >= deadline {
                            let _ = child.kill();
                            let _ = child.wait();
                            break;
                        }
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    Err(_) => break,
                }
            }
        }
        self.closed = true;
        // Pool + ack Channel drop here, unlinking the owner-created
        // regions. Each control region was owned by its (now exited)
        // worker; our Sender handles just detach.
    }
}

impl Drop for Sink {
    fn drop(&mut self) {
        self.close();
    }
}

/// Draw a 128-bit job id. Prefers `/dev/urandom`; if that's
/// unavailable, falls back to a mix guaranteed unique *within this
/// process* (a monotonic counter in the high 64 bits, so successive
/// calls never collide) plus pid + wall-clock nanos to reduce
/// cross-process collision odds. Job ids key `jobs` / `outstanding`,
/// so a collision would corrupt in-flight state — uniqueness matters
/// more here than cryptographic randomness.
fn random_u128() -> u128 {
    let mut bytes = [0u8; 16];
    if let Ok(mut f) = std::fs::File::open("/dev/urandom") {
        use std::io::Read;
        if f.read_exact(&mut bytes).is_ok() {
            return u128::from_le_bytes(bytes);
        }
    }
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id() as u64;
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    // High 64 bits = monotonic counter → unique per process per call.
    // Low 64 bits = pid mixed with wall-clock nanos.
    ((n as u128) << 64) | ((pid as u128) << 32) | ((nanos as u64) as u128 & 0xFFFF_FFFF)
}
