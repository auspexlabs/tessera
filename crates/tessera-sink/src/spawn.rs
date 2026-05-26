//! Worker-process spawn: binary discovery + argv build/parse.
//!
//! The owner spawns each worker as a `tessera-sink-worker` OS process
//! (the locked v0.1 model — true multiprocess, fault-isolated, exercises
//! the real cross-process Pool + Channel path). This module is the
//! single source of truth for the argv contract: [`build_worker_command`]
//! (owner side) and [`parse_worker_args`] (bin side) are inverses, kept
//! in sync by the shared `ARG_*` constants and exercised by a round-trip
//! test.

use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::Command;

use crate::config::SinkConfig;
use crate::error::{Result, TesseraSinkError};
use crate::worker::WorkerParams;

/// Default executable name probed on `PATH` as a last resort.
pub const WORKER_BIN_NAME: &str = "tessera-sink-worker";

/// Env var the owner consults for an explicit worker-binary path when
/// `SinkConfig::worker_bin_path` is `None`.
pub const WORKER_BIN_ENV: &str = "TESSERA_SINK_WORKER_BIN";

const ARG_POOL_DESC: &str = "--pool-description";
const ARG_CONTROL_DESC: &str = "--control-description";
const ARG_ACK_DESC: &str = "--ack-description";
const ARG_POOL_SLOT_COUNT: &str = "--pool-slot-count";
const ARG_POOL_SLOT_SIZE: &str = "--pool-slot-size-bytes";
const ARG_CONTROL_SLOT_COUNT: &str = "--control-slot-count";
const ARG_CONTROL_SLOT_SIZE: &str = "--control-slot-size-bytes";
const ARG_ACK_SLOT_COUNT: &str = "--ack-slot-count";
const ARG_ACK_SLOT_SIZE: &str = "--ack-slot-size-bytes";
const ARG_WORKER_ID: &str = "--worker-id";

/// Build the `Command` that launches one worker with `params`.
pub fn build_worker_command(bin: &Path, params: &WorkerParams) -> Command {
    let mut cmd = Command::new(bin);
    cmd.arg(ARG_POOL_DESC)
        .arg(&params.pool_description)
        .arg(ARG_CONTROL_DESC)
        .arg(&params.control_description)
        .arg(ARG_ACK_DESC)
        .arg(&params.ack_description)
        .arg(ARG_POOL_SLOT_COUNT)
        .arg(params.pool_slot_count.to_string())
        .arg(ARG_POOL_SLOT_SIZE)
        .arg(params.pool_slot_size_bytes.to_string())
        .arg(ARG_CONTROL_SLOT_COUNT)
        .arg(params.control_slot_count.to_string())
        .arg(ARG_CONTROL_SLOT_SIZE)
        .arg(params.control_slot_size_bytes.to_string())
        .arg(ARG_ACK_SLOT_COUNT)
        .arg(params.ack_slot_count.to_string())
        .arg(ARG_ACK_SLOT_SIZE)
        .arg(params.ack_slot_size_bytes.to_string())
        .arg(ARG_WORKER_ID)
        .arg(params.worker_id.to_string());
    cmd
}

/// Parse a worker's argv (excluding argv[0]) into [`WorkerParams`].
/// Used by the `tessera-sink-worker` bin crate.
pub fn parse_worker_args<I>(args: I) -> Result<WorkerParams>
where
    I: IntoIterator<Item = OsString>,
{
    let mut pool_description = None;
    let mut control_description = None;
    let mut ack_description = None;
    let mut pool_slot_count = None;
    let mut pool_slot_size_bytes = None;
    let mut control_slot_count = None;
    let mut control_slot_size_bytes = None;
    let mut ack_slot_count = None;
    let mut ack_slot_size_bytes = None;
    let mut worker_id = None;

    let mut it = args.into_iter();
    while let Some(key) = it.next() {
        let key = key.to_string_lossy().into_owned();
        let val = it.next().ok_or_else(|| {
            TesseraSinkError::Config(format!("missing value for worker arg {key}"))
        })?;
        let val = val.to_string_lossy().into_owned();
        match key.as_str() {
            ARG_POOL_DESC => pool_description = Some(val),
            ARG_CONTROL_DESC => control_description = Some(val),
            ARG_ACK_DESC => ack_description = Some(val),
            ARG_POOL_SLOT_COUNT => pool_slot_count = Some(parse_u32(&key, &val)?),
            ARG_POOL_SLOT_SIZE => pool_slot_size_bytes = Some(parse_u32(&key, &val)?),
            ARG_CONTROL_SLOT_COUNT => control_slot_count = Some(parse_u32(&key, &val)?),
            ARG_CONTROL_SLOT_SIZE => control_slot_size_bytes = Some(parse_u32(&key, &val)?),
            ARG_ACK_SLOT_COUNT => ack_slot_count = Some(parse_u32(&key, &val)?),
            ARG_ACK_SLOT_SIZE => ack_slot_size_bytes = Some(parse_u32(&key, &val)?),
            ARG_WORKER_ID => worker_id = Some(parse_u32(&key, &val)?),
            other => {
                return Err(TesseraSinkError::Config(format!(
                    "unknown worker arg {other}"
                )))
            }
        }
    }

    Ok(WorkerParams {
        pool_description: require(pool_description, ARG_POOL_DESC)?,
        control_description: require(control_description, ARG_CONTROL_DESC)?,
        ack_description: require(ack_description, ARG_ACK_DESC)?,
        pool_slot_count: require(pool_slot_count, ARG_POOL_SLOT_COUNT)?,
        pool_slot_size_bytes: require(pool_slot_size_bytes, ARG_POOL_SLOT_SIZE)?,
        control_slot_count: require(control_slot_count, ARG_CONTROL_SLOT_COUNT)?,
        control_slot_size_bytes: require(control_slot_size_bytes, ARG_CONTROL_SLOT_SIZE)?,
        ack_slot_count: require(ack_slot_count, ARG_ACK_SLOT_COUNT)?,
        ack_slot_size_bytes: require(ack_slot_size_bytes, ARG_ACK_SLOT_SIZE)?,
        worker_id: require(worker_id, ARG_WORKER_ID)?,
    })
}

/// Locate the worker executable. Probe order: explicit config path →
/// `TESSERA_SINK_WORKER_BIN` env → sibling of the current executable →
/// bare `tessera-sink-worker` on `PATH`.
pub fn resolve_worker_bin(config: &SinkConfig) -> Result<PathBuf> {
    let mut tried = Vec::new();

    if let Some(p) = &config.worker_bin_path {
        tried.push(p.display().to_string());
        if p.exists() {
            return Ok(p.clone());
        }
        // Explicit request that doesn't exist is a hard error — don't
        // silently fall through to a different binary.
        return Err(TesseraSinkError::WorkerBinaryNotFound { tried });
    }

    if let Some(env_val) = std::env::var_os(WORKER_BIN_ENV) {
        let p = PathBuf::from(env_val);
        tried.push(p.display().to_string());
        if p.exists() {
            return Ok(p);
        }
    }

    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            let sibling = dir.join(WORKER_BIN_NAME);
            tried.push(sibling.display().to_string());
            if sibling.exists() {
                return Ok(sibling);
            }
        }
    }

    // Last resort: bare name, resolved via PATH at spawn time. We can't
    // cheaply stat PATH here, so hand back the bare name and let a spawn
    // failure surface as WorkerSpawn with the OS message.
    tried.push(format!("{WORKER_BIN_NAME} (via PATH)"));
    Ok(PathBuf::from(WORKER_BIN_NAME))
}

fn parse_u32(key: &str, val: &str) -> Result<u32> {
    val.parse::<u32>()
        .map_err(|e| TesseraSinkError::Config(format!("invalid u32 for {key}: {val:?} ({e})")))
}

fn require<T>(opt: Option<T>, name: &str) -> Result<T> {
    opt.ok_or_else(|| TesseraSinkError::Config(format!("missing required worker arg {name}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params() -> WorkerParams {
        WorkerParams {
            pool_description: "certus/artifacts/pool".into(),
            control_description: "certus/artifacts/control/2".into(),
            ack_description: "certus/artifacts/ack".into(),
            pool_slot_count: 8,
            pool_slot_size_bytes: 65536,
            control_slot_count: 64,
            control_slot_size_bytes: 4096,
            ack_slot_count: 256,
            ack_slot_size_bytes: 2048,
            worker_id: 2,
        }
    }

    #[test]
    fn argv_round_trips_through_command_and_parser() {
        let p = params();
        let cmd = build_worker_command(Path::new("/bin/tessera-sink-worker"), &p);
        // Extract the args the Command was built with.
        let args: Vec<OsString> = cmd.get_args().map(|a| a.to_owned()).collect();
        let parsed = parse_worker_args(args).expect("parse");
        assert_eq!(parsed.pool_description, p.pool_description);
        assert_eq!(parsed.control_description, p.control_description);
        assert_eq!(parsed.ack_description, p.ack_description);
        assert_eq!(parsed.pool_slot_count, p.pool_slot_count);
        assert_eq!(parsed.pool_slot_size_bytes, p.pool_slot_size_bytes);
        assert_eq!(parsed.control_slot_count, p.control_slot_count);
        assert_eq!(parsed.control_slot_size_bytes, p.control_slot_size_bytes);
        assert_eq!(parsed.ack_slot_count, p.ack_slot_count);
        assert_eq!(parsed.ack_slot_size_bytes, p.ack_slot_size_bytes);
        assert_eq!(parsed.worker_id, p.worker_id);
    }

    #[test]
    fn parse_rejects_missing_arg() {
        let err = parse_worker_args([OsString::from(ARG_WORKER_ID)]).unwrap_err();
        assert!(matches!(err, TesseraSinkError::Config(_)));
    }

    #[test]
    fn parse_rejects_unknown_arg() {
        let err = parse_worker_args([OsString::from("--nope"), OsString::from("1")]).unwrap_err();
        assert!(matches!(err, TesseraSinkError::Config(_)));
    }

    #[test]
    fn resolve_explicit_missing_path_errors() {
        let mut c = crate::config::tests_support_config();
        c.worker_bin_path = Some(PathBuf::from("/nonexistent/tessera-sink-worker-xyz"));
        let err = resolve_worker_bin(&c).unwrap_err();
        assert!(matches!(err, TesseraSinkError::WorkerBinaryNotFound { .. }));
    }
}
