//! Python facade for tessera-sink.
//!
//! Thin wrapper around `tessera_sink::Sink` exposed via PyO3:
//! - `tessera_sink.Sink` — context-manager-friendly Sink class.
//! - `tessera_sink.TesseraSinkError` — base Python exception.
//!
//! The facade owns ergonomics only — chunking, serialization, hashing,
//! atomic write all live in the Rust core (§3.4 lock). The caller hands
//! `submit` pre-serialized `bytes`; the library never picks an encoding.
//!
//! ## v0.1 limitation (GIL)
//!
//! `tessera_sink::Sink` wraps `!Send` SHM handles, so the owner is
//! single-threaded and the Python class is `unsendable`. `submit` /
//! `flush` block (spin + sleep) while holding the GIL, mirroring the
//! Channel facade. Drive a Sink from one Python thread; for parallelism,
//! the worker *subprocesses* already provide it.

use std::path::PathBuf;

use parking_lot::Mutex;
use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use tessera_sink::{Sink as RustSink, SinkConfig, TesseraSinkError as RustSinkError};

create_exception!(_native, TesseraSinkError, PyException);

fn map_err(e: RustSinkError) -> PyErr {
    TesseraSinkError::new_err(e.to_string())
}

/// Atomic-write worker pool to disk.
///
/// ```python
/// from tessera_sink import Sink
///
/// with Sink(description="my-app/artifacts",
///           worker_count=4,
///           pool_slot_count=8,
///           pool_slot_size_bytes=64 * 1024 * 1024) as sink:
///     sink.submit("/data/out.parquet", payload_bytes, fsync=True)
///     sink.flush()
/// ```
///
/// The worker executable is discovered via (in order) the
/// `worker_bin_path` kwarg, the `TESSERA_SINK_WORKER_BIN` env var, a
/// sibling of the current executable, then `PATH`.
#[pyclass(name = "Sink", module = "tessera_sink", unsendable)]
struct PySink {
    // Boxed: `Sink` transitively embeds a `crossbeam_queue::SegQueue`
    // whose `CachePadded` is `#[repr(align(128))]`. Python's object
    // allocator only guarantees ~16-byte alignment for the pyclass
    // storage, so holding a `Sink` inline would be UB (misaligned).
    // The `Box` keeps only a pointer inline; the heap allocation honors
    // the 128-byte alignment.
    inner: Mutex<Option<Box<RustSink>>>,
    worker_count: u32,
    description: String,
}

impl PySink {
    fn with_inner_mut<R>(
        &self,
        op: impl FnOnce(&mut RustSink) -> Result<R, RustSinkError>,
    ) -> PyResult<R> {
        let mut guard = self.inner.lock();
        let sink = guard
            .as_mut()
            .ok_or_else(|| TesseraSinkError::new_err("Sink is closed"))?;
        op(&mut **sink).map_err(map_err)
    }
}

#[pymethods]
impl PySink {
    /// Start a Sink: create the Pool + ack channel, spawn `worker_count`
    /// worker subprocesses, attach the control channels.
    ///
    /// Required kwargs:
    ///   - `description`: base namespace string (regions derive from it).
    ///   - `worker_count`: number of worker subprocesses.
    ///   - `pool_slot_count`: in-flight chunk slots.
    ///   - `pool_slot_size_bytes`: max chunk size (payloads split to this).
    ///
    /// Optional kwargs (with defaults):
    ///   - `ttl_micros` (60_000_000): pool lease TTL.
    ///   - `acquire_timeout_micros` (15_000_000): slot-acquire timeout.
    ///   - `control_slot_count` (64) / `control_slot_size_bytes` (8192).
    ///   - `ack_slot_count` (256) / `ack_slot_size_bytes` (8192).
    ///   - `worker_bin_path` (None): explicit worker executable path.
    ///   - `force_recreate` (False): recovery escape hatch.
    #[new]
    #[pyo3(signature = (
        *,
        description,
        worker_count,
        pool_slot_count,
        pool_slot_size_bytes,
        ttl_micros = 60_000_000,
        acquire_timeout_micros = 15_000_000,
        control_slot_count = 64,
        control_slot_size_bytes = 8192,
        ack_slot_count = 256,
        ack_slot_size_bytes = 8192,
        worker_bin_path = None,
        force_recreate = false,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        description: String,
        worker_count: u32,
        pool_slot_count: u32,
        pool_slot_size_bytes: u32,
        ttl_micros: u64,
        acquire_timeout_micros: u64,
        control_slot_count: u32,
        control_slot_size_bytes: u32,
        ack_slot_count: u32,
        ack_slot_size_bytes: u32,
        worker_bin_path: Option<String>,
        force_recreate: bool,
    ) -> PyResult<Self> {
        let config = SinkConfig {
            description: description.clone(),
            worker_count,
            pool_slot_count,
            pool_slot_size_bytes,
            ttl_micros,
            acquire_timeout_micros,
            control_slot_count,
            control_slot_size_bytes,
            ack_slot_count,
            ack_slot_size_bytes,
            worker_bin_path: worker_bin_path.map(PathBuf::from),
            force_recreate,
        };
        let sink = RustSink::start(config).map_err(map_err)?;
        Ok(Self {
            inner: Mutex::new(Some(Box::new(sink))),
            worker_count,
            description,
        })
    }

    /// Submit `data` to be written atomically to `path`. Returns the
    /// 128-bit job id. The write completes asynchronously on a worker;
    /// call `flush` to wait for it and surface any failure.
    #[pyo3(signature = (path, data, fsync = false))]
    fn submit(&self, path: &str, data: &Bound<'_, PyBytes>, fsync: bool) -> PyResult<u128> {
        let bytes = data.as_bytes();
        self.with_inner_mut(|s| s.submit(path, bytes, fsync))
    }

    /// Wait for every submitted job to finish. Raises
    /// `TesseraSinkError` describing the first failure, if any.
    fn flush(&self) -> PyResult<()> {
        self.with_inner_mut(|s| s.flush())
    }

    /// Number of worker subprocesses.
    #[getter]
    fn worker_count(&self) -> u32 {
        self.worker_count
    }

    /// Base description this Sink namespaces its regions under.
    #[getter]
    fn description(&self) -> &str {
        &self.description
    }

    /// True once `close()` / `__exit__` has run.
    #[getter]
    fn is_closed(&self) -> bool {
        self.inner.lock().is_none()
    }

    /// Gracefully shut down the Sink (signal workers, wait, then kill
    /// stragglers) and unlink the owner-created SHM regions. Idempotent.
    fn close(&self) -> PyResult<()> {
        // Dropping the Rust Sink runs its graceful shutdown.
        self.inner.lock().take();
        Ok(())
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __exit__(
        &self,
        _exc_type: PyObject,
        _exc_value: PyObject,
        _traceback: PyObject,
    ) -> PyResult<()> {
        self.close()
    }

    fn __repr__(&self) -> String {
        format!(
            "Sink(description={:?}, worker_count={}, closed={})",
            self.description,
            self.worker_count,
            self.is_closed()
        )
    }
}

#[pymodule]
fn _native(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PySink>()?;
    m.add("TesseraSinkError", py.get_type_bound::<TesseraSinkError>())?;
    Ok(())
}
