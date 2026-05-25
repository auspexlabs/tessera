//! Python facade for tessera-pool.
//!
//! Thin wrapper around `tessera_pool::Pool` exposed via PyO3:
//! - `tessera_pool.Pool` — context-manager-friendly Pool class.
//! - `tessera_pool.Lease` — opaque owner-side handle (return value).
//! - `tessera_pool.Descriptor` — read-only IPC token (return value).
//! - `tessera_pool.TesseraPoolError` — base Python exception.
//!
//! The facade owns ergonomics only — every data operation delegates
//! to the Rust core. No serialization happens in Python (§3.4 lock).

use std::time::Duration;

use parking_lot::Mutex;
use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use tessera_pool::{
    Descriptor as RustDescriptor, Lease as RustLease, Pool as RustPool, PoolConfig,
    TesseraPoolError as RustPoolError,
};

// One base exception class for the whole crate. Specific failure
// modes surface as the message; programmatic catch on type works at
// the base level (matches what Python users expect from new packages).
create_exception!(_native, TesseraPoolError, PyException);

fn map_err(e: RustPoolError) -> PyErr {
    TesseraPoolError::new_err(e.to_string())
}

/// Owner-side lease handle. Returned by `Pool.acquire`. Read-only
/// from Python; use as an opaque value.
#[pyclass(name = "Lease", module = "tessera_pool", frozen)]
#[derive(Clone)]
struct PyLease {
    inner: RustLease,
}

#[pymethods]
impl PyLease {
    #[getter]
    fn slot_index(&self) -> u32 {
        self.inner.slot_index()
    }

    #[getter]
    fn generation(&self) -> u64 {
        self.inner.generation()
    }

    #[getter]
    fn lease_id_hex(&self) -> String {
        format!("{}", self.inner.lease_id())
    }

    fn __repr__(&self) -> String {
        format!(
            "Lease(slot_index={}, generation={}, lease_id={})",
            self.inner.slot_index(),
            self.inner.generation(),
            self.inner.lease_id()
        )
    }
}

/// Read-only descriptor for cross-IPC handoff. Returned by `Pool.write`.
#[pyclass(name = "Descriptor", module = "tessera_pool", frozen)]
#[derive(Clone)]
struct PyDescriptor {
    inner: RustDescriptor,
}

#[pymethods]
impl PyDescriptor {
    #[getter]
    fn slot_index(&self) -> u32 {
        self.inner.slot_index()
    }

    #[getter]
    fn generation(&self) -> u64 {
        self.inner.generation()
    }

    #[getter]
    fn lease_id_hex(&self) -> String {
        format!("{}", self.inner.lease_id())
    }

    #[getter]
    fn size_bytes(&self) -> u32 {
        self.inner.size_bytes()
    }

    fn __repr__(&self) -> String {
        format!(
            "Descriptor(slot_index={}, generation={}, size_bytes={}, lease_id={})",
            self.inner.slot_index(),
            self.inner.generation(),
            self.inner.size_bytes(),
            self.inner.lease_id()
        )
    }
}

/// Non-lossy lease-backed shared-memory pool.
///
/// Construct with keyword arguments; use as a context manager for
/// scoped lifetime.
///
/// ```python
/// from tessera_pool import Pool
///
/// with Pool(description="my-app/batches",
///           slot_count=8,
///           slot_size_bytes=64 * 1024 * 1024) as pool:
///     lease = pool.acquire(timeout_seconds=1.0)
///     descriptor = pool.write(lease, payload_bytes)
///     # hand descriptor across IPC; worker calls pool.read_payload(descriptor)
///     pool.release(lease)
/// ```
#[pyclass(name = "Pool", module = "tessera_pool", unsendable)]
struct PyPool {
    // Interior mutability: PyO3 method receivers are &self, but the
    // underlying RustPool needs &mut for mutations. parking_lot::Mutex
    // is cheap and re-entrant-free (so we won't accidentally deadlock
    // ourselves).
    inner: Mutex<RustPool>,
    // Cached so getters don't have to lock.
    is_owner: bool,
    slot_count: u32,
    slot_size_bytes: u32,
    ttl_micros: u64,
}

#[pymethods]
impl PyPool {
    /// Construct a Pool.
    ///
    /// Required kwargs: `description`, `slot_count`, `slot_size_bytes`.
    ///
    /// Optional kwargs (with defaults):
    /// - `is_owner` (default `True`): create the SHM region (owner)
    ///   vs attach to an existing one (`False`).
    /// - `ttl_seconds` (default `60.0`): lease TTL in seconds.
    ///   Ignored on attach (TTL is inherited from the SHM header).
    #[new]
    #[pyo3(signature = (*, description, slot_count, slot_size_bytes, is_owner=true, ttl_seconds=60.0))]
    fn new(
        description: String,
        slot_count: u32,
        slot_size_bytes: u32,
        is_owner: bool,
        ttl_seconds: f64,
    ) -> PyResult<Self> {
        let ttl_micros = if is_owner {
            (ttl_seconds * 1_000_000.0).max(1.0) as u64
        } else {
            // Ignored on attach; underlying Pool::new inherits from
            // the header. Pass anything non-zero through the config.
            1
        };
        let config = PoolConfig {
            description,
            slot_count,
            slot_size_bytes,
            is_owner,
            ttl_micros,
        };
        let pool = RustPool::new(config).map_err(map_err)?;
        Ok(Self {
            is_owner: pool.is_owner(),
            slot_count: pool.slot_count(),
            slot_size_bytes: pool.slot_size_bytes(),
            ttl_micros: pool.ttl_micros(),
            inner: Mutex::new(pool),
        })
    }

    /// True if this Pool owns the SHM region's lifecycle.
    #[getter]
    fn is_owner(&self) -> bool {
        self.is_owner
    }

    /// Configured slot count.
    #[getter]
    fn slot_count(&self) -> u32 {
        self.slot_count
    }

    /// Configured per-slot byte capacity.
    #[getter]
    fn slot_size_bytes(&self) -> u32 {
        self.slot_size_bytes
    }

    /// TTL in seconds (owner-stamped; non-owners inherit).
    #[getter]
    fn ttl_seconds(&self) -> f64 {
        self.ttl_micros as f64 / 1_000_000.0
    }

    /// Current count of leased slots. Useful for monitoring.
    fn in_use_count(&self) -> u32 {
        self.inner.lock().in_use_count()
    }

    /// Acquire one free slot (owner-only). Blocks up to
    /// `timeout_seconds` for availability.
    #[pyo3(signature = (timeout_seconds=30.0))]
    fn acquire(&self, timeout_seconds: f64) -> PyResult<PyLease> {
        let timeout = Duration::from_secs_f64(timeout_seconds.max(0.0));
        let lease = self.inner.lock().acquire(timeout).map_err(map_err)?;
        Ok(PyLease { inner: lease })
    }

    /// Write a payload into the leased slot. One-shot per lease;
    /// raises `TesseraPoolError` on a second call. Returns a
    /// Descriptor suitable for cross-IPC handoff.
    fn write<'py>(
        &self,
        _py: Python<'py>,
        lease: &PyLease,
        payload: &Bound<'py, PyBytes>,
    ) -> PyResult<PyDescriptor> {
        // v0.1: hold the GIL through the copy. The bytes are slot-bounded
        // (configurable per pool) so worst-case latency is bounded.
        // Future: `py.allow_threads(...)` after pulling out an owned
        // payload bytes Vec; needs care around the Send bound on the
        // captured mutex guard.
        let bytes = payload.as_bytes();
        let descriptor = self
            .inner
            .lock()
            .write(&lease.inner, bytes)
            .map_err(map_err)?;
        Ok(PyDescriptor { inner: descriptor })
    }

    /// Read the bytes referenced by a descriptor. Available to both
    /// owner and attacher Pool instances; validates that the
    /// descriptor isn't stale before returning.
    fn read_payload<'py>(
        &self,
        py: Python<'py>,
        descriptor: &PyDescriptor,
    ) -> PyResult<Bound<'py, PyBytes>> {
        let bytes = self
            .inner
            .lock()
            .read_payload(&descriptor.inner)
            .map_err(map_err)?;
        Ok(PyBytes::new_bound(py, &bytes))
    }

    /// Release a leased slot (owner-only).
    fn release(&self, lease: &PyLease) -> PyResult<()> {
        self.inner.lock().release(&lease.inner).map_err(map_err)
    }

    /// Renew a lease's `acquired_at` (owner-only). Use during long
    /// owner-side operations to prevent reclaim_stale from reclaiming.
    fn renew(&self, lease: &PyLease) -> PyResult<()> {
        self.inner.lock().renew(&lease.inner).map_err(map_err)
    }

    /// Reclaim slots whose lease has been outstanding longer than the
    /// configured TTL. Returns the count reclaimed. Owner-only.
    fn reclaim_stale(&self) -> PyResult<u32> {
        self.inner.lock().reclaim_stale().map_err(map_err)
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
        // Resource cleanup happens via Drop on the underlying RustPool
        // when the Python object is finalized. No-op here keeps the
        // protocol satisfied.
        Ok(())
    }

    fn __repr__(&self) -> String {
        format!(
            "Pool(is_owner={}, slot_count={}, slot_size_bytes={}, ttl_seconds={:.3})",
            self.is_owner,
            self.slot_count,
            self.slot_size_bytes,
            self.ttl_micros as f64 / 1_000_000.0
        )
    }
}

#[pymodule]
fn _native(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyPool>()?;
    m.add_class::<PyLease>()?;
    m.add_class::<PyDescriptor>()?;
    m.add(
        "TesseraPoolError",
        py.get_type_bound::<TesseraPoolError>(),
    )?;
    Ok(())
}
