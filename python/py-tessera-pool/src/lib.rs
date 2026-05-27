//! Python facade for tessera-pool.
//!
//! Thin wrapper around `tessera_pool::Pool` exposed via PyO3:
//! - `tessera_pool.Pool` — context-manager-friendly Pool class.
//! - `tessera_pool.Lease` — opaque owner-side handle (return value).
//! - `tessera_pool.Descriptor` — read-only IPC token (return value).
//! - `tessera_pool.TesseraPoolError` — base Python exception.
//!
//! The facade owns ergonomics only — every data operation delegates
//! to the Rust core. No serialization happens in Python.

use std::time::Duration;

use parking_lot::Mutex;
use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use tessera_pool::{
    Descriptor as RustDescriptor, Lease as RustLease, LeaseId, Pool as RustPool, PoolConfig,
    TesseraPoolError as RustPoolError,
};

// One base exception class for the whole crate. Specific failure
// modes surface as the message; programmatic catch on type works at
// the base level (matches what Python users expect from new packages).
create_exception!(_native, TesseraPoolError, PyException);

fn map_err(e: RustPoolError) -> PyErr {
    TesseraPoolError::new_err(e.to_string())
}

/// Validate that a Python-supplied f64 seconds value is safe to convert
/// to a Duration (or to multiply by 1e6 into u64 micros).
///
/// `Duration::from_secs_f64` panics on NaN, infinity, negative values,
/// and values larger than Duration::MAX (~584 years). We catch all of
/// those before they reach the Rust core. Zero is allowed iff
/// `allow_zero` (true for timeouts, false for TTLs).
fn validate_seconds(field: &str, value: f64, allow_zero: bool) -> PyResult<()> {
    if !value.is_finite() {
        return Err(TesseraPoolError::new_err(format!(
            "{field} must be finite (got {value}); inf/NaN not allowed"
        )));
    }
    let min_ok = if allow_zero { value >= 0.0 } else { value > 0.0 };
    if !min_ok {
        return Err(TesseraPoolError::new_err(format!(
            "{field} must be {} (got {value})",
            if allow_zero { ">= 0" } else { "> 0" }
        )));
    }
    // 100 years in seconds = 3.155_692_6e9. Anything beyond this is
    // almost certainly a unit-conversion bug at the caller (e.g. they
    // passed micros instead of seconds). Bounded well under Duration::MAX
    // so the subsequent `Duration::from_secs_f64` cannot panic.
    const MAX_SECONDS: f64 = 100.0 * 365.25 * 86400.0;
    if value > MAX_SECONDS {
        return Err(TesseraPoolError::new_err(format!(
            "{field} {value} is unreasonably large (max {MAX_SECONDS:.0}); \
            check unit conversion (expected seconds, not micros / millis)"
        )));
    }
    Ok(())
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

    /// Picklable via `(_lease_from_bytes, (slot_index, generation, lease_id_bytes))`.
    fn __reduce__<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<(Bound<'py, PyAny>, (u32, u64, Bound<'py, PyBytes>))> {
        let factory = py
            .import_bound("tessera_pool")?
            .getattr("_lease_from_bytes")?;
        let lease_bytes = PyBytes::new_bound(py, &self.inner.lease_id().to_bytes());
        Ok((
            factory,
            (self.inner.slot_index(), self.inner.generation(), lease_bytes),
        ))
    }
}

/// Factory used by `Lease.__reduce__` to rebuild a Lease from
/// (slot_index, generation, lease_id_bytes). Exposed at module level
/// so pickle can resolve it as `tessera_pool._lease_from_bytes`.
#[pyfunction]
fn _lease_from_bytes(slot_index: u32, generation: u64, lease_id_bytes: &[u8]) -> PyResult<PyLease> {
    let bytes: [u8; 16] = lease_id_bytes.try_into().map_err(|_| {
        pyo3::exceptions::PyValueError::new_err("lease_id_bytes must be 16 bytes")
    })?;
    Ok(PyLease {
        inner: RustLease::new(slot_index, LeaseId::from_bytes(bytes), generation),
    })
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

    /// Picklable via `(_descriptor_from_bytes, (slot_index, generation, lease_id_bytes, size_bytes))`.
    /// This is the canonical IPC handoff path: send a Descriptor through a
    /// multiprocessing.Queue / Pipe; pickle reconstructs it on the worker side.
    fn __reduce__<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<(Bound<'py, PyAny>, (u32, u64, Bound<'py, PyBytes>, u32))> {
        let factory = py
            .import_bound("tessera_pool")?
            .getattr("_descriptor_from_bytes")?;
        let lease_bytes = PyBytes::new_bound(py, &self.inner.lease_id().to_bytes());
        Ok((
            factory,
            (
                self.inner.slot_index(),
                self.inner.generation(),
                lease_bytes,
                self.inner.size_bytes(),
            ),
        ))
    }
}

/// Factory used by `Descriptor.__reduce__` to rebuild a Descriptor from
/// (slot_index, generation, lease_id_bytes, size_bytes). Exposed at
/// module level so pickle can resolve it as
/// `tessera_pool._descriptor_from_bytes`.
#[pyfunction]
fn _descriptor_from_bytes(
    slot_index: u32,
    generation: u64,
    lease_id_bytes: &[u8],
    size_bytes: u32,
) -> PyResult<PyDescriptor> {
    let bytes: [u8; 16] = lease_id_bytes.try_into().map_err(|_| {
        pyo3::exceptions::PyValueError::new_err("lease_id_bytes must be 16 bytes")
    })?;
    Ok(PyDescriptor {
        inner: RustDescriptor::new(slot_index, LeaseId::from_bytes(bytes), generation, size_bytes),
    })
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
    //
    // Option<RustPool> so `close()` / `__exit__` can deterministically
    // drop the inner Pool (which unlinks the SHM region in its Drop
    // impl) without relying on Python's GC timing. After close, all
    // operations return TesseraPoolError("Pool is closed").
    inner: Mutex<Option<RustPool>>,
    // Cached so getters don't have to lock.
    is_owner: bool,
    slot_count: u32,
    slot_size_bytes: u32,
    ttl_micros: u64,
}

impl PyPool {
    /// Locked-mutable access to the inner Pool, or `Pool is closed` if
    /// the user has already exited the context manager.
    fn with_inner_mut<R>(
        &self,
        op: impl FnOnce(&mut RustPool) -> Result<R, RustPoolError>,
    ) -> PyResult<R> {
        let mut guard = self.inner.lock();
        let pool = guard
            .as_mut()
            .ok_or_else(|| TesseraPoolError::new_err("Pool is closed"))?;
        op(pool).map_err(map_err)
    }

    /// Locked-immutable access (only `read_payload` uses this).
    fn with_inner<R>(
        &self,
        op: impl FnOnce(&RustPool) -> Result<R, RustPoolError>,
    ) -> PyResult<R> {
        let guard = self.inner.lock();
        let pool = guard
            .as_ref()
            .ok_or_else(|| TesseraPoolError::new_err("Pool is closed"))?;
        op(pool).map_err(map_err)
    }
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
    /// - `force_recreate` (default `False`): owner-side recovery
    ///   escape hatch for crashed-prior-owner scenarios. When True,
    ///   the existing SHM segment is unconditionally unlinked and
    ///   recreated. Misuse will silently clobber a live peer; only
    ///   set this during explicit recovery. Ignored on attach.
    #[new]
    #[pyo3(signature = (*, description, slot_count, slot_size_bytes, is_owner=true, ttl_seconds=60.0, force_recreate=false))]
    fn new(
        description: String,
        slot_count: u32,
        slot_size_bytes: u32,
        is_owner: bool,
        ttl_seconds: f64,
        force_recreate: bool,
    ) -> PyResult<Self> {
        let ttl_micros = if is_owner {
            validate_seconds("ttl_seconds", ttl_seconds, /*allow_zero=*/ false)?;
            // Safe: value is finite and within bounds (validate_seconds
            // checked); multiplication by 1e6 stays well below u64::MAX
            // (100 years in micros ≈ 3.15e15, far under u64::MAX 1.8e19).
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
            force_recreate,
        };
        let pool = RustPool::new(config).map_err(map_err)?;
        Ok(Self {
            is_owner: pool.is_owner(),
            slot_count: pool.slot_count(),
            slot_size_bytes: pool.slot_size_bytes(),
            ttl_micros: pool.ttl_micros(),
            inner: Mutex::new(Some(pool)),
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
    fn in_use_count(&self) -> PyResult<u32> {
        self.with_inner(|p| p.in_use_count())
    }

    /// True if the Pool has been closed (either via `close()` or by
    /// leaving its `with` block).
    #[getter]
    fn is_closed(&self) -> bool {
        self.inner.lock().is_none()
    }

    /// Acquire one free slot (owner-only). Blocks up to
    /// `timeout_seconds` for availability.
    #[pyo3(signature = (timeout_seconds=30.0))]
    fn acquire(&self, timeout_seconds: f64) -> PyResult<PyLease> {
        validate_seconds("timeout_seconds", timeout_seconds, /*allow_zero=*/ true)?;
        // Safe: validate_seconds confirmed value is finite and within
        // Duration's safe range, so from_secs_f64 cannot panic.
        let timeout = Duration::from_secs_f64(timeout_seconds);
        self.with_inner_mut(|p| p.acquire(timeout))
            .map(|inner| PyLease { inner })
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
        // Later: `py.allow_threads(...)` after pulling out an owned
        // payload bytes Vec; needs care around the Send bound on the
        // captured mutex guard.
        let bytes = payload.as_bytes();
        self.with_inner_mut(|p| p.write(&lease.inner, bytes))
            .map(|inner| PyDescriptor { inner })
    }

    /// Read the bytes referenced by a descriptor. Available to both
    /// owner and attacher Pool instances; validates that the
    /// descriptor isn't stale before returning.
    fn read_payload<'py>(
        &self,
        py: Python<'py>,
        descriptor: &PyDescriptor,
    ) -> PyResult<Bound<'py, PyBytes>> {
        let bytes = self.with_inner(|p| p.read_payload(&descriptor.inner))?;
        Ok(PyBytes::new_bound(py, &bytes))
    }

    /// Release a leased slot (owner-only).
    fn release(&self, lease: &PyLease) -> PyResult<()> {
        self.with_inner_mut(|p| p.release(&lease.inner))
    }

    /// Renew a lease's `acquired_at` (owner-only). Use during long
    /// owner-side operations to prevent reclaim_stale from reclaiming.
    fn renew(&self, lease: &PyLease) -> PyResult<()> {
        self.with_inner_mut(|p| p.renew(&lease.inner))
    }

    /// Reclaim slots whose lease has been outstanding longer than the
    /// configured TTL. Returns the count reclaimed. Owner-only.
    fn reclaim_stale(&self) -> PyResult<u32> {
        self.with_inner_mut(|p| p.reclaim_stale())
    }

    /// Drop the underlying Rust Pool, unlinking the SHM region (for
    /// owner Pools) and detaching the mapping (for attachers).
    ///
    /// Idempotent: calling close() on an already-closed Pool is a no-op.
    /// After close, all other operations raise TesseraPoolError("Pool
    /// is closed").
    fn close(&self) -> PyResult<()> {
        // Taking the Option out of the Mutex drops the RustPool here;
        // Shmem's Drop runs the underlying shm_unlink for owner mappings.
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
        // Honor context-manager semantics: deterministic cleanup at
        // scope exit, NOT deferred to Python GC. Drops the RustPool
        // which unlinks the SHM region.
        self.close()
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
    m.add_function(wrap_pyfunction!(_lease_from_bytes, m)?)?;
    m.add_function(wrap_pyfunction!(_descriptor_from_bytes, m)?)?;
    m.add(
        "TesseraPoolError",
        py.get_type_bound::<TesseraPoolError>(),
    )?;
    Ok(())
}
