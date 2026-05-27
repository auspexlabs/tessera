//! Python facade for tessera-channel.
//!
//! Thin wrapper around `tessera_channel::Channel` exposed via PyO3:
//! - `tessera_channel.Channel` — context-manager-friendly Channel class.
//! - `tessera_channel.TesseraChannelError` — base Python exception.
//!
//! The facade owns ergonomics only — every data operation delegates
//! to the Rust core. No serialization happens in Python.

use std::time::Duration;

use parking_lot::Mutex;
use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use tessera_channel::{
    Channel as RustChannel, ChannelConfig, ChannelRole,
    TesseraChannelError as RustChannelError,
};

// One base exception class for the whole crate.
create_exception!(_native, TesseraChannelError, PyException);

fn map_err(e: RustChannelError) -> PyErr {
    TesseraChannelError::new_err(e.to_string())
}

/// Validate that a Python-supplied f64 seconds value is safe to
/// convert to a `Duration`. Mirrors py-tessera-pool's discipline.
fn validate_seconds(field: &str, value: f64, allow_zero: bool) -> PyResult<()> {
    if !value.is_finite() {
        return Err(TesseraChannelError::new_err(format!(
            "{field} must be finite (got {value}); inf/NaN not allowed"
        )));
    }
    let min_ok = if allow_zero { value >= 0.0 } else { value > 0.0 };
    if !min_ok {
        return Err(TesseraChannelError::new_err(format!(
            "{field} must be {} (got {value})",
            if allow_zero { ">= 0" } else { "> 0" }
        )));
    }
    const MAX_SECONDS: f64 = 100.0 * 365.25 * 86400.0;
    if value > MAX_SECONDS {
        return Err(TesseraChannelError::new_err(format!(
            "{field} {value} is unreasonably large (max {MAX_SECONDS:.0}); \
            check unit conversion (expected seconds, not micros / millis)"
        )));
    }
    Ok(())
}

fn parse_role(s: &str) -> PyResult<ChannelRole> {
    match s.to_ascii_lowercase().as_str() {
        "receiver" | "recv" | "consumer" => Ok(ChannelRole::Receiver),
        "sender" | "send" | "producer" => Ok(ChannelRole::Sender),
        other => Err(TesseraChannelError::new_err(format!(
            "invalid role {other:?}: expected 'receiver' or 'sender'"
        ))),
    }
}

/// Non-lossy MPSC shared-memory queue.
///
/// Construct with keyword arguments; use as a context manager for
/// scoped lifetime.
///
/// ```python
/// from tessera_channel import Channel
///
/// # Receiver creates the region:
/// with Channel(description="my-app/control",
///              slot_count=256,
///              slot_size_bytes=4096,
///              role="receiver") as chan:
///     msg = chan.recv()
///
/// # Sender (in another process) attaches:
/// with Channel(description="my-app/control",
///              slot_count=256,
///              slot_size_bytes=4096,
///              role="sender") as chan:
///     chan.send(b"hello channel")
/// ```
#[pyclass(name = "Channel", module = "tessera_channel", unsendable)]
struct PyChannel {
    inner: Mutex<Option<RustChannel>>,
    role: ChannelRole,
    slot_count: u32,
    slot_size_bytes: u32,
}

impl PyChannel {
    fn with_inner<R>(
        &self,
        op: impl FnOnce(&RustChannel) -> Result<R, RustChannelError>,
    ) -> PyResult<R> {
        let guard = self.inner.lock();
        let chan = guard
            .as_ref()
            .ok_or_else(|| TesseraChannelError::new_err("Channel is closed"))?;
        op(chan).map_err(map_err)
    }
}

#[pymethods]
impl PyChannel {
    /// Construct a Channel.
    ///
    /// Required kwargs:
    ///   - `description`: human-readable namespace (BLAKE3-derived).
    ///   - `slot_count`: number of slots in the ring.
    ///   - `slot_size_bytes`: per-slot payload capacity (must be a
    ///     multiple of 8 for AtomicU64 alignment).
    ///   - `role`: `"receiver"` (creates the region) or `"sender"`
    ///     (attaches). MPSC: exactly one Receiver per region.
    ///
    /// Optional kwargs (with defaults):
    ///   - `force_recreate` (default `False`): Receiver-only
    ///     recovery escape hatch. Misuse will clobber a live
    ///     Receiver; only set this during explicit recovery.
    #[new]
    #[pyo3(signature = (*, description, slot_count, slot_size_bytes, role, force_recreate=false))]
    fn new(
        description: String,
        slot_count: u32,
        slot_size_bytes: u32,
        role: String,
        force_recreate: bool,
    ) -> PyResult<Self> {
        let role = parse_role(&role)?;
        let config = ChannelConfig {
            description,
            slot_count,
            slot_size_bytes,
            role,
            force_recreate,
        };
        let chan = RustChannel::open(config).map_err(map_err)?;
        Ok(Self {
            role,
            slot_count,
            slot_size_bytes,
            inner: Mutex::new(Some(chan)),
        })
    }

    /// True if this Channel opened as the SHM region creator (Receiver).
    #[getter]
    fn is_owner(&self) -> bool {
        matches!(self.role, ChannelRole::Receiver)
    }

    /// Role this Channel was opened with: `"receiver"` or `"sender"`.
    #[getter]
    fn role(&self) -> &'static str {
        match self.role {
            ChannelRole::Receiver => "receiver",
            ChannelRole::Sender => "sender",
        }
    }

    /// Configured slot count.
    #[getter]
    fn slot_count(&self) -> u32 {
        self.slot_count
    }

    /// Configured slot size (bytes).
    #[getter]
    fn slot_size_bytes(&self) -> u32 {
        self.slot_size_bytes
    }

    /// True if the Channel has been closed (close() called or
    /// __exit__ already fired).
    #[getter]
    fn is_closed(&self) -> bool {
        self.inner.lock().is_none()
    }

    /// Snapshot of `(head, tail)` positions. Useful for diagnostics.
    fn positions(&self) -> PyResult<(u64, u64)> {
        self.with_inner(|c| Ok(c.positions()))
    }

    /// Publish one message. Blocks until room is available.
    /// Receiver-side handles raise `TesseraChannelError`.
    fn send(&self, bytes: &Bound<'_, PyBytes>) -> PyResult<()> {
        let buf = bytes.as_bytes();
        self.with_inner(|c| c.send(buf))
    }

    /// Non-blocking publish. Raises `TesseraChannelError` with
    /// `"Channel is full"` if the queue is full at call time.
    fn try_send(&self, bytes: &Bound<'_, PyBytes>) -> PyResult<()> {
        let buf = bytes.as_bytes();
        self.with_inner(|c| c.try_send(buf))
    }

    /// Bounded-blocking publish. Raises `TesseraChannelError` with
    /// `"timed out"` on budget exhaustion.
    fn send_timeout(&self, bytes: &Bound<'_, PyBytes>, timeout_seconds: f64) -> PyResult<()> {
        validate_seconds("timeout_seconds", timeout_seconds, /*allow_zero=*/ true)?;
        // SAFETY (numeric): validate_seconds confirmed value is
        // finite + within Duration's safe range.
        let timeout = Duration::from_secs_f64(timeout_seconds);
        let buf = bytes.as_bytes();
        self.with_inner(|c| c.send_timeout(buf, timeout))
    }

    /// Dequeue one message. Blocks until a message is available.
    fn recv<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        let bytes = self.with_inner(|c| c.recv())?;
        Ok(PyBytes::new_bound(py, &bytes))
    }

    /// Non-blocking dequeue. Raises `TesseraChannelError` with
    /// `"Channel is empty"` if no message is available.
    fn try_recv<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        let bytes = self.with_inner(|c| c.try_recv())?;
        Ok(PyBytes::new_bound(py, &bytes))
    }

    /// Bounded-blocking dequeue. Raises `TesseraChannelError` with
    /// `"timed out"` on budget exhaustion.
    fn recv_timeout<'py>(
        &self,
        py: Python<'py>,
        timeout_seconds: f64,
    ) -> PyResult<Bound<'py, PyBytes>> {
        validate_seconds("timeout_seconds", timeout_seconds, /*allow_zero=*/ true)?;
        let timeout = Duration::from_secs_f64(timeout_seconds);
        let bytes = self.with_inner(|c| c.recv_timeout(timeout))?;
        Ok(PyBytes::new_bound(py, &bytes))
    }

    /// Drop the underlying Rust Channel, unlinking the SHM region
    /// (for Receiver-role Channels) and detaching the mapping (for
    /// Sender-role). Idempotent.
    fn close(&self) -> PyResult<()> {
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
            "Channel(role={:?}, slot_count={}, slot_size_bytes={}, closed={})",
            match self.role {
                ChannelRole::Receiver => "receiver",
                ChannelRole::Sender => "sender",
            },
            self.slot_count,
            self.slot_size_bytes,
            self.is_closed()
        )
    }
}

#[pymodule]
fn _native(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyChannel>()?;
    m.add(
        "TesseraChannelError",
        py.get_type_bound::<TesseraChannelError>(),
    )?;
    Ok(())
}
