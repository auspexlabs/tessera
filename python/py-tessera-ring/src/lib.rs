//! Python facade for tessera-ring.
//!
//! Thin wrapper around `tessera_ring::Ring` exposed via PyO3:
//! - `tessera_ring.Ring` — context-manager-friendly Ring class.
//! - `tessera_ring.Writer` — handle returned by `Ring.writer()`.
//! - `tessera_ring.Reader` — handle returned by `Ring.reader(section_id)`.
//! - `tessera_ring.Event` — frozen result of `Reader.poll()`.
//! - `tessera_ring.ReaderStats` — frozen result of `Reader.stats()`.
//! - `tessera_ring.TesseraRingError` — base Python exception.
//!
//! The facade owns ergonomics only — every data operation delegates
//! to the Rust core. No serialization happens in Python.

// pyo3 0.22's `#[pymethods]`/`#[pyfunction]` expansion injects an
// identity `PyErr: From<PyErr>` conversion that clippy reports as
// `useless_conversion` against our return-type spans. There is no
// literal `.into()` in this file to remove — the conversion is
// macro-generated. Suppress the false positive crate-wide.
#![allow(clippy::useless_conversion)]

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;
use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList, PyTuple};

use tessera_ring::{
    Event as RustEvent, Reader as RustReader, ReaderStats as RustReaderStats, Ring as RustRing,
    RingConfig, SectionConfig, TesseraRingError as RustRingError, Writer as RustWriter,
};

// One base exception class for the whole crate. Specific failure
// modes surface as the message; programmatic catch on type works at
// the base level.
create_exception!(_native, TesseraRingError, PyException);

fn map_err(e: RustRingError) -> PyErr {
    TesseraRingError::new_err(e.to_string())
}

// ---------------------------------------------------------------------
// Event (frozen) and ReaderStats (frozen)
// ---------------------------------------------------------------------

/// One event drained from the ring. Returned in the list from
/// `Reader.poll()`. Frozen; pickle-compatible.
#[pyclass(name = "Event", module = "tessera_ring", frozen)]
#[derive(Clone)]
struct PyEvent {
    section_id: u32,
    position: u64,
    timestamp_nanos: u64,
    payload: Vec<u8>,
}

impl PyEvent {
    fn from_rust(e: RustEvent) -> Self {
        Self {
            section_id: e.section_id,
            position: e.position,
            timestamp_nanos: e.timestamp_nanos,
            payload: e.payload,
        }
    }
}

#[pymethods]
impl PyEvent {
    #[getter]
    fn section_id(&self) -> u32 {
        self.section_id
    }

    #[getter]
    fn position(&self) -> u64 {
        self.position
    }

    #[getter]
    fn timestamp_nanos(&self) -> u64 {
        self.timestamp_nanos
    }

    /// Event payload bytes.
    #[getter]
    fn payload<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new_bound(py, &self.payload)
    }

    fn __repr__(&self) -> String {
        format!(
            "Event(section_id={}, position={}, timestamp_nanos={}, payload_len={})",
            self.section_id,
            self.position,
            self.timestamp_nanos,
            self.payload.len()
        )
    }

    /// Picklable via `(_event_from_parts, (section_id, position, ts_ns, payload))`.
    // The tuple is the pickle `__reduce__` contract: (callable, args).
    #[allow(clippy::type_complexity)]
    fn __reduce__<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<(Bound<'py, PyAny>, (u32, u64, u64, Bound<'py, PyBytes>))> {
        let factory = py
            .import_bound("tessera_ring")?
            .getattr("_event_from_parts")?;
        let payload = PyBytes::new_bound(py, &self.payload);
        Ok((
            factory,
            (self.section_id, self.position, self.timestamp_nanos, payload),
        ))
    }
}

#[pyfunction]
fn _event_from_parts(
    section_id: u32,
    position: u64,
    timestamp_nanos: u64,
    payload: &[u8],
) -> PyEvent {
    PyEvent {
        section_id,
        position,
        timestamp_nanos,
        payload: payload.to_vec(),
    }
}

/// Per-section reader statistics. Frozen.
#[pyclass(name = "ReaderStats", module = "tessera_ring", frozen)]
#[derive(Clone, Copy)]
struct PyReaderStats {
    section_id: u32,
    cursor: u64,
    latest: u64,
    dropped: u64,
}

impl PyReaderStats {
    fn from_rust(s: RustReaderStats) -> Self {
        Self {
            section_id: s.section_id,
            cursor: s.cursor,
            latest: s.latest,
            dropped: s.dropped,
        }
    }
}

#[pymethods]
impl PyReaderStats {
    #[getter]
    fn section_id(&self) -> u32 {
        self.section_id
    }

    #[getter]
    fn cursor(&self) -> u64 {
        self.cursor
    }

    #[getter]
    fn latest(&self) -> u64 {
        self.latest
    }

    #[getter]
    fn dropped(&self) -> u64 {
        self.dropped
    }

    fn __repr__(&self) -> String {
        format!(
            "ReaderStats(section_id={}, cursor={}, latest={}, dropped={})",
            self.section_id, self.cursor, self.latest, self.dropped
        )
    }
}

// ---------------------------------------------------------------------
// Writer
// ---------------------------------------------------------------------

/// Writer handle. Issued by `Ring.writer()`; use `publish(section_id, bytes)`.
///
/// Holds a clone of the parent Ring's `closed` flag (shared via Arc).
/// After `Ring.close()` flips the flag, subsequent `publish` calls
/// raise `TesseraRingError("Ring is closed")` — Codex P2 fix on PR #2
/// (`d467b14`).
#[pyclass(name = "Writer", module = "tessera_ring")]
struct PyWriter {
    inner: RustWriter,
    closed: Arc<AtomicBool>,
}

#[pymethods]
impl PyWriter {
    /// Publish one event to the named section.
    fn publish(&self, section_id: u32, bytes: &Bound<'_, PyBytes>) -> PyResult<()> {
        if self.closed.load(Ordering::Acquire) {
            return Err(TesseraRingError::new_err("Ring is closed"));
        }
        let buf = bytes.as_bytes();
        self.inner.publish(section_id, buf).map_err(map_err)
    }

    fn __repr__(&self) -> String {
        let state = if self.closed.load(Ordering::Acquire) {
            "closed"
        } else {
            "open"
        };
        format!("Writer({state})")
    }
}

// ---------------------------------------------------------------------
// Reader (mutable cursor → Mutex interior)
// ---------------------------------------------------------------------

/// Per-section reader handle. Issued by `Ring.reader(section_id)`.
/// Each Reader maintains its own cursor; multiple Readers on the
/// same section are independent (multi-reader broadcast).
///
/// Holds a clone of the parent Ring's `closed` flag (shared via Arc).
/// After `Ring.close()` flips the flag, subsequent `poll` / `stats`
/// calls raise `TesseraRingError("Ring is closed")` — Codex P2 fix on
/// PR #2 (`d467b14`). The Rust-side `RustReader` keeps its own
/// `Arc<Region>` clone, so the underlying SHM mapping stays alive
/// until this PyReader is also dropped; we just block API access at
/// the facade.
#[pyclass(name = "Reader", module = "tessera_ring")]
struct PyReader {
    inner: Mutex<RustReader>,
    section_id: u32,
    closed: Arc<AtomicBool>,
}

impl PyReader {
    fn check_open(&self) -> PyResult<()> {
        if self.closed.load(Ordering::Acquire) {
            Err(TesseraRingError::new_err("Ring is closed"))
        } else {
            Ok(())
        }
    }
}

#[pymethods]
impl PyReader {
    /// Drain all events between the reader's cursor and the current
    /// writer position. Returns a list of `Event`s.
    fn poll<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        self.check_open()?;
        let events = self.inner.lock().poll().map_err(map_err)?;
        let py_events: Vec<Py<PyEvent>> = events
            .into_iter()
            .map(|e| Py::new(py, PyEvent::from_rust(e)))
            .collect::<PyResult<_>>()?;
        Ok(PyList::new_bound(py, py_events))
    }

    /// Snapshot reader stats: section_id, cursor, latest writer
    /// position, total dropped count.
    fn stats(&self) -> PyResult<PyReaderStats> {
        self.check_open()?;
        self.inner
            .lock()
            .stats()
            .map(PyReaderStats::from_rust)
            .map_err(map_err)
    }

    #[getter]
    fn section_id(&self) -> u32 {
        self.section_id
    }

    #[getter]
    fn cursor(&self) -> u64 {
        // Cursor / dropped are process-local; readable even after
        // close() so consumers can inspect their final state.
        self.inner.lock().cursor()
    }

    #[getter]
    fn dropped(&self) -> u64 {
        self.inner.lock().dropped()
    }

    fn __repr__(&self) -> String {
        let g = self.inner.lock();
        let state = if self.closed.load(Ordering::Acquire) {
            "closed"
        } else {
            "open"
        };
        format!(
            "Reader(section_id={}, cursor={}, dropped={}, ring_state={state})",
            self.section_id,
            g.cursor(),
            g.dropped()
        )
    }
}

// ---------------------------------------------------------------------
// Ring
// ---------------------------------------------------------------------

/// Tessera Ring — lossy mmap-backed multi-writer / multi-reader ring buffer.
///
/// Construct with keyword arguments; use as a context manager.
///
/// ```python
/// from tessera_ring import Ring
///
/// with Ring(description="my-app/telemetry",
///           sections=[(0, 4096, 2048)],
///           is_owner=True) as ring:
///     writer = ring.writer()
///     reader = ring.reader(0)
///     writer.publish(0, b"hello")
///     for event in reader.poll():
///         ...
/// ```
///
/// `sections` is a list of `(section_id, slot_count, slot_size_bytes)`
/// 3-tuples. The library does not classify event bytes — sections are
/// caller-named logical streams inside one Ring region.
#[pyclass(name = "Ring", module = "tessera_ring")]
struct PyRing {
    // Option<Ring> so close() / __exit__ can deterministically drop
    // the inner Ring; after close, operations raise TesseraRingError.
    inner: Mutex<Option<RustRing>>,
    is_owner: bool,
    // Shared "closed" flag — cloned into every PyWriter / PyReader
    // issued by this Ring. close() flips the flag so all child
    // handles also start raising "Ring is closed" on their
    // operations. Codex P2 fix on PR #2 (`d467b14`): previously a
    // close on PyRing only dropped THIS handle's RustRing reference,
    // leaving previously-issued PyWriter / PyReader objects fully
    // functional (because Tessera's Rust Writer/Reader hold their
    // own Arc<Region> clones). The underlying SHM mapping still
    // stays alive until all Arc clones drop, but at least the API
    // surface now matches Python users' "close means done" mental
    // model.
    closed: Arc<AtomicBool>,
}

impl PyRing {
    fn with_inner<R>(&self, op: impl FnOnce(&RustRing) -> PyResult<R>) -> PyResult<R> {
        let guard = self.inner.lock();
        let r = guard
            .as_ref()
            .ok_or_else(|| TesseraRingError::new_err("Ring is closed"))?;
        op(r)
    }
}

#[pymethods]
impl PyRing {
    /// Construct a Ring.
    ///
    /// Required kwargs:
    ///   - `description`: human-readable namespace (BLAKE3-derived).
    ///   - `sections`: list of `(section_id, slot_count, slot_size_bytes)`
    ///     3-tuples.
    ///
    /// Optional kwargs (with defaults):
    ///   - `is_owner` (default `True`): create the SHM region (owner)
    ///     vs attach to an existing one (`False`).
    ///   - `force_recreate` (default `False`): owner-side recovery
    ///     escape hatch. Misuse will clobber a live peer; only set
    ///     this during explicit recovery. Ignored on attach.
    #[new]
    #[pyo3(signature = (*, description, sections, is_owner=true, force_recreate=false))]
    fn new(
        description: String,
        sections: &Bound<'_, PyList>,
        is_owner: bool,
        force_recreate: bool,
    ) -> PyResult<Self> {
        let section_list = parse_sections(sections)?;
        let config = RingConfig {
            description,
            sections: section_list,
            is_owner,
            force_recreate,
        };
        let ring = RustRing::open(config).map_err(map_err)?;
        Ok(Self {
            is_owner: ring.is_owner(),
            inner: Mutex::new(Some(ring)),
            closed: Arc::new(AtomicBool::new(false)),
        })
    }

    /// True if this Ring opened as the SHM region creator.
    #[getter]
    fn is_owner(&self) -> bool {
        self.is_owner
    }

    /// True if the Ring has been closed.
    #[getter]
    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// Issue a Writer handle. Multiple writers may coexist; each
    /// `publish` claims an independent global position via fetch_add.
    fn writer(&self) -> PyResult<PyWriter> {
        let closed = Arc::clone(&self.closed);
        self.with_inner(|r| {
            Ok(PyWriter {
                inner: r.writer(),
                closed,
            })
        })
    }

    /// Issue a Reader handle bound to one section. Fresh readers start
    /// at the current writer position (see new events only, not
    /// historical).
    fn reader(&self, section_id: u32) -> PyResult<PyReader> {
        let closed = Arc::clone(&self.closed);
        self.with_inner(|r| {
            let rd = r.reader(section_id).map_err(map_err)?;
            Ok(PyReader {
                inner: Mutex::new(rd),
                section_id,
                closed,
            })
        })
    }

    /// Close this Ring and invalidate all derived Writer / Reader
    /// handles issued from it.
    ///
    /// After `close()`:
    ///   * `Ring.writer()` and `Ring.reader(...)` raise
    ///     `TesseraRingError("Ring is closed")`.
    ///   * Any previously issued `Writer.publish(...)` and
    ///     `Reader.poll() / Reader.stats()` calls also raise
    ///     `TesseraRingError("Ring is closed")`.
    ///   * `Reader.cursor` / `Reader.dropped` getters remain readable
    ///     so consumers can inspect their final state.
    ///
    /// Note on the underlying SHM lifecycle (Codex P2 disclosure):
    /// Tessera's Rust `Writer` and `Reader` hold their own
    /// `Arc<Region>` clones; the SHM segment isn't actually unlinked
    /// until ALL such handles (this Ring AND every issued child) are
    /// dropped. `close()` blocks API access at the Python facade and
    /// drops this Ring's own reference; remaining unlink work happens
    /// when the last child is garbage-collected. For deterministic
    /// SHM unlink in tests, drop all references explicitly or use a
    /// `with` block that scopes both Ring and its children.
    ///
    /// Idempotent.
    fn close(&self) -> PyResult<()> {
        // Order matters: flip the flag FIRST so any concurrent call
        // through a child handle sees "closed" and refuses to issue
        // new operations against the about-to-be-dropped RustRing.
        self.closed.store(true, Ordering::Release);
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
        format!("Ring(is_owner={}, closed={})", self.is_owner, self.is_closed())
    }
}

/// Parse a Python list of `(section_id, slot_count, slot_size_bytes)`
/// 3-tuples into `Vec<SectionConfig>`. Each entry must be exactly 3
/// integers.
fn parse_sections(sections: &Bound<'_, PyList>) -> PyResult<Vec<SectionConfig>> {
    let mut out = Vec::with_capacity(sections.len());
    for (i, item) in sections.iter().enumerate() {
        let tup: Bound<'_, PyTuple> = item.extract().map_err(|_| {
            TesseraRingError::new_err(format!(
                "sections[{i}] must be a 3-tuple (section_id, slot_count, slot_size_bytes)"
            ))
        })?;
        if tup.len() != 3 {
            return Err(TesseraRingError::new_err(format!(
                "sections[{i}] must be a 3-tuple, got len={}",
                tup.len()
            )));
        }
        let section_id: u32 = tup.get_item(0)?.extract()?;
        let slot_count: u32 = tup.get_item(1)?.extract()?;
        let slot_size_bytes: u32 = tup.get_item(2)?.extract()?;
        out.push(SectionConfig::new(section_id, slot_count, slot_size_bytes));
    }
    Ok(out)
}

#[pymodule]
fn _native(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyRing>()?;
    m.add_class::<PyWriter>()?;
    m.add_class::<PyReader>()?;
    m.add_class::<PyEvent>()?;
    m.add_class::<PyReaderStats>()?;
    m.add_function(wrap_pyfunction!(_event_from_parts, m)?)?;
    m.add("TesseraRingError", py.get_type_bound::<TesseraRingError>())?;
    Ok(())
}
