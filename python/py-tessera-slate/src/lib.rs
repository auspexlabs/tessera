//! Python facade for tessera-slate.
//!
//! Thin wrapper around `tessera_slate::Slate` exposed via PyO3:
//! - `tessera_slate.Slate` — context-manager-friendly writer/owner class.
//! - `tessera_slate.SlateReader` — read-only handle; also from `Slate.reader()`.
//! - `tessera_slate.Header` — frozen result of `SlateReader.header()`.
//! - `tessera_slate.SlotRead` — frozen result of `SlateReader.read_slot()`.
//! - `tessera_slate.TesseraSlateError` — base Python exception.
//!
//! The facade owns ergonomics only — every data operation delegates
//! to the Rust core. No serialization happens in Python.
//!
//! Slate read/write are lock-free (bounded seqlock retry) and never
//! block, so — unlike the Channel facade — they are not wrapped in
//! `py.allow_threads`.

// pyo3 0.22's `#[pymethods]`/`#[pyfunction]` expansion injects an
// identity `PyErr: From<PyErr>` conversion that clippy reports as
// `useless_conversion` against our return-type spans. There is no
// literal `.into()` in this file to remove — the conversion is
// macro-generated. Suppress the false positive crate-wide.
#![allow(clippy::useless_conversion)]

use parking_lot::Mutex;
use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use tessera_slate::{
    HeaderSnapshot, ReadResult, Slate as RustSlate, SlateConfig, SlateReader as RustSlateReader,
    TesseraSlateError as RustSlateError,
};

// One base exception class for the whole crate. Specific failure
// modes (OversizedPayload, SlotIndexOutOfRange, GeometryMismatch,
// SchemaHashMismatch, Region, ...) surface as the message; programmatic
// catch on type works at the base level.
create_exception!(_native, TesseraSlateError, PyException);

fn map_err(e: RustSlateError) -> PyErr {
    TesseraSlateError::new_err(e.to_string())
}

// ---------------------------------------------------------------------
// Header (frozen) and SlotRead (frozen)
// ---------------------------------------------------------------------

/// Region-global header counters. Returned by `SlateReader.header()`.
/// Frozen.
#[pyclass(name = "Header", module = "tessera_slate", frozen)]
#[derive(Clone, Copy)]
struct PyHeader {
    writer_seq: u64,
    last_update_ns: u64,
}

impl PyHeader {
    fn from_rust(h: HeaderSnapshot) -> Self {
        Self {
            writer_seq: h.writer_seq,
            last_update_ns: h.last_update_ns,
        }
    }
}

#[pymethods]
impl PyHeader {
    #[getter]
    fn writer_seq(&self) -> u64 {
        self.writer_seq
    }

    #[getter]
    fn last_update_ns(&self) -> u64 {
        self.last_update_ns
    }

    fn __repr__(&self) -> String {
        format!(
            "Header(writer_seq={}, last_update_ns={})",
            self.writer_seq, self.last_update_ns
        )
    }
}

/// One snapshot read from a slot. Returned by `SlateReader.read_slot()`.
/// Frozen; pickle-compatible.
///
/// `state` is one of `"slot"` (a coherent value — `value` holds the
/// payload bytes), `"empty"` (the slot has never been written), or
/// `"torn"` (every retry collided with a concurrent write; keep the
/// previous value and poll again). For `"empty"` / `"torn"`, `value` is
/// `None` and `sequence` / `timestamp_nanos` are `0`.
#[pyclass(name = "SlotRead", module = "tessera_slate", frozen)]
#[derive(Clone)]
struct PySlotRead {
    state: String,
    value: Option<Vec<u8>>,
    sequence: u64,
    timestamp_nanos: u64,
}

impl PySlotRead {
    fn from_rust(r: ReadResult) -> Self {
        match r {
            ReadResult::Empty => Self {
                state: "empty".to_string(),
                value: None,
                sequence: 0,
                timestamp_nanos: 0,
            },
            ReadResult::Torn => Self {
                state: "torn".to_string(),
                value: None,
                sequence: 0,
                timestamp_nanos: 0,
            },
            ReadResult::Slot {
                bytes,
                sequence,
                timestamp_nanos,
            } => Self {
                state: "slot".to_string(),
                value: Some(bytes),
                sequence,
                timestamp_nanos,
            },
        }
    }
}

#[pymethods]
impl PySlotRead {
    /// One of `"slot"`, `"empty"`, `"torn"`.
    #[getter]
    fn state(&self) -> &str {
        &self.state
    }

    /// Payload bytes for a `"slot"` read; `None` for `"empty"` / `"torn"`.
    #[getter]
    fn value<'py>(&self, py: Python<'py>) -> Option<Bound<'py, PyBytes>> {
        self.value.as_ref().map(|v| PyBytes::new_bound(py, v))
    }

    #[getter]
    fn sequence(&self) -> u64 {
        self.sequence
    }

    #[getter]
    fn timestamp_nanos(&self) -> u64 {
        self.timestamp_nanos
    }

    /// True if `state == "slot"`.
    #[getter]
    fn is_slot(&self) -> bool {
        self.state == "slot"
    }

    /// True if `state == "empty"`.
    #[getter]
    fn is_empty(&self) -> bool {
        self.state == "empty"
    }

    /// True if `state == "torn"`.
    #[getter]
    fn is_torn(&self) -> bool {
        self.state == "torn"
    }

    fn __repr__(&self) -> String {
        let value_len = match &self.value {
            Some(v) => v.len() as i64,
            None => -1,
        };
        format!(
            "SlotRead(state={:?}, value_len={}, sequence={}, timestamp_nanos={})",
            self.state, value_len, self.sequence, self.timestamp_nanos
        )
    }

    /// Picklable via `(_slot_read_from_parts, (state, value, sequence, ts_ns))`.
    // The tuple is the pickle `__reduce__` contract: (callable, args).
    #[allow(clippy::type_complexity)]
    fn __reduce__<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<(Bound<'py, PyAny>, (String, Option<Bound<'py, PyBytes>>, u64, u64))> {
        let factory = py
            .import_bound("tessera_slate")?
            .getattr("_slot_read_from_parts")?;
        let value = self.value.as_ref().map(|v| PyBytes::new_bound(py, v));
        Ok((
            factory,
            (self.state.clone(), value, self.sequence, self.timestamp_nanos),
        ))
    }
}

#[pyfunction]
// `value` is `Option<&[u8]>` (None for empty/torn reads) and is followed
// by required params, so pyo3 needs an explicit signature to know it's a
// positional arg, not a defaulted one. All four come straight from
// `__reduce__`; none is optional at the call site.
#[pyo3(signature = (state, value, sequence, timestamp_nanos))]
fn _slot_read_from_parts(
    state: String,
    value: Option<&[u8]>,
    sequence: u64,
    timestamp_nanos: u64,
) -> PySlotRead {
    PySlotRead {
        state,
        value: value.map(|v| v.to_vec()),
        sequence,
        timestamp_nanos,
    }
}

// ---------------------------------------------------------------------
// SlateReader (shares the mapping via Arc<Region> inside RustSlateReader)
// ---------------------------------------------------------------------

/// Read-only handle over a Slate region (the polling / display side).
///
/// Construct directly to attach to an existing region, or obtain one
/// from `Slate.reader()` (which shares the writer's mapping). Reads are
/// lock-free and torn-read-tolerant; `read_slot` converges to the latest
/// coherent bytes.
#[pyclass(name = "SlateReader", module = "tessera_slate")]
struct PySlateReader {
    // Option<SlateReader> so close() / __exit__ can deterministically
    // drop the inner reader; after close, operations raise
    // TesseraSlateError.
    inner: Mutex<Option<RustSlateReader>>,
    slot_count: u32,
    slot_size_bytes: u32,
}

impl PySlateReader {
    fn with_inner<R>(&self, op: impl FnOnce(&RustSlateReader) -> PyResult<R>) -> PyResult<R> {
        let guard = self.inner.lock();
        let r = guard
            .as_ref()
            .ok_or_else(|| TesseraSlateError::new_err("SlateReader is closed"))?;
        op(r)
    }
}

#[pymethods]
impl PySlateReader {
    /// Attach to an existing Slate region for reading.
    ///
    /// Required args:
    ///   - `description`: human-readable namespace (BLAKE3-derived).
    ///   - `slot_count`: number of slots in the table.
    ///   - `slot_size_bytes`: per-slot payload capacity in bytes.
    ///
    /// Optional args (with defaults):
    ///   - `schema_hash` (default `0`): caller-defined layout hash; must
    ///     match the creator's or the attach is rejected (drift guard).
    ///
    /// `slot_count`, `slot_size_bytes`, and `schema_hash` must all match
    /// the creator's config or the attach fails.
    #[new]
    #[pyo3(signature = (description, slot_count, slot_size_bytes, schema_hash=0))]
    fn new(
        description: &str,
        slot_count: u32,
        slot_size_bytes: u32,
        schema_hash: u64,
    ) -> PyResult<Self> {
        let reader =
            RustSlateReader::open(description, slot_count, slot_size_bytes, schema_hash)
                .map_err(map_err)?;
        Ok(Self {
            slot_count: reader.slot_count(),
            slot_size_bytes: reader.slot_size_bytes(),
            inner: Mutex::new(Some(reader)),
        })
    }

    /// Configured slot count.
    #[getter]
    fn slot_count(&self) -> u32 {
        self.slot_count
    }

    /// Configured per-slot payload capacity in bytes.
    #[getter]
    fn slot_size_bytes(&self) -> u32 {
        self.slot_size_bytes
    }

    /// True if the reader has been closed.
    #[getter]
    fn is_closed(&self) -> bool {
        self.inner.lock().is_none()
    }

    /// Region-global counters: total writes so far and the last write
    /// time (nanoseconds).
    fn header(&self) -> PyResult<PyHeader> {
        self.with_inner(|r| Ok(PyHeader::from_rust(r.header())))
    }

    /// Read the latest coherent snapshot of slot `index`.
    ///
    /// Returns a `SlotRead` whose `state` is `"slot"` (with `value`
    /// bytes), `"empty"` (never written), or `"torn"` (collided with a
    /// concurrent write every retry — keep the previous value).
    fn read_slot(&self, index: u32) -> PyResult<PySlotRead> {
        self.with_inner(|r| {
            let result = r.read_slot(index).map_err(map_err)?;
            Ok(PySlotRead::from_rust(result))
        })
    }

    /// Close this reader and drop its mapping reference. Idempotent.
    /// After `close()`, `header()` / `read_slot(...)` raise
    /// `TesseraSlateError("SlateReader is closed")`.
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
            "SlateReader(slot_count={}, slot_size_bytes={}, closed={})",
            self.slot_count,
            self.slot_size_bytes,
            self.is_closed()
        )
    }
}

// ---------------------------------------------------------------------
// Slate (writer/owner)
// ---------------------------------------------------------------------

/// Tessera Slate — seqlock-protected latest-value snapshot slot table.
///
/// Construct with positional / keyword arguments; use as a context
/// manager.
///
/// ```python
/// from tessera_slate import Slate
///
/// with Slate(description="my-app/snapshots",
///            slot_count=8,
///            slot_size_bytes=64) as slate:
///     slate.write_slot(2, b"hi")
///     reader = slate.reader()
///     read = reader.read_slot(2)
///     if read.is_slot:
///         print(read.value)
/// ```
///
/// One writer per slot is the protocol; distinct slots may be written
/// from distinct threads / processes concurrently. Readers are lock-free
/// and converge to the latest written bytes (no history).
#[pyclass(name = "Slate", module = "tessera_slate")]
struct PySlate {
    // Option<Slate> so close() / __exit__ can deterministically drop the
    // inner Slate; after close, operations raise TesseraSlateError. The
    // inner Slate is itself Arc<Region>-backed, so reader() hands its
    // child a region clone that outlives this handle's close().
    inner: Mutex<Option<RustSlate>>,
    is_owner: bool,
    slot_count: u32,
    slot_size_bytes: u32,
}

impl PySlate {
    fn with_inner<R>(&self, op: impl FnOnce(&RustSlate) -> PyResult<R>) -> PyResult<R> {
        let guard = self.inner.lock();
        let s = guard
            .as_ref()
            .ok_or_else(|| TesseraSlateError::new_err("Slate is closed"))?;
        op(s)
    }
}

#[pymethods]
impl PySlate {
    /// Construct a Slate writer / owner.
    ///
    /// Required args:
    ///   - `description`: human-readable namespace (BLAKE3-derived).
    ///   - `slot_count`: number of slots in the table.
    ///   - `slot_size_bytes`: per-slot payload capacity (must be a
    ///     multiple of 8).
    ///
    /// Optional args (with defaults):
    ///   - `schema_hash` (default `0`): caller-defined layout hash;
    ///     attachers and readers must supply the same value. Use `0` for
    ///     "no schema".
    ///   - `is_owner` (default `True`): create the SHM region (owner) vs
    ///     attach to an existing one (`False`, e.g. a worker writing its
    ///     own slots).
    ///   - `force_recreate` (default `False`): owner-side recovery escape
    ///     hatch. Misuse will clobber a live peer; only set this during
    ///     explicit recovery. Ignored on attach.
    #[new]
    #[pyo3(signature = (description, slot_count, slot_size_bytes, schema_hash=0, is_owner=true, force_recreate=false))]
    fn new(
        description: String,
        slot_count: u32,
        slot_size_bytes: u32,
        schema_hash: u64,
        is_owner: bool,
        force_recreate: bool,
    ) -> PyResult<Self> {
        let config = SlateConfig {
            description,
            slot_count,
            slot_size_bytes,
            schema_hash,
            is_owner,
            force_recreate,
        };
        let slate = RustSlate::open(config).map_err(map_err)?;
        Ok(Self {
            is_owner: slate.is_owner(),
            slot_count: slate.slot_count(),
            slot_size_bytes: slate.slot_size_bytes(),
            inner: Mutex::new(Some(slate)),
        })
    }

    /// True if this Slate opened as the SHM region creator.
    #[getter]
    fn is_owner(&self) -> bool {
        self.is_owner
    }

    /// Configured slot count.
    #[getter]
    fn slot_count(&self) -> u32 {
        self.slot_count
    }

    /// Configured per-slot payload capacity in bytes.
    #[getter]
    fn slot_size_bytes(&self) -> u32 {
        self.slot_size_bytes
    }

    /// True if the Slate has been closed.
    #[getter]
    fn is_closed(&self) -> bool {
        self.inner.lock().is_none()
    }

    /// Overwrite slot `index` with `data` (≤ `slot_size_bytes`).
    ///
    /// One writer per slot: distinct slots may be written concurrently,
    /// but two writers on the *same* slot is a protocol violation.
    fn write_slot(&self, index: u32, data: &Bound<'_, PyBytes>) -> PyResult<()> {
        self.with_inner(|s| s.write_slot(index, data.as_bytes()).map_err(map_err))
    }

    /// Issue a reader sharing this Slate's mapping. The reader keeps its
    /// own clone of the underlying region, so it stays usable after this
    /// Slate is closed.
    fn reader(&self) -> PyResult<PySlateReader> {
        self.with_inner(|s| {
            let rd = s.reader();
            Ok(PySlateReader {
                slot_count: rd.slot_count(),
                slot_size_bytes: rd.slot_size_bytes(),
                inner: Mutex::new(Some(rd)),
            })
        })
    }

    /// Unlink the SHM name (owner only).
    ///
    /// Requires this to be the sole live handle to the region: no other
    /// `Slate` / `SlateReader` clones (including any issued by
    /// `reader()`) may be outstanding, or this raises
    /// `TesseraSlateError`. Dropping the owning handle also unlinks, so
    /// explicit unlink is only needed for deterministic early cleanup.
    fn unlink(&self) -> PyResult<()> {
        let mut guard = self.inner.lock();
        let slate = guard
            .as_mut()
            .ok_or_else(|| TesseraSlateError::new_err("Slate is closed"))?;
        // Slate::unlink itself requires the region's Arc to be uniquely
        // held; if a reader() child (or another clone) still holds the
        // region, it returns a Region error which we surface verbatim.
        slate.unlink().map_err(map_err)
    }

    /// Close this Slate and drop its mapping reference. Idempotent.
    /// After `close()`, `write_slot(...)` / `reader()` / `unlink()` raise
    /// `TesseraSlateError("Slate is closed")`. Readers previously issued
    /// by `reader()` keep their own region clones and stay usable.
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
            "Slate(is_owner={}, slot_count={}, slot_size_bytes={}, closed={})",
            self.is_owner,
            self.slot_count,
            self.slot_size_bytes,
            self.is_closed()
        )
    }
}

#[pymodule]
fn _native(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PySlate>()?;
    m.add_class::<PySlateReader>()?;
    m.add_class::<PyHeader>()?;
    m.add_class::<PySlotRead>()?;
    m.add_function(wrap_pyfunction!(_slot_read_from_parts, m)?)?;
    m.add("TesseraSlateError", py.get_type_bound::<TesseraSlateError>())?;
    Ok(())
}
