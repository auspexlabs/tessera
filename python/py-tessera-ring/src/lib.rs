//! PyO3 bindings for tessera-ring.
//!
//! v0.0.1 SCAFFOLD ONLY. Bindings land alongside the core implementation
//! in Stage 4b.

use pyo3::prelude::*;

#[pymodule]
fn _native(_py: Python<'_>, _m: &Bound<'_, PyModule>) -> PyResult<()> {
    Ok(())
}
