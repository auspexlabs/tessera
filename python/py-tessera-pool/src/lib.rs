//! PyO3 bindings for tessera-pool.
//!
//! v0.0.1 SCAFFOLD ONLY. Bindings land alongside the core implementation
//! in Stage 4a.

use pyo3::prelude::*;

#[pymodule]
fn _native(_py: Python<'_>, _m: &Bound<'_, PyModule>) -> PyResult<()> {
    Ok(())
}
