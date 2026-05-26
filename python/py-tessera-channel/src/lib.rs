//! PyO3 bindings for tessera-channel.
//!
//! v0.0.1 SCAFFOLD ONLY. Bindings land alongside the core
//! implementation in Stage 4c.

use pyo3::prelude::*;

#[pymodule]
fn _native(_py: Python<'_>, _m: &Bound<'_, PyModule>) -> PyResult<()> {
    Ok(())
}
