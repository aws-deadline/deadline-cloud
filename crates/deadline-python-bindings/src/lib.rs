//! PyO3 extension module exposing Deadline Cloud business logic to Python.
//!
//! Replaces the C ABI layer (`deadline-gui-ffi` + `_ffi.py`).
//! Python imports this as `deadline._native`.

// PyO3's #[pyfunction] macro requires `pub` visibility, but this crate is a
// cdylib (shared library loaded by Python) — no Rust code links to it, so
// every `pub fn` appears unreachable from Rust's perspective.
#![allow(unreachable_pub, reason = "PyO3 #[pyfunction] requires pub visibility")]

use pyo3::prelude::*;

pyo3::create_exception!(deadline._native, DeadlineOperationError, pyo3::exceptions::PyException);

mod auth;
mod config;
mod resources;
mod submission;
mod telemetry;

/// Create a tokio runtime for async operations.
fn make_runtime() -> PyResult<tokio::runtime::Runtime> {
    tokio::runtime::Runtime::new()
        .map_err(|e| DeadlineOperationError::new_err(format!("Failed to create runtime: {e}")))
}

/// Load config from a path or the default location.
fn load_config(config_path: Option<&str>) -> PyResult<deadline_config::ini::IniConfig> {
    match config_path {
        Some(p) => deadline_config::config_file::read_config_from(std::path::Path::new(p)),
        None => deadline_config::config_file::read_config(),
    }
    .map_err(|e| DeadlineOperationError::new_err(e.to_string()))
}

#[pymodule]
#[pyo3(name = "_native")]
fn deadline_native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Config
    m.add_function(wrap_pyfunction!(config::get_setting, m)?)?;
    m.add_function(wrap_pyfunction!(config::set_setting, m)?)?;
    m.add_function(wrap_pyfunction!(config::read_config, m)?)?;

    // Auth
    m.add_function(wrap_pyfunction!(auth::get_credentials_source, m)?)?;
    m.add_function(wrap_pyfunction!(auth::check_auth_status, m)?)?;
    m.add_function(wrap_pyfunction!(auth::check_auth_status_with_progress, m)?)?;
    m.add_function(wrap_pyfunction!(auth::check_api_available, m)?)?;
    m.add_function(wrap_pyfunction!(auth::login, m)?)?;
    m.add_function(wrap_pyfunction!(auth::logout, m)?)?;

    // Resources
    m.add_function(wrap_pyfunction!(resources::list_farms, m)?)?;
    m.add_function(wrap_pyfunction!(resources::get_farm, m)?)?;
    m.add_function(wrap_pyfunction!(resources::list_queues, m)?)?;
    m.add_function(wrap_pyfunction!(resources::get_queue, m)?)?;
    m.add_function(wrap_pyfunction!(resources::list_storage_profiles_for_queue, m)?)?;
    m.add_function(wrap_pyfunction!(resources::get_queue_parameter_definitions, m)?)?;

    // Submission
    m.add_function(wrap_pyfunction!(submission::create_job_from_job_bundle, m)?)?;

    // Telemetry
    m.add_class::<telemetry::TelemetryClient>()?;

    // Exception type
    m.add("DeadlineOperationError", m.py().get_type::<DeadlineOperationError>())?;

    Ok(())
}
