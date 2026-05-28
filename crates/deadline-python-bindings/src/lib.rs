//! PyO3 extension module exposing Deadline Cloud business logic to Python.
//!
//! Replaces the C ABI layer (`deadline-gui-ffi` + `_ffi.py`).
//! Python imports this as `deadline._native`.

// PyO3's #[pyfunction] macro requires `pub` visibility, but this crate is a
// cdylib (shared library loaded by Python) — no Rust code links to it, so
// every `pub fn` appears unreachable from Rust's perspective.
#![allow(unreachable_pub, reason = "PyO3 #[pyfunction] requires pub visibility")]
#![allow(
    clippy::result_large_err,
    reason = "AWS SDK error types (SdkError<...>) are 464+ bytes; can't box without changing SDK API"
)]

use pyo3::prelude::*;

pyo3::create_exception!(
    deadline._native,
    DeadlineOperationError,
    pyo3::exceptions::PyException
);

mod auth;
mod config;
mod resources;
mod submission;
mod telemetry;

/// Create a tokio runtime for async operations.
fn make_runtime() -> PyResult<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| DeadlineOperationError::new_err(format!("Failed to create runtime: {e}")))
}

/// Run a closure on a scoped thread with the platform default stack size (8 MB
/// on macOS/Linux), returning its result.
///
/// `QThread` on macOS defaults to only 512 KB stack, which is insufficient for
/// the deep call chains in rustls/webpki certificate parsing. By running on a
/// scoped thread we get the OS default (8 MB) while still allowing the closure
/// to borrow from the caller's stack frame.
fn on_large_stack<F, T>(f: F) -> PyResult<T>
where
    F: FnOnce() -> T + Send,
    T: Send,
{
    std::thread::scope(|s| s.spawn(f).join())
        .map_err(|_| DeadlineOperationError::new_err("Worker thread panicked"))
}

/// Load config from a path or the default location.
fn load_config(config_path: Option<&str>) -> PyResult<deadline_lib::config::ini::IniConfig> {
    match config_path {
        Some(p) => deadline_lib::config::config_file::read_config_from(std::path::Path::new(p)),
        None => deadline_lib::config::config_file::read_config(),
    }
    .map_err(|e| DeadlineOperationError::new_err(e.to_string()))
}

/// Extract profile from config for session calls.
fn extract_profile(config: &deadline_lib::config::ini::IniConfig) -> Option<String> {
    deadline_lib::api::session::resolve_profile_name(config)
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
    m.add_function(wrap_pyfunction!(
        resources::list_storage_profiles_for_queue,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        resources::get_queue_parameter_definitions,
        m
    )?)?;

    // Submission
    m.add_function(wrap_pyfunction!(submission::create_job_from_job_bundle, m)?)?;

    // Telemetry
    m.add_class::<telemetry::TelemetryClient>()?;

    // Exception type
    m.add(
        "DeadlineOperationError",
        m.py().get_type::<DeadlineOperationError>(),
    )?;

    // Version (from Cargo.toml at compile time)
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;

    Ok(())
}
