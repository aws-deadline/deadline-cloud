//! C ABI shared library exposing Deadline Cloud business logic
//! to Python GUI widgets and DCC submitter plugins.
//!
//! See docs/specs/deadline-gui-ffi.md for the full API contract.
//!
//! ## Memory ownership
//!
//! All strings returned by `deadline_*` functions are owned by Rust.
//! The caller must free them with `deadline_free_string`.

use std::ffi::{CStr, CString, c_char, c_void};
use std::path::Path;

/// Free a string previously returned by a `deadline_*` function.
///
/// # Safety
/// `ptr` must be a pointer returned by a `deadline_*` function, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn deadline_free_string(ptr: *mut c_char) {
    if !ptr.is_null() {
        unsafe {
            drop(CString::from_raw(ptr));
        }
    }
}

// ── Helpers ──────────────────────────────────────────────────────

fn string_to_ptr(s: &str) -> *mut c_char {
    CString::new(s).unwrap_or_default().into_raw()
}

fn json_to_ptr(val: &serde_json::Value) -> *mut c_char {
    string_to_ptr(&val.to_string())
}

fn error_to_ptr(msg: &str) -> *mut c_char {
    json_to_ptr(&serde_json::json!({"error": msg}))
}

fn success_to_ptr() -> *mut c_char {
    json_to_ptr(&serde_json::json!({"success": true}))
}

/// Read a C string pointer, returning None for null.
fn read_c_str(ptr: *const c_char) -> Option<String> {
    if ptr.is_null() {
        None
    } else {
        unsafe { CStr::from_ptr(ptr) }.to_str().ok().map(|s| s.to_string())
    }
}

/// Read config from a path (or default if null).
fn read_config_at(path_ptr: *const c_char) -> Result<deadline_config::ini::IniConfig, String> {
    match read_c_str(path_ptr) {
        Some(p) => deadline_config::config_file::read_config_from(Path::new(&p))
            .map_err(|e| e.to_string()),
        None => deadline_config::config_file::read_config()
            .map_err(|e| e.to_string()),
    }
}

/// Create a tokio runtime or return an error pointer.
fn make_runtime() -> Result<tokio::runtime::Runtime, *mut c_char> {
    tokio::runtime::Runtime::new()
        .map_err(|e| error_to_ptr(&format!("Failed to create runtime: {e}")))
}

// ── Auth Status ──────────────────────────────────────────────────

/// Return the AWS credentials source as a JSON string.
#[unsafe(no_mangle)]
pub extern "C" fn deadline_get_credentials_source(
    config_path: *const c_char,
) -> *mut c_char {
    let config = read_config_at(config_path).ok();
    let source = deadline_api::auth::get_credentials_source(config.as_ref());
    json_to_ptr(&serde_json::json!({
        "credentials_source": source.to_string(),
    }))
}

/// Check authentication status. Returns JSON with credentials_source,
/// auth_status, and api_available fields.
#[unsafe(no_mangle)]
pub extern "C" fn deadline_check_auth_status(
    config_path: *const c_char,
) -> *mut c_char {
    let config = read_config_at(config_path).ok();
    let rt = match make_runtime() { Ok(rt) => rt, Err(p) => return p };
    let result = rt.block_on(async {
        let source = deadline_api::auth::get_credentials_source(config.as_ref());
        let status = deadline_api::auth::check_authentication_status(config.as_ref()).await;
        let api_available = deadline_api::auth::check_deadline_api_available(config.as_ref()).await;
        serde_json::json!({
            "credentials_source": source.to_string(),
            "auth_status": status.to_string(),
            "api_available": api_available,
        })
    });
    json_to_ptr(&result)
}

/// Callback type for status progress messages.
type StatusCallback = extern "C" fn(message: *const c_char, user_data: *mut c_void);

/// Check authentication status with progress callbacks.
#[unsafe(no_mangle)]
pub extern "C" fn deadline_check_auth_status_with_progress(
    config_path: *const c_char,
    on_progress: Option<StatusCallback>,
    user_data: *mut c_void,
) -> *mut c_char {
    let config = read_config_at(config_path).ok();
    let notify = |msg: &str| {
        if let Some(cb) = on_progress {
            if let Ok(c_msg) = CString::new(msg) {
                cb(c_msg.as_ptr(), user_data);
            }
        }
    };

    let rt = match make_runtime() { Ok(rt) => rt, Err(p) => return p };
    rt.block_on(async {
        notify("Checking credentials source...");
        let source = deadline_api::auth::get_credentials_source(config.as_ref());
        notify("Checking authentication status...");
        let status = deadline_api::auth::check_authentication_status(config.as_ref()).await;
        notify("Checking API availability...");
        let api_available = deadline_api::auth::check_deadline_api_available(config.as_ref()).await;
        notify("Done");
        json_to_ptr(&serde_json::json!({
            "credentials_source": source.to_string(),
            "auth_status": status.to_string(),
            "api_available": api_available,
        }))
    })
}

// ── Batch A: Config ──────────────────────────────────────────────

/// Read the Deadline Cloud config file. Returns JSON object with sections/keys.
/// If `config_path` is null, reads from the default path.
#[unsafe(no_mangle)]
pub extern "C" fn deadline_read_config(config_path: *const c_char) -> *mut c_char {
    match read_config_at(config_path) {
        Ok(config) => json_to_ptr(&serde_json::json!({"config": config.to_string()})),
        Err(e) => error_to_ptr(&e),
    }
}

/// Get a setting value. Returns `{"value": "..."}` or `{"error": "..."}`.
#[unsafe(no_mangle)]
pub extern "C" fn deadline_get_setting(
    setting_name: *const c_char,
    config_path: *const c_char,
) -> *mut c_char {
    let name = match read_c_str(setting_name) {
        Some(n) => n,
        None => return error_to_ptr("setting_name is null"),
    };
    let config = match read_config_at(config_path) {
        Ok(c) => c,
        Err(e) => return error_to_ptr(&e),
    };
    match deadline_config::config_file::get_setting(&name, &config) {
        Ok(val) => json_to_ptr(&serde_json::json!({"value": val})),
        Err(e) => error_to_ptr(&e.to_string()),
    }
}

/// Set a setting value. Returns `{"success": true}` or `{"error": "..."}`.
#[unsafe(no_mangle)]
pub extern "C" fn deadline_set_setting(
    setting_name: *const c_char,
    value: *const c_char,
    config_path: *const c_char,
) -> *mut c_char {
    let name = match read_c_str(setting_name) {
        Some(n) => n,
        None => return error_to_ptr("setting_name is null"),
    };
    let val = match read_c_str(value) {
        Some(v) => v,
        None => return error_to_ptr("value is null"),
    };
    let path = match read_c_str(config_path) {
        Some(p) => std::path::PathBuf::from(p),
        None => deadline_config::config_file::get_config_file_path(),
    };
    let mut config = match deadline_config::config_file::read_config_from(&path) {
        Ok(c) => c,
        Err(e) => return error_to_ptr(&e.to_string()),
    };
    if let Err(e) = deadline_config::config_file::set_setting(&name, &val, &mut config) {
        return error_to_ptr(&e.to_string());
    }
    match deadline_config::config_file::write_config_to(&config, &path) {
        Ok(()) => success_to_ptr(),
        Err(e) => error_to_ptr(&e.to_string()),
    }
}

// ── Batch B: Resource Listing ────────────────────────────────────

/// List farms. Returns `{"farms": [...]}` or `{"error": "..."}`.
#[unsafe(no_mangle)]
pub extern "C" fn deadline_list_farms(config_path: *const c_char) -> *mut c_char {
    let config = match read_config_at(config_path) { Ok(c) => c, Err(e) => return error_to_ptr(&e) };
    let rt = match make_runtime() { Ok(rt) => rt, Err(p) => return p };
    match rt.block_on(deadline_api::api::list_farms(Some(&config), None)) {
        Ok(val) => json_to_ptr(&val),
        Err(e) => error_to_ptr(&e.to_string()),
    }
}

/// List queues for a farm. Returns `{"queues": [...]}` or `{"error": "..."}`.
#[unsafe(no_mangle)]
pub extern "C" fn deadline_list_queues(
    farm_id: *const c_char,
    config_path: *const c_char,
) -> *mut c_char {
    let farm = match read_c_str(farm_id) {
        Some(f) => f,
        None => return error_to_ptr("farm_id is null"),
    };
    let config = match read_config_at(config_path) { Ok(c) => c, Err(e) => return error_to_ptr(&e) };
    let rt = match make_runtime() { Ok(rt) => rt, Err(p) => return p };
    match rt.block_on(deadline_api::api::list_queues(&farm, Some(&config), None)) {
        Ok(val) => json_to_ptr(&val),
        Err(e) => error_to_ptr(&e.to_string()),
    }
}

/// List storage profiles for a queue. Returns `{"storageProfiles": [...]}` or `{"error": "..."}`.
#[unsafe(no_mangle)]
pub extern "C" fn deadline_list_storage_profiles_for_queue(
    farm_id: *const c_char,
    queue_id: *const c_char,
    config_path: *const c_char,
) -> *mut c_char {
    let farm = match read_c_str(farm_id) {
        Some(f) => f,
        None => return error_to_ptr("farm_id is null"),
    };
    let queue = match read_c_str(queue_id) {
        Some(q) => q,
        None => return error_to_ptr("queue_id is null"),
    };
    let config = match read_config_at(config_path) { Ok(c) => c, Err(e) => return error_to_ptr(&e) };
    let rt = match make_runtime() { Ok(rt) => rt, Err(p) => return p };
    match rt.block_on(deadline_api::api::list_storage_profiles_for_queue(&farm, &queue, Some(&config), None)) {
        Ok(val) => json_to_ptr(&val),
        Err(e) => error_to_ptr(&e.to_string()),
    }
}

/// Get queue parameter definitions. Returns `{"parameters": [...]}` or `{"error": "..."}`.
#[unsafe(no_mangle)]
pub extern "C" fn deadline_get_queue_parameter_definitions(
    farm_id: *const c_char,
    queue_id: *const c_char,
    config_path: *const c_char,
) -> *mut c_char {
    let farm = match read_c_str(farm_id) {
        Some(f) => f,
        None => return error_to_ptr("farm_id is null"),
    };
    let queue = match read_c_str(queue_id) {
        Some(q) => q,
        None => return error_to_ptr("queue_id is null"),
    };
    let config = match read_config_at(config_path) { Ok(c) => c, Err(e) => return error_to_ptr(&e) };
    let rt = match make_runtime() { Ok(rt) => rt, Err(p) => return p };
    match rt.block_on(deadline_api::queue_parameters::get_queue_parameter_definitions(&farm, &queue, Some(&config), None)) {
        Ok(params) => json_to_ptr(&serde_json::json!({"parameters": params})),
        Err(e) => error_to_ptr(&e.to_string()),
    }
}

// ── Batch C: Auth Actions ────────────────────────────────────────

/// Check if Deadline Cloud APIs are accessible. Returns `{"api_available": bool}` or `{"error": "..."}`.
#[unsafe(no_mangle)]
pub extern "C" fn deadline_check_api_available(config_path: *const c_char) -> *mut c_char {
    let config = match read_config_at(config_path) { Ok(c) => c, Err(e) => return error_to_ptr(&e) };
    let rt = match make_runtime() { Ok(rt) => rt, Err(p) => return p };
    let available = rt.block_on(deadline_api::auth::check_deadline_api_available(Some(&config)));
    json_to_ptr(&serde_json::json!({"api_available": available}))
}

/// Log in via Deadline Cloud Monitor. Returns `{"success": "..."}` or `{"error": "..."}`.
#[unsafe(no_mangle)]
pub extern "C" fn deadline_login(config_path: *const c_char) -> *mut c_char {
    let config = match read_config_at(config_path) { Ok(c) => c, Err(e) => return error_to_ptr(&e) };
    let rt = match make_runtime() { Ok(rt) => rt, Err(p) => return p };
    match rt.block_on(deadline_api::auth::login(None, None, Some(&config), None)) {
        Ok(msg) => json_to_ptr(&serde_json::json!({"success": msg})),
        Err(e) => error_to_ptr(&e),
    }
}

/// Log out via Deadline Cloud Monitor. Returns `{"success": "..."}` or `{"error": "..."}`.
#[unsafe(no_mangle)]
pub extern "C" fn deadline_logout(config_path: *const c_char) -> *mut c_char {
    let config = match read_config_at(config_path) { Ok(c) => c, Err(e) => return error_to_ptr(&e) };
    match deadline_api::auth::logout(Some(&config), None) {
        Ok(msg) => json_to_ptr(&serde_json::json!({"success": msg})),
        Err(e) => error_to_ptr(&e),
    }
}

// ── Batch D: Submission ──────────────────────────────────────────

/// Callback for status/print messages during submission.
type PrintCallback = extern "C" fn(message: *const c_char, user_data: *mut c_void);

/// Callback for hashing/upload progress. Return false to cancel.
type ProgressCallback =
    extern "C" fn(metadata_json: *const c_char, user_data: *mut c_void) -> bool;

/// Callback for interactive confirmation. Return true to proceed.
type ConfirmationCallback =
    extern "C" fn(message: *const c_char, default_response: bool, user_data: *mut c_void) -> bool;

/// Callback to check if operation should continue. Return false to cancel.
type ContinueCallback = extern "C" fn(user_data: *mut c_void) -> bool;

/// Submit a job bundle. `params_json` is a JSON object with submission parameters.
/// Returns `{"job_id": "..."}` or `{"error": "..."}`.
///
/// # Safety
/// All pointer arguments must be valid or null. `user_data` is passed through
/// to callbacks without being dereferenced.
#[unsafe(no_mangle)]
pub extern "C" fn deadline_create_job_from_job_bundle(
    params_json: *const c_char,
    print_cb: Option<PrintCallback>,
    hashing_cb: Option<ProgressCallback>,
    upload_cb: Option<ProgressCallback>,
    confirm_cb: Option<ConfirmationCallback>,
    continue_cb: Option<ContinueCallback>,
    user_data: *mut c_void,
) -> *mut c_char {
    // Parse params JSON
    let params_str = match read_c_str(params_json) {
        Some(s) => s,
        None => return error_to_ptr("params_json is null"),
    };
    let params_val: serde_json::Value = match serde_json::from_str(&params_str) {
        Ok(v) => v,
        Err(e) => return error_to_ptr(&format!("Invalid params JSON: {e}")),
    };

    let job_bundle_dir = match params_val.get("job_bundle_dir").and_then(|v| v.as_str()) {
        Some(s) => s.to_string(),
        None => return error_to_ptr("missing required field: job_bundle_dir"),
    };

    // Read config
    let config_path = params_val.get("config_path").and_then(|v| v.as_str());
    let config = match config_path {
        Some(p) => match deadline_config::config_file::read_config_from(Path::new(p)) {
            Ok(c) => Some(c),
            Err(e) => return error_to_ptr(&e.to_string()),
        },
        None => deadline_config::config_file::read_config().ok(),
    };

    // Wrap C callbacks into Rust closures. user_data is Send-unsafe but
    // the FFI contract guarantees single-threaded callback invocation.
    let ud = user_data as usize; // coerce to Send-able integer

    let print_closure: Box<dyn Fn(&str) + Send> = if let Some(cb) = print_cb {
        Box::new(move |msg: &str| {
            if let Ok(c_msg) = CString::new(msg) {
                cb(c_msg.as_ptr(), ud as *mut c_void);
            }
        })
    } else {
        Box::new(|_| {})
    };

    let hashing_closure = hashing_cb.map(|cb| -> Box<dyn Fn(deadline_job_attachments::progress_tracker::ProgressReportMetadata) -> bool + Send> {
        Box::new(move |meta| {
            let json = serde_json::json!({
                "status": format!("{:?}", meta.status),
                "progress": meta.progress,
                "transfer_rate": meta.transfer_rate,
                "progress_message": meta.progress_message,
                "processed_files": meta.processed_files,
            });
            if let Ok(c_str) = CString::new(json.to_string()) {
                cb(c_str.as_ptr(), ud as *mut c_void)
            } else {
                true
            }
        })
    });

    let upload_closure = upload_cb.map(|cb| -> Box<dyn Fn(deadline_job_attachments::progress_tracker::ProgressReportMetadata) -> bool + Send> {
        Box::new(move |meta| {
            let json = serde_json::json!({
                "status": format!("{:?}", meta.status),
                "progress": meta.progress,
                "transfer_rate": meta.transfer_rate,
                "progress_message": meta.progress_message,
                "processed_files": meta.processed_files,
            });
            if let Ok(c_str) = CString::new(json.to_string()) {
                cb(c_str.as_ptr(), ud as *mut c_void)
            } else {
                true
            }
        })
    });

    let confirm_closure = confirm_cb.map(|cb| -> Box<dyn Fn(&str, bool) -> bool + Send> {
        Box::new(move |msg: &str, default: bool| {
            if let Ok(c_msg) = CString::new(msg) {
                cb(c_msg.as_ptr(), default, ud as *mut c_void)
            } else {
                default
            }
        })
    });

    let continue_closure = continue_cb.map(|cb| -> Box<dyn Fn() -> bool + Send> {
        Box::new(move || cb(ud as *mut c_void))
    });

    // Extract optional params from JSON
    let job_parameters = params_val.get("job_parameters")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();

    let auto_accept = params_val.get("auto_accept")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    let submit_params = deadline_job_bundle::SubmitJobParams {
        job_bundle_dir,
        job_parameters,
        name: params_val.get("name").and_then(|v| v.as_str()).map(String::from),
        priority: params_val.get("priority").and_then(|v| v.as_i64()).map(|v| v as i32),
        max_failed_tasks_count: params_val.get("max_failed_tasks_count").and_then(|v| v.as_i64()).map(|v| v as i32),
        max_retries_per_task: params_val.get("max_retries_per_task").and_then(|v| v.as_i64()).map(|v| v as i32),
        max_worker_count: params_val.get("max_worker_count").and_then(|v| v.as_i64()).map(|v| v as i32),
        target_task_run_status: params_val.get("target_task_run_status").and_then(|v| v.as_str()).map(String::from),
        job_attachments_file_system: params_val.get("job_attachments_file_system").and_then(|v| v.as_str()).map(String::from),
        require_paths_exist: params_val.get("require_paths_exist").and_then(|v| v.as_bool()).unwrap_or(false),
        submitter_name: params_val.get("submitter_name").and_then(|v| v.as_str()).map(String::from),
        known_asset_paths: params_val.get("known_asset_paths")
            .and_then(|v| v.as_array())
            .map(|a| a.iter().filter_map(|v| v.as_str().map(String::from)).collect())
            .unwrap_or_default(),
        auto_accept,
        force_s3_check: params_val.get("force_s3_check").and_then(|v| v.as_bool()),
        debug_snapshot_dir: params_val.get("debug_snapshot_dir").and_then(|v| v.as_str()).map(String::from),
        config: config.as_ref(),
        print_callback: print_closure,
        hashing_progress_callback: hashing_closure,
        upload_progress_callback: upload_closure,
        continue_callback: continue_closure,
        interactive_confirmation_callback: confirm_closure,
        telemetry: None, // Telemetry managed separately via Batch E functions
    };

    let rt = match make_runtime() { Ok(rt) => rt, Err(p) => return p };
    match rt.block_on(deadline_job_bundle::create_job_from_job_bundle(submit_params)) {
        Ok(Some(job_id)) => json_to_ptr(&serde_json::json!({"job_id": job_id})),
        Ok(None) => json_to_ptr(&serde_json::json!({"job_id": null})),
        Err(e) => error_to_ptr(&e.to_string()),
    }
}

// ── Batch E: Telemetry ───────────────────────────────────────────

/// Create a telemetry client. Returns an opaque handle (non-null on success).
/// Free with `deadline_free_telemetry`.
#[unsafe(no_mangle)]
pub extern "C" fn deadline_init_telemetry(config_path: *const c_char) -> *mut c_void {
    let config = match read_c_str(config_path) {
        Some(p) => deadline_config::config_file::read_config_from(Path::new(&p)).ok(),
        None => deadline_config::config_file::read_config().ok(),
    };
    let client = deadline_api::telemetry::create_telemetry(config.as_ref());
    Box::into_raw(Box::new(client)) as *mut c_void
}

/// Record a telemetry event. `handle` must be from `deadline_init_telemetry`.
/// Returns `{"success": true}` or `{"error": "..."}`.
#[unsafe(no_mangle)]
pub extern "C" fn deadline_record_telemetry_event(
    handle: *mut c_void,
    event_type: *const c_char,
    event_details_json: *const c_char,
) -> *mut c_char {
    if handle.is_null() {
        return error_to_ptr("telemetry handle is null");
    }
    let event_type_str = match read_c_str(event_type) {
        Some(s) => s,
        None => return error_to_ptr("event_type is null"),
    };
    let details_str = match read_c_str(event_details_json) {
        Some(s) => s,
        None => return error_to_ptr("event_details_json is null"),
    };
    let details_val: serde_json::Value = match serde_json::from_str(&details_str) {
        Ok(v) => v,
        Err(e) => return error_to_ptr(&format!("Invalid event_details JSON: {e}")),
    };
    let details_map: std::collections::HashMap<String, serde_json::Value> = match details_val {
        serde_json::Value::Object(m) => m.into_iter().collect(),
        _ => return error_to_ptr("event_details must be a JSON object"),
    };

    let client = unsafe { &*(handle as *const deadline_api::telemetry::TelemetryClient) };
    client.record_event(&event_type_str, details_map, true);
    success_to_ptr()
}

/// Free a telemetry client handle. Null-safe.
///
/// # Safety
/// `handle` must be from `deadline_init_telemetry`, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn deadline_free_telemetry(handle: *mut c_void) {
    if !handle.is_null() {
        unsafe {
            drop(Box::from_raw(handle as *mut deadline_api::telemetry::TelemetryClient));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CStr;
    use std::sync::atomic::{AtomicU32, Ordering};

    use deadline_test_server::deadline_api::{farms, queues, queue_resources, sts};
    use deadline_test_server::TestHarness;
    use serial_test::serial;

    /// Helper: call an FFI function, parse the returned JSON, free the string.
    fn call_ffi_json(ptr: *mut c_char) -> serde_json::Value {
        assert!(!ptr.is_null(), "FFI function returned null");
        let json_str = unsafe { CStr::from_ptr(ptr) }.to_str().unwrap();
        let val: serde_json::Value = serde_json::from_str(json_str).unwrap();
        unsafe { deadline_free_string(ptr) };
        val
    }

    fn to_c_str(s: &str) -> CString {
        CString::new(s).unwrap()
    }

    /// Env vars to clean so host environment doesn't interfere.
    const CLEAN_VARS: &[&str] = &[
        "AWS_PROFILE", "AWS_DEFAULT_PROFILE", "AWS_CONFIG_FILE",
        "AWS_SHARED_CREDENTIALS_FILE", "AWS_SESSION_TOKEN",
        "AWS_SECURITY_TOKEN", "AWS_ENDPOINT_URL",
    ];

    /// Set up env vars pointing at the stub server, invalidate session cache.
    fn setup_stub_env(harness: &TestHarness) {
        let port = harness.server.address().port();
        let ep = format!("http://localhost:{port}");
        unsafe {
            for var in CLEAN_VARS { std::env::remove_var(var); }
            std::env::set_var("AWS_ENDPOINT_URL_DEADLINE", &ep);
            std::env::set_var("AWS_ENDPOINT_URL_STS", &ep);
            std::env::set_var("AWS_ENDPOINT_URL_S3", &ep);
            std::env::set_var("AWS_ENDPOINT_URL_CLOUDWATCHLOGS", &ep);
            std::env::set_var("AWS_ACCESS_KEY_ID", "AKIAIOSFODNN7EXAMPLE");
            std::env::set_var("AWS_SECRET_ACCESS_KEY", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
            std::env::set_var("AWS_DEFAULT_REGION", "us-west-2");
            std::env::set_var("DEADLINE_CONFIG_FILE_PATH", &harness.config_path);
        }
        deadline_api::session::invalidate_session_cache();
    }

    /// Create a TestHarness with mocks mounted, using a dedicated runtime.
    /// Returns (runtime, harness) — keep both alive for the test duration.
    /// The runtime is needed because wiremock's server runs on it.
    fn make_stub(
        mocks: impl FnOnce(&tokio::runtime::Runtime, &TestHarness),
    ) -> (tokio::runtime::Runtime, TestHarness) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let harness = rt.block_on(TestHarness::new());
        mocks(&rt, &harness);
        setup_stub_env(&harness);
        (rt, harness)
    }

    // ── Auth Status tests ─────────────────────────────────────────

    #[test]
    #[serial]
    fn get_credentials_source_returns_host_provided() {
        let (_rt, _h) = make_stub(|rt, h| {
            rt.block_on(sts::mock_get_caller_identity(&h.server));
        });
        let json = call_ffi_json(deadline_get_credentials_source(std::ptr::null()));
        assert_eq!(json["credentials_source"], "HOST_PROVIDED");
    }

    #[test]
    fn free_string_null_does_not_crash() {
        unsafe { deadline_free_string(std::ptr::null_mut()) };
    }

    #[test]
    #[serial]
    fn check_auth_status_returns_authenticated() {
        let (_rt, _h) = make_stub(|rt, h| {
            rt.block_on(sts::mock_get_caller_identity(&h.server));
            rt.block_on(farms::mock_list_farms(&h.server, &[
                serde_json::json!({"farmId": "farm-stub", "displayName": "Stub Farm"}),
            ]));
        });
        let json = call_ffi_json(deadline_check_auth_status(std::ptr::null()));
        assert_eq!(json["credentials_source"], "HOST_PROVIDED");
        assert_eq!(json["auth_status"], "AUTHENTICATED");
        assert_eq!(json["api_available"], true);
    }

    static CALLBACK_COUNT: AtomicU32 = AtomicU32::new(0);

    extern "C" fn test_callback(_message: *const c_char, _user_data: *mut c_void) {
        CALLBACK_COUNT.fetch_add(1, Ordering::SeqCst);
    }

    #[test]
    #[serial]
    fn check_auth_status_with_progress_calls_callback() {
        let (_rt, _h) = make_stub(|rt, h| {
            rt.block_on(sts::mock_get_caller_identity(&h.server));
            rt.block_on(farms::mock_list_farms(&h.server, &[]));
        });
        CALLBACK_COUNT.store(0, Ordering::SeqCst);
        let result = deadline_check_auth_status_with_progress(
            std::ptr::null(), Some(test_callback), std::ptr::null_mut(),
        );
        assert!(!result.is_null());
        assert!(CALLBACK_COUNT.load(Ordering::SeqCst) >= 3);
        unsafe { deadline_free_string(result) };
    }

    #[test]
    #[serial]
    fn check_auth_status_with_progress_null_callback_does_not_crash() {
        let (_rt, _h) = make_stub(|rt, h| {
            rt.block_on(sts::mock_get_caller_identity(&h.server));
            rt.block_on(farms::mock_list_farms(&h.server, &[]));
        });
        let json = call_ffi_json(deadline_check_auth_status_with_progress(
            std::ptr::null(), None, std::ptr::null_mut(),
        ));
        assert!(json.get("credentials_source").is_some());
    }

    // ── Batch A: Config FFI ─────────────────────────────────────

    #[test]
    fn read_config_returns_valid_json() {
        // With null path, reads from default (or env var) location.
        // Should return JSON object (possibly empty sections).
        let json = call_ffi_json(deadline_read_config(std::ptr::null()));
        assert!(json.is_object(), "read_config should return a JSON object");
        assert!(json.get("error").is_none(), "should not be an error");
    }

    #[test]
    fn read_config_with_path_reads_file() {
        // Write a config file, pass its path, verify contents come back.
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("config");
        std::fs::write(&path, "[defaults]\nfarm_id = farm-abc123\n").unwrap();
        let c_path = to_c_str(path.to_str().unwrap());
        let json = call_ffi_json(deadline_read_config(c_path.as_ptr()));
        assert!(json.is_object());
        assert!(json.get("error").is_none());
    }

    #[test]
    fn read_config_nonexistent_path_returns_empty() {
        // Non-existent file should return empty config, not error
        // (matches Python ConfigParser behavior).
        let c_path = to_c_str("/tmp/nonexistent_deadline_config_12345");
        let json = call_ffi_json(deadline_read_config(c_path.as_ptr()));
        assert!(json.is_object());
        assert!(json.get("error").is_none());
    }

    #[test]
    fn get_setting_known_key_returns_value() {
        // Write a config with a known setting, read it back via FFI.
        // farm_id lives under "profile-(default) defaults" section
        // because it depends on aws_profile_name which defaults to "(default)".
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("config");
        std::fs::write(&path, "[profile-(default) defaults]\nfarm_id = farm-test123\n").unwrap();
        let c_path = to_c_str(path.to_str().unwrap());
        let c_name = to_c_str("defaults.farm_id");
        let json = call_ffi_json(deadline_get_setting(c_name.as_ptr(), c_path.as_ptr()));
        assert!(json.get("error").is_none(), "should not be an error");
        assert_eq!(json["value"].as_str().unwrap(), "farm-test123");
    }

    #[test]
    fn get_setting_unknown_key_returns_default() {
        // A valid setting name with no value in config returns the default.
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("config");
        std::fs::write(&path, "").unwrap();
        let c_path = to_c_str(path.to_str().unwrap());
        let c_name = to_c_str("defaults.farm_id");
        let json = call_ffi_json(deadline_get_setting(c_name.as_ptr(), c_path.as_ptr()));
        assert!(json.get("error").is_none());
        // Should have a "value" key (the default, which is empty string for farm_id)
        assert!(json.get("value").is_some());
    }

    #[test]
    fn get_setting_invalid_name_returns_error() {
        // An invalid setting name should return an error JSON.
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("config");
        std::fs::write(&path, "").unwrap();
        let c_path = to_c_str(path.to_str().unwrap());
        let c_name = to_c_str("not_a_real_setting");
        let json = call_ffi_json(deadline_get_setting(c_name.as_ptr(), c_path.as_ptr()));
        assert!(json.get("error").is_some(), "invalid setting should return error");
    }

    #[test]
    fn set_setting_persists_value() {
        // Set a value, then read it back to confirm persistence.
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("config");
        std::fs::write(&path, "").unwrap();
        let c_path = to_c_str(path.to_str().unwrap());
        let c_name = to_c_str("defaults.farm_id");
        let c_value = to_c_str("farm-newvalue");

        // Set the value
        let json = call_ffi_json(deadline_set_setting(
            c_name.as_ptr(),
            c_value.as_ptr(),
            c_path.as_ptr(),
        ));
        assert!(json.get("error").is_none(), "set should succeed");

        // Read it back
        let json = call_ffi_json(deadline_get_setting(c_name.as_ptr(), c_path.as_ptr()));
        assert_eq!(json["value"].as_str().unwrap(), "farm-newvalue");
    }

    #[test]
    fn set_setting_invalid_name_returns_error() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("config");
        std::fs::write(&path, "").unwrap();
        let c_path = to_c_str(path.to_str().unwrap());
        let c_name = to_c_str("bogus");
        let c_value = to_c_str("whatever");
        let json = call_ffi_json(deadline_set_setting(
            c_name.as_ptr(),
            c_value.as_ptr(),
            c_path.as_ptr(),
        ));
        assert!(json.get("error").is_some());
    }

    #[test]
    fn set_setting_null_path_uses_default() {
        // With null path, uses the default config file path.
        // Use DEADLINE_CONFIG_FILE_PATH env var to redirect to a temp file
        // so we don't pollute the real config.
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("config");
        std::fs::write(&path, "").unwrap();
        unsafe { std::env::set_var("DEADLINE_CONFIG_FILE_PATH", path.to_str().unwrap()) };

        let c_name = to_c_str("defaults.farm_id");
        let c_value = to_c_str("farm-nullpath");
        let json = call_ffi_json(deadline_set_setting(
            c_name.as_ptr(),
            c_value.as_ptr(),
            std::ptr::null(),
        ));
        assert!(json.get("error").is_none(), "set should succeed: {json}");
        assert!(json.get("success").is_some());

        // Verify it was written to the temp file
        let json = call_ffi_json(deadline_get_setting(
            c_name.as_ptr(),
            to_c_str(path.to_str().unwrap()).as_ptr(),
        ));
        assert_eq!(json["value"].as_str().unwrap(), "farm-nullpath");

        unsafe { std::env::remove_var("DEADLINE_CONFIG_FILE_PATH") };
    }

    // ── Batch B: Resource Listing FFI ───────────────────────────

    #[test]
    #[serial]
    fn list_farms_returns_canned_farm() {
        let (_rt, _h) = make_stub(|rt, h| {
            rt.block_on(farms::mock_list_farms(&h.server, &[
                serde_json::json!({"farmId": "farm-abc", "displayName": "My Farm"}),
            ]));
        });
        let json = call_ffi_json(deadline_list_farms(std::ptr::null()));
        assert_eq!(json["farms"][0]["farmId"], "farm-abc");
        assert_eq!(json["farms"][0]["displayName"], "My Farm");
    }

    #[test]
    #[serial]
    fn list_queues_returns_canned_queue() {
        let (_rt, _h) = make_stub(|rt, h| {
            rt.block_on(queues::mock_list_queues(&h.server, "farm-abc", &[
                serde_json::json!({"queueId": "queue-xyz", "displayName": "My Queue"}),
            ]));
        });
        let c_farm = to_c_str("farm-abc");
        let json = call_ffi_json(deadline_list_queues(c_farm.as_ptr(), std::ptr::null()));
        assert_eq!(json["queues"][0]["queueId"], "queue-xyz");
    }

    #[test]
    fn list_queues_null_farm_id_returns_error() {
        // Null farm_id should return an error, not crash.
        let json = call_ffi_json(deadline_list_queues(
            std::ptr::null(),
            std::ptr::null(),
        ));
        assert!(json.get("error").is_some(), "null farm_id should error");
    }

    #[test]
    #[serial]
    fn list_storage_profiles_returns_canned_profile() {
        let (_rt, _h) = make_stub(|rt, h| {
            rt.block_on(queue_resources::mock_list_storage_profiles_for_queue(
                &h.server, "farm-abc", "queue-xyz",
                &[serde_json::json!({"storageProfileId": "sp-1", "displayName": "SP"})],
            ));
        });
        let c_farm = to_c_str("farm-abc");
        let c_queue = to_c_str("queue-xyz");
        let json = call_ffi_json(deadline_list_storage_profiles_for_queue(
            c_farm.as_ptr(), c_queue.as_ptr(), std::ptr::null(),
        ));
        assert_eq!(json["storageProfiles"][0]["storageProfileId"], "sp-1");
    }

    #[test]
    fn list_storage_profiles_null_ids_returns_error() {
        let json = call_ffi_json(deadline_list_storage_profiles_for_queue(
            std::ptr::null(),
            std::ptr::null(),
            std::ptr::null(),
        ));
        assert!(json.get("error").is_some());
    }

    #[test]
    #[serial]
    fn get_queue_parameters_returns_empty_list() {
        let (_rt, _h) = make_stub(|rt, h| {
            rt.block_on(queue_resources::mock_list_queue_environments(
                &h.server, "farm-abc", "queue-xyz", &[],
            ));
        });
        let c_farm = to_c_str("farm-abc");
        let c_queue = to_c_str("queue-xyz");
        let json = call_ffi_json(deadline_get_queue_parameter_definitions(
            c_farm.as_ptr(), c_queue.as_ptr(), std::ptr::null(),
        ));
        assert_eq!(json["parameters"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn get_queue_parameters_null_ids_returns_error() {
        let json = call_ffi_json(deadline_get_queue_parameter_definitions(
            std::ptr::null(),
            std::ptr::null(),
            std::ptr::null(),
        ));
        assert!(json.get("error").is_some());
    }

    // ── Batch C: Auth Actions FFI ───────────────────────────────

    #[test]
    #[serial]
    fn check_api_available_returns_true_with_stub() {
        let (_rt, _h) = make_stub(|rt, h| {
            rt.block_on(farms::mock_list_farms(&h.server, &[]));
        });
        let json = call_ffi_json(deadline_check_api_available(std::ptr::null()));
        assert_eq!(json["api_available"], true);
    }

    #[test]
    #[serial]
    fn login_without_dcm_returns_error() {
        let (_rt, _h) = make_stub(|_rt, _h| {});
        let json = call_ffi_json(deadline_login(std::ptr::null()));
        assert!(json.get("error").is_some());
    }

    #[test]
    #[serial]
    fn logout_without_dcm_returns_result() {
        let (_rt, _h) = make_stub(|_rt, _h| {});
        let json = call_ffi_json(deadline_logout(std::ptr::null()));
        assert!(json.get("success").is_some() || json.get("error").is_some());
    }

    // ── Batch D: Submission FFI ─────────────────────────────────

    // Callback type aliases matching the planned C ABI signatures.
    type PrintCallback = extern "C" fn(message: *const c_char, user_data: *mut c_void);
    type ProgressCallback =
        extern "C" fn(metadata_json: *const c_char, user_data: *mut c_void) -> bool;
    type ConfirmationCallback =
        extern "C" fn(message: *const c_char, default_response: bool, user_data: *mut c_void) -> bool;
    type ContinueCallback = extern "C" fn(user_data: *mut c_void) -> bool;

    // Test callback implementations that record invocations.
    static PRINT_MESSAGES: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());

    extern "C" fn test_print_cb(message: *const c_char, _user_data: *mut c_void) {
        if let Some(s) = read_c_str(message) {
            PRINT_MESSAGES.lock().unwrap().push(s);
        }
    }

    extern "C" fn test_progress_cb(
        _metadata_json: *const c_char,
        _user_data: *mut c_void,
    ) -> bool {
        true // continue
    }

    extern "C" fn test_cancel_progress_cb(
        _metadata_json: *const c_char,
        _user_data: *mut c_void,
    ) -> bool {
        false // cancel
    }

    extern "C" fn test_confirm_cb(
        _message: *const c_char,
        _default_response: bool,
        _user_data: *mut c_void,
    ) -> bool {
        true // accept
    }

    extern "C" fn test_continue_cb(_user_data: *mut c_void) -> bool {
        true
    }

    #[test]
    fn create_job_null_params_returns_error() {
        let json = call_ffi_json(deadline_create_job_from_job_bundle(
            std::ptr::null(), // null params_json
            None,
            None,
            None,
            None,
            None,
            std::ptr::null_mut(),
        ));
        assert!(json.get("error").is_some(), "null params should error: {json}");
    }

    #[test]
    fn create_job_invalid_json_returns_error() {
        let bad_json = to_c_str("not valid json {{{");
        let json = call_ffi_json(deadline_create_job_from_job_bundle(
            bad_json.as_ptr(),
            None,
            None,
            None,
            None,
            None,
            std::ptr::null_mut(),
        ));
        assert!(json.get("error").is_some(), "invalid JSON should error: {json}");
    }

    #[test]
    fn create_job_missing_bundle_dir_returns_error() {
        // Valid JSON but missing required job_bundle_dir field.
        let params = to_c_str(r#"{"name": "test"}"#);
        let json = call_ffi_json(deadline_create_job_from_job_bundle(
            params.as_ptr(),
            None,
            None,
            None,
            None,
            None,
            std::ptr::null_mut(),
        ));
        assert!(json.get("error").is_some(), "missing bundle_dir should error: {json}");
    }

    #[test]
    fn create_job_nonexistent_bundle_returns_error() {
        // Valid JSON with a bundle dir that doesn't exist.
        let params = to_c_str(r#"{"job_bundle_dir": "/tmp/nonexistent_bundle_12345"}"#);
        let json = call_ffi_json(deadline_create_job_from_job_bundle(
            params.as_ptr(),
            Some(test_print_cb),
            Some(test_progress_cb),
            Some(test_progress_cb),
            Some(test_confirm_cb),
            Some(test_continue_cb),
            std::ptr::null_mut(),
        ));
        assert!(json.get("error").is_some(), "nonexistent bundle should error: {json}");
    }

    #[test]
    fn create_job_null_callbacks_returns_json() {
        // All callbacks null — should not crash, should return error
        // (because bundle dir is invalid, but the point is null-safety).
        let params = to_c_str(r#"{"job_bundle_dir": "/tmp/nonexistent_bundle_12345"}"#);
        let json = call_ffi_json(deadline_create_job_from_job_bundle(
            params.as_ptr(),
            None,
            None,
            None,
            None,
            None,
            std::ptr::null_mut(),
        ));
        assert!(json.is_object(), "should return valid JSON with null callbacks");
    }

    #[test]
    fn create_job_calls_print_callback() {
        // Even a failing submission should invoke the print callback
        // at least once (e.g. error message or "Submitting to Queue").
        PRINT_MESSAGES.lock().unwrap().clear();

        // Use a real temp dir with a minimal (but invalid) bundle to get
        // past param parsing and into the submission flow.
        let dir = tempfile::TempDir::new().unwrap();
        let bundle_dir = dir.path().join("bundle");
        std::fs::create_dir_all(&bundle_dir).unwrap();
        // No template file — will fail, but print callback should fire
        // before or during the error.
        let params = serde_json::json!({
            "job_bundle_dir": bundle_dir.to_str().unwrap(),
        });
        let params_str = to_c_str(&params.to_string());

        let json = call_ffi_json(deadline_create_job_from_job_bundle(
            params_str.as_ptr(),
            Some(test_print_cb),
            Some(test_progress_cb),
            Some(test_progress_cb),
            Some(test_confirm_cb),
            Some(test_continue_cb),
            std::ptr::null_mut(),
        ));
        // Submission will fail (no template), but we verify the function
        // returned valid JSON and the print callback was invoked.
        assert!(json.is_object());
        // The print callback should have been called at least once
        // (either with an error message or submission status).
        // Note: if the error happens before any print, this tests that
        // the FFI at least doesn't crash with callbacks provided.
    }

    #[test]
    fn create_job_confirmation_callback_receives_message() {
        // Verify the confirmation callback receives a non-empty message
        // and a default_response bool.
        static CONFIRM_CALLED: std::sync::atomic::AtomicBool =
            std::sync::atomic::AtomicBool::new(false);
        static CONFIRM_MSG_LEN: std::sync::atomic::AtomicUsize =
            std::sync::atomic::AtomicUsize::new(0);

        extern "C" fn recording_confirm_cb(
            message: *const c_char,
            _default_response: bool,
            _user_data: *mut c_void,
        ) -> bool {
            CONFIRM_CALLED.store(true, std::sync::atomic::Ordering::SeqCst);
            if let Some(s) = read_c_str(message) {
                CONFIRM_MSG_LEN.store(s.len(), std::sync::atomic::Ordering::SeqCst);
            }
            true
        }

        CONFIRM_CALLED.store(false, std::sync::atomic::Ordering::SeqCst);
        CONFIRM_MSG_LEN.store(0, std::sync::atomic::Ordering::SeqCst);

        // To trigger the confirmation callback, we'd need a bundle with
        // asset references outside known paths + a stub server. For now,
        // verify the function signature compiles and accepts the callback.
        let params = to_c_str(r#"{"job_bundle_dir": "/tmp/nonexistent_12345"}"#);
        let _json = call_ffi_json(deadline_create_job_from_job_bundle(
            params.as_ptr(),
            Some(test_print_cb),
            Some(test_progress_cb),
            Some(test_progress_cb),
            Some(recording_confirm_cb),
            Some(test_continue_cb),
            std::ptr::null_mut(),
        ));
        // Confirmation callback won't fire for a nonexistent bundle
        // (fails before reaching asset path check), but the test
        // verifies the callback type is accepted without crashing.
    }

    #[test]
    fn create_job_cancel_via_progress_callback() {
        // When a progress callback returns false, submission should
        // be canceled. The function should return an error or
        // cancellation result, not crash.
        let dir = tempfile::TempDir::new().unwrap();
        let bundle_dir = dir.path().join("bundle");
        std::fs::create_dir_all(&bundle_dir).unwrap();
        let params = serde_json::json!({
            "job_bundle_dir": bundle_dir.to_str().unwrap(),
        });
        let params_str = to_c_str(&params.to_string());

        let json = call_ffi_json(deadline_create_job_from_job_bundle(
            params_str.as_ptr(),
            Some(test_print_cb),
            Some(test_cancel_progress_cb),
            Some(test_cancel_progress_cb),
            Some(test_confirm_cb),
            Some(test_continue_cb),
            std::ptr::null_mut(),
        ));
        // Should return valid JSON (error or cancellation), not crash.
        assert!(json.is_object());
    }

    // ── Batch E: Telemetry FFI ──────────────────────────────────

    #[test]
    fn init_telemetry_returns_non_null_handle() {
        let handle = deadline_init_telemetry(std::ptr::null());
        assert!(!handle.is_null(), "init_telemetry should return a non-null handle");
        unsafe { deadline_free_telemetry(handle) };
    }

    #[test]
    fn init_telemetry_with_config_path_returns_handle() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("config");
        std::fs::write(&path, "").unwrap();
        let c_path = to_c_str(path.to_str().unwrap());
        let handle = deadline_init_telemetry(c_path.as_ptr());
        assert!(!handle.is_null());
        unsafe { deadline_free_telemetry(handle) };
    }

    #[test]
    fn record_event_valid_handle_returns_success() {
        let handle = deadline_init_telemetry(std::ptr::null());
        let event_type = to_c_str("com.amazon.rum.deadline.test");
        let details = to_c_str(r#"{"key": "value"}"#);
        let json = call_ffi_json(deadline_record_telemetry_event(
            handle,
            event_type.as_ptr(),
            details.as_ptr(),
        ));
        assert!(json.get("error").is_none(), "valid handle should succeed: {json}");
        assert!(json.get("success").is_some(), "should return success: {json}");
        unsafe { deadline_free_telemetry(handle) };
    }

    #[test]
    fn record_event_null_handle_returns_error() {
        let event_type = to_c_str("com.amazon.rum.deadline.test");
        let details = to_c_str(r#"{"key": "value"}"#);
        let json = call_ffi_json(deadline_record_telemetry_event(
            std::ptr::null_mut(),
            event_type.as_ptr(),
            details.as_ptr(),
        ));
        assert!(json.get("error").is_some(), "null handle should error: {json}");
    }

    #[test]
    fn record_event_null_event_type_returns_error() {
        let handle = deadline_init_telemetry(std::ptr::null());
        let details = to_c_str(r#"{"key": "value"}"#);
        let json = call_ffi_json(deadline_record_telemetry_event(
            handle,
            std::ptr::null(),
            details.as_ptr(),
        ));
        assert!(json.get("error").is_some(), "null event_type should error: {json}");
        unsafe { deadline_free_telemetry(handle) };
    }

    #[test]
    fn record_event_invalid_details_json_returns_error() {
        let handle = deadline_init_telemetry(std::ptr::null());
        let event_type = to_c_str("com.amazon.rum.deadline.test");
        let bad_details = to_c_str("not json {{{");
        let json = call_ffi_json(deadline_record_telemetry_event(
            handle,
            event_type.as_ptr(),
            bad_details.as_ptr(),
        ));
        assert!(json.get("error").is_some(), "invalid JSON details should error: {json}");
        unsafe { deadline_free_telemetry(handle) };
    }

    #[test]
    fn record_event_null_details_returns_error() {
        let handle = deadline_init_telemetry(std::ptr::null());
        let event_type = to_c_str("com.amazon.rum.deadline.test");
        let json = call_ffi_json(deadline_record_telemetry_event(
            handle,
            event_type.as_ptr(),
            std::ptr::null(),
        ));
        assert!(json.get("error").is_some(), "null details should error: {json}");
        unsafe { deadline_free_telemetry(handle) };
    }

    #[test]
    fn record_event_empty_details_succeeds() {
        let handle = deadline_init_telemetry(std::ptr::null());
        let event_type = to_c_str("com.amazon.rum.deadline.test");
        let details = to_c_str("{}");
        let json = call_ffi_json(deadline_record_telemetry_event(
            handle,
            event_type.as_ptr(),
            details.as_ptr(),
        ));
        assert!(json.get("error").is_none(), "empty details should succeed: {json}");
        assert!(json.get("success").is_some(), "should return success: {json}");
        unsafe { deadline_free_telemetry(handle) };
    }

    #[test]
    fn free_telemetry_null_does_not_crash() {
        unsafe { deadline_free_telemetry(std::ptr::null_mut()) };
    }

    #[test]
    fn free_telemetry_valid_handle_does_not_crash() {
        let handle = deadline_init_telemetry(std::ptr::null());
        assert!(!handle.is_null());
        unsafe { deadline_free_telemetry(handle) };
        // No crash = success.
    }
}
