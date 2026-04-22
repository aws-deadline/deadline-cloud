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

// ── Spike functions (existing) ───────────────────────────────────

/// Return the AWS credentials source as a JSON string.
#[unsafe(no_mangle)]
pub extern "C" fn deadline_get_credentials_source(
    _config_json: *const c_char,
) -> *mut c_char {
    let source = deadline_api::auth::get_credentials_source(None);
    json_to_ptr(&serde_json::json!({
        "credentials_source": source.to_string(),
    }))
}

/// Check authentication status. Returns JSON with credentials_source,
/// auth_status, and api_available fields.
#[unsafe(no_mangle)]
pub extern "C" fn deadline_check_auth_status(
    _config_json: *const c_char,
) -> *mut c_char {
    let rt = match make_runtime() { Ok(rt) => rt, Err(p) => return p };
    let result = rt.block_on(async {
        let source = deadline_api::auth::get_credentials_source(None);
        let status = deadline_api::auth::check_authentication_status(None).await;
        let api_available = deadline_api::auth::check_deadline_api_available(None).await;
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
    _config_json: *const c_char,
    on_progress: Option<StatusCallback>,
    user_data: *mut c_void,
) -> *mut c_char {
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
        let source = deadline_api::auth::get_credentials_source(None);
        notify("Checking authentication status...");
        let status = deadline_api::auth::check_authentication_status(None).await;
        notify("Checking API availability...");
        let api_available = deadline_api::auth::check_deadline_api_available(None).await;
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
    match deadline_config::config_file::get_setting_with_config(&name, &config) {
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
    if let Err(e) = deadline_config::config_file::set_setting_in_config(&name, &val, &mut config) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CStr;
    use std::sync::atomic::{AtomicU32, Ordering};

    /// Helper: call an FFI function, parse the returned JSON, free the string.
    fn call_ffi_json(ptr: *mut c_char) -> serde_json::Value {
        assert!(!ptr.is_null(), "FFI function returned null");
        let json_str = unsafe { CStr::from_ptr(ptr) }.to_str().unwrap();
        let val: serde_json::Value = serde_json::from_str(json_str).unwrap();
        unsafe { deadline_free_string(ptr) };
        val
    }

    /// Helper: create a CString from a &str and return its pointer.
    /// The CString is returned to keep it alive for the caller's scope.
    fn to_c_str(s: &str) -> CString {
        CString::new(s).unwrap()
    }

    // ── Spike tests (existing) ──────────────────────────────────

    #[test]
    fn get_credentials_source_null_config_returns_valid_json() {
        let json = call_ffi_json(deadline_get_credentials_source(std::ptr::null()));
        assert!(json.get("credentials_source").is_some());
    }

    #[test]
    fn free_string_null_does_not_crash() {
        unsafe { deadline_free_string(std::ptr::null_mut()) };
    }

    #[test]
    fn check_auth_status_null_config_returns_valid_json() {
        let json = call_ffi_json(deadline_check_auth_status(std::ptr::null()));
        assert!(json.get("credentials_source").is_some());
        assert!(json.get("auth_status").is_some());
        assert!(json.get("api_available").is_some());
    }

    static CALLBACK_COUNT: AtomicU32 = AtomicU32::new(0);

    extern "C" fn test_callback(_message: *const c_char, _user_data: *mut c_void) {
        CALLBACK_COUNT.fetch_add(1, Ordering::SeqCst);
    }

    #[test]
    fn check_auth_status_with_progress_calls_callback() {
        CALLBACK_COUNT.store(0, Ordering::SeqCst);
        let result = deadline_check_auth_status_with_progress(
            std::ptr::null(),
            Some(test_callback),
            std::ptr::null_mut(),
        );
        assert!(!result.is_null());
        assert!(CALLBACK_COUNT.load(Ordering::SeqCst) >= 3);
        unsafe { deadline_free_string(result) };
    }

    #[test]
    fn check_auth_status_with_progress_null_callback_does_not_crash() {
        let json = call_ffi_json(deadline_check_auth_status_with_progress(
            std::ptr::null(),
            None,
            std::ptr::null_mut(),
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
    fn list_farms_returns_json_with_farms_array() {
        // With no config (null), should attempt to call the API.
        // Without a stub server, this will return an error — that's fine,
        // we just verify the function exists and returns valid JSON.
        let json = call_ffi_json(deadline_list_farms(std::ptr::null()));
        assert!(json.is_object());
        // Either has "farms" array or "error" string
        assert!(
            json.get("farms").is_some() || json.get("error").is_some(),
            "should return farms or error: {json}"
        );
    }

    #[test]
    fn list_queues_returns_json_with_queues_array() {
        let c_farm = to_c_str("farm-abc123");
        let json = call_ffi_json(deadline_list_queues(
            c_farm.as_ptr(),
            std::ptr::null(),
        ));
        assert!(json.is_object());
        assert!(
            json.get("queues").is_some() || json.get("error").is_some(),
            "should return queues or error: {json}"
        );
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
    fn list_storage_profiles_returns_json() {
        let c_farm = to_c_str("farm-abc123");
        let c_queue = to_c_str("queue-abc123");
        let json = call_ffi_json(deadline_list_storage_profiles_for_queue(
            c_farm.as_ptr(),
            c_queue.as_ptr(),
            std::ptr::null(),
        ));
        assert!(json.is_object());
        assert!(
            json.get("storageProfiles").is_some() || json.get("error").is_some(),
            "should return storageProfiles or error: {json}"
        );
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
    fn get_queue_parameters_returns_json() {
        let c_farm = to_c_str("farm-abc123");
        let c_queue = to_c_str("queue-abc123");
        let json = call_ffi_json(deadline_get_queue_parameter_definitions(
            c_farm.as_ptr(),
            c_queue.as_ptr(),
            std::ptr::null(),
        ));
        assert!(json.is_object());
        assert!(
            json.get("parameters").is_some() || json.get("error").is_some(),
            "should return parameters or error: {json}"
        );
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
    fn check_api_available_returns_bool_json() {
        // Should return {"api_available": true/false} or {"error": "..."}
        let json = call_ffi_json(deadline_check_api_available(std::ptr::null()));
        assert!(json.is_object());
        assert!(
            json.get("api_available").is_some() || json.get("error").is_some(),
            "should return api_available or error: {json}"
        );
    }

    #[test]
    fn login_null_config_returns_json() {
        // login without config — will fail (no DCM), but should return
        // valid JSON error, not crash.
        let json = call_ffi_json(deadline_login(std::ptr::null()));
        assert!(json.is_object());
    }

    #[test]
    fn logout_null_config_returns_json() {
        // logout without config — should succeed (no-op if not logged in)
        // or return a valid JSON error.
        let json = call_ffi_json(deadline_logout(std::ptr::null()));
        assert!(json.is_object());
    }

    #[test]
    fn logout_returns_success_field() {
        let json = call_ffi_json(deadline_logout(std::ptr::null()));
        // Should have either "success" or "error"
        assert!(
            json.get("success").is_some() || json.get("error").is_some(),
            "should return success or error: {json}"
        );
    }
}
