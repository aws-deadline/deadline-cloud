//! C ABI shared library exposing Deadline Cloud business logic
//! to Python GUI widgets and DCC submitter plugins.
//!
//! See docs/specs/deadline-gui-ffi.md for the full API contract.
//!
//! ## Memory ownership
//!
//! All strings returned by `deadline_*` functions are owned by Rust.
//! The caller must free them with `deadline_free_string`.

use std::ffi::{CString, c_char, c_void};

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

/// Return the AWS credentials source as a JSON string.
///
/// Returns `{"credentials_source": "HOST_PROVIDED"}` or similar.
/// If `config_json` is null, uses the default config.
#[unsafe(no_mangle)]
pub extern "C" fn deadline_get_credentials_source(
    _config_json: *const c_char,
) -> *mut c_char {
    let source = deadline_client::auth::get_credentials_source(None);
    let json = serde_json::json!({
        "credentials_source": source.to_string(),
    });
    string_to_ptr(&json.to_string())
}

/// Check authentication status. Returns JSON with credentials_source,
/// auth_status, and api_available fields.
///
/// This creates an internal tokio runtime and blocks — safe to call
/// from a Python worker thread.
#[unsafe(no_mangle)]
pub extern "C" fn deadline_check_auth_status(
    _config_json: *const c_char,
) -> *mut c_char {
    let rt = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(e) => return error_to_ptr(&format!("Failed to create runtime: {e}")),
    };

    let result = rt.block_on(async {
        let source = deadline_client::auth::get_credentials_source(None);
        let status = deadline_client::auth::check_authentication_status(None).await;
        let api_available = deadline_client::auth::check_deadline_api_available(None).await;
        serde_json::json!({
            "credentials_source": source.to_string(),
            "auth_status": status.to_string(),
            "api_available": api_available,
        })
    });

    string_to_ptr(&result.to_string())
}

/// Callback type for status progress messages.
type StatusCallback = extern "C" fn(message: *const c_char, user_data: *mut c_void);

/// Check authentication status with progress callbacks.
///
/// Calls `on_progress` with status messages during the operation.
/// Returns the same JSON as `deadline_check_auth_status`.
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

    let rt = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(e) => return error_to_ptr(&format!("Failed to create runtime: {e}")),
    };

    rt.block_on(async {
        notify("Checking credentials source...");
        let source = deadline_client::auth::get_credentials_source(None);

        notify("Checking authentication status...");
        let status = deadline_client::auth::check_authentication_status(None).await;

        notify("Checking API availability...");
        let api_available = deadline_client::auth::check_deadline_api_available(None).await;

        notify("Done");

        let result = serde_json::json!({
            "credentials_source": source.to_string(),
            "auth_status": status.to_string(),
            "api_available": api_available,
        });
        string_to_ptr(&result.to_string())
    })
}

// ── Helpers ──────────────────────────────────────────────────────

fn string_to_ptr(s: &str) -> *mut c_char {
    CString::new(s).unwrap_or_default().into_raw()
}

fn error_to_ptr(msg: &str) -> *mut c_char {
    let json = serde_json::json!({"error": msg});
    string_to_ptr(&json.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CStr;
    use std::sync::atomic::{AtomicU32, Ordering};

    // ── Sub-task 1: Basic C ABI call ────────────────────────────

    #[test]
    fn get_credentials_source_null_config_returns_valid_json() {
        let result = deadline_get_credentials_source(std::ptr::null());
        assert!(!result.is_null());
        let json_str = unsafe { CStr::from_ptr(result) }.to_str().unwrap();
        let json: serde_json::Value = serde_json::from_str(json_str).unwrap();
        assert!(json.get("credentials_source").is_some());
        unsafe { deadline_free_string(result) };
    }

    #[test]
    fn free_string_null_does_not_crash() {
        unsafe { deadline_free_string(std::ptr::null_mut()) };
    }

    // ── Sub-task 2: Async call ──────────────────────────────────

    #[test]
    fn check_auth_status_null_config_returns_valid_json() {
        let result = deadline_check_auth_status(std::ptr::null());
        assert!(!result.is_null());
        let json_str = unsafe { CStr::from_ptr(result) }.to_str().unwrap();
        let json: serde_json::Value = serde_json::from_str(json_str).unwrap();
        // Must have all three fields
        assert!(json.get("credentials_source").is_some());
        assert!(json.get("auth_status").is_some());
        assert!(json.get("api_available").is_some());
        unsafe { deadline_free_string(result) };
    }

    // ── Sub-task 3: Callback ────────────────────────────────────

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
        // Should have been called at least 3 times (source, status, api, done)
        assert!(CALLBACK_COUNT.load(Ordering::SeqCst) >= 3);
        unsafe { deadline_free_string(result) };
    }

    #[test]
    fn check_auth_status_with_progress_null_callback_does_not_crash() {
        let result = deadline_check_auth_status_with_progress(
            std::ptr::null(),
            None,
            std::ptr::null_mut(),
        );
        assert!(!result.is_null());
        let json_str = unsafe { CStr::from_ptr(result) }.to_str().unwrap();
        let json: serde_json::Value = serde_json::from_str(json_str).unwrap();
        assert!(json.get("credentials_source").is_some());
        unsafe { deadline_free_string(result) };
    }
}
