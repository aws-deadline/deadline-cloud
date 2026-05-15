//! Level 1 tests for the hooks module (Batch B, E, G).
//!
//! Tests hook validation, payload merging, and execution behavior
//! including timeout handling and structured errors.

use deadline_lib::api::errors::DeadlineError;
use deadline_lib::bundle::hooks::{
    HookManager, HookMetadata, merge_asset_references, merge_payload, validate_configuration,
    validate_modified_payload,
};
use deadline_lib::bundle::submission::SubmissionHandler;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::fs;
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Test SubmissionHandler (captures messages)
// ---------------------------------------------------------------------------

struct TestHandler {
    messages: std::sync::Mutex<Vec<String>>,
}

impl TestHandler {
    fn new() -> Self {
        Self {
            messages: std::sync::Mutex::new(Vec::new()),
        }
    }
}

impl SubmissionHandler for TestHandler {
    fn on_message(&self, msg: &str) {
        self.messages.lock().unwrap().push(msg.to_owned());
    }
    fn confirm(&self, _msg: &str, _default: bool) -> bool {
        true
    }
    fn should_continue(&self) -> bool {
        true
    }
    fn on_upload_summary(
        &self,
        _stats: &deadline_lib::attachments::progress_tracker::SummaryStatistics,
    ) {
    }
}

// =====================================================================
// validate_configuration — happy path and error cases
// =====================================================================

#[test]
fn validate_configuration_valid_config_succeeds() {
    let config = json!({
        "version": "1.0",
        "preSubmission": [
            {"command": "echo", "args": ["hello"], "timeout": 30}
        ],
        "postSubmission": [
            {"command": "notify.sh"}
        ]
    });
    assert!(validate_configuration(&config).is_ok());
}

#[test]
fn validate_configuration_empty_hooks_succeeds() {
    let config = json!({"version": "1.0"});
    assert!(validate_configuration(&config).is_ok());
}

#[test]
fn validate_configuration_missing_command_fails() {
    let config = json!({
        "version": "1.0",
        "preSubmission": [{"args": ["hello"]}]
    });
    let err = validate_configuration(&config).unwrap_err();
    assert!(
        err.to_string().contains("missing required 'command' field"),
        "got: {err}"
    );
}

#[test]
fn validate_configuration_invalid_timeout_fails() {
    let config = json!({
        "version": "1.0",
        "preSubmission": [{"command": "echo", "timeout": -5}]
    });
    let err = validate_configuration(&config).unwrap_err();
    assert!(
        err.to_string()
            .contains("'timeout' must be a positive integer"),
        "got: {err}"
    );
}

#[test]
fn validate_configuration_unsupported_version_fails() {
    let config = json!({"version": "2.0", "preSubmission": []});
    let err = validate_configuration(&config).unwrap_err();
    assert!(
        err.to_string().contains("Unsupported hooks version"),
        "got: {err}"
    );
}

#[test]
fn validate_configuration_non_array_hooks_fails() {
    let config = json!({"version": "1.0", "preSubmission": "not an array"});
    let err = validate_configuration(&config).unwrap_err();
    assert!(err.to_string().contains("must be a list"), "got: {err}");
}

#[test]
fn validate_configuration_non_object_hook_fails() {
    let config = json!({"version": "1.0", "preSubmission": ["not an object"]});
    let err = validate_configuration(&config).unwrap_err();
    assert!(err.to_string().contains("must be an object"), "got: {err}");
}

// =====================================================================
// merge_payload — preserves non-asset fields, protects asset references
// =====================================================================

#[test]
fn merge_payload_modified_fields_override_original() {
    let original = json!({"name": "old", "priority": 50});
    let modified = json!({"name": "new", "extra": "added"});
    let result = merge_payload(&original, &modified);
    assert_eq!(result["name"], "new");
    assert_eq!(result["priority"], 50);
    assert_eq!(result["extra"], "added");
}

#[test]
fn merge_payload_asset_references_merged_not_replaced() {
    let original = json!({
        "attachments": {
            "assetReferences": {
                "inputFilenames": ["/a.txt"],
                "outputDirectories": ["/out"]
            },
            "manifests": ["existing"]
        }
    });
    let modified = json!({
        "attachments": {
            "assetReferences": {
                "inputFilenames": ["/b.txt"],
                "referencedPaths": ["/ref"]
            }
        }
    });
    let result = merge_payload(&original, &modified);
    let refs = &result["attachments"]["assetReferences"];
    // Modified fields override
    assert_eq!(refs["inputFilenames"], json!(["/b.txt"]));
    assert_eq!(refs["referencedPaths"], json!(["/ref"]));
    // Original fields preserved
    assert_eq!(refs["outputDirectories"], json!(["/out"]));
    // Non-assetReferences attachment fields preserved from original
    assert_eq!(result["attachments"]["manifests"], json!(["existing"]));
}

#[test]
fn merge_payload_no_attachments_in_modified_passes_through() {
    let original = json!({"name": "job", "attachments": {"manifests": ["m1"]}});
    let modified = json!({"name": "updated"});
    let result = merge_payload(&original, &modified);
    assert_eq!(result["name"], "updated");
    assert_eq!(result["attachments"]["manifests"], json!(["m1"]));
}

// =====================================================================
// merge_asset_references — union behavior
// =====================================================================

#[test]
fn merge_asset_references_union_inputs_outputs() {
    let original = json!({"inputFilenames": ["/a"], "outputDirectories": ["/out"]});
    let modified = json!({"inputFilenames": ["/b"], "referencedPaths": ["/ref"]});
    let result = merge_asset_references(Some(&original), Some(&modified));
    // Modified overrides matching keys
    assert_eq!(result["inputFilenames"], json!(["/b"]));
    assert_eq!(result["referencedPaths"], json!(["/ref"]));
    // Original-only keys preserved
    assert_eq!(result["outputDirectories"], json!(["/out"]));
}

#[test]
fn merge_asset_references_none_original_uses_modified() {
    let modified = json!({"inputFilenames": ["/x"]});
    let result = merge_asset_references(None, Some(&modified));
    assert_eq!(result["inputFilenames"], json!(["/x"]));
}

#[test]
fn merge_asset_references_none_modified_uses_original() {
    let original = json!({"outputDirectories": ["/y"]});
    let result = merge_asset_references(Some(&original), None);
    assert_eq!(result["outputDirectories"], json!(["/y"]));
}

// =====================================================================
// validate_modified_payload
// =====================================================================

#[test]
fn validate_modified_payload_valid_object_succeeds() {
    let payload =
        json!({"name": "job", "attachments": {"assetReferences": {"inputFilenames": []}}});
    assert!(validate_modified_payload(&payload, "test-hook").is_ok());
}

#[test]
fn validate_modified_payload_non_object_fails() {
    let payload = json!("not an object");
    let err = validate_modified_payload(&payload, "test-hook").unwrap_err();
    assert!(
        err.to_string().contains("must be a JSON object"),
        "got: {err}"
    );
}

#[test]
fn validate_modified_payload_invalid_asset_references_fails() {
    let payload = json!({"attachments": {"assetReferences": "not an object"}});
    let err = validate_modified_payload(&payload, "test-hook").unwrap_err();
    assert!(
        err.to_string()
            .contains("'assetReferences' must be an object"),
        "got: {err}"
    );
}

// =====================================================================
// Batch B: execute_hook timeout joins thread (no leak)
// =====================================================================

#[test]
fn execute_hook_timeout_returns_timed_out_result() {
    let tmp = TempDir::new().unwrap();
    let script_path = tmp.path().join("slow.sh");
    fs::write(&script_path, "#!/bin/sh\nexec sleep 60\n").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&script_path, fs::Permissions::from_mode(0o755)).unwrap();
    }

    let handler = TestHandler::new();
    let mut manager = HookManager::new(tmp.path(), &handler);

    // Manually set hooks with a 1-second timeout
    let config = json!({
        "version": "1.0",
        "preSubmission": [{
            "command": "slow.sh",
            "timeout": 1
        }]
    });
    // Write hooks file so load_hooks finds it
    fs::write(
        tmp.path().join("hooks.json"),
        serde_json::to_string(&config).unwrap(),
    )
    .unwrap();
    manager.load_hooks().unwrap();

    let mut metadata = HookMetadata {
        job_name: "test".into(),
        priority: 50,
        farm_id: "farm-123".into(),
        queue_id: "queue-456".into(),
        job_bundle_dir: tmp.path().to_path_buf(),
        parameters: HashMap::new(),
        submitter_name: "test".into(),
        asset_references: json!({}),
        submission_payload: json!({}),
        storage_profile_id: None,
        job_id: None,
    };

    let start = std::time::Instant::now();
    let result = manager.execute_pre_submission_hooks(&mut metadata, json!({"name": "job"}));
    let elapsed = start.elapsed();

    // Must complete within 3s (timeout is 1s + join overhead)
    assert!(
        elapsed.as_secs() < 3,
        "Hook execution took {elapsed:?} — thread likely leaked"
    );
    // Must be an error (pre-submission hooks fail on timeout)
    let err = result.unwrap_err();
    assert!(err.to_string().contains("timed out"), "got: {err}");
}

#[test]
fn execute_hook_success_returns_stdout() {
    let tmp = TempDir::new().unwrap();
    let script_path = tmp.path().join("echo_hook.sh");
    fs::write(&script_path, "#!/bin/sh\necho '{\"name\": \"modified\"}'\n").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&script_path, fs::Permissions::from_mode(0o755)).unwrap();
    }

    let handler = TestHandler::new();
    let mut manager = HookManager::new(tmp.path(), &handler);

    let config = json!({
        "version": "1.0",
        "preSubmission": [{"command": "echo_hook.sh", "timeout": 10}]
    });
    fs::write(
        tmp.path().join("hooks.json"),
        serde_json::to_string(&config).unwrap(),
    )
    .unwrap();
    manager.load_hooks().unwrap();

    let mut metadata = HookMetadata {
        job_name: "test".into(),
        priority: 50,
        farm_id: "farm-123".into(),
        queue_id: "queue-456".into(),
        job_bundle_dir: tmp.path().to_path_buf(),
        parameters: HashMap::new(),
        submitter_name: "test".into(),
        asset_references: json!({}),
        submission_payload: json!({}),
        storage_profile_id: None,
        job_id: None,
    };

    let result = manager
        .execute_pre_submission_hooks(&mut metadata, json!({"name": "original"}))
        .unwrap();
    // Hook output merged into payload
    assert_eq!(result["name"], "modified");
}

#[test]
fn execute_hook_nonzero_exit_reports_failure() {
    let tmp = TempDir::new().unwrap();
    let script_path = tmp.path().join("fail.sh");
    fs::write(&script_path, "#!/bin/sh\nexit 42\n").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&script_path, fs::Permissions::from_mode(0o755)).unwrap();
    }

    let handler = TestHandler::new();
    let mut manager = HookManager::new(tmp.path(), &handler);

    let config = json!({
        "version": "1.0",
        "preSubmission": [{"command": "fail.sh", "timeout": 10}]
    });
    fs::write(
        tmp.path().join("hooks.json"),
        serde_json::to_string(&config).unwrap(),
    )
    .unwrap();
    manager.load_hooks().unwrap();

    let mut metadata = HookMetadata {
        job_name: "test".into(),
        priority: 50,
        farm_id: "farm-123".into(),
        queue_id: "queue-456".into(),
        job_bundle_dir: tmp.path().to_path_buf(),
        parameters: HashMap::new(),
        submitter_name: "test".into(),
        asset_references: json!({}),
        submission_payload: json!({}),
        storage_profile_id: None,
        job_id: None,
    };

    let err = manager
        .execute_pre_submission_hooks(&mut metadata, json!({"name": "job"}))
        .unwrap_err();
    assert!(err.to_string().contains("exit code 42"), "got: {err}");
}

#[test]
fn execute_hook_receives_metadata_on_stdin() {
    let tmp = TempDir::new().unwrap();
    let output_file = tmp.path().join("stdin_capture.txt");
    let script_path = tmp.path().join("capture.sh");
    // Script reads stdin and writes it to a file
    fs::write(
        &script_path,
        format!("#!/bin/sh\ncat > '{}'\n", output_file.display()),
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&script_path, fs::Permissions::from_mode(0o755)).unwrap();
    }

    let handler = TestHandler::new();
    let mut manager = HookManager::new(tmp.path(), &handler);

    let config = json!({
        "version": "1.0",
        "preSubmission": [{"command": "capture.sh", "timeout": 10}]
    });
    fs::write(
        tmp.path().join("hooks.json"),
        serde_json::to_string(&config).unwrap(),
    )
    .unwrap();
    manager.load_hooks().unwrap();

    let mut metadata = HookMetadata {
        job_name: "my-job".into(),
        priority: 75,
        farm_id: "farm-abc".into(),
        queue_id: "queue-xyz".into(),
        job_bundle_dir: tmp.path().to_path_buf(),
        parameters: HashMap::new(),
        submitter_name: "test-submitter".into(),
        asset_references: json!({"inputFilenames": ["/a.txt"]}),
        submission_payload: json!({"name": "my-job"}),
        storage_profile_id: None,
        job_id: None,
    };

    // Hook produces no stdout → payload unchanged
    let _result = manager
        .execute_pre_submission_hooks(&mut metadata, json!({"name": "my-job"}))
        .unwrap();

    // Verify the hook received metadata as JSON on stdin
    let captured = fs::read_to_string(&output_file).unwrap();
    let parsed: Value = serde_json::from_str(&captured).unwrap();
    assert_eq!(parsed["jobName"], "my-job");
    assert_eq!(parsed["farmId"], "farm-abc");
    assert_eq!(parsed["queueId"], "queue-xyz");
    assert_eq!(parsed["priority"], 75);
}

// =====================================================================
// Batch E: Structured HookFailed error variant
// =====================================================================

#[test]
fn execute_hook_failure_returns_hook_failed_variant() {
    let tmp = TempDir::new().unwrap();
    let script_path = tmp.path().join("fail.sh");
    fs::write(&script_path, "#!/bin/sh\nexit 42\n").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&script_path, fs::Permissions::from_mode(0o755)).unwrap();
    }

    let handler = TestHandler::new();
    let mut manager = HookManager::new(tmp.path(), &handler);

    let config = json!({
        "version": "1.0",
        "preSubmission": [{"command": "fail.sh", "timeout": 10}]
    });
    fs::write(
        tmp.path().join("hooks.json"),
        serde_json::to_string(&config).unwrap(),
    )
    .unwrap();
    manager.load_hooks().unwrap();

    let mut metadata = HookMetadata {
        job_name: "test".into(),
        priority: 50,
        farm_id: "farm-123".into(),
        queue_id: "queue-456".into(),
        job_bundle_dir: tmp.path().to_path_buf(),
        parameters: HashMap::new(),
        submitter_name: "test".into(),
        asset_references: json!({}),
        submission_payload: json!({}),
        storage_profile_id: None,
        job_id: None,
    };

    let err = manager
        .execute_pre_submission_hooks(&mut metadata, json!({"name": "job"}))
        .unwrap_err();

    // Must be the structured HookFailed variant, not a generic OperationError
    match err {
        DeadlineError::HookFailed {
            index,
            exit_code,
            timed_out,
            ..
        } => {
            assert_eq!(index, 1);
            assert_eq!(exit_code, 42);
            assert!(!timed_out);
        }
        other => panic!("expected HookFailed variant, got: {other:?}"),
    }
}

#[test]
fn execute_hook_timeout_returns_hook_failed_with_timed_out_flag() {
    let tmp = TempDir::new().unwrap();
    let script_path = tmp.path().join("slow.sh");
    fs::write(&script_path, "#!/bin/sh\nexec sleep 60\n").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&script_path, fs::Permissions::from_mode(0o755)).unwrap();
    }

    let handler = TestHandler::new();
    let mut manager = HookManager::new(tmp.path(), &handler);

    let config = json!({
        "version": "1.0",
        "preSubmission": [{"command": "slow.sh", "timeout": 1}]
    });
    fs::write(
        tmp.path().join("hooks.json"),
        serde_json::to_string(&config).unwrap(),
    )
    .unwrap();
    manager.load_hooks().unwrap();

    let mut metadata = HookMetadata {
        job_name: "test".into(),
        priority: 50,
        farm_id: "farm-123".into(),
        queue_id: "queue-456".into(),
        job_bundle_dir: tmp.path().to_path_buf(),
        parameters: HashMap::new(),
        submitter_name: "test".into(),
        asset_references: json!({}),
        submission_payload: json!({}),
        storage_profile_id: None,
        job_id: None,
    };

    let err = manager
        .execute_pre_submission_hooks(&mut metadata, json!({"name": "job"}))
        .unwrap_err();

    // Must be HookFailed with timed_out = true
    match err {
        DeadlineError::HookFailed {
            index,
            timed_out,
            name,
            ..
        } => {
            assert_eq!(index, 1);
            assert!(timed_out);
            assert!(
                name.contains("slow.sh"),
                "name should contain script: {name}"
            );
        }
        other => panic!("expected HookFailed variant, got: {other:?}"),
    }
}
