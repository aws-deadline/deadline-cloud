//! Level 2 tests for `deadline bundle submit`.
//!
//! Covers the CLI command and key library behaviors reachable through
//! the CLI: config interaction, symlink containment, attachment handling,
//! creation polling, and credential scoping.

use deadline_test_server::TestHarness;
use deadline_test_server::deadline_api::{bundle, jobs, queues, queue_resources, s3, sts, telemetry};
use insta_cmd::assert_cmd_snapshot;
use serde_json::json;
use std::fs;

fn bundle_settings() -> insta::Settings {
    let mut settings = insta::Settings::clone_current();
    // Redact temp directory paths (vary per run)
    settings.add_filter(r"/var/folders/[^\s]+", "[TEMP_PATH]");
    settings.add_filter(r"/tmp/[^\s]+", "[TEMP_PATH]");
    // Redact timing and transfer rate values in summaries
    settings.add_filter(r"[\d.]+ seconds at .*/s", "[TIME] seconds at [RATE]/s");
    settings
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const FARM: &str = "farm-0123456789abcdef0123456789abcdef";
const QUEUE: &str = "queue-0123456789abcdef0123456789abcdef";
const JOB: &str = "job-0123456789abcdef0123456789abcdef";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn setup_config(harness: &TestHarness) {
    harness.cli(&["config", "set", "defaults.farm_id", FARM]).assert().success();
    harness.cli(&["config", "set", "defaults.queue_id", QUEUE]).assert().success();
}

/// Minimal valid YAML template.
fn create_bundle(harness: &TestHarness, name: &str) -> String {
    let dir = harness.config_dir.path().join(name);
    fs::create_dir_all(&dir).unwrap();
    fs::write(dir.join("template.yaml"), "\
specificationVersion: jobtemplate-2023-09
name: TestJob
steps:
  - name: Step1
    script:
      actions:
        onRun:
          command: echo
          args: ['hello']
").unwrap();
    dir.to_str().unwrap().to_string()
}

/// Bundle with a STRING parameter.
fn create_bundle_with_params(harness: &TestHarness, name: &str) -> String {
    let dir = harness.config_dir.path().join(name);
    fs::create_dir_all(&dir).unwrap();
    fs::write(dir.join("template.yaml"), "\
specificationVersion: jobtemplate-2023-09
name: ParamJob
parameterDefinitions:
  - name: Frames
    type: STRING
    default: '1-10'
steps:
  - name: Step1
    script:
      actions:
        onRun:
          command: echo
          args: ['{{Param.Frames}}']
").unwrap();
    dir.to_str().unwrap().to_string()
}

/// Bundle with a JSON template.
fn create_json_bundle(harness: &TestHarness, name: &str) -> String {
    let dir = harness.config_dir.path().join(name);
    fs::create_dir_all(&dir).unwrap();
    fs::write(dir.join("template.json"), serde_json::to_string_pretty(&json!({
        "specificationVersion": "jobtemplate-2023-09",
        "name": "JsonJob",
        "steps": [{"name": "Step1", "script": {"actions": {"onRun": {"command": "echo", "args": ["hello"]}}}}]
    })).unwrap()).unwrap();
    dir.to_str().unwrap().to_string()
}

/// Bundle with input files for attachment testing.
fn create_bundle_with_attachments(harness: &TestHarness, name: &str) -> String {
    let dir = harness.config_dir.path().join(name);
    fs::create_dir_all(&dir).unwrap();
    // Create an input file
    let input_dir = dir.join("inputs");
    fs::create_dir_all(&input_dir).unwrap();
    fs::write(input_dir.join("data.txt"), "test file content").unwrap();
    // Template
    fs::write(dir.join("template.yaml"), "\
specificationVersion: jobtemplate-2023-09
name: AttachmentJob
steps:
  - name: Step1
    script:
      actions:
        onRun:
          command: echo
          args: ['hello']
").unwrap();
    // Asset references pointing to the input dir
    fs::write(dir.join("asset_references.yaml"), format!("\
assetReferences:
  inputs:
    directories:
      - {input_dir}
    filenames: []
  outputs:
    directories: []
  referencedPaths: []
", input_dir = input_dir.display())).unwrap();
    dir.to_str().unwrap().to_string()
}

/// Bundle with a symlink that escapes the bundle directory.
fn create_bundle_with_escaping_symlink(harness: &TestHarness, name: &str) -> String {
    let dir = harness.config_dir.path().join(name);
    fs::create_dir_all(&dir).unwrap();
    fs::write(dir.join("template.yaml"), "\
specificationVersion: jobtemplate-2023-09
name: SymlinkJob
steps:
  - name: Step1
    script:
      actions:
        onRun:
          command: echo
").unwrap();
    // Create a symlink pointing outside the bundle
    #[cfg(unix)]
    std::os::unix::fs::symlink("/etc/hosts", dir.join("escape_link")).unwrap();
    dir.to_str().unwrap().to_string()
}

fn queue_role_credentials() -> serde_json::Value {
    json!({
        "credentials": {
            "accessKeyId": "ASIAQUEUESCOPED",
            "secretAccessKey": "queuesecret",
            "sessionToken": "queuetoken",
            "expiration": "2026-12-18T01:30:00Z"
        }
    })
}

/// Mock the standard APIs for a no-attachment submission.
async fn mock_submit_no_attachments(harness: &TestHarness) {
    telemetry::mock_telemetry_endpoint(&harness.server).await;
    queues::mock_get_queue(&harness.server, FARM, json!({
        "queueId": QUEUE,
        "displayName": "Test Queue",
    })).await;
    queue_resources::mock_list_queue_environments(
        &harness.server, FARM, QUEUE, &[],
    ).await;
    bundle::mock_create_job(&harness.server, FARM, QUEUE, JOB).await;
    jobs::mock_get_job(&harness.server, FARM, QUEUE, json!({
        "jobId": JOB,
        "lifecycleStatus": "CREATE_COMPLETE",
        "lifecycleStatusMessage": "Job created successfully",
    })).await;
}

/// Mock APIs for a submission with attachments (queue has jobAttachmentSettings).
async fn mock_submit_with_attachments(harness: &TestHarness) {
    telemetry::mock_telemetry_endpoint(&harness.server).await;
    queues::mock_get_queue(&harness.server, FARM, json!({
        "queueId": QUEUE,
        "displayName": "Test Queue",
        "jobAttachmentSettings": {
            "s3BucketName": "test-bucket",
            "rootPrefix": "Data",
        },
    })).await;
    queue_resources::mock_list_queue_environments(
        &harness.server, FARM, QUEUE, &[],
    ).await;
    queue_resources::mock_assume_queue_role_for_user(
        &harness.server, FARM, QUEUE, queue_role_credentials(),
    ).await;
    sts::mock_get_caller_identity(&harness.server).await;
    s3::mock_s3_head_not_found(&harness.server).await;
    s3::mock_s3_put_success(&harness.server).await;
    bundle::mock_create_job(&harness.server, FARM, QUEUE, JOB).await;
    jobs::mock_get_job(&harness.server, FARM, QUEUE, json!({
        "jobId": JOB,
        "lifecycleStatus": "CREATE_COMPLETE",
        "lifecycleStatusMessage": "Job created successfully",
    })).await;
}

// =====================================================================
// valid job bundle, no attachments — prints job ID
// =====================================================================

#[tokio::test]
async fn bundle_submit_valid_bundle_prints_job_id() {
    let harness = TestHarness::new().await;
    setup_config(&harness);
    mock_submit_no_attachments(&harness).await;
    let bundle_dir = create_bundle(&harness, "case1");
    let _guard = bundle_settings().bind_to_scope();
    assert_cmd_snapshot!(harness.cmd(&["bundle", "submit", &bundle_dir, "--yes"]));
}

// =====================================================================
// --parameter key=value
// =====================================================================

#[tokio::test]
async fn bundle_submit_with_parameter_override() {
    let harness = TestHarness::new().await;
    setup_config(&harness);
    mock_submit_no_attachments(&harness).await;
    let bundle_dir = create_bundle_with_params(&harness, "case2");
    let _guard = bundle_settings().bind_to_scope();
    assert_cmd_snapshot!(harness.cmd(&[
        "bundle", "submit", &bundle_dir, "--yes",
        "--parameter", "Frames=20-30",
    ]));
}

// =====================================================================
// --name overrides job name
// =====================================================================

#[tokio::test]
async fn bundle_submit_with_name_override() {
    let harness = TestHarness::new().await;
    setup_config(&harness);
    mock_submit_no_attachments(&harness).await;
    let bundle_dir = create_bundle(&harness, "case4");
    let _guard = bundle_settings().bind_to_scope();
    assert_cmd_snapshot!(harness.cmd(&[
        "bundle", "submit", &bundle_dir, "--yes",
        "--name", "Custom Job Name",
    ]));
}

// =====================================================================
// --priority
// =====================================================================

#[tokio::test]
async fn bundle_submit_with_priority() {
    let harness = TestHarness::new().await;
    setup_config(&harness);
    mock_submit_no_attachments(&harness).await;
    let bundle_dir = create_bundle(&harness, "case5");
    let _guard = bundle_settings().bind_to_scope();
    assert_cmd_snapshot!(harness.cmd(&[
        "bundle", "submit", &bundle_dir, "--yes", "--priority", "75",
    ]));
}

// =====================================================================
// --max-failed-tasks-count
// =====================================================================

#[tokio::test]
async fn bundle_submit_with_max_failed_tasks() {
    let harness = TestHarness::new().await;
    setup_config(&harness);
    mock_submit_no_attachments(&harness).await;
    let bundle_dir = create_bundle(&harness, "case6");
    let _guard = bundle_settings().bind_to_scope();
    assert_cmd_snapshot!(harness.cmd(&[
        "bundle", "submit", &bundle_dir, "--yes", "--max-failed-tasks-count", "10",
    ]));
}

// =====================================================================
// --max-retries-per-task
// =====================================================================

#[tokio::test]
async fn bundle_submit_with_max_retries() {
    let harness = TestHarness::new().await;
    setup_config(&harness);
    mock_submit_no_attachments(&harness).await;
    let bundle_dir = create_bundle(&harness, "case7");
    let _guard = bundle_settings().bind_to_scope();
    assert_cmd_snapshot!(harness.cmd(&[
        "bundle", "submit", &bundle_dir, "--yes", "--max-retries-per-task", "3",
    ]));
}

// =====================================================================
// --yes skips confirmation (no prompt, succeeds)
// Tested implicitly by all happy-path tests above. This test verifies
// that WITHOUT --yes the command still works (auto_accept from config).
// =====================================================================

#[tokio::test]
async fn bundle_submit_auto_accept_from_config() {
    let harness = TestHarness::new().await;
    setup_config(&harness);
    harness.cli(&["config", "set", "settings.auto_accept", "true"]).assert().success();
    mock_submit_no_attachments(&harness).await;
    let bundle_dir = create_bundle(&harness, "case8");
    // No --yes flag, but auto_accept is set in config
    let _guard = bundle_settings().bind_to_scope();
    assert_cmd_snapshot!(harness.cmd(&["bundle", "submit", &bundle_dir]));
}

// =====================================================================
// bundle directory does not exist
// =====================================================================

#[tokio::test]
async fn bundle_submit_nonexistent_dir_exits_with_error() {
    let harness = TestHarness::new().await;
    setup_config(&harness);
    let _guard = bundle_settings().bind_to_scope();
    assert_cmd_snapshot!(harness.cmd(&[
        "bundle", "submit", "/nonexistent/path/to/bundle",
    ]));
}

// =====================================================================
// bundle missing required template file
// =====================================================================

#[tokio::test]
async fn bundle_submit_missing_template_exits_with_error() {
    let harness = TestHarness::new().await;
    setup_config(&harness);
    let dir = harness.config_dir.path().join("empty_bundle");
    fs::create_dir_all(&dir).unwrap();
    let _guard = bundle_settings().bind_to_scope();
    assert_cmd_snapshot!(harness.cmd(&["bundle", "submit", dir.to_str().unwrap()]));
}

// =====================================================================
// bundle with job attachments — files uploaded
// =====================================================================

#[tokio::test]
async fn bundle_submit_with_attachments_uploads_files() {
    let harness = TestHarness::new().await;
    setup_config(&harness);
    mock_submit_with_attachments(&harness).await;
    let bundle_dir = create_bundle_with_attachments(&harness, "case12");
    // Set known_asset_paths so safety check doesn't cancel
    let temp_root = harness.config_dir.path().to_string_lossy().to_string();
    harness.cli(&["config", "set", "settings.known_asset_paths", &temp_root]).assert().success();
    let _guard = bundle_settings().bind_to_scope();
    assert_cmd_snapshot!(harness.cmd(&[
        "bundle", "submit", &bundle_dir, "--yes",
    ]));
}

// =====================================================================
// API call fails (CreateJob returns error)
// =====================================================================

#[tokio::test]
async fn bundle_submit_api_error_exits_with_error() {
    let harness = TestHarness::new().await;
    setup_config(&harness);
    telemetry::mock_telemetry_endpoint(&harness.server).await;
    queues::mock_get_queue(&harness.server, FARM, json!({
        "queueId": QUEUE, "displayName": "Test Queue",
    })).await;
    queue_resources::mock_list_queue_environments(
        &harness.server, FARM, QUEUE, &[],
    ).await;
    bundle::mock_create_job_error(
        &harness.server, FARM, QUEUE, 403, "AccessDeniedException",
    ).await;
    let bundle_dir = create_bundle(&harness, "case13");
    let _guard = bundle_settings().bind_to_scope();
    assert_cmd_snapshot!(harness.cmd(&["bundle", "submit", &bundle_dir, "--yes"]));
}

// bundle submit API error should show resource suggestions
#[tokio::test]
async fn bundle_submit_access_denied_suggests_available_queues() {
    let harness = TestHarness::new().await;
    setup_config(&harness);
    telemetry::mock_telemetry_endpoint(&harness.server).await;
    queues::mock_get_queue(&harness.server, FARM, json!({
        "queueId": QUEUE, "displayName": "Test Queue",
    })).await;
    queue_resources::mock_list_queue_environments(
        &harness.server, FARM, QUEUE, &[],
    ).await;
    bundle::mock_create_job_error(
        &harness.server, FARM, QUEUE, 403, "AccessDeniedException",
    ).await;
    // Mount queue list for suggestion chain
    queues::mock_list_queues(
        &harness.server, FARM,
        &[json!({"queueId": "queue-real", "displayName": "Real Queue"})],
    ).await;
    let bundle_dir = create_bundle(&harness, "suggest_case");
    let _guard = bundle_settings().bind_to_scope();
    assert_cmd_snapshot!(harness.cmd(&["bundle", "submit", &bundle_dir, "--yes"]));
}

// =====================================================================
// JSON template variant
// =====================================================================

#[tokio::test]
async fn bundle_submit_json_template() {
    let harness = TestHarness::new().await;
    setup_config(&harness);
    mock_submit_no_attachments(&harness).await;
    let bundle_dir = create_json_bundle(&harness, "json_tmpl");
    let _guard = bundle_settings().bind_to_scope();
    assert_cmd_snapshot!(harness.cmd(&["bundle", "submit", &bundle_dir, "--yes"]));
}

// =====================================================================
// symlink containment — symlink escapes bundle
// =====================================================================

#[cfg(unix)]
#[tokio::test]
async fn bundle_submit_symlink_outside_bundle_exits_with_error() {
    let harness = TestHarness::new().await;
    setup_config(&harness);
    let bundle_dir = create_bundle_with_escaping_symlink(&harness, "symlink_escape");
    let _guard = bundle_settings().bind_to_scope();
    assert_cmd_snapshot!(harness.cmd(&["bundle", "submit", &bundle_dir, "--yes"]));
}

// =====================================================================
// Missing required config: no farm_id
// =====================================================================

#[tokio::test]
async fn bundle_submit_missing_farm_id_exits_with_error() {
    let harness = TestHarness::new().await;
    // No farm_id or queue_id configured
    let bundle_dir = create_bundle(&harness, "no_farm");
    let _guard = bundle_settings().bind_to_scope();
    assert_cmd_snapshot!(harness.cmd(&["bundle", "submit", &bundle_dir]));
}

// =====================================================================
// storage_profile_id set in config — included in CreateJob
// =====================================================================

#[tokio::test]
async fn bundle_submit_with_storage_profile_fetches_profile() {
    let harness = TestHarness::new().await;
    setup_config(&harness);
    harness.cli(&["config", "set", "settings.storage_profile_id", "sp-abc123"]).assert().success();
    telemetry::mock_telemetry_endpoint(&harness.server).await;
    queues::mock_get_queue(&harness.server, FARM, json!({
        "queueId": QUEUE, "displayName": "Test Queue",
    })).await;
    queue_resources::mock_list_queue_environments(
        &harness.server, FARM, QUEUE, &[],
    ).await;
    queue_resources::mock_get_storage_profile_for_queue(
        &harness.server, FARM, QUEUE, "sp-abc123",
        json!({
            "storageProfileId": "sp-abc123",
            "displayName": "My Storage Profile",
            "osFamily": "LINUX",
            "fileSystemLocations": [],
        }),
    ).await;
    bundle::mock_create_job(&harness.server, FARM, QUEUE, JOB).await;
    jobs::mock_get_job(&harness.server, FARM, QUEUE, json!({
        "jobId": JOB,
        "lifecycleStatus": "CREATE_COMPLETE",
        "lifecycleStatusMessage": "Job created successfully",
    })).await;
    let bundle_dir = create_bundle(&harness, "storage_profile");
    let _guard = bundle_settings().bind_to_scope();
    assert_cmd_snapshot!(harness.cmd(&["bundle", "submit", &bundle_dir, "--yes"]));
}

// =====================================================================
// defaults.job_id updated after successful submission
// (only when no --profile/--farm-id/--queue-id/--storage-profile-id overrides)
// =====================================================================

#[tokio::test]
async fn bundle_submit_updates_default_job_id() {
    let harness = TestHarness::new().await;
    setup_config(&harness);
    mock_submit_no_attachments(&harness).await;
    let bundle_dir = create_bundle(&harness, "job_id_update");

    harness.cli(&["bundle", "submit", &bundle_dir, "--yes"]).assert().success();

    // Verify defaults.job_id was updated
    let output = harness.cli(&["config", "get", "defaults.job_id"])
        .output().expect("failed to run");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains(JOB),
        "Expected defaults.job_id to be set to {JOB}, got: {stdout}"
    );
}

// =====================================================================
// defaults.job_id NOT updated when --farm-id override used
// =====================================================================

#[tokio::test]
async fn bundle_submit_with_farm_override_does_not_update_default_job_id() {
    let harness = TestHarness::new().await;
    setup_config(&harness);
    mock_submit_no_attachments(&harness).await;
    let bundle_dir = create_bundle(&harness, "no_job_id_update");

    harness.cli(&[
        "bundle", "submit", &bundle_dir, "--yes", "--farm-id", FARM,
    ]).assert().success();

    // defaults.job_id should NOT be updated
    let output = harness.cli(&["config", "get", "defaults.job_id"])
        .output().expect("failed to run");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        !stdout.contains(JOB),
        "Expected defaults.job_id to NOT be set, got: {stdout}"
    );
}

// =====================================================================
// CreateJob returns CREATE_FAILED
// =====================================================================

#[tokio::test]
async fn bundle_submit_create_failed_exits_with_error() {
    let harness = TestHarness::new().await;
    setup_config(&harness);
    telemetry::mock_telemetry_endpoint(&harness.server).await;
    queues::mock_get_queue(&harness.server, FARM, json!({
        "queueId": QUEUE, "displayName": "Test Queue",
    })).await;
    queue_resources::mock_list_queue_environments(
        &harness.server, FARM, QUEUE, &[],
    ).await;
    bundle::mock_create_job(&harness.server, FARM, QUEUE, JOB).await;
    // GetJob returns CREATE_FAILED
    jobs::mock_get_job(&harness.server, FARM, QUEUE, json!({
        "jobId": JOB,
        "lifecycleStatus": "CREATE_FAILED",
        "lifecycleStatusMessage": "Template validation failed",
    })).await;
    let bundle_dir = create_bundle(&harness, "create_failed");
    let _guard = bundle_settings().bind_to_scope();
    assert_cmd_snapshot!(harness.cmd(&["bundle", "submit", &bundle_dir, "--yes"]));
}

// =====================================================================
// queue has no jobAttachmentSettings, bundle has asset refs
// — no upload, submission proceeds
// =====================================================================

#[tokio::test]
async fn bundle_submit_no_attachment_settings_skips_upload() {
    let harness = TestHarness::new().await;
    setup_config(&harness);
    mock_submit_no_attachments(&harness).await;
    // Queue has no jobAttachmentSettings (already the case in mock_submit_no_attachments)
    let bundle_dir = create_bundle_with_attachments(&harness, "no_attach_settings");
    let _guard = bundle_settings().bind_to_scope();
    assert_cmd_snapshot!(harness.cmd(&["bundle", "submit", &bundle_dir, "--yes"]));
}

// =====================================================================
// input directory does not exist, require_paths_exist=true
// =====================================================================

#[tokio::test]
async fn bundle_submit_missing_input_dir_require_paths_exits_with_error() {
    let harness = TestHarness::new().await;
    setup_config(&harness);
    mock_submit_with_attachments(&harness).await;

    let dir = harness.config_dir.path().join("missing_input");
    fs::create_dir_all(&dir).unwrap();
    fs::write(dir.join("template.yaml"), "\
specificationVersion: jobtemplate-2023-09
name: MissingInputJob
steps:
  - name: Step1
    script:
      actions:
        onRun:
          command: echo
").unwrap();
    fs::write(dir.join("asset_references.yaml"), "\
assetReferences:
  inputs:
    directories:
      - /nonexistent/input/dir
    filenames: []
  outputs:
    directories: []
  referencedPaths: []
").unwrap();

    let _guard = bundle_settings().bind_to_scope();
    assert_cmd_snapshot!(harness.cmd(&[
        "bundle", "submit", dir.to_str().unwrap(),
        "--yes", "--require-paths-exist",
    ]));
}

// =====================================================================
// --target-task-run-status SUSPENDED
// =====================================================================

#[tokio::test]
async fn bundle_submit_target_task_run_status_suspended() {
    let harness = TestHarness::new().await;
    setup_config(&harness);
    mock_submit_no_attachments(&harness).await;
    let bundle_dir = create_bundle(&harness, "suspended");
    let _guard = bundle_settings().bind_to_scope();
    assert_cmd_snapshot!(harness.cmd(&[
        "bundle", "submit", &bundle_dir, "--yes",
        "--target-task-run-status", "SUSPENDED",
    ]));
}

// =====================================================================
// --max-worker-count
// =====================================================================

#[tokio::test]
async fn bundle_submit_with_max_worker_count() {
    let harness = TestHarness::new().await;
    setup_config(&harness);
    mock_submit_no_attachments(&harness).await;
    let bundle_dir = create_bundle(&harness, "max_workers");
    let _guard = bundle_settings().bind_to_scope();
    assert_cmd_snapshot!(harness.cmd(&[
        "bundle", "submit", &bundle_dir, "--yes", "--max-worker-count", "5",
    ]));
}

// =====================================================================
// --submitter-name
// =====================================================================

#[tokio::test]
async fn bundle_submit_with_submitter_name() {
    let harness = TestHarness::new().await;
    setup_config(&harness);
    mock_submit_no_attachments(&harness).await;
    let bundle_dir = create_bundle(&harness, "submitter");
    let _guard = bundle_settings().bind_to_scope();
    assert_cmd_snapshot!(harness.cmd(&[
        "bundle", "submit", &bundle_dir, "--yes", "--submitter-name", "MyApp",
    ]));
}

// =====================================================================
// Both template.yaml and template.json present — error
// =====================================================================

#[tokio::test]
async fn bundle_submit_both_templates_exits_with_error() {
    let harness = TestHarness::new().await;
    setup_config(&harness);
    let dir = harness.config_dir.path().join("both_templates");
    fs::create_dir_all(&dir).unwrap();
    fs::write(dir.join("template.yaml"), "specificationVersion: jobtemplate-2023-09\nname: Y\nsteps: []").unwrap();
    fs::write(dir.join("template.json"), r#"{"specificationVersion":"jobtemplate-2023-09","name":"J","steps":[]}"#).unwrap();
    let _guard = bundle_settings().bind_to_scope();
    assert_cmd_snapshot!(harness.cmd(&["bundle", "submit", dir.to_str().unwrap(), "--yes"]));
}

// =====================================================================
// Invalid --parameter format
// =====================================================================

#[tokio::test]
async fn bundle_submit_invalid_parameter_format_exits_with_error() {
    let harness = TestHarness::new().await;
    setup_config(&harness);
    let bundle_dir = create_bundle(&harness, "bad_param");
    let _guard = bundle_settings().bind_to_scope();
    assert_cmd_snapshot!(harness.cmd(&[
        "bundle", "submit", &bundle_dir, "--yes",
        "--parameter", "not-a-valid-format",
    ]));
}

// =====================================================================
// CLI flag overrides: --farm-id, --queue-id, --profile
// =====================================================================

#[tokio::test]
async fn bundle_submit_with_cli_overrides() {
    let harness = TestHarness::new().await;
    // Don't set config — use CLI flags instead
    mock_submit_no_attachments(&harness).await;
    let bundle_dir = create_bundle(&harness, "cli_overrides");
    let _guard = bundle_settings().bind_to_scope();
    assert_cmd_snapshot!(harness.cmd(&[
        "bundle", "submit", &bundle_dir, "--yes",
        "--farm-id", FARM, "--queue-id", QUEUE,
    ]));
}

// =====================================================================
// files outside known paths should produce a warning
// =====================================================================

// When --yes (auto_accept) is set and files are outside known
// paths, Python cancels the submission as a safety measure. Rust must match.
#[tokio::test]
async fn bundle_submit_auto_accept_unknown_paths_cancels() {
    let harness = TestHarness::new().await;
    setup_config(&harness);
    mock_submit_with_attachments(&harness).await;
    // Input files are in a temp dir which is NOT a known asset path.
    let bundle_dir = create_bundle_with_attachments(&harness, "known_paths_warn");
    let _guard = bundle_settings().bind_to_scope();
    let output = harness.cmd(&["bundle", "submit", &bundle_dir, "--yes"])
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        !output.status.success(),
        "Expected failure when auto_accept + unknown paths, got success:\n{stdout}"
    );
    assert!(
        stdout.contains("auto_accept") || stdout.contains("unknown paths") || stdout.contains("canceled"),
        "Expected cancellation message mentioning auto_accept/unknown paths, got:\n{stdout}"
    );
}

// =====================================================================
// CREATE_FAILED error should include job ID
// =====================================================================

#[tokio::test]
async fn bundle_submit_create_failed_includes_job_id() {
    let harness = TestHarness::new().await;
    setup_config(&harness);
    telemetry::mock_telemetry_endpoint(&harness.server).await;
    queues::mock_get_queue(&harness.server, FARM, json!({
        "queueId": QUEUE, "displayName": "Test Queue",
    })).await;
    queue_resources::mock_list_queue_environments(
        &harness.server, FARM, QUEUE, &[],
    ).await;
    bundle::mock_create_job(&harness.server, FARM, QUEUE, JOB).await;
    jobs::mock_get_job(&harness.server, FARM, QUEUE, json!({
        "jobId": JOB,
        "lifecycleStatus": "CREATE_FAILED",
        "lifecycleStatusMessage": "Template validation failed",
    })).await;
    let bundle_dir = create_bundle(&harness, "create_failed_jobid");
    let _guard = bundle_settings().bind_to_scope();
    let output = harness.cmd(&["bundle", "submit", &bundle_dir, "--yes"])
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains(JOB),
        "Expected job ID '{JOB}' in CREATE_FAILED error, got:\n{stdout}"
    );
}

// =====================================================================
// --job-attachments-file-system rejects invalid values
// =====================================================================

#[tokio::test]
async fn bundle_submit_invalid_file_system_value_exits_with_error() {
    let harness = TestHarness::new().await;
    setup_config(&harness);
    let bundle_dir = create_bundle(&harness, "bad_fs");
    let _guard = bundle_settings().bind_to_scope();
    let output = harness.cmd(&[
        "bundle", "submit", &bundle_dir, "--yes",
        "--job-attachments-file-system", "INVALID",
    ]).output().unwrap();
    let all_output = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    assert!(
        !output.status.success(),
        "Expected error for invalid --job-attachments-file-system, but command succeeded"
    );
    assert!(
        all_output.contains("invalid value 'INVALID'") || all_output.contains("COPIED"),
        "Expected clap validation error mentioning valid values, got:\n{all_output}"
    );
}

// =====================================================================
// invalid parameter name (starts with digit) exits with error
// =====================================================================

#[tokio::test]
async fn bundle_submit_invalid_parameter_name_exits_with_error() {
    let harness = TestHarness::new().await;
    setup_config(&harness);
    let bundle_dir = create_bundle(&harness, "bad_param_name");
    let _guard = bundle_settings().bind_to_scope();
    assert_cmd_snapshot!(harness.cmd(&[
        "bundle", "submit", &bundle_dir, "--yes",
        "--parameter", "123Invalid=value",
    ]));
}
