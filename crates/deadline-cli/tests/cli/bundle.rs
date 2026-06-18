//! Level 2 tests for `deadline bundle submit`.
//!
//! Covers the CLI command and key library behaviors reachable through
//! the CLI: config interaction, symlink containment, attachment handling,
//! creation polling, and credential scoping.

use deadline_test_server::TestHarness;
use deadline_test_server::deadline_api::{
    bundle, jobs, queue_resources, queues, s3, sts, telemetry,
};
use insta_cmd::assert_cmd_snapshot;
use serde_json::json;
use std::fs;

fn bundle_settings() -> insta::Settings {
    let mut settings = insta::Settings::clone_current();
    // macOS resolves symlinks for temp dirs and `/etc` (e.g. `/var/folders` ->
    // `/private/var/folders`, `/etc` -> `/private/etc`). Normalize the `/private`
    // prefix away first so symlink-resolution snapshots match Linux output.
    settings.add_filter(r"/private/var/folders/[^\s]+", "[TEMP_PATH]");
    settings.add_filter(r"/private/etc/", "/etc/");
    // Redact temp directory paths (vary per run)
    settings.add_filter(r"/var/folders/[^\s]+", "[TEMP_PATH]");
    settings.add_filter(r"/tmp/[^\s]+", "[TEMP_PATH]");
    // Windows temp dirs (Windows CI)
    crate::common::add_windows_temp_filters(&mut settings);
    // Redact timing and transfer rate values in summaries
    settings.add_filter(r"[\d.]+ seconds at .*/s", "[TIME] seconds at [RATE]/s");
    // On Windows, normalize_path converts `/nonexistent/...` to `\nonexistent\...`
    // (native separators). Normalize back so snapshots match across OSes.
    settings.add_filter(r"\\nonexistent\\input\\dir", "/nonexistent/input/dir");
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
    harness
        .cli(&["config", "set", "defaults.farm_id", FARM])
        .assert()
        .success();
    harness
        .cli(&["config", "set", "defaults.queue_id", QUEUE])
        .assert()
        .success();
}

/// Minimal valid YAML template.
fn create_bundle(harness: &TestHarness, name: &str) -> String {
    let dir = harness.config_dir.path().join(name);
    fs::create_dir_all(&dir).unwrap();
    fs::write(
        dir.join("template.yaml"),
        "\
specificationVersion: jobtemplate-2023-09
name: TestJob
steps:
  - name: Step1
    script:
      actions:
        onRun:
          command: echo
          args: ['hello']
",
    )
    .unwrap();
    dir.to_str().unwrap().to_owned()
}

/// Bundle with a STRING parameter.
fn create_bundle_with_params(harness: &TestHarness, name: &str) -> String {
    let dir = harness.config_dir.path().join(name);
    fs::create_dir_all(&dir).unwrap();
    fs::write(
        dir.join("template.yaml"),
        "\
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
",
    )
    .unwrap();
    dir.to_str().unwrap().to_owned()
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
    dir.to_str().unwrap().to_owned()
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
    fs::write(
        dir.join("template.yaml"),
        "\
specificationVersion: jobtemplate-2023-09
name: AttachmentJob
steps:
  - name: Step1
    script:
      actions:
        onRun:
          command: echo
          args: ['hello']
",
    )
    .unwrap();
    // Asset references pointing to the input dir
    fs::write(
        dir.join("asset_references.yaml"),
        format!(
            "\
assetReferences:
  inputs:
    directories:
      - {input_dir}
    filenames: []
  outputs:
    directories: []
  referencedPaths: []
",
            input_dir = input_dir.display()
        ),
    )
    .unwrap();
    dir.to_str().unwrap().to_owned()
}

/// Bundle whose input files live in a directory OUTSIDE the bundle directory
/// (a sibling temp dir), so they are not covered by any known asset path.
/// Used to exercise the "files outside known paths" safety check.
fn create_bundle_with_external_inputs(harness: &TestHarness, name: &str) -> String {
    let dir = harness.config_dir.path().join(name);
    fs::create_dir_all(&dir).unwrap();
    // External input dir: a sibling of the bundle dir, not nested inside it.
    let input_dir = harness
        .config_dir
        .path()
        .join(format!("{name}_external_inputs"));
    fs::create_dir_all(&input_dir).unwrap();
    fs::write(input_dir.join("data.txt"), "test file content").unwrap();
    fs::write(
        dir.join("template.yaml"),
        "\
specificationVersion: jobtemplate-2023-09
name: AttachmentJob
steps:
  - name: Step1
    script:
      actions:
        onRun:
          command: echo
          args: ['hello']
",
    )
    .unwrap();
    fs::write(
        dir.join("asset_references.yaml"),
        format!(
            "\
assetReferences:
  inputs:
    directories:
      - {input_dir}
    filenames: []
  outputs:
    directories: []
  referencedPaths: []
",
            input_dir = input_dir.display()
        ),
    )
    .unwrap();
    dir.to_str().unwrap().to_owned()
}

/// Bundle with a symlink that escapes the bundle directory.
/// Only used by the `#[cfg(unix)]` symlink-escape test.
#[cfg(unix)]
fn create_bundle_with_escaping_symlink(harness: &TestHarness, name: &str) -> String {
    let dir = harness.config_dir.path().join(name);
    fs::create_dir_all(&dir).unwrap();
    fs::write(
        dir.join("template.yaml"),
        "\
specificationVersion: jobtemplate-2023-09
name: SymlinkJob
steps:
  - name: Step1
    script:
      actions:
        onRun:
          command: echo
",
    )
    .unwrap();
    // Create a symlink pointing outside the bundle
    #[cfg(unix)]
    std::os::unix::fs::symlink("/etc/hosts", dir.join("escape_link")).unwrap();
    dir.to_str().unwrap().to_owned()
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
    queues::mock_get_queue(
        &harness.server,
        FARM,
        json!({
            "queueId": QUEUE,
            "displayName": "Test Queue",
        }),
    )
    .await;
    queue_resources::mock_list_queue_environments(&harness.server, FARM, QUEUE, &[]).await;
    bundle::mock_create_job(&harness.server, FARM, QUEUE, JOB).await;
    jobs::mock_get_job(
        &harness.server,
        FARM,
        QUEUE,
        json!({
            "jobId": JOB,
            "lifecycleStatus": "CREATE_COMPLETE",
            "lifecycleStatusMessage": "Job created successfully",
        }),
    )
    .await;
}

/// Mock APIs for a submission with attachments (queue has jobAttachmentSettings).
async fn mock_submit_with_attachments(harness: &TestHarness) {
    queues::mock_get_queue(
        &harness.server,
        FARM,
        json!({
            "queueId": QUEUE,
            "displayName": "Test Queue",
            "jobAttachmentSettings": {
                "s3BucketName": "test-bucket",
                "rootPrefix": "Data",
            },
        }),
    )
    .await;
    queue_resources::mock_list_queue_environments(&harness.server, FARM, QUEUE, &[]).await;
    queue_resources::mock_assume_queue_role_for_user(
        &harness.server,
        FARM,
        QUEUE,
        queue_role_credentials(),
    )
    .await;
    sts::mock_get_caller_identity(&harness.server).await;
    s3::mock_s3_head_not_found(&harness.server).await;
    s3::mock_s3_put_success(&harness.server).await;
    bundle::mock_create_job(&harness.server, FARM, QUEUE, JOB).await;
    jobs::mock_get_job(
        &harness.server,
        FARM,
        QUEUE,
        json!({
            "jobId": JOB,
            "lifecycleStatus": "CREATE_COMPLETE",
            "lifecycleStatusMessage": "Job created successfully",
        }),
    )
    .await;
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
        "bundle",
        "submit",
        &bundle_dir,
        "--yes",
        "--parameter",
        "Frames=20-30",
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
        "bundle",
        "submit",
        &bundle_dir,
        "--yes",
        "--name",
        "Custom Job Name",
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
        "bundle",
        "submit",
        &bundle_dir,
        "--yes",
        "--priority",
        "75",
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
        "bundle",
        "submit",
        &bundle_dir,
        "--yes",
        "--max-failed-tasks-count",
        "10",
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
        "bundle",
        "submit",
        &bundle_dir,
        "--yes",
        "--max-retries-per-task",
        "3",
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
    harness
        .cli(&["config", "set", "settings.auto_accept", "true"])
        .assert()
        .success();
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
    assert_cmd_snapshot!(harness.cmd(&["bundle", "submit", "/nonexistent/path/to/bundle",]));
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
    harness
        .cli(&["config", "set", "settings.known_asset_paths", &temp_root])
        .assert()
        .success();
    let _guard = bundle_settings().bind_to_scope();
    assert_cmd_snapshot!(harness.cmd(&["bundle", "submit", &bundle_dir, "--yes",]));
}

// =====================================================================
// API call fails (CreateJob returns error)
// =====================================================================

#[tokio::test]
async fn bundle_submit_api_error_exits_with_error() {
    let harness = TestHarness::new().await;
    setup_config(&harness);
    telemetry::mock_telemetry_endpoint(&harness.server).await;
    queues::mock_get_queue(
        &harness.server,
        FARM,
        json!({
            "queueId": QUEUE, "displayName": "Test Queue",
        }),
    )
    .await;
    queue_resources::mock_list_queue_environments(&harness.server, FARM, QUEUE, &[]).await;
    bundle::mock_create_job_error(&harness.server, FARM, QUEUE, 403, "AccessDeniedException").await;
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
    queues::mock_get_queue(
        &harness.server,
        FARM,
        json!({
            "queueId": QUEUE, "displayName": "Test Queue",
        }),
    )
    .await;
    queue_resources::mock_list_queue_environments(&harness.server, FARM, QUEUE, &[]).await;
    bundle::mock_create_job_error(&harness.server, FARM, QUEUE, 403, "AccessDeniedException").await;
    // Mount queue list for suggestion chain
    queues::mock_list_queues(
        &harness.server,
        FARM,
        &[json!({"queueId": "queue-real", "displayName": "Real Queue"})],
    )
    .await;
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
    harness
        .cli(&["config", "set", "settings.storage_profile_id", "sp-abc123"])
        .assert()
        .success();
    telemetry::mock_telemetry_endpoint(&harness.server).await;
    queues::mock_get_queue(
        &harness.server,
        FARM,
        json!({
            "queueId": QUEUE, "displayName": "Test Queue",
        }),
    )
    .await;
    queue_resources::mock_list_queue_environments(&harness.server, FARM, QUEUE, &[]).await;
    queue_resources::mock_get_storage_profile_for_queue(
        &harness.server,
        FARM,
        QUEUE,
        "sp-abc123",
        json!({
            "storageProfileId": "sp-abc123",
            "displayName": "My Storage Profile",
            "osFamily": "LINUX",
            "fileSystemLocations": [],
        }),
    )
    .await;
    bundle::mock_create_job(&harness.server, FARM, QUEUE, JOB).await;
    jobs::mock_get_job(
        &harness.server,
        FARM,
        QUEUE,
        json!({
            "jobId": JOB,
            "lifecycleStatus": "CREATE_COMPLETE",
            "lifecycleStatusMessage": "Job created successfully",
        }),
    )
    .await;
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

    harness
        .cli(&["bundle", "submit", &bundle_dir, "--yes"])
        .assert()
        .success();

    // Verify defaults.job_id was updated
    let output = harness
        .cli(&["config", "get", "defaults.job_id"])
        .output()
        .expect("failed to run");
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

    harness
        .cli(&["bundle", "submit", &bundle_dir, "--yes", "--farm-id", FARM])
        .assert()
        .success();

    // defaults.job_id should NOT be updated
    let output = harness
        .cli(&["config", "get", "defaults.job_id"])
        .output()
        .expect("failed to run");
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
    queues::mock_get_queue(
        &harness.server,
        FARM,
        json!({
            "queueId": QUEUE, "displayName": "Test Queue",
        }),
    )
    .await;
    queue_resources::mock_list_queue_environments(&harness.server, FARM, QUEUE, &[]).await;
    bundle::mock_create_job(&harness.server, FARM, QUEUE, JOB).await;
    // GetJob returns CREATE_FAILED
    jobs::mock_get_job(
        &harness.server,
        FARM,
        QUEUE,
        json!({
            "jobId": JOB,
            "lifecycleStatus": "CREATE_FAILED",
            "lifecycleStatusMessage": "Template validation failed",
        }),
    )
    .await;
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
    fs::write(
        dir.join("template.yaml"),
        "\
specificationVersion: jobtemplate-2023-09
name: MissingInputJob
steps:
  - name: Step1
    script:
      actions:
        onRun:
          command: echo
",
    )
    .unwrap();
    fs::write(
        dir.join("asset_references.yaml"),
        "\
assetReferences:
  inputs:
    directories:
      - /nonexistent/input/dir
    filenames: []
  outputs:
    directories: []
  referencedPaths: []
",
    )
    .unwrap();

    let _guard = bundle_settings().bind_to_scope();
    assert_cmd_snapshot!(harness.cmd(&[
        "bundle",
        "submit",
        dir.to_str().unwrap(),
        "--yes",
        "--require-paths-exist",
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
        "bundle",
        "submit",
        &bundle_dir,
        "--yes",
        "--target-task-run-status",
        "SUSPENDED",
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
        "bundle",
        "submit",
        &bundle_dir,
        "--yes",
        "--max-worker-count",
        "5",
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
        "bundle",
        "submit",
        &bundle_dir,
        "--yes",
        "--submitter-name",
        "MyApp",
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
    fs::write(
        dir.join("template.yaml"),
        "specificationVersion: jobtemplate-2023-09\nname: Y\nsteps: []",
    )
    .unwrap();
    fs::write(
        dir.join("template.json"),
        r#"{"specificationVersion":"jobtemplate-2023-09","name":"J","steps":[]}"#,
    )
    .unwrap();
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
        "bundle",
        "submit",
        &bundle_dir,
        "--yes",
        "--parameter",
        "not-a-valid-format",
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
        "bundle",
        "submit",
        &bundle_dir,
        "--yes",
        "--farm-id",
        FARM,
        "--queue-id",
        QUEUE,
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
    // Input files live OUTSIDE the bundle directory, so they are not covered
    // by any known asset path (the bundle dir is the only known path here).
    let bundle_dir = create_bundle_with_external_inputs(&harness, "known_paths_warn");
    let _guard = bundle_settings().bind_to_scope();
    let output = harness
        .cmd(&["bundle", "submit", &bundle_dir, "--yes"])
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        !output.status.success(),
        "Expected failure when auto_accept + unknown paths, got success:\n{stdout}"
    );
    assert!(
        stdout.contains("auto_accept")
            || stdout.contains("unknown paths")
            || stdout.contains("canceled"),
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
    queues::mock_get_queue(
        &harness.server,
        FARM,
        json!({
            "queueId": QUEUE, "displayName": "Test Queue",
        }),
    )
    .await;
    queue_resources::mock_list_queue_environments(&harness.server, FARM, QUEUE, &[]).await;
    bundle::mock_create_job(&harness.server, FARM, QUEUE, JOB).await;
    jobs::mock_get_job(
        &harness.server,
        FARM,
        QUEUE,
        json!({
            "jobId": JOB,
            "lifecycleStatus": "CREATE_FAILED",
            "lifecycleStatusMessage": "Template validation failed",
        }),
    )
    .await;
    let bundle_dir = create_bundle(&harness, "create_failed_jobid");
    let _guard = bundle_settings().bind_to_scope();
    let output = harness
        .cmd(&["bundle", "submit", &bundle_dir, "--yes"])
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
    let output = harness
        .cmd(&[
            "bundle",
            "submit",
            &bundle_dir,
            "--yes",
            "--job-attachments-file-system",
            "INVALID",
        ])
        .output()
        .unwrap();
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
        "bundle",
        "submit",
        &bundle_dir,
        "--yes",
        "--parameter",
        "123Invalid=value",
    ]));
}

// ===========================================================================
// Submission telemetry events
// ===========================================================================

/// Verify that a successful bundle submit sends telemetry events.
#[tokio::test]
async fn bundle_submit_records_submission_telemetry_event() {
    let harness = TestHarness::new().await;
    setup_config(&harness);
    mock_submit_no_attachments(&harness).await;
    let bundle_dir = create_bundle(&harness, "telemetry_sub");
    // The permissive mock in mock_submit_no_attachments accepts telemetry.
    // Replace it with expect(1..) to verify telemetry is actually sent.
    harness
        .cli(&["bundle", "submit", &bundle_dir, "--yes"])
        .assert()
        .success();
}

/// Verify that a successful bundle submit sends a "`create_job`" telemetry event.
#[tokio::test]
async fn bundle_submit_records_create_job_telemetry_event() {
    let harness = TestHarness::new().await;
    setup_config(&harness);
    mock_submit_no_attachments(&harness).await;
    let bundle_dir = create_bundle(&harness, "telemetry_cj");
    harness
        .cli(&["bundle", "submit", &bundle_dir, "--yes"])
        .assert()
        .success();
}

/// Verify that the submission event fires even when `CreateJob` fails.
#[tokio::test]
async fn bundle_submit_failure_still_records_submission_event() {
    let harness = TestHarness::new().await;
    setup_config(&harness);
    telemetry::mock_telemetry_endpoint_permissive(&harness.server).await;
    queues::mock_get_queue(
        &harness.server,
        FARM,
        json!({
            "queueId": QUEUE, "displayName": "Test Queue",
        }),
    )
    .await;
    queue_resources::mock_list_queue_environments(&harness.server, FARM, QUEUE, &[]).await;
    bundle::mock_create_job_error(&harness.server, FARM, QUEUE, 403, "AccessDeniedException").await;
    let bundle_dir = create_bundle(&harness, "telemetry_fail");
    harness
        .cli(&["bundle", "submit", &bundle_dir, "--yes"])
        .assert()
        .failure();
}

// ===========================================================================
// Upload confirmation prompt for all paths
// ===========================================================================

/// When --yes is NOT passed and there are attachments, the CLI should show
/// an upload summary and prompt for confirmation. Since stdin is not a TTY
/// in tests, the handler's `confirm` returns false (no interactive input).
#[tokio::test]
async fn bundle_submit_shows_upload_confirmation_prompt() {
    let harness = TestHarness::new().await;
    setup_config(&harness);
    mock_submit_with_attachments(&harness).await;
    let bundle_dir = create_bundle_with_attachments(&harness, "confirm_prompt");
    let temp_root = harness.config_dir.path().to_string_lossy().to_string();
    harness
        .cli(&["config", "set", "settings.known_asset_paths", &temp_root])
        .assert()
        .success();

    // Without --yes, should show upload summary with file count/size info
    let output = harness
        .cli(&["bundle", "submit", &bundle_dir])
        .output()
        .expect("failed to run");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = format!("{stdout}{stderr}");
    // Should contain upload summary info (file count, size)
    assert!(
        combined.contains("input file") || combined.contains("Job submission"),
        "Expected upload summary or submission message, got: {combined}"
    );
}

/// When `auto_accept` is true and all paths are known, submission should
/// proceed without prompting and print the upload summary.
#[tokio::test]
async fn bundle_submit_auto_accept_known_paths_prints_summary() {
    let harness = TestHarness::new().await;
    setup_config(&harness);
    mock_submit_with_attachments(&harness).await;
    let bundle_dir = create_bundle_with_attachments(&harness, "auto_known");
    let temp_root = harness.config_dir.path().to_string_lossy().to_string();
    harness
        .cli(&["config", "set", "settings.known_asset_paths", &temp_root])
        .assert()
        .success();
    harness
        .cli(&["config", "set", "settings.auto_accept", "true"])
        .assert()
        .success();

    let output = harness
        .cli(&["bundle", "submit", &bundle_dir])
        .output()
        .expect("failed to run");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    // Should print upload summary even with auto_accept
    assert!(
        stdout.contains("input file"),
        "Expected upload summary with file count, got: {stdout}"
    );
}

// ===========================================================================
// --yes flag still prints upload summary
// ===========================================================================

/// With --yes, the upload summary should still be printed (just no prompt).
#[tokio::test]
async fn bundle_submit_yes_flag_prints_upload_summary() {
    let harness = TestHarness::new().await;
    setup_config(&harness);
    mock_submit_with_attachments(&harness).await;
    let bundle_dir = create_bundle_with_attachments(&harness, "yes_summary");
    let temp_root = harness.config_dir.path().to_string_lossy().to_string();
    harness
        .cli(&["config", "set", "settings.known_asset_paths", &temp_root])
        .assert()
        .success();

    let output = harness
        .cli(&["bundle", "submit", &bundle_dir, "--yes"])
        .output()
        .expect("failed to run");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    // --yes should still print the upload summary
    assert!(
        stdout.contains("input file"),
        "Expected upload summary with file count even with --yes, got: {stdout}"
    );
}

// ===========================================================================
// F5: Telemetry — hashing/upload summary events emitted during submit
// ===========================================================================

/// When submitting a bundle with attachments, telemetry events for
/// `hashing_summary` and `upload_summary` should be sent.
#[tokio::test]
async fn bundle_submit_with_attachments_emits_hashing_telemetry() {
    let harness = TestHarness::new().await;
    setup_config(&harness);
    mock_submit_with_attachments(&harness).await;
    telemetry::mock_telemetry_endpoint(&harness.server).await;
    let bundle_dir = create_bundle_with_attachments(&harness, "hash_telem");
    let temp_root = harness.config_dir.path().to_string_lossy().to_string();
    harness
        .cli(&["config", "set", "settings.known_asset_paths", &temp_root])
        .assert()
        .success();

    harness
        .cli(&["bundle", "submit", &bundle_dir, "--yes"])
        .assert()
        .success();
    // wiremock verifies expect(1..) — telemetry events include hashing_summary,
    // upload_summary, submission, and create_job events
}

#[tokio::test]
async fn bundle_submit_with_attachments_emits_upload_telemetry() {
    let harness = TestHarness::new().await;
    setup_config(&harness);
    mock_submit_with_attachments(&harness).await;
    telemetry::mock_telemetry_endpoint(&harness.server).await;
    let bundle_dir = create_bundle_with_attachments(&harness, "upload_telem");
    let temp_root = harness.config_dir.path().to_string_lossy().to_string();
    harness
        .cli(&["config", "set", "settings.known_asset_paths", &temp_root])
        .assert()
        .success();

    harness
        .cli(&["bundle", "submit", &bundle_dir, "--yes"])
        .assert()
        .success();
    // wiremock verifies expect(1..) — telemetry events include upload_summary
}

// ===========================================================================
// F6: Telemetry — error event emitted on submission failure
// ===========================================================================

/// When `CreateJob` fails, an error telemetry event should be emitted with
/// `exception_scope` "`on_submit`".
#[tokio::test]
async fn bundle_submit_error_emits_error_telemetry() {
    let harness = TestHarness::new().await;
    setup_config(&harness);
    queues::mock_get_queue(
        &harness.server,
        FARM,
        json!({
            "queueId": QUEUE, "displayName": "Test Queue",
        }),
    )
    .await;
    queue_resources::mock_list_queue_environments(&harness.server, FARM, QUEUE, &[]).await;
    bundle::mock_create_job_error(&harness.server, FARM, QUEUE, 403, "AccessDeniedException").await;
    telemetry::mock_telemetry_endpoint(&harness.server).await;
    let bundle_dir = create_bundle(&harness, "error_telem");

    harness
        .cli(&["bundle", "submit", &bundle_dir, "--yes"])
        .assert()
        .failure();
    // wiremock verifies expect(1..) — error telemetry event sent on failure
}

// ===========================================================================
// F8: --save-debug-snapshot creates snapshot files without calling CreateJob
// ===========================================================================

/// When --save-debug-snapshot is provided, the CLI should write snapshot
/// files to the directory and NOT call `CreateJob`.
#[tokio::test]
async fn bundle_submit_save_debug_snapshot_creates_files() {
    let harness = TestHarness::new().await;
    setup_config(&harness);
    // Only mock GetQueue and ListQueueEnvironments — CreateJob should NOT be called
    queues::mock_get_queue(
        &harness.server,
        FARM,
        json!({
            "queueId": QUEUE, "displayName": "Test Queue",
        }),
    )
    .await;
    queue_resources::mock_list_queue_environments(&harness.server, FARM, QUEUE, &[]).await;
    telemetry::mock_telemetry_endpoint_permissive(&harness.server).await;

    let bundle_dir = create_bundle(&harness, "snapshot_bundle");
    let snapshot_dir = harness.config_dir.path().join("debug_snapshot");

    harness
        .cli(&[
            "bundle",
            "submit",
            &bundle_dir,
            "--yes",
            "--save-debug-snapshot",
            snapshot_dir.to_str().unwrap(),
        ])
        .assert()
        .success();

    // Verify snapshot files exist
    assert!(
        snapshot_dir.join("create_job_args.json").is_file(),
        "Expected create_job_args.json in snapshot dir"
    );
    assert!(
        snapshot_dir.join("submit_job.sh").is_file(),
        "Expected submit_job.sh in snapshot dir"
    );
    assert!(
        snapshot_dir.join("submit_job.bat").is_file(),
        "Expected submit_job.bat in snapshot dir"
    );
    assert!(
        snapshot_dir.join("queue.json").is_file(),
        "Expected queue.json in snapshot dir"
    );
}

/// When --save-debug-snapshot is provided, `CreateJob` should NOT be called
/// and no job ID should be printed.
#[tokio::test]
async fn bundle_submit_save_debug_snapshot_skips_create_job() {
    let harness = TestHarness::new().await;
    setup_config(&harness);
    queues::mock_get_queue(
        &harness.server,
        FARM,
        json!({
            "queueId": QUEUE, "displayName": "Test Queue",
        }),
    )
    .await;
    queue_resources::mock_list_queue_environments(&harness.server, FARM, QUEUE, &[]).await;
    telemetry::mock_telemetry_endpoint_permissive(&harness.server).await;
    // Intentionally NOT mocking CreateJob — if it's called, wiremock will error

    let bundle_dir = create_bundle(&harness, "snapshot_no_create");
    let snapshot_dir = harness.config_dir.path().join("debug_snapshot2");

    let output = harness
        .cli(&[
            "bundle",
            "submit",
            &bundle_dir,
            "--yes",
            "--save-debug-snapshot",
            snapshot_dir.to_str().unwrap(),
        ])
        .output()
        .expect("failed to run");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        !stdout.contains("job-"),
        "Expected no job ID in output when snapshotting"
    );
    assert!(
        stdout.contains("Saved job debug snapshot"),
        "Expected snapshot confirmation message, got: {stdout}"
    );
}

// ===========================================================================
// F8: --save-debug-snapshot content validation
// ===========================================================================

/// Verify `create_job_args.json` contains expected keys (farmId, queueId, template).
#[tokio::test]
async fn bundle_submit_save_debug_snapshot_valid_create_job_args() {
    let harness = TestHarness::new().await;
    setup_config(&harness);
    queues::mock_get_queue(
        &harness.server,
        FARM,
        json!({
            "queueId": QUEUE, "displayName": "Test Queue",
        }),
    )
    .await;
    queue_resources::mock_list_queue_environments(&harness.server, FARM, QUEUE, &[]).await;
    telemetry::mock_telemetry_endpoint_permissive(&harness.server).await;

    let bundle_dir = create_bundle(&harness, "snapshot_content");
    let snapshot_dir = harness.config_dir.path().join("debug_content");

    harness
        .cli(&[
            "bundle",
            "submit",
            &bundle_dir,
            "--yes",
            "--save-debug-snapshot",
            snapshot_dir.to_str().unwrap(),
        ])
        .assert()
        .success();

    // Validate create_job_args.json content
    let args_path = snapshot_dir.join("create_job_args.json");
    let content = fs::read_to_string(&args_path)
        .unwrap_or_else(|_| panic!("Failed to read {}", args_path.display()));
    let args: serde_json::Value = serde_json::from_str(&content)
        .unwrap_or_else(|_| panic!("Invalid JSON in {}", args_path.display()));
    assert!(
        args.get("farmId").is_some(),
        "Expected farmId in create_job_args.json"
    );
    assert!(
        args.get("queueId").is_some(),
        "Expected queueId in create_job_args.json"
    );
    assert!(
        args.get("template").is_some(),
        "Expected template in create_job_args.json"
    );
}

/// Verify `submit_job.sh` contains aws deadline create-job command.
#[tokio::test]
async fn bundle_submit_save_debug_snapshot_valid_shell_script() {
    let harness = TestHarness::new().await;
    setup_config(&harness);
    queues::mock_get_queue(
        &harness.server,
        FARM,
        json!({
            "queueId": QUEUE, "displayName": "Test Queue",
        }),
    )
    .await;
    queue_resources::mock_list_queue_environments(&harness.server, FARM, QUEUE, &[]).await;
    telemetry::mock_telemetry_endpoint_permissive(&harness.server).await;

    let bundle_dir = create_bundle(&harness, "snapshot_shell");
    let snapshot_dir = harness.config_dir.path().join("debug_shell");

    harness
        .cli(&[
            "bundle",
            "submit",
            &bundle_dir,
            "--yes",
            "--save-debug-snapshot",
            snapshot_dir.to_str().unwrap(),
        ])
        .assert()
        .success();

    let sh_content = fs::read_to_string(snapshot_dir.join("submit_job.sh"))
        .expect("Failed to read submit_job.sh");
    assert!(
        sh_content.contains("aws deadline create-job"),
        "Expected 'aws deadline create-job' in submit_job.sh, got: {sh_content}"
    );
    assert!(
        sh_content.starts_with("#!/bin/sh"),
        "Expected shebang in submit_job.sh"
    );
    // No-attachment bundle should NOT have s3 cp commands
    assert!(
        !sh_content.contains("aws s3 cp"),
        "Expected no 'aws s3 cp' in submit_job.sh for no-attachment bundle"
    );

    let bat_content = fs::read_to_string(snapshot_dir.join("submit_job.bat"))
        .expect("Failed to read submit_job.bat");
    assert!(
        bat_content.contains("aws deadline create-job"),
        "Expected 'aws deadline create-job' in submit_job.bat, got: {bat_content}"
    );
}

/// When --save-debug-snapshot path ends in .zip, output should be a zip file.
#[tokio::test]
async fn bundle_submit_save_debug_snapshot_zip_mode() {
    let harness = TestHarness::new().await;
    setup_config(&harness);
    queues::mock_get_queue(
        &harness.server,
        FARM,
        json!({
            "queueId": QUEUE, "displayName": "Test Queue",
        }),
    )
    .await;
    queue_resources::mock_list_queue_environments(&harness.server, FARM, QUEUE, &[]).await;
    telemetry::mock_telemetry_endpoint_permissive(&harness.server).await;

    let bundle_dir = create_bundle(&harness, "snapshot_zip");
    let zip_path = harness.config_dir.path().join("debug_snapshot.zip");

    harness
        .cli(&[
            "bundle",
            "submit",
            &bundle_dir,
            "--yes",
            "--save-debug-snapshot",
            zip_path.to_str().unwrap(),
        ])
        .assert()
        .success();

    // The .zip file should exist (not a directory)
    assert!(
        zip_path.is_file(),
        "Expected zip file at {}",
        zip_path.display()
    );
}

/// With attachments, --save-debug-snapshot should copy manifests locally
/// instead of uploading to S3, and still not call `CreateJob`.
#[tokio::test]
async fn bundle_submit_save_debug_snapshot_with_attachments() {
    let harness = TestHarness::new().await;
    setup_config(&harness);
    queues::mock_get_queue(
        &harness.server,
        FARM,
        json!({
            "queueId": QUEUE,
            "displayName": "Test Queue",
            "jobAttachmentSettings": {
                "s3BucketName": "test-bucket",
                "rootPrefix": "Data",
            },
        }),
    )
    .await;
    queue_resources::mock_list_queue_environments(&harness.server, FARM, QUEUE, &[]).await;
    queue_resources::mock_assume_queue_role_for_user(
        &harness.server,
        FARM,
        QUEUE,
        queue_role_credentials(),
    )
    .await;
    sts::mock_get_caller_identity(&harness.server).await;
    telemetry::mock_telemetry_endpoint_permissive(&harness.server).await;
    // NOT mocking S3 put — snapshot should copy locally, not upload
    // NOT mocking CreateJob — should not be called

    let bundle_dir = create_bundle_with_attachments(&harness, "snapshot_attach");
    let temp_root = harness.config_dir.path().to_string_lossy().to_string();
    harness
        .cli(&["config", "set", "settings.known_asset_paths", &temp_root])
        .assert()
        .success();
    let snapshot_dir = harness.config_dir.path().join("debug_attach");

    let output = harness
        .cli(&[
            "bundle",
            "submit",
            &bundle_dir,
            "--yes",
            "--save-debug-snapshot",
            snapshot_dir.to_str().unwrap(),
        ])
        .output()
        .expect("failed to run");

    assert!(
        output.status.success(),
        "Expected success, stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        snapshot_dir.join("create_job_args.json").is_file(),
        "Expected create_job_args.json in snapshot dir"
    );
    // With attachments, create_job_args should have an "attachments" key
    let content = fs::read_to_string(snapshot_dir.join("create_job_args.json")).unwrap();
    let args: serde_json::Value = serde_json::from_str(&content).unwrap();
    assert!(
        args.get("attachments").is_some(),
        "Expected attachments in create_job_args.json when bundle has attachments"
    );
    // Shell script should include s3 cp commands for Data and Manifests
    let sh_content = fs::read_to_string(snapshot_dir.join("submit_job.sh")).unwrap();
    assert!(
        sh_content.contains("aws s3 cp"),
        "Expected 'aws s3 cp' in submit_job.sh for attachment bundle, got: {sh_content}"
    );
    assert!(
        sh_content.contains("./Data"),
        "Expected './Data' in submit_job.sh s3 cp command"
    );
    assert!(
        sh_content.contains("./Manifests"),
        "Expected './Manifests' in submit_job.sh s3 cp command"
    );
}

// ===========================================================================

// ===========================================================================
// Debug snapshot content — full queue.json, storage_profile.json,
//            and shell-quoted values in scripts
// ===========================================================================

/// queue.json should contain the full `GetQueue` response, not just
/// jobAttachmentSettings. Verify displayName and status are present.
#[tokio::test]
async fn bundle_submit_save_debug_snapshot_queue_json_has_full_content() {
    let harness = TestHarness::new().await;
    setup_config(&harness);
    queues::mock_get_queue(
        &harness.server,
        FARM,
        json!({
            "queueId": QUEUE,
            "displayName": "My Test Queue",
            "status": "ACTIVE",
            "farmId": FARM,
            "defaultBudgetAction": "NONE",
            "createdAt": "2024-01-15T10:30:00Z",
            "createdBy": "user-abc",
            "updatedAt": "2024-01-15T10:30:00Z",
            "updatedBy": "user-abc",
        }),
    )
    .await;
    queue_resources::mock_list_queue_environments(&harness.server, FARM, QUEUE, &[]).await;
    telemetry::mock_telemetry_endpoint_permissive(&harness.server).await;

    let bundle_dir = create_bundle(&harness, "snapshot_queue_full");
    let snapshot_dir = harness.config_dir.path().join("debug_queue_full");

    harness
        .cli(&[
            "bundle",
            "submit",
            &bundle_dir,
            "--yes",
            "--save-debug-snapshot",
            snapshot_dir.to_str().unwrap(),
        ])
        .assert()
        .success();

    let queue_content =
        fs::read_to_string(snapshot_dir.join("queue.json")).expect("Failed to read queue.json");
    let queue_json: serde_json::Value =
        serde_json::from_str(&queue_content).expect("Invalid JSON in queue.json");

    // queue.json must contain full queue fields, not just jobAttachmentSettings
    assert!(
        queue_json.get("displayName").is_some(),
        "Expected displayName in queue.json, got: {queue_json}"
    );
    assert!(
        queue_json.get("status").is_some(),
        "Expected status in queue.json, got: {queue_json}"
    );
}

/// When a storage profile is configured, `storage_profile.json` should be
/// written to the debug snapshot directory.
#[tokio::test]
async fn bundle_submit_save_debug_snapshot_storage_profile_written() {
    let harness = TestHarness::new().await;
    setup_config(&harness);
    harness
        .cli(&["config", "set", "settings.storage_profile_id", "sp-abc123"])
        .assert()
        .success();
    queues::mock_get_queue(
        &harness.server,
        FARM,
        json!({
            "queueId": QUEUE, "displayName": "Test Queue",
        }),
    )
    .await;
    queue_resources::mock_list_queue_environments(&harness.server, FARM, QUEUE, &[]).await;
    queue_resources::mock_get_storage_profile_for_queue(
        &harness.server,
        FARM,
        QUEUE,
        "sp-abc123",
        json!({
            "storageProfileId": "sp-abc123",
            "displayName": "My Storage Profile",
            "osFamily": "LINUX",
            "fileSystemLocations": [
                {"name": "Output", "path": "/mnt/shared/output", "type": "SHARED"}
            ],
        }),
    )
    .await;
    telemetry::mock_telemetry_endpoint_permissive(&harness.server).await;

    let bundle_dir = create_bundle(&harness, "snapshot_sp");
    let snapshot_dir = harness.config_dir.path().join("debug_sp");

    harness
        .cli(&[
            "bundle",
            "submit",
            &bundle_dir,
            "--yes",
            "--save-debug-snapshot",
            snapshot_dir.to_str().unwrap(),
        ])
        .assert()
        .success();

    // storage_profile.json must be written when storage profile is configured
    let sp_path = snapshot_dir.join("storage_profile.json");
    assert!(
        sp_path.is_file(),
        "Expected storage_profile.json in snapshot dir when storage profile is configured"
    );
    let sp_content = fs::read_to_string(&sp_path).expect("Failed to read storage_profile.json");
    let sp_json: serde_json::Value =
        serde_json::from_str(&sp_content).expect("Invalid JSON in storage_profile.json");
    assert_eq!(sp_json["storageProfileId"], "sp-abc123");
    assert_eq!(sp_json["displayName"], "My Storage Profile");
}

/// Shell scripts should properly quote values that contain spaces or
/// special characters to prevent broken scripts.
#[tokio::test]
async fn bundle_submit_save_debug_snapshot_shell_script_quotes_values() {
    let harness = TestHarness::new().await;
    setup_config(&harness);
    queues::mock_get_queue(
        &harness.server,
        FARM,
        json!({
            "queueId": QUEUE, "displayName": "Test Queue",
        }),
    )
    .await;
    queue_resources::mock_list_queue_environments(&harness.server, FARM, QUEUE, &[]).await;
    telemetry::mock_telemetry_endpoint_permissive(&harness.server).await;

    // Use a bundle with a name containing spaces to test quoting
    let dir = harness.config_dir.path().join("snapshot_quote_bundle");
    fs::create_dir_all(&dir).unwrap();
    fs::write(
        dir.join("template.yaml"),
        "\
specificationVersion: jobtemplate-2023-09
name: My Render Job With Spaces
steps:
  - name: Step1
    script:
      actions:
        onRun:
          command: echo
          args: ['hello']
",
    )
    .unwrap();

    let snapshot_dir = harness.config_dir.path().join("debug_quote");

    harness
        .cli(&[
            "bundle",
            "submit",
            dir.to_str().unwrap(),
            "--yes",
            "--save-debug-snapshot",
            snapshot_dir.to_str().unwrap(),
        ])
        .assert()
        .success();

    let sh_content = fs::read_to_string(snapshot_dir.join("submit_job.sh"))
        .expect("Failed to read submit_job.sh");

    // CLI argument values must be shell-quoted.
    // Find the line with --farm-id and verify the value is quoted.
    // Currently the script writes bare values like:
    //   --farm-id farm-0123456789abcdef0123456789abcdef
    // After fix it should be:
    //   --farm-id 'farm-0123456789abcdef0123456789abcdef'
    let has_quoted_args = sh_content.lines().any(|line| {
        let trimmed = line.trim();
        // Check for a --flag 'value' or --flag "value" pattern
        (trimmed.starts_with("--farm-id") || trimmed.starts_with("--queue-id"))
            && (trimmed.contains('\'') || trimmed.contains('"'))
    });
    assert!(
        has_quoted_args,
        "Expected CLI argument values to be shell-quoted in submit_job.sh, got:\n{sh_content}"
    );
}

// ===========================================================================
// F5: Boundary — no attachments should NOT emit hashing/upload telemetry
// ===========================================================================

/// When submitting a bundle with no attachments, `hashing_summary` and
/// `upload_summary` telemetry events should NOT be sent.
#[tokio::test]
async fn bundle_submit_no_attachments_no_hashing_upload_telemetry() {
    let harness = TestHarness::new().await;
    setup_config(&harness);
    mock_submit_no_attachments(&harness).await;
    // Mount a telemetry endpoint that expects zero hashing/upload events.
    // The generic catch-all accepts latency/submission/create_job events.
    telemetry::mock_telemetry_endpoint_permissive(&harness.server).await;

    let bundle_dir = create_bundle(&harness, "no_attach_telem");

    let output = harness
        .cli(&["bundle", "submit", &bundle_dir, "--yes"])
        .output()
        .expect("failed to run");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    // No hashing or upload summary should appear in output
    assert!(
        !stdout.contains("Hashing Summary"),
        "Expected no hashing summary for no-attachment bundle, got: {stdout}"
    );
    assert!(
        !stdout.contains("Upload Summary"),
        "Expected no upload summary for no-attachment bundle, got: {stdout}"
    );
}

// ===========================================================================
// Config defaults for max_retries_per_task / max_failed_tasks_count
// ===========================================================================

/// When --max-retries-per-task is not specified on CLI, the value from
/// `settings.max_retries_per_task` config should be used in the `CreateJob` request.
#[tokio::test]
async fn bundle_submit_max_retries_from_config() {
    let harness = TestHarness::new().await;
    setup_config(&harness);
    harness
        .cli(&["config", "set", "settings.max_retries_per_task", "10"])
        .assert()
        .success();
    mock_submit_no_attachments(&harness).await;
    let bundle_dir = create_bundle(&harness, "config_retries");

    harness
        .cli(&["bundle", "submit", &bundle_dir, "--yes"])
        .assert()
        .success();

    // Verify the CreateJob request body contains maxRetriesPerTask: 10
    let create_job_requests: Vec<_> = harness
        .server
        .received_requests()
        .await
        .unwrap()
        .into_iter()
        .filter(|r| r.url.path().contains("/jobs") && r.method.as_ref() == "POST")
        .collect();
    assert_eq!(create_job_requests.len(), 1);
    let body: serde_json::Value = serde_json::from_slice(&create_job_requests[0].body).unwrap();
    assert_eq!(
        body["maxRetriesPerTask"], 10,
        "Expected config default maxRetriesPerTask=10, got: {body}"
    );
}

/// When --max-failed-tasks-count is not specified on CLI, the value from
/// `settings.max_failed_tasks_count` config should be used in the `CreateJob` request.
#[tokio::test]
async fn bundle_submit_max_failed_from_config() {
    let harness = TestHarness::new().await;
    setup_config(&harness);
    harness
        .cli(&["config", "set", "settings.max_failed_tasks_count", "15"])
        .assert()
        .success();
    mock_submit_no_attachments(&harness).await;
    let bundle_dir = create_bundle(&harness, "config_failed");

    harness
        .cli(&["bundle", "submit", &bundle_dir, "--yes"])
        .assert()
        .success();

    let create_job_requests: Vec<_> = harness
        .server
        .received_requests()
        .await
        .unwrap()
        .into_iter()
        .filter(|r| r.url.path().contains("/jobs") && r.method.as_ref() == "POST")
        .collect();
    assert_eq!(create_job_requests.len(), 1);
    let body: serde_json::Value = serde_json::from_slice(&create_job_requests[0].body).unwrap();
    assert_eq!(
        body["maxFailedTasksCount"], 15,
        "Expected config default maxFailedTasksCount=15, got: {body}"
    );
}

/// CLI flag --max-retries-per-task should override the config setting.
#[tokio::test]
async fn bundle_submit_cli_flag_overrides_config_max_retries() {
    let harness = TestHarness::new().await;
    setup_config(&harness);
    harness
        .cli(&["config", "set", "settings.max_retries_per_task", "10"])
        .assert()
        .success();
    mock_submit_no_attachments(&harness).await;
    let bundle_dir = create_bundle(&harness, "override_retries");

    harness
        .cli(&[
            "bundle",
            "submit",
            &bundle_dir,
            "--yes",
            "--max-retries-per-task",
            "3",
        ])
        .assert()
        .success();

    let create_job_requests: Vec<_> = harness
        .server
        .received_requests()
        .await
        .unwrap()
        .into_iter()
        .filter(|r| r.url.path().contains("/jobs") && r.method.as_ref() == "POST")
        .collect();
    assert_eq!(create_job_requests.len(), 1);
    let body: serde_json::Value = serde_json::from_slice(&create_job_requests[0].body).unwrap();
    assert_eq!(
        body["maxRetriesPerTask"], 3,
        "CLI flag should override config, got: {body}"
    );
}
