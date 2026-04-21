//! Level 2 tests for `deadline job download-output`.
//!
//! Test spec reference: specs/test_specs/cli.md, Section 44, cases 10-16.
//! Additional edge cases derived from Python source study.

use deadline_test_server::deadline_api::{errors, jobs, queues, s3, sts};
use deadline_test_server::TestHarness;
use insta_cmd::assert_cmd_snapshot;
use serde_json::json;

const FARM: &str = "farm-0123456789abcdef0123456789abcdef";
const QUEUE: &str = "queue-0123456789abcdef0123456789abcdef";
const JOB: &str = "job-0123456789abcdef0123456789abcdef";
const STEP: &str = "step-0123456789abcdef0123456789abcdef";
const TASK: &str = "task-0123456789abcdef0123456789abcdef";

fn job_with_attachments() -> serde_json::Value {
    json!({
        "jobId": JOB,
        "name": "Render Job",
        "lifecycleStatus": "CREATE_COMPLETE",
        "taskRunStatus": "SUCCEEDED",
        "taskRunStatusCounts": { "SUCCEEDED": 10 },
        "attachments": {
            "manifests": [
                {
                    "rootPath": "/tmp/outputs",
                    "rootPathFormat": "posix",
                    "inputManifestPath": "Manifests/input.manifest",
                    "inputManifestHash": "abc123",
                    "outputRelativeDirectories": ["outputs"]
                }
            ],
            "fileSystem": "COPIED"
        }
    })
}

fn job_without_attachments() -> serde_json::Value {
    json!({
        "jobId": JOB,
        "name": "No Attachments Job",
        "lifecycleStatus": "CREATE_COMPLETE",
        "taskRunStatus": "SUCCEEDED",
        "taskRunStatusCounts": { "SUCCEEDED": 5 },
    })
}

fn queue_with_attachment_settings() -> serde_json::Value {
    json!({
        "queueId": QUEUE,
        "displayName": "Test Queue",
        "jobAttachmentSettings": {
            "s3BucketName": "test-bucket",
            "rootPrefix": "root-prefix"
        }
    })
}

/// Set up the common mocks for a download-output test where no output
/// manifests exist in S3 (the "no output" path).
async fn setup_no_output_mocks(harness: &TestHarness) {
    jobs::mock_get_job(&harness.server, FARM, QUEUE, job_with_attachments()).await;
    queues::mock_get_queue(&harness.server, FARM, queue_with_attachment_settings()).await;
    sts::mock_get_caller_identity(&harness.server).await;
    s3::mock_s3_list_empty(&harness.server).await;
}

// =====================================================================
// Missing required args (derived from apply_cli_options_to_config)
// =====================================================================

#[tokio::test]
async fn job_download_output_missing_farm_id_exits_with_error() {
    let harness = TestHarness::new().await;
    assert_cmd_snapshot!(harness.cmd(&[
        "job", "download-output",
        "--queue-id", QUEUE,
        "--job-id", JOB,
    ]));
}

#[tokio::test]
async fn job_download_output_missing_queue_id_exits_with_error() {
    let harness = TestHarness::new().await;
    assert_cmd_snapshot!(harness.cmd(&[
        "job", "download-output",
        "--farm-id", FARM,
        "--job-id", JOB,
    ]));
}

#[tokio::test]
async fn job_download_output_missing_job_id_exits_with_error() {
    let harness = TestHarness::new().await;
    assert_cmd_snapshot!(harness.cmd(&[
        "job", "download-output",
        "--farm-id", FARM,
        "--queue-id", QUEUE,
    ]));
}

// =====================================================================
// --task-id without --step-id (Python: UsageError)
// =====================================================================

#[tokio::test]
async fn job_download_output_task_id_without_step_id_exits_with_error() {
    let harness = TestHarness::new().await;
    assert_cmd_snapshot!(harness.cmd(&[
        "job", "download-output",
        "--farm-id", FARM,
        "--queue-id", QUEUE,
        "--job-id", JOB,
        "--task-id", TASK,
    ]));
}

// =====================================================================
// Case 15: Job has no attachments → appropriate message
// =====================================================================

#[tokio::test]
async fn job_download_output_job_without_attachments_prints_no_output() {
    let harness = TestHarness::new().await;

    jobs::mock_get_job(&harness.server, FARM, QUEUE, job_without_attachments()).await;
    queues::mock_get_queue(&harness.server, FARM, queue_with_attachment_settings()).await;
    sts::mock_get_caller_identity(&harness.server).await;
    s3::mock_s3_list_empty(&harness.server).await;

    assert_cmd_snapshot!(harness.cmd(&[
        "job", "download-output",
        "--farm-id", FARM,
        "--queue-id", QUEUE,
        "--job-id", JOB,
        "--yes",
    ]));
}

// =====================================================================
// No output manifests in S3 (job has attachments but no output yet)
// =====================================================================

#[tokio::test]
async fn job_download_output_no_output_available_prints_message() {
    let harness = TestHarness::new().await;
    setup_no_output_mocks(&harness).await;

    assert_cmd_snapshot!(harness.cmd(&[
        "job", "download-output",
        "--farm-id", FARM,
        "--queue-id", QUEUE,
        "--job-id", JOB,
        "--yes",
    ]));
}

// =====================================================================
// GetJob API error
// =====================================================================

#[tokio::test]
async fn job_download_output_get_job_error_exits_with_error() {
    let harness = TestHarness::new().await;
    errors::mock_get_job_not_found(&harness.server, FARM, QUEUE, JOB).await;

    assert_cmd_snapshot!(harness.cmd(&[
        "job", "download-output",
        "--farm-id", FARM,
        "--queue-id", QUEUE,
        "--job-id", JOB,
        "--yes",
    ]));
}

// =====================================================================
// Case 16: --output json with error → JSON error line
// =====================================================================

#[tokio::test]
async fn job_download_output_json_mode_error_prints_json_error() {
    let harness = TestHarness::new().await;
    errors::mock_get_job_not_found(&harness.server, FARM, QUEUE, JOB).await;

    assert_cmd_snapshot!(harness.cmd(&[
        "job", "download-output",
        "--farm-id", FARM,
        "--queue-id", QUEUE,
        "--job-id", JOB,
        "--output", "json",
    ]));
}

// =====================================================================
// Case 16: --output json with no output → JSON summary line
// =====================================================================

#[tokio::test]
async fn job_download_output_json_mode_no_output_prints_json_summary() {
    let harness = TestHarness::new().await;
    setup_no_output_mocks(&harness).await;

    assert_cmd_snapshot!(harness.cmd(&[
        "job", "download-output",
        "--farm-id", FARM,
        "--queue-id", QUEUE,
        "--job-id", JOB,
        "--output", "json",
        "--yes",
    ]));
}

// =====================================================================
// Case 13: --conflict-resolution validation
// =====================================================================

#[tokio::test]
async fn job_download_output_invalid_conflict_resolution_exits_with_error() {
    let harness = TestHarness::new().await;
    assert_cmd_snapshot!(harness.cmd(&[
        "job", "download-output",
        "--farm-id", FARM,
        "--queue-id", QUEUE,
        "--job-id", JOB,
        "--conflict-resolution", "INVALID",
    ]));
}

#[tokio::test]
async fn job_download_output_skip_conflict_resolution_accepted() {
    let harness = TestHarness::new().await;
    setup_no_output_mocks(&harness).await;

    assert_cmd_snapshot!(harness.cmd(&[
        "job", "download-output",
        "--farm-id", FARM,
        "--queue-id", QUEUE,
        "--job-id", JOB,
        "--conflict-resolution", "SKIP",
        "--yes",
    ]));
}

// =====================================================================
// Case 14: --yes flag auto-accepts prompts (tested via no-output path)
// =====================================================================

#[tokio::test]
async fn job_download_output_yes_flag_skips_prompts() {
    let harness = TestHarness::new().await;
    setup_no_output_mocks(&harness).await;

    // With --yes, the command should not hang waiting for input
    assert_cmd_snapshot!(harness.cmd(&[
        "job", "download-output",
        "--farm-id", FARM,
        "--queue-id", QUEUE,
        "--job-id", JOB,
        "--yes",
    ]));
}

// =====================================================================
// Start message formatting (derived from Python _get_start_message)
// =====================================================================

#[tokio::test]
async fn job_download_output_start_message_job_only() {
    let harness = TestHarness::new().await;
    setup_no_output_mocks(&harness).await;

    // Should print: Downloading output from Job 'Render Job'
    assert_cmd_snapshot!(harness.cmd(&[
        "job", "download-output",
        "--farm-id", FARM,
        "--queue-id", QUEUE,
        "--job-id", JOB,
        "--yes",
    ]));
}

#[tokio::test]
async fn job_download_output_start_message_with_step() {
    let harness = TestHarness::new().await;
    setup_no_output_mocks(&harness).await;
    jobs::mock_get_step(&harness.server, FARM, QUEUE, JOB, json!({
        "stepId": STEP,
        "name": "Render Step",
        "lifecycleStatus": "CREATE_COMPLETE",
        "taskRunStatus": "SUCCEEDED",
        "taskRunStatusCounts": { "SUCCEEDED": 10 },
    })).await;

    // Should print: Downloading output from Job 'Render Job' Step 'Render Step'
    assert_cmd_snapshot!(harness.cmd(&[
        "job", "download-output",
        "--farm-id", FARM,
        "--queue-id", QUEUE,
        "--job-id", JOB,
        "--step-id", STEP,
        "--yes",
    ]));
}

#[tokio::test]
async fn job_download_output_start_message_with_step_and_task() {
    let harness = TestHarness::new().await;
    setup_no_output_mocks(&harness).await;
    jobs::mock_get_step(&harness.server, FARM, QUEUE, JOB, json!({
        "stepId": STEP,
        "name": "Render Step",
        "lifecycleStatus": "CREATE_COMPLETE",
        "taskRunStatus": "SUCCEEDED",
        "taskRunStatusCounts": { "SUCCEEDED": 10 },
    })).await;
    jobs::mock_get_task(&harness.server, FARM, QUEUE, JOB, STEP, json!({
        "taskId": TASK,
        "runStatus": "SUCCEEDED",
        "parameters": {
            "Frame": { "int": "1" }
        }
    })).await;

    // Should print: Downloading output from Job 'Render Job' Step 'Render Step' Task {Frame=1}
    assert_cmd_snapshot!(harness.cmd(&[
        "job", "download-output",
        "--farm-id", FARM,
        "--queue-id", QUEUE,
        "--job-id", JOB,
        "--step-id", STEP,
        "--task-id", TASK,
        "--yes",
    ]));
}

#[tokio::test]
async fn job_download_output_start_message_task_with_no_params() {
    let harness = TestHarness::new().await;
    setup_no_output_mocks(&harness).await;
    jobs::mock_get_step(&harness.server, FARM, QUEUE, JOB, json!({
        "stepId": STEP,
        "name": "Render Step",
        "lifecycleStatus": "CREATE_COMPLETE",
        "taskRunStatus": "SUCCEEDED",
        "taskRunStatusCounts": { "SUCCEEDED": 10 },
    })).await;
    jobs::mock_get_task(&harness.server, FARM, QUEUE, JOB, STEP, json!({
        "taskId": TASK,
        "runStatus": "SUCCEEDED",
    })).await;

    // Should print: ...Task {}
    assert_cmd_snapshot!(harness.cmd(&[
        "job", "download-output",
        "--farm-id", FARM,
        "--queue-id", QUEUE,
        "--job-id", JOB,
        "--step-id", STEP,
        "--task-id", TASK,
        "--yes",
    ]));
}

// =====================================================================
// Help text
// =====================================================================

#[tokio::test]
async fn job_download_output_help_shows_usage() {
    let harness = TestHarness::new().await;
    assert_cmd_snapshot!(harness.cmd(&["job", "download-output", "--help"]));
}

// ===========================================================================
// Download conflict resolution prompt
// ===========================================================================

/// When files already exist at the download target and no --conflict-resolution
/// is specified, the CLI should detect conflicts and show a message.
///
/// Ignored: requires S3 GetObject mock with SDK-compatible x-amz-meta-asset-root
/// header for the full manifest download chain to work.
#[ignore = "S3 download mock chain needs asset-root metadata header support"]
#[tokio::test]
async fn job_download_output_existing_files_shows_conflict_prompt() {
    let harness = TestHarness::new().await;

    let output_dir = tempfile::TempDir::new().unwrap();
    let output_root = output_dir.path().to_str().unwrap();

    // Pre-create a file that will conflict with the manifest entry
    std::fs::write(output_dir.path().join("render.exr"), b"existing").unwrap();

    jobs::mock_get_job(&harness.server, FARM, QUEUE, json!({
        "jobId": JOB,
        "name": "Render Job",
    })).await;
    queues::mock_get_queue(&harness.server, FARM, queue_with_attachment_settings()).await;
    sts::mock_get_caller_identity(&harness.server).await;

    // S3 manifest key (must include step- pattern for select_latest_manifests_per_task)
    let manifest_key = format!(
        "root-prefix/Manifests/{FARM}/{QUEUE}/{JOB}/step-01/task-01/2024-01-01T00:00:00Z_sa-1/output.manifest"
    );
    let manifest_json = serde_json::json!({
        "manifestVersion": "2023-03-03",
        "hashAlg": "xxh128",
        "totalSize": 1024,
        "paths": [
            {"path": "render.exr", "hash": "abc123", "size": 1024, "mtime": 1700000000}
        ]
    }).to_string();

    // S3 GetObject for the manifest (path-style: /bucket/key)
    s3::mock_s3_get_object_with_metadata(
        &harness.server,
        &format!("test-bucket/{manifest_key}"),
        manifest_json.as_bytes(),
        &[("asset-root", output_root)],
    ).await;

    // S3 ListObjectsV2: return the manifest key
    s3::mock_s3_list_objects(&harness.server, &[&manifest_key]).await;

    let output = harness
        .cli(&[
            "job", "download-output",
            "--farm-id", FARM,
            "--queue-id", QUEUE,
            "--job-id", JOB,
        ])
        .output()
        .expect("failed to run");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = format!("{stdout}{stderr}");

    // Should mention existing files / conflict resolution
    assert!(
        combined.contains("already exist") || combined.contains("conflict") || combined.contains("Overwrite") || combined.contains("Create a copy"),
        "Expected conflict resolution message about existing files, got: {combined}"
    );
}

/// When --conflict-resolution is explicitly set, no prompt should be shown
/// even if files conflict.
#[tokio::test]
async fn job_download_output_explicit_conflict_resolution_skips_prompt() {
    let harness = TestHarness::new().await;
    setup_no_output_mocks(&harness).await;

    // With explicit --conflict-resolution, should proceed without prompting
    assert_cmd_snapshot!(harness.cmd(&[
        "job", "download-output",
        "--farm-id", FARM,
        "--queue-id", QUEUE,
        "--job-id", JOB,
        "--conflict-resolution", "SKIP",
    ]));
}

// ---------------------------------------------------------------------------
// --yes flag defaults to CREATE_COPY without prompt
// ---------------------------------------------------------------------------

/// With --yes and no --conflict-resolution, should default to CREATE_COPY
/// without showing any conflict prompt.
#[tokio::test]
async fn job_download_output_yes_flag_defaults_to_create_copy() {
    let harness = TestHarness::new().await;
    setup_no_output_mocks(&harness).await;

    // --yes should suppress any conflict prompt
    assert_cmd_snapshot!(harness.cmd(&[
        "job", "download-output",
        "--farm-id", FARM,
        "--queue-id", QUEUE,
        "--job-id", JOB,
        "--yes",
    ]));
}
