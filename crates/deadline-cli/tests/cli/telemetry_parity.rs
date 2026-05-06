//! Level 2 tests for telemetry success/fail events (#21g).
//!
//! Python uses `@record_success_fail_telemetry_event(metric_name=...)` to wrap
//! operations and emit `com.amazon.rum.deadline.{metric_name}` with `is_success`
//! and optional `exception_type`.
//!
//! Rust uses `telemetry::record_success_fail()` at equivalent wrapping points:
//! - `asset_upload` — wraps the upload phase in bundle submit
//! - `queue_sync_output` — wraps the entire sync-output command
//! - `download_job_output` — wraps the entire download-output command
//!
//! These tests verify the Rust CLI emits the same events.

use deadline_test_server::TestHarness;
use deadline_test_server::deadline_api::{
    bundle, errors, jobs, queue_resources, queues, s3, sts, telemetry,
};
use serde_json::json;
use std::fs;
use tempfile::TempDir;

// =========================================================================
// Shared helpers
// =========================================================================

const FARM: &str = "farm-abc";
const QUEUE: &str = "queue-aaa";
const JOB: &str = "job-001";

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

fn create_bundle_with_file(harness: &TestHarness, name: &str) -> String {
    let dir = harness.config_dir.path().join(name);
    fs::create_dir_all(&dir).unwrap();
    let input_dir = dir.join("inputs");
    fs::create_dir_all(&input_dir).unwrap();
    fs::write(input_dir.join("data.txt"), "test data content").unwrap();
    fs::write(
        dir.join("template.yaml"),
        "specificationVersion: jobtemplate-2023-09\nname: TestJob\nsteps:\n  - name: Step1\n    script:\n      actions:\n        onRun:\n          command: echo\n          args: [hello]\n",
    )
    .unwrap();
    fs::write(
        dir.join("asset_references.yaml"),
        format!(
            "assetReferences:\n  inputs:\n    directories:\n      - {}\n    filenames: []\n  outputs:\n    directories: []\n  referencedPaths: []\n",
            input_dir.display()
        ),
    )
    .unwrap();
    dir.to_str().unwrap().to_owned()
}

// =========================================================================
// asset_upload success telemetry
// =========================================================================

/// Successful bundle submit with attachments emits `asset_upload` success event.
#[tokio::test]
async fn bundle_submit_with_attachments_emits_asset_upload_success_event() {
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
        json!({"credentials": {"accessKeyId": "AK", "secretAccessKey": "SK", "sessionToken": "ST", "expiration": "2025-12-18T01:00:00Z"}}),
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

    // Expect telemetry event containing "asset_upload"
    telemetry::mock_telemetry_event_type(&harness.server, "com.amazon.rum.deadline.asset_upload")
        .await;

    let bundle_dir = create_bundle_with_file(&harness, "upload_telem");
    let temp_root = harness.config_dir.path().to_string_lossy().to_string();
    harness
        .cli(&["config", "set", "settings.known_asset_paths", &temp_root])
        .assert()
        .success();

    harness
        .cli(&["bundle", "submit", &bundle_dir, "--yes"])
        .assert()
        .success();
    // wiremock verifies expect(1..) on the event_type mock
}

// =========================================================================
// queue_sync_output success/fail telemetry
// =========================================================================

/// Successful sync-output emits `queue_sync_output` success event.
#[tokio::test]
async fn sync_output_success_emits_telemetry_event() {
    let harness = TestHarness::new().await;
    setup_config(&harness);
    let checkpoint_dir = TempDir::new().unwrap();

    queues::mock_get_queue(
        &harness.server,
        FARM,
        json!({
            "queueId": QUEUE,
            "displayName": "Test Queue",
            "farmId": FARM,
            "status": "SCHEDULING",
            "defaultBudgetAction": "NONE",
            "jobAttachmentSettings": {
                "s3BucketName": "my-bucket",
                "rootPrefix": "DeadlineCloud"
            },
            "createdAt": "2024-06-15T10:30:00Z",
            "createdBy": "user"
        }),
    )
    .await;
    sts::mock_get_caller_identity(&harness.server).await;

    // No jobs found — still a successful operation
    jobs::mock_search_jobs(&harness.server, FARM, &[], 0).await;

    // Expect telemetry event for queue_sync_output
    telemetry::mock_telemetry_event_type(
        &harness.server,
        "com.amazon.rum.deadline.queue_sync_output",
    )
    .await;

    harness
        .cli(&[
            "queue",
            "sync-output",
            "--ignore-storage-profiles",
            "--checkpoint-dir",
            checkpoint_dir.path().to_str().unwrap(),
        ])
        .assert()
        .success();
}

/// Failed sync-output (queue access denied) emits `queue_sync_output` failure event.
/// Matches Python: decorator wraps the entire command, so early failures emit telemetry.
#[tokio::test]
async fn sync_output_failure_emits_telemetry_event() {
    let harness = TestHarness::new().await;
    setup_config(&harness);
    let checkpoint_dir = TempDir::new().unwrap();

    // GetQueue returns 403
    errors::mock_get_queue_access_denied(&harness.server, FARM, QUEUE).await;

    // Expect telemetry event for queue_sync_output with failure
    telemetry::mock_telemetry_event_type(
        &harness.server,
        "com.amazon.rum.deadline.queue_sync_output",
    )
    .await;

    harness
        .cli(&[
            "queue",
            "sync-output",
            "--ignore-storage-profiles",
            "--checkpoint-dir",
            checkpoint_dir.path().to_str().unwrap(),
        ])
        .assert()
        .failure();
}

// =========================================================================
// download_job_output success/fail telemetry
// =========================================================================

/// Successful job download-output emits `download_job_output` success event.
#[tokio::test]
async fn download_output_success_emits_telemetry_event() {
    let harness = TestHarness::new().await;
    setup_config(&harness);
    let download_dir = TempDir::new().unwrap();
    let download_root = download_dir.path().to_str().unwrap();

    jobs::mock_get_job(
        &harness.server,
        FARM,
        QUEUE,
        json!({
            "jobId": JOB,
            "name": "Render Job",
            "lifecycleStatus": "CREATE_COMPLETE",
            "taskRunStatus": "SUCCEEDED",
            "taskRunStatusCounts": { "SUCCEEDED": 1 },
            "attachments": {
                "manifests": [{
                    "rootPath": download_root,
                    "rootPathFormat": "posix",
                    "outputRelativeDirectories": ["out"]
                }],
                "fileSystem": "COPIED"
            }
        }),
    )
    .await;
    queues::mock_get_queue(
        &harness.server,
        FARM,
        json!({
            "queueId": QUEUE,
            "displayName": "Test Queue",
            "jobAttachmentSettings": {
                "s3BucketName": "test-bucket",
                "rootPrefix": "root-prefix"
            }
        }),
    )
    .await;
    sts::mock_get_caller_identity(&harness.server).await;

    // No output manifests in S3 — still a successful operation
    s3::mock_s3_list_empty(&harness.server).await;

    // Expect telemetry event for download_job_output
    telemetry::mock_telemetry_event_type(
        &harness.server,
        "com.amazon.rum.deadline.download_job_output",
    )
    .await;

    harness
        .cli(&[
            "job",
            "download-output",
            "--farm-id",
            FARM,
            "--queue-id",
            QUEUE,
            "--job-id",
            JOB,
            "--yes",
        ])
        .assert()
        .success();
}

/// Failed job download-output (job not found) emits `download_job_output` failure event.
/// We wrap at the command level so any failure (including early API errors) emits telemetry.
#[tokio::test]
async fn download_output_failure_emits_telemetry_event() {
    let harness = TestHarness::new().await;
    setup_config(&harness);

    // GetJob returns 404
    errors::mock_get_job_not_found(&harness.server, FARM, QUEUE, JOB).await;

    // Expect telemetry event for download_job_output with failure
    telemetry::mock_telemetry_event_type(
        &harness.server,
        "com.amazon.rum.deadline.download_job_output",
    )
    .await;

    harness
        .cli(&[
            "job",
            "download-output",
            "--farm-id",
            FARM,
            "--queue-id",
            QUEUE,
            "--job-id",
            JOB,
            "--yes",
        ])
        .assert()
        .failure();
}
