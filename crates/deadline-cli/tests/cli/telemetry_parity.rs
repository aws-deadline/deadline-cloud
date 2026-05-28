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
    bundle, errors, farms, jobs, queue_resources, queues, s3, sts, telemetry,
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

// =========================================================================
// Batch 3b: download-input telemetry (GAP-2)
// =========================================================================

/// Successful job download-input (no input attachments) emits `download_job_input` event.
#[tokio::test]
async fn download_input_success_emits_telemetry_event() {
    let harness = TestHarness::new().await;
    setup_config(&harness);

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

    // Expect telemetry event for download_job_input
    telemetry::mock_telemetry_event_type(
        &harness.server,
        "com.amazon.rum.deadline.download_job_input",
    )
    .await;

    harness
        .cli(&[
            "job",
            "download-input",
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

/// Failed job download-input (job not found) emits `download_job_input` failure event.
#[tokio::test]
async fn download_input_failure_emits_telemetry_event() {
    let harness = TestHarness::new().await;
    setup_config(&harness);

    // GetJob returns 404
    errors::mock_get_job_not_found(&harness.server, FARM, QUEUE, JOB).await;

    // Expect telemetry event for download_job_input with failure
    telemetry::mock_telemetry_event_type(
        &harness.server,
        "com.amazon.rum.deadline.download_job_input",
    )
    .await;

    harness
        .cli(&[
            "job",
            "download-input",
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

// =========================================================================
// Batch 4: submission event telemetry (#21h)
// =========================================================================

/// Bundle submit emits `com.amazon.rum.deadline.submission` event with `submitter_name`.
#[tokio::test]
async fn bundle_submit_emits_submission_event() {
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

    // Expect submission event containing submitter_name
    telemetry::mock_telemetry_event_type(&harness.server, "com.amazon.rum.deadline.submission")
        .await;

    let bundle_dir = create_bundle_with_file(&harness, "submission_telem");
    let temp_root = harness.config_dir.path().to_string_lossy().to_string();
    harness
        .cli(&["config", "set", "settings.known_asset_paths", &temp_root])
        .assert()
        .success();

    harness
        .cli(&["bundle", "submit", &bundle_dir, "--yes"])
        .assert()
        .success();
}

/// Bundle submit emits `com.amazon.rum.deadline.create_job` event with `is_success`.
#[tokio::test]
async fn bundle_submit_emits_create_job_event() {
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

    // Expect create_job event
    telemetry::mock_telemetry_event_type(&harness.server, "com.amazon.rum.deadline.create_job")
        .await;

    let bundle_dir = create_bundle_with_file(&harness, "create_job_telem");
    let temp_root = harness.config_dir.path().to_string_lossy().to_string();
    harness
        .cli(&["config", "set", "settings.known_asset_paths", &temp_root])
        .assert()
        .success();

    harness
        .cli(&["bundle", "submit", &bundle_dir, "--yes"])
        .assert()
        .success();
}

/// Bundle submit emits `com.amazon.rum.deadline.job_attachments.hashing_summary`.
#[tokio::test]
async fn bundle_submit_emits_hashing_summary_event() {
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

    // Expect hashing_summary event
    telemetry::mock_telemetry_event_type(
        &harness.server,
        "com.amazon.rum.deadline.job_attachments.hashing_summary",
    )
    .await;

    let bundle_dir = create_bundle_with_file(&harness, "hashing_telem");
    let temp_root = harness.config_dir.path().to_string_lossy().to_string();
    harness
        .cli(&["config", "set", "settings.known_asset_paths", &temp_root])
        .assert()
        .success();

    harness
        .cli(&["bundle", "submit", &bundle_dir, "--yes"])
        .assert()
        .success();
}

/// Bundle submit emits `com.amazon.rum.deadline.job_attachments.upload_summary`.
#[tokio::test]
async fn bundle_submit_emits_upload_summary_event() {
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

    // Expect upload_summary event
    telemetry::mock_telemetry_event_type(
        &harness.server,
        "com.amazon.rum.deadline.job_attachments.upload_summary",
    )
    .await;

    let bundle_dir = create_bundle_with_file(&harness, "upload_summary_telem");
    let temp_root = harness.config_dir.path().to_string_lossy().to_string();
    harness
        .cli(&["config", "set", "settings.known_asset_paths", &temp_root])
        .assert()
        .success();

    harness
        .cli(&["bundle", "submit", &bundle_dir, "--yes"])
        .assert()
        .success();
}

/// Bundle submit failure emits `com.amazon.rum.deadline.error` event.
#[tokio::test]
async fn bundle_submit_failure_emits_error_event() {
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

    // CreateJob returns 403 — triggers error telemetry
    bundle::mock_create_job_error(&harness.server, FARM, QUEUE, 403, "AccessDeniedException").await;

    // Expect error event
    telemetry::mock_telemetry_event_type(&harness.server, "com.amazon.rum.deadline.error").await;

    let bundle_dir = create_bundle_with_file(&harness, "error_telem");
    let temp_root = harness.config_dir.path().to_string_lossy().to_string();
    harness
        .cli(&["config", "set", "settings.known_asset_paths", &temp_root])
        .assert()
        .success();

    harness
        .cli(&["bundle", "submit", &bundle_dir, "--yes"])
        .assert()
        .failure();
}

// =========================================================================
// Batch 2: queue_sync_output_stats telemetry (#21j)
// =========================================================================

/// Successful sync-output emits `queue_sync_output_stats` event with download statistics.
#[tokio::test]
async fn sync_output_emits_stats_telemetry_event() {
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

    // No jobs found
    jobs::mock_search_jobs(&harness.server, FARM, &[], 0).await;

    // Expect queue_sync_output_stats event
    telemetry::mock_telemetry_event_type(
        &harness.server,
        "com.amazon.rum.deadline.queue_sync_output_stats",
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

// =========================================================================
// Batch 3: MCP telemetry (#21k)
// =========================================================================

/// MCP server startup emits `mcp.server_startup` telemetry event.
#[tokio::test]
async fn mcp_server_emits_startup_telemetry() {
    use rmcp::ServiceExt;
    use rmcp::transport::{ConfigureCommandExt, TokioChildProcess};
    use tokio::process::Command;

    let harness = TestHarness::new().await;
    farms::mock_list_farms(&harness.server, &[]).await;

    // Expect server_startup event
    telemetry::mock_telemetry_event_type(
        &harness.server,
        "com.amazon.rum.deadline.mcp.server_startup",
    )
    .await;

    let bin = assert_cmd::cargo::cargo_bin("deadline");
    let ep = harness.endpoint_url();
    let config_path = harness.config_path.clone();
    let home = harness.config_dir.path().to_str().unwrap().to_owned();

    let transport = TokioChildProcess::new(Command::new(&bin).configure(move |cmd| {
        cmd.arg("mcp-server");
        cmd.env("AWS_ENDPOINT_URL_DEADLINE", &ep);
        cmd.env("AWS_ACCESS_KEY_ID", "AKIAIOSFODNN7EXAMPLE");
        cmd.env(
            "AWS_SECRET_ACCESS_KEY",
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        );
        cmd.env("AWS_DEFAULT_REGION", "us-west-2");
        cmd.env("DEADLINE_CONFIG_FILE_PATH", &config_path);
        cmd.env("HOME", &home);
        for var in &[
            "AWS_PROFILE",
            "AWS_DEFAULT_PROFILE",
            "AWS_CONFIG_FILE",
            "AWS_SHARED_CREDENTIALS_FILE",
            "AWS_SESSION_TOKEN",
            "AWS_SECURITY_TOKEN",
            "AWS_ENDPOINT_URL",
        ] {
            cmd.env_remove(var);
        }
    }))
    .expect("failed to spawn mcp-server");

    let client = ().serve(transport).await.expect("failed to initialize MCP client");

    // Give server time to emit startup telemetry
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    client.cancel().await.unwrap();
    // wiremock verifies expect(1..) on drop
}

/// MCP tool call emits `mcp.latency` telemetry event.
#[tokio::test]
async fn mcp_tool_emits_latency_telemetry() {
    use rmcp::ServiceExt;
    use rmcp::model::CallToolRequestParams;
    use rmcp::transport::{ConfigureCommandExt, TokioChildProcess};
    use tokio::process::Command;

    let harness = TestHarness::new().await;
    farms::mock_list_farms(
        &harness.server,
        &[json!({"farmId": "farm-aaa", "displayName": "Test Farm"})],
    )
    .await;

    // Expect mcp.latency event
    telemetry::mock_telemetry_event_type(&harness.server, "com.amazon.rum.deadline.mcp.latency")
        .await;

    let bin = assert_cmd::cargo::cargo_bin("deadline");
    let ep = harness.endpoint_url();
    let config_path = harness.config_path.clone();
    let home = harness.config_dir.path().to_str().unwrap().to_owned();

    let transport = TokioChildProcess::new(Command::new(&bin).configure(move |cmd| {
        cmd.arg("mcp-server");
        cmd.env("AWS_ENDPOINT_URL_DEADLINE", &ep);
        cmd.env("AWS_ACCESS_KEY_ID", "AKIAIOSFODNN7EXAMPLE");
        cmd.env(
            "AWS_SECRET_ACCESS_KEY",
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        );
        cmd.env("AWS_DEFAULT_REGION", "us-west-2");
        cmd.env("DEADLINE_CONFIG_FILE_PATH", &config_path);
        cmd.env("HOME", &home);
        for var in &[
            "AWS_PROFILE",
            "AWS_DEFAULT_PROFILE",
            "AWS_CONFIG_FILE",
            "AWS_SHARED_CREDENTIALS_FILE",
            "AWS_SESSION_TOKEN",
            "AWS_SECURITY_TOKEN",
            "AWS_ENDPOINT_URL",
        ] {
            cmd.env_remove(var);
        }
    }))
    .expect("failed to spawn mcp-server");

    let client = ().serve(transport).await.expect("failed to initialize MCP client");

    // Call a tool to trigger latency telemetry
    let _result = client
        .call_tool(CallToolRequestParams::new("deadline_list_farms"))
        .await
        .expect("call_tool failed");

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    client.cancel().await.unwrap();
}

/// MCP tool call emits `mcp.usage` telemetry event.
#[tokio::test]
async fn mcp_tool_emits_usage_telemetry() {
    use rmcp::ServiceExt;
    use rmcp::model::CallToolRequestParams;
    use rmcp::transport::{ConfigureCommandExt, TokioChildProcess};
    use tokio::process::Command;

    let harness = TestHarness::new().await;
    farms::mock_list_farms(
        &harness.server,
        &[json!({"farmId": "farm-aaa", "displayName": "Test Farm"})],
    )
    .await;

    // Expect mcp.usage event
    telemetry::mock_telemetry_event_type(&harness.server, "com.amazon.rum.deadline.mcp.usage")
        .await;

    let bin = assert_cmd::cargo::cargo_bin("deadline");
    let ep = harness.endpoint_url();
    let config_path = harness.config_path.clone();
    let home = harness.config_dir.path().to_str().unwrap().to_owned();

    let transport = TokioChildProcess::new(Command::new(&bin).configure(move |cmd| {
        cmd.arg("mcp-server");
        cmd.env("AWS_ENDPOINT_URL_DEADLINE", &ep);
        cmd.env("AWS_ACCESS_KEY_ID", "AKIAIOSFODNN7EXAMPLE");
        cmd.env(
            "AWS_SECRET_ACCESS_KEY",
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        );
        cmd.env("AWS_DEFAULT_REGION", "us-west-2");
        cmd.env("DEADLINE_CONFIG_FILE_PATH", &config_path);
        cmd.env("HOME", &home);
        for var in &[
            "AWS_PROFILE",
            "AWS_DEFAULT_PROFILE",
            "AWS_CONFIG_FILE",
            "AWS_SHARED_CREDENTIALS_FILE",
            "AWS_SESSION_TOKEN",
            "AWS_SECURITY_TOKEN",
            "AWS_ENDPOINT_URL",
        ] {
            cmd.env_remove(var);
        }
    }))
    .expect("failed to spawn mcp-server");

    let client = ().serve(transport).await.expect("failed to initialize MCP client");

    // Call a tool to trigger usage telemetry
    let _result = client
        .call_tool(CallToolRequestParams::new("deadline_list_farms"))
        .await
        .expect("call_tool failed");

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    client.cancel().await.unwrap();
}

// =========================================================================
// Fire-and-forget resilience: telemetry failure doesn't affect commands
// =========================================================================

/// MCP tool call succeeds even when telemetry endpoint returns 500.
/// Verifies fire-and-forget: telemetry errors never stall or fail the tool.
#[tokio::test]
async fn mcp_tool_succeeds_when_telemetry_endpoint_errors() {
    use rmcp::ServiceExt;
    use rmcp::model::CallToolRequestParams;
    use rmcp::transport::{ConfigureCommandExt, TokioChildProcess};
    use tokio::process::Command;

    let harness = TestHarness::new().await;
    farms::mock_list_farms(
        &harness.server,
        &[json!({"farmId": "farm-aaa", "displayName": "Test Farm"})],
    )
    .await;

    // Telemetry endpoint returns 500 — should not affect tool result
    telemetry::mock_telemetry_endpoint_error_500(&harness.server).await;

    let bin = assert_cmd::cargo::cargo_bin("deadline");
    let ep = harness.endpoint_url();
    let config_path = harness.config_path.clone();
    let home = harness.config_dir.path().to_str().unwrap().to_owned();

    let transport = TokioChildProcess::new(Command::new(&bin).configure(move |cmd| {
        cmd.arg("mcp-server");
        cmd.env("AWS_ENDPOINT_URL_DEADLINE", &ep);
        cmd.env("AWS_ACCESS_KEY_ID", "AKIAIOSFODNN7EXAMPLE");
        cmd.env(
            "AWS_SECRET_ACCESS_KEY",
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        );
        cmd.env("AWS_DEFAULT_REGION", "us-west-2");
        cmd.env("DEADLINE_CONFIG_FILE_PATH", &config_path);
        cmd.env("HOME", &home);
        for var in &[
            "AWS_PROFILE",
            "AWS_DEFAULT_PROFILE",
            "AWS_CONFIG_FILE",
            "AWS_SHARED_CREDENTIALS_FILE",
            "AWS_SESSION_TOKEN",
            "AWS_SECURITY_TOKEN",
            "AWS_ENDPOINT_URL",
        ] {
            cmd.env_remove(var);
        }
    }))
    .expect("failed to spawn mcp-server");

    let client = ().serve(transport).await.expect("failed to initialize MCP client");

    // Tool call should succeed despite telemetry errors
    let result = client
        .call_tool(CallToolRequestParams::new("deadline_list_farms"))
        .await
        .expect("call_tool should succeed even with telemetry errors");

    let text = result
        .content
        .first()
        .and_then(|c| c.raw.as_text())
        .expect("expected text content");
    assert!(
        text.text.contains("farm-aaa"),
        "tool should return farm data"
    );

    client.cancel().await.unwrap();
}

/// sync-output succeeds even when telemetry endpoint returns 500.
/// Verifies fire-and-forget for the new `queue_sync_output_stats` event.
#[tokio::test]
async fn sync_output_succeeds_when_telemetry_endpoint_errors() {
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
    jobs::mock_search_jobs(&harness.server, FARM, &[], 0).await;

    // Telemetry returns 500 — command should still succeed
    telemetry::mock_telemetry_endpoint_error_500(&harness.server).await;

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
