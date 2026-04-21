//! Level 2 tests for `deadline queue sync-output`.
//!
//! Tests exercise the full orchestration through the CLI binary:
//! SearchJobs → GetJob → ListSessions → ListSessionActions → S3 download.

use deadline_test_server::deadline_api::{errors, jobs, queues, queue_resources, sessions, s3, sts, telemetry};
use deadline_test_server::TestHarness;
use insta_cmd::assert_cmd_snapshot;
use serde_json::json;
use std::fs;
use tempfile::TempDir;

// =========================================================================
// Shared test data
// =========================================================================

fn queue_with_attachments() -> serde_json::Value {
    json!({
        "queueId": "queue-aaa",
        "displayName": "Test Queue",
        "farmId": "farm-abc",
        "status": "SCHEDULING",
        "defaultBudgetAction": "NONE",
        "jobAttachmentSettings": {
            "s3BucketName": "my-bucket",
            "rootPrefix": "DeadlineCloud"
        },
        "createdAt": "2024-06-15T10:30:00Z",
        "createdBy": "user"
    })
}

fn queue_without_attachments() -> serde_json::Value {
    json!({
        "queueId": "queue-aaa",
        "displayName": "Test Queue",
        "farmId": "farm-abc",
        "status": "SCHEDULING",
        "defaultBudgetAction": "NONE",
        "createdAt": "2024-06-15T10:30:00Z",
        "createdBy": "user"
    })
}

fn storage_profile() -> serde_json::Value {
    json!({
        "storageProfileId": "sp-linux-123",
        "displayName": "Linux Profile",
        "osFamily": "linux",
        "fileSystemLocations": [
            {"name": "shared", "path": "/mnt/shared", "type": "SHARED"}
        ]
    })
}

fn active_job(job_id: &str, name: &str, succeeded: i64, ready: i64) -> serde_json::Value {
    json!({
        "jobId": job_id,
        "name": name,
        "taskRunStatus": "READY",
        "taskRunStatusCounts": {"SUCCEEDED": succeeded, "READY": ready, "FAILED": 0},
        "createdAt": "2024-06-15T10:00:00Z",
        "createdBy": "user"
    })
}

fn job_detail_with_attachments(job_id: &str, storage_profile_id: Option<&str>) -> serde_json::Value {
    let mut j = json!({
        "jobId": job_id,
        "name": "Test Job",
        "attachments": {
            "manifests": [{
                "rootPath": "/mnt/shared",
                "rootPathFormat": "posix",
                "fileSystemLocationName": ""
            }],
            "fileSystem": "COPIED"
        }
    });
    if let Some(sp) = storage_profile_id {
        j["storageProfileId"] = json!(sp);
    }
    j
}

fn job_detail_no_attachments(job_id: &str) -> serde_json::Value {
    json!({
        "jobId": job_id,
        "name": "No Attachments Job"
    })
}

async fn setup_config(harness: &TestHarness) {
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.queue_id", "queue-aaa"]).assert().success();
}

async fn setup_config_with_storage_profile(harness: &TestHarness) {
    setup_config(harness).await;
    harness.cli(&["config", "set", "settings.storage_profile_id", "sp-linux-123"]).assert().success();
}

fn timestamp_filters() -> insta::Settings {
    let mut settings = insta::Settings::clone_current();
    settings.add_filter(r"From: .*", "From: [TIMESTAMP]");
    settings.add_filter(r"To: .*", "To: [TIMESTAMP]");
    settings.add_filter(r"Length: .*", "Length: [DURATION]");
    settings.add_filter(r"Initializing from: .*", "Initializing from: [TIMESTAMP]");
    settings.add_filter(r"Continuing from: .*", "Continuing from: [TIMESTAMP]");
    settings.add_filter(r"Checkpoint: .*", "Checkpoint: [PATH]");
    settings.add_filter(r"\.\.\.retrieval completed.*", "...retrieval completed");
    settings.add_filter(r"\.\.\.categorization completed.*", "...categorization completed");
    settings.add_filter(r"\.\.\.downloaded manifests in.*", "...downloaded manifests in [DURATION]");
    settings.add_filter(r"\.\.\.downloaded in.*", "...downloaded in [DURATION]");
    settings.add_filter(r"\.\.\.populated in.*", "...populated in [DURATION]");
    settings
}

// =========================================================================
// Error path tests — validation short-circuits
// =========================================================================

// Both --storage-profile-id and --ignore-storage-profiles
#[tokio::test]
async fn sync_output_storage_profile_and_ignore_mutual_exclusion() {
    let harness = TestHarness::new().await;
    setup_config(&harness).await;
    let checkpoint_dir = TempDir::new().unwrap();

    assert_cmd_snapshot!(harness.cmd(&[
        "queue", "sync-output",
        "--storage-profile-id", "sp-123",
        "--ignore-storage-profiles",
        "--checkpoint-dir", checkpoint_dir.path().to_str().unwrap(),
    ]));
}

// No storage profile configured
#[tokio::test]
async fn sync_output_no_storage_profile_configured_returns_error() {
    let harness = TestHarness::new().await;
    setup_config(&harness).await;
    let checkpoint_dir = TempDir::new().unwrap();

    assert_cmd_snapshot!(harness.cmd(&[
        "queue", "sync-output",
        "--checkpoint-dir", checkpoint_dir.path().to_str().unwrap(),
    ]));
}

// Checkpoint storage profile mismatch
#[tokio::test]
async fn sync_output_checkpoint_storage_profile_mismatch_returns_error() {
    let harness = TestHarness::new().await;
    setup_config_with_storage_profile(&harness).await;
    let checkpoint_dir = TempDir::new().unwrap();

    queues::mock_get_queue(&harness.server, "farm-abc", queue_with_attachments()).await;
    queue_resources::mock_get_storage_profile_for_queue(
        &harness.server, "farm-abc", "queue-aaa", "sp-linux-123", storage_profile(),
    ).await;

    let checkpoint_file = checkpoint_dir.path().join("queue-aaa_sp-linux-123_download_checkpoint.json");
    fs::write(&checkpoint_file, serde_json::to_string_pretty(&json!({
        "localStorageProfileId": "sp-OTHER-999",
        "downloadsStartedTimestamp": "2024-06-15T10:00:00Z",
        "downloadsCompletedTimestamp": "2024-06-15T10:00:00Z",
        "eventualConsistencyMaxSeconds": 120,
        "jobs": []
    })).unwrap()).unwrap();

    let mut settings = insta::Settings::clone_current();
    settings.add_filter(r"Checkpoint: .*", "Checkpoint: [PATH]");
    let _guard = settings.bind_to_scope();

    assert_cmd_snapshot!(harness.cmd(&[
        "queue", "sync-output",
        "--checkpoint-dir", checkpoint_dir.path().to_str().unwrap(),
    ]));
}

// Queue has no job attachment settings
#[tokio::test]
async fn sync_output_queue_no_attachments_returns_error() {
    let harness = TestHarness::new().await;
    setup_config_with_storage_profile(&harness).await;
    let checkpoint_dir = TempDir::new().unwrap();

    queues::mock_get_queue(&harness.server, "farm-abc", queue_without_attachments()).await;
    queue_resources::mock_get_storage_profile_for_queue(
        &harness.server, "farm-abc", "queue-aaa", "sp-linux-123", storage_profile(),
    ).await;

    assert_cmd_snapshot!(harness.cmd(&[
        "queue", "sync-output",
        "--checkpoint-dir", checkpoint_dir.path().to_str().unwrap(),
    ]));
}

// Checkpoint directory not writable
#[tokio::test]
async fn sync_output_checkpoint_dir_not_writable_returns_error() {
    let harness = TestHarness::new().await;
    setup_config_with_storage_profile(&harness).await;

    assert_cmd_snapshot!(harness.cmd(&[
        "queue", "sync-output",
        "--checkpoint-dir", "/nonexistent/readonly/dir",
    ]));
}

// PID lock prevents concurrent runs
#[tokio::test]
async fn sync_output_pid_lock_prevents_concurrent_runs() {
    let harness = TestHarness::new().await;
    setup_config_with_storage_profile(&harness).await;
    let checkpoint_dir = TempDir::new().unwrap();

    queues::mock_get_queue(&harness.server, "farm-abc", queue_with_attachments()).await;
    queue_resources::mock_get_storage_profile_for_queue(
        &harness.server, "farm-abc", "queue-aaa", "sp-linux-123", storage_profile(),
    ).await;

    // Create PID lock with current process PID (which is alive)
    let pid_file = checkpoint_dir.path().join("queue-aaa_sp-linux-123_download_checkpoint.json.pid");
    fs::write(&pid_file, format!("{}", std::process::id())).unwrap();

    let mut settings = insta::Settings::clone_current();
    settings.add_filter(r"pid \d+", "pid [PID]");
    settings.add_filter(checkpoint_dir.path().to_str().unwrap(), "[CHECKPOINT_DIR]");
    let _guard = settings.bind_to_scope();

    assert_cmd_snapshot!(harness.cmd(&[
        "queue", "sync-output",
        "--checkpoint-dir", checkpoint_dir.path().to_str().unwrap(),
    ]));
}

// =========================================================================
// Happy path: first run, no jobs (spec #14 — empty SearchJobs)
// =========================================================================

#[tokio::test]
async fn sync_output_first_run_no_jobs() {
    let harness = TestHarness::new().await;
    setup_config_with_storage_profile(&harness).await;
    let checkpoint_dir = TempDir::new().unwrap();

    queues::mock_get_queue(&harness.server, "farm-abc", queue_with_attachments()).await;
    queue_resources::mock_get_storage_profile_for_queue(
        &harness.server, "farm-abc", "queue-aaa", "sp-linux-123", storage_profile(),
    ).await;
    telemetry::mock_telemetry_endpoint(&harness.server).await;
    sts::mock_get_caller_identity(&harness.server).await;
    s3::mock_s3_list_empty(&harness.server).await;
    jobs::mock_search_jobs(&harness.server, "farm-abc", &[], 0).await;

    let _guard = timestamp_filters().bind_to_scope();

    assert_cmd_snapshot!(harness.cmd(&[
        "queue", "sync-output",
        "--bootstrap-lookback-minutes", "60",
        "--checkpoint-dir", checkpoint_dir.path().to_str().unwrap(),
    ]));

    // Verify checkpoint was saved
    let checkpoint_file = checkpoint_dir.path().join("queue-aaa_sp-linux-123_download_checkpoint.json");
    assert!(checkpoint_file.exists(), "checkpoint file should be saved");
}

// =========================================================================
// Happy path: first run with 1 new job (spec #14 — full categorization)
// =========================================================================

#[tokio::test]
async fn sync_output_first_run_one_new_job_with_attachments() {
    let harness = TestHarness::new().await;
    setup_config(&harness).await;
    let checkpoint_dir = TempDir::new().unwrap();

    queues::mock_get_queue(&harness.server, "farm-abc", queue_with_attachments()).await;
    telemetry::mock_telemetry_endpoint(&harness.server).await;
    sts::mock_get_caller_identity(&harness.server).await;
    s3::mock_s3_list_empty(&harness.server).await;

    // SearchJobs returns 1 active job with SUCCEEDED tasks
    let job = active_job("job-001", "Render Job", 3, 2);
    jobs::mock_search_jobs(&harness.server, "farm-abc", &[job], 1).await;

    // GetJob returns attachments
    jobs::mock_get_job(&harness.server, "farm-abc", "queue-aaa",
        job_detail_with_attachments("job-001", None)).await;

    // ListSessions returns empty (no sessions yet)
    sessions::mock_list_sessions(&harness.server, "farm-abc", "queue-aaa", "job-001", &[]).await;

    let _guard = timestamp_filters().bind_to_scope();

    assert_cmd_snapshot!(harness.cmd(&[
        "queue", "sync-output",
        "--ignore-storage-profiles",
        "--checkpoint-dir", checkpoint_dir.path().to_str().unwrap(),
    ]));
}

// =========================================================================
// Happy path: job without attachments → categorized as attachments-free
// =========================================================================

#[tokio::test]
async fn sync_output_job_without_attachments_skipped() {
    let harness = TestHarness::new().await;
    setup_config(&harness).await;
    let checkpoint_dir = TempDir::new().unwrap();

    queues::mock_get_queue(&harness.server, "farm-abc", queue_with_attachments()).await;
    telemetry::mock_telemetry_endpoint(&harness.server).await;
    sts::mock_get_caller_identity(&harness.server).await;
    s3::mock_s3_list_empty(&harness.server).await;

    let job = active_job("job-noatt", "No Attachments Job", 1, 0);
    jobs::mock_search_jobs(&harness.server, "farm-abc", &[job], 1).await;

    // GetJob returns no attachments
    jobs::mock_get_job(&harness.server, "farm-abc", "queue-aaa",
        job_detail_no_attachments("job-noatt")).await;

    let _guard = timestamp_filters().bind_to_scope();

    assert_cmd_snapshot!(harness.cmd(&[
        "queue", "sync-output",
        "--ignore-storage-profiles",
        "--checkpoint-dir", checkpoint_dir.path().to_str().unwrap(),
    ]));
}

// =========================================================================
// Happy path: --ignore-storage-profiles (spec #17)
// =========================================================================

#[tokio::test]
async fn sync_output_ignore_storage_profiles() {
    let harness = TestHarness::new().await;
    setup_config(&harness).await;
    let checkpoint_dir = TempDir::new().unwrap();

    queues::mock_get_queue(&harness.server, "farm-abc", queue_with_attachments()).await;
    telemetry::mock_telemetry_endpoint(&harness.server).await;
    sts::mock_get_caller_identity(&harness.server).await;
    s3::mock_s3_list_empty(&harness.server).await;
    jobs::mock_search_jobs(&harness.server, "farm-abc", &[], 0).await;

    let _guard = timestamp_filters().bind_to_scope();

    assert_cmd_snapshot!(harness.cmd(&[
        "queue", "sync-output",
        "--ignore-storage-profiles",
        "--checkpoint-dir", checkpoint_dir.path().to_str().unwrap(),
    ]));
}

// =========================================================================
// Happy path: subsequent run resumes from checkpoint (spec #15)
// =========================================================================

#[tokio::test]
async fn sync_output_subsequent_run_resumes_from_checkpoint() {
    let harness = TestHarness::new().await;
    setup_config_with_storage_profile(&harness).await;
    let checkpoint_dir = TempDir::new().unwrap();

    queues::mock_get_queue(&harness.server, "farm-abc", queue_with_attachments()).await;
    queue_resources::mock_get_storage_profile_for_queue(
        &harness.server, "farm-abc", "queue-aaa", "sp-linux-123", storage_profile(),
    ).await;
    telemetry::mock_telemetry_endpoint(&harness.server).await;
    sts::mock_get_caller_identity(&harness.server).await;
    s3::mock_s3_list_empty(&harness.server).await;
    jobs::mock_search_jobs(&harness.server, "farm-abc", &[], 0).await;

    // Write existing checkpoint
    let checkpoint_file = checkpoint_dir.path().join("queue-aaa_sp-linux-123_download_checkpoint.json");
    fs::write(&checkpoint_file, serde_json::to_string_pretty(&json!({
        "localStorageProfileId": "sp-linux-123",
        "downloadsStartedTimestamp": "2024-06-15T10:00:00Z",
        "downloadsCompletedTimestamp": "2024-06-15T11:00:00Z",
        "eventualConsistencyMaxSeconds": 120,
        "jobs": []
    })).unwrap()).unwrap();

    let _guard = timestamp_filters().bind_to_scope();

    assert_cmd_snapshot!(harness.cmd(&[
        "queue", "sync-output",
        "--checkpoint-dir", checkpoint_dir.path().to_str().unwrap(),
    ]));
}

// =========================================================================
// Happy path: --force-bootstrap overwrites checkpoint (spec #16)
// =========================================================================

#[tokio::test]
async fn sync_output_force_bootstrap_overwrites_checkpoint() {
    let harness = TestHarness::new().await;
    setup_config_with_storage_profile(&harness).await;
    let checkpoint_dir = TempDir::new().unwrap();

    queues::mock_get_queue(&harness.server, "farm-abc", queue_with_attachments()).await;
    queue_resources::mock_get_storage_profile_for_queue(
        &harness.server, "farm-abc", "queue-aaa", "sp-linux-123", storage_profile(),
    ).await;
    telemetry::mock_telemetry_endpoint(&harness.server).await;
    sts::mock_get_caller_identity(&harness.server).await;
    s3::mock_s3_list_empty(&harness.server).await;
    jobs::mock_search_jobs(&harness.server, "farm-abc", &[], 0).await;

    // Write existing checkpoint
    let checkpoint_file = checkpoint_dir.path().join("queue-aaa_sp-linux-123_download_checkpoint.json");
    fs::write(&checkpoint_file, serde_json::to_string_pretty(&json!({
        "localStorageProfileId": "sp-linux-123",
        "downloadsStartedTimestamp": "2024-06-15T10:00:00Z",
        "downloadsCompletedTimestamp": "2024-06-15T11:00:00Z",
        "eventualConsistencyMaxSeconds": 120,
        "jobs": []
    })).unwrap()).unwrap();

    let _guard = timestamp_filters().bind_to_scope();

    assert_cmd_snapshot!(harness.cmd(&[
        "queue", "sync-output",
        "--force-bootstrap",
        "--bootstrap-lookback-minutes", "30",
        "--checkpoint-dir", checkpoint_dir.path().to_str().unwrap(),
    ]));
}

// =========================================================================
// Happy path: --dry-run (spec #23)
// =========================================================================

#[tokio::test]
async fn sync_output_dry_run_does_not_save_checkpoint() {
    let harness = TestHarness::new().await;
    setup_config(&harness).await;
    let checkpoint_dir = TempDir::new().unwrap();

    queues::mock_get_queue(&harness.server, "farm-abc", queue_with_attachments()).await;
    telemetry::mock_telemetry_endpoint(&harness.server).await;
    sts::mock_get_caller_identity(&harness.server).await;
    s3::mock_s3_list_empty(&harness.server).await;

    let job = active_job("job-dry", "Dry Run Job", 1, 1);
    jobs::mock_search_jobs(&harness.server, "farm-abc", &[job], 1).await;
    jobs::mock_get_job(&harness.server, "farm-abc", "queue-aaa",
        job_detail_with_attachments("job-dry", None)).await;
    sessions::mock_list_sessions(&harness.server, "farm-abc", "queue-aaa", "job-dry", &[]).await;

    let _guard = timestamp_filters().bind_to_scope();

    assert_cmd_snapshot!(harness.cmd(&[
        "queue", "sync-output",
        "--dry-run",
        "--ignore-storage-profiles",
        "--checkpoint-dir", checkpoint_dir.path().to_str().unwrap(),
    ]));

    // Verify no checkpoint file was created
    let entries: Vec<_> = fs::read_dir(checkpoint_dir.path()).unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map_or(false, |ext| ext == "json"))
        .collect();
    assert!(entries.is_empty(), "dry-run should not save checkpoint file");
}

// =========================================================================
// Happy path: --conflict-resolution SKIP (spec #24)
// =========================================================================

#[tokio::test]
async fn sync_output_conflict_resolution_skip() {
    let harness = TestHarness::new().await;
    setup_config_with_storage_profile(&harness).await;
    let checkpoint_dir = TempDir::new().unwrap();

    queues::mock_get_queue(&harness.server, "farm-abc", queue_with_attachments()).await;
    queue_resources::mock_get_storage_profile_for_queue(
        &harness.server, "farm-abc", "queue-aaa", "sp-linux-123", storage_profile(),
    ).await;
    telemetry::mock_telemetry_endpoint(&harness.server).await;
    sts::mock_get_caller_identity(&harness.server).await;
    s3::mock_s3_list_empty(&harness.server).await;
    jobs::mock_search_jobs(&harness.server, "farm-abc", &[], 0).await;

    let _guard = timestamp_filters().bind_to_scope();

    assert_cmd_snapshot!(harness.cmd(&[
        "queue", "sync-output",
        "--conflict-resolution", "SKIP",
        "--checkpoint-dir", checkpoint_dir.path().to_str().unwrap(),
    ]));
}

// =========================================================================
// Happy path: --json flag (spec #25)
// =========================================================================

#[tokio::test]
async fn sync_output_json_output() {
    let harness = TestHarness::new().await;
    setup_config(&harness).await;
    let checkpoint_dir = TempDir::new().unwrap();

    queues::mock_get_queue(&harness.server, "farm-abc", queue_with_attachments()).await;
    telemetry::mock_telemetry_endpoint(&harness.server).await;
    sts::mock_get_caller_identity(&harness.server).await;
    s3::mock_s3_list_empty(&harness.server).await;
    jobs::mock_search_jobs(&harness.server, "farm-abc", &[], 0).await;

    let _guard = timestamp_filters().bind_to_scope();

    assert_cmd_snapshot!(harness.cmd(&[
        "queue", "sync-output",
        "--json",
        "--ignore-storage-profiles",
        "--checkpoint-dir", checkpoint_dir.path().to_str().unwrap(),
    ]));
}

// =========================================================================
// Happy path: job with session actions but no output manifests
// =========================================================================

#[tokio::test]
async fn sync_output_job_with_session_actions_no_manifests() {
    let harness = TestHarness::new().await;
    setup_config(&harness).await;
    let checkpoint_dir = TempDir::new().unwrap();

    queues::mock_get_queue(&harness.server, "farm-abc", queue_with_attachments()).await;
    telemetry::mock_telemetry_endpoint(&harness.server).await;
    sts::mock_get_caller_identity(&harness.server).await;
    s3::mock_s3_list_empty(&harness.server).await;

    let job = active_job("job-sa", "Session Action Job", 1, 1);
    jobs::mock_search_jobs(&harness.server, "farm-abc", &[job], 1).await;
    jobs::mock_get_job(&harness.server, "farm-abc", "queue-aaa",
        job_detail_with_attachments("job-sa", None)).await;

    // Session with one succeeded taskRun action
    sessions::mock_list_sessions(&harness.server, "farm-abc", "queue-aaa", "job-sa", &[
        json!({
            "sessionId": "session-001",
            "fleetId": "fleet-001",
            "workerId": "worker-001",
            "startedAt": "2024-06-15T10:00:00Z",
            "lifecycleStatus": "STARTED"
        })
    ]).await;

    sessions::mock_list_session_actions(
        &harness.server, "farm-abc", "queue-aaa", "job-sa", "session-001",
        &[json!({
            "sessionActionId": "sessionaction-001-0",
            "status": "SUCCEEDED",
            "startedAt": "2024-06-15T10:01:00Z",
            "endedAt": "2024-06-15T10:02:00Z",
            "definition": {
                "taskRun": {
                    "taskId": "task-001",
                    "stepId": "step-001"
                }
            }
        })]
    ).await;

    let _guard = timestamp_filters().bind_to_scope();

    assert_cmd_snapshot!(harness.cmd(&[
        "queue", "sync-output",
        "--ignore-storage-profiles",
        "--checkpoint-dir", checkpoint_dir.path().to_str().unwrap(),
    ]));
}

// =========================================================================
// Happy path: SearchJobs API fails → error
// =========================================================================

#[tokio::test]
async fn sync_output_search_jobs_fails_returns_error() {
    let harness = TestHarness::new().await;
    setup_config(&harness).await;
    let checkpoint_dir = TempDir::new().unwrap();

    queues::mock_get_queue(&harness.server, "farm-abc", queue_with_attachments()).await;
    telemetry::mock_telemetry_endpoint(&harness.server).await;
    sts::mock_get_caller_identity(&harness.server).await;
    s3::mock_s3_list_empty(&harness.server).await;

    // Mock SearchJobs with access denied error
    errors::mock_search_jobs_access_denied(&harness.server, "farm-abc").await;

    let _guard = timestamp_filters().bind_to_scope();

    assert_cmd_snapshot!(harness.cmd(&[
        "queue", "sync-output",
        "--ignore-storage-profiles",
        "--checkpoint-dir", checkpoint_dir.path().to_str().unwrap(),
    ]));
}

// =========================================================================
// Multi-run lifecycle: job goes NEW → UNCHANGED across two runs
// =========================================================================

#[tokio::test]
async fn sync_output_multi_run_job_unchanged_on_second_run() {
    let harness = TestHarness::new().await;
    setup_config(&harness).await;
    let checkpoint_dir = TempDir::new().unwrap();

    queues::mock_get_queue(&harness.server, "farm-abc", queue_with_attachments()).await;
    telemetry::mock_telemetry_endpoint(&harness.server).await;
    sts::mock_get_caller_identity(&harness.server).await;
    s3::mock_s3_list_empty(&harness.server).await;

    // Job with 1 succeeded task
    let job = active_job("job-multi", "Multi Run Job", 1, 1);
    jobs::mock_search_jobs(&harness.server, "farm-abc", &[job], 1).await;
    jobs::mock_get_job(&harness.server, "farm-abc", "queue-aaa",
        job_detail_with_attachments("job-multi", None)).await;
    sessions::mock_list_sessions(&harness.server, "farm-abc", "queue-aaa", "job-multi", &[]).await;

    // Run 1: bootstrap
    harness.cli(&[
        "queue", "sync-output",
        "--ignore-storage-profiles",
        "--checkpoint-dir", checkpoint_dir.path().to_str().unwrap(),
    ]).assert().success();

    // Verify checkpoint was saved
    let checkpoint_file = checkpoint_dir.path().join("queue-aaa_ignore-storage-profiles_download_checkpoint.json");
    assert!(checkpoint_file.exists(), "checkpoint should exist after run 1");

    // Run 2: same job, same state → should be UNCHANGED
    let output = harness.cli(&[
        "queue", "sync-output",
        "--ignore-storage-profiles",
        "--checkpoint-dir", checkpoint_dir.path().to_str().unwrap(),
    ]).output().expect("run 2 should execute");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "run 2 should succeed: {stderr}");
    assert!(stderr.contains("Checkpoint found"), "should load checkpoint: {stderr}");
    assert!(stderr.contains("UNCHANGED Job: Multi Run Job (job-multi)"),
        "job should be unchanged on second run: {stderr}");
    assert!(stderr.contains("unchanged: 1"), "summary should show 1 unchanged: {stderr}");
}

// =========================================================================
// Multi-run: job EXISTING (task count changed)
// =========================================================================

#[tokio::test]
async fn sync_output_multi_run_job_existing_task_count_changed() {
    let harness = TestHarness::new().await;
    setup_config(&harness).await;
    let checkpoint_dir = TempDir::new().unwrap();

    queues::mock_get_queue(&harness.server, "farm-abc", queue_with_attachments()).await;
    telemetry::mock_telemetry_endpoint(&harness.server).await;
    sts::mock_get_caller_identity(&harness.server).await;
    s3::mock_s3_list_empty(&harness.server).await;

    // Run 1: job with 1/3 succeeded
    let job1 = active_job("job-ex", "Existing Job", 1, 2);
    jobs::mock_search_jobs(&harness.server, "farm-abc", &[job1], 1).await;
    jobs::mock_get_job(&harness.server, "farm-abc", "queue-aaa",
        job_detail_with_attachments("job-ex", None)).await;
    sessions::mock_list_sessions(&harness.server, "farm-abc", "queue-aaa", "job-ex", &[]).await;

    harness.cli(&[
        "queue", "sync-output", "--ignore-storage-profiles",
        "--checkpoint-dir", checkpoint_dir.path().to_str().unwrap(),
    ]).assert().success();

    // Run 2: same job now has 2/3 succeeded
    harness.server.reset().await;
    queues::mock_get_queue(&harness.server, "farm-abc", queue_with_attachments()).await;
    telemetry::mock_telemetry_endpoint(&harness.server).await;
    sts::mock_get_caller_identity(&harness.server).await;
    s3::mock_s3_list_empty(&harness.server).await;
    let job2 = active_job("job-ex", "Existing Job", 2, 1);
    jobs::mock_search_jobs(&harness.server, "farm-abc", &[job2], 1).await;
    sessions::mock_list_sessions(&harness.server, "farm-abc", "queue-aaa", "job-ex", &[]).await;

    let output = harness.cli(&[
        "queue", "sync-output", "--ignore-storage-profiles",
        "--checkpoint-dir", checkpoint_dir.path().to_str().unwrap(),
    ]).output().expect("run 2");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "run 2 should succeed: {stderr}");
    assert!(stderr.contains("EXISTING Job: Existing Job (job-ex)"), "should show EXISTING: {stderr}");
    assert!(stderr.contains("Succeeded tasks (before): 1 / 3"), "should show before count: {stderr}");
    assert!(stderr.contains("Succeeded tasks (now)   : 2 / 3"), "should show after count: {stderr}");
    assert!(stderr.contains("updated: 1"), "summary should show 1 updated: {stderr}");
}

// =========================================================================
// Multi-run: job FINISHED TRACKING (job succeeded then dropped)
// =========================================================================

#[tokio::test]
async fn sync_output_multi_run_job_finished_tracking_succeeded() {
    let harness = TestHarness::new().await;
    setup_config(&harness).await;
    let checkpoint_dir = TempDir::new().unwrap();

    queues::mock_get_queue(&harness.server, "farm-abc", queue_with_attachments()).await;
    telemetry::mock_telemetry_endpoint(&harness.server).await;
    sts::mock_get_caller_identity(&harness.server).await;
    s3::mock_s3_list_empty(&harness.server).await;

    // Run 1: job with all tasks succeeded
    let mut job1 = active_job("job-fin", "Finished Job", 2, 0);
    job1["endedAt"] = json!("2024-06-15T11:00:00Z");
    job1["taskRunStatus"] = json!("SUCCEEDED");
    jobs::mock_search_jobs(&harness.server, "farm-abc", &[job1], 1).await;
    jobs::mock_get_job(&harness.server, "farm-abc", "queue-aaa",
        job_detail_with_attachments("job-fin", None)).await;
    sessions::mock_list_sessions(&harness.server, "farm-abc", "queue-aaa", "job-fin", &[]).await;

    harness.cli(&[
        "queue", "sync-output", "--ignore-storage-profiles",
        "--checkpoint-dir", checkpoint_dir.path().to_str().unwrap(),
    ]).assert().success();

    // Run 2: job no longer in SearchJobs results → FINISHED TRACKING
    harness.server.reset().await;
    queues::mock_get_queue(&harness.server, "farm-abc", queue_with_attachments()).await;
    telemetry::mock_telemetry_endpoint(&harness.server).await;
    sts::mock_get_caller_identity(&harness.server).await;
    s3::mock_s3_list_empty(&harness.server).await;
    jobs::mock_search_jobs(&harness.server, "farm-abc", &[], 0).await;

    let output = harness.cli(&[
        "queue", "sync-output", "--ignore-storage-profiles",
        "--checkpoint-dir", checkpoint_dir.path().to_str().unwrap(),
    ]).output().expect("run 2");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "run 2 should succeed: {stderr}");
    assert!(stderr.contains("FINISHED TRACKING Job: Finished Job (job-fin)"),
        "should show FINISHED TRACKING: {stderr}");
    assert!(stderr.contains("Job succeeded"), "should say job succeeded: {stderr}");
    assert!(stderr.contains("inactive: 1"), "summary should show 1 inactive: {stderr}");
}

// =========================================================================
// Multi-run: job canceled (dropped before completion)
// =========================================================================

#[tokio::test]
async fn sync_output_multi_run_job_canceled() {
    let harness = TestHarness::new().await;
    setup_config(&harness).await;
    let checkpoint_dir = TempDir::new().unwrap();

    queues::mock_get_queue(&harness.server, "farm-abc", queue_with_attachments()).await;
    telemetry::mock_telemetry_endpoint(&harness.server).await;
    sts::mock_get_caller_identity(&harness.server).await;
    s3::mock_s3_list_empty(&harness.server).await;

    // Run 1: job with 1/2 succeeded (still running)
    let job1 = active_job("job-can", "Canceled Job", 1, 1);
    jobs::mock_search_jobs(&harness.server, "farm-abc", &[job1], 1).await;
    jobs::mock_get_job(&harness.server, "farm-abc", "queue-aaa",
        job_detail_with_attachments("job-can", None)).await;
    sessions::mock_list_sessions(&harness.server, "farm-abc", "queue-aaa", "job-can", &[]).await;

    harness.cli(&[
        "queue", "sync-output", "--ignore-storage-profiles",
        "--checkpoint-dir", checkpoint_dir.path().to_str().unwrap(),
    ]).assert().success();

    // Run 2: job no longer in SearchJobs → canceled/failed
    harness.server.reset().await;
    queues::mock_get_queue(&harness.server, "farm-abc", queue_with_attachments()).await;
    telemetry::mock_telemetry_endpoint(&harness.server).await;
    sts::mock_get_caller_identity(&harness.server).await;
    s3::mock_s3_list_empty(&harness.server).await;
    jobs::mock_search_jobs(&harness.server, "farm-abc", &[], 0).await;

    let output = harness.cli(&[
        "queue", "sync-output", "--ignore-storage-profiles",
        "--checkpoint-dir", checkpoint_dir.path().to_str().unwrap(),
    ]).output().expect("run 2");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "run 2 should succeed: {stderr}");
    assert!(stderr.contains("FINISHED TRACKING Job: Canceled Job (job-can)"),
        "should show FINISHED TRACKING: {stderr}");
    assert!(stderr.contains("Job is not a download candidate anymore (likely suspended, canceled or failed)"),
        "should explain reason: {stderr}");
    assert!(stderr.contains("inactive: 1"), "summary should show 1 inactive: {stderr}");
}

// =========================================================================
// Multi-run: job without attachments tracked across runs
// =========================================================================

#[tokio::test]
async fn sync_output_multi_run_job_without_attachments_tracked() {
    let harness = TestHarness::new().await;
    setup_config(&harness).await;
    let checkpoint_dir = TempDir::new().unwrap();

    queues::mock_get_queue(&harness.server, "farm-abc", queue_with_attachments()).await;
    telemetry::mock_telemetry_endpoint(&harness.server).await;
    sts::mock_get_caller_identity(&harness.server).await;
    s3::mock_s3_list_empty(&harness.server).await;

    // Run 1: job without attachments
    let job1 = active_job("job-noatt2", "No Att Job", 1, 0);
    jobs::mock_search_jobs(&harness.server, "farm-abc", &[job1.clone()], 1).await;
    jobs::mock_get_job(&harness.server, "farm-abc", "queue-aaa",
        job_detail_no_attachments("job-noatt2")).await;

    harness.cli(&[
        "queue", "sync-output", "--ignore-storage-profiles",
        "--checkpoint-dir", checkpoint_dir.path().to_str().unwrap(),
    ]).assert().success();

    // Run 2: same job still active → should be attachments-free again (not call GetJob)
    harness.server.reset().await;
    queues::mock_get_queue(&harness.server, "farm-abc", queue_with_attachments()).await;
    telemetry::mock_telemetry_endpoint(&harness.server).await;
    sts::mock_get_caller_identity(&harness.server).await;
    s3::mock_s3_list_empty(&harness.server).await;
    jobs::mock_search_jobs(&harness.server, "farm-abc", &[job1], 1).await;
    // Note: NOT mocking GetJob — if the code calls it, the test will fail

    let output = harness.cli(&[
        "queue", "sync-output", "--ignore-storage-profiles",
        "--checkpoint-dir", checkpoint_dir.path().to_str().unwrap(),
    ]).output().expect("run 2");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "run 2 should succeed without calling GetJob: {stderr}");
    assert!(stderr.contains("not using job attachments: 1"),
        "should still count as attachments-free: {stderr}");
}

// =========================================================================
// Storage profile path mapping rule printing
// =========================================================================

#[tokio::test]
async fn sync_output_storage_profile_path_mapping_rules_printed() {
    let harness = TestHarness::new().await;
    setup_config_with_storage_profile(&harness).await;
    let checkpoint_dir = TempDir::new().unwrap();

    queues::mock_get_queue(&harness.server, "farm-abc", queue_with_attachments()).await;
    telemetry::mock_telemetry_endpoint(&harness.server).await;
    sts::mock_get_caller_identity(&harness.server).await;
    s3::mock_s3_list_empty(&harness.server).await;

    // Local storage profile (Linux)
    queue_resources::mock_get_storage_profile_for_queue(
        &harness.server, "farm-abc", "queue-aaa", "sp-linux-123", storage_profile(),
    ).await;

    // Job submitted from a macOS machine with different storage profile
    let mut job = active_job("job-map", "Mapped Job", 1, 1);
    jobs::mock_search_jobs(&harness.server, "farm-abc", &[job], 1).await;

    let mut job_detail = job_detail_with_attachments("job-map", Some("sp-macos-456"));
    jobs::mock_get_job(&harness.server, "farm-abc", "queue-aaa", job_detail).await;
    sessions::mock_list_sessions(&harness.server, "farm-abc", "queue-aaa", "job-map", &[]).await;

    // Mock the job's storage profile (macOS with different paths)
    queue_resources::mock_get_storage_profile_for_queue(
        &harness.server, "farm-abc", "queue-aaa", "sp-macos-456",
        json!({
            "storageProfileId": "sp-macos-456",
            "displayName": "macOS Profile",
            "osFamily": "macos",
            "fileSystemLocations": [
                {"name": "shared", "path": "/Volumes/shared", "type": "SHARED"}
            ]
        }),
    ).await;

    let output = harness.cli(&[
        "queue", "sync-output",
        "--checkpoint-dir", checkpoint_dir.path().to_str().unwrap(),
    ]).output().expect("should run");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "should succeed: {stderr}");
    assert!(stderr.contains("Local storage profile is Linux Profile (sp-linux-123)"),
        "should print local profile: {stderr}");
    assert!(stderr.contains("Path mapping rules for 1 download candidate jobs with storage profile macOS Profile (sp-macos-456)"),
        "should print mapping header: {stderr}");
    assert!(stderr.contains("- from: /Volumes/shared"), "should print source path: {stderr}");
    assert!(stderr.contains("to:   /mnt/shared"), "should print dest path: {stderr}");
}

// =========================================================================
// sync-output actually downloads files to disk
// =========================================================================

#[tokio::test]
async fn sync_output_downloads_files_to_disk() {
    let harness = TestHarness::new().await;
    setup_config(&harness).await;
    let checkpoint_dir = TempDir::new().unwrap();
    let download_dir = TempDir::new().unwrap();
    let download_root = download_dir.path().to_str().unwrap();

    queues::mock_get_queue(&harness.server, "farm-abc", queue_with_attachments()).await;
    telemetry::mock_telemetry_endpoint(&harness.server).await;
    sts::mock_get_caller_identity(&harness.server).await;

    // Job with 1 succeeded task
    let job = active_job("job-dl", "Download Job", 1, 0);
    jobs::mock_search_jobs(&harness.server, "farm-abc", &[job.clone()], 1).await;

    // GetJob returns attachments with a root path pointing to our temp dir
    let mut job_detail = job_detail_with_attachments("job-dl", None);
    job_detail["attachments"]["manifests"][0]["rootPath"] = json!(download_root);
    jobs::mock_get_job(&harness.server, "farm-abc", "queue-aaa", job_detail).await;

    // Session with one succeeded taskRun action
    sessions::mock_list_sessions(&harness.server, "farm-abc", "queue-aaa", "job-dl", &[
        json!({
            "sessionId": "session-dl1",
            "fleetId": "fleet-001",
            "workerId": "worker-001",
            "startedAt": "2024-06-15T10:00:00Z",
            "lifecycleStatus": "ENDED"
        })
    ]).await;

    sessions::mock_list_session_actions(
        &harness.server, "farm-abc", "queue-aaa", "job-dl", "session-dl1",
        &[json!({
            "sessionActionId": "sessionaction-dl1-0",
            "status": "SUCCEEDED",
            "startedAt": "2024-06-15T10:01:00Z",
            "endedAt": "2024-06-15T10:02:00Z",
            "definition": {
                "taskRun": {
                    "taskId": "task-001",
                    "stepId": "step-001"
                }
            }
        })]
    ).await;

    // Mock S3 ListObjectsV2 for output manifests — realistic key with colons
    let manifest_key = "DeadlineCloud/Manifests/farm-abc/queue-aaa/job-dl/step-001/task-001/2024-06-15T10:02:00Z_sessionaction-dl1-0/abcdef.manifest";
    s3::mock_s3_list_objects(&harness.server, &[manifest_key]).await;

    // Build a manifest JSON with one file
    let file_hash = "aabbccdd11223344aabbccdd11223344";
    let manifest_json = serde_json::to_string(&json!({
        "hashAlg": "xxh128",
        "manifestVersion": "2023-03-03",
        "paths": [{
            "path": "output/render.exr",
            "hash": file_hash,
            "size": 5,
            "mtime": 1700000000000000_i64
        }],
        "totalSize": 5
    })).unwrap();

    // Mock S3 GetObject for the manifest (with asset-root metadata).
    s3::mock_s3_get_object_with_metadata(
        &harness.server,
        &format!("my-bucket/{manifest_key}"),
        manifest_json.as_bytes(),
        &[("asset-root", download_root)],
    ).await;

    // Mock S3 GetObject for the actual file content (CAS path)
    s3::mock_s3_get_object(
        &harness.server,
        &format!("my-bucket/DeadlineCloud/Data/{file_hash}.xxh128"),
        b"hello",
    ).await;

    let _guard = timestamp_filters().bind_to_scope();

    let output = harness.cli(&[
        "queue", "sync-output",
        "--ignore-storage-profiles",
        "--checkpoint-dir", checkpoint_dir.path().to_str().unwrap(),
    ]).output().expect("should run");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "should succeed: {stderr}");

    // The key assertion: files were actually downloaded
    assert!(!stderr.contains("Downloaded files: 0"),
        "should have downloaded files, not 0: {stderr}");
    assert!(stderr.contains("Downloaded files: 1") || stderr.contains("downloaded_files\": 1"),
        "should show 1 downloaded file: {stderr}");

    // Verify the file exists on disk
    let downloaded_file = download_dir.path().join("output/render.exr");
    assert!(downloaded_file.exists(),
        "file should be downloaded to {}", downloaded_file.display());
    assert_eq!(fs::read_to_string(&downloaded_file).unwrap(), "hello",
        "file content should match");
}

// =====================================================================
// Job discovery must paginate beyond 100 jobs
// =====================================================================

/// When SearchJobs returns totalResults > len(jobs), the CLI
/// must paginate using createdAt thresholding to discover all jobs.
/// The current code calls search_jobs_with_filters once with page_size=100
/// and silently drops any jobs beyond the first page.
#[tokio::test]
async fn sync_output_paginates_beyond_100_jobs() {
    let harness = TestHarness::new().await;
    setup_config_with_storage_profile(&harness).await;
    let checkpoint_dir = TempDir::new().unwrap();

    queues::mock_get_queue(&harness.server, "farm-abc", queue_with_attachments()).await;
    queue_resources::mock_get_storage_profile_for_queue(
        &harness.server, "farm-abc", "queue-aaa", "sp-linux-123", storage_profile(),
    ).await;
    sts::mock_get_caller_identity(&harness.server).await;
    telemetry::mock_telemetry_endpoint(&harness.server).await;
    s3::mock_s3_list_empty(&harness.server).await;

    // Build 3 jobs: 2 on the "first page" and 1 that only appears
    // on the "second page" (after createdAt thresholding).
    let job_a = json!({
        "jobId": "job-aaa", "name": "Job A",
        "taskRunStatus": "READY",
        "taskRunStatusCounts": {"SUCCEEDED": 1, "READY": 1},
        "createdAt": "2024-06-15T10:00:00Z", "createdBy": "user"
    });
    let job_b = json!({
        "jobId": "job-bbb", "name": "Job B",
        "taskRunStatus": "READY",
        "taskRunStatusCounts": {"SUCCEEDED": 1, "READY": 0},
        "createdAt": "2024-06-15T11:00:00Z", "createdBy": "user"
    });
    let job_c = json!({
        "jobId": "job-ccc", "name": "Job C",
        "taskRunStatus": "READY",
        "taskRunStatusCounts": {"SUCCEEDED": 1, "READY": 2},
        "createdAt": "2024-06-15T12:00:00Z", "createdBy": "user"
    });

    // Active jobs query: page 1 returns [A, B] with totalResults=3,
    // page 2 (with CREATED_AT >= B.createdAt) returns [B, C].
    // The pagination algorithm deduplicates by jobId.
    // "ANY_EQUALS" marker differentiates active-jobs from ended-jobs queries.
    jobs::mock_search_jobs_paginated(
        &harness.server, "farm-abc",
        "ANY_EQUALS",
        &[job_a, job_b.clone()], 3,
        &[job_b, job_c],
    ).await;

    // Ended jobs query returns empty (separate from active-jobs pagination)
    jobs::mock_search_jobs(&harness.server, "farm-abc", &[], 0).await;

    // GetJob for each new job (to get attachments)
    for job_id in &["job-aaa", "job-bbb", "job-ccc"] {
        jobs::mock_get_job(
            &harness.server, "farm-abc", "queue-aaa",
            job_detail_no_attachments(job_id),
        ).await;
    }

    let _guard = timestamp_filters().bind_to_scope();

    let output = harness.cli(&[
        "queue", "sync-output",
        "--checkpoint-dir", checkpoint_dir.path().to_str().unwrap(),
        "--dry-run",
    ]).output().expect("should run");

    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);

    // The key assertion: all 3 jobs should be discovered (not just 2).
    // Each job has no attachments, so they'll show as "not using job attachments".
    assert!(
        stderr.contains("Job A") && stderr.contains("Job B") && stderr.contains("Job C"),
        "All 3 jobs should be discovered via pagination.\nstderr:\n{stderr}\nstdout:\n{stdout}"
    );
}

// =========================================================================
// SYNC-001: Session action count excludes no-output actions
// =========================================================================

/// Python filters out session actions that have no output manifests before
/// counting `downloaded_session_actions`. The count should only reflect
/// actions that actually produced downloadable output.
///
/// Setup: 1 job with 1 session, 2 succeeded taskRun actions.
/// Action 0 has no output manifest on S3. Action 1 has a manifest.
/// Expected: `Downloaded session actions: 1` (not 2).
#[tokio::test]
async fn sync_output_session_action_count_excludes_no_output_actions() {
    let harness = TestHarness::new().await;
    setup_config(&harness).await;
    let checkpoint_dir = TempDir::new().unwrap();
    let download_dir = TempDir::new().unwrap();
    let download_root = download_dir.path().to_str().unwrap();

    queues::mock_get_queue(&harness.server, "farm-abc", queue_with_attachments()).await;
    telemetry::mock_telemetry_endpoint(&harness.server).await;
    sts::mock_get_caller_identity(&harness.server).await;

    let job = active_job("job-mix", "Mixed Actions Job", 2, 0);
    jobs::mock_search_jobs(&harness.server, "farm-abc", &[job], 1).await;

    let mut job_detail = job_detail_with_attachments("job-mix", None);
    job_detail["attachments"]["manifests"][0]["rootPath"] = json!(download_root);
    jobs::mock_get_job(&harness.server, "farm-abc", "queue-aaa", job_detail).await;

    sessions::mock_list_sessions(&harness.server, "farm-abc", "queue-aaa", "job-mix", &[
        json!({
            "sessionId": "session-mix1",
            "fleetId": "fleet-001",
            "workerId": "worker-001",
            "startedAt": "2024-06-15T10:00:00Z",
            "lifecycleStatus": "ENDED"
        })
    ]).await;

    // Two succeeded taskRun actions
    sessions::mock_list_session_actions(
        &harness.server, "farm-abc", "queue-aaa", "job-mix", "session-mix1",
        &[
            json!({
                "sessionActionId": "sessionaction-mix1-0",
                "status": "SUCCEEDED",
                "startedAt": "2024-06-15T10:01:00Z",
                "endedAt": "2024-06-15T10:02:00Z",
                "definition": { "taskRun": { "taskId": "task-001", "stepId": "step-001" } }
            }),
            json!({
                "sessionActionId": "sessionaction-mix1-1",
                "status": "SUCCEEDED",
                "startedAt": "2024-06-15T10:03:00Z",
                "endedAt": "2024-06-15T10:04:00Z",
                "definition": { "taskRun": { "taskId": "task-002", "stepId": "step-002" } }
            }),
        ]
    ).await;

    // S3: only action 1 has an output manifest; action 0 has none
    let manifest_key = "DeadlineCloud/Manifests/farm-abc/queue-aaa/job-mix/step-002/task-002/2024-06-15T10:04:00Z_sessionaction-mix1-1/abc.manifest";
    s3::mock_s3_list_objects(&harness.server, &[manifest_key]).await;

    let file_hash = "aabbccdd11223344aabbccdd11223344";
    let manifest_json = serde_json::to_string(&json!({
        "hashAlg": "xxh128",
        "manifestVersion": "2023-03-03",
        "paths": [{ "path": "output/result.exr", "hash": file_hash, "size": 10, "mtime": 1700000000000000_i64 }],
        "totalSize": 10
    })).unwrap();

    s3::mock_s3_get_object_with_metadata(
        &harness.server,
        &format!("my-bucket/{manifest_key}"),
        manifest_json.as_bytes(),
        &[("asset-root", download_root)],
    ).await;

    s3::mock_s3_get_object(
        &harness.server,
        &format!("my-bucket/DeadlineCloud/Data/{file_hash}.xxh128"),
        b"resultdata",
    ).await;

    let output = harness.cli(&[
        "queue", "sync-output",
        "--ignore-storage-profiles",
        "--checkpoint-dir", checkpoint_dir.path().to_str().unwrap(),
    ]).output().expect("should run");

    let stderr = String::from_utf8_lossy(&output.stderr);
    // SYNC-001: count should be 1 (only the action with a manifest), not 2
    assert!(stderr.contains("Downloaded session actions: 1"),
        "Should count only session actions with output manifests, not all succeeded actions.\nstderr:\n{stderr}");
}

// =========================================================================
// SYNC-002: New job prints "Manifest file system paths"
// =========================================================================

/// Python prints manifest root paths for each NEW job with attachments:
///   Manifest file system paths:
///     - /mnt/shared (posix)
/// Rust currently omits this.
#[tokio::test]
async fn sync_output_new_job_prints_manifest_file_system_paths() {
    let harness = TestHarness::new().await;
    setup_config(&harness).await;
    let checkpoint_dir = TempDir::new().unwrap();

    queues::mock_get_queue(&harness.server, "farm-abc", queue_with_attachments()).await;
    telemetry::mock_telemetry_endpoint(&harness.server).await;
    sts::mock_get_caller_identity(&harness.server).await;
    s3::mock_s3_list_empty(&harness.server).await;

    let job = active_job("job-mfp", "Manifest Paths Job", 1, 1);
    jobs::mock_search_jobs(&harness.server, "farm-abc", &[job], 1).await;

    // Job with two manifest entries (different root paths)
    let job_detail = json!({
        "jobId": "job-mfp",
        "name": "Manifest Paths Job",
        "attachments": {
            "manifests": [
                { "rootPath": "/mnt/shared", "rootPathFormat": "posix", "fileSystemLocationName": "" },
                { "rootPath": "/mnt/output", "rootPathFormat": "posix", "fileSystemLocationName": "" }
            ],
            "fileSystem": "COPIED"
        }
    });
    jobs::mock_get_job(&harness.server, "farm-abc", "queue-aaa", job_detail).await;
    sessions::mock_list_sessions(&harness.server, "farm-abc", "queue-aaa", "job-mfp", &[]).await;

    let output = harness.cli(&[
        "queue", "sync-output",
        "--ignore-storage-profiles",
        "--checkpoint-dir", checkpoint_dir.path().to_str().unwrap(),
    ]).output().expect("should run");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("Manifest file system paths:"),
        "Should print 'Manifest file system paths:' header for new jobs with attachments.\nstderr:\n{stderr}");
    assert!(stderr.contains("- /mnt/shared (posix)"),
        "Should list first manifest root path.\nstderr:\n{stderr}");
    assert!(stderr.contains("- /mnt/output (posix)"),
        "Should list second manifest root path.\nstderr:\n{stderr}");
}

// =========================================================================
// SYNC-003: WARNING for session actions without output manifests
// =========================================================================

/// Python prints a WARNING when a job has session actions that produced
/// no output manifests:
///   WARNING: Job Test Job (job-123) ran 1 / 2 session actions with no output.
///            This may indicate steps in the job that strictly perform validation...
#[tokio::test]
async fn sync_output_warning_for_session_actions_without_manifests() {
    let harness = TestHarness::new().await;
    setup_config(&harness).await;
    let checkpoint_dir = TempDir::new().unwrap();
    let download_dir = TempDir::new().unwrap();
    let download_root = download_dir.path().to_str().unwrap();

    queues::mock_get_queue(&harness.server, "farm-abc", queue_with_attachments()).await;
    telemetry::mock_telemetry_endpoint(&harness.server).await;
    sts::mock_get_caller_identity(&harness.server).await;

    let job = active_job("job-warn", "Warning Job", 2, 0);
    jobs::mock_search_jobs(&harness.server, "farm-abc", &[job], 1).await;

    let mut job_detail = job_detail_with_attachments("job-warn", None);
    job_detail["attachments"]["manifests"][0]["rootPath"] = json!(download_root);
    jobs::mock_get_job(&harness.server, "farm-abc", "queue-aaa", job_detail).await;

    sessions::mock_list_sessions(&harness.server, "farm-abc", "queue-aaa", "job-warn", &[
        json!({
            "sessionId": "session-w1",
            "fleetId": "fleet-001",
            "workerId": "worker-001",
            "startedAt": "2024-06-15T10:00:00Z",
            "lifecycleStatus": "ENDED"
        })
    ]).await;

    // Two succeeded taskRun actions, but only one has output on S3
    sessions::mock_list_session_actions(
        &harness.server, "farm-abc", "queue-aaa", "job-warn", "session-w1",
        &[
            json!({
                "sessionActionId": "sessionaction-w1-0",
                "status": "SUCCEEDED",
                "startedAt": "2024-06-15T10:01:00Z",
                "endedAt": "2024-06-15T10:02:00Z",
                "definition": { "taskRun": { "taskId": "task-001", "stepId": "step-001" } }
            }),
            json!({
                "sessionActionId": "sessionaction-w1-1",
                "status": "SUCCEEDED",
                "startedAt": "2024-06-15T10:03:00Z",
                "endedAt": "2024-06-15T10:04:00Z",
                "definition": { "taskRun": { "taskId": "task-002", "stepId": "step-002" } }
            }),
        ]
    ).await;

    // Only step-002/task-002 has a manifest; step-001/task-001 has none
    let manifest_key = "DeadlineCloud/Manifests/farm-abc/queue-aaa/job-warn/step-002/task-002/2024-06-15T10:04:00Z_sessionaction-w1-1/abc.manifest";
    s3::mock_s3_list_objects(&harness.server, &[manifest_key]).await;

    let file_hash = "aabbccdd11223344aabbccdd11223344";
    let manifest_json = serde_json::to_string(&json!({
        "hashAlg": "xxh128",
        "manifestVersion": "2023-03-03",
        "paths": [{ "path": "output/result.exr", "hash": file_hash, "size": 10, "mtime": 1700000000000000_i64 }],
        "totalSize": 10
    })).unwrap();

    s3::mock_s3_get_object_with_metadata(
        &harness.server,
        &format!("my-bucket/{manifest_key}"),
        manifest_json.as_bytes(),
        &[("asset-root", download_root)],
    ).await;

    s3::mock_s3_get_object(
        &harness.server,
        &format!("my-bucket/DeadlineCloud/Data/{file_hash}.xxh128"),
        b"resultdata",
    ).await;

    let output = harness.cli(&[
        "queue", "sync-output",
        "--ignore-storage-profiles",
        "--checkpoint-dir", checkpoint_dir.path().to_str().unwrap(),
    ]).output().expect("should run");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("WARNING: Job Warning Job (job-warn) ran 1 / 2 session actions with no output."),
        "Should print WARNING about session actions without output manifests.\nstderr:\n{stderr}");
    assert!(stderr.contains("This may indicate steps in the job that strictly perform validation"),
        "Should print explanation about validation steps.\nstderr:\n{stderr}");
}

// =========================================================================
// SYNC-004: Path summary shows per-file listing with sizes
// =========================================================================

/// Python uses `summarize_path_list(paths, total_size_by_path=sizes, max_entries=30)`
/// which produces a per-directory/per-file summary with sizes.
/// Rust currently prints only "{N} files, {size}" as an aggregate.
#[tokio::test]
async fn sync_output_path_summary_shows_per_file_listing() {
    let harness = TestHarness::new().await;
    setup_config(&harness).await;
    let checkpoint_dir = TempDir::new().unwrap();
    let download_dir = TempDir::new().unwrap();
    let download_root = download_dir.path().to_str().unwrap();

    queues::mock_get_queue(&harness.server, "farm-abc", queue_with_attachments()).await;
    telemetry::mock_telemetry_endpoint(&harness.server).await;
    sts::mock_get_caller_identity(&harness.server).await;

    let job = active_job("job-sum", "Summary Job", 1, 0);
    jobs::mock_search_jobs(&harness.server, "farm-abc", &[job], 1).await;

    let mut job_detail = job_detail_with_attachments("job-sum", None);
    job_detail["attachments"]["manifests"][0]["rootPath"] = json!(download_root);
    jobs::mock_get_job(&harness.server, "farm-abc", "queue-aaa", job_detail).await;

    sessions::mock_list_sessions(&harness.server, "farm-abc", "queue-aaa", "job-sum", &[
        json!({
            "sessionId": "session-sum1",
            "fleetId": "fleet-001",
            "workerId": "worker-001",
            "startedAt": "2024-06-15T10:00:00Z",
            "lifecycleStatus": "ENDED"
        })
    ]).await;

    sessions::mock_list_session_actions(
        &harness.server, "farm-abc", "queue-aaa", "job-sum", "session-sum1",
        &[json!({
            "sessionActionId": "sessionaction-sum1-0",
            "status": "SUCCEEDED",
            "startedAt": "2024-06-15T10:01:00Z",
            "endedAt": "2024-06-15T10:02:00Z",
            "definition": { "taskRun": { "taskId": "task-001", "stepId": "step-001" } }
        })]
    ).await;

    let manifest_key = "DeadlineCloud/Manifests/farm-abc/queue-aaa/job-sum/step-001/task-001/2024-06-15T10:02:00Z_sessionaction-sum1-0/abc.manifest";
    s3::mock_s3_list_objects(&harness.server, &[manifest_key]).await;

    // Manifest with two files in different directories
    let hash_a = "aaaa000000000000aaaa000000000000";
    let hash_b = "bbbb000000000000bbbb000000000000";
    let manifest_json = serde_json::to_string(&json!({
        "hashAlg": "xxh128",
        "manifestVersion": "2023-03-03",
        "paths": [
            { "path": "renders/frame_001.exr", "hash": hash_a, "size": 1500000, "mtime": 1700000000000000_i64 },
            { "path": "renders/frame_002.exr", "hash": hash_b, "size": 1500000, "mtime": 1700000000000000_i64 }
        ],
        "totalSize": 3000000
    })).unwrap();

    s3::mock_s3_get_object_with_metadata(
        &harness.server,
        &format!("my-bucket/{manifest_key}"),
        manifest_json.as_bytes(),
        &[("asset-root", download_root)],
    ).await;

    s3::mock_s3_get_object(
        &harness.server,
        &format!("my-bucket/DeadlineCloud/Data/{hash_a}.xxh128"),
        &vec![0u8; 1500000],
    ).await;
    s3::mock_s3_get_object(
        &harness.server,
        &format!("my-bucket/DeadlineCloud/Data/{hash_b}.xxh128"),
        &vec![0u8; 1500000],
    ).await;

    let output = harness.cli(&[
        "queue", "sync-output",
        "--ignore-storage-profiles",
        "--dry-run",
        "--checkpoint-dir", checkpoint_dir.path().to_str().unwrap(),
    ]).output().expect("should run");

    let stderr = String::from_utf8_lossy(&output.stderr);
    // SYNC-004: Should show per-file listing, not just aggregate "2 files, 3.0 MB"
    // The old format was a bare "  2 files, 3 MB" line with no directory context.
    // The new format uses summarize_path_list which shows directory grouping:
    //   /path/to/renders/ (2 files, 3 MB):
    //     frame_001.exr (1 file)
    //     frame_002.exr (1 file)
    let summary_section = stderr.split("Summary of paths to download:").nth(1).unwrap_or("");
    assert!(summary_section.contains("renders/") || summary_section.contains("renders\\"),
        "Path summary should show directory grouping.\nstderr:\n{stderr}");
    assert!(summary_section.contains("frame_001.exr") || summary_section.contains("frame_%d.exr"),
        "Path summary should show individual files or sequence patterns.\nstderr:\n{stderr}");
    // SYNC-007: Dry-run should still report would-be file/byte counts
    assert!(stderr.contains("Downloaded files: 2"),
        "Dry-run should report would-be file count.\nstderr:\n{stderr}");
    assert!(stderr.contains("Downloaded bytes: 3 MB"),
        "Dry-run should report would-be byte count.\nstderr:\n{stderr}");
}
