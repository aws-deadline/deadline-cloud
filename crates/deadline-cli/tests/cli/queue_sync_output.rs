//! Level 2 tests for `deadline queue sync-output`.
//!
//! Tests exercise the full orchestration through the CLI binary:
//! SearchJobs → GetJob → ListSessions → ListSessionActions → S3 download.

use deadline_test_server::deadline_api::{errors, jobs, queues, queue_resources, sessions, s3, telemetry};
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
// Error path tests (spec cases 18-22, 26) — validation short-circuits
// =========================================================================

// Spec #18: Both --storage-profile-id and --ignore-storage-profiles
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

// Spec #19: No storage profile configured
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

// Spec #20: Checkpoint storage profile mismatch
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

// Spec #21: Queue has no job attachment settings
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

// Spec #22: Checkpoint directory not writable
#[tokio::test]
async fn sync_output_checkpoint_dir_not_writable_returns_error() {
    let harness = TestHarness::new().await;
    setup_config_with_storage_profile(&harness).await;

    assert_cmd_snapshot!(harness.cmd(&[
        "queue", "sync-output",
        "--checkpoint-dir", "/nonexistent/readonly/dir",
    ]));
}

// Spec #26: PID lock prevents concurrent runs
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
