//! Level 2 tests for `deadline queue sync-output`.

use deadline_test_server::deadline_api::{jobs, queues, queue_resources, telemetry};
use deadline_test_server::TestHarness;
use insta_cmd::assert_cmd_snapshot;
use serde_json::json;
use std::fs;
use tempfile::TempDir;

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

async fn setup_config(harness: &TestHarness) {
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.queue_id", "queue-aaa"]).assert().success();
}

async fn setup_config_with_storage_profile(harness: &TestHarness) {
    setup_config(harness).await;
    harness.cli(&["config", "set", "settings.storage_profile_id", "sp-linux-123"]).assert().success();
}

// Spec #14: First run with --bootstrap-lookback-minutes
#[tokio::test]
async fn sync_output_first_run_bootstraps_from_lookback() {
    let harness = TestHarness::new().await;
    setup_config_with_storage_profile(&harness).await;
    let checkpoint_dir = TempDir::new().unwrap();

    queues::mock_get_queue(&harness.server, "farm-abc", queue_with_attachments()).await;
    queue_resources::mock_get_storage_profile_for_queue(
        &harness.server, "farm-abc", "queue-aaa", "sp-linux-123", storage_profile(),
    ).await;
    telemetry::mock_telemetry_endpoint(&harness.server).await;
    // SearchJobs returns empty — no jobs to download
    jobs::mock_search_jobs(&harness.server, "farm-abc", &[], 0).await;

    let mut settings = insta::Settings::clone_current();
    settings.add_filter(r"From: .*", "From: [TIMESTAMP]");
    settings.add_filter(r"To: .*", "To: [TIMESTAMP]");
    settings.add_filter(r"Length: .*", "Length: [DURATION]");
    settings.add_filter(r"Initializing from: .*", "Initializing from: [TIMESTAMP]");
    settings.add_filter(r"Checkpoint: .*", "Checkpoint: [PATH]");
    settings.add_filter(r"\.\.\.retrieval completed in .*", "...retrieval completed in [DURATION]");
    settings.add_filter(r"\.\.\.categorization completed in .*", "...categorization completed in [DURATION]");
    let _guard = settings.bind_to_scope();

    assert_cmd_snapshot!(harness.cmd(&[
        "queue", "sync-output",
        "--bootstrap-lookback-minutes", "60",
        "--checkpoint-dir", checkpoint_dir.path().to_str().unwrap(),
    ]));
}

// Spec #17: --ignore-storage-profiles
#[tokio::test]
async fn sync_output_ignore_storage_profiles() {
    let harness = TestHarness::new().await;
    setup_config(&harness).await;
    let checkpoint_dir = TempDir::new().unwrap();

    queues::mock_get_queue(&harness.server, "farm-abc", queue_with_attachments()).await;
    telemetry::mock_telemetry_endpoint(&harness.server).await;
    jobs::mock_search_jobs(&harness.server, "farm-abc", &[], 0).await;

    let mut settings = insta::Settings::clone_current();
    settings.add_filter(r"From: .*", "From: [TIMESTAMP]");
    settings.add_filter(r"To: .*", "To: [TIMESTAMP]");
    settings.add_filter(r"Length: .*", "Length: [DURATION]");
    settings.add_filter(r"Initializing from: .*", "Initializing from: [TIMESTAMP]");
    settings.add_filter(r"Checkpoint: .*", "Checkpoint: [PATH]");
    settings.add_filter(r"\.\.\.retrieval completed in .*", "...retrieval completed in [DURATION]");
    settings.add_filter(r"\.\.\.categorization completed in .*", "...categorization completed in [DURATION]");
    let _guard = settings.bind_to_scope();

    assert_cmd_snapshot!(harness.cmd(&[
        "queue", "sync-output",
        "--ignore-storage-profiles",
        "--checkpoint-dir", checkpoint_dir.path().to_str().unwrap(),
    ]));
}

// Spec #18: Both --storage-profile-id and --ignore-storage-profiles → usage error
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

// Spec #19: No storage profile configured and --ignore-storage-profiles not set
#[tokio::test]
async fn sync_output_no_storage_profile_configured_returns_error() {
    let harness = TestHarness::new().await;
    setup_config(&harness).await;
    // No storage_profile_id set in config, no --ignore-storage-profiles
    let checkpoint_dir = TempDir::new().unwrap();

    assert_cmd_snapshot!(harness.cmd(&[
        "queue", "sync-output",
        "--checkpoint-dir", checkpoint_dir.path().to_str().unwrap(),
    ]));
}

// Spec #20: Checkpoint storage profile ID does not match current
#[tokio::test]
async fn sync_output_checkpoint_storage_profile_mismatch_returns_error() {
    let harness = TestHarness::new().await;
    setup_config_with_storage_profile(&harness).await;
    let checkpoint_dir = TempDir::new().unwrap();

    queues::mock_get_queue(&harness.server, "farm-abc", queue_with_attachments()).await;
    queue_resources::mock_get_storage_profile_for_queue(
        &harness.server, "farm-abc", "queue-aaa", "sp-linux-123", storage_profile(),
    ).await;

    // Write a checkpoint with a different storage profile ID
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

// Spec #23: --dry-run flag
#[tokio::test]
async fn sync_output_dry_run_does_not_save_checkpoint() {
    let harness = TestHarness::new().await;
    setup_config_with_storage_profile(&harness).await;
    let checkpoint_dir = TempDir::new().unwrap();

    queues::mock_get_queue(&harness.server, "farm-abc", queue_with_attachments()).await;
    queue_resources::mock_get_storage_profile_for_queue(
        &harness.server, "farm-abc", "queue-aaa", "sp-linux-123", storage_profile(),
    ).await;
    telemetry::mock_telemetry_endpoint(&harness.server).await;
    jobs::mock_search_jobs(&harness.server, "farm-abc", &[], 0).await;

    let mut settings = insta::Settings::clone_current();
    settings.add_filter(r"From: .*", "From: [TIMESTAMP]");
    settings.add_filter(r"To: .*", "To: [TIMESTAMP]");
    settings.add_filter(r"Length: .*", "Length: [DURATION]");
    settings.add_filter(r"Initializing from: .*", "Initializing from: [TIMESTAMP]");
    settings.add_filter(r"Checkpoint: .*", "Checkpoint: [PATH]");
    settings.add_filter(r"\.\.\.retrieval completed in .*", "...retrieval completed in [DURATION]");
    settings.add_filter(r"\.\.\.categorization completed in .*", "...categorization completed in [DURATION]");
    let _guard = settings.bind_to_scope();

    assert_cmd_snapshot!(harness.cmd(&[
        "queue", "sync-output",
        "--dry-run",
        "--checkpoint-dir", checkpoint_dir.path().to_str().unwrap(),
        "--ignore-storage-profiles",
    ]));

    // Verify no checkpoint file was created
    let entries: Vec<_> = fs::read_dir(checkpoint_dir.path()).unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map_or(false, |ext| ext == "json"))
        .collect();
    assert!(entries.is_empty(), "dry-run should not save checkpoint file");
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

    // Create a PID lock file with the current process's PID (which is running)
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

// Spec #15: Subsequent run with existing checkpoint
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

    // Write an existing checkpoint
    let checkpoint_file = checkpoint_dir.path().join("queue-aaa_sp-linux-123_download_checkpoint.json");
    fs::write(&checkpoint_file, serde_json::to_string_pretty(&json!({
        "localStorageProfileId": "sp-linux-123",
        "downloadsStartedTimestamp": "2024-06-15T10:00:00Z",
        "downloadsCompletedTimestamp": "2024-06-15T11:00:00Z",
        "eventualConsistencyMaxSeconds": 120,
        "jobs": []
    })).unwrap()).unwrap();

    let mut settings = insta::Settings::clone_current();
    settings.add_filter(r"From: .*", "From: [TIMESTAMP]");
    settings.add_filter(r"To: .*", "To: [TIMESTAMP]");
    settings.add_filter(r"Length: .*", "Length: [DURATION]");
    settings.add_filter(r"Continuing from: .*", "Continuing from: [TIMESTAMP]");
    settings.add_filter(r"Checkpoint: .*", "Checkpoint: [PATH]");
    settings.add_filter(r"\.\.\.retrieval completed in .*", "...retrieval completed in [DURATION]");
    settings.add_filter(r"\.\.\.categorization completed in .*", "...categorization completed in [DURATION]");
    let _guard = settings.bind_to_scope();

    assert_cmd_snapshot!(harness.cmd(&[
        "queue", "sync-output",
        "--checkpoint-dir", checkpoint_dir.path().to_str().unwrap(),
    ]));
}

// Spec #16: --force-bootstrap with existing checkpoint
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

    // Write an existing checkpoint
    let checkpoint_file = checkpoint_dir.path().join("queue-aaa_sp-linux-123_download_checkpoint.json");
    fs::write(&checkpoint_file, serde_json::to_string_pretty(&json!({
        "localStorageProfileId": "sp-linux-123",
        "downloadsStartedTimestamp": "2024-06-15T10:00:00Z",
        "downloadsCompletedTimestamp": "2024-06-15T11:00:00Z",
        "eventualConsistencyMaxSeconds": 120,
        "jobs": []
    })).unwrap()).unwrap();

    let mut settings = insta::Settings::clone_current();
    settings.add_filter(r"From: .*", "From: [TIMESTAMP]");
    settings.add_filter(r"To: .*", "To: [TIMESTAMP]");
    settings.add_filter(r"Length: .*", "Length: [DURATION]");
    settings.add_filter(r"Initializing from: .*", "Initializing from: [TIMESTAMP]");
    settings.add_filter(r"Checkpoint: .*", "Checkpoint: [PATH]");
    settings.add_filter(r"\.\.\.retrieval completed in .*", "...retrieval completed in [DURATION]");
    settings.add_filter(r"\.\.\.categorization completed in .*", "...categorization completed in [DURATION]");
    let _guard = settings.bind_to_scope();

    assert_cmd_snapshot!(harness.cmd(&[
        "queue", "sync-output",
        "--force-bootstrap",
        "--bootstrap-lookback-minutes", "30",
        "--checkpoint-dir", checkpoint_dir.path().to_str().unwrap(),
    ]));
}

// Spec #24: --conflict-resolution SKIP passed through to download
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

    let mut settings = insta::Settings::clone_current();
    settings.add_filter(r"From: .*", "From: [TIMESTAMP]");
    settings.add_filter(r"To: .*", "To: [TIMESTAMP]");
    settings.add_filter(r"Length: .*", "Length: [DURATION]");
    settings.add_filter(r"Initializing from: .*", "Initializing from: [TIMESTAMP]");
    settings.add_filter(r"Checkpoint: .*", "Checkpoint: [PATH]");
    settings.add_filter(r"\.\.\.retrieval completed in .*", "...retrieval completed in [DURATION]");
    settings.add_filter(r"\.\.\.categorization completed in .*", "...categorization completed in [DURATION]");
    let _guard = settings.bind_to_scope();

    assert_cmd_snapshot!(harness.cmd(&[
        "queue", "sync-output",
        "--conflict-resolution", "SKIP",
        "--checkpoint-dir", checkpoint_dir.path().to_str().unwrap(),
    ]));
}

// Spec #25: --json flag
#[tokio::test]
async fn sync_output_json_output() {
    let harness = TestHarness::new().await;
    setup_config_with_storage_profile(&harness).await;
    let checkpoint_dir = TempDir::new().unwrap();

    queues::mock_get_queue(&harness.server, "farm-abc", queue_with_attachments()).await;
    queue_resources::mock_get_storage_profile_for_queue(
        &harness.server, "farm-abc", "queue-aaa", "sp-linux-123", storage_profile(),
    ).await;
    telemetry::mock_telemetry_endpoint(&harness.server).await;
    jobs::mock_search_jobs(&harness.server, "farm-abc", &[], 0).await;

    let mut settings = insta::Settings::clone_current();
    settings.add_filter(r"From: .*", "From: [TIMESTAMP]");
    settings.add_filter(r"To: .*", "To: [TIMESTAMP]");
    settings.add_filter(r"Length: .*", "Length: [DURATION]");
    settings.add_filter(r"Initializing from: .*", "Initializing from: [TIMESTAMP]");
    settings.add_filter(r"Checkpoint: .*", "Checkpoint: [PATH]");
    settings.add_filter(r"\.\.\.retrieval completed in .*", "...retrieval completed in [DURATION]");
    settings.add_filter(r"\.\.\.categorization completed in .*", "...categorization completed in [DURATION]");
    let _guard = settings.bind_to_scope();

    assert_cmd_snapshot!(harness.cmd(&[
        "queue", "sync-output",
        "--json",
        "--checkpoint-dir", checkpoint_dir.path().to_str().unwrap(),
        "--ignore-storage-profiles",
    ]));
}
