//! Level 2 tests for credential scoping (§53 cases 3, 11-12).
//!
//! Tests that `job logs` uses queue-scoped credentials for DCM users
//! and base credentials for non-DCM users.
//!
//! DCM is detected by the presence of `monitor_id` in the AWS profile.
//! When DCM is active, `get_session_logs` calls `AssumeQueueRoleForUser`
//! and builds the CloudWatch client with queue credentials.
//! When DCM is not active, base credentials are used directly.
//! If queue role assumption fails for a DCM user, the error is propagated
//! (matching Python's behavior — it raises DeadlineOperationError).
//!
//! Worker log credential scoping (§53 cases 6-8) is tested at Level 1
//! in `deadline-client` since `get_worker_logs` is not exposed via CLI.

use deadline_test_server::deadline_api::{cloudwatch, jobs, queue_resources, queues, sessions, sts};
use deadline_test_server::TestHarness;
use serde_json::json;

/// Write a fake ~/.aws/config with a DCM profile into the harness temp dir.
fn write_dcm_aws_config(harness: &TestHarness, profile_name: &str) {
    let aws_dir = harness.config_dir.path().join(".aws");
    std::fs::create_dir_all(&aws_dir).unwrap();
    std::fs::write(
        aws_dir.join("config"),
        format!(
            "[profile {profile_name}]\n\
             region=us-west-2\n\
             credential_process=echo dummy\n\
             monitor_id=monitor-abc123\n\
             user_id=user-dcm-test-id\n\
             identity_store_id=d-teststore\n"
        ),
    )
    .unwrap();
}

fn setup_dcm_config(harness: &TestHarness) {
    // Set profile FIRST, then set farm/queue/job under that profile
    harness
        .cli(&[
            "config",
            "set",
            "defaults.aws_profile_name",
            "dcm-profile",
        ])
        .assert()
        .success();
    harness
        .cli(&["config", "set", "defaults.farm_id", "farm-abc"])
        .assert()
        .success();
    harness
        .cli(&["config", "set", "defaults.queue_id", "queue-abc"])
        .assert()
        .success();
    harness
        .cli(&["config", "set", "defaults.job_id", "job-aaa"])
        .assert()
        .success();
}

fn setup_config(harness: &TestHarness) {
    harness
        .cli(&["config", "set", "defaults.farm_id", "farm-abc"])
        .assert()
        .success();
    harness
        .cli(&["config", "set", "defaults.queue_id", "queue-abc"])
        .assert()
        .success();
    harness
        .cli(&["config", "set", "defaults.job_id", "job-aaa"])
        .assert()
        .success();
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

// ---------------------------------------------------------------------------
// §53 case 11: DCM profile — `job logs` uses queue-scoped credentials
// AssumeQueueRoleForUser MUST be called for DCM users.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn job_logs_dcm_user_uses_queue_scoped_credentials() {
    let harness = TestHarness::new().await;
    write_dcm_aws_config(&harness, "dcm-profile");
    setup_dcm_config(&harness);

    jobs::mock_get_job(
        &harness.server,
        "farm-abc",
        "queue-abc",
        json!({
            "jobId": "job-aaa",
            "name": "Render Job",
        }),
    )
    .await;

    sessions::mock_get_session(
        &harness.server,
        "farm-abc",
        "queue-abc",
        "job-aaa",
        json!({
            "sessionId": "session-001",
            "startedAt": "2024-12-18T00:00:00Z",
            "fleetId": "fleet-abc",
            "workerId": "worker-001",
        }),
    )
    .await;

    // AssumeQueueRoleForUser — MUST be called for DCM users
    queue_resources::mock_assume_queue_role_for_user(
        &harness.server,
        "farm-abc",
        "queue-abc",
        queue_role_credentials(),
    )
    .await;

    cloudwatch::mock_get_log_events(
        &harness.server,
        &[json!({"timestamp": 1702857600000_i64, "message": "DCM log line"})],
        None,
    )
    .await;

    let output = harness
        .cli(&["job", "logs", "--session-id", "session-001"])
        .output()
        .expect("failed to run");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "Expected success, got exit code {:?}\nstdout: {stdout}\nstderr: {stderr}",
        output.status.code()
    );
    assert!(
        stdout.contains("DCM log line"),
        "Expected log output, got:\n{stdout}"
    );

    // Verify AssumeQueueRoleForUser was actually called
    let assume_role_requests: Vec<_> = harness
        .server
        .received_requests()
        .await
        .unwrap()
        .into_iter()
        .filter(|r| r.url.path().contains("user-roles"))
        .collect();
    assert_eq!(
        assume_role_requests.len(),
        1,
        "Expected exactly 1 AssumeQueueRoleForUser call for DCM user, got {}",
        assume_role_requests.len()
    );
}

// ---------------------------------------------------------------------------
// §53 case 12: Non-DCM profile — `job logs` uses base credentials
// AssumeQueueRoleForUser MUST NOT be called for non-DCM users.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn job_logs_non_dcm_user_uses_base_credentials() {
    let harness = TestHarness::new().await;
    setup_config(&harness);

    jobs::mock_get_job(
        &harness.server,
        "farm-abc",
        "queue-abc",
        json!({
            "jobId": "job-aaa",
            "name": "Render Job",
        }),
    )
    .await;

    sessions::mock_get_session(
        &harness.server,
        "farm-abc",
        "queue-abc",
        "job-aaa",
        json!({
            "sessionId": "session-001",
            "startedAt": "2024-12-18T00:00:00Z",
            "fleetId": "fleet-abc",
            "workerId": "worker-001",
        }),
    )
    .await;

    cloudwatch::mock_get_log_events(
        &harness.server,
        &[json!({"timestamp": 1702857600000_i64, "message": "base cred log line"})],
        None,
    )
    .await;

    let output = harness
        .cli(&["job", "logs", "--session-id", "session-001"])
        .output()
        .expect("failed to run");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "Expected success, got exit code {:?}\nstdout: {stdout}\nstderr: {stderr}",
        output.status.code()
    );
    assert!(
        stdout.contains("base cred log line"),
        "Expected log output, got:\n{stdout}"
    );

    // Verify AssumeQueueRoleForUser was NOT called
    let assume_role_requests: Vec<_> = harness
        .server
        .received_requests()
        .await
        .unwrap()
        .into_iter()
        .filter(|r| r.url.path().contains("user-roles"))
        .collect();
    assert_eq!(
        assume_role_requests.len(),
        0,
        "Expected 0 AssumeQueueRoleForUser calls for non-DCM user, got {}",
        assume_role_requests.len()
    );
}

// ---------------------------------------------------------------------------
// §53 case 13: `attachment download` no --profile — uses queue credentials
// AssumeQueueRoleForUser MUST be called unconditionally (not DCM-gated).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn attachment_download_no_profile_uses_queue_credentials() {
    let harness = TestHarness::new().await;
    setup_config(&harness);

    // Mock get_queue to return jobAttachmentSettings
    queues::mock_get_queue(
        &harness.server,
        "farm-abc",
        json!({
            "queueId": "queue-abc",
            "displayName": "Test Queue",
            "jobAttachmentSettings": {
                "s3BucketName": "test-bucket",
                "rootPrefix": "Data"
            }
        }),
    )
    .await;

    // AssumeQueueRoleForUser — MUST be called when no --profile
    queue_resources::mock_assume_queue_role_for_user(
        &harness.server,
        "farm-abc",
        "queue-abc",
        queue_role_credentials(),
    )
    .await;

    // STS GetCallerIdentity for get_account_id
    sts::mock_get_caller_identity(&harness.server).await;

    // Write a minimal manifest file
    let manifest_path = harness.config_dir.path().join("test.manifest");
    std::fs::write(
        &manifest_path,
        serde_json::to_string(&json!({
            "manifestVersion": "2023-03-03",
            "hashAlg": "xxh128",
            "totalSize": 100,
            "paths": [{"path": "test.txt", "hash": "abc123", "size": 100, "mtime": 1000}]
        }))
        .unwrap(),
    )
    .unwrap();

    // Run attachment download — S3 will fail (no mock) but we verify
    // AssumeQueueRoleForUser was called
    let _output = harness
        .cli(&[
            "attachment",
            "download",
            "--manifests",
            manifest_path.to_str().unwrap(),
        ])
        .output()
        .expect("failed to run");

    // Verify AssumeQueueRoleForUser was called
    let assume_role_requests: Vec<_> = harness
        .server
        .received_requests()
        .await
        .unwrap()
        .into_iter()
        .filter(|r| r.url.path().contains("user-roles"))
        .collect();
    assert_eq!(
        assume_role_requests.len(),
        1,
        "Expected exactly 1 AssumeQueueRoleForUser call when no --profile, got {}",
        assume_role_requests.len()
    );
}

// ---------------------------------------------------------------------------
// §53 case 14: `attachment download` with --profile — skips queue role
// AssumeQueueRoleForUser MUST NOT be called when --profile is provided.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn attachment_download_with_profile_skips_queue_credentials() {
    let harness = TestHarness::new().await;
    setup_config(&harness);

    // STS GetCallerIdentity for get_account_id
    sts::mock_get_caller_identity(&harness.server).await;

    // Write a minimal manifest file
    let manifest_path = harness.config_dir.path().join("test.manifest");
    std::fs::write(
        &manifest_path,
        serde_json::to_string(&json!({
            "manifestVersion": "2023-03-03",
            "hashAlg": "xxh128",
            "totalSize": 100,
            "paths": [{"path": "test.txt", "hash": "abc123", "size": 100, "mtime": 1000}]
        }))
        .unwrap(),
    )
    .unwrap();

    // Run with --profile and --s3-root-uri — should use profile creds directly
    let _output = harness
        .cli(&[
            "attachment",
            "download",
            "--manifests",
            manifest_path.to_str().unwrap(),
            "--s3-root-uri",
            "s3://bucket/prefix",
            "--profile",
            "test-profile",
        ])
        .output()
        .expect("failed to run");

    // Verify AssumeQueueRoleForUser was NOT called
    let assume_role_requests: Vec<_> = harness
        .server
        .received_requests()
        .await
        .unwrap()
        .into_iter()
        .filter(|r| r.url.path().contains("user-roles"))
        .collect();
    assert_eq!(
        assume_role_requests.len(),
        0,
        "Expected 0 AssumeQueueRoleForUser calls with --profile, got {}",
        assume_role_requests.len()
    );
}

// ---------------------------------------------------------------------------
// §53 case 15: `attachment upload` no --profile — uses queue credentials
// AssumeQueueRoleForUser MUST be called unconditionally (not DCM-gated).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn attachment_upload_no_profile_uses_queue_credentials() {
    let harness = TestHarness::new().await;
    setup_config(&harness);

    // Mock get_queue to return jobAttachmentSettings
    queues::mock_get_queue(
        &harness.server,
        "farm-abc",
        json!({
            "queueId": "queue-abc",
            "displayName": "Test Queue",
            "jobAttachmentSettings": {
                "s3BucketName": "test-bucket",
                "rootPrefix": "Data"
            }
        }),
    )
    .await;

    // AssumeQueueRoleForUser — MUST be called when no --profile
    queue_resources::mock_assume_queue_role_for_user(
        &harness.server,
        "farm-abc",
        "queue-abc",
        queue_role_credentials(),
    )
    .await;

    // STS GetCallerIdentity for get_account_id
    sts::mock_get_caller_identity(&harness.server).await;

    // Write a minimal manifest file
    let manifest_path = harness.config_dir.path().join("test.manifest");
    std::fs::write(
        &manifest_path,
        serde_json::to_string(&json!({
            "manifestVersion": "2023-03-03",
            "hashAlg": "xxh128",
            "totalSize": 100,
            "paths": [{"path": "test.txt", "hash": "abc123", "size": 100, "mtime": 1000}]
        }))
        .unwrap(),
    )
    .unwrap();

    // Create a root dir with a test file
    let root_dir = harness.config_dir.path().join("upload_root");
    std::fs::create_dir_all(&root_dir).unwrap();
    std::fs::write(root_dir.join("test.txt"), "test content").unwrap();

    // Run attachment upload — S3 will fail (no mock) but we verify
    // AssumeQueueRoleForUser was called
    let _output = harness
        .cli(&[
            "attachment",
            "upload",
            "--manifests",
            manifest_path.to_str().unwrap(),
            "--root-dirs",
            root_dir.to_str().unwrap(),
        ])
        .output()
        .expect("failed to run");

    // Verify AssumeQueueRoleForUser was called
    let assume_role_requests: Vec<_> = harness
        .server
        .received_requests()
        .await
        .unwrap()
        .into_iter()
        .filter(|r| r.url.path().contains("user-roles"))
        .collect();
    assert_eq!(
        assume_role_requests.len(),
        1,
        "Expected exactly 1 AssumeQueueRoleForUser call when no --profile, got {}",
        assume_role_requests.len()
    );
}

// ---------------------------------------------------------------------------
// §53 case 16: `attachment upload` with --profile — skips queue role
// AssumeQueueRoleForUser MUST NOT be called when --profile is provided.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn attachment_upload_with_profile_skips_queue_credentials() {
    let harness = TestHarness::new().await;
    setup_config(&harness);

    // STS GetCallerIdentity for get_account_id
    sts::mock_get_caller_identity(&harness.server).await;

    // Write a minimal manifest file
    let manifest_path = harness.config_dir.path().join("test.manifest");
    std::fs::write(
        &manifest_path,
        serde_json::to_string(&json!({
            "manifestVersion": "2023-03-03",
            "hashAlg": "xxh128",
            "totalSize": 100,
            "paths": [{"path": "test.txt", "hash": "abc123", "size": 100, "mtime": 1000}]
        }))
        .unwrap(),
    )
    .unwrap();

    // Create a root dir with a test file
    let root_dir = harness.config_dir.path().join("upload_root");
    std::fs::create_dir_all(&root_dir).unwrap();
    std::fs::write(root_dir.join("test.txt"), "test content").unwrap();

    // Run with --profile and --s3-root-uri — should use profile creds directly
    let _output = harness
        .cli(&[
            "attachment",
            "upload",
            "--manifests",
            manifest_path.to_str().unwrap(),
            "--root-dirs",
            root_dir.to_str().unwrap(),
            "--s3-root-uri",
            "s3://bucket/prefix",
            "--profile",
            "test-profile",
        ])
        .output()
        .expect("failed to run");

    // Verify AssumeQueueRoleForUser was NOT called
    let assume_role_requests: Vec<_> = harness
        .server
        .received_requests()
        .await
        .unwrap()
        .into_iter()
        .filter(|r| r.url.path().contains("user-roles"))
        .collect();
    assert_eq!(
        assume_role_requests.len(),
        0,
        "Expected 0 AssumeQueueRoleForUser calls with --profile, got {}",
        assume_role_requests.len()
    );
}

// ---------------------------------------------------------------------------
// §53 case 3: DCM user, AssumeQueueRoleForUser fails — error propagated
// Python raises DeadlineOperationError("Failed to get queue credentials: ...").
// Rust must propagate the error, NOT fall back to base credentials.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn job_logs_dcm_user_queue_role_failure_propagates_error() {
    let harness = TestHarness::new().await;
    write_dcm_aws_config(&harness, "dcm-profile");
    setup_dcm_config(&harness);

    jobs::mock_get_job(
        &harness.server,
        "farm-abc",
        "queue-abc",
        json!({
            "jobId": "job-aaa",
            "name": "Render Job",
        }),
    )
    .await;

    sessions::mock_get_session(
        &harness.server,
        "farm-abc",
        "queue-abc",
        "job-aaa",
        json!({
            "sessionId": "session-001",
            "startedAt": "2024-12-18T00:00:00Z",
            "fleetId": "fleet-abc",
            "workerId": "worker-001",
        }),
    )
    .await;

    // AssumeQueueRoleForUser returns 403 — error must propagate
    queue_resources::mock_assume_queue_role_for_user_error(
        &harness.server,
        "farm-abc",
        "queue-abc",
        403,
        "AccessDeniedException",
    )
    .await;

    let output = harness
        .cli(&["job", "logs", "--session-id", "session-001"])
        .output()
        .expect("failed to run");

    assert!(
        !output.status.success(),
        "Expected failure when queue role assumption fails for DCM user"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let all_output = format!("{stdout}{stderr}");
    assert!(
        all_output.contains("AccessDeniedException") || all_output.contains("queue credentials") || all_output.contains("Failed"),
        "Expected error about queue credentials or access denied, got:\nstdout: {stdout}\nstderr: {stderr}"
    );
}
