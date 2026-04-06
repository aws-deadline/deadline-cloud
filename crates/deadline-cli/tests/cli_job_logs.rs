//! Level 2 tests for `deadline job logs` subcommand.
//!
//! Covers get_session_logs (cases 16-30) via the CLI interface.
//! DCM credential path tests (cases 22, 32) are deferred — they require
//! AWS config file setup for DCM profiles.

use deadline_test_server::TestHarness;
use deadline_test_server::deadline_api::{jobs, sessions, cloudwatch};
use insta_cmd::assert_cmd_snapshot;
use serde_json::json;

fn setup_config(harness: &TestHarness) {
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.queue_id", "queue-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.job_id", "job-aaa"]).assert().success();
}

fn insta_settings() -> insta::Settings {
    let mut settings = insta::Settings::clone_current();
    settings.add_filter(r"Retrieving logs for .* from log group", "Retrieving logs for [SESSION] from log group");
    settings.add_filter(r"Using the (?:latest|only available) session: session-\S+", "Using session: [SESSION_ID]");
    settings
}

// ---------------------------------------------------------------------------
// Session logs: explicit session_id, returns log events (case 16)
// Also covers: log group/stream pattern (case 21), trailing whitespace
// stripped (case 30), ingestion_time present (case 28), missing
// ingestion_time (case 29 — second event has no ingestionTime)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn job_logs_with_session_id_prints_events() {
    let harness = TestHarness::new().await;
    setup_config(&harness);

    jobs::mock_get_job(&harness.server, "farm-abc", "queue-abc", json!({
        "jobId": "job-aaa", "name": "Render Job",
    })).await;

    sessions::mock_get_session(&harness.server, "farm-abc", "queue-abc", "job-aaa", json!({
        "sessionId": "session-001",
        "startedAt": "2024-12-18T00:00:00Z",
        "fleetId": "fleet-abc",
        "workerId": "worker-001",
    })).await;

    cloudwatch::mock_get_log_events(&harness.server, &[
        json!({"timestamp": 1702857600000_i64, "message": "Starting session\n", "ingestionTime": 1702857601000_i64}),
        json!({"timestamp": 1702857601000_i64, "message": "Task running   "}),
    ], None).await;

    let _guard = insta_settings().bind_to_scope();
    assert_cmd_snapshot!(harness.cmd(&["job", "logs", "--session-id", "session-001"]));
}

// ---------------------------------------------------------------------------
// Session auto-selection: ongoing session preferred (case 17)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn job_logs_auto_selects_ongoing_session() {
    let harness = TestHarness::new().await;
    setup_config(&harness);

    jobs::mock_get_job(&harness.server, "farm-abc", "queue-abc", json!({
        "jobId": "job-aaa", "name": "Render Job",
    })).await;

    sessions::mock_list_sessions(&harness.server, "farm-abc", "queue-abc", "job-aaa", &[
        json!({"sessionId": "session-ended", "startedAt": "2024-12-18T00:00:00Z", "endedAt": "2024-12-18T01:00:00Z", "fleetId": "fleet-abc", "workerId": "worker-001"}),
        json!({"sessionId": "session-ongoing", "startedAt": "2024-12-18T02:00:00Z", "fleetId": "fleet-abc", "workerId": "worker-002"}),
    ]).await;

    sessions::mock_get_session(&harness.server, "farm-abc", "queue-abc", "job-aaa", json!({
        "sessionId": "session-ongoing",
        "startedAt": "2024-12-18T02:00:00Z",
        "fleetId": "fleet-abc",
        "workerId": "worker-002",
    })).await;

    cloudwatch::mock_get_log_events(&harness.server, &[
        json!({"timestamp": 1702944000000_i64, "message": "log line"}),
    ], None).await;

    let _guard = insta_settings().bind_to_scope();
    assert_cmd_snapshot!(harness.cmd(&["job", "logs"]));
}

// ---------------------------------------------------------------------------
// Session auto-selection: no ongoing, most recently ended (case 18)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn job_logs_auto_selects_most_recently_ended() {
    let harness = TestHarness::new().await;
    setup_config(&harness);

    jobs::mock_get_job(&harness.server, "farm-abc", "queue-abc", json!({
        "jobId": "job-aaa", "name": "Render Job",
    })).await;

    sessions::mock_list_sessions(&harness.server, "farm-abc", "queue-abc", "job-aaa", &[
        json!({"sessionId": "session-old", "startedAt": "2024-12-17T00:00:00Z", "endedAt": "2024-12-17T01:00:00Z", "fleetId": "fleet-abc", "workerId": "worker-001"}),
        json!({"sessionId": "session-recent", "startedAt": "2024-12-18T00:00:00Z", "endedAt": "2024-12-18T02:00:00Z", "fleetId": "fleet-abc", "workerId": "worker-002"}),
    ]).await;

    sessions::mock_get_session(&harness.server, "farm-abc", "queue-abc", "job-aaa", json!({
        "sessionId": "session-recent",
        "startedAt": "2024-12-18T00:00:00Z",
        "fleetId": "fleet-abc",
        "workerId": "worker-002",
    })).await;

    cloudwatch::mock_get_log_events(&harness.server, &[], None).await;

    let _guard = insta_settings().bind_to_scope();
    assert_cmd_snapshot!(harness.cmd(&["job", "logs"]));
}

// ---------------------------------------------------------------------------
// No sessions found for job (case 20)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn job_logs_no_sessions_exits_with_error() {
    let harness = TestHarness::new().await;
    setup_config(&harness);

    jobs::mock_get_job(&harness.server, "farm-abc", "queue-abc", json!({
        "jobId": "job-aaa", "name": "Render Job",
    })).await;

    sessions::mock_list_sessions(&harness.server, "farm-abc", "queue-abc", "job-aaa", &[]).await;

    assert_cmd_snapshot!(harness.cmd(&["job", "logs"]));
}

// ---------------------------------------------------------------------------
// ResourceNotFoundException returns no-logs message (case 26)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn job_logs_log_group_not_found_shows_no_logs() {
    let harness = TestHarness::new().await;
    setup_config(&harness);

    jobs::mock_get_job(&harness.server, "farm-abc", "queue-abc", json!({
        "jobId": "job-aaa", "name": "Render Job",
    })).await;

    sessions::mock_get_session(&harness.server, "farm-abc", "queue-abc", "job-aaa", json!({
        "sessionId": "session-001",
        "startedAt": "2024-12-18T00:00:00Z",
        "fleetId": "fleet-abc",
        "workerId": "worker-001",
    })).await;

    cloudwatch::mock_get_log_events_not_found(&harness.server).await;

    let _guard = insta_settings().bind_to_scope();
    assert_cmd_snapshot!(harness.cmd(&["job", "logs", "--session-id", "session-001"]));
}

// ---------------------------------------------------------------------------
// JSON output with nextToken (cases 25 + JSON output)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn job_logs_json_output_with_next_token() {
    let harness = TestHarness::new().await;
    setup_config(&harness);

    jobs::mock_get_job(&harness.server, "farm-abc", "queue-abc", json!({
        "jobId": "job-aaa", "name": "Render Job",
    })).await;

    sessions::mock_get_session(&harness.server, "farm-abc", "queue-abc", "job-aaa", json!({
        "sessionId": "session-001",
        "startedAt": "2024-12-18T00:00:00Z",
        "fleetId": "fleet-abc",
        "workerId": "worker-001",
    })).await;

    cloudwatch::mock_get_log_events(&harness.server, &[
        json!({"timestamp": 1702857600000_i64, "message": "hello"}),
    ], Some("f/next-page")).await;

    let output = harness.cli(&["job", "logs", "--session-id", "session-001", "--output", "json"])
        .output()
        .expect("failed to run");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let parsed: serde_json::Value = serde_json::from_str(&stdout)
        .unwrap_or_else(|e| panic!("expected valid JSON: {e}, got: {stdout}"));
    assert_eq!(parsed["count"], 1);
    assert_eq!(parsed["events"][0]["message"], "hello");
    assert!(parsed["nextToken"].is_string(), "expected nextToken");
    assert_eq!(parsed["logGroup"], "/aws/deadline/farm-abc/queue-abc");
    assert_eq!(parsed["logStream"], "session-001");
}

// ---------------------------------------------------------------------------
// Pagination token passthrough (case 25)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn job_logs_next_token_passed_through() {
    let harness = TestHarness::new().await;
    setup_config(&harness);

    jobs::mock_get_job(&harness.server, "farm-abc", "queue-abc", json!({
        "jobId": "job-aaa", "name": "Render Job",
    })).await;

    sessions::mock_get_session(&harness.server, "farm-abc", "queue-abc", "job-aaa", json!({
        "sessionId": "session-001",
        "startedAt": "2024-12-18T00:00:00Z",
        "fleetId": "fleet-abc",
        "workerId": "worker-001",
    })).await;

    cloudwatch::mock_get_log_events(&harness.server, &[
        json!({"timestamp": 1702857700000_i64, "message": "page 2 line"}),
    ], None).await;

    let output = harness.cli(&[
        "job", "logs", "--session-id", "session-001",
        "--next-token", "f/previous-page-token",
        "--output", "json",
    ])
        .output()
        .expect("failed to run");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let parsed: serde_json::Value = serde_json::from_str(&stdout)
        .unwrap_or_else(|e| panic!("expected valid JSON: {e}, got: {stdout}"));
    assert_eq!(parsed["events"][0]["message"], "page 2 line");
}

// ---------------------------------------------------------------------------
// Timestamp format: local (case 20)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn job_logs_timestamp_format_local() {
    let harness = TestHarness::new().await;
    setup_config(&harness);

    jobs::mock_get_job(&harness.server, "farm-abc", "queue-abc", json!({
        "jobId": "job-aaa", "name": "Render Job",
    })).await;

    sessions::mock_get_session(&harness.server, "farm-abc", "queue-abc", "job-aaa", json!({
        "sessionId": "session-001",
        "startedAt": "2024-12-18T00:00:00Z",
        "fleetId": "fleet-abc",
        "workerId": "worker-001",
    })).await;

    cloudwatch::mock_get_log_events(&harness.server, &[
        json!({"timestamp": 1702857600000_i64, "message": "hello"}),
    ], None).await;

    let output = harness.cli(&["job", "logs", "--session-id", "session-001", "--timestamp-format", "local"])
        .output()
        .expect("failed to run");

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(output.status.success(), "got: {stdout}");
    // Local format should NOT contain +00:00 (unless local IS UTC)
    // It should contain a timezone offset like -07:00 or +05:30
    assert!(stdout.contains("hello"), "expected log message");
    // The timestamp should be in ISO format with a timezone offset
    assert!(stdout.contains("[20"), "expected timestamp with year prefix");
}

// ---------------------------------------------------------------------------
// Timestamp format: relative (case 21)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn job_logs_timestamp_format_relative() {
    let harness = TestHarness::new().await;
    setup_config(&harness);

    jobs::mock_get_job(&harness.server, "farm-abc", "queue-abc", json!({
        "jobId": "job-aaa", "name": "Render Job",
    })).await;

    sessions::mock_get_session(&harness.server, "farm-abc", "queue-abc", "job-aaa", json!({
        "sessionId": "session-001",
        "startedAt": "2023-12-18T00:00:00Z",
        "fleetId": "fleet-abc",
        "workerId": "worker-001",
    })).await;

    // Event is 60 seconds after session start (2023-12-18T00:00:00Z = epoch 1702857600)
    cloudwatch::mock_get_log_events(&harness.server, &[
        json!({"timestamp": 1702857660000_i64, "message": "one minute in"}),
    ], None).await;

    let output = harness.cli(&["job", "logs", "--session-id", "session-001", "--timestamp-format", "relative"])
        .output()
        .expect("failed to run");

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(output.status.success(), "got: {stdout}");
    // Relative format: should show time delta like "0:01:00"
    assert!(stdout.contains("0:01:00"), "expected relative timestamp 0:01:00, got: {stdout}");
    // Should show "Logs relative to start time" message
    assert!(stdout.contains("Logs relative to start time"), "expected relative reference message");
}

// ---------------------------------------------------------------------------
// Timestamp format in JSON output uses the formatter too (case 19-21)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn job_logs_json_timestamp_format_utc() {
    let harness = TestHarness::new().await;
    setup_config(&harness);

    jobs::mock_get_job(&harness.server, "farm-abc", "queue-abc", json!({
        "jobId": "job-aaa", "name": "Render Job",
    })).await;

    sessions::mock_get_session(&harness.server, "farm-abc", "queue-abc", "job-aaa", json!({
        "sessionId": "session-001",
        "startedAt": "2024-12-18T00:00:00Z",
        "fleetId": "fleet-abc",
        "workerId": "worker-001",
    })).await;

    cloudwatch::mock_get_log_events(&harness.server, &[
        json!({"timestamp": 1702857600000_i64, "message": "hello"}),
    ], None).await;

    let output = harness.cli(&["job", "logs", "--session-id", "session-001", "--output", "json", "--timestamp-format", "utc"])
        .output()
        .expect("failed to run");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let parsed: serde_json::Value = serde_json::from_str(&stdout).expect("valid JSON");
    let ts = parsed["events"][0]["timestamp"].as_str().unwrap();
    // Python UTC isoformat: 2024-12-18T00:00:00+00:00
    assert!(ts.contains("+00:00"), "UTC timestamp should have +00:00 offset, got: {ts}");
}
