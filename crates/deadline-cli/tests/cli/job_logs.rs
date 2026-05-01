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
// Session logs: explicit session_id, returns log events
// Also covers: log group/stream pattern, trailing whitespace
// stripped, ingestion_time present, missing
// ingestion_time (second event has no ingestionTime)
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
// Session auto-selection: ongoing session preferred
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
// Session auto-selection: no ongoing, most recently ended
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
// No sessions found for job
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
// ResourceNotFoundException returns no-logs message
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
// Pagination token passthrough
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
// Timestamp format: local
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
// Timestamp format: relative
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
// Timestamp format in JSON output uses the formatter too
// ---------------------------------------------------------------------------

// Negative timedelta (log event before session start) should
// produce "-H:MM:SS", not "0:-MM:SS"
#[tokio::test]
async fn job_logs_timestamp_format_relative_negative_timedelta() {
    let harness = TestHarness::new().await;
    setup_config(&harness);

    jobs::mock_get_job(&harness.server, "farm-abc", "queue-abc", json!({
        "jobId": "job-aaa", "name": "Render Job",
    })).await;

    sessions::mock_get_session(&harness.server, "farm-abc", "queue-abc", "job-aaa", json!({
        "sessionId": "session-001",
        "startedAt": "2023-12-18T00:01:00Z",
        "fleetId": "fleet-abc",
        "workerId": "worker-001",
    })).await;

    // Event is 30 seconds BEFORE session start
    // session start = 2023-12-18T00:01:00Z = epoch 1702857660
    // event = 1702857630 = 2023-12-18T00:00:30Z (30 seconds before start)
    cloudwatch::mock_get_log_events(&harness.server, &[
        json!({"timestamp": 1702857630000_i64, "message": "early event"}),
    ], None).await;

    let output = harness.cli(&["job", "logs", "--session-id", "session-001", "--timestamp-format", "relative"])
        .output()
        .expect("failed to run");

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(output.status.success(), "got: {stdout}");
    // Should show negative timedelta like "-0:00:30", NOT "0:-00:30" or "0:00:-30"
    assert!(stdout.contains("-0:00:30"), "expected negative timedelta -0:00:30, got: {stdout}");
}

// ---------------------------------------------------------------------------
// Timestamp format in JSON output uses the formatter too
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

// ===========================================================================
// #15c: Job logs auto-selection messages
// ===========================================================================

// Auto-select single session → prints "Using the only available session"
#[tokio::test]
async fn job_logs_auto_select_single_session_prints_message() {
    let harness = TestHarness::new().await;
    setup_config(&harness);

    jobs::mock_get_job(&harness.server, "farm-abc", "queue-abc", json!({
        "jobId": "job-aaa", "name": "Render Job",
    })).await;

    sessions::mock_list_sessions(&harness.server, "farm-abc", "queue-abc", "job-aaa", &[
        json!({"sessionId": "session-only", "startedAt": "2024-12-18T00:00:00Z", "endedAt": "2024-12-18T01:00:00Z", "fleetId": "fleet-abc", "workerId": "worker-001"}),
    ]).await;

    sessions::mock_get_session(&harness.server, "farm-abc", "queue-abc", "job-aaa", json!({
        "sessionId": "session-only", "startedAt": "2024-12-18T00:00:00Z",
        "fleetId": "fleet-abc", "workerId": "worker-001",
    })).await;

    cloudwatch::mock_get_log_events(&harness.server, &[
        json!({"timestamp": 1702857600000_i64, "message": "log line"}),
    ], None).await;

    let output = harness.cli(&["job", "logs"])
        .output()
        .expect("failed to run");

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(output.status.success(), "got: {stdout}");
    assert!(
        stdout.contains("Using the only available session"),
        "Expected 'Using the only available session' message, got:\n{stdout}"
    );
}

// Auto-select from multiple sessions → prints "Using the latest session"
#[tokio::test]
async fn job_logs_auto_select_multiple_sessions_prints_message() {
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

    let output = harness.cli(&["job", "logs"])
        .output()
        .expect("failed to run");

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(output.status.success(), "got: {stdout}");
    assert!(
        stdout.contains("Using the latest session"),
        "Expected 'Using the latest session' message, got:\n{stdout}"
    );
}

// Auto-select messages should NOT appear in JSON output
#[tokio::test]
async fn job_logs_auto_select_json_no_message() {
    let harness = TestHarness::new().await;
    setup_config(&harness);

    jobs::mock_get_job(&harness.server, "farm-abc", "queue-abc", json!({
        "jobId": "job-aaa", "name": "Render Job",
    })).await;

    sessions::mock_list_sessions(&harness.server, "farm-abc", "queue-abc", "job-aaa", &[
        json!({"sessionId": "session-only", "startedAt": "2024-12-18T00:00:00Z", "endedAt": "2024-12-18T01:00:00Z", "fleetId": "fleet-abc", "workerId": "worker-001"}),
    ]).await;

    sessions::mock_get_session(&harness.server, "farm-abc", "queue-abc", "job-aaa", json!({
        "sessionId": "session-only", "startedAt": "2024-12-18T00:00:00Z",
        "fleetId": "fleet-abc", "workerId": "worker-001",
    })).await;

    cloudwatch::mock_get_log_events(&harness.server, &[
        json!({"timestamp": 1702857600000_i64, "message": "log line"}),
    ], None).await;

    let output = harness.cli(&["job", "logs", "--output", "json"])
        .output()
        .expect("failed to run");

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(output.status.success());
    // JSON output should be valid JSON (no extra text)
    let _: serde_json::Value = serde_json::from_str(&stdout)
        .unwrap_or_else(|e| panic!("Expected valid JSON (no auto-select message mixed in): {e}"));
    assert!(!stdout.contains("Using the"), "JSON output should not contain auto-select message");
}

// Auto-select from multiple sessions with one ongoing → "Using the latest session"
// (not "only" — there are 2+ sessions, even though one is ongoing)
#[tokio::test]
async fn job_logs_auto_select_ongoing_from_multiple_prints_latest_message() {
    let harness = TestHarness::new().await;
    setup_config(&harness);

    jobs::mock_get_job(&harness.server, "farm-abc", "queue-abc", json!({
        "jobId": "job-aaa", "name": "Render Job",
    })).await;

    sessions::mock_list_sessions(&harness.server, "farm-abc", "queue-abc", "job-aaa", &[
        json!({"sessionId": "session-ended", "startedAt": "2024-12-17T00:00:00Z", "endedAt": "2024-12-17T01:00:00Z", "fleetId": "fleet-abc", "workerId": "worker-001"}),
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

    let output = harness.cli(&["job", "logs"])
        .output()
        .expect("failed to run");

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(output.status.success(), "got: {stdout}");
    assert!(
        stdout.contains("Using the latest session"),
        "Expected 'Using the latest session' (not 'only'), got:\n{stdout}"
    );
}

// =====================================================================
// relative timestamp for auto-selected sessions uses session startedAt
// =====================================================================

#[tokio::test]
async fn job_logs_relative_timestamp_auto_selected_session_uses_session_start() {
    let harness = TestHarness::new().await;
    setup_config(&harness);

    jobs::mock_get_job(&harness.server, "farm-abc", "queue-abc", json!({
        "jobId": "job-aaa", "name": "Render Job",
    })).await;

    // Auto-select: no --session-id, single session available
    sessions::mock_list_sessions(&harness.server, "farm-abc", "queue-abc", "job-aaa", &[
        json!({
            "sessionId": "session-auto",
            "startedAt": "2023-12-18T00:00:00Z",
            "endedAt": "2023-12-18T01:00:00Z",
            "fleetId": "fleet-abc",
            "workerId": "worker-001",
        }),
    ]).await;

    // mock_get_session needed because the fix fetches startedAt after auto-selection
    sessions::mock_get_session(&harness.server, "farm-abc", "queue-abc", "job-aaa", json!({
        "sessionId": "session-auto",
        "startedAt": "2023-12-18T00:00:00Z",
        "fleetId": "fleet-abc",
        "workerId": "worker-001",
    })).await;

    // Event is 60 seconds after session start (2023-12-18T00:00:00Z = epoch 1702857600)
    cloudwatch::mock_get_log_events(&harness.server, &[
        json!({"timestamp": 1702857660000_i64, "message": "one minute in"}),
    ], None).await;

    let output = harness.cli(&["job", "logs", "--timestamp-format", "relative"])
        .output()
        .expect("failed to run");

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(output.status.success(), "got: {stdout}");
    // Relative to session start (2023-12-18T00:00:00Z), event at +60s should show 0:01:00
    // If the bug exists (falls back to now()), the delta would be huge (years), not 0:01:00
    assert!(
        stdout.contains("0:01:00"),
        "Expected relative timestamp 0:01:00 (relative to session startedAt), got:\n{stdout}"
    );
}

// =====================================================================
// CloudWatch AccessDeniedException shows error code, not generic "service error"
// =====================================================================

#[tokio::test]
async fn job_logs_cloudwatch_access_denied_shows_error_code() {
    let harness = TestHarness::new().await;
    setup_config(&harness);

    jobs::mock_get_job(&harness.server, "farm-abc", "queue-abc", json!({
        "jobId": "job-aaa", "name": "Render Job",
    })).await;

    sessions::mock_list_sessions(&harness.server, "farm-abc", "queue-abc", "job-aaa", &[
        json!({
            "sessionId": "session-abc",
            "lifecycleStatus": "ENDED",
            "startedAt": "2024-12-18T01:00:00Z",
            "endedAt": "2024-12-18T02:00:00Z",
            "fleetId": "fleet-abc",
            "workerId": "worker-001",
        }),
    ]).await;

    cloudwatch::mock_get_log_events_access_denied(&harness.server).await;

    let _guard = insta_settings().bind_to_scope();
    let output = harness.cmd(&["job", "logs"])
        .output()
        .expect("failed to run");

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(!output.status.success(), "Expected failure, got success");
    assert!(
        stdout.contains("AccessDeniedException"),
        "Expected 'AccessDeniedException' in error, got:\n{stdout}"
    );
}

// ===========================================================================
// --session-action-id flag
// ===========================================================================

// ---------------------------------------------------------------------------
// session-action-id derives session ID and scopes time window
// ---------------------------------------------------------------------------

#[tokio::test]
async fn job_logs_session_action_id_derives_session_and_scopes_time() {
    let harness = TestHarness::new().await;
    setup_config(&harness);

    jobs::mock_get_job(&harness.server, "farm-abc", "queue-abc", json!({
        "jobId": "job-aaa", "name": "Render Job",
    })).await;

    // The session action ID encodes the session UUID
    sessions::mock_get_session_action(&harness.server, "farm-abc", "queue-abc", "job-aaa", json!({
        "sessionActionId": "sessionaction-00000000000000000000000000000001-0",
        "startedAt": "2024-12-18T00:00:00Z",
        "endedAt": "2024-12-18T00:05:00Z",
        "status": "SUCCEEDED",
    })).await;

    // The derived session ID is session-{uuid}
    sessions::mock_get_session(&harness.server, "farm-abc", "queue-abc", "job-aaa", json!({
        "sessionId": "session-00000000000000000000000000000001",
        "startedAt": "2024-12-18T00:00:00Z",
        "fleetId": "fleet-abc",
        "workerId": "worker-001",
    })).await;

    cloudwatch::mock_get_log_events(&harness.server, &[
        json!({"timestamp": 1702857600000_i64, "message": "Action started"}),
    ], None).await;

    let _guard = insta_settings().bind_to_scope();
    assert_cmd_snapshot!(harness.cmd(&[
        "job", "logs",
        "--session-action-id", "sessionaction-00000000000000000000000000000001-0",
    ]));
}

// ---------------------------------------------------------------------------
// Invalid session-action-id format
// ---------------------------------------------------------------------------

#[tokio::test]
async fn job_logs_session_action_id_invalid_format_errors() {
    let harness = TestHarness::new().await;
    setup_config(&harness);

    jobs::mock_get_job(&harness.server, "farm-abc", "queue-abc", json!({
        "jobId": "job-aaa", "name": "Render Job",
    })).await;

    let _guard = insta_settings().bind_to_scope();
    assert_cmd_snapshot!(harness.cmd(&[
        "job", "logs",
        "--session-action-id", "not-a-valid-id",
    ]));
}

// ---------------------------------------------------------------------------
// Both --session-id and --session-action-id with mismatch
// ---------------------------------------------------------------------------

#[tokio::test]
async fn job_logs_session_action_id_conflicts_with_session_id_errors() {
    let harness = TestHarness::new().await;
    setup_config(&harness);

    jobs::mock_get_job(&harness.server, "farm-abc", "queue-abc", json!({
        "jobId": "job-aaa", "name": "Render Job",
    })).await;

    let _guard = insta_settings().bind_to_scope();
    assert_cmd_snapshot!(harness.cmd(&[
        "job", "logs",
        "--session-id", "session-ffffffffffffffffffffffffffffffff",
        "--session-action-id", "sessionaction-00000000000000000000000000000001-0",
    ]));
}

// ---------------------------------------------------------------------------
// session-action-id not found (404)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn job_logs_session_action_id_not_found_errors() {
    let harness = TestHarness::new().await;
    setup_config(&harness);

    jobs::mock_get_job(&harness.server, "farm-abc", "queue-abc", json!({
        "jobId": "job-aaa", "name": "Render Job",
    })).await;

    sessions::mock_get_session_action_error(
        &harness.server, "farm-abc", "queue-abc", "job-aaa",
        "sessionaction-00000000000000000000000000000001-0",
        404, "ResourceNotFoundException",
    ).await;

    let _guard = insta_settings().bind_to_scope();
    assert_cmd_snapshot!(harness.cmd(&[
        "job", "logs",
        "--session-action-id", "sessionaction-00000000000000000000000000000001-0",
    ]));
}

// ---------------------------------------------------------------------------
// session-action-id: action hasn't started yet (no startedAt)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn job_logs_session_action_id_not_started_errors() {
    let harness = TestHarness::new().await;
    setup_config(&harness);

    jobs::mock_get_job(&harness.server, "farm-abc", "queue-abc", json!({
        "jobId": "job-aaa", "name": "Render Job",
    })).await;

    sessions::mock_get_session_action(&harness.server, "farm-abc", "queue-abc", "job-aaa", json!({
        "sessionActionId": "sessionaction-00000000000000000000000000000001-0",
        "status": "ASSIGNED",
    })).await;

    let _guard = insta_settings().bind_to_scope();
    assert_cmd_snapshot!(harness.cmd(&[
        "job", "logs",
        "--session-action-id", "sessionaction-00000000000000000000000000000001-0",
    ]));
}

// ---------------------------------------------------------------------------
// session-action-id: both flags provided and they MATCH (should succeed)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn job_logs_session_action_id_matches_session_id_succeeds() {
    let harness = TestHarness::new().await;
    setup_config(&harness);

    jobs::mock_get_job(&harness.server, "farm-abc", "queue-abc", json!({
        "jobId": "job-aaa", "name": "Render Job",
    })).await;

    sessions::mock_get_session_action(&harness.server, "farm-abc", "queue-abc", "job-aaa", json!({
        "sessionActionId": "sessionaction-00000000000000000000000000000001-0",
        "startedAt": "2024-12-18T00:00:00Z",
        "endedAt": "2024-12-18T00:05:00Z",
        "status": "SUCCEEDED",
    })).await;

    sessions::mock_get_session(&harness.server, "farm-abc", "queue-abc", "job-aaa", json!({
        "sessionId": "session-00000000000000000000000000000001",
        "startedAt": "2024-12-18T00:00:00Z",
        "fleetId": "fleet-abc",
        "workerId": "worker-001",
    })).await;

    cloudwatch::mock_get_log_events(&harness.server, &[
        json!({"timestamp": 1702857600000_i64, "message": "Action started"}),
    ], None).await;

    let _guard = insta_settings().bind_to_scope();
    // Both flags provided, they match — should succeed
    assert_cmd_snapshot!(harness.cmd(&[
        "job", "logs",
        "--session-id", "session-00000000000000000000000000000001",
        "--session-action-id", "sessionaction-00000000000000000000000000000001-0",
    ]));
}

// ===========================================================================
// --timezone deprecated flag
// ===========================================================================

/// `--timezone utc` should be accepted as a deprecated alias for `--timestamp-format utc`.
/// A deprecation warning should appear on stderr.
#[tokio::test]
async fn job_logs_timezone_deprecated_flag() {
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
        json!({"timestamp": 1702857600000_i64, "message": "Test log line"}),
    ], None).await;

    let output = harness.cli(&[
        "job", "logs",
        "--session-id", "session-001",
        "--timezone", "utc",
    ]).output().expect("failed to run");

    assert!(output.status.success(), "Expected success, stderr: {}", String::from_utf8_lossy(&output.stderr));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("--timezone is deprecated"), "Expected deprecation warning on stderr, got: {stderr}");
}

/// Using both --timezone and --timestamp-format should produce an error.
#[tokio::test]
async fn job_logs_timezone_and_timestamp_format_conflict() {
    let harness = TestHarness::new().await;
    setup_config(&harness);

    let output = harness.cli(&[
        "job", "logs",
        "--session-id", "session-001",
        "--timezone", "utc",
        "--timestamp-format", "local",
    ]).output().expect("failed to run");

    assert!(!output.status.success(), "Expected failure when both flags are provided");
    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let combined = format!("{stdout}{stderr}");
    assert!(
        combined.contains("Cannot use both") || combined.contains("conflict"),
        "Expected conflict error, got stdout: {stdout}, stderr: {stderr}"
    );
}

/// Using both --timezone and --timestamp-format should error even when values match.
/// The conflict is about using both flags, not about the values.
#[tokio::test]
async fn job_logs_timezone_and_timestamp_format_conflict_same_value() {
    let harness = TestHarness::new().await;
    setup_config(&harness);

    let output = harness.cli(&[
        "job", "logs",
        "--session-id", "session-001",
        "--timezone", "utc",
        "--timestamp-format", "utc",
    ]).output().expect("failed to run");

    assert!(!output.status.success(), "Expected failure when both flags are provided, even with same value");
    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let combined = format!("{stdout}{stderr}");
    assert!(
        combined.contains("Cannot use both"),
        "Expected conflict error, got stdout: {stdout}, stderr: {stderr}"
    );
}
