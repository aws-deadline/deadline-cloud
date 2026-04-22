//! Level 2 tests for `deadline job wait` subcommand.
//!
//! Covers job completion polling and CLI wait output formatting.
//! Callbacks are partially covered via verbose/json output behavior.
//! Exponential backoff timing is not directly testable at Level 2.

use deadline_test_server::TestHarness;
use deadline_test_server::deadline_api::{jobs, sessions};
use insta_cmd::assert_cmd_snapshot;
use serde_json::json;

fn insta_settings() -> insta::Settings {
    let mut settings = insta::Settings::clone_current();
    settings.add_filter(r"Elapsed time: .* seconds", "Elapsed time: [TIME] seconds");
    settings.add_filter(r"\[[\d.]+s elapsed.*\]", "[ELAPSED]");
    settings.add_filter(r"after [\d.]+ seconds", "after [TIME] seconds");
    settings.add_filter(r"\r.*\n", "");  // strip \r status line overwrites
    settings
}

// ---------------------------------------------------------------------------
// Job completes with SUCCEEDED, exits 0
// ---------------------------------------------------------------------------

#[tokio::test]
async fn job_wait_succeeded_exits_0() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.queue_id", "queue-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.job_id", "job-aaa"]).assert().success();

    jobs::mock_get_job(&harness.server, "farm-abc", "queue-abc", json!({
        "jobId": "job-aaa",
        "name": "Render Job",
        "taskRunStatus": "SUCCEEDED",
        "taskRunStatusCounts": { "SUCCEEDED": 10 },
    })).await;

    let _guard = insta_settings().bind_to_scope();
    assert_cmd_snapshot!(harness.cmd(&["job", "wait"]));
}

// ---------------------------------------------------------------------------
// Job completes with FAILED, exits 2, has failed_tasks
// ---------------------------------------------------------------------------

#[tokio::test]
async fn job_wait_failed_exits_2_with_failed_tasks() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.queue_id", "queue-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.job_id", "job-aaa"]).assert().success();

    jobs::mock_get_job(&harness.server, "farm-abc", "queue-abc", json!({
        "jobId": "job-aaa",
        "name": "Render Job",
        "taskRunStatus": "FAILED",
        "taskRunStatusCounts": { "FAILED": 1, "SUCCEEDED": 5 },
    })).await;

    sessions::mock_list_steps(&harness.server, "farm-abc", "queue-abc", "job-aaa", &[
        json!({
            "stepId": "step-001",
            "name": "Render Step",
            "taskRunStatusCounts": { "FAILED": 1, "SUCCEEDED": 5 },
        }),
    ]).await;

    sessions::mock_list_tasks(&harness.server, "farm-abc", "queue-abc", "job-aaa", "step-001", &[
        json!({
            "taskId": "task-001",
            "runStatus": "FAILED",
            "parameters": { "Frame": { "int": "1" } },
            "latestSessionActionId": "sessionaction-abc123def45678901234567890abcdef-3",
        }),
        json!({
            "taskId": "task-002",
            "runStatus": "SUCCEEDED",
            "parameters": { "Frame": { "int": "2" } },
        }),
    ]).await;

    let _guard = insta_settings().bind_to_scope();
    assert_cmd_snapshot!(harness.cmd(&["job", "wait"]));
}

// ---------------------------------------------------------------------------
// Job completes with CANCELED
// ---------------------------------------------------------------------------

#[tokio::test]
async fn job_wait_canceled_exits_3() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.queue_id", "queue-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.job_id", "job-aaa"]).assert().success();

    jobs::mock_get_job(&harness.server, "farm-abc", "queue-abc", json!({
        "jobId": "job-aaa",
        "name": "Render Job",
        "taskRunStatus": "CANCELED",
        "taskRunStatusCounts": { "CANCELED": 10 },
    })).await;

    sessions::mock_list_steps(&harness.server, "farm-abc", "queue-abc", "job-aaa", &[]).await;

    let output = harness.cli(&["job", "wait"])
        .output()
        .expect("failed to run");

    assert_eq!(output.status.code(), Some(3), "expected exit code 3 for CANCELED");
}

// ---------------------------------------------------------------------------
// Job completes with SUSPENDED
// ---------------------------------------------------------------------------

#[tokio::test]
async fn job_wait_suspended_exits_4() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.queue_id", "queue-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.job_id", "job-aaa"]).assert().success();

    jobs::mock_get_job(&harness.server, "farm-abc", "queue-abc", json!({
        "jobId": "job-aaa",
        "name": "Render Job",
        "taskRunStatus": "SUSPENDED",
        "taskRunStatusCounts": { "SUSPENDED": 10 },
    })).await;

    sessions::mock_list_steps(&harness.server, "farm-abc", "queue-abc", "job-aaa", &[]).await;

    let output = harness.cli(&["job", "wait"])
        .output()
        .expect("failed to run");

    assert_eq!(output.status.code(), Some(4), "expected exit code 4 for SUSPENDED");
}

// ---------------------------------------------------------------------------
// Job completes with NOT_COMPATIBLE
// ---------------------------------------------------------------------------

#[tokio::test]
async fn job_wait_not_compatible_exits_5() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.queue_id", "queue-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.job_id", "job-aaa"]).assert().success();

    jobs::mock_get_job(&harness.server, "farm-abc", "queue-abc", json!({
        "jobId": "job-aaa",
        "name": "Render Job",
        "taskRunStatus": "NOT_COMPATIBLE",
        "taskRunStatusCounts": {},
    })).await;

    sessions::mock_list_steps(&harness.server, "farm-abc", "queue-abc", "job-aaa", &[]).await;

    let output = harness.cli(&["job", "wait"])
        .output()
        .expect("failed to run");

    assert_eq!(output.status.code(), Some(5), "expected exit code 5 for NOT_COMPATIBLE");
}

// ---------------------------------------------------------------------------
// Timeout exceeded
// ---------------------------------------------------------------------------

#[tokio::test]
async fn job_wait_timeout_exits_1() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.queue_id", "queue-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.job_id", "job-aaa"]).assert().success();

    jobs::mock_get_job(&harness.server, "farm-abc", "queue-abc", json!({
        "jobId": "job-aaa",
        "name": "Render Job",
        "taskRunStatus": "RUNNING",
        "taskRunStatusCounts": { "RUNNING": 5 },
    })).await;

    let _guard = insta_settings().bind_to_scope();
    assert_cmd_snapshot!(harness.cmd(&["job", "wait", "--timeout", "1"]));
}

// ---------------------------------------------------------------------------
// Verbose status line on stderr
// ---------------------------------------------------------------------------

#[tokio::test]
async fn job_wait_verbose_shows_status_line() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.queue_id", "queue-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.job_id", "job-aaa"]).assert().success();

    jobs::mock_get_job(&harness.server, "farm-abc", "queue-abc", json!({
        "jobId": "job-aaa",
        "name": "Render Job",
        "taskRunStatus": "SUCCEEDED",
        "taskRunStatusCounts": { "SUCCEEDED": 10, "RUNNING": 0 },
    })).await;

    let _guard = insta_settings().bind_to_scope();
    assert_cmd_snapshot!(harness.cmd(&["job", "wait"]));
}

// ---------------------------------------------------------------------------
// get_job API error propagated
// ---------------------------------------------------------------------------

#[tokio::test]
async fn job_wait_api_error_exits_1() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.queue_id", "queue-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.job_id", "job-aaa"]).assert().success();

    // No mock — initial get_job for name fails via CliError (exit 1)
    let _guard = insta_settings().bind_to_scope();
    assert_cmd_snapshot!(harness.cmd(&["job", "wait"]));
}

// ---------------------------------------------------------------------------
// Failed tasks with full details + session_id extraction (JSON)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn job_wait_json_failed_tasks_have_full_details() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.queue_id", "queue-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.job_id", "job-aaa"]).assert().success();

    jobs::mock_get_job(&harness.server, "farm-abc", "queue-abc", json!({
        "jobId": "job-aaa",
        "name": "Render Job",
        "taskRunStatus": "FAILED",
        "taskRunStatusCounts": { "FAILED": 1 },
    })).await;

    sessions::mock_list_steps(&harness.server, "farm-abc", "queue-abc", "job-aaa", &[
        json!({
            "stepId": "step-001",
            "name": "Render Step",
            "taskRunStatusCounts": { "FAILED": 1 },
        }),
    ]).await;

    sessions::mock_list_tasks(&harness.server, "farm-abc", "queue-abc", "job-aaa", "step-001", &[
        json!({
            "taskId": "task-001",
            "createdAt": "2024-01-01T00:00:00Z",
            "createdBy": "user:test",
            "runStatus": "FAILED",
            "parameters": { "Frame": { "int": "1" } },
            "latestSessionActionId": "sessionaction-abc123def45678901234567890abcdef-3",
        }),
    ]).await;

    let output = harness.cli(&["job", "wait", "--output", "json"])
        .output()
        .expect("failed to run");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let parsed: serde_json::Value = serde_json::from_str(&stdout)
        .unwrap_or_else(|e| panic!("expected valid JSON, got error {e}: {stdout}"));
    let task = &parsed["failedTasks"][0];
    assert_eq!(task["stepId"], "step-001");
    assert_eq!(task["taskId"], "task-001");
    assert_eq!(task["stepName"], "Render Step");
    assert_eq!(task["sessionId"], "session-abc123def45678901234567890abcdef");
}

// ---------------------------------------------------------------------------
// latestSessionActionId absent — session_id is null (JSON)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn job_wait_json_failed_task_no_session_action_id() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.queue_id", "queue-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.job_id", "job-aaa"]).assert().success();

    jobs::mock_get_job(&harness.server, "farm-abc", "queue-abc", json!({
        "jobId": "job-aaa",
        "name": "Render Job",
        "taskRunStatus": "FAILED",
        "taskRunStatusCounts": { "FAILED": 1 },
    })).await;

    sessions::mock_list_steps(&harness.server, "farm-abc", "queue-abc", "job-aaa", &[
        json!({
            "stepId": "step-001",
            "name": "Render Step",
            "taskRunStatusCounts": { "FAILED": 1 },
        }),
    ]).await;

    sessions::mock_list_tasks(&harness.server, "farm-abc", "queue-abc", "job-aaa", "step-001", &[
        json!({
            "taskId": "task-001",
            "createdAt": "2024-01-01T00:00:00Z",
            "createdBy": "user:test",
            "runStatus": "FAILED",
            "parameters": {},
        }),
    ]).await;

    let output = harness.cli(&["job", "wait", "--output", "json"])
        .output()
        .expect("failed to run");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let parsed: serde_json::Value = serde_json::from_str(&stdout)
        .unwrap_or_else(|e| panic!("expected valid JSON, got error {e}: {stdout}"));
    let task = &parsed["failedTasks"][0];
    assert!(task["sessionId"].is_null(), "expected null sessionId when latestSessionActionId absent");
}

// ---------------------------------------------------------------------------
// Step with FAILED=0 is skipped (JSON)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn job_wait_skips_steps_with_no_failed_tasks() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.queue_id", "queue-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.job_id", "job-aaa"]).assert().success();

    jobs::mock_get_job(&harness.server, "farm-abc", "queue-abc", json!({
        "jobId": "job-aaa",
        "name": "Render Job",
        "taskRunStatus": "FAILED",
        "taskRunStatusCounts": { "FAILED": 1, "SUCCEEDED": 9 },
    })).await;

    sessions::mock_list_steps(&harness.server, "farm-abc", "queue-abc", "job-aaa", &[
        json!({
            "stepId": "step-001",
            "name": "Setup Step",
            "taskRunStatusCounts": { "SUCCEEDED": 5, "FAILED": 0 },
        }),
        json!({
            "stepId": "step-002",
            "name": "Render Step",
            "taskRunStatusCounts": { "FAILED": 1, "SUCCEEDED": 4 },
        }),
    ]).await;

    sessions::mock_list_tasks(&harness.server, "farm-abc", "queue-abc", "job-aaa", "step-002", &[
        json!({
            "taskId": "task-010",
            "createdAt": "2024-01-01T00:00:00Z",
            "createdBy": "user:test",
            "runStatus": "FAILED",
            "parameters": {},
        }),
    ]).await;

    let output = harness.cli(&["job", "wait", "--output", "json"])
        .output()
        .expect("failed to run");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let parsed: serde_json::Value = serde_json::from_str(&stdout)
        .unwrap_or_else(|e| panic!("expected valid JSON, got error {e}: {stdout}"));
    assert_eq!(parsed["failedTasks"].as_array().unwrap().len(), 1);
}

// ---------------------------------------------------------------------------
// JSON output for succeeded job
// ---------------------------------------------------------------------------

#[tokio::test]
async fn job_wait_json_output_succeeded() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.queue_id", "queue-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.job_id", "job-aaa"]).assert().success();

    jobs::mock_get_job(&harness.server, "farm-abc", "queue-abc", json!({
        "jobId": "job-aaa",
        "name": "Render Job",
        "taskRunStatus": "SUCCEEDED",
        "taskRunStatusCounts": { "SUCCEEDED": 10 },
    })).await;

    let output = harness.cli(&["job", "wait", "--output", "json"])
        .output()
        .expect("failed to run");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let parsed: serde_json::Value = serde_json::from_str(&stdout).expect("expected valid JSON");
    assert_eq!(parsed["status"], "SUCCEEDED");
    assert_eq!(parsed["jobId"], "job-aaa");
    assert_eq!(parsed["jobName"], "Render Job");
    assert!(parsed["elapsedTime"].is_number());
    assert!(parsed["failedTasks"].as_array().unwrap().is_empty());
}

// ---------------------------------------------------------------------------
// Missing job ID — usage error
// ---------------------------------------------------------------------------

#[tokio::test]
async fn job_wait_no_job_id_exits_with_error() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.queue_id", "queue-abc"]).assert().success();

    assert_cmd_snapshot!(harness.cmd(&["job", "wait"]));
}

// ---------------------------------------------------------------------------
// Timeout with JSON output
// ---------------------------------------------------------------------------

#[tokio::test]
async fn job_wait_timeout_json_output() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.queue_id", "queue-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.job_id", "job-aaa"]).assert().success();

    jobs::mock_get_job(&harness.server, "farm-abc", "queue-abc", json!({
        "jobId": "job-aaa",
        "name": "Render Job",
        "taskRunStatus": "RUNNING",
        "taskRunStatusCounts": { "RUNNING": 5 },
    })).await;

    let output = harness.cli(&["job", "wait", "--timeout", "1", "--output", "json"])
        .output()
        .expect("failed to run");

    assert_eq!(output.status.code(), Some(1));
    let stdout = String::from_utf8_lossy(&output.stdout);
    let parsed: serde_json::Value = serde_json::from_str(&stdout).expect("expected valid JSON");
    assert_eq!(parsed["timeout"], true);
    assert_eq!(parsed["jobId"], "job-aaa");
    assert!(parsed["error"].as_str().unwrap().contains("Timeout"));
}
