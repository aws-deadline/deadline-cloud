//! Level 2 tests for `deadline job cancel` and `deadline job requeue-tasks`.

use deadline_test_server::deadline_api::{errors, jobs, sessions};
use deadline_test_server::TestHarness;
use insta_cmd::assert_cmd_snapshot;
use serde_json::json;

// --- helpers ---

async fn setup(harness: &TestHarness) {
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.queue_id", "queue-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.job_id", "job-aaf4cdf8aae242f58fb84c5bb19f199b"]).assert().success();
}

const FARM: &str = "farm-abc";
const QUEUE: &str = "queue-abc";
const JOB: &str = "job-aaf4cdf8aae242f58fb84c5bb19f199b";

// --- job cancel ---

#[tokio::test]
async fn job_cancel_with_yes_prints_summary_and_cancels() {
    let harness = TestHarness::new().await;
    setup(&harness).await;
    jobs::mock_get_job(&harness.server, FARM, QUEUE, json!({
        "jobId": JOB,
        "name": "Render Job", "lifecycleStatus": "CREATE_COMPLETE", "lifecycleStatusMessage": "", "priority": 50,
        "taskRunStatus": "RUNNING",
        "taskRunStatusCounts": {
            "RUNNING": 3, "SUCCEEDED": 7, "FAILED": 0,
            "PENDING": 0, "CANCELED": 0, "SUSPENDED": 0
        },
        "startedAt": "2025-01-27T07:37:53Z",
        "createdBy": "user-abc",
        "createdAt": "2025-01-27T07:34:41Z",
    })).await;
    jobs::mock_update_job(&harness.server, FARM, QUEUE, JOB).await;

    assert_cmd_snapshot!(harness.cmd(&["job", "cancel", "--yes"]));
}

#[tokio::test]
async fn job_cancel_mark_as_suspended_with_yes() {
    let harness = TestHarness::new().await;
    setup(&harness).await;
    jobs::mock_get_job(&harness.server, FARM, QUEUE, json!({
        "jobId": JOB,
        "name": "Render Job", "lifecycleStatus": "CREATE_COMPLETE", "lifecycleStatusMessage": "", "priority": 50,
        "taskRunStatus": "RUNNING",
        "taskRunStatusCounts": { "RUNNING": 3, "SUCCEEDED": 7 },
        "startedAt": "2025-01-27T07:37:53Z",
        "createdBy": "user-abc",
        "createdAt": "2025-01-27T07:34:41Z",
    })).await;
    jobs::mock_update_job(&harness.server, FARM, QUEUE, JOB).await;

    assert_cmd_snapshot!(harness.cmd(&["job", "cancel", "--mark-as", "SUSPENDED", "--yes"]));
}

#[tokio::test]
async fn job_cancel_get_job_fails_prints_error_with_suggestions() {
    let harness = TestHarness::new().await;
    setup(&harness).await;
    errors::mock_get_job_not_found(&harness.server, FARM, QUEUE, JOB).await;
    jobs::mock_list_jobs(&harness.server, FARM, QUEUE,
        &[json!({"jobId": "job-real", "name": "Real Job"})]).await;

    assert_cmd_snapshot!(harness.cmd(&["job", "cancel", "--yes"]));
}

// --mark-as BANANA should exit with error listing valid values
#[tokio::test]
async fn job_cancel_mark_as_invalid_value_exits_with_error() {
    let harness = TestHarness::new().await;
    setup(&harness).await;

    assert_cmd_snapshot!(harness.cmd(&["job", "cancel", "--mark-as", "BANANA", "--yes"]));
}

// --- job requeue-tasks ---

#[tokio::test]
async fn job_requeue_tasks_with_yes_requeues_failed_tasks() {
    let harness = TestHarness::new().await;
    setup(&harness).await;
    jobs::mock_get_job(&harness.server, FARM, QUEUE, json!({
        "jobId": JOB, "name": "Render Job", "lifecycleStatus": "CREATE_COMPLETE", "lifecycleStatusMessage": "", "priority": 50,
        "taskRunStatus": "FAILED",
        "taskRunStatusCounts": { "FAILED": 2, "SUCCEEDED": 5, "CANCELED": 1 },
    })).await;
    sessions::mock_list_steps(&harness.server, FARM, QUEUE, JOB, &[json!({
        "stepId": "step-aaaa", "name": "Render Step",
        "taskRunStatusCounts": { "FAILED": 2, "SUCCEEDED": 5, "CANCELED": 1 },
    })]).await;
    sessions::mock_list_tasks(&harness.server, FARM, QUEUE, JOB, "step-aaaa", &[
        json!({"taskId": "task-0001", "runStatus": "FAILED"}),
        json!({"taskId": "task-0002", "runStatus": "SUCCEEDED"}),
        json!({"taskId": "task-0003", "runStatus": "FAILED"}),
        json!({"taskId": "task-0004", "runStatus": "CANCELED"}),
    ]).await;
    for tid in &["task-0001", "task-0003", "task-0004"] {
        sessions::mock_update_task(&harness.server, FARM, QUEUE, JOB, "step-aaaa", tid).await;
    }

    assert_cmd_snapshot!(harness.cmd(&["job", "requeue-tasks", "--yes"]));
}

#[tokio::test]
async fn job_requeue_tasks_no_matching_tasks_exits_zero() {
    let harness = TestHarness::new().await;
    setup(&harness).await;
    jobs::mock_get_job(&harness.server, FARM, QUEUE, json!({
        "jobId": JOB, "name": "Render Job", "lifecycleStatus": "CREATE_COMPLETE", "lifecycleStatusMessage": "", "priority": 50,
        "taskRunStatus": "SUCCEEDED",
        "taskRunStatusCounts": { "SUCCEEDED": 10, "FAILED": 0, "CANCELED": 0, "SUSPENDED": 0 },
    })).await;

    assert_cmd_snapshot!(harness.cmd(&["job", "requeue-tasks", "--yes"]));
}

#[tokio::test]
async fn job_requeue_tasks_custom_run_status() {
    let harness = TestHarness::new().await;
    setup(&harness).await;
    jobs::mock_get_job(&harness.server, FARM, QUEUE, json!({
        "jobId": JOB, "name": "Render Job", "lifecycleStatus": "CREATE_COMPLETE", "lifecycleStatusMessage": "", "priority": 50,
        "taskRunStatus": "FAILED",
        "taskRunStatusCounts": { "FAILED": 2, "SUCCEEDED": 5, "CANCELED": 1 },
    })).await;
    sessions::mock_list_steps(&harness.server, FARM, QUEUE, JOB, &[json!({
        "stepId": "step-aaaa", "name": "Render Step",
        "taskRunStatusCounts": { "FAILED": 2, "SUCCEEDED": 5, "CANCELED": 1 },
    })]).await;
    sessions::mock_list_tasks(&harness.server, FARM, QUEUE, JOB, "step-aaaa", &[
        json!({"taskId": "task-0001", "runStatus": "FAILED"}),
        json!({"taskId": "task-0002", "runStatus": "SUCCEEDED"}),
        json!({"taskId": "task-0003", "runStatus": "FAILED"}),
        json!({"taskId": "task-0004", "runStatus": "CANCELED"}),
    ]).await;
    // --run-status FAILED --run-status SUCCEEDED: matches task-0001, task-0002, task-0003
    for tid in &["task-0001", "task-0002", "task-0003"] {
        sessions::mock_update_task(&harness.server, FARM, QUEUE, JOB, "step-aaaa", tid).await;
    }

    assert_cmd_snapshot!(harness.cmd(&[
        "job", "requeue-tasks", "--run-status", "FAILED", "--run-status", "SUCCEEDED", "--yes"
    ]));
}

// Task parameters use union type format per Smithy TaskParameterValue —
// must be {"Frame": {"int": "1"}}, NOT {"Frame": "1"}.
#[tokio::test]
async fn job_requeue_tasks_with_parameters_shows_param_format() {
    let harness = TestHarness::new().await;
    setup(&harness).await;
    jobs::mock_get_job(&harness.server, FARM, QUEUE, json!({
        "jobId": JOB, "name": "Render Job", "lifecycleStatus": "CREATE_COMPLETE", "lifecycleStatusMessage": "", "priority": 50,
        "taskRunStatus": "FAILED",
        "taskRunStatusCounts": { "FAILED": 1 },
    })).await;
    sessions::mock_list_steps(&harness.server, FARM, QUEUE, JOB, &[json!({
        "stepId": "step-aaaa", "name": "Render Step",
        "taskRunStatusCounts": { "FAILED": 1 },
    })]).await;
    sessions::mock_list_tasks(&harness.server, FARM, QUEUE, JOB, "step-aaaa", &[json!({
        "taskId": "task-0001", "runStatus": "FAILED",
        "parameters": {
            "Frame": { "int": "1" },
            "Camera": { "string": "main" }
        },
    })]).await;
    sessions::mock_update_task(&harness.server, FARM, QUEUE, JOB, "step-aaaa", "task-0001").await;

    assert_cmd_snapshot!(harness.cmd(&["job", "requeue-tasks", "--yes"]));
}

// step with no matching tasks prints message and continues
#[tokio::test]
async fn job_requeue_tasks_step_with_no_matching_tasks() {
    let harness = TestHarness::new().await;
    setup(&harness).await;
    jobs::mock_get_job(&harness.server, FARM, QUEUE, json!({
        "jobId": JOB, "name": "Render Job", "lifecycleStatus": "CREATE_COMPLETE", "lifecycleStatusMessage": "", "priority": 50,
        "taskRunStatus": "FAILED",
        "taskRunStatusCounts": { "FAILED": 1, "SUCCEEDED": 5 },
    })).await;
    sessions::mock_list_steps(&harness.server, FARM, QUEUE, JOB, &[
        json!({"stepId": "step-aaaa", "name": "Setup Step", "taskRunStatusCounts": {"SUCCEEDED": 5}}),
        json!({"stepId": "step-bbbb", "name": "Render Step", "taskRunStatusCounts": {"FAILED": 1}}),
    ]).await;
    sessions::mock_list_tasks(&harness.server, FARM, QUEUE, JOB, "step-aaaa",
        &[json!({"taskId": "task-0001", "runStatus": "SUCCEEDED"})]).await;
    sessions::mock_list_tasks(&harness.server, FARM, QUEUE, JOB, "step-bbbb",
        &[json!({"taskId": "task-0002", "runStatus": "FAILED"})]).await;
    sessions::mock_update_task(&harness.server, FARM, QUEUE, JOB, "step-bbbb", "task-0002").await;

    assert_cmd_snapshot!(harness.cmd(&["job", "requeue-tasks", "--yes"]));
}

// --run-status BANANA should exit with error listing valid values
#[tokio::test]
async fn job_requeue_tasks_run_status_invalid_value_exits_with_error() {
    let harness = TestHarness::new().await;
    setup(&harness).await;

    assert_cmd_snapshot!(harness.cmd(&["job", "requeue-tasks", "--run-status", "BANANA", "--yes"]));
}

// cancel confirmation with empty input should re-prompt (not treat as "no")
#[tokio::test]
async fn job_cancel_confirm_empty_input_reprompts() {
    let harness = TestHarness::new().await;
    setup(&harness).await;
    jobs::mock_get_job(&harness.server, FARM, QUEUE, json!({
        "jobId": JOB,
        "name": "Render Job", "lifecycleStatus": "CREATE_COMPLETE", "lifecycleStatusMessage": "", "priority": 50,
        "taskRunStatus": "RUNNING",
        "taskRunStatusCounts": { "RUNNING": 3, "SUCCEEDED": 7 },
        "startedAt": "2025-01-27T07:37:53Z",
        "createdBy": "user-abc",
        "createdAt": "2025-01-27T07:34:41Z",
    })).await;

    // Send empty line then "n" — Python re-prompts on empty, then accepts "n"
    let output = harness.cli(&["job", "cancel"])
        .write_stdin("\nn\n")
        .output()
        .expect("failed to run");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = format!("{stdout}{stderr}");
    // After the fix, empty input should trigger "Error: invalid input" and re-prompt
    assert!(combined.contains("invalid input") || combined.contains("Error"),
        "expected re-prompt on empty input, got stdout: {stdout}\nstderr: {stderr}");
}

// EOF on stdin should exit cleanly, not loop forever
#[tokio::test]
async fn job_cancel_confirm_eof_exits_cleanly() {
    let harness = TestHarness::new().await;
    setup(&harness).await;
    jobs::mock_get_job(&harness.server, FARM, QUEUE, json!({
        "jobId": JOB,
        "name": "Render Job", "lifecycleStatus": "CREATE_COMPLETE", "lifecycleStatusMessage": "", "priority": 50,
        "taskRunStatus": "RUNNING",
        "taskRunStatusCounts": { "RUNNING": 3, "SUCCEEDED": 7 },
        "startedAt": "2025-01-27T07:37:53Z",
        "createdBy": "user-abc",
        "createdAt": "2025-01-27T07:34:41Z",
    })).await;

    // Pipe empty stdin (immediate EOF) — should exit, not hang
    let output = harness.cli(&["job", "cancel"])
        .write_stdin("")
        .timeout(std::time::Duration::from_secs(5))
        .output()
        .expect("failed to run");

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(!output.status.success(), "should exit non-zero on EOF");
    assert!(stdout.contains("Job not canceled."), "expected 'Job not canceled.' on EOF, got: {stdout}");
}

#[tokio::test]
async fn job_requeue_tasks_get_job_fails_prints_error() {
    let harness = TestHarness::new().await;
    setup(&harness).await;
    errors::mock_get_job_not_found(&harness.server, FARM, QUEUE, JOB).await;
    jobs::mock_list_jobs(&harness.server, FARM, QUEUE,
        &[json!({"jobId": "job-real", "name": "Real Job"})]).await;

    assert_cmd_snapshot!(harness.cmd(&["job", "requeue-tasks", "--yes"]));
}
