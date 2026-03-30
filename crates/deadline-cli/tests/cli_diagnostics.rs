//! Level 2 tests for diagnostic API commands: get-session, list-sessions, list-steps, list-tasks.

use deadline_test_server::TestHarness;
use deadline_test_server::deadline_api::sessions;
use insta_cmd::assert_cmd_snapshot;
use serde_json::json;

// --- job get-session ---

#[tokio::test]
async fn job_get_session_prints_details() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.queue_id", "queue-aaa"]).assert().success();
    sessions::mock_get_session(
        &harness.server,
        "farm-abc",
        "queue-aaa",
        "job-001",
        json!({
            "sessionId": "session-001",
            "fleetId": "fleet-abc",
            "workerId": "worker-001",
            "startedAt": "2024-12-18T00:00:00Z",
            "lifecycleStatus": "ENDED",
            "endedAt": "2024-12-18T01:00:00Z"
        }),
    )
    .await;

    assert_cmd_snapshot!(harness.cmd(&[
        "job", "get-session",
        "--job-id", "job-001",
        "--session-id", "session-001"
    ]));
}

// --- job list-sessions ---

#[tokio::test]
async fn job_list_sessions_prints_sessions() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.queue_id", "queue-aaa"]).assert().success();
    sessions::mock_list_sessions(
        &harness.server,
        "farm-abc",
        "queue-aaa",
        "job-001",
        &[
            json!({
                "sessionId": "session-001",
                "fleetId": "fleet-abc",
                "workerId": "worker-001",
                "startedAt": "2024-12-18T00:00:00Z",
                "lifecycleStatus": "ENDED"
            }),
            json!({
                "sessionId": "session-002",
                "fleetId": "fleet-abc",
                "workerId": "worker-002",
                "startedAt": "2024-12-18T00:30:00Z",
                "lifecycleStatus": "STARTED"
            }),
        ],
    )
    .await;

    assert_cmd_snapshot!(harness.cmd(&[
        "job", "list-sessions",
        "--job-id", "job-001"
    ]));
}

// --- job list-steps ---

#[tokio::test]
async fn job_list_steps_prints_steps() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.queue_id", "queue-aaa"]).assert().success();
    sessions::mock_list_steps(
        &harness.server,
        "farm-abc",
        "queue-aaa",
        "job-001",
        &[
            json!({
                "stepId": "step-001",
                "name": "Render",
                "lifecycleStatus": "COMPLETE",
                "createdAt": "2024-12-18T00:00:00Z"
            }),
        ],
    )
    .await;

    assert_cmd_snapshot!(harness.cmd(&[
        "job", "list-steps",
        "--job-id", "job-001"
    ]));
}

// --- job list-tasks ---

#[tokio::test]
async fn job_list_tasks_prints_tasks() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.queue_id", "queue-aaa"]).assert().success();
    sessions::mock_list_tasks(
        &harness.server,
        "farm-abc",
        "queue-aaa",
        "job-001",
        "step-001",
        &[
            json!({
                "taskId": "task-001",
                "runStatus": "SUCCEEDED",
                "createdAt": "2024-12-18T00:00:00Z"
            }),
            json!({
                "taskId": "task-002",
                "runStatus": "FAILED",
                "createdAt": "2024-12-18T00:01:00Z"
            }),
        ],
    )
    .await;

    assert_cmd_snapshot!(harness.cmd(&[
        "job", "list-tasks",
        "--job-id", "job-001",
        "--step-id", "step-001"
    ]));
}
