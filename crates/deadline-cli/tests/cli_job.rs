//! Level 2 tests for `deadline job` subcommands.

use deadline_test_server::TestHarness;
use deadline_test_server::deadline_api::jobs;
use insta_cmd::assert_cmd_snapshot;
use serde_json::json;

fn mock_jobs() -> Vec<serde_json::Value> {
    vec![
        json!({
            "jobId": "job-aaf4cdf8aae242f58fb84c5bb19f199b",
            "name": "CLI Job",
            "taskRunStatus": "RUNNING",
            "lifecycleStatus": "SUCCEEDED",
            "createdBy": "b801f3c0-c071-70bc-b869-6804bc732408",
            "createdAt": "2023-01-27T07:34:41Z",
            "startedAt": "2023-01-27T07:37:53Z",
            "endedAt": "2023-01-27T07:39:17Z",
            "priority": 50,
        }),
        json!({
            "jobId": "job-0d239749fa05435f90263b3a8be54144",
            "name": "CLI Job",
            "taskRunStatus": "COMPLETED",
            "lifecycleStatus": "SUCCEEDED",
            "createdBy": "b801f3c0-c071-70bc-b869-6804bc732408",
            "createdAt": "2023-01-27T07:24:22Z",
            "startedAt": "2023-01-27T07:27:06Z",
            "endedAt": "2023-01-27T07:29:51Z",
            "priority": 50,
        }),
    ]
}

#[tokio::test]
async fn job_list_prints_jobs_with_count() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.queue_id", "queue-abc"]).assert().success();
    jobs::mock_search_jobs(&harness.server, "farm-abc", &mock_jobs(), 12).await;

    assert_cmd_snapshot!(harness.cmd(&["job", "list"]));
}

#[tokio::test]
async fn job_list_with_page_size_and_offset() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.queue_id", "queue-abc"]).assert().success();
    jobs::mock_search_jobs(&harness.server, "farm-abc", &mock_jobs()[..1], 12).await;

    assert_cmd_snapshot!(harness.cmd(&["job", "list", "--page-size", "1", "--item-offset", "3"]));
}

#[tokio::test]
async fn job_get_prints_details() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.queue_id", "queue-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.job_id", "job-aaa"]).assert().success();
    jobs::mock_get_job(&harness.server, "farm-abc", "queue-abc", json!({
        "jobId": "job-aaa", "name": "Render Job", "lifecycleStatus": "CREATE_COMPLETE",
    })).await;

    assert_cmd_snapshot!(harness.cmd(&["job", "get"]));
}

#[tokio::test]
async fn job_get_no_job_id_exits_with_error() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.queue_id", "queue-abc"]).assert().success();

    assert_cmd_snapshot!(harness.cmd(&["job", "get"]));
}
