//! Level 2 tests for `deadline job` subcommands (list/get only).

use deadline_test_server::TestHarness;
use deadline_test_server::deadline_api::jobs;
use insta_cmd::assert_cmd_snapshot;
use serde_json::json;

#[tokio::test]
async fn job_list_prints_job_ids_and_names() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.queue_id", "queue-abc"]).assert().success();
    jobs::mock_list_jobs(&harness.server, "farm-abc", "queue-abc", &[
        json!({"jobId": "job-aaa", "name": "Render Job"}),
    ]).await;

    assert_cmd_snapshot!(harness.cmd(&["job", "list"]));
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
