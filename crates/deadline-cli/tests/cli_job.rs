//! Level 2 tests for `deadline job` subcommands (list/get only).

use deadline_test_server::TestHarness;
use deadline_test_server::deadline_api::jobs;
use predicates::prelude::*;
use serde_json::json;

#[tokio::test]
async fn job_list_prints_job_ids_and_names() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.queue_id", "queue-abc"]).assert().success();
    jobs::mock_list_jobs(&harness.server, "farm-abc", "queue-abc", &[
        json!({"jobId": "job-aaa", "name": "Render Job"}),
    ]).await;

    harness.cli(&["job", "list"])
        .assert().success()
        .stdout(predicate::str::contains("job-aaa"))
        .stdout(predicate::str::contains("Render Job"));
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

    harness.cli(&["job", "get"])
        .assert().success()
        .stdout(predicate::str::contains("job-aaa"))
        .stdout(predicate::str::contains("Render Job"));
}

#[tokio::test]
async fn job_get_no_job_id_exits_with_error() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.queue_id", "queue-abc"]).assert().success();

    harness.cli(&["job", "get"])
        .assert().code(1)
        .stdout(predicate::str::contains("job"));
}
