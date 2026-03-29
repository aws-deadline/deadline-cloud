//! Level 2 tests for `deadline queue` subcommands (list/get only).

use deadline_test_server::TestHarness;
use deadline_test_server::deadline_api::queues;
use predicates::prelude::*;
use serde_json::json;

#[tokio::test]
async fn queue_list_prints_queue_ids_and_names() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    queues::mock_list_queues(&harness.server, "farm-abc", &[
        json!({"queueId": "queue-aaa", "displayName": "Queue A"}),
    ]).await;

    harness.cli(&["queue", "list"])
        .assert().success()
        .stdout(predicate::str::contains("queue-aaa"))
        .stdout(predicate::str::contains("Queue A"));
}

#[tokio::test]
async fn queue_list_no_farm_id_exits_with_error() {
    let harness = TestHarness::new().await;
    harness.cli(&["queue", "list"])
        .assert().code(1)
        .stdout(predicate::str::contains("farm"));
}

#[tokio::test]
async fn queue_get_prints_details() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.queue_id", "queue-aaa"]).assert().success();
    queues::mock_get_queue(&harness.server, "farm-abc", json!({
        "queueId": "queue-aaa", "displayName": "My Queue",
    })).await;

    harness.cli(&["queue", "get"])
        .assert().success()
        .stdout(predicate::str::contains("queue-aaa"))
        .stdout(predicate::str::contains("My Queue"));
}
