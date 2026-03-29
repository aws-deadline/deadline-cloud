//! Level 2 tests for `deadline worker` subcommands (§43 cases 1-7).

use deadline_test_server::deadline_api::{errors, workers};
use deadline_test_server::TestHarness;
use insta_cmd::assert_cmd_snapshot;
use serde_json::json;

#[tokio::test]
async fn worker_list_prints_workers_with_count() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    workers::mock_search_workers(
        &harness.server, "farm-abc",
        &[
            json!({"workerId": "worker-aaa", "status": "RUNNING", "createdAt": "2024-01-01T00:00:00Z"}),
            json!({"workerId": "worker-bbb", "status": "IDLE", "createdAt": "2024-01-02T00:00:00Z"}),
        ],
        2,
    ).await;

    assert_cmd_snapshot!(harness.cmd(&["worker", "list", "--fleet-id", "fleet-abc"]));
}

#[tokio::test]
async fn worker_list_with_page_size_and_offset() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    workers::mock_search_workers(
        &harness.server, "farm-abc",
        &[json!({"workerId": "worker-ccc", "status": "IDLE", "createdAt": "2024-01-03T00:00:00Z"})],
        20,
    ).await;

    assert_cmd_snapshot!(harness.cmd(&[
        "worker", "list", "--fleet-id", "fleet-abc", "--page-size", "10", "--item-offset", "5",
    ]));
}

#[tokio::test]
async fn worker_list_no_fleet_id_exits_with_error() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();

    assert_cmd_snapshot!(harness.cmd(&["worker", "list"]));
}

#[tokio::test]
async fn worker_list_api_failure_prints_error() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    errors::mock_search_workers_access_denied(&harness.server, "farm-abc", "Access denied").await;

    assert_cmd_snapshot!(harness.cmd(&["worker", "list", "--fleet-id", "fleet-abc"]));
}

#[tokio::test]
async fn worker_get_prints_details() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    workers::mock_get_worker(
        &harness.server, "farm-abc", "fleet-abc",
        json!({"workerId": "worker-aaa", "fleetId": "fleet-abc", "farmId": "farm-abc", "status": "RUNNING"}),
    ).await;

    assert_cmd_snapshot!(harness.cmd(&[
        "worker", "get", "--fleet-id", "fleet-abc", "--worker-id", "worker-aaa",
    ]));
}

#[tokio::test]
async fn worker_get_missing_fleet_id_exits_with_error() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();

    assert_cmd_snapshot!(harness.cmd(&["worker", "get", "--worker-id", "worker-aaa"]));
}

#[tokio::test]
async fn worker_get_api_failure_prints_error() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    errors::mock_get_worker_not_found(
        &harness.server, "farm-abc", "fleet-abc", "worker-bad", "Worker not found",
    ).await;

    assert_cmd_snapshot!(harness.cmd(&[
        "worker", "get", "--fleet-id", "fleet-abc", "--worker-id", "worker-bad",
    ]));
}
