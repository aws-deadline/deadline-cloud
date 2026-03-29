//! Level 2 tests for `deadline worker` subcommands (§43 cases 1-7).

use deadline_test_server::deadline_api::{errors, workers};
use deadline_test_server::TestHarness;
use predicates::prelude::*;
use serde_json::json;

// §43 case 1: worker list with fleet-id and farm prints workers with count
#[tokio::test]
async fn worker_list_prints_workers_with_count() {
    let harness = TestHarness::new().await;
    harness
        .cli(&["config", "set", "defaults.farm_id", "farm-abc"])
        .assert()
        .success();

    workers::mock_search_workers(
        &harness.server,
        "farm-abc",
        &[
            json!({"workerId": "worker-aaa", "status": "RUNNING", "createdAt": "2024-01-01T00:00:00Z"}),
            json!({"workerId": "worker-bbb", "status": "IDLE", "createdAt": "2024-01-02T00:00:00Z"}),
        ],
        2,
    )
    .await;

    harness
        .cli(&["worker", "list", "--fleet-id", "fleet-abc"])
        .assert()
        .success()
        .stdout(predicate::str::contains("worker-aaa"))
        .stdout(predicate::str::contains("worker-bbb"))
        .stdout(predicate::str::contains("Displaying 2 of 2"));
}

// §43 case 2: worker list with page-size and item-offset
#[tokio::test]
async fn worker_list_with_page_size_and_offset() {
    let harness = TestHarness::new().await;
    harness
        .cli(&["config", "set", "defaults.farm_id", "farm-abc"])
        .assert()
        .success();

    workers::mock_search_workers(
        &harness.server,
        "farm-abc",
        &[json!({"workerId": "worker-ccc", "status": "IDLE", "createdAt": "2024-01-03T00:00:00Z"})],
        20,
    )
    .await;

    harness
        .cli(&[
            "worker", "list", "--fleet-id", "fleet-abc", "--page-size", "10", "--item-offset", "5",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("worker-ccc"))
        .stdout(predicate::str::contains("starting at 5"));
}

// §43 case 3: worker list without fleet-id exits with error
#[tokio::test]
async fn worker_list_no_fleet_id_exits_with_error() {
    let harness = TestHarness::new().await;
    harness
        .cli(&["config", "set", "defaults.farm_id", "farm-abc"])
        .assert()
        .success();

    harness
        .cli(&["worker", "list"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("--fleet-id"));
}

// §43 case 4: worker list API failure prints error with suggestions
#[tokio::test]
async fn worker_list_api_failure_prints_error() {
    let harness = TestHarness::new().await;
    harness
        .cli(&["config", "set", "defaults.farm_id", "farm-abc"])
        .assert()
        .success();

    errors::mock_search_workers_access_denied(&harness.server, "farm-abc", "Access denied").await;

    harness
        .cli(&["worker", "list", "--fleet-id", "fleet-abc"])
        .assert()
        .code(1)
        .stdout(predicate::str::contains("Failed to get Workers"));
}

// §43 case 5: worker get with fleet-id and worker-id prints details
#[tokio::test]
async fn worker_get_prints_details() {
    let harness = TestHarness::new().await;
    harness
        .cli(&["config", "set", "defaults.farm_id", "farm-abc"])
        .assert()
        .success();

    workers::mock_get_worker(
        &harness.server,
        "farm-abc",
        "fleet-abc",
        json!({
            "workerId": "worker-aaa",
            "fleetId": "fleet-abc",
            "farmId": "farm-abc",
            "status": "RUNNING",
        }),
    )
    .await;

    harness
        .cli(&[
            "worker", "get", "--fleet-id", "fleet-abc", "--worker-id", "worker-aaa",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("worker-aaa"))
        .stdout(predicate::str::contains("RUNNING"));
}

// §43 case 6: worker get without required args exits with error
#[tokio::test]
async fn worker_get_missing_fleet_id_exits_with_error() {
    let harness = TestHarness::new().await;
    harness
        .cli(&["config", "set", "defaults.farm_id", "farm-abc"])
        .assert()
        .success();

    harness
        .cli(&["worker", "get", "--worker-id", "worker-aaa"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("--fleet-id"));
}

// §43 case 7: worker get API failure prints error
#[tokio::test]
async fn worker_get_api_failure_prints_error() {
    let harness = TestHarness::new().await;
    harness
        .cli(&["config", "set", "defaults.farm_id", "farm-abc"])
        .assert()
        .success();

    // Mount a 404 for the specific worker path
    errors::mock_get_worker_not_found(
        &harness.server,
        "farm-abc",
        "fleet-abc",
        "worker-bad",
        "Worker not found",
    )
    .await;

    harness
        .cli(&[
            "worker", "get", "--fleet-id", "fleet-abc", "--worker-id", "worker-bad",
        ])
        .assert()
        .code(1)
        .stdout(predicate::str::contains("Failed to get Worker"));
}
