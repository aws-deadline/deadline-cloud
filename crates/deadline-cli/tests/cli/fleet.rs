//! Level 2 tests for `deadline fleet` subcommands.

use deadline_test_server::TestHarness;
use deadline_test_server::deadline_api::{fleets, queues, telemetry};
use insta_cmd::assert_cmd_snapshot;
use serde_json::json;

// --- fleet list ---

#[tokio::test]
async fn fleet_list_prints_fleet_ids_and_names() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    fleets::mock_list_fleets(&harness.server, "farm-abc", &[
        json!({"fleetId": "fleet-aaa", "displayName": "Fleet A"}),
    ]).await;

    assert_cmd_snapshot!(harness.cmd(&["fleet", "list"]));
}

#[tokio::test]
async fn fleet_list_no_farm_id_exits_with_error() {
    let harness = TestHarness::new().await;
    assert_cmd_snapshot!(harness.cmd(&["fleet", "list"]));
}

// --- fleet get (--fleet-id mode) ---

#[tokio::test]
async fn fleet_get_with_fleet_id_prints_details() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    fleets::mock_get_fleet(&harness.server, "farm-abc", json!({
        "fleetId": "fleet-aaa", "displayName": "My Fleet",
    })).await;

    assert_cmd_snapshot!(harness.cmd(&["fleet", "get", "--fleet-id", "fleet-aaa"]));
}

// --- fleet get (--queue-id mode) ---

#[tokio::test]
async fn fleet_get_with_queue_id_shows_associated_fleets() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();

    // Mock get_queue to return queue display name
    queues::mock_get_queue(&harness.server, "farm-abc", json!({
        "queueId": "queue-111",
        "displayName": "Render Queue",
    })).await;

    // Mock list_queue_fleet_associations
    fleets::mock_list_queue_fleet_associations(&harness.server, "farm-abc", "queue-111", &[
        json!({
            "queueId": "queue-111",
            "fleetId": "fleet-aaa",
            "status": "ACTIVE",
        }),
        json!({
            "queueId": "queue-111",
            "fleetId": "fleet-bbb",
            "status": "STOPPED",
        }),
    ]).await;

    // Mock get_fleet for each associated fleet
    fleets::mock_get_fleet(&harness.server, "farm-abc", json!({
        "fleetId": "fleet-aaa",
        "displayName": "Fleet Alpha",
        "status": "ACTIVE",
    })).await;
    fleets::mock_get_fleet(&harness.server, "farm-abc", json!({
        "fleetId": "fleet-bbb",
        "displayName": "Fleet Beta",
        "status": "ACTIVE",
    })).await;

    assert_cmd_snapshot!(harness.cmd(&["fleet", "get", "--queue-id", "queue-111"]));
}

#[tokio::test]
async fn fleet_get_both_fleet_id_and_queue_id_errors() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();

    assert_cmd_snapshot!(harness.cmd(&["fleet", "get", "--fleet-id", "fleet-aaa", "--queue-id", "queue-111"]));
}

#[tokio::test]
async fn fleet_get_no_fleet_id_no_queue_id_errors() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();

    assert_cmd_snapshot!(harness.cmd(&["fleet", "get"]));
}

// --- telemetry ---

#[tokio::test]
async fn fleet_list_sends_latency_telemetry() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    fleets::mock_list_fleets(&harness.server, "farm-abc", &[
        json!({"fleetId": "fleet-aaa", "displayName": "Fleet A"}),
    ]).await;
    telemetry::mock_telemetry_endpoint(&harness.server).await;

    harness.cli(&["fleet", "list"]).assert().success();
}

#[tokio::test]
async fn fleet_get_sends_latency_telemetry() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    fleets::mock_get_fleet(&harness.server, "farm-abc", json!({
        "fleetId": "fleet-aaa", "displayName": "My Fleet",
    })).await;
    telemetry::mock_telemetry_endpoint(&harness.server).await;

    harness.cli(&["fleet", "get", "--fleet-id", "fleet-aaa"]).assert().success();
}
