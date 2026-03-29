//! Level 2 tests for `deadline fleet` subcommands.

use deadline_test_server::TestHarness;
use deadline_test_server::deadline_api::fleets;
use insta_cmd::assert_cmd_snapshot;
use serde_json::json;

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

#[tokio::test]
async fn fleet_get_with_fleet_id_prints_details() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    fleets::mock_get_fleet(&harness.server, "farm-abc", json!({
        "fleetId": "fleet-aaa", "displayName": "My Fleet",
    })).await;

    assert_cmd_snapshot!(harness.cmd(&["fleet", "get", "--fleet-id", "fleet-aaa"]));
}
