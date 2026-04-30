//! Level 2 tests for `deadline fleet` subcommands.

use deadline_test_server::TestHarness;
use deadline_test_server::deadline_api::{errors, farms, fleets, queues, telemetry};
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
        "fleetId": "fleet-aaa",
        "farmId": "farm-abc",
        "displayName": "My Fleet",
        "status": "ACTIVE",
        "workerCount": 5,
        "minWorkerCount": 1,
        "maxWorkerCount": 10,
        "configuration": {
            "customerManaged": {
                "mode": "NO_SCALING",
                "workerCapabilities": {
                    "vCpuCount": {"min": 1, "max": 4},
                    "memoryMiB": {"min": 1024, "max": 8192},
                    "osFamily": "LINUX",
                    "cpuArchitectureType": "x86_64"
                }
            }
        },
        "roleArn": "arn:aws:iam::123456789012:role/FleetRole",
        "autoScalingStatus": "STEADY",
        "targetWorkerCount": 5,
        "createdAt": "2024-06-15T10:30:00Z",
        "createdBy": "arn:aws:sts::123456789012:assumed-role/Admin/user",
        "updatedAt": "2024-07-01T12:00:00Z",
        "updatedBy": "arn:aws:sts::123456789012:assumed-role/Admin/user",
        "description": "A fleet for rendering jobs."
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
        "farmId": "farm-abc",
        "queueId": "queue-111",
        "displayName": "Render Queue",
        "status": "ACTIVE",
        "defaultBudgetAction": "NONE",
        "createdAt": "2024-01-01T00:00:00Z",
        "createdBy": "arn:aws:sts::123456789012:user/test"
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
        "farmId": "farm-abc",
        "displayName": "Fleet Alpha",
        "status": "ACTIVE",
        "workerCount": 3,
        "minWorkerCount": 0,
        "maxWorkerCount": 10,
        "roleArn": "arn:aws:iam::123456789012:role/FleetRole",
        "createdAt": "2024-06-15T10:30:00Z",
        "createdBy": "arn:aws:sts::123456789012:assumed-role/Admin/user"
    })).await;
    fleets::mock_get_fleet(&harness.server, "farm-abc", json!({
        "fleetId": "fleet-bbb",
        "farmId": "farm-abc",
        "displayName": "Fleet Beta",
        "status": "ACTIVE",
        "workerCount": 0,
        "minWorkerCount": 0,
        "maxWorkerCount": 5,
        "roleArn": "arn:aws:iam::123456789012:role/FleetRole",
        "createdAt": "2024-06-20T08:00:00Z",
        "createdBy": "arn:aws:sts::123456789012:assumed-role/Admin/user"
    })).await;

    assert_cmd_snapshot!(harness.cmd(&["fleet", "get", "--queue-id", "queue-111"]));
}

#[tokio::test]
async fn fleet_get_queue_id_mode_queue_not_found_suggests_queues() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();

    // GetQueue returns access denied
    errors::mock_get_queue_access_denied(&harness.server, "farm-abc", "queue-bad").await;

    // Suggestion chain: try listing queues
    queues::mock_list_queues(&harness.server, "farm-abc", &[
        json!({"queueId": "queue-111", "displayName": "Good Queue", "createdAt": "2024-01-01T00:00:00Z", "createdBy": "user"}),
    ]).await;

    assert_cmd_snapshot!(harness.cmd(&["fleet", "get", "--queue-id", "queue-bad"]));
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
