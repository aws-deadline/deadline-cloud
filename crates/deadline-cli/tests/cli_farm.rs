//! Level 2 tests for `deadline farm` subcommands.

use deadline_test_server::TestHarness;
use deadline_test_server::deadline_api::{farms, errors};
use insta_cmd::assert_cmd_snapshot;
use serde_json::json;

// --- farm list ---

#[tokio::test]
async fn farm_list_prints_farm_ids_and_names() {
    let harness = TestHarness::new().await;
    farms::mock_list_farms(
        &harness.server,
        &[
            json!({"farmId": "farm-aaa", "displayName": "Alpha Farm"}),
            json!({"farmId": "farm-bbb", "displayName": "Beta Farm"}),
        ],
    )
    .await;

    assert_cmd_snapshot!(harness.cmd(&["farm", "list"]));
}

#[tokio::test]
async fn farm_list_with_profile_option() {
    let harness = TestHarness::new().await;
    farms::mock_list_farms(&harness.server, &[]).await;

    assert_cmd_snapshot!(harness.cmd(&["farm", "list", "--profile", "my-profile"]));
}

#[tokio::test]
async fn farm_list_api_failure_prints_error() {
    let harness = TestHarness::new().await;
    errors::mock_list_farms_access_denied(&harness.server, "Access denied").await;

    assert_cmd_snapshot!(harness.cmd(&["farm", "list"]));
}

// --- farm get ---

#[tokio::test]
async fn farm_get_with_farm_id_prints_details() {
    let harness = TestHarness::new().await;
    farms::mock_get_farm(
        &harness.server,
        json!({
            "farmId": "farm-abc",
            "displayName": "My Farm",
            "kmsKeyArn": "arn:aws:kms:us-west-2:123456789012:key/abc"
        }),
    )
    .await;

    assert_cmd_snapshot!(harness.cmd(&["farm", "get", "--farm-id", "farm-abc"]));
}

#[tokio::test]
async fn farm_get_uses_default_farm_id_from_config() {
    let harness = TestHarness::new().await;
    harness
        .cli(&["config", "set", "defaults.farm_id", "farm-from-config"])
        .assert()
        .success();
    farms::mock_get_farm(
        &harness.server,
        json!({"farmId": "farm-from-config", "displayName": "Config Farm"}),
    )
    .await;

    assert_cmd_snapshot!(harness.cmd(&["farm", "get"]));
}

#[tokio::test]
async fn farm_get_no_farm_id_exits_with_error() {
    let harness = TestHarness::new().await;

    assert_cmd_snapshot!(harness.cmd(&["farm", "get"]));
}

#[tokio::test]
async fn farm_get_api_failure_prints_error() {
    let harness = TestHarness::new().await;
    errors::mock_get_farm_not_found(&harness.server, "farm-nonexistent", "Farm not found").await;

    assert_cmd_snapshot!(harness.cmd(&["farm", "get", "--farm-id", "farm-nonexistent"]));
}

// --- pagination ---

#[tokio::test]
async fn farm_list_paginated_concatenates_all_pages() {
    let harness = TestHarness::new().await;
    farms::mock_list_farms_paginated(
        &harness.server,
        &[json!({"farmId": "farm-page1", "displayName": "Page 1 Farm"})],
        &[json!({"farmId": "farm-page2", "displayName": "Page 2 Farm"})],
    )
    .await;

    assert_cmd_snapshot!(harness.cmd(&["farm", "list"]));
}

#[tokio::test]
async fn farm_list_empty_prints_empty_list() {
    let harness = TestHarness::new().await;
    farms::mock_list_farms(&harness.server, &[]).await;

    assert_cmd_snapshot!(harness.cmd(&["farm", "list"]));
}
