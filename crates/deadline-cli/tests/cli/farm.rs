//! Level 2 tests for `deadline farm` subcommands.

use deadline_test_server::TestHarness;
use deadline_test_server::deadline_api::{farms, errors, telemetry};
use insta_cmd::assert_cmd_snapshot;
use serde_json::json;

// --- farm list ---

#[tokio::test]
async fn farm_list_prints_farm_ids_and_names() {
    let harness = TestHarness::new().await;
    farms::mock_list_farms(
        &harness.server,
        &[
            json!({
                "farmId": "farm-aaa",
                "displayName": "Alpha Farm",
                "createdAt": "2024-01-01T00:00:00Z",
                "createdBy": "arn:aws:sts::123456789012:user/test"
            }),
            json!({
                "farmId": "farm-bbb",
                "displayName": "Beta Farm",
                "createdAt": "2024-01-02T00:00:00Z",
                "createdBy": "arn:aws:sts::123456789012:user/test"
            }),
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
    errors::mock_list_farms_access_denied(&harness.server).await;

    assert_cmd_snapshot!(harness.cmd(&["farm", "list"]));
}

// --- farm get ---

#[tokio::test]
async fn farm_get_with_farm_id_prints_details() {
    let harness = TestHarness::new().await;
    // All fields that GetFarm returns, matching real API response shape
    farms::mock_get_farm(
        &harness.server,
        json!({
            "farmId": "farm-abc",
            "displayName": "My Farm",
            "description": "A test farm for rendering.",
            "kmsKeyArn": "arn:aws:kms:us-west-2:123456789012:key/abc",
            "createdAt": "2024-06-15T10:30:00Z",
            "createdBy": "arn:aws:sts::123456789012:assumed-role/Admin/user",
            "updatedAt": "2024-07-01T12:00:00Z",
            "updatedBy": "arn:aws:sts::123456789012:assumed-role/Admin/user",
            "costScaleFactor": 1.5
        }),
    )
    .await;

    assert_cmd_snapshot!(harness.cmd(&["farm", "get", "--farm-id", "farm-abc"]));
}

#[tokio::test]
async fn farm_get_minimal_fields_shows_defaults() {
    let harness = TestHarness::new().await;
    // Only required fields — costScaleFactor defaults to 1.0, optional fields omitted
    farms::mock_get_farm(
        &harness.server,
        json!({
            "farmId": "farm-minimal",
            "displayName": "Minimal Farm",
            "createdAt": "2024-01-01T00:00:00Z",
            "createdBy": "arn:aws:sts::123456789012:user/test"
        }),
    )
    .await;

    assert_cmd_snapshot!(harness.cmd(&["farm", "get", "--farm-id", "farm-minimal"]));
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
        json!({
            "farmId": "farm-from-config",
            "displayName": "Config Farm",
            "createdAt": "2024-01-01T00:00:00Z",
            "createdBy": "arn:aws:sts::123456789012:user/test"
        }),
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
    errors::mock_get_farm_not_found(&harness.server, "farm-nonexistent").await;
    assert_cmd_snapshot!(harness.cmd(&["farm", "get", "--farm-id", "farm-nonexistent"]));
}

// --- pagination ---

#[tokio::test]
async fn farm_list_paginated_concatenates_all_pages() {
    let harness = TestHarness::new().await;
    farms::mock_list_farms_paginated(
        &harness.server,
        &[json!({
            "farmId": "farm-page1",
            "displayName": "Page 1 Farm",
            "createdAt": "2024-01-01T00:00:00Z",
            "createdBy": "arn:aws:sts::123456789012:user/test"
        })],
        &[json!({
            "farmId": "farm-page2",
            "displayName": "Page 2 Farm",
            "createdAt": "2024-01-02T00:00:00Z",
            "createdBy": "arn:aws:sts::123456789012:user/test"
        })],
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

// --- telemetry ---
// Telemetry tests are separate from functional tests. Telemetry is best-effort
// fire-and-forget — the TelemetryClient silently swallows errors, so functional
// tests pass without telemetry mocks. These tests verify latency events are sent
// when the endpoint is reachable.

#[tokio::test]
async fn farm_list_sends_latency_telemetry() {
    let harness = TestHarness::new().await;
    farms::mock_list_farms(&harness.server, &[
        json!({"farmId": "farm-aaa", "displayName": "Farm A", "createdAt": "2024-01-01T00:00:00Z", "createdBy": "user"}),
    ]).await;
    telemetry::mock_telemetry_endpoint(&harness.server).await;

    harness.cli(&["farm", "list"]).assert().success();
}

#[tokio::test]
async fn farm_get_sends_latency_telemetry() {
    let harness = TestHarness::new().await;
    farms::mock_get_farm(&harness.server, json!({
        "farmId": "farm-abc", "displayName": "My Farm",
        "createdAt": "2024-01-01T00:00:00Z", "createdBy": "user",
    })).await;
    telemetry::mock_telemetry_endpoint(&harness.server).await;

    harness.cli(&["farm", "get", "--farm-id", "farm-abc"]).assert().success();
}
