//! Level 2 tests for `deadline auth` subcommands.

use deadline_test_server::TestHarness;
use deadline_test_server::deadline_api::{farms, sts};
use insta_cmd::assert_cmd_snapshot;
use serde_json::json;

// --- auth status (verbose) ---

// Authenticated with API available — shows all four fields
#[tokio::test]
async fn auth_status_authenticated_api_available() {
    let harness = TestHarness::new().await;
    sts::mock_get_caller_identity(&harness.server).await;
    farms::mock_list_farms(&harness.server, &[json!({"farmId": "farm-abc", "displayName": "F"})]).await;

    assert_cmd_snapshot!(harness.cmd(&["auth", "status"]));
}

// STS fails — shows CONFIGURATION_ERROR
#[tokio::test]
async fn auth_status_configuration_error() {
    let harness = TestHarness::new().await;
    sts::mock_get_caller_identity_failure(&harness.server).await;

    assert_cmd_snapshot!(harness.cmd(&["auth", "status"]));
}

// --profile option uses specified profile name
#[tokio::test]
async fn auth_status_with_profile_option() {
    let harness = TestHarness::new().await;
    sts::mock_get_caller_identity(&harness.server).await;
    farms::mock_list_farms(&harness.server, &[]).await;

    assert_cmd_snapshot!(harness.cmd(&["auth", "status", "--profile", "custom-profile"]));
}

// API unavailable (no ListFarms mock)
#[tokio::test]
async fn auth_status_api_unavailable() {
    let harness = TestHarness::new().await;
    sts::mock_get_caller_identity(&harness.server).await;

    assert_cmd_snapshot!(harness.cmd(&["auth", "status"]));
}

// --- auth status (JSON) ---

#[tokio::test]
async fn auth_status_json_authenticated() {
    let harness = TestHarness::new().await;
    sts::mock_get_caller_identity(&harness.server).await;
    farms::mock_list_farms(&harness.server, &[]).await;

    assert_cmd_snapshot!(harness.cmd(&["auth", "status", "--output", "json"]));
}

#[tokio::test]
async fn auth_status_json_configuration_error() {
    let harness = TestHarness::new().await;
    sts::mock_get_caller_identity_failure(&harness.server).await;

    assert_cmd_snapshot!(harness.cmd(&["auth", "status", "--output", "json"]));
}

#[tokio::test]
async fn auth_status_json_api_unavailable() {
    let harness = TestHarness::new().await;
    sts::mock_get_caller_identity(&harness.server).await;

    assert_cmd_snapshot!(harness.cmd(&["auth", "status", "--output", "json"]));
}
