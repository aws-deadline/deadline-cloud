//! Level 2 tests for `deadline farm` subcommands.

use deadline_test_server::TestHarness;
use deadline_test_server::deadline_api::{farms, errors};
use predicates::prelude::*;
use serde_json::json;

// --- farm list ---

// farm list with farms prints farmId and displayName
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

    harness
        .cli(&["farm", "list"])
        .assert()
        .success()
        .stdout(predicate::str::contains("farm-aaa"))
        .stdout(predicate::str::contains("Alpha Farm"))
        .stdout(predicate::str::contains("farm-bbb"))
        .stdout(predicate::str::contains("Beta Farm"));
}

// farm list with --profile option
#[tokio::test]
async fn farm_list_with_profile_option() {
    let harness = TestHarness::new().await;
    farms::mock_list_farms(&harness.server, &[]).await;

    harness
        .cli(&["farm", "list", "--profile", "my-profile"])
        .assert()
        .success();
}

// farm list API failure prints error and exits 1
#[tokio::test]
async fn farm_list_api_failure_prints_error() {
    let harness = TestHarness::new().await;
    errors::mock_list_farms_access_denied(&harness.server, "Access denied").await;

    harness
        .cli(&["farm", "list"])
        .assert()
        .code(1)
        .stdout(predicate::str::contains("Failed to get Farms"));
}

// --- farm get ---

// farm get with --farm-id prints farm details
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

    harness
        .cli(&["farm", "get", "--farm-id", "farm-abc"])
        .assert()
        .success()
        .stdout(predicate::str::contains("farm-abc"))
        .stdout(predicate::str::contains("My Farm"));
}

// farm get with default farm ID from config
#[tokio::test]
async fn farm_get_uses_default_farm_id_from_config() {
    let harness = TestHarness::new().await;
    // Use config set to write the value in the correct format
    harness
        .cli(&["config", "set", "defaults.farm_id", "farm-from-config"])
        .assert()
        .success();
    farms::mock_get_farm(
        &harness.server,
        json!({"farmId": "farm-from-config", "displayName": "Config Farm"}),
    )
    .await;

    harness
        .cli(&["farm", "get"])
        .assert()
        .success()
        .stdout(predicate::str::contains("farm-from-config"));
}

// farm get with no farm ID available exits with error
#[tokio::test]
async fn farm_get_no_farm_id_exits_with_error() {
    let harness = TestHarness::new().await;

    harness
        .cli(&["farm", "get"])
        .assert()
        .code(1)
        .stdout(predicate::str::contains("farm"));
}

// farm get API failure prints error and exits 1
#[tokio::test]
async fn farm_get_api_failure_prints_error() {
    let harness = TestHarness::new().await;
    errors::mock_get_farm_not_found(&harness.server, "farm-nonexistent", "Farm not found").await;

    harness
        .cli(&["farm", "get", "--farm-id", "farm-nonexistent"])
        .assert()
        .code(1)
        .stdout(predicate::str::contains("Failed to get Farm"));
}

// --- pagination ---

// farm list with paginated response concatenates all pages
#[tokio::test]
async fn farm_list_paginated_concatenates_all_pages() {
    let harness = TestHarness::new().await;
    farms::mock_list_farms_paginated(
        &harness.server,
        &[json!({"farmId": "farm-page1", "displayName": "Page 1 Farm"})],
        &[json!({"farmId": "farm-page2", "displayName": "Page 2 Farm"})],
    )
    .await;

    harness
        .cli(&["farm", "list"])
        .assert()
        .success()
        .stdout(predicate::str::contains("farm-page1"))
        .stdout(predicate::str::contains("Page 1 Farm"))
        .stdout(predicate::str::contains("farm-page2"))
        .stdout(predicate::str::contains("Page 2 Farm"));
}

// farm list with empty response prints empty output
#[tokio::test]
async fn farm_list_empty_prints_empty_list() {
    let harness = TestHarness::new().await;
    farms::mock_list_farms(&harness.server, &[]).await;

    harness
        .cli(&["farm", "list"])
        .assert()
        .success();
}
