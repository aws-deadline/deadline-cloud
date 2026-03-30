//! Level 2 tests for `deadline queue` subcommands — export-credentials and diagnostics.

use deadline_test_server::TestHarness;
use deadline_test_server::deadline_api::queue_resources;
use insta_cmd::assert_cmd_snapshot;
use serde_json::json;

// --- queue export-credentials ---

#[tokio::test]
async fn queue_export_credentials_user_mode_prints_credentials_json() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.queue_id", "queue-aaa"]).assert().success();
    // Use fractional seconds to verify RFC 3339 T separator is preserved
    queue_resources::mock_assume_queue_role_for_user(
        &harness.server,
        "farm-abc",
        "queue-aaa",
        json!({
            "credentials": {
                "accessKeyId": "ASIAQUEUEUSER",
                "secretAccessKey": "secretqueue",
                "sessionToken": "tokenqueue",
                "expiration": "2024-12-18T01:30:45.123Z"
            }
        }),
    )
    .await;

    assert_cmd_snapshot!(harness.cmd(&["queue", "export-credentials"]));
}

#[tokio::test]
async fn queue_export_credentials_read_mode_prints_credentials_json() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.queue_id", "queue-aaa"]).assert().success();
    queue_resources::mock_assume_queue_role_for_read(
        &harness.server,
        "farm-abc",
        "queue-aaa",
        json!({
            "credentials": {
                "accessKeyId": "ASIAQUEREAD",
                "secretAccessKey": "secretread",
                "sessionToken": "tokenread",
                "expiration": "2024-12-18T01:00:00Z"
            }
        }),
    )
    .await;

    assert_cmd_snapshot!(harness.cmd(&["queue", "export-credentials", "--mode", "READ"]));
}

#[tokio::test]
async fn queue_export_credentials_access_denied_prints_error() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.queue_id", "queue-aaa"]).assert().success();
    queue_resources::mock_assume_queue_role_for_user_error(
        &harness.server,
        "farm-abc",
        "queue-aaa",
        403,
        "AccessDeniedException",
        "User is not authorized to assume queue role",
    )
    .await;

    assert_cmd_snapshot!(harness.cmd(&["queue", "export-credentials"]));
}

// --- storage profile ---

#[tokio::test]
async fn queue_get_storage_profile_prints_details() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.queue_id", "queue-aaa"]).assert().success();
    queue_resources::mock_get_storage_profile_for_queue(
        &harness.server,
        "farm-abc",
        "queue-aaa",
        "sp-001",
        json!({
            "storageProfileId": "sp-001",
            "displayName": "Linux Profile",
            "osFamily": "LINUX",
            "fileSystemLocations": [
                {
                    "name": "Project",
                    "path": "/mnt/project",
                    "type": "LOCAL"
                }
            ]
        }),
    )
    .await;

    assert_cmd_snapshot!(harness.cmd(&[
        "queue", "get-storage-profile",
        "--storage-profile-id", "sp-001"
    ]));
}
