//! Level 2 tests for `deadline queue` subcommands — export-credentials.

use deadline_test_server::TestHarness;
use deadline_test_server::deadline_api::queue_resources;
use insta_cmd::assert_cmd_snapshot;
use serde_json::json;

// --- queue export-credentials ---

#[tokio::test]
async fn queue_export_credentials_user_mode_prints_credentials_json() {
    let harness = TestHarness::new().await;
    harness
        .cli(&["config", "set", "defaults.farm_id", "farm-abc"])
        .assert()
        .success();
    harness
        .cli(&["config", "set", "defaults.queue_id", "queue-aaa"])
        .assert()
        .success();
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
    harness
        .cli(&["config", "set", "defaults.farm_id", "farm-abc"])
        .assert()
        .success();
    harness
        .cli(&["config", "set", "defaults.queue_id", "queue-aaa"])
        .assert()
        .success();
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
    harness
        .cli(&["config", "set", "defaults.farm_id", "farm-abc"])
        .assert()
        .success();
    harness
        .cli(&["config", "set", "defaults.queue_id", "queue-aaa"])
        .assert()
        .success();
    queue_resources::mock_assume_queue_role_for_user_error(
        &harness.server,
        "farm-abc",
        "queue-aaa",
        403,
        "AccessDeniedException",
    )
    .await;

    assert_cmd_snapshot!(harness.cmd(&["queue", "export-credentials"]));
}

// --- queue export-credentials error paths ---
// These supplement the existing access_denied test with additional error
// scenarios from Python test_queue_export_credentials and L1 session.rs tests.

#[tokio::test]
async fn queue_export_credentials_auth_error_prints_error() {
    let harness = TestHarness::new().await;
    harness
        .cli(&["config", "set", "defaults.farm_id", "farm-abc"])
        .assert()
        .success();
    harness
        .cli(&["config", "set", "defaults.queue_id", "queue-aaa"])
        .assert()
        .success();
    queue_resources::mock_assume_queue_role_for_user_error(
        &harness.server,
        "farm-abc",
        "queue-aaa",
        403,
        "UnrecognizedClientException",
    )
    .await;

    assert_cmd_snapshot!(harness.cmd(&["queue", "export-credentials"]));
}

#[tokio::test]
async fn queue_export_credentials_throttling_prints_error() {
    let harness = TestHarness::new().await;
    harness
        .cli(&["config", "set", "defaults.farm_id", "farm-abc"])
        .assert()
        .success();
    harness
        .cli(&["config", "set", "defaults.queue_id", "queue-aaa"])
        .assert()
        .success();
    queue_resources::mock_assume_queue_role_for_user_error(
        &harness.server,
        "farm-abc",
        "queue-aaa",
        403,
        "ThrottlingException",
    )
    .await;

    assert_cmd_snapshot!(harness.cmd(&["queue", "export-credentials"]));
}

#[tokio::test]
async fn queue_export_credentials_internal_error_prints_error() {
    let harness = TestHarness::new().await;
    harness
        .cli(&["config", "set", "defaults.farm_id", "farm-abc"])
        .assert()
        .success();
    harness
        .cli(&["config", "set", "defaults.queue_id", "queue-aaa"])
        .assert()
        .success();
    queue_resources::mock_assume_queue_role_for_user_error(
        &harness.server,
        "farm-abc",
        "queue-aaa",
        500,
        "InternalServerException",
    )
    .await;

    assert_cmd_snapshot!(harness.cmd(&["queue", "export-credentials"]));
}

// Empty/missing credentials should produce an error, not null JSON output.
// Python raises KeyError when credential fields are missing.

#[tokio::test]
async fn queue_export_credentials_empty_credentials_exits_with_error() {
    let harness = TestHarness::new().await;
    harness
        .cli(&["config", "set", "defaults.farm_id", "farm-abc"])
        .assert()
        .success();
    harness
        .cli(&["config", "set", "defaults.queue_id", "queue-aaa"])
        .assert()
        .success();
    queue_resources::mock_assume_queue_role_for_user(
        &harness.server,
        "farm-abc",
        "queue-aaa",
        json!({
            "credentials": {}
        }),
    )
    .await;

    assert_cmd_snapshot!(harness.cmd(&["queue", "export-credentials"]));
}

#[tokio::test]
async fn queue_export_credentials_missing_credentials_key_exits_with_error() {
    let harness = TestHarness::new().await;
    harness
        .cli(&["config", "set", "defaults.farm_id", "farm-abc"])
        .assert()
        .success();
    harness
        .cli(&["config", "set", "defaults.queue_id", "queue-aaa"])
        .assert()
        .success();
    queue_resources::mock_assume_queue_role_for_user(
        &harness.server,
        "farm-abc",
        "queue-aaa",
        json!({}),
    )
    .await;

    assert_cmd_snapshot!(harness.cmd(&["queue", "export-credentials"]));
}

// ===========================================================================
// --output-format backward-compat flag
// ===========================================================================

/// `--output-format credentials_process` should be accepted (same behavior as without it).
#[tokio::test]
async fn export_credentials_output_format_flag() {
    let harness = TestHarness::new().await;
    harness
        .cli(&["config", "set", "defaults.farm_id", "farm-abc"])
        .assert()
        .success();
    harness
        .cli(&["config", "set", "defaults.queue_id", "queue-aaa"])
        .assert()
        .success();
    queue_resources::mock_assume_queue_role_for_user(
        &harness.server,
        "farm-abc",
        "queue-aaa",
        json!({
            "credentials": {
                "accessKeyId": "ASIAQUEUEUSER",
                "secretAccessKey": "secretqueue",
                "sessionToken": "tokenqueue",
                "expiration": "2024-12-18T01:30:45Z"
            }
        }),
    )
    .await;

    // The --output-format flag should be accepted without error
    harness
        .cli(&[
            "queue",
            "export-credentials",
            "--output-format",
            "credentials_process",
        ])
        .assert()
        .success();
}
