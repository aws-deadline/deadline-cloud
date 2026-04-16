//! Level 2 tests for telemetry integration.

use deadline_test_server::TestHarness;
use deadline_test_server::deadline_api::{queue_resources, telemetry};
use serde_json::json;

#[tokio::test]
async fn export_credentials_success_sends_telemetry() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.queue_id", "queue-aaa"]).assert().success();
    queue_resources::mock_assume_queue_role_for_user(
        &harness.server, "farm-abc", "queue-aaa",
        json!({"credentials": {"accessKeyId": "AK", "secretAccessKey": "SK", "sessionToken": "ST", "expiration": "2024-12-18T01:00:00Z"}}),
    ).await;
    telemetry::mock_telemetry_endpoint(&harness.server).await;

    harness.cli(&["queue", "export-credentials"]).assert().success();
    // wiremock verifies expect(1..) on drop
}

#[tokio::test]
async fn export_credentials_failure_sends_telemetry() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.queue_id", "queue-aaa"]).assert().success();
    queue_resources::mock_assume_queue_role_for_user_error(
        &harness.server, "farm-abc", "queue-aaa", 403, "AccessDeniedException",
    ).await;
    telemetry::mock_telemetry_endpoint(&harness.server).await;

    harness.cli(&["queue", "export-credentials"]).assert().failure();
}

#[tokio::test]
async fn export_credentials_telemetry_opted_out_no_post() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.queue_id", "queue-aaa"]).assert().success();
    queue_resources::mock_assume_queue_role_for_user(
        &harness.server, "farm-abc", "queue-aaa",
        json!({"credentials": {"accessKeyId": "AK", "secretAccessKey": "SK", "sessionToken": "ST", "expiration": "2024-12-18T01:00:00Z"}}),
    ).await;
    telemetry::mock_telemetry_endpoint_expect_none(&harness.server).await;

    let mut cmd = harness.cli(&["queue", "export-credentials"]);
    cmd.env("DEADLINE_CLOUD_TELEMETRY_OPT_OUT", "true");
    cmd.assert().success();
}
