//! Level 2 tests for `suggest_resources_on_client_error` (§37 cases 48-53).
//!
//! These test that CLI commands include resource suggestions in error output
//! when API calls fail with AccessDenied or ResourceNotFound.

use deadline_test_server::deadline_api::{errors, farms, jobs, queues};
use deadline_test_server::TestHarness;
use insta_cmd::assert_cmd_snapshot;
use serde_json::json;

// AccessDeniedException on GetQueue with valid farm_id lists available queues
#[tokio::test]
async fn queue_get_access_denied_suggests_available_queues() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();

    errors::mock_get_queue_access_denied(&harness.server, "farm-abc", "queue-bad", "Access denied").await;
    queues::mock_list_queues(
        &harness.server, "farm-abc",
        &[json!({"queueId": "queue-111", "displayName": "Good Queue"})],
    ).await;

    assert_cmd_snapshot!(harness.cmd(&["queue", "get", "--queue-id", "queue-bad"]));
}

// ResourceNotFoundException on GetFarm lists available farms
#[tokio::test]
async fn farm_get_not_found_suggests_available_farms() {
    let harness = TestHarness::new().await;

    errors::mock_get_farm_not_found(&harness.server, "farm-bad", "Farm not found").await;
    farms::mock_list_farms(
        &harness.server,
        &[json!({"farmId": "farm-real", "displayName": "Real Farm"})],
    ).await;

    assert_cmd_snapshot!(harness.cmd(&["farm", "get", "--farm-id", "farm-bad"]));
}

// AccessDeniedException on GetJob falls back through chain
#[tokio::test]
async fn job_get_access_denied_suggests_jobs() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.queue_id", "queue-abc"]).assert().success();

    errors::mock_get_job_not_found(&harness.server, "farm-abc", "queue-abc", "job-bad", "Job not found").await;
    jobs::mock_list_jobs(
        &harness.server, "farm-abc", "queue-abc",
        &[json!({"jobId": "job-real", "name": "Real Job"})],
    ).await;

    assert_cmd_snapshot!(harness.cmd(&["job", "get", "--job-id", "job-bad"]));
}

// Listing resources also fails — hint about missing List permissions
#[tokio::test]
async fn farm_get_not_found_list_also_fails_shows_permission_hint() {
    let harness = TestHarness::new().await;

    errors::mock_get_farm_not_found(&harness.server, "farm-bad", "Farm not found").await;
    errors::mock_list_farms_access_denied(&harness.server, "No list permission").await;

    assert_cmd_snapshot!(harness.cmd(&["farm", "get", "--farm-id", "farm-bad"]));
}

// More than 10 resources shows "... and N more"
#[tokio::test]
async fn farm_get_not_found_more_than_10_farms_shows_and_more() {
    let harness = TestHarness::new().await;

    errors::mock_get_farm_not_found(&harness.server, "farm-bad", "Farm not found").await;

    let many_farms: Vec<serde_json::Value> = (0..15)
        .map(|i| json!({"farmId": format!("farm-{i:03}"), "displayName": format!("Farm {i}")}))
        .collect();
    farms::mock_list_farms(&harness.server, &many_farms).await;

    assert_cmd_snapshot!(harness.cmd(&["farm", "get", "--farm-id", "farm-bad"]));
}
