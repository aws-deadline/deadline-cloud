//! Level 2 tests for `suggest_resources_on_client_error`.
//!
//! These test that CLI commands include resource suggestions in error output
//! when API calls fail with AccessDenied or ResourceNotFound.

use deadline_test_server::deadline_api::{errors, farms, fleets, jobs, queues, queue_resources, workers};
use deadline_test_server::TestHarness;
use insta_cmd::assert_cmd_snapshot;
use serde_json::json;

// AccessDeniedException on GetQueue with valid farm_id lists available queues
#[tokio::test]
async fn queue_get_access_denied_suggests_available_queues() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();

    errors::mock_get_queue_access_denied(&harness.server, "farm-abc", "queue-bad").await;
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

    errors::mock_get_farm_not_found(&harness.server, "farm-bad").await;
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

    errors::mock_get_job_not_found(&harness.server, "farm-abc", "queue-abc", "job-bad").await;
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

    errors::mock_get_farm_not_found(&harness.server, "farm-bad").await;
    errors::mock_list_farms_access_denied(&harness.server).await;

    assert_cmd_snapshot!(harness.cmd(&["farm", "get", "--farm-id", "farm-bad"]));
}

// More than 10 resources shows "... and N more"
#[tokio::test]
async fn farm_get_not_found_more_than_10_farms_shows_and_more() {
    let harness = TestHarness::new().await;

    errors::mock_get_farm_not_found(&harness.server, "farm-bad").await;

    let many_farms: Vec<serde_json::Value> = (0..15)
        .map(|i| json!({"farmId": format!("farm-{i:03}"), "displayName": format!("Farm {i}")}))
        .collect();
    farms::mock_list_farms(&harness.server, &many_farms).await;

    assert_cmd_snapshot!(harness.cmd(&["farm", "get", "--farm-id", "farm-bad"]));
}

// ── suggest_resources dispatch by operation name ────────
// The suggestion chain must be determined by which API operation failed,
// not by which resource IDs happen to be available. When both jobs and
// queues are listable, a GetQueue error should suggest queues (not jobs).

// GetQueue error with both jobs and queues available → suggests queues
#[tokio::test]
async fn queue_get_error_suggests_queues_not_jobs() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();

    errors::mock_get_queue_access_denied(&harness.server, "farm-abc", "queue-bad").await;
    // Mount BOTH jobs and queues — greedy dispatch would pick jobs (wrong)
    jobs::mock_list_jobs(
        &harness.server, "farm-abc", "queue-bad",
        &[json!({"jobId": "job-111", "name": "Some Job"})],
    ).await;
    queues::mock_list_queues(
        &harness.server, "farm-abc",
        &[json!({"queueId": "queue-good", "displayName": "Good Queue"})],
    ).await;

    assert_cmd_snapshot!(harness.cmd(&["queue", "get", "--queue-id", "queue-bad"]));
}

// GetFleet error with both workers and fleets available → suggests fleets
#[tokio::test]
async fn fleet_get_error_suggests_fleets_not_workers() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();

    errors::mock_get_fleet_not_found(&harness.server, "farm-abc", "fleet-bad").await;
    // Mount BOTH workers and fleets — greedy dispatch would pick workers (wrong)
    workers::mock_search_workers(
        &harness.server, "farm-abc",
        &[json!({"workerId": "worker-111", "status": "RUNNING"})], 1,
    ).await;
    fleets::mock_list_fleets(
        &harness.server, "farm-abc",
        &[json!({"fleetId": "fleet-good", "displayName": "Good Fleet"})],
    ).await;

    assert_cmd_snapshot!(harness.cmd(&["fleet", "get", "--fleet-id", "fleet-bad"]));
}

// GetJob error with jobs, queues, and farms available → suggests jobs first
#[tokio::test]
async fn job_get_error_suggests_jobs_first() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.queue_id", "queue-abc"]).assert().success();

    errors::mock_get_job_not_found(&harness.server, "farm-abc", "queue-abc", "job-bad").await;
    jobs::mock_list_jobs(
        &harness.server, "farm-abc", "queue-abc",
        &[json!({"jobId": "job-real", "name": "Real Job"})],
    ).await;
    queues::mock_list_queues(
        &harness.server, "farm-abc",
        &[json!({"queueId": "queue-abc", "displayName": "My Queue"})],
    ).await;

    assert_cmd_snapshot!(harness.cmd(&["job", "get", "--job-id", "job-bad"]));
}

// ── AUDIT-056: storage profile suggestion chain ─────────
// GetStorageProfileForQueue error with storage profiles available → suggests profiles

#[tokio::test]
async fn queue_get_storage_profile_error_suggests_available_profiles() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.queue_id", "queue-abc"]).assert().success();

    errors::mock_get_storage_profile_not_found(
        &harness.server, "farm-abc", "queue-abc", "sp-bad",
    ).await;
    queue_resources::mock_list_storage_profiles_for_queue(
        &harness.server, "farm-abc", "queue-abc",
        &[json!({
            "storageProfileId": "sp-good-123",
            "displayName": "Linux Profile"
        })],
    ).await;

    assert_cmd_snapshot!(harness.cmd(&[
        "queue", "get-storage-profile",
        "--storage-profile-id", "sp-bad",
    ]));
}

// GetJob error with paginated list_jobs for suggestions — verifies all pages are shown
#[tokio::test]
async fn job_get_error_paginated_list_jobs_shows_all_suggestions() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.queue_id", "queue-abc"]).assert().success();

    errors::mock_get_job_not_found(&harness.server, "farm-abc", "queue-abc", "job-bad").await;
    jobs::mock_list_jobs_paginated(
        &harness.server, "farm-abc", "queue-abc",
        &[json!({"jobId": "job-page1", "name": "First Page Job"})],
        &[json!({"jobId": "job-page2", "name": "Second Page Job"})],
    ).await;

    assert_cmd_snapshot!(harness.cmd(&["job", "get", "--job-id", "job-bad"]));
}
