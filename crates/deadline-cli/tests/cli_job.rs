//! Level 2 tests for `deadline job` subcommands.

use deadline_test_server::TestHarness;
use deadline_test_server::deadline_api::{errors, jobs, queues, telemetry};
use insta_cmd::assert_cmd_snapshot;
use serde_json::json;

fn mock_jobs() -> Vec<serde_json::Value> {
    vec![
        json!({
            "jobId": "job-aaf4cdf8aae242f58fb84c5bb19f199b",
            "name": "CLI Job",
            "taskRunStatus": "RUNNING",
            "lifecycleStatus": "SUCCEEDED",
            "createdBy": "b801f3c0-c071-70bc-b869-6804bc732408",
            "createdAt": "2023-01-27T07:34:41Z",
            "startedAt": "2023-01-27T07:37:53Z",
            "endedAt": "2023-01-27T07:39:17Z",
            "priority": 50,
        }),
        json!({
            "jobId": "job-0d239749fa05435f90263b3a8be54144",
            "name": "CLI Job",
            "taskRunStatus": "COMPLETED",
            "lifecycleStatus": "SUCCEEDED",
            "createdBy": "b801f3c0-c071-70bc-b869-6804bc732408",
            "createdAt": "2023-01-27T07:24:22Z",
            "startedAt": "2023-01-27T07:27:06Z",
            "endedAt": "2023-01-27T07:29:51Z",
            "priority": 50,
        }),
    ]
}

#[tokio::test]
async fn job_list_prints_jobs_with_count() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.queue_id", "queue-abc"]).assert().success();
    jobs::mock_search_jobs(&harness.server, "farm-abc", &mock_jobs(), 12).await;

    assert_cmd_snapshot!(harness.cmd(&["job", "list"]));
}

#[tokio::test]
async fn job_list_with_page_size_and_offset() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.queue_id", "queue-abc"]).assert().success();
    jobs::mock_search_jobs(&harness.server, "farm-abc", &mock_jobs()[..1], 12).await;

    assert_cmd_snapshot!(harness.cmd(&["job", "list", "--page-size", "1", "--item-offset", "3"]));
}

#[tokio::test]
async fn job_get_prints_details() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.queue_id", "queue-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.job_id", "job-aaa"]).assert().success();
    jobs::mock_get_job(&harness.server, "farm-abc", "queue-abc", json!({
        "jobId": "job-aaa", "name": "Render Job", "lifecycleStatus": "CREATE_COMPLETE",
    })).await;

    assert_cmd_snapshot!(harness.cmd(&["job", "get"]));
}

#[tokio::test]
async fn job_get_no_job_id_exits_with_error() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.queue_id", "queue-abc"]).assert().success();

    assert_cmd_snapshot!(harness.cmd(&["job", "get"]));
}

// --- behavior gap fixes ---

#[tokio::test]
async fn job_list_api_failure_prints_error_with_suggestions() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.queue_id", "queue-abc"]).assert().success();
    errors::mock_search_jobs_access_denied(&harness.server, "farm-abc", "Access denied").await;
    queues::mock_list_queues(
        &harness.server, "farm-abc",
        &[json!({"queueId": "queue-111", "displayName": "Good Queue"})],
    ).await;

    assert_cmd_snapshot!(harness.cmd(&["job", "list"]));
}

#[tokio::test]
async fn job_get_prints_estimated_time_remaining() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.queue_id", "queue-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.job_id", "job-aaa"]).assert().success();
    jobs::mock_get_job(&harness.server, "farm-abc", "queue-abc", json!({
        "jobId": "job-aaa", "name": "Render Job", "lifecycleStatus": "CREATE_COMPLETE",
        "taskRunStatusCounts": {"SUCCEEDED": 5, "RUNNING": 2, "PENDING": 3},
    })).await;

    assert_cmd_snapshot!(harness.cmd(&["job", "get"]));
}

// --- telemetry ---
// Telemetry tests are separate from functional tests. Telemetry is best-effort
// fire-and-forget — the TelemetryClient silently swallows errors, so functional
// tests pass without telemetry mocks. These tests verify latency events are sent
// when the endpoint is reachable.

#[tokio::test]
async fn job_list_sends_latency_telemetry() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.queue_id", "queue-abc"]).assert().success();
    jobs::mock_search_jobs(&harness.server, "farm-abc", &mock_jobs(), 12).await;
    telemetry::mock_telemetry_endpoint(&harness.server).await;

    harness.cli(&["job", "list"]).assert().success();
}

#[tokio::test]
async fn job_get_sends_latency_telemetry() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.queue_id", "queue-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.job_id", "job-aaa"]).assert().success();
    jobs::mock_get_job(&harness.server, "farm-abc", "queue-abc", json!({
        "jobId": "job-aaa", "name": "Render Job", "lifecycleStatus": "CREATE_COMPLETE",
    })).await;
    telemetry::mock_telemetry_endpoint(&harness.server).await;

    harness.cli(&["job", "get"]).assert().success();
}

// ===========================================================================
// #12b: deadline job search (§44 cases 6-9)
// ===========================================================================

// §44.6: job search with --filter-expressions JSON
#[tokio::test]
async fn job_search_with_filter_expressions() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.queue_id", "queue-abc"]).assert().success();
    jobs::mock_search_jobs(&harness.server, "farm-abc", &mock_jobs()[..1], 1).await;

    assert_cmd_snapshot!(harness.cmd(&[
        "job", "search",
        "--filter-expressions", r#"{"filters":[{"searchTermFilter":{"searchTerm":"render","matchType":"CONTAINS"}}],"operator":"AND"}"#,
    ]));
}

// §44.7: job search with file:// filter
#[tokio::test]
async fn job_search_with_file_filter() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.queue_id", "queue-abc"]).assert().success();
    jobs::mock_search_jobs(&harness.server, "farm-abc", &mock_jobs()[..1], 1).await;

    // Write filter to a temp file in the harness config dir
    let filter_path = harness.config_dir.path().join("filter.json");
    std::fs::write(&filter_path, r#"{"filters":[{"searchTermFilter":{"searchTerm":"render","matchType":"CONTAINS"}}],"operator":"AND"}"#).unwrap();

    assert_cmd_snapshot!(harness.cmd(&[
        "job", "search",
        "--filter-expressions", &format!("file://{}", filter_path.display()),
    ]));
}

// §44.8: job search with --sort-expressions
#[tokio::test]
async fn job_search_with_sort_expressions() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.queue_id", "queue-abc"]).assert().success();
    jobs::mock_search_jobs(&harness.server, "farm-abc", &mock_jobs(), 2).await;

    assert_cmd_snapshot!(harness.cmd(&[
        "job", "search",
        "--sort-expressions", r#"[{"fieldSort":{"name":"CREATED_AT","sortOrder":"ASCENDING"}}]"#,
    ]));
}

// §44.9: job search with pagination
#[tokio::test]
async fn job_search_with_pagination() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.queue_id", "queue-abc"]).assert().success();
    jobs::mock_search_jobs(&harness.server, "farm-abc", &mock_jobs()[..1], 10).await;

    assert_cmd_snapshot!(harness.cmd(&[
        "job", "search",
        "--page-size", "1",
        "--item-offset", "5",
    ]));
}

// ===========================================================================
// #15b: deadline job get [SEARCH_TERM] (§44 cases 1-5 extensions)
// ===========================================================================

// job get with search term that is a job ID → treated as --job-id
#[tokio::test]
async fn job_get_search_term_is_job_id() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.queue_id", "queue-abc"]).assert().success();
    jobs::mock_get_job(&harness.server, "farm-abc", "queue-abc", json!({
        "jobId": "job-aaf4cdf8aae242f58fb84c5bb19f199b",
        "name": "Render Job",
        "lifecycleStatus": "CREATE_COMPLETE",
    })).await;

    assert_cmd_snapshot!(harness.cmd(&["job", "get", "job-aaf4cdf8aae242f58fb84c5bb19f199b"]));
}

// job get with search term → single match shows details
#[tokio::test]
async fn job_get_search_term_single_match_shows_details() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.queue_id", "queue-abc"]).assert().success();

    // Search returns 1 result
    jobs::mock_search_jobs(&harness.server, "farm-abc", &[json!({
        "jobId": "job-aaf4cdf8aae242f58fb84c5bb19f199b",
        "name": "Render Job",
        "taskRunStatus": "SUCCEEDED",
        "taskRunStatusCounts": {"SUCCEEDED": 10},
        "createdAt": "2023-01-27T07:34:41Z",
    })], 1).await;

    // Then get_job for the details
    jobs::mock_get_job(&harness.server, "farm-abc", "queue-abc", json!({
        "jobId": "job-aaf4cdf8aae242f58fb84c5bb19f199b",
        "name": "Render Job",
        "lifecycleStatus": "CREATE_COMPLETE",
    })).await;

    assert_cmd_snapshot!(harness.cmd(&["job", "get", "render"]));
}

// job get with search term → multiple matches shows summary
#[tokio::test]
async fn job_get_search_term_multiple_matches_shows_summary() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.queue_id", "queue-abc"]).assert().success();

    jobs::mock_search_jobs(&harness.server, "farm-abc", &[
        json!({
            "jobId": "job-aaf4cdf8aae242f58fb84c5bb19f199b",
            "name": "Render Job 1",
            "taskRunStatus": "SUCCEEDED",
            "taskRunStatusCounts": {"SUCCEEDED": 10},
            "createdAt": "2023-01-27T07:34:41Z",
        }),
        json!({
            "jobId": "job-0d239749fa05435f90263b3a8be54144",
            "name": "Render Job 2",
            "taskRunStatus": "RUNNING",
            "taskRunStatusCounts": {"RUNNING": 5, "PENDING": 3},
            "createdAt": "2023-01-28T10:00:00Z",
        }),
    ], 2).await;

    // Redact local timezone in timestamps (e.g. "2023-01-27 00:34:41 -0700" → "[LOCAL_TIMESTAMP]")
    // to prevent snapshot flakiness across timezone/DST changes.
    let mut settings = insta::Settings::clone_current();
    settings.add_filter(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} [+-]\d{4}", "[LOCAL_TIMESTAMP]");
    let _guard = settings.bind_to_scope();

    assert_cmd_snapshot!(harness.cmd(&["job", "get", "render"]));
}

// job get with search term → no matches
#[tokio::test]
async fn job_get_search_term_no_matches() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.queue_id", "queue-abc"]).assert().success();

    jobs::mock_search_jobs(&harness.server, "farm-abc", &[], 0).await;

    assert_cmd_snapshot!(harness.cmd(&["job", "get", "nonexistent"]));
}

// job get search term → search API fails
#[tokio::test]
async fn job_get_search_term_api_failure() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.queue_id", "queue-abc"]).assert().success();
    errors::mock_search_jobs_access_denied(&harness.server, "farm-abc", "Access denied").await;

    assert_cmd_snapshot!(harness.cmd(&["job", "get", "render"]));
}

// job get with both --job-id and search term → --job-id takes precedence
#[tokio::test]
async fn job_get_job_id_flag_takes_precedence_over_search_term() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.queue_id", "queue-abc"]).assert().success();
    jobs::mock_get_job(&harness.server, "farm-abc", "queue-abc", json!({
        "jobId": "job-aaf4cdf8aae242f58fb84c5bb19f199b",
        "name": "Specific Job",
        "lifecycleStatus": "CREATE_COMPLETE",
    })).await;

    // --job-id should win; search term "render" should be ignored
    assert_cmd_snapshot!(harness.cmd(&[
        "job", "get", "render",
        "--job-id", "job-aaf4cdf8aae242f58fb84c5bb19f199b",
    ]));
}
