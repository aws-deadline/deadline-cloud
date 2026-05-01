use serde_json::{Value, json};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Mount a ListJobs response (GET). Used by suggest_resources_on_client_error.
pub async fn mock_list_jobs(server: &MockServer, farm_id: &str, queue_id: &str, jobs: &[Value]) {
    Mock::given(method("GET"))
        .and(path(format!("/2023-10-12/farms/{farm_id}/queues/{queue_id}/jobs")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "jobs": jobs })))
        .mount(server)
        .await;
}

/// Mount a paginated ListJobs response (GET, 2 pages).
pub async fn mock_list_jobs_paginated(
    server: &MockServer,
    farm_id: &str,
    queue_id: &str,
    page1: &[Value],
    page2: &[Value],
) {
    use wiremock::matchers::query_param;
    Mock::given(method("GET"))
        .and(path(format!("/2023-10-12/farms/{farm_id}/queues/{queue_id}/jobs")))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({
                "jobs": page1,
                "nextToken": "jobs-page2"
            })),
        )
        .up_to_n_times(1)
        .mount(server)
        .await;

    Mock::given(method("GET"))
        .and(path(format!("/2023-10-12/farms/{farm_id}/queues/{queue_id}/jobs")))
        .and(query_param("nextToken", "jobs-page2"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({ "jobs": page2 })),
        )
        .mount(server)
        .await;
}

/// Mount a SearchJobs response (POST). Used by `deadline job list`.
pub async fn mock_search_jobs(
    server: &MockServer,
    farm_id: &str,
    jobs: &[Value],
    total_results: usize,
) {
    Mock::given(method("POST"))
        .and(path(format!("/2023-10-12/farms/{farm_id}/search/jobs")))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({
                "jobs": jobs,
                "totalResults": total_results,
            })),
        )
        .mount(server)
        .await;
}

/// Mount a paginated SearchJobs sequence for `list_jobs_by_filter_expression`.
///
/// Page 1 is served once (via `up_to_n_times(1)`) with `totalResults` > page1 len
/// to signal more pages. Page 2 is served for any subsequent request that contains
/// `GREATER_THAN_EQUAL_TO` in the body (the createdAt threshold filter).
///
/// `filter_marker` is a string that must appear in the request body to match
/// these mocks (e.g. `"ANY_EQUALS"` for active-jobs, `"ENDED_AT"` for ended-jobs).
/// This prevents the active-jobs pagination mock from matching the ended-jobs query.
pub async fn mock_search_jobs_paginated(
    server: &MockServer,
    farm_id: &str,
    filter_marker: &str,
    page1_jobs: &[Value],
    page1_total: usize,
    page2_jobs: &[Value],
) {
    use wiremock::matchers::body_string_contains;

    // Page 1: first request matching the filter marker
    Mock::given(method("POST"))
        .and(path(format!("/2023-10-12/farms/{farm_id}/search/jobs")))
        .and(body_string_contains(filter_marker))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({
                "jobs": page1_jobs,
                "totalResults": page1_total,
            })),
        )
        .up_to_n_times(1)
        .mount(server)
        .await;

    // Page 2: subsequent request with createdAt threshold
    Mock::given(method("POST"))
        .and(path(format!("/2023-10-12/farms/{farm_id}/search/jobs")))
        .and(body_string_contains(filter_marker))
        .and(body_string_contains("GREATER_THAN_EQUAL_TO"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({
                "jobs": page2_jobs,
                "totalResults": page2_jobs.len(),
            })),
        )
        .mount(server)
        .await;
}

pub async fn mock_get_job(server: &MockServer, farm_id: &str, queue_id: &str, job: Value) {
    let job_id = job["jobId"].as_str().unwrap_or("job-mock");
    Mock::given(method("GET"))
        .and(path(format!("/2023-10-12/farms/{farm_id}/queues/{queue_id}/jobs/{job_id}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(job))
        .mount(server)
        .await;
}

pub async fn mock_get_step(server: &MockServer, farm_id: &str, queue_id: &str, job_id: &str, step: Value) {
    let step_id = step["stepId"].as_str().unwrap_or("step-mock");
    Mock::given(method("GET"))
        .and(path(format!("/2023-10-12/farms/{farm_id}/queues/{queue_id}/jobs/{job_id}/steps/{step_id}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(step))
        .mount(server)
        .await;
}

pub async fn mock_get_task(server: &MockServer, farm_id: &str, queue_id: &str, job_id: &str, step_id: &str, task: Value) {
    let task_id = task["taskId"].as_str().unwrap_or("task-mock");
    Mock::given(method("GET"))
        .and(path(format!("/2023-10-12/farms/{farm_id}/queues/{queue_id}/jobs/{job_id}/steps/{step_id}/tasks/{task_id}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(task))
        .mount(server)
        .await;
}

/// Mount an UpdateJob response (PATCH). Used by `deadline job cancel`.
pub async fn mock_update_job(server: &MockServer, farm_id: &str, queue_id: &str, job_id: &str) {
    Mock::given(method("PATCH"))
        .and(path(format!("/2023-10-12/farms/{farm_id}/queues/{queue_id}/jobs/{job_id}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
        .mount(server)
        .await;
}
