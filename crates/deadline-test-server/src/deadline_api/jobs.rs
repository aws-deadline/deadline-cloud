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

pub async fn mock_get_job(server: &MockServer, farm_id: &str, queue_id: &str, job: Value) {
    let job_id = job["jobId"].as_str().unwrap_or("job-mock");
    Mock::given(method("GET"))
        .and(path(format!("/2023-10-12/farms/{farm_id}/queues/{queue_id}/jobs/{job_id}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(job))
        .mount(server)
        .await;
}
