use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Mount a `CreateJob` response (POST).
pub async fn mock_create_job(server: &MockServer, farm_id: &str, queue_id: &str, job_id: &str) {
    Mock::given(method("POST"))
        .and(path(format!(
            "/2023-10-12/farms/{farm_id}/queues/{queue_id}/jobs"
        )))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobId": job_id,
        })))
        .mount(server)
        .await;
}

/// Mount a `CreateJob` error response.
pub async fn mock_create_job_error(
    server: &MockServer,
    farm_id: &str,
    queue_id: &str,
    status: u16,
    error_type: &str,
) {
    Mock::given(method("POST"))
        .and(path(format!(
            "/2023-10-12/farms/{farm_id}/queues/{queue_id}/jobs"
        )))
        .respond_with(ResponseTemplate::new(status).set_body_json(json!({
            "__type": error_type,
        })))
        .mount(server)
        .await;
}
