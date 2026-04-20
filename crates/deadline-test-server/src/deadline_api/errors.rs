use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Mount an AccessDeniedException for ListFarms.
pub async fn mock_list_farms_access_denied(server: &MockServer) {
    Mock::given(method("GET"))
        .and(path("/2023-10-12/farms"))
        .respond_with(
            ResponseTemplate::new(403).set_body_json(json!({
                "__type": "AccessDeniedException"
            })),
        )
        .mount(server)
        .await;
}

/// Mount a ResourceNotFoundException for GetFarm.
pub async fn mock_get_farm_not_found(server: &MockServer, farm_id: &str) {
    Mock::given(method("GET"))
        .and(path(format!("/2023-10-12/farms/{farm_id}")))
        .respond_with(
            ResponseTemplate::new(404).set_body_json(json!({
                "__type": "ResourceNotFoundException"
            })),
        )
        .mount(server)
        .await;
}

/// Mount an AccessDeniedException for GetQueue.
pub async fn mock_get_queue_access_denied(
    server: &MockServer,
    farm_id: &str,
    queue_id: &str,
) {
    Mock::given(method("GET"))
        .and(path(format!(
            "/2023-10-12/farms/{farm_id}/queues/{queue_id}"
        )))
        .respond_with(
            ResponseTemplate::new(403).set_body_json(json!({
                "__type": "AccessDeniedException"
            })),
        )
        .mount(server)
        .await;
}

/// Mount a ResourceNotFoundException for GetJob.
pub async fn mock_get_job_not_found(
    server: &MockServer,
    farm_id: &str,
    queue_id: &str,
    job_id: &str,
) {
    Mock::given(method("GET"))
        .and(path(format!(
            "/2023-10-12/farms/{farm_id}/queues/{queue_id}/jobs/{job_id}"
        )))
        .respond_with(
            ResponseTemplate::new(404).set_body_json(json!({
                "__type": "ResourceNotFoundException"
            })),
        )
        .mount(server)
        .await;
}

/// Mount an AccessDeniedException for SearchWorkers.
pub async fn mock_search_workers_access_denied(server: &MockServer, farm_id: &str) {
    Mock::given(method("POST"))
        .and(path(format!(
            "/2023-10-12/farms/{farm_id}/search/workers"
        )))
        .respond_with(
            ResponseTemplate::new(403).set_body_json(json!({
                "__type": "AccessDeniedException"
            })),
        )
        .mount(server)
        .await;
}

/// Mount a ResourceNotFoundException for GetWorker.
pub async fn mock_get_worker_not_found(
    server: &MockServer,
    farm_id: &str,
    fleet_id: &str,
    worker_id: &str,
) {
    Mock::given(method("GET"))
        .and(path(format!(
            "/2023-10-12/farms/{farm_id}/fleets/{fleet_id}/workers/{worker_id}"
        )))
        .respond_with(
            ResponseTemplate::new(404).set_body_json(json!({
                "__type": "ResourceNotFoundException"
            })),
        )
        .mount(server)
        .await;
}

/// Mount an AccessDeniedException for SearchJobs.
pub async fn mock_search_jobs_access_denied(server: &MockServer, farm_id: &str) {
    Mock::given(method("POST"))
        .and(path(format!("/2023-10-12/farms/{farm_id}/search/jobs")))
        .respond_with(
            ResponseTemplate::new(403).set_body_json(json!({
                "__type": "AccessDeniedException"
            })),
        )
        .mount(server)
        .await;
}

/// Mount a ResourceNotFoundException for GetFleet.
pub async fn mock_get_fleet_not_found(
    server: &MockServer,
    farm_id: &str,
    fleet_id: &str,
) {
    Mock::given(method("GET"))
        .and(path(format!(
            "/2023-10-12/farms/{farm_id}/fleets/{fleet_id}"
        )))
        .respond_with(
            ResponseTemplate::new(404).set_body_json(json!({
                "__type": "ResourceNotFoundException"
            })),
        )
        .mount(server)
        .await;
}

/// Mount a ResourceNotFoundException for GetStorageProfileForQueue.
pub async fn mock_get_storage_profile_not_found(
    server: &MockServer,
    farm_id: &str,
    queue_id: &str,
    storage_profile_id: &str,
) {
    Mock::given(method("GET"))
        .and(path(format!(
            "/2023-10-12/farms/{farm_id}/queues/{queue_id}/storage-profiles/{storage_profile_id}"
        )))
        .respond_with(
            ResponseTemplate::new(404).set_body_json(json!({
                "__type": "ResourceNotFoundException"
            })),
        )
        .mount(server)
        .await;
}
