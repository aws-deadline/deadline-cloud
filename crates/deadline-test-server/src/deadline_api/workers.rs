use serde_json::{Value, json};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Mount a SearchWorkers response (POST).
/// Python CLI uses SearchWorkers, not ListWorkers.
pub async fn mock_search_workers(
    server: &MockServer,
    farm_id: &str,
    workers: &[Value],
    total_results: usize,
) {
    Mock::given(method("POST"))
        .and(path(format!("/2023-10-12/farms/{farm_id}/search/workers")))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({
                "workers": workers,
                "totalResults": total_results,
            })),
        )
        .mount(server)
        .await;
}

/// Mount a GetWorker response.
pub async fn mock_get_worker(server: &MockServer, farm_id: &str, fleet_id: &str, worker: Value) {
    let worker_id = worker["workerId"].as_str().unwrap_or("worker-mock");
    Mock::given(method("GET"))
        .and(path(format!(
            "/2023-10-12/farms/{farm_id}/fleets/{fleet_id}/workers/{worker_id}"
        )))
        .respond_with(ResponseTemplate::new(200).set_body_json(worker))
        .mount(server)
        .await;
}
