use serde_json::{Value, json};
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

pub async fn mock_list_queues(server: &MockServer, farm_id: &str, queues: &[Value]) {
    Mock::given(method("GET"))
        .and(path(format!("/2023-10-12/farms/{farm_id}/queues")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "queues": queues })))
        .mount(server)
        .await;
}

pub async fn mock_get_queue(server: &MockServer, farm_id: &str, queue: Value) {
    let queue_id = queue["queueId"].as_str().unwrap_or("queue-mock");
    Mock::given(method("GET"))
        .and(path(format!("/2023-10-12/farms/{farm_id}/queues/{queue_id}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(queue))
        .mount(server)
        .await;
}

/// Mount a `ListQueues` response that requires a specific principalId query param.
pub async fn mock_list_queues_with_principal_id(
    server: &MockServer,
    farm_id: &str,
    principal_id: &str,
    queues: &[Value],
) {
    Mock::given(method("GET"))
        .and(path(format!("/2023-10-12/farms/{farm_id}/queues")))
        .and(query_param("principalId", principal_id))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({ "queues": queues })),
        )
        .mount(server)
        .await;
}
