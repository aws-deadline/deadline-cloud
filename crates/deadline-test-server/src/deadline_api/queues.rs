use serde_json::{Value, json};
use wiremock::matchers::{header, method};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Mount a ListQueues response.
pub async fn mock_list_queues(server: &MockServer, queues: &[Value]) {
    Mock::given(method("POST"))
        .and(header("x-amz-target", "Deadline.ListQueues"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({
                "queues": queues,
                "nextToken": null
            })),
        )
        .mount(server)
        .await;
}

/// Mount a GetQueue response.
pub async fn mock_get_queue(server: &MockServer, queue: Value) {
    Mock::given(method("POST"))
        .and(header("x-amz-target", "Deadline.GetQueue"))
        .respond_with(ResponseTemplate::new(200).set_body_json(queue))
        .mount(server)
        .await;
}
