use serde_json::json;
use wiremock::matchers::{header, method};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Mount a throttling (429) response for a specific API action.
pub async fn mock_throttle(server: &MockServer, action: &str) {
    Mock::given(method("POST"))
        .and(header("x-amz-target", format!("Deadline.{action}")))
        .respond_with(
            ResponseTemplate::new(429).set_body_json(json!({
                "__type": "ThrottlingException",
                "message": "Rate exceeded"
            })),
        )
        .mount(server)
        .await;
}

/// Mount a ResourceNotFoundException for a specific API action.
pub async fn mock_resource_not_found(server: &MockServer, action: &str, message: &str) {
    Mock::given(method("POST"))
        .and(header("x-amz-target", format!("Deadline.{action}")))
        .respond_with(
            ResponseTemplate::new(404).set_body_json(json!({
                "__type": "ResourceNotFoundException",
                "message": message
            })),
        )
        .mount(server)
        .await;
}

/// Mount an AccessDeniedException for a specific API action.
pub async fn mock_access_denied(server: &MockServer, action: &str, message: &str) {
    Mock::given(method("POST"))
        .and(header("x-amz-target", format!("Deadline.{action}")))
        .respond_with(
            ResponseTemplate::new(403).set_body_json(json!({
                "__type": "AccessDeniedException",
                "message": message
            })),
        )
        .mount(server)
        .await;
}
