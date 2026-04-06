use serde_json::{Value, json};
use wiremock::matchers::{method, header};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Mount a CloudWatch GetLogEvents response.
/// CloudWatch uses JSON-RPC: POST / with X-Amz-Target header.
pub async fn mock_get_log_events(
    server: &MockServer,
    events: &[Value],
    next_forward_token: Option<&str>,
) {
    let body = json!({
        "events": events,
        "nextBackwardToken": "b/token",
        "nextForwardToken": next_forward_token.unwrap_or("f/token"),
    });

    Mock::given(method("POST"))
        .and(header("x-amz-target", "Logs_20140328.GetLogEvents"))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .mount(server)
        .await;
}

/// Mount a CloudWatch GetLogEvents ResourceNotFoundException.
pub async fn mock_get_log_events_not_found(server: &MockServer) {
    Mock::given(method("POST"))
        .and(header("x-amz-target", "Logs_20140328.GetLogEvents"))
        .respond_with(
            ResponseTemplate::new(400).set_body_json(json!({
                "__type": "ResourceNotFoundException",
                "message": "The specified log group does not exist."
            })),
        )
        .mount(server)
        .await;
}
