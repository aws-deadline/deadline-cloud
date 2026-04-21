use wiremock::matchers::{method, path, body_string_contains};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Mount a telemetry endpoint that accepts POST to /2023-10-12/telemetry.
/// Expects at least 1 call — use in tests that verify telemetry is sent.
pub async fn mock_telemetry_endpoint(server: &MockServer) {
    Mock::given(method("POST"))
        .and(path("/2023-10-12/telemetry"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1..)
        .mount(server)
        .await;
}

/// Mount a permissive telemetry endpoint that accepts any number of POSTs
/// (including zero). Use in tests that don't care about telemetry but need
/// the endpoint available so the background thread doesn't die.
pub async fn mock_telemetry_endpoint_permissive(server: &MockServer) {
    Mock::given(method("POST"))
        .and(path("/2023-10-12/telemetry"))
        .respond_with(ResponseTemplate::new(200))
        .mount(server)
        .await;
}

/// Mount a telemetry endpoint that expects at least N calls.
pub async fn mock_telemetry_endpoint_expect_at_least(server: &MockServer, min_calls: u64) {
    Mock::given(method("POST"))
        .and(path("/2023-10-12/telemetry"))
        .respond_with(ResponseTemplate::new(200))
        .expect(min_calls..)
        .mount(server)
        .await;
}

/// Mount a telemetry endpoint that expects a specific event type string
/// in the request body. Also mounts a generic catch-all (mounted first,
/// lower priority) to accept latency events and keep the telemetry thread alive.
pub async fn mock_telemetry_event_type(server: &MockServer, event_type: &str) {
    // Generic catch-all (mounted first = lower priority in wiremock).
    Mock::given(method("POST"))
        .and(path("/2023-10-12/telemetry"))
        .respond_with(ResponseTemplate::new(200))
        .mount(server)
        .await;
    // Specific matcher (mounted last = higher priority in wiremock).
    Mock::given(method("POST"))
        .and(path("/2023-10-12/telemetry"))
        .and(body_string_contains(event_type))
        .respond_with(ResponseTemplate::new(200))
        .expect(1..)
        .mount(server)
        .await;
}

/// Mount a telemetry endpoint that expects zero calls (for opt-out tests).
pub async fn mock_telemetry_endpoint_expect_none(server: &MockServer) {
    Mock::given(method("POST"))
        .and(path("/2023-10-12/telemetry"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(server)
        .await;
}
