use wiremock::matchers::{method, path_regex};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Mount a telemetry endpoint that accepts POST to /2023-10-12/telemetry.
pub async fn mock_telemetry_endpoint(server: &MockServer) {
    Mock::given(method("POST"))
        .and(path_regex(".*/2023-10-12/telemetry"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1..)
        .mount(server)
        .await;
}

/// Mount a telemetry endpoint that expects zero calls (for opt-out tests).
pub async fn mock_telemetry_endpoint_expect_none(server: &MockServer) {
    Mock::given(method("POST"))
        .and(path_regex(".*/2023-10-12/telemetry"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(server)
        .await;
}
