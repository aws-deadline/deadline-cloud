use wiremock::matchers::method;
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Mount a catch-all S3 HEAD response (404 = not found, triggers upload).
pub async fn mock_s3_head_not_found(server: &MockServer) {
    Mock::given(method("HEAD"))
        .respond_with(ResponseTemplate::new(404))
        .mount(server)
        .await;
}

/// Mount a catch-all S3 PUT response (200 = upload success).
pub async fn mock_s3_put_success(server: &MockServer) {
    Mock::given(method("PUT"))
        .respond_with(ResponseTemplate::new(200))
        .mount(server)
        .await;
}
