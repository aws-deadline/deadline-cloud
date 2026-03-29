use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Mount an AccessDeniedException for ListFarms.
pub async fn mock_list_farms_access_denied(server: &MockServer, message: &str) {
    Mock::given(method("GET"))
        .and(path("/2023-10-12/farms"))
        .respond_with(
            ResponseTemplate::new(403).set_body_json(json!({
                "__type": "AccessDeniedException",
                "message": message
            })),
        )
        .mount(server)
        .await;
}

/// Mount a ResourceNotFoundException for GetFarm.
pub async fn mock_get_farm_not_found(server: &MockServer, farm_id: &str, message: &str) {
    Mock::given(method("GET"))
        .and(path(format!("/2023-10-12/farms/{farm_id}")))
        .respond_with(
            ResponseTemplate::new(404).set_body_json(json!({
                "__type": "ResourceNotFoundException",
                "message": message
            })),
        )
        .mount(server)
        .await;
}
