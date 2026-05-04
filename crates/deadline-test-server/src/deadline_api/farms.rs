use serde_json::{Value, json};
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Mount a `ListFarms` response returning the given farms (single page).
pub async fn mock_list_farms(server: &MockServer, farms: &[Value]) {
    Mock::given(method("GET"))
        .and(path("/2023-10-12/farms"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "farms": farms })))
        .mount(server)
        .await;
}

/// Mount a paginated `ListFarms` response.
pub async fn mock_list_farms_paginated(server: &MockServer, page1: &[Value], page2: &[Value]) {
    Mock::given(method("GET"))
        .and(path("/2023-10-12/farms"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "farms": page1,
            "nextToken": "page2-token"
        })))
        .up_to_n_times(1)
        .mount(server)
        .await;

    Mock::given(method("GET"))
        .and(path("/2023-10-12/farms"))
        .and(query_param("nextToken", "page2-token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "farms": page2 })))
        .mount(server)
        .await;
}

/// Mount a `GetFarm` response with all fields.
pub async fn mock_get_farm(server: &MockServer, farm: Value) {
    let farm_id = farm["farmId"].as_str().unwrap_or("farm-mock");
    Mock::given(method("GET"))
        .and(path(format!("/2023-10-12/farms/{farm_id}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(farm))
        .mount(server)
        .await;
}

/// Mount a `ListFarms` response that requires a specific principalId query param.
pub async fn mock_list_farms_with_principal_id(
    server: &MockServer,
    principal_id: &str,
    farms: &[Value],
) {
    Mock::given(method("GET"))
        .and(path("/2023-10-12/farms"))
        .and(query_param("principalId", principal_id))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "farms": farms })))
        .mount(server)
        .await;
}
