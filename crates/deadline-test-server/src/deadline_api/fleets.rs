use serde_json::{Value, json};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

pub async fn mock_list_fleets(server: &MockServer, farm_id: &str, fleets: &[Value]) {
    Mock::given(method("GET"))
        .and(path(format!("/2023-10-12/farms/{farm_id}/fleets")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "fleets": fleets })))
        .mount(server)
        .await;
}

pub async fn mock_get_fleet(server: &MockServer, farm_id: &str, fleet: Value) {
    let fleet_id = fleet["fleetId"].as_str().unwrap_or("fleet-mock");
    Mock::given(method("GET"))
        .and(path(format!("/2023-10-12/farms/{farm_id}/fleets/{fleet_id}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(fleet))
        .mount(server)
        .await;
}
