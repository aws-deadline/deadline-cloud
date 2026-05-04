use serde_json::{Value, json};
use wiremock::matchers::{method, path, query_param};
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
        .and(path(format!(
            "/2023-10-12/farms/{farm_id}/fleets/{fleet_id}"
        )))
        .respond_with(ResponseTemplate::new(200).set_body_json(fleet))
        .mount(server)
        .await;
}

/// Mock `ListQueueFleetAssociations`.
/// API: GET /2023-10-12/farms/{farmId}/queue-fleet-associations?queueId={queueId}
pub async fn mock_list_queue_fleet_associations(
    server: &MockServer,
    farm_id: &str,
    queue_id: &str,
    associations: &[Value],
) {
    Mock::given(method("GET"))
        .and(path(format!(
            "/2023-10-12/farms/{farm_id}/queue-fleet-associations"
        )))
        .and(query_param("queueId", queue_id))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "queueFleetAssociations": associations,
        })))
        .mount(server)
        .await;
}

/// Mount an `AssumeFleetRoleForRead` response.
/// API: GET /2023-10-12/farms/{farmId}/fleets/{fleetId}/read-roles
/// Response: { "credentials": { "accessKeyId", "secretAccessKey", "sessionToken", "expiration" } }
pub async fn mock_assume_fleet_role_for_read(
    server: &MockServer,
    farm_id: &str,
    fleet_id: &str,
    response: Value,
) {
    Mock::given(method("GET"))
        .and(path(format!(
            "/2023-10-12/farms/{farm_id}/fleets/{fleet_id}/read-roles"
        )))
        .respond_with(ResponseTemplate::new(200).set_body_json(response))
        .mount(server)
        .await;
}

/// Mount an error response for `AssumeFleetRoleForRead`.
pub async fn mock_assume_fleet_role_for_read_error(
    server: &MockServer,
    farm_id: &str,
    fleet_id: &str,
    status: u16,
    error_type: &str,
) {
    Mock::given(method("GET"))
        .and(path(format!(
            "/2023-10-12/farms/{farm_id}/fleets/{fleet_id}/read-roles"
        )))
        .respond_with(ResponseTemplate::new(status).set_body_json(json!({
            "__type": error_type
        })))
        .mount(server)
        .await;
}

/// Mount a `ListFleets` response that requires a specific principalId query param.
pub async fn mock_list_fleets_with_principal_id(
    server: &MockServer,
    farm_id: &str,
    principal_id: &str,
    fleets: &[Value],
) {
    Mock::given(method("GET"))
        .and(path(format!("/2023-10-12/farms/{farm_id}/fleets")))
        .and(query_param("principalId", principal_id))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "fleets": fleets })))
        .mount(server)
        .await;
}
