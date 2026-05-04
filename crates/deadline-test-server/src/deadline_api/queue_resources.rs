use serde_json::{Value, json};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

pub async fn mock_assume_queue_role_for_user(
    server: &MockServer,
    farm_id: &str,
    queue_id: &str,
    response: Value,
) {
    Mock::given(method("GET"))
        .and(path(format!(
            "/2023-10-12/farms/{farm_id}/queues/{queue_id}/user-roles"
        )))
        .respond_with(ResponseTemplate::new(200).set_body_json(response))
        .mount(server)
        .await;
}

pub async fn mock_assume_queue_role_for_user_error(
    server: &MockServer,
    farm_id: &str,
    queue_id: &str,
    status: u16,
    error_type: &str,
) {
    Mock::given(method("GET"))
        .and(path(format!(
            "/2023-10-12/farms/{farm_id}/queues/{queue_id}/user-roles"
        )))
        .respond_with(ResponseTemplate::new(status).set_body_json(json!({
            "__type": error_type
        })))
        .mount(server)
        .await;
}

pub async fn mock_assume_queue_role_for_read(
    server: &MockServer,
    farm_id: &str,
    queue_id: &str,
    response: Value,
) {
    Mock::given(method("GET"))
        .and(path(format!(
            "/2023-10-12/farms/{farm_id}/queues/{queue_id}/read-roles"
        )))
        .respond_with(ResponseTemplate::new(200).set_body_json(response))
        .mount(server)
        .await;
}

pub async fn mock_get_storage_profile_for_queue(
    server: &MockServer,
    farm_id: &str,
    queue_id: &str,
    storage_profile_id: &str,
    response: Value,
) {
    Mock::given(method("GET"))
        .and(path(format!(
            "/2023-10-12/farms/{farm_id}/queues/{queue_id}/storage-profiles/{storage_profile_id}"
        )))
        .respond_with(ResponseTemplate::new(200).set_body_json(response))
        .mount(server)
        .await;
}

pub async fn mock_list_storage_profiles_for_queue(
    server: &MockServer,
    farm_id: &str,
    queue_id: &str,
    profiles: &[Value],
) {
    Mock::given(method("GET"))
        .and(path(format!(
            "/2023-10-12/farms/{farm_id}/queues/{queue_id}/storage-profiles"
        )))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({ "storageProfiles": profiles })),
        )
        .mount(server)
        .await;
}

pub async fn mock_list_queue_environments(
    server: &MockServer,
    farm_id: &str,
    queue_id: &str,
    environments: &[Value],
) {
    Mock::given(method("GET"))
        .and(path(format!(
            "/2023-10-12/farms/{farm_id}/queues/{queue_id}/environments"
        )))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "environments": environments,
        })))
        .mount(server)
        .await;
}

pub async fn mock_get_queue_environment(
    server: &MockServer,
    farm_id: &str,
    queue_id: &str,
    env_id: &str,
    response: Value,
) {
    Mock::given(method("GET"))
        .and(path(format!(
            "/2023-10-12/farms/{farm_id}/queues/{queue_id}/environments/{env_id}"
        )))
        .respond_with(ResponseTemplate::new(200).set_body_json(response))
        .mount(server)
        .await;
}
