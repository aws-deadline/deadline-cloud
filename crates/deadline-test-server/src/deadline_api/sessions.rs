use serde_json::{Value, json};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

pub async fn mock_get_session(
    server: &MockServer,
    farm_id: &str,
    queue_id: &str,
    job_id: &str,
    session: Value,
) {
    let session_id = session["sessionId"].as_str().unwrap_or("session-mock");
    Mock::given(method("GET"))
        .and(path(format!(
            "/2023-10-12/farms/{farm_id}/queues/{queue_id}/jobs/{job_id}/sessions/{session_id}"
        )))
        .respond_with(ResponseTemplate::new(200).set_body_json(session))
        .mount(server)
        .await;
}

pub async fn mock_list_sessions(
    server: &MockServer,
    farm_id: &str,
    queue_id: &str,
    job_id: &str,
    sessions: &[Value],
) {
    Mock::given(method("GET"))
        .and(path(format!(
            "/2023-10-12/farms/{farm_id}/queues/{queue_id}/jobs/{job_id}/sessions"
        )))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({ "sessions": sessions })),
        )
        .mount(server)
        .await;
}

pub async fn mock_list_steps(
    server: &MockServer,
    farm_id: &str,
    queue_id: &str,
    job_id: &str,
    steps: &[Value],
) {
    Mock::given(method("GET"))
        .and(path(format!(
            "/2023-10-12/farms/{farm_id}/queues/{queue_id}/jobs/{job_id}/steps"
        )))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({ "steps": steps })),
        )
        .mount(server)
        .await;
}

pub async fn mock_list_tasks(
    server: &MockServer,
    farm_id: &str,
    queue_id: &str,
    job_id: &str,
    step_id: &str,
    tasks: &[Value],
) {
    Mock::given(method("GET"))
        .and(path(format!(
            "/2023-10-12/farms/{farm_id}/queues/{queue_id}/jobs/{job_id}/steps/{step_id}/tasks"
        )))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({ "tasks": tasks })),
        )
        .mount(server)
        .await;
}

/// Mount an UpdateTask response (PATCH). Used by `deadline job requeue-tasks`.
pub async fn mock_update_task(
    server: &MockServer,
    farm_id: &str,
    queue_id: &str,
    job_id: &str,
    step_id: &str,
    task_id: &str,
) {
    Mock::given(method("PATCH"))
        .and(path(format!(
            "/2023-10-12/farms/{farm_id}/queues/{queue_id}/jobs/{job_id}/steps/{step_id}/tasks/{task_id}"
        )))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
        .mount(server)
        .await;
}

pub async fn mock_list_session_actions(
    server: &MockServer,
    farm_id: &str,
    queue_id: &str,
    job_id: &str,
    session_id: &str,
    session_actions: &[Value],
) {
    Mock::given(method("GET"))
        .and(path(format!(
            "/2023-10-12/farms/{farm_id}/queues/{queue_id}/jobs/{job_id}/sessions/{session_id}/session-actions"
        )))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({ "sessionActions": session_actions })),
        )
        .mount(server)
        .await;
}
