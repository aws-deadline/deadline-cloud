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
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "sessions": sessions })))
        .mount(server)
        .await;
}

/// Mount a paginated `ListSessions` response (2 pages).
pub async fn mock_list_sessions_paginated(
    server: &MockServer,
    farm_id: &str,
    queue_id: &str,
    job_id: &str,
    page1: &[Value],
    page2: &[Value],
) {
    use wiremock::matchers::query_param;
    Mock::given(method("GET"))
        .and(path(format!(
            "/2023-10-12/farms/{farm_id}/queues/{queue_id}/jobs/{job_id}/sessions"
        )))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "sessions": page1,
            "nextToken": "sessions-page2"
        })))
        .up_to_n_times(1)
        .mount(server)
        .await;

    Mock::given(method("GET"))
        .and(path(format!(
            "/2023-10-12/farms/{farm_id}/queues/{queue_id}/jobs/{job_id}/sessions"
        )))
        .and(query_param("nextToken", "sessions-page2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "sessions": page2 })))
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
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "steps": steps })))
        .mount(server)
        .await;
}

/// Mount a paginated `ListSteps` response (2 pages).
pub async fn mock_list_steps_paginated(
    server: &MockServer,
    farm_id: &str,
    queue_id: &str,
    job_id: &str,
    page1: &[Value],
    page2: &[Value],
) {
    use wiremock::matchers::query_param;
    Mock::given(method("GET"))
        .and(path(format!(
            "/2023-10-12/farms/{farm_id}/queues/{queue_id}/jobs/{job_id}/steps"
        )))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "steps": page1,
            "nextToken": "steps-page2"
        })))
        .up_to_n_times(1)
        .mount(server)
        .await;

    Mock::given(method("GET"))
        .and(path(format!(
            "/2023-10-12/farms/{farm_id}/queues/{queue_id}/jobs/{job_id}/steps"
        )))
        .and(query_param("nextToken", "steps-page2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "steps": page2 })))
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
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "tasks": tasks })))
        .mount(server)
        .await;
}

/// Mount a paginated `ListTasks` response (2 pages).
pub async fn mock_list_tasks_paginated(
    server: &MockServer,
    farm_id: &str,
    queue_id: &str,
    job_id: &str,
    step_id: &str,
    page1: &[Value],
    page2: &[Value],
) {
    use wiremock::matchers::query_param;
    Mock::given(method("GET"))
        .and(path(format!(
            "/2023-10-12/farms/{farm_id}/queues/{queue_id}/jobs/{job_id}/steps/{step_id}/tasks"
        )))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "tasks": page1,
            "nextToken": "tasks-page2"
        })))
        .up_to_n_times(1)
        .mount(server)
        .await;

    Mock::given(method("GET"))
        .and(path(format!(
            "/2023-10-12/farms/{farm_id}/queues/{queue_id}/jobs/{job_id}/steps/{step_id}/tasks"
        )))
        .and(query_param("nextToken", "tasks-page2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "tasks": page2 })))
        .mount(server)
        .await;
}

/// Mount an `UpdateTask` response (PATCH). Used by `deadline job requeue-tasks`.
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

pub async fn mock_get_session_action(
    server: &MockServer,
    farm_id: &str,
    queue_id: &str,
    job_id: &str,
    session_action: Value,
) {
    let session_action_id = session_action["sessionActionId"]
        .as_str()
        .unwrap_or("sessionaction-mock-0");
    Mock::given(method("GET"))
        .and(path(format!(
            "/2023-10-12/farms/{farm_id}/queues/{queue_id}/jobs/{job_id}/session-actions/{session_action_id}"
        )))
        .respond_with(ResponseTemplate::new(200).set_body_json(session_action))
        .mount(server)
        .await;
}

pub async fn mock_get_session_action_error(
    server: &MockServer,
    farm_id: &str,
    queue_id: &str,
    job_id: &str,
    session_action_id: &str,
    status: u16,
    error_type: &str,
) {
    Mock::given(method("GET"))
        .and(path(format!(
            "/2023-10-12/farms/{farm_id}/queues/{queue_id}/jobs/{job_id}/session-actions/{session_action_id}"
        )))
        .respond_with(
            ResponseTemplate::new(status).set_body_json(json!({
                "__type": error_type,
            })),
        )
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
    use wiremock::matchers::query_param;
    Mock::given(method("GET"))
        .and(path(format!(
            "/2023-10-12/farms/{farm_id}/queues/{queue_id}/jobs/{job_id}/session-actions"
        )))
        .and(query_param("sessionId", session_id))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({ "sessionActions": session_actions })),
        )
        .mount(server)
        .await;
}

/// Mount a `BatchGetStep` response (POST). Returns steps and errors arrays.
pub async fn mock_batch_get_steps(server: &MockServer, steps: &[Value], errors: &[Value]) {
    Mock::given(method("POST"))
        .and(path("/2023-10-12/batch-get-step"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "steps": steps,
            "errors": errors,
        })))
        .mount(server)
        .await;
}

/// Mount a `BatchGetTask` response (POST). Returns tasks and errors arrays.
pub async fn mock_batch_get_tasks(server: &MockServer, tasks: &[Value], errors: &[Value]) {
    Mock::given(method("POST"))
        .and(path("/2023-10-12/batch-get-task"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "tasks": tasks,
            "errors": errors,
        })))
        .mount(server)
        .await;
}
