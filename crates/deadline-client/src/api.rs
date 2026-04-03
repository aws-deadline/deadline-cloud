use crate::{auth, raw_response::ResponseBodyCapture, session};
use deadline_common::telemetry::{TelemetryClient, with_telemetry_latency_async};
use deadline_config::ini::IniConfig;
use deadline_models::errors::DeadlineError;
use serde_json::Value;
use std::future::Future;

/// Format an AWS SDK error to include the error code and message.
fn format_sdk_error<E: std::fmt::Display + aws_sdk_deadline::error::ProvideErrorMetadata>(
    err: &aws_sdk_deadline::error::SdkError<E>,
) -> String {
    match err {
        aws_sdk_deadline::error::SdkError::ServiceError(e) => {
            let inner = e.err();
            let code = aws_sdk_deadline::error::ProvideErrorMetadata::code(inner)
                .unwrap_or("Unknown");
            let msg = aws_sdk_deadline::error::ProvideErrorMetadata::message(inner)
                .unwrap_or("No message");
            format!("{code}: {msg}")
        }
        other => format!("{other}"),
    }
}

fn sdk_err<E: std::fmt::Display + aws_sdk_deadline::error::ProvideErrorMetadata>(
    e: aws_sdk_deadline::error::SdkError<E>,
) -> DeadlineError {
    DeadlineError::OperationError(format_sdk_error(&e))
}

fn capture_err(e: serde_json::Error) -> DeadlineError {
    DeadlineError::OperationError(e.to_string())
}

// ---------------------------------------------------------------------------
// Paginated list helper
// ---------------------------------------------------------------------------

/// Generic paginated list using ResponseBodyCapture + manual nextToken loop.
/// `send_page` is called for each page with an optional nextToken.
/// `items_key` is the JSON key containing the items array (e.g. "farms").
async fn paginated_list<F, Fut>(
    items_key: &str,
    send_page: F,
) -> Result<Value, DeadlineError>
where
    F: Fn(Option<String>) -> Fut,
    Fut: Future<Output = Result<Value, DeadlineError>>,
{
    let mut all_items = Vec::new();
    let mut next_token: Option<String> = None;
    loop {
        let page = send_page(next_token.take()).await?;
        if let Some(items) = page[items_key].as_array() {
            all_items.extend(items.iter().cloned());
        }
        match page.get("nextToken").and_then(|t| t.as_str()) {
            Some(t) => next_token = Some(t.to_string()),
            None => break,
        }
    }
    Ok(serde_json::json!({items_key: all_items}))
}

/// Helper: send a single ResponseBodyCapture request and return parsed JSON.
async fn capture_send<F, R, E>(build: F) -> Result<Value, DeadlineError>
where
    F: FnOnce(ResponseBodyCapture) -> R,
    R: Future<Output = Result<(), aws_sdk_deadline::error::SdkError<E>>>,
    E: std::fmt::Display + aws_sdk_deadline::error::ProvideErrorMetadata,
{
    let capture = ResponseBodyCapture::new();
    build(capture.clone()).await.map_err(sdk_err)?;
    capture.json().map_err(capture_err)
}

// ---------------------------------------------------------------------------
// Farm
// ---------------------------------------------------------------------------

pub async fn list_farms(config: Option<&IniConfig>, telemetry: Option<&TelemetryClient>) -> Result<Value, DeadlineError> {
    with_telemetry_latency_async("list_farms", config, telemetry, || async {
        let client = session::deadline_client(config).await;
        let (user_id, _) = auth::get_user_and_identity_store_id(config);
        paginated_list("farms", |token| {
            let client = client.clone();
            let user_id = user_id.clone();
            async move {
                capture_send(|cap| {
                    let mut req = client.list_farms();
                    if let Some(ref uid) = user_id { req = req.principal_id(uid.as_str()); }
                    if let Some(t) = token { req = req.next_token(t); }
                    async move { req.customize().interceptor(cap).send().await.map(|_| ()) }
                }).await
            }
        }).await
    }).await
}

pub async fn get_farm(farm_id: &str, config: Option<&IniConfig>, telemetry: Option<&TelemetryClient>) -> Result<Value, DeadlineError> {
    with_telemetry_latency_async("get_farm", config, telemetry, || async {
        let client = session::deadline_client(config).await;
        capture_send(|cap| async move {
            client.get_farm().farm_id(farm_id)
                .customize().interceptor(cap).send().await.map(|_| ())
        }).await
    }).await
}

// ---------------------------------------------------------------------------
// Queue
// ---------------------------------------------------------------------------

pub async fn list_queues(farm_id: &str, config: Option<&IniConfig>, telemetry: Option<&TelemetryClient>) -> Result<Value, DeadlineError> {
    with_telemetry_latency_async("list_queues", config, telemetry, || async {
        let client = session::deadline_client(config).await;
        let (user_id, _) = auth::get_user_and_identity_store_id(config);
        let farm_id = farm_id.to_string();
        paginated_list("queues", |token| {
            let client = client.clone();
            let user_id = user_id.clone();
            let farm_id = farm_id.clone();
            async move {
                capture_send(|cap| {
                    let mut req = client.list_queues().farm_id(&farm_id);
                    if let Some(ref uid) = user_id { req = req.principal_id(uid.as_str()); }
                    if let Some(t) = token { req = req.next_token(t); }
                    async move { req.customize().interceptor(cap).send().await.map(|_| ()) }
                }).await
            }
        }).await
    }).await
}

pub async fn get_queue(farm_id: &str, queue_id: &str, config: Option<&IniConfig>, telemetry: Option<&TelemetryClient>) -> Result<Value, DeadlineError> {
    with_telemetry_latency_async("get_queue", config, telemetry, || async {
        let client = session::deadline_client(config).await;
        capture_send(|cap| async move {
            client.get_queue().farm_id(farm_id).queue_id(queue_id)
                .customize().interceptor(cap).send().await.map(|_| ())
        }).await
    }).await
}

// ---------------------------------------------------------------------------
// Fleet
// ---------------------------------------------------------------------------

pub async fn list_fleets(farm_id: &str, config: Option<&IniConfig>, telemetry: Option<&TelemetryClient>) -> Result<Value, DeadlineError> {
    with_telemetry_latency_async("list_fleets", config, telemetry, || async {
        let client = session::deadline_client(config).await;
        let (user_id, _) = auth::get_user_and_identity_store_id(config);
        let farm_id = farm_id.to_string();
        paginated_list("fleets", |token| {
            let client = client.clone();
            let user_id = user_id.clone();
            let farm_id = farm_id.clone();
            async move {
                capture_send(|cap| {
                    let mut req = client.list_fleets().farm_id(&farm_id);
                    if let Some(ref uid) = user_id { req = req.principal_id(uid.as_str()); }
                    if let Some(t) = token { req = req.next_token(t); }
                    async move { req.customize().interceptor(cap).send().await.map(|_| ()) }
                }).await
            }
        }).await
    }).await
}

pub async fn get_fleet(farm_id: &str, fleet_id: &str, config: Option<&IniConfig>, telemetry: Option<&TelemetryClient>) -> Result<Value, DeadlineError> {
    with_telemetry_latency_async("get_fleet", config, telemetry, || async {
        let client = session::deadline_client(config).await;
        capture_send(|cap| async move {
            client.get_fleet().farm_id(farm_id).fleet_id(fleet_id)
                .customize().interceptor(cap).send().await.map(|_| ())
        }).await
    }).await
}

// ---------------------------------------------------------------------------
// Job
// ---------------------------------------------------------------------------

pub async fn list_jobs(farm_id: &str, queue_id: &str, config: Option<&IniConfig>, telemetry: Option<&TelemetryClient>) -> Result<Value, DeadlineError> {
    with_telemetry_latency_async("list_jobs", config, telemetry, || async {
        let client = session::deadline_client(config).await;
        let (user_id, _) = auth::get_user_and_identity_store_id(config);
        let farm_id = farm_id.to_string();
        let queue_id = queue_id.to_string();
        paginated_list("jobs", |token| {
            let client = client.clone();
            let user_id = user_id.clone();
            let farm_id = farm_id.clone();
            let queue_id = queue_id.clone();
            async move {
                capture_send(|cap| {
                    let mut req = client.list_jobs().farm_id(&farm_id).queue_id(&queue_id);
                    if let Some(ref uid) = user_id { req = req.principal_id(uid.as_str()); }
                    if let Some(t) = token { req = req.next_token(t); }
                    async move { req.customize().interceptor(cap).send().await.map(|_| ()) }
                }).await
            }
        }).await
    }).await
}

pub async fn search_jobs(
    farm_id: &str,
    queue_ids: &[&str],
    item_offset: i32,
    page_size: i32,
    config: Option<&IniConfig>,
    telemetry: Option<&TelemetryClient>,
) -> Result<Value, DeadlineError> {
    with_telemetry_latency_async("search_jobs", config, telemetry, || async {
        let client = session::deadline_client(config).await;
        capture_send(|cap| async move {
            client
                .search_jobs()
                .farm_id(farm_id)
                .set_queue_ids(Some(queue_ids.iter().map(|s| s.to_string()).collect()))
                .item_offset(item_offset)
                .page_size(page_size)
                .sort_expressions(
                    aws_sdk_deadline::types::SearchSortExpression::FieldSort(
                        aws_sdk_deadline::types::FieldSortExpression::builder()
                            .name("CREATED_AT")
                            .sort_order(aws_sdk_deadline::types::SortOrder::Descending)
                            .build()
                            .unwrap(),
                    ),
                )
                .customize()
                .interceptor(cap)
                .send()
                .await
                .map(|_| ())
        }).await
    }).await
}

pub async fn get_job(farm_id: &str, queue_id: &str, job_id: &str, config: Option<&IniConfig>, telemetry: Option<&TelemetryClient>) -> Result<Value, DeadlineError> {
    with_telemetry_latency_async("get_job", config, telemetry, || async {
        let client = session::deadline_client(config).await;
        capture_send(|cap| async move {
            client.get_job().farm_id(farm_id).queue_id(queue_id).job_id(job_id)
                .customize().interceptor(cap).send().await.map(|_| ())
        }).await
    }).await
}

// ---------------------------------------------------------------------------
// Worker
// ---------------------------------------------------------------------------

pub async fn search_workers(
    farm_id: &str,
    fleet_ids: &[&str],
    item_offset: i32,
    page_size: i32,
    config: Option<&IniConfig>,
    telemetry: Option<&TelemetryClient>,
) -> Result<Value, DeadlineError> {
    with_telemetry_latency_async("search_workers", config, telemetry, || async {
        let client = session::deadline_client(config).await;
        capture_send(|cap| async move {
            client
                .search_workers()
                .farm_id(farm_id)
                .set_fleet_ids(Some(fleet_ids.iter().map(|s| s.to_string()).collect()))
                .item_offset(item_offset)
                .page_size(page_size)
                .customize()
                .interceptor(cap)
                .send()
                .await
                .map(|_| ())
        }).await
    }).await
}

pub async fn get_worker(
    farm_id: &str,
    fleet_id: &str,
    worker_id: &str,
    config: Option<&IniConfig>,
    telemetry: Option<&TelemetryClient>,
) -> Result<Value, DeadlineError> {
    with_telemetry_latency_async("get_worker", config, telemetry, || async {
        let client = session::deadline_client(config).await;
        capture_send(|cap| async move {
            client.get_worker().farm_id(farm_id).fleet_id(fleet_id).worker_id(worker_id)
                .customize().interceptor(cap).send().await.map(|_| ())
        }).await
    }).await
}

// ---------------------------------------------------------------------------
// Session / Step / Task
// ---------------------------------------------------------------------------

pub async fn get_session(
    farm_id: &str,
    queue_id: &str,
    job_id: &str,
    session_id: &str,
    config: Option<&IniConfig>,
    telemetry: Option<&TelemetryClient>,
) -> Result<Value, DeadlineError> {
    with_telemetry_latency_async("get_session", config, telemetry, || async {
        let client = session::deadline_client(config).await;
        capture_send(|cap| async move {
            client.get_session().farm_id(farm_id).queue_id(queue_id).job_id(job_id).session_id(session_id)
                .customize().interceptor(cap).send().await.map(|_| ())
        }).await
    }).await
}

pub async fn list_sessions(
    farm_id: &str,
    queue_id: &str,
    job_id: &str,
    config: Option<&IniConfig>,
    telemetry: Option<&TelemetryClient>,
) -> Result<Value, DeadlineError> {
    with_telemetry_latency_async("list_sessions", config, telemetry, || async {
        let client = session::deadline_client(config).await;
        let farm_id = farm_id.to_string();
        let queue_id = queue_id.to_string();
        let job_id = job_id.to_string();
        paginated_list("sessions", |token| {
            let client = client.clone();
            let farm_id = farm_id.clone();
            let queue_id = queue_id.clone();
            let job_id = job_id.clone();
            async move {
                capture_send(|cap| {
                    let mut req = client.list_sessions().farm_id(&farm_id).queue_id(&queue_id).job_id(&job_id);
                    if let Some(t) = token { req = req.next_token(t); }
                    async move { req.customize().interceptor(cap).send().await.map(|_| ()) }
                }).await
            }
        }).await
    }).await
}

pub async fn list_steps(
    farm_id: &str,
    queue_id: &str,
    job_id: &str,
    config: Option<&IniConfig>,
    telemetry: Option<&TelemetryClient>,
) -> Result<Value, DeadlineError> {
    with_telemetry_latency_async("list_steps", config, telemetry, || async {
        let client = session::deadline_client(config).await;
        let farm_id = farm_id.to_string();
        let queue_id = queue_id.to_string();
        let job_id = job_id.to_string();
        paginated_list("steps", |token| {
            let client = client.clone();
            let farm_id = farm_id.clone();
            let queue_id = queue_id.clone();
            let job_id = job_id.clone();
            async move {
                capture_send(|cap| {
                    let mut req = client.list_steps().farm_id(&farm_id).queue_id(&queue_id).job_id(&job_id);
                    if let Some(t) = token { req = req.next_token(t); }
                    async move { req.customize().interceptor(cap).send().await.map(|_| ()) }
                }).await
            }
        }).await
    }).await
}

pub async fn list_tasks(
    farm_id: &str,
    queue_id: &str,
    job_id: &str,
    step_id: &str,
    config: Option<&IniConfig>,
    telemetry: Option<&TelemetryClient>,
) -> Result<Value, DeadlineError> {
    with_telemetry_latency_async("list_tasks", config, telemetry, || async {
        let client = session::deadline_client(config).await;
        let farm_id = farm_id.to_string();
        let queue_id = queue_id.to_string();
        let job_id = job_id.to_string();
        let step_id = step_id.to_string();
        paginated_list("tasks", |token| {
            let client = client.clone();
            let farm_id = farm_id.clone();
            let queue_id = queue_id.clone();
            let job_id = job_id.clone();
            let step_id = step_id.clone();
            async move {
                capture_send(|cap| {
                    let mut req = client.list_tasks().farm_id(&farm_id).queue_id(&queue_id).job_id(&job_id).step_id(&step_id);
                    if let Some(t) = token { req = req.next_token(t); }
                    async move { req.customize().interceptor(cap).send().await.map(|_| ()) }
                }).await
            }
        }).await
    }).await
}

// ---------------------------------------------------------------------------
// Queue credentials
// ---------------------------------------------------------------------------

pub async fn assume_queue_role_for_user(
    farm_id: &str,
    queue_id: &str,
    config: Option<&IniConfig>,
    telemetry: Option<&TelemetryClient>,
) -> Result<Value, DeadlineError> {
    with_telemetry_latency_async("assume_queue_role_for_user", config, telemetry, || async {
        let client = session::deadline_client(config).await;
        capture_send(|cap| async move {
            client.assume_queue_role_for_user().farm_id(farm_id).queue_id(queue_id)
                .customize().interceptor(cap).send().await.map(|_| ())
        }).await
    }).await
}

pub async fn assume_queue_role_for_read(
    farm_id: &str,
    queue_id: &str,
    config: Option<&IniConfig>,
    telemetry: Option<&TelemetryClient>,
) -> Result<Value, DeadlineError> {
    with_telemetry_latency_async("assume_queue_role_for_read", config, telemetry, || async {
        let client = session::deadline_client(config).await;
        capture_send(|cap| async move {
            client.assume_queue_role_for_read().farm_id(farm_id).queue_id(queue_id)
                .customize().interceptor(cap).send().await.map(|_| ())
        }).await
    }).await
}

// ---------------------------------------------------------------------------
// Storage profile
// ---------------------------------------------------------------------------

pub async fn get_storage_profile_for_queue(
    farm_id: &str,
    queue_id: &str,
    storage_profile_id: &str,
    config: Option<&IniConfig>,
    telemetry: Option<&TelemetryClient>,
) -> Result<Value, DeadlineError> {
    with_telemetry_latency_async("get_storage_profile_for_queue", config, telemetry, || async {
        let client = session::deadline_client(config).await;
        capture_send(|cap| async move {
            client.get_storage_profile_for_queue()
                .farm_id(farm_id).queue_id(queue_id).storage_profile_id(storage_profile_id)
                .customize().interceptor(cap).send().await.map(|_| ())
        }).await
    }).await
}
