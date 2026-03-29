use crate::{auth, raw_response::ResponseBodyCapture, session};
use deadline_config::ini::IniConfig;
use deadline_models::errors::DeadlineError;
use serde_json::Value;

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
// Farm
// ---------------------------------------------------------------------------

pub async fn list_farms(config: Option<&IniConfig>) -> Result<Value, DeadlineError> {
    let client = session::deadline_client(config).await;
    let (user_id, _) = auth::get_user_and_identity_store_id(config);
    let mut all_items = Vec::new();
    let mut next_token: Option<String> = None;
    loop {
        let capture = ResponseBodyCapture::new();
        let mut req = client.list_farms();
        if let Some(ref uid) = user_id { req = req.principal_id(uid.as_str()); }
        if let Some(t) = next_token.take() { req = req.next_token(t); }
        req.customize().interceptor(capture.clone()).send().await.map_err(sdk_err)?;
        let page = capture.json().map_err(capture_err)?;
        if let Some(items) = page["farms"].as_array() {
            all_items.extend(items.iter().cloned());
        }
        match page.get("nextToken").and_then(|t| t.as_str()) {
            Some(t) => next_token = Some(t.to_string()),
            None => break,
        }
    }
    Ok(serde_json::json!({"farms": all_items}))
}

pub async fn get_farm(farm_id: &str, config: Option<&IniConfig>) -> Result<Value, DeadlineError> {
    let client = session::deadline_client(config).await;
    let capture = ResponseBodyCapture::new();
    client.get_farm().farm_id(farm_id)
        .customize().interceptor(capture.clone()).send().await.map_err(sdk_err)?;
    capture.json().map_err(capture_err)
}

// ---------------------------------------------------------------------------
// Queue
// ---------------------------------------------------------------------------

pub async fn list_queues(farm_id: &str, config: Option<&IniConfig>) -> Result<Value, DeadlineError> {
    let client = session::deadline_client(config).await;
    let (user_id, _) = auth::get_user_and_identity_store_id(config);
    let mut all_items = Vec::new();
    let mut next_token: Option<String> = None;
    loop {
        let capture = ResponseBodyCapture::new();
        let mut req = client.list_queues().farm_id(farm_id);
        if let Some(ref uid) = user_id { req = req.principal_id(uid.as_str()); }
        if let Some(t) = next_token.take() { req = req.next_token(t); }
        req.customize().interceptor(capture.clone()).send().await.map_err(sdk_err)?;
        let page = capture.json().map_err(capture_err)?;
        if let Some(items) = page["queues"].as_array() {
            all_items.extend(items.iter().cloned());
        }
        match page.get("nextToken").and_then(|t| t.as_str()) {
            Some(t) => next_token = Some(t.to_string()),
            None => break,
        }
    }
    Ok(serde_json::json!({"queues": all_items}))
}

pub async fn get_queue(farm_id: &str, queue_id: &str, config: Option<&IniConfig>) -> Result<Value, DeadlineError> {
    let client = session::deadline_client(config).await;
    let capture = ResponseBodyCapture::new();
    client.get_queue().farm_id(farm_id).queue_id(queue_id)
        .customize().interceptor(capture.clone()).send().await.map_err(sdk_err)?;
    capture.json().map_err(capture_err)
}

// ---------------------------------------------------------------------------
// Fleet
// ---------------------------------------------------------------------------

pub async fn list_fleets(farm_id: &str, config: Option<&IniConfig>) -> Result<Value, DeadlineError> {
    let client = session::deadline_client(config).await;
    let (user_id, _) = auth::get_user_and_identity_store_id(config);
    let mut all_items = Vec::new();
    let mut next_token: Option<String> = None;
    loop {
        let capture = ResponseBodyCapture::new();
        let mut req = client.list_fleets().farm_id(farm_id);
        if let Some(ref uid) = user_id { req = req.principal_id(uid.as_str()); }
        if let Some(t) = next_token.take() { req = req.next_token(t); }
        req.customize().interceptor(capture.clone()).send().await.map_err(sdk_err)?;
        let page = capture.json().map_err(capture_err)?;
        if let Some(items) = page["fleets"].as_array() {
            all_items.extend(items.iter().cloned());
        }
        match page.get("nextToken").and_then(|t| t.as_str()) {
            Some(t) => next_token = Some(t.to_string()),
            None => break,
        }
    }
    Ok(serde_json::json!({"fleets": all_items}))
}

pub async fn get_fleet(farm_id: &str, fleet_id: &str, config: Option<&IniConfig>) -> Result<Value, DeadlineError> {
    let client = session::deadline_client(config).await;
    let capture = ResponseBodyCapture::new();
    client.get_fleet().farm_id(farm_id).fleet_id(fleet_id)
        .customize().interceptor(capture.clone()).send().await.map_err(sdk_err)?;
    capture.json().map_err(capture_err)
}

// ---------------------------------------------------------------------------
// Job
// ---------------------------------------------------------------------------

pub async fn list_jobs(farm_id: &str, queue_id: &str, config: Option<&IniConfig>) -> Result<Value, DeadlineError> {
    let client = session::deadline_client(config).await;
    let (user_id, _) = auth::get_user_and_identity_store_id(config);
    let mut all_items = Vec::new();
    let mut next_token: Option<String> = None;
    loop {
        let capture = ResponseBodyCapture::new();
        let mut req = client.list_jobs().farm_id(farm_id).queue_id(queue_id);
        if let Some(ref uid) = user_id { req = req.principal_id(uid.as_str()); }
        if let Some(t) = next_token.take() { req = req.next_token(t); }
        req.customize().interceptor(capture.clone()).send().await.map_err(sdk_err)?;
        let page = capture.json().map_err(capture_err)?;
        if let Some(items) = page["jobs"].as_array() {
            all_items.extend(items.iter().cloned());
        }
        match page.get("nextToken").and_then(|t| t.as_str()) {
            Some(t) => next_token = Some(t.to_string()),
            None => break,
        }
    }
    Ok(serde_json::json!({"jobs": all_items}))
}

pub async fn get_job(farm_id: &str, queue_id: &str, job_id: &str, config: Option<&IniConfig>) -> Result<Value, DeadlineError> {
    let client = session::deadline_client(config).await;
    let capture = ResponseBodyCapture::new();
    client.get_job().farm_id(farm_id).queue_id(queue_id).job_id(job_id)
        .customize().interceptor(capture.clone()).send().await.map_err(sdk_err)?;
    capture.json().map_err(capture_err)
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
) -> Result<Value, DeadlineError> {
    let client = session::deadline_client(config).await;
    let capture = ResponseBodyCapture::new();
    client
        .search_workers()
        .farm_id(farm_id)
        .set_fleet_ids(Some(fleet_ids.iter().map(|s| s.to_string()).collect()))
        .item_offset(item_offset)
        .page_size(page_size)
        .customize()
        .interceptor(capture.clone())
        .send()
        .await
        .map_err(sdk_err)?;
    capture.json().map_err(capture_err)
}

pub async fn get_worker(
    farm_id: &str,
    fleet_id: &str,
    worker_id: &str,
    config: Option<&IniConfig>,
) -> Result<Value, DeadlineError> {
    let client = session::deadline_client(config).await;
    let capture = ResponseBodyCapture::new();
    client.get_worker().farm_id(farm_id).fleet_id(fleet_id).worker_id(worker_id)
        .customize().interceptor(capture.clone()).send().await.map_err(sdk_err)?;
    capture.json().map_err(capture_err)
}
