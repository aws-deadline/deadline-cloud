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

/// Format an AWS SDK DateTime to match Python/boto3 YAML output.
/// SDK gives "2024-12-18T00:37:38Z", Python gives "2024-12-18 00:37:38+00:00".
pub fn fmt_datetime(dt: &aws_sdk_deadline::primitives::DateTime) -> String {
    dt.fmt(aws_sdk_deadline::primitives::DateTimeFormat::DateTime)
        .unwrap_or_default()
        .replace('T', " ")
        .replace('Z', "+00:00")
}

/// Insert a string value into a JSON map.
pub fn put(m: &mut serde_json::Map<String, Value>, k: &str, v: &str) {
    m.insert(k.into(), Value::String(v.to_string()));
}

/// Insert an optional string value into a JSON map (skip if None).
pub fn put_opt(m: &mut serde_json::Map<String, Value>, k: &str, v: Option<&str>) {
    if let Some(v) = v { m.insert(k.into(), Value::String(v.to_string())); }
}

/// Insert a DateTime value into a JSON map.
pub fn put_dt(m: &mut serde_json::Map<String, Value>, k: &str, v: &aws_sdk_deadline::primitives::DateTime) {
    m.insert(k.into(), Value::String(fmt_datetime(v)));
}

/// Insert an optional DateTime value into a JSON map (skip if None).
pub fn put_dt_opt(m: &mut serde_json::Map<String, Value>, k: &str, v: Option<&aws_sdk_deadline::primitives::DateTime>) {
    if let Some(v) = v { m.insert(k.into(), Value::String(fmt_datetime(v))); }
}

// ---------------------------------------------------------------------------
// Farm
// ---------------------------------------------------------------------------

pub async fn list_farms(config: Option<&IniConfig>) -> Result<Value, DeadlineError> {
    let client = session::deadline_client(config).await;
    let (user_id, _) = auth::get_user_and_identity_store_id(config);

    let mut builder = client.list_farms();
    if let Some(ref uid) = user_id { builder = builder.principal_id(uid.as_str()); }

    let mut stream = builder.into_paginator().items().send();
    let mut all = Vec::new();
    while let Some(item) = stream.try_next().await.map_err(sdk_err)? {
        // Python selects ["farmId", "displayName"] in this order
        let mut m = serde_json::Map::new();
        put(&mut m, "farmId", item.farm_id());
        put(&mut m, "displayName", item.display_name());
        all.push(Value::Object(m));
    }
    Ok(serde_json::json!({"farms": all}))
}

pub async fn get_farm(farm_id: &str, config: Option<&IniConfig>) -> Result<Value, DeadlineError> {
    let client = session::deadline_client(config).await;
    let capture = ResponseBodyCapture::new();
    client.get_farm().farm_id(farm_id)
        .customize().interceptor(capture.clone()).send().await.map_err(sdk_err)?;
    capture.json().map_err(|e| DeadlineError::OperationError(e.to_string()))
}

// ---------------------------------------------------------------------------
// Queue
// ---------------------------------------------------------------------------

pub async fn list_queues(farm_id: &str, config: Option<&IniConfig>) -> Result<Value, DeadlineError> {
    let client = session::deadline_client(config).await;
    let (user_id, _) = auth::get_user_and_identity_store_id(config);

    let mut builder = client.list_queues().farm_id(farm_id);
    if let Some(ref uid) = user_id { builder = builder.principal_id(uid.as_str()); }

    let mut stream = builder.into_paginator().items().send();
    let mut all = Vec::new();
    while let Some(item) = stream.try_next().await.map_err(sdk_err)? {
        let mut m = serde_json::Map::new();
        put(&mut m, "queueId", item.queue_id());
        put(&mut m, "displayName", item.display_name());
        all.push(Value::Object(m));
    }
    Ok(serde_json::json!({"queues": all}))
}

pub async fn get_queue(farm_id: &str, queue_id: &str, config: Option<&IniConfig>) -> Result<Value, DeadlineError> {
    let client = session::deadline_client(config).await;
    let capture = ResponseBodyCapture::new();
    client.get_queue().farm_id(farm_id).queue_id(queue_id)
        .customize().interceptor(capture.clone()).send().await.map_err(sdk_err)?;
    capture.json().map_err(|e| DeadlineError::OperationError(e.to_string()))
}

// ---------------------------------------------------------------------------
// Fleet
// ---------------------------------------------------------------------------

pub async fn list_fleets(farm_id: &str, config: Option<&IniConfig>) -> Result<Value, DeadlineError> {
    let client = session::deadline_client(config).await;
    let (user_id, _) = auth::get_user_and_identity_store_id(config);

    let mut builder = client.list_fleets().farm_id(farm_id);
    if let Some(ref uid) = user_id { builder = builder.principal_id(uid.as_str()); }

    let mut stream = builder.into_paginator().items().send();
    let mut all = Vec::new();
    while let Some(item) = stream.try_next().await.map_err(sdk_err)? {
        let mut m = serde_json::Map::new();
        put(&mut m, "fleetId", item.fleet_id());
        put(&mut m, "displayName", item.display_name());
        all.push(Value::Object(m));
    }
    Ok(serde_json::json!({"fleets": all}))
}

pub async fn get_fleet(farm_id: &str, fleet_id: &str, config: Option<&IniConfig>) -> Result<Value, DeadlineError> {
    let client = session::deadline_client(config).await;
    let capture = ResponseBodyCapture::new();
    client.get_fleet().farm_id(farm_id).fleet_id(fleet_id)
        .customize().interceptor(capture.clone()).send().await.map_err(sdk_err)?;
    capture.json().map_err(|e| DeadlineError::OperationError(e.to_string()))
}

pub async fn list_jobs(farm_id: &str, queue_id: &str, config: Option<&IniConfig>) -> Result<Value, DeadlineError> {
    let client = session::deadline_client(config).await;
    let (user_id, _) = auth::get_user_and_identity_store_id(config);
    let mut all = Vec::new();
    let mut next_token: Option<String> = None;
    loop {
        let mut req = client.list_jobs().farm_id(farm_id).queue_id(queue_id);
        if let Some(ref uid) = user_id { req = req.principal_id(uid.as_str()); }
        if let Some(t) = next_token.take() { req = req.next_token(t); }
        let resp = req.send().await.map_err(sdk_err)?;
        for j in resp.jobs() {
            all.push(serde_json::json!({"jobId": j.job_id(), "name": j.name()}));
        }
        match resp.next_token() { Some(t) => next_token = Some(t.to_string()), None => break }
    }
    Ok(serde_json::json!({"jobs": all}))
}

pub async fn get_job(farm_id: &str, queue_id: &str, job_id: &str, config: Option<&IniConfig>) -> Result<Value, DeadlineError> {
    let client = session::deadline_client(config).await;
    let capture = ResponseBodyCapture::new();
    client.get_job().farm_id(farm_id).queue_id(queue_id).job_id(job_id)
        .customize().interceptor(capture.clone()).send().await.map_err(sdk_err)?;
    capture.json().map_err(|e| DeadlineError::OperationError(e.to_string()))
}

pub async fn search_workers(
    farm_id: &str,
    fleet_ids: &[&str],
    item_offset: i32,
    page_size: i32,
    config: Option<&IniConfig>,
) -> Result<Value, DeadlineError> {
    let client = session::deadline_client(config).await;
    let resp = client
        .search_workers()
        .farm_id(farm_id)
        .set_fleet_ids(Some(fleet_ids.iter().map(|s| s.to_string()).collect()))
        .item_offset(item_offset)
        .page_size(page_size)
        .send()
        .await
        .map_err(sdk_err)?;

    let workers: Vec<Value> = resp
        .workers()
        .iter()
        .map(|w| {
            let mut m = serde_json::Map::new();
            if let Some(id) = w.worker_id() {
                m.insert("workerId".into(), Value::String(id.to_string()));
            }
            if let Some(s) = w.status() {
                m.insert("status".into(), Value::String(s.as_str().to_string()));
            }
            if let Some(t) = w.created_at() {
                if let Ok(s) = t.fmt(aws_sdk_deadline::primitives::DateTimeFormat::DateTime) {
                    m.insert("createdAt".into(), Value::String(s));
                }
            }
            Value::Object(m)
        })
        .collect();

    Ok(serde_json::json!({
        "workers": workers,
        "totalResults": resp.total_results(),
    }))
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
    capture.json().map_err(|e| DeadlineError::OperationError(e.to_string()))
}
