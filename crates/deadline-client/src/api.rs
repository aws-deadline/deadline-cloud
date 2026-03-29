use crate::session;
use deadline_config::ini::IniConfig;
use deadline_models::errors::DeadlineError;
use serde_json::Value;

/// Format an AWS SDK error to include the error code and message.
/// This is important for suggest_resources_on_client_error to detect
/// AccessDeniedException, ResourceNotFoundException, etc.
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

pub async fn list_farms(config: Option<&IniConfig>) -> Result<Value, DeadlineError> {
    let client = session::deadline_client(config).await;
    let mut all = Vec::new();
    let mut next_token: Option<String> = None;
    loop {
        let mut req = client.list_farms();
        if let Some(t) = next_token.take() { req = req.next_token(t); }
        let resp = req.send().await.map_err(sdk_err)?;
        for f in resp.farms() {
            all.push(serde_json::json!({"farmId": f.farm_id(), "displayName": f.display_name()}));
        }
        match resp.next_token() { Some(t) => next_token = Some(t.to_string()), None => break }
    }
    Ok(serde_json::json!({"farms": all}))
}

pub async fn get_farm(farm_id: &str, config: Option<&IniConfig>) -> Result<Value, DeadlineError> {
    let client = session::deadline_client(config).await;
    let resp = client.get_farm().farm_id(farm_id).send().await
        .map_err(sdk_err)?;
    let mut m = serde_json::Map::new();
    m.insert("farmId".into(), Value::String(resp.farm_id().to_string()));
    m.insert("displayName".into(), Value::String(resp.display_name().to_string()));
    if let Some(v) = resp.description() { m.insert("description".into(), Value::String(v.to_string())); }
    if let Some(v) = resp.kms_key_arn() { m.insert("kmsKeyArn".into(), Value::String(v.to_string())); }
    Ok(Value::Object(m))
}

pub async fn list_queues(farm_id: &str, config: Option<&IniConfig>) -> Result<Value, DeadlineError> {
    let client = session::deadline_client(config).await;
    let mut all = Vec::new();
    let mut next_token: Option<String> = None;
    loop {
        let mut req = client.list_queues().farm_id(farm_id);
        if let Some(t) = next_token.take() { req = req.next_token(t); }
        let resp = req.send().await.map_err(sdk_err)?;
        for q in resp.queues() {
            all.push(serde_json::json!({"queueId": q.queue_id(), "displayName": q.display_name()}));
        }
        match resp.next_token() { Some(t) => next_token = Some(t.to_string()), None => break }
    }
    Ok(serde_json::json!({"queues": all}))
}

pub async fn get_queue(farm_id: &str, queue_id: &str, config: Option<&IniConfig>) -> Result<Value, DeadlineError> {
    let client = session::deadline_client(config).await;
    let resp = client.get_queue().farm_id(farm_id).queue_id(queue_id).send().await
        .map_err(sdk_err)?;
    let mut m = serde_json::Map::new();
    m.insert("queueId".into(), Value::String(resp.queue_id().to_string()));
    m.insert("displayName".into(), Value::String(resp.display_name().to_string()));
    Ok(Value::Object(m))
}

pub async fn list_fleets(farm_id: &str, config: Option<&IniConfig>) -> Result<Value, DeadlineError> {
    let client = session::deadline_client(config).await;
    let mut all = Vec::new();
    let mut next_token: Option<String> = None;
    loop {
        let mut req = client.list_fleets().farm_id(farm_id);
        if let Some(t) = next_token.take() { req = req.next_token(t); }
        let resp = req.send().await.map_err(sdk_err)?;
        for f in resp.fleets() {
            all.push(serde_json::json!({"fleetId": f.fleet_id(), "displayName": f.display_name()}));
        }
        match resp.next_token() { Some(t) => next_token = Some(t.to_string()), None => break }
    }
    Ok(serde_json::json!({"fleets": all}))
}

pub async fn get_fleet(farm_id: &str, fleet_id: &str, config: Option<&IniConfig>) -> Result<Value, DeadlineError> {
    let client = session::deadline_client(config).await;
    let resp = client.get_fleet().farm_id(farm_id).fleet_id(fleet_id).send().await
        .map_err(sdk_err)?;
    let mut m = serde_json::Map::new();
    m.insert("fleetId".into(), Value::String(resp.fleet_id().to_string()));
    m.insert("displayName".into(), Value::String(resp.display_name().to_string()));
    Ok(Value::Object(m))
}

pub async fn list_jobs(farm_id: &str, queue_id: &str, config: Option<&IniConfig>) -> Result<Value, DeadlineError> {
    let client = session::deadline_client(config).await;
    let mut all = Vec::new();
    let mut next_token: Option<String> = None;
    loop {
        let mut req = client.list_jobs().farm_id(farm_id).queue_id(queue_id);
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
    let resp = client.get_job().farm_id(farm_id).queue_id(queue_id).job_id(job_id).send().await
        .map_err(sdk_err)?;
    let mut m = serde_json::Map::new();
    m.insert("jobId".into(), Value::String(resp.job_id().to_string()));
    m.insert("name".into(), Value::String(resp.name().to_string()));
    m.insert("lifecycleStatus".into(), Value::String(resp.lifecycle_status().as_str().to_string()));
    Ok(Value::Object(m))
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
    let resp = client
        .get_worker()
        .farm_id(farm_id)
        .fleet_id(fleet_id)
        .worker_id(worker_id)
        .send()
        .await
        .map_err(sdk_err)?;

    let mut m = serde_json::Map::new();
    m.insert("workerId".into(), Value::String(resp.worker_id().to_string()));
    m.insert("farmId".into(), Value::String(resp.farm_id().to_string()));
    m.insert("fleetId".into(), Value::String(resp.fleet_id().to_string()));
    m.insert("status".into(), Value::String(resp.status().as_str().to_string()));
    let t = resp.created_at();
    if let Ok(s) = t.fmt(aws_sdk_deadline::primitives::DateTimeFormat::DateTime) {
        m.insert("createdAt".into(), Value::String(s));
    }
    Ok(Value::Object(m))
}
