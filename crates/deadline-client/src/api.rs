use crate::session;
use deadline_config::ini::IniConfig;
use deadline_models::errors::DeadlineError;
use serde_json::Value;

pub async fn list_farms(config: Option<&IniConfig>) -> Result<Value, DeadlineError> {
    let client = session::deadline_client(config).await;
    let mut all = Vec::new();
    let mut next_token: Option<String> = None;
    loop {
        let mut req = client.list_farms();
        if let Some(t) = next_token.take() { req = req.next_token(t); }
        let resp = req.send().await.map_err(|e| DeadlineError::OperationError(format!("{e}")))?;
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
        .map_err(|e| DeadlineError::OperationError(format!("{e}")))?;
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
        let resp = req.send().await.map_err(|e| DeadlineError::OperationError(format!("{e}")))?;
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
        .map_err(|e| DeadlineError::OperationError(format!("{e}")))?;
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
        let resp = req.send().await.map_err(|e| DeadlineError::OperationError(format!("{e}")))?;
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
        .map_err(|e| DeadlineError::OperationError(format!("{e}")))?;
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
        let resp = req.send().await.map_err(|e| DeadlineError::OperationError(format!("{e}")))?;
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
        .map_err(|e| DeadlineError::OperationError(format!("{e}")))?;
    let mut m = serde_json::Map::new();
    m.insert("jobId".into(), Value::String(resp.job_id().to_string()));
    m.insert("name".into(), Value::String(resp.name().to_string()));
    m.insert("lifecycleStatus".into(), Value::String(resp.lifecycle_status().as_str().to_string()));
    Ok(Value::Object(m))
}
