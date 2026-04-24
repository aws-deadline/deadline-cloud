use chrono::{DateTime, TimeZone, Utc};
use deadline_config::ini::IniConfig;

use crate::errors::DeadlineError;
use crate::{api, auth, session};

/// A single log event from CloudWatch Logs.
#[derive(Debug)]
pub struct LogEvent {
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub message: String,
    pub ingestion_time: Option<chrono::DateTime<chrono::Utc>>,
    pub event_id: Option<String>,
}

/// Result of fetching session logs from CloudWatch.
#[derive(Debug)]
pub struct SessionLogResult {
    pub events: Vec<LogEvent>,
    pub next_token: Option<String>,
    pub log_group: String,
    pub log_stream: String,
    pub count: usize,
}

/// Result of fetching worker logs from CloudWatch.
#[derive(Debug)]
pub struct WorkerLogResult {
    pub events: Vec<LogEvent>,
    pub next_token: Option<String>,
    pub log_group: String,
    pub log_stream: String,
    pub worker_id: String,
    pub fleet_id: String,
    pub count: usize,
}

/// How the session was selected for log retrieval.
#[derive(Debug, Clone, PartialEq)]
pub enum SessionAutoSelect {
    /// Session ID was provided explicitly.
    Provided,
    /// Only one session exists for the job.
    OnlySession(String),
    /// Multiple sessions exist; the latest was selected.
    LatestSession(String),
}

/// Build a CloudWatch Logs client from an SdkConfig.
fn logs_client(sdk_config: &aws_config::SdkConfig) -> aws_sdk_cloudwatchlogs::Client {
    let mut builder = aws_sdk_cloudwatchlogs::config::Builder::from(sdk_config);
    if let Ok(url) = std::env::var("AWS_ENDPOINT_URL_CLOUDWATCHLOGS") {
        builder = builder.endpoint_url(url);
    }
    aws_sdk_cloudwatchlogs::Client::from_conf(builder.build())
}

/// Get an SdkConfig with fleet-scoped credentials for worker log access.
///
/// If the user is logged in via DCM, calls `AssumeFleetRoleForRead` and
/// builds a temporary SdkConfig from the returned credentials. If not DCM,
/// returns the base SdkConfig. If fleet role assumption fails for a DCM
/// user, the error is propagated (matching Python behavior).
async fn get_fleet_scoped_config(
    farm_id: &str,
    fleet_id: &str,
    config: Option<&IniConfig>,
) -> Result<aws_config::SdkConfig, DeadlineError> {
    let (user_id, identity_store_id) = auth::get_user_and_identity_store_id(config);
    if user_id.is_some() && identity_store_id.is_some() {
        // DCM user — assume fleet role
        let resp = api::assume_fleet_role_for_read(farm_id, fleet_id, config, None).await
            .map_err(|e| DeadlineError::OperationError(
                format!("Failed to get fleet credentials: {e}")
            ))?;
        let creds = &resp["credentials"];
        let access_key = creds["accessKeyId"].as_str().unwrap_or_default();
        let secret_key = creds["secretAccessKey"].as_str().unwrap_or_default();
        let session_token = creds["sessionToken"].as_str().unwrap_or_default();

        let base_config = session::get_sdk_config(config).await;
        let region = base_config.region().cloned();

        let credentials = aws_credential_types::Credentials::new(
            access_key, secret_key, Some(session_token.to_string()), None, "fleet-role",
        );
        let mut builder = aws_config::SdkConfig::builder()
            .behavior_version(aws_config::BehaviorVersion::latest())
            .credentials_provider(aws_credential_types::provider::SharedCredentialsProvider::new(credentials));
        if let Some(r) = region {
            builder = builder.region(r);
        }
        Ok(builder.build())
    } else {
        // Non-DCM user — use base credentials
        Ok(session::get_sdk_config(config).await)
    }
}

/// Format a CloudWatch SDK error using the common smithy trait.
fn cw_sdk_err<E>(err: &aws_sdk_cloudwatchlogs::error::SdkError<E>) -> String
where
    E: std::fmt::Display + aws_smithy_types::error::metadata::ProvideErrorMetadata + std::error::Error + 'static,
{
    match err {
        aws_sdk_cloudwatchlogs::error::SdkError::ServiceError(e) => {
            let inner = e.err();
            let code = inner.code().unwrap_or("Unknown");
            let msg = inner.message().unwrap_or("No message");
            format!("{code}: {msg}")
        }
        other => format!("{}", aws_smithy_types::error::display::DisplayErrorContext(other)),
    }
}

/// Check if a CloudWatch error is ResourceNotFoundException.
fn is_resource_not_found<E>(err: &aws_sdk_cloudwatchlogs::error::SdkError<E>) -> bool
where
    E: aws_smithy_types::error::metadata::ProvideErrorMetadata,
{
    use aws_smithy_types::error::metadata::ProvideErrorMetadata;
    err.code() == Some("ResourceNotFoundException")
}

/// Parse CloudWatch log events into our LogEvent type.
fn parse_events(raw_events: &[aws_sdk_cloudwatchlogs::types::OutputLogEvent]) -> Vec<LogEvent> {
    raw_events
        .iter()
        .map(|e| {
            let timestamp = e.timestamp.map(|ms| Utc.timestamp_millis_opt(ms).unwrap())
                .unwrap_or_else(Utc::now);
            let message = e.message.as_deref().unwrap_or("").trim_end().to_string();
            let ingestion_time = e.ingestion_time
                .map(|ms| Utc.timestamp_millis_opt(ms).unwrap());
            LogEvent {
                timestamp,
                message,
                ingestion_time,
                event_id: None,
            }
        })
        .collect()
}

pub async fn get_session_logs(
    farm_id: &str,
    queue_id: &str,
    session_id: Option<&str>,
    job_id: Option<&str>,
    limit: i32,
    start_time: Option<DateTime<Utc>>,
    end_time: Option<DateTime<Utc>>,
    next_token: Option<&str>,
    config: Option<&IniConfig>,
) -> Result<(SessionLogResult, SessionAutoSelect), DeadlineError> {
    // Resolve session_id
    let (resolved_session_id, auto_select) = match session_id {
        Some(id) => (id.to_string(), SessionAutoSelect::Provided),
        None => {
            let jid = job_id.ok_or_else(|| {
                DeadlineError::OperationError(
                    "Either session_id or job_id must be provided".into(),
                )
            })?;
            auto_select_session(farm_id, queue_id, jid, config).await?
        }
    };

    let log_group = format!("/aws/deadline/{farm_id}/{queue_id}");
    // Use queue-scoped credentials for DCM users (matching Python behavior)
    let sdk_config = session::get_queue_scoped_config(farm_id, queue_id, config).await?;
    let client = logs_client(&sdk_config);

    let mut req = client
        .get_log_events()
        .log_group_name(&log_group)
        .log_stream_name(&resolved_session_id)
        .limit(limit)
        .start_from_head(false);

    if let Some(token) = next_token {
        req = req.next_token(token);
    }
    if let Some(t) = start_time {
        req = req.start_time(t.timestamp_millis());
    }
    if let Some(t) = end_time {
        req = req.end_time(t.timestamp_millis());
    }

    match req.send().await {
        Ok(resp) => {
            let raw_events = resp.events();
            let events = parse_events(raw_events);
            let count = events.len();
            Ok((SessionLogResult {
                events,
                next_token: resp.next_forward_token().map(|s| s.to_string()),
                log_group,
                log_stream: resolved_session_id,
                count,
            }, auto_select))
        }
        Err(e) => {
            if is_resource_not_found(&e) {
                Ok((SessionLogResult {
                    events: vec![],
                    next_token: None,
                    log_group,
                    log_stream: resolved_session_id,
                    count: 0,
                }, auto_select))
            } else {
                Err(DeadlineError::OperationError(format!(
                    "Failed to retrieve logs: {}", cw_sdk_err(&e)
                )))
            }
        }
    }
}

pub async fn get_worker_logs(
    farm_id: &str,
    fleet_id: &str,
    worker_id: &str,
    limit: i32,
    start_time: Option<DateTime<Utc>>,
    end_time: Option<DateTime<Utc>>,
    next_token: Option<&str>,
    config: Option<&IniConfig>,
) -> Result<WorkerLogResult, DeadlineError> {
    let log_group = format!("/aws/deadline/{farm_id}/{fleet_id}");
    // Use fleet-scoped credentials for DCM users (matching Python behavior)
    let sdk_config = get_fleet_scoped_config(farm_id, fleet_id, config).await?;
    let client = logs_client(&sdk_config);

    let mut req = client
        .get_log_events()
        .log_group_name(&log_group)
        .log_stream_name(worker_id)
        .limit(limit)
        .start_from_head(false);

    if let Some(token) = next_token {
        req = req.next_token(token);
    }
    if let Some(t) = start_time {
        req = req.start_time(t.timestamp_millis());
    }
    if let Some(t) = end_time {
        req = req.end_time(t.timestamp_millis());
    }

    match req.send().await {
        Ok(resp) => {
            let raw_events = resp.events();
            let events = parse_events(raw_events);
            let count = events.len();
            Ok(WorkerLogResult {
                events,
                next_token: resp.next_forward_token().map(|s| s.to_string()),
                log_group,
                log_stream: worker_id.to_string(),
                worker_id: worker_id.to_string(),
                fleet_id: fleet_id.to_string(),
                count,
            })
        }
        Err(e) => {
            if is_resource_not_found(&e) {
                Ok(WorkerLogResult {
                    events: vec![],
                    next_token: None,
                    log_group,
                    log_stream: worker_id.to_string(),
                    worker_id: worker_id.to_string(),
                    fleet_id: fleet_id.to_string(),
                    count: 0,
                })
            } else {
                Err(DeadlineError::OperationError(format!(
                    "Failed to retrieve worker logs: {}", cw_sdk_err(&e)
                )))
            }
        }
    }
}

/// Auto-select a session for a job: prefer ongoing, then most recently ended.
async fn auto_select_session(
    farm_id: &str,
    queue_id: &str,
    job_id: &str,
    config: Option<&IniConfig>,
) -> Result<(String, SessionAutoSelect), DeadlineError> {
    let resp = api::list_sessions(farm_id, queue_id, job_id, config, None).await?;
    let empty = vec![];
    let sessions = resp["sessions"].as_array().unwrap_or(&empty);

    if sessions.is_empty() {
        return Err(DeadlineError::OperationError(format!(
            "No sessions found for job {job_id}"
        )));
    }

    if sessions.len() == 1 {
        let id = sessions[0]["sessionId"].as_str().unwrap_or("").to_string();
        return Ok((id.clone(), SessionAutoSelect::OnlySession(id)));
    }

    // Prefer ongoing sessions (no endedAt), most recently started
    let ongoing: Vec<&serde_json::Value> = sessions
        .iter()
        .filter(|s| s.get("endedAt").is_none())
        .collect();

    let best = if !ongoing.is_empty() {
        ongoing
            .iter()
            .max_by_key(|s| s["startedAt"].as_str().unwrap_or(""))
            .unwrap()
    } else {
        // Fall back to most recently ended
        sessions
            .iter()
            .max_by_key(|s| s["endedAt"].as_str().unwrap_or(""))
            .unwrap()
    };

    let id = best["sessionId"].as_str().unwrap_or("").to_string();
    Ok((id.clone(), SessionAutoSelect::LatestSession(id)))
}


#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;
    use wiremock::matchers::{method, header};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    async fn setup_env(server: &MockServer) {
        let url = format!("http://localhost:{}", server.address().port());
        unsafe {
            std::env::set_var("AWS_ENDPOINT_URL_DEADLINE", &url);
            std::env::set_var("AWS_ENDPOINT_URL_CLOUDWATCHLOGS", &url);
            std::env::set_var("AWS_ACCESS_KEY_ID", "AKIAIOSFODNN7EXAMPLE");
            std::env::set_var("AWS_SECRET_ACCESS_KEY", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
            std::env::set_var("AWS_DEFAULT_REGION", "us-west-2");
            // Prevent host ~/.aws/config from triggering DCM credential scoping
            std::env::set_var("AWS_CONFIG_FILE", "/dev/null");
        }
    }

    async fn mock_cw_events(server: &MockServer, events: &[serde_json::Value]) {
        Mock::given(method("POST"))
            .and(header("x-amz-target", "Logs_20140328.GetLogEvents"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "events": events,
                "nextBackwardToken": "b/tok",
                "nextForwardToken": "f/tok",
            })))
            .mount(server)
            .await;
    }

    // Level 1: get_worker_logs happy path — not reachable via CLI
    #[tokio::test]
    #[serial]
    async fn get_worker_logs_returns_events_and_metadata() {
        let server = MockServer::start().await;
        setup_env(&server).await;

        mock_cw_events(&server, &[
            serde_json::json!({"timestamp": 1702857600000_i64, "message": "Worker started\n", "ingestionTime": 1702857601000_i64}),
        ]).await;

        let result = get_worker_logs(
            "farm-abc", "fleet-001", "worker-001", 100, None, None, None, None,
        ).await.unwrap();

        assert_eq!(result.count, 1);
        assert_eq!(result.log_group, "/aws/deadline/farm-abc/fleet-001");
        assert_eq!(result.log_stream, "worker-001");
        assert_eq!(result.worker_id, "worker-001");
        assert_eq!(result.fleet_id, "fleet-001");
        assert_eq!(result.events[0].message, "Worker started");
    }

    // Level 1: get_worker_logs ResourceNotFoundException returns empty
    #[tokio::test]
    #[serial]
    async fn get_worker_logs_not_found_returns_empty() {
        let server = MockServer::start().await;
        setup_env(&server).await;

        Mock::given(method("POST"))
            .and(header("x-amz-target", "Logs_20140328.GetLogEvents"))
            .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
                "__type": "ResourceNotFoundException",
                "message": "The specified log group does not exist."
            })))
            .mount(&server)
            .await;

        let result = get_worker_logs(
            "farm-abc", "fleet-001", "worker-001", 100, None, None, None, None,
        ).await.unwrap();

        assert_eq!(result.count, 0);
        assert!(result.events.is_empty());
    }
}
