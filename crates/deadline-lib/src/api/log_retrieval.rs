use chrono::{DateTime, TimeZone, Utc};

use crate::api::client::format_sdk_error;
use crate::api::errors::DeadlineError;
use crate::api::{auth, session};

/// A single log event from `CloudWatch` Logs.
#[derive(Debug)]
pub struct LogEvent {
    pub timestamp: DateTime<Utc>,
    pub message: String,
    pub ingestion_time: Option<DateTime<Utc>>,
    pub event_id: Option<String>,
}

/// Result of fetching session logs from `CloudWatch`.
#[derive(Debug)]
pub struct SessionLogResult {
    pub events: Vec<LogEvent>,
    pub next_token: Option<String>,
    pub log_group: String,
    pub log_stream: String,
    pub count: usize,
}

/// Result of fetching worker logs from `CloudWatch`.
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

/// Build a `CloudWatch` Logs client from an `SdkConfig`.
fn logs_client(sdk_config: &aws_config::SdkConfig) -> aws_sdk_cloudwatchlogs::Client {
    let mut builder = aws_sdk_cloudwatchlogs::config::Builder::from(sdk_config);
    if let Ok(url) = std::env::var("AWS_ENDPOINT_URL_CLOUDWATCHLOGS") {
        builder = builder.endpoint_url(url);
    }
    aws_sdk_cloudwatchlogs::Client::from_conf(builder.build())
}

/// Get an `SdkConfig` with fleet-scoped credentials for worker log access.
///
/// If the user is logged in via DCM, calls `AssumeFleetRoleForRead` and
/// builds a temporary `SdkConfig` from the returned credentials. If not DCM,
/// returns the base `SdkConfig`. If fleet role assumption fails for a DCM
/// user, the error is propagated (matching Python behavior).
async fn get_fleet_scoped_config(
    farm_id: &str,
    fleet_id: &str,
    profile: Option<&str>,
) -> Result<aws_config::SdkConfig, DeadlineError> {
    let (user_id, identity_store_id) = auth::get_user_and_identity_store_id_for_profile(profile);
    if user_id.is_some() && identity_store_id.is_some() {
        // DCM user — assume fleet role
        let dl = session::deadline_client(profile).await;
        let resp = dl
            .assume_fleet_role_for_read()
            .farm_id(farm_id)
            .fleet_id(fleet_id)
            .send()
            .await
            .map_err(|e| {
                DeadlineError::OperationError(format!(
                    "Failed to get fleet credentials: {}",
                    format_sdk_error(&e)
                ))
            })?;
        let creds = resp.credentials().ok_or_else(|| {
            DeadlineError::OperationError(
                "Failed to get fleet credentials: Empty credentials received.".into(),
            )
        })?;

        let base_config = session::get_sdk_config(profile).await;
        let region = base_config.region().cloned();

        let credentials = aws_credential_types::Credentials::new(
            creds.access_key_id(),
            creds.secret_access_key(),
            Some(creds.session_token().to_owned()),
            None,
            "fleet-role",
        );
        let mut builder = aws_config::SdkConfig::builder()
            .behavior_version(aws_config::BehaviorVersion::latest())
            .credentials_provider(
                aws_credential_types::provider::SharedCredentialsProvider::new(credentials),
            );
        if let Some(r) = region {
            builder = builder.region(r);
        }
        Ok(builder.build())
    } else {
        // Non-DCM user — use base credentials
        Ok(session::get_sdk_config(profile).await)
    }
}

/// Check if a `CloudWatch` error is `ResourceNotFoundException`.
fn is_resource_not_found<E>(err: &aws_sdk_cloudwatchlogs::error::SdkError<E>) -> bool
where
    E: aws_smithy_types::error::metadata::ProvideErrorMetadata,
{
    use aws_smithy_types::error::metadata::ProvideErrorMetadata;
    err.code() == Some("ResourceNotFoundException")
}

/// Parse `CloudWatch` log events into our `LogEvent` type.
fn parse_events(raw_events: &[aws_sdk_cloudwatchlogs::types::OutputLogEvent]) -> Vec<LogEvent> {
    raw_events
        .iter()
        .map(|e| {
            let timestamp = e
                .timestamp
                .map_or_else(Utc::now, |ms| Utc.timestamp_millis_opt(ms).unwrap());
            let message = e.message.as_deref().unwrap_or("").trim_end().to_owned();
            let ingestion_time = e
                .ingestion_time
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
    profile: Option<&str>,
) -> Result<(SessionLogResult, SessionAutoSelect), DeadlineError> {
    // Resolve session_id
    let (resolved_session_id, auto_select) = if let Some(id) = session_id {
        (id.to_owned(), SessionAutoSelect::Provided)
    } else {
        let jid = job_id.ok_or_else(|| {
            DeadlineError::OperationError("Either session_id or job_id must be provided".into())
        })?;
        auto_select_session(farm_id, queue_id, jid, profile).await?
    };

    let log_group = format!("/aws/deadline/{farm_id}/{queue_id}");
    // Use queue-scoped credentials for DCM users (matching Python behavior)
    let sdk_config = session::get_queue_scoped_config(farm_id, queue_id, profile).await?;
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
            Ok((
                SessionLogResult {
                    events,
                    next_token: resp.next_forward_token().map(ToOwned::to_owned),
                    log_group,
                    log_stream: resolved_session_id,
                    count,
                },
                auto_select,
            ))
        }
        Err(e) => {
            if is_resource_not_found(&e) {
                Ok((
                    SessionLogResult {
                        events: vec![],
                        next_token: None,
                        log_group,
                        log_stream: resolved_session_id,
                        count: 0,
                    },
                    auto_select,
                ))
            } else {
                Err(DeadlineError::OperationError(format!(
                    "Failed to retrieve logs: {}",
                    format_sdk_error(&e)
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
    profile: Option<&str>,
) -> Result<WorkerLogResult, DeadlineError> {
    let log_group = format!("/aws/deadline/{farm_id}/{fleet_id}");
    // Use fleet-scoped credentials for DCM users (matching Python behavior)
    let sdk_config = get_fleet_scoped_config(farm_id, fleet_id, profile).await?;
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
                next_token: resp.next_forward_token().map(ToOwned::to_owned),
                log_group,
                log_stream: worker_id.to_owned(),
                worker_id: worker_id.to_owned(),
                fleet_id: fleet_id.to_owned(),
                count,
            })
        }
        Err(e) => {
            if is_resource_not_found(&e) {
                Ok(WorkerLogResult {
                    events: vec![],
                    next_token: None,
                    log_group,
                    log_stream: worker_id.to_owned(),
                    worker_id: worker_id.to_owned(),
                    fleet_id: fleet_id.to_owned(),
                    count: 0,
                })
            } else {
                Err(DeadlineError::OperationError(format!(
                    "Failed to retrieve worker logs: {}",
                    format_sdk_error(&e)
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
    profile: Option<&str>,
) -> Result<(String, SessionAutoSelect), DeadlineError> {
    let client = session::deadline_client(profile).await;
    let resp = crate::api::client::collect_paginated(
        client
            .list_sessions()
            .farm_id(farm_id)
            .queue_id(queue_id)
            .job_id(job_id)
            .into_paginator()
            .send(),
    )
    .await?;
    let sessions: Vec<&aws_sdk_deadline::types::SessionSummary> = resp
        .iter()
        .flat_map(aws_sdk_deadline::operation::list_sessions::ListSessionsOutput::sessions)
        .collect();

    if sessions.is_empty() {
        return Err(DeadlineError::OperationError(format!(
            "No sessions found for job {job_id}"
        )));
    }

    if sessions.len() == 1 {
        let id = sessions[0].session_id().to_owned();
        return Ok((id.clone(), SessionAutoSelect::OnlySession(id)));
    }

    // Prefer ongoing sessions (no endedAt), most recently started
    let ongoing: Vec<&&aws_sdk_deadline::types::SessionSummary> =
        sessions.iter().filter(|s| s.ended_at().is_none()).collect();

    let best = if ongoing.is_empty() {
        // Fall back to most recently ended
        sessions
            .iter()
            .max_by_key(|s| s.ended_at())
            .expect("sessions is non-empty")
    } else {
        ongoing
            .iter()
            .max_by_key(|s| s.started_at())
            .expect("ongoing is non-empty")
    };

    let id = best.session_id().to_owned();
    Ok((id.clone(), SessionAutoSelect::LatestSession(id)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;
    use wiremock::matchers::{header, method};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    async fn setup_env(server: &MockServer) {
        let url = format!("http://localhost:{}", server.address().port());
        unsafe {
            std::env::set_var("AWS_ENDPOINT_URL_DEADLINE", &url);
            std::env::set_var("AWS_ENDPOINT_URL_CLOUDWATCHLOGS", &url);
            std::env::set_var("AWS_ACCESS_KEY_ID", "AKIAIOSFODNN7EXAMPLE");
            std::env::set_var(
                "AWS_SECRET_ACCESS_KEY",
                "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            );
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
            serde_json::json!({"timestamp": 1_702_857_600_000_i64, "message": "Worker started\n", "ingestionTime": 1_702_857_601_000_i64}),
        ]).await;

        let result = get_worker_logs(
            "farm-abc",
            "fleet-001",
            "worker-001",
            100,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

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
            "farm-abc",
            "fleet-001",
            "worker-001",
            100,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

        assert_eq!(result.count, 0);
        assert!(result.events.is_empty());
    }
}
