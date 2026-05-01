use clap::Subcommand;
use deadline_api::{api, client, job_monitoring, log_retrieval, session};
use deadline_api::log_retrieval::SessionAutoSelect;
use deadline_api::responses::{self, JobResponse, SessionResponse};
use deadline_config::config_file;
use deadline_config::ini::IniConfig;
use regex::Regex;
use std::sync::LazyLock;

use super::config::CliError;
use super::helpers::suggest_resources_on_client_error;

static SESSION_ACTION_ID_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^sessionaction-([0-9a-f]{32})-\d+$").unwrap());

/// Parse a session action ID and derive the session ID.
/// Format: `sessionaction-{uuid}-{number}` → `session-{uuid}`
fn parse_session_action_id(session_action_id: &str) -> Result<String, CliError> {
    let caps = SESSION_ACTION_ID_RE.captures(session_action_id).ok_or_else(|| {
        CliError::Operation(format!(
            "Invalid session action ID format: '{}'. Expected format: sessionaction-{{uuid}}-{{number}}",
            session_action_id
        ))
    })?;
    Ok(format!("session-{}", &caps[1]))
}

fn parse_trace_format(s: &str) -> Result<String, String> {
    match s.to_lowercase().as_str() {
        "chrome" => Ok("chrome".to_string()),
        other => Err(format!("Invalid value '{other}' for --trace-format. Valid values: chrome")),
    }
}

fn parse_conflict_resolution(s: &str) -> Result<deadline_job_attachments::models::FileConflictResolution, String> {
    match s.to_uppercase().as_str() {
        "SKIP" => Ok(deadline_job_attachments::models::FileConflictResolution::Skip),
        "OVERWRITE" => Ok(deadline_job_attachments::models::FileConflictResolution::Overwrite),
        "CREATE_COPY" => Ok(deadline_job_attachments::models::FileConflictResolution::CreateCopy),
        other => Err(format!(
            "Invalid conflict resolution: {other}. Use SKIP, OVERWRITE, or CREATE_COPY"
        )),
    }
}

/// Set up config from CLI options and extract required settings.
/// Returns (config, farm_id, queue_id) or (config, farm_id, queue_id, job_id).
fn setup_config(
    profile: Option<String>,
    farm_id: Option<String>,
    queue_id: Option<String>,
    job_id: Option<String>,
    yes: bool,
    required: &[&str],
) -> Result<IniConfig, CliError> {
    let mut config = config_file::read_config()
        .map_err(|e| CliError::Operation(e.to_string()))?;
    crate::common::apply_cli_options_to_config(
        &mut config,
        &crate::common::CliOptions { profile, farm_id, queue_id, job_id, yes, ..Default::default() },
        required,
    )?;
    Ok(config)
}

fn get(config: &IniConfig, setting: &str) -> String {
    config_file::get_setting(setting, config).unwrap_or_default()
}

#[derive(Subcommand)]
pub enum JobAction {
    /// List jobs in a queue
    List {
        #[arg(long)] profile: Option<String>,
        #[arg(long)] farm_id: Option<String>,
        #[arg(long)] queue_id: Option<String>,
        #[arg(long, default_value = "5")]
        page_size: i32,
        #[arg(long, default_value = "0")]
        item_offset: i32,
    },
    /// Get details of a specific job, or search for jobs with a search term
    Get {
        /// A job ID (job-xxx) or search string to find matching jobs
        #[arg()]
        search_term: Option<String>,
        #[arg(long)] profile: Option<String>,
        #[arg(long)] farm_id: Option<String>,
        #[arg(long)] queue_id: Option<String>,
        #[arg(long)] job_id: Option<String>,
    },
    /// Get details of a specific session
    GetSession {
        #[arg(long)] profile: Option<String>,
        #[arg(long)] farm_id: Option<String>,
        #[arg(long)] queue_id: Option<String>,
        #[arg(long)] job_id: Option<String>,
        #[arg(long)] session_id: String,
    },
    /// List sessions for a job
    ListSessions {
        #[arg(long)] profile: Option<String>,
        #[arg(long)] farm_id: Option<String>,
        #[arg(long)] queue_id: Option<String>,
        #[arg(long)] job_id: Option<String>,
    },
    /// List steps for a job
    ListSteps {
        #[arg(long)] profile: Option<String>,
        #[arg(long)] farm_id: Option<String>,
        #[arg(long)] queue_id: Option<String>,
        #[arg(long)] job_id: Option<String>,
    },
    /// List tasks for a step
    ListTasks {
        #[arg(long)] profile: Option<String>,
        #[arg(long)] farm_id: Option<String>,
        #[arg(long)] queue_id: Option<String>,
        #[arg(long)] job_id: Option<String>,
        #[arg(long)] step_id: String,
    },
    /// Wait for a job to complete
    Wait {
        #[arg(long)] profile: Option<String>,
        #[arg(long)] farm_id: Option<String>,
        #[arg(long)] queue_id: Option<String>,
        #[arg(long)] job_id: Option<String>,
        #[arg(long, default_value = "120")]
        max_poll_interval: u64,
        #[arg(long, default_value = "0")]
        timeout: u64,
        #[arg(long, default_value = "verbose")]
        output: String,
    },
    /// Print session logs from CloudWatch for a job
    Logs {
        #[arg(long)] profile: Option<String>,
        #[arg(long)] farm_id: Option<String>,
        #[arg(long)] queue_id: Option<String>,
        #[arg(long)] job_id: Option<String>,
        #[arg(long)] session_id: Option<String>,
        #[arg(long)] session_action_id: Option<String>,
        #[arg(long, default_value = "100")]
        limit: i32,
        #[arg(long)] start_time: Option<String>,
        #[arg(long)] end_time: Option<String>,
        #[arg(long)] next_token: Option<String>,
        #[arg(long, default_value = "verbose")]
        output: String,
        #[arg(long, default_value = "utc")]
        timestamp_format: String,
    },
    /// Cancel a job, optionally marking it with an alternative status
    Cancel {
        #[arg(long)] profile: Option<String>,
        #[arg(long)] farm_id: Option<String>,
        #[arg(long)] queue_id: Option<String>,
        #[arg(long)] job_id: Option<String>,
        #[arg(long, default_value = "CANCELED")]
        mark_as: String,
        #[arg(long)]
        yes: bool,
    },
    /// Requeue tasks of a job
    RequeueTasks {
        #[arg(long)] profile: Option<String>,
        #[arg(long)] farm_id: Option<String>,
        #[arg(long)] queue_id: Option<String>,
        #[arg(long)] job_id: Option<String>,
        #[arg(long)]
        run_status: Vec<String>,
        #[arg(long)]
        yes: bool,
    },
    /// Search for jobs with filter and sort expressions
    Search {
        #[arg(long)] profile: Option<String>,
        #[arg(long)] farm_id: Option<String>,
        #[arg(long)] queue_id: Option<String>,
        #[arg(long)] filter_expressions: Option<String>,
        #[arg(long)] sort_expressions: Option<String>,
        #[arg(long, default_value = "5")]
        page_size: i32,
        #[arg(long, default_value = "0")]
        item_offset: i32,
    },
    /// EXPERIMENTAL - Generate statistics from a job with a trace
    TraceSchedule {
        #[arg(long)] profile: Option<String>,
        #[arg(long)] farm_id: Option<String>,
        #[arg(long)] queue_id: Option<String>,
        #[arg(long)] job_id: Option<String>,
        /// Output verbose trace details
        #[arg(short, long)]
        verbose: bool,
        /// The tracing format to write (only "chrome" supported)
        #[arg(long, value_parser = parse_trace_format)]
        trace_format: Option<String>,
        /// The tracing file to write
        #[arg(long)]
        trace_file: Option<String>,
    },
    /// Download the output of a job saved as job attachments
    DownloadOutput {
        #[arg(long)] profile: Option<String>,
        #[arg(long)] farm_id: Option<String>,
        #[arg(long)] queue_id: Option<String>,
        #[arg(long)] job_id: Option<String>,
        #[arg(long)] step_id: Option<String>,
        #[arg(long)] task_id: Option<String>,
        #[arg(long, value_parser = parse_conflict_resolution)]
        conflict_resolution: Option<deadline_job_attachments::models::FileConflictResolution>,
        #[arg(long)]
        yes: bool,
        #[arg(long, default_value = "verbose")]
        output: String,
    },
}

pub fn run(action: JobAction) -> Result<(), CliError> {
    tokio::runtime::Runtime::new()
        .map_err(|e| CliError::Operation(e.to_string()))?
        .block_on(run_async(action))
}

async fn run_async(action: JobAction) -> Result<(), CliError> {
    match action {
        JobAction::List { profile, farm_id, queue_id, page_size, item_offset } => {
            let config = setup_config(profile, farm_id, queue_id, None, false, &["farm_id", "queue_id"])?;
            let farm = get(&config, "defaults.farm_id");
            let queue = get(&config, "defaults.queue_id");
            let resp = match search_jobs_call(&farm, &[&queue], item_offset, page_size, None, None, Some(&config)).await {
                Ok(r) => r,
                Err(e) => {
                    let suggestion = suggest_resources_on_client_error(
                        &e.to_string(), "SearchJobs", Some(&farm), Some(&queue), None, Some(&config),
                    ).await;
                    return Err(CliError::Operation(format!(
                        "Failed to get Jobs from Deadline:\n{e}{suggestion}"
                    )));
                }
            };
            print_search_jobs_output(&resp, item_offset);
            Ok(())
        }
        JobAction::Get { search_term, profile, farm_id, queue_id, job_id } => {
            // If --job-id is provided, it takes precedence over search_term
            let mut effective_job_id = job_id;
            let mut search = None;

            if let Some(ref term) = search_term {
                if effective_job_id.is_none() {
                    // Check if search_term is a job ID pattern
                    if regex::Regex::new(r"^job-[0-9a-f]{32}$").unwrap().is_match(term) {
                        effective_job_id = Some(term.clone());
                    } else {
                        search = Some(term.clone());
                    }
                }
            }

            if let Some(search_term) = search {
                // Search mode
                let config = setup_config(profile, farm_id, queue_id, None, false, &["farm_id", "queue_id"])?;
                let farm = get(&config, "defaults.farm_id");
                let queue = get(&config, "defaults.queue_id");
                resolve_job_search(&farm, &queue, &search_term, &config).await
            } else {
                // Direct get mode
                let config = setup_config(profile, farm_id, queue_id, effective_job_id, false, &["farm_id", "queue_id", "job_id"])?;
                let farm = get(&config, "defaults.farm_id");
                let queue = get(&config, "defaults.queue_id");
                let job = get(&config, "defaults.job_id");
                print_job_details(&farm, &queue, &job, &config).await
            }
        }
        JobAction::GetSession { profile, farm_id, queue_id, job_id, session_id } => {
            let config = setup_config(profile, farm_id, queue_id, job_id, false, &["farm_id", "queue_id", "job_id"])?;
            let farm = get(&config, "defaults.farm_id");
            let queue = get(&config, "defaults.queue_id");
            let job = get(&config, "defaults.job_id");
            let resp = session::deadline_client(Some(&config)).await
                .get_session().farm_id(&farm).queue_id(&queue).job_id(&job).session_id(&session_id)
                .send().await
                .map_err(|e| CliError::Operation(format!("Failed to get Session from Deadline:\n{}", client::format_sdk_error(&e))))?;
            let session_resp = SessionResponse::from(resp);
            let val = serde_json::to_value(&session_resp).map_err(|e| CliError::Operation(e.to_string()))?;
            println!("{}", crate::common::cli_object_repr(&val));
            Ok(())
        }
        JobAction::ListSessions { profile, farm_id, queue_id, job_id } => {
            let config = setup_config(profile, farm_id, queue_id, job_id, false, &["farm_id", "queue_id", "job_id"])?;
            let farm = get(&config, "defaults.farm_id");
            let queue = get(&config, "defaults.queue_id");
            let job = get(&config, "defaults.job_id");
            let pages = client::collect_paginated(
                session::deadline_client(Some(&config)).await
                    .list_sessions().farm_id(&farm).queue_id(&queue).job_id(&job)
                    .into_paginator().send()
            ).await
                .map_err(|e| CliError::Operation(format!("Failed to list Sessions from Deadline:\n{e}")))?;
            let sessions: Vec<serde_json::Value> = pages.iter()
                .flat_map(|p| p.sessions())
                .map(|s| {
                    let mut m = serde_json::Map::new();
                    m.insert("sessionId".into(), serde_json::Value::String(s.session_id().to_string()));
                    m.insert("fleetId".into(), serde_json::Value::String(s.fleet_id().to_string()));
                    m.insert("workerId".into(), serde_json::Value::String(s.worker_id().to_string()));
                    m.insert("startedAt".into(), serde_json::Value::String(format_datetime(s.started_at())));
                    m.insert("lifecycleStatus".into(), serde_json::Value::String(s.lifecycle_status().as_str().to_string()));
                    if let Some(ended) = s.ended_at() { m.insert("endedAt".into(), serde_json::Value::String(format_datetime(ended))); }
                    serde_json::Value::Object(m)
                })
                .collect();
            println!("{}", crate::common::cli_object_repr(&serde_json::Value::Array(sessions)));
            Ok(())
        }
        JobAction::ListSteps { profile, farm_id, queue_id, job_id } => {
            let config = setup_config(profile, farm_id, queue_id, job_id, false, &["farm_id", "queue_id", "job_id"])?;
            let farm = get(&config, "defaults.farm_id");
            let queue = get(&config, "defaults.queue_id");
            let job = get(&config, "defaults.job_id");
            let pages = client::collect_paginated(
                session::deadline_client(Some(&config)).await
                    .list_steps().farm_id(&farm).queue_id(&queue).job_id(&job)
                    .into_paginator().send()
            ).await
                .map_err(|e| CliError::Operation(format!("Failed to list Steps from Deadline:\n{e}")))?;
            let steps: Vec<serde_json::Value> = pages.iter()
                .flat_map(|p| p.steps())
                .map(|s| {
                    let mut m = serde_json::Map::new();
                    m.insert("stepId".into(), serde_json::Value::String(s.step_id().to_string()));
                    m.insert("name".into(), serde_json::Value::String(s.name().to_string()));
                    m.insert("lifecycleStatus".into(), serde_json::Value::String(s.lifecycle_status().as_str().to_string()));
                    m.insert("createdAt".into(), serde_json::Value::String(format_datetime(&s.created_at)));
                    serde_json::Value::Object(m)
                })
                .collect();
            println!("{}", crate::common::cli_object_repr(&serde_json::Value::Array(steps)));
            Ok(())
        }
        JobAction::ListTasks { profile, farm_id, queue_id, job_id, step_id } => {
            let config = setup_config(profile, farm_id, queue_id, job_id, false, &["farm_id", "queue_id", "job_id"])?;
            let farm = get(&config, "defaults.farm_id");
            let queue = get(&config, "defaults.queue_id");
            let job = get(&config, "defaults.job_id");
            let pages = client::collect_paginated(
                session::deadline_client(Some(&config)).await
                    .list_tasks().farm_id(&farm).queue_id(&queue).job_id(&job).step_id(&step_id)
                    .into_paginator().send()
            ).await
                .map_err(|e| CliError::Operation(format!("Failed to list Tasks from Deadline:\n{e}")))?;
            let tasks: Vec<serde_json::Value> = pages.iter()
                .flat_map(|p| p.tasks())
                .map(|t| {
                    let mut m = serde_json::Map::new();
                    m.insert("taskId".into(), serde_json::Value::String(t.task_id().to_string()));
                    m.insert("runStatus".into(), serde_json::Value::String(t.run_status().as_str().to_string()));
                    m.insert("createdAt".into(), serde_json::Value::String(format_datetime(&t.created_at)));
                    m.insert("createdBy".into(), serde_json::Value::String(t.created_by().to_string()));
                    serde_json::Value::Object(m)
                })
                .collect();
            println!("{}", crate::common::cli_object_repr(&serde_json::Value::Array(tasks)));
            Ok(())
        }
        JobAction::Wait { profile, farm_id, queue_id, job_id, max_poll_interval, timeout, output } => {
            let config = setup_config(profile, farm_id, queue_id, job_id, false, &["farm_id", "queue_id", "job_id"])?;
            let farm = get(&config, "defaults.farm_id");
            let queue = get(&config, "defaults.queue_id");
            let job = get(&config, "defaults.job_id");
            let is_json = output.eq_ignore_ascii_case("json");

            let job_resp = session::deadline_client(Some(&config)).await
                .get_job().farm_id(&farm).queue_id(&queue).job_id(&job)
                .send().await
                .map_err(|e| CliError::Operation(format!("Error waiting for job completion: {}", client::format_sdk_error(&e))))?;
            let job_name = job_resp.name().to_string();

            let job_cb: Box<dyn Fn(&aws_sdk_deadline::operation::get_job::GetJobOutput, f64, u64)> = if is_json {
                Box::new(|_, _, _| {})
            } else {
                Box::new(|j: &aws_sdk_deadline::operation::get_job::GetJobOutput, elapsed: f64, t: u64| {
                    let counts = j.task_run_status_counts();
                    let get_count = |s: aws_sdk_deadline::types::TaskRunStatus| -> i64 {
                        counts.and_then(|m| m.get(&s)).copied().unwrap_or(0) as i64
                    };
                    let running = get_count(aws_sdk_deadline::types::TaskRunStatus::Running)
                        + get_count(aws_sdk_deadline::types::TaskRunStatus::Assigned)
                        + get_count(aws_sdk_deadline::types::TaskRunStatus::Starting);
                    let ok = get_count(aws_sdk_deadline::types::TaskRunStatus::Succeeded);
                    let total: i64 = counts.map_or(0, |m| m.values().sum::<i32>() as i64);
                    let s = j.task_run_status.as_ref().map(|s| s.as_str()).unwrap_or("");
                    let ti = if t > 0 {
                        let r = (t as f64 - elapsed).max(0.0);
                        format!(" [{elapsed:.1}s elapsed, {r:.1}s remaining]")
                    } else {
                        format!(" [{elapsed:.1}s elapsed]")
                    };
                    eprint!("\rCurrent status: {s} ({ok}/{total} tasks succeeded, {running} workers running).{ti}");
                })
            };

            if !is_json {
                eprintln!("Waiting for job {job} to complete...");
                eprintln!("Job Name: {job_name}");
            }

            match job_monitoring::wait_for_job_completion(
                &farm, &queue, &job, max_poll_interval, timeout,
                Some(&config), None, Some(&*job_cb),
            ).await {
                Ok(result) => {
                    let failed_json: Vec<serde_json::Value> = result.failed_tasks.iter().map(|t| {
                        serde_json::json!({
                            "stepId": t.step_id, "taskId": t.task_id,
                            "stepName": t.step_name, "sessionId": t.session_id,
                        })
                    }).collect();

                    if is_json {
                        println!("{}", serde_json::to_string_pretty(&serde_json::json!({
                            "jobId": job, "jobName": job_name,
                            "status": result.status, "elapsedTime": result.elapsed_time,
                            "failedTasks": failed_json,
                        })).unwrap());
                    } else {
                        eprintln!();
                        println!("Job ID: {job}");
                        println!("Job completed with status: {}", result.status);
                        println!("Elapsed time: {:.1} seconds", result.elapsed_time);
                        if result.failed_tasks.is_empty() {
                            println!("No failed tasks found.");
                        } else {
                            println!("Found {} failed tasks:", result.failed_tasks.len());
                            println!("{}", crate::common::cli_object_repr(&serde_json::json!(failed_json)));
                        }
                    }

                    let exit_code = match result.status.as_str() {
                        "SUCCEEDED" if result.failed_tasks.is_empty() => 0,
                        "CANCELED" => 3,
                        "SUSPENDED" | "ARCHIVED" => 4,
                        "NOT_COMPATIBLE" => 5,
                        _ => 2,
                    };
                    if exit_code == 0 { Ok(()) } else {
                        Err(CliError::ExitCode { code: exit_code, message: String::new() })
                    }
                }
                Err(e) => {
                    let is_timeout = matches!(e, deadline_api::errors::DeadlineError::OperationTimedOut(_));
                    if is_json {
                        println!("{}", serde_json::to_string_pretty(&serde_json::json!({
                            "error": e.to_string(), "timeout": is_timeout,
                            "jobId": job, "jobName": job_name,
                        })).unwrap());
                    } else {
                        println!("Job ID: {job}");
                        println!("Job Name: {job_name}");
                        println!("Error waiting for job completion: {e}");
                    }
                    Err(CliError::ExitCode { code: if is_timeout { 1 } else { 2 }, message: String::new() })
                }
            }
        }
        JobAction::Logs { profile, farm_id, queue_id, job_id, session_id, session_action_id, limit, start_time, end_time, next_token, output, timestamp_format } => {
            let config = setup_config(profile, farm_id, queue_id, job_id, false, &["farm_id", "queue_id", "job_id"])?;
            let farm = get(&config, "defaults.farm_id");
            let queue = get(&config, "defaults.queue_id");
            let is_json = output.eq_ignore_ascii_case("json");

            let job = get(&config, "defaults.job_id");

            // Validate --session-action-id format early (before API calls)
            let (mut resolved_session_id_owned, mut action_start, mut action_end) = (None, None, None);
            if let Some(ref said) = session_action_id {
                let derived = parse_session_action_id(said)?;
                if let Some(ref explicit_sid) = session_id {
                    if *explicit_sid != derived {
                        return Err(CliError::Operation(format!(
                            "Session ID mismatch: --session-id '{}' does not match \
                             session ID '{}' derived from --session-action-id '{}'",
                            explicit_sid, derived, said
                        )));
                    }
                }
                resolved_session_id_owned = Some(derived);
            }

            let dl = session::deadline_client(Some(&config)).await;

            let job_resp = dl.get_job().farm_id(&farm).queue_id(&queue).job_id(&job)
                .send().await
                .map_err(|e| CliError::Operation(format!("Failed to get job: {}", client::format_sdk_error(&e))))?;
            let job_name = job_resp.name();

            // Get session action details for time bounds (after validation)
            if let Some(ref said) = session_action_id {
                let sa = dl.get_session_action()
                    .farm_id(&farm).queue_id(&queue).job_id(&job).session_action_id(said)
                    .send().await
                    .map_err(|e| CliError::Operation(format!(
                        "Session action '{}' not found in job '{}':\n{}", said, job, client::format_sdk_error(&e)
                    )))?;
                let sa_start = sa.started_at().map(|dt| responses::format_datetime(dt));
                if sa_start.is_none() {
                    return Err(CliError::Operation(format!(
                        "Session action '{}' has not started yet. No logs are available.", said
                    )));
                }
                action_start = sa_start;
                action_end = sa.ended_at().map(|dt| responses::format_datetime(dt));
            }

            let sid = resolved_session_id_owned.as_deref().or(session_id.as_deref());

            // Use action time bounds if available, otherwise use explicit start/end
            let start = action_start.as_deref().or(start_time.as_deref()).and_then(|s| {
                chrono::DateTime::parse_from_rfc3339(&s.replace(' ', "T").replace("+00:00", "Z").replace('Z', "+00:00"))
                    .ok()
                    .map(|dt| dt.with_timezone(&chrono::Utc))
            });
            let end = action_end.as_deref().or(end_time.as_deref()).and_then(|s| {
                chrono::DateTime::parse_from_rfc3339(&s.replace(' ', "T").replace("+00:00", "Z").replace('Z', "+00:00"))
                    .ok()
                    .map(|dt| dt.with_timezone(&chrono::Utc))
            });

            let (result, auto_select) = log_retrieval::get_session_logs(
                &farm, &queue, sid, Some(&job), limit, start, end,
                next_token.as_deref(), Some(&config),
            ).await.map_err(|e| CliError::Operation(format!("{e}")))?;

            // Resolve the actual session ID (may have been auto-selected)
            let resolved_session_id = match &auto_select {
                SessionAutoSelect::OnlySession(id) | SessionAutoSelect::LatestSession(id) => id.as_str(),
                SessionAutoSelect::Provided => session_id.as_deref().unwrap_or(&result.log_stream),
            };

            // Get session start time for timestamp formatting (needed for relative mode)
            let reference_start = {
                let sess = dl.get_session().farm_id(&farm).queue_id(&queue).job_id(&job).session_id(resolved_session_id)
                    .send().await
                    .map_err(|e| CliError::Operation(format!("Failed to get session: {}", client::format_sdk_error(&e))))?;
                let s = responses::format_datetime(&sess.started_at);
                chrono::DateTime::parse_from_rfc3339(&s.replace(' ', "T").replace("+00:00", "Z").replace('Z', "+00:00"))
                    .ok()
            };

            // Build timestamp formatter
            let ts_fmt = match timestamp_format.to_lowercase().as_str() {
                "local" => crate::common::TimestampFormat::Local,
                "relative" => {
                    let reference = reference_start.unwrap_or_else(|| {
                        chrono::Utc::now().fixed_offset()
                    });
                    crate::common::TimestampFormat::new_relative(reference)
                }
                _ => crate::common::TimestampFormat::Utc,
            };

            // Print auto-selection message then header (non-JSON only, matching Python order)
            if !is_json {
                match &auto_select {
                    SessionAutoSelect::OnlySession(id) => {
                        println!("Using the only available session: {id}");
                    }
                    SessionAutoSelect::LatestSession(id) => {
                        println!("Using the latest session: {id}");
                    }
                    SessionAutoSelect::Provided => {}
                }
                println!("Retrieving logs for {} from log group /aws/deadline/{farm}/{queue}...",
                    if session_action_id.is_some() {
                        format!("session action {}", session_action_id.as_deref().unwrap())
                    } else {
                        format!("session {}", result.log_stream)
                    });
                println!("Job ID: {job}");
                println!("Job Name: {job_name}");

                // Show session action time bounds if available
                if let (Some(sa_start), Some(_said)) = (&action_start, &session_action_id) {
                    println!("Session action start: {sa_start}");
                    if let Some(sa_end) = &action_end {
                        println!("Session action end: {sa_end}");
                        // Parse and compute duration
                        if let (Ok(start_dt), Ok(end_dt)) = (
                            chrono::DateTime::parse_from_rfc3339(&sa_start.replace(' ', "T").replace("+00:00", "Z").replace('Z', "+00:00")),
                            chrono::DateTime::parse_from_rfc3339(&sa_end.replace(' ', "T").replace("+00:00", "Z").replace('Z', "+00:00")),
                        ) {
                            let duration = end_dt.signed_duration_since(start_dt);
                            let secs = duration.num_seconds();
                            let micros = duration.num_microseconds().unwrap_or(0) % 1_000_000;
                            println!("Session action duration: {}:{:02}:{:02}.{:06}",
                                secs / 3600, (secs % 3600) / 60, secs % 60, micros);
                        }
                    }
                }
            }

            if is_json {
                let response = serde_json::json!({
                    "jobId": job,
                    "jobName": job_name,
                    "events": result.events.iter().map(|e| {
                        let ts_fixed = e.timestamp.fixed_offset();
                        serde_json::json!({
                            "timestamp": ts_fmt.format(&ts_fixed),
                            "message": e.message,
                            "ingestionTime": e.ingestion_time.map(|t| ts_fmt.format(&t.fixed_offset())),
                            "eventId": e.event_id,
                        })
                    }).collect::<Vec<_>>(),
                    "count": result.count,
                    "nextToken": result.next_token,
                    "logGroup": result.log_group,
                    "logStream": result.log_stream,
                });
                println!("{}", serde_json::to_string_pretty(&response).unwrap());
            } else {
                // Show reference time for relative format
                if let crate::common::TimestampFormat::Relative { ref reference } = ts_fmt {
                    println!("Logs relative to start time: {}", reference.to_rfc3339());
                }

                println!();
                if result.events.is_empty() {
                    println!("No logs found for the specified session.");
                } else {
                    for event in &result.events {
                        let ts_fixed = event.timestamp.fixed_offset();
                        let ts = ts_fmt.format(&ts_fixed);
                        println!("[{ts}] {}", event.message);
                    }
                    println!("\nRetrieved {} log events.", result.count);
                }
                if let Some(ref token) = result.next_token {
                    println!("More logs are available. Use --next-token \"{token}\" to retrieve the next page.");
                }
            }
            Ok(())
        }
        JobAction::Cancel { profile, farm_id, queue_id, job_id, mark_as, yes } => {
            let config = setup_config(profile, farm_id, queue_id, job_id, yes, &["farm_id", "queue_id", "job_id"])?;
            let farm = get(&config, "defaults.farm_id");
            let queue = get(&config, "defaults.queue_id");
            let job_id = get(&config, "defaults.job_id");
            let mark_as = mark_as.to_uppercase();
            const VALID_MARK_AS: &[&str] = &["SUSPENDED", "CANCELED", "FAILED", "SUCCEEDED"];
            if !VALID_MARK_AS.contains(&mark_as.as_str()) {
                return Err(CliError::ExitCode {
                    code: 2,
                    message: format!(
                        "Invalid value for --mark-as: {mark_as}. Valid values: {}",
                        VALID_MARK_AS.join(", ")
                    ),
                });
            }
            let auto_accept = is_auto_accept(&config);

            let job = match session::deadline_client(Some(&config)).await
                .get_job().farm_id(&farm).queue_id(&queue).job_id(&job_id)
                .send().await {
                Ok(j) => j,
                Err(e) => {
                    let err_str = client::format_sdk_error(&e);
                    let suggestion = suggest_resources_on_client_error(
                        &err_str, "GetJob", Some(&farm), Some(&queue), None, Some(&config),
                    ).await;
                    return Err(CliError::Operation(format!(
                        "Failed to get Job from Deadline:\n{err_str}{suggestion}"
                    )));
                }
            };

            // Filter taskRunStatusCounts to non-zero entries (sorted for deterministic output)
            let mut counts = serde_json::Map::new();
            if let Some(m) = job.task_run_status_counts() {
                let mut sorted: Vec<_> = m.iter().filter(|(_, v)| **v != 0).collect();
                sorted.sort_by_key(|(k, _)| k.as_str());
                for (k, v) in sorted {
                    counts.insert(k.as_str().to_string(), serde_json::json!(v));
                }
            }

            // Build filtered summary
            let mut summary = serde_json::Map::new();
            summary.insert("name".into(), serde_json::json!(job.name()));
            summary.insert("jobId".into(), serde_json::json!(job.job_id()));
            if let Some(s) = job.task_run_status() {
                summary.insert("taskRunStatus".into(), serde_json::json!(s.as_str()));
            }
            summary.insert("taskRunStatusCounts".into(), serde_json::Value::Object(counts));
            summary.insert("startedAt".into(), serde_json::json!(job.started_at().map(|d| responses::format_datetime(d)).unwrap_or_default()));
            summary.insert("endedAt".into(), serde_json::json!(job.ended_at().map(|d| responses::format_datetime(d)).unwrap_or_default()));
            summary.insert("createdBy".into(), serde_json::json!(job.created_by()));
            summary.insert("createdAt".into(), serde_json::json!(responses::format_datetime(job.created_at())));
            println!("{}", crate::common::cli_object_repr(&serde_json::Value::Object(summary)));

            if !auto_accept {
                let msg = if mark_as == "CANCELED" {
                    "Are you sure you want to cancel this job?".to_string()
                } else {
                    format!("Are you sure you want to cancel this job and mark its taskRunStatus as {mark_as}?")
                };
                eprint!("{msg} [y/n]: ");
                loop {
                    let mut input = String::new();
                    let bytes = std::io::stdin().read_line(&mut input).unwrap_or(0);
                    if bytes == 0 {
                        println!("Job not canceled.");
                        return Err(CliError::ExitCode { code: 1, message: String::new() });
                    }
                    match input.trim().to_lowercase().as_str() {
                        "y" | "yes" => break,
                        "n" | "no" => {
                            println!("Job not canceled.");
                            return Err(CliError::ExitCode { code: 1, message: String::new() });
                        }
                        _ => {
                            eprintln!("Error: invalid input");
                            eprint!("{msg} [y/n]: ");
                        }
                    }
                }
            }

            if mark_as == "CANCELED" {
                println!("Canceling job...");
            } else {
                println!("Canceling job and marking as {mark_as}...");
            }
            let dl = session::deadline_client(Some(&config)).await;
            let status: aws_sdk_deadline::types::JobTargetTaskRunStatus = mark_as.as_str().into();
            dl.update_job()
                .farm_id(&farm).queue_id(&queue).job_id(&job_id)
                .target_task_run_status(status)
                .send().await
                .map_err(|e| CliError::Operation(format!("Failed to update job:\n{}", client::format_sdk_error(&e))))?;
            Ok(())
        }
        JobAction::RequeueTasks { profile, farm_id, queue_id, job_id, run_status, yes } => {
            let config = setup_config(profile, farm_id, queue_id, job_id, yes, &["farm_id", "queue_id", "job_id"])?;
            let farm = get(&config, "defaults.farm_id");
            let queue = get(&config, "defaults.queue_id");
            let job_id = get(&config, "defaults.job_id");
            let auto_accept = is_auto_accept(&config);

            let run_status_set: std::collections::HashSet<String> = if run_status.is_empty() {
                ["SUSPENDED", "CANCELED", "FAILED"].iter().map(|s| s.to_string()).collect()
            } else {
                run_status.iter().map(|s| s.to_uppercase()).collect()
            };
            const VALID_RUN_STATUSES: &[&str] = &["SUSPENDED", "CANCELED", "FAILED", "SUCCEEDED", "NOT_COMPATIBLE"];
            for status in &run_status_set {
                if !VALID_RUN_STATUSES.contains(&status.as_str()) {
                    return Err(CliError::ExitCode {
                        code: 2,
                        message: format!(
                            "Invalid value for --run-status: {status}. Valid values: {}",
                            VALID_RUN_STATUSES.join(", ")
                        ),
                    });
                }
            }

            let job = match session::deadline_client(Some(&config)).await
                .get_job().farm_id(&farm).queue_id(&queue).job_id(&job_id)
                .send().await {
                Ok(j) => j,
                Err(e) => {
                    let err_str = client::format_sdk_error(&e);
                    let suggestion = suggest_resources_on_client_error(
                        &err_str, "GetJob", Some(&farm), Some(&queue), None, Some(&config),
                    ).await;
                    return Err(CliError::Operation(format!(
                        "Failed to get Job from Deadline:\n{err_str}{suggestion}"
                    )));
                }
            };

            println!("Job: {} ({})", job.name(), job.job_id());

            // Print taskRunStatusCounts (non-zero, keys uppercased, sorted)
            let mut counts_map = serde_json::Map::new();
            if let Some(m) = job.task_run_status_counts() {
                let mut sorted: Vec<_> = m.iter().filter(|(_, v)| **v != 0).collect();
                sorted.sort_by_key(|(k, _)| k.as_str());
                for (k, v) in sorted {
                    counts_map.insert(k.as_str().to_uppercase(), serde_json::json!(v));
                }
            }
            println!("{}", crate::common::cli_object_repr(&serde_json::json!({"taskRunStatusCounts": counts_map})));

            let sorted_statuses: Vec<&String> = {
                let mut v: Vec<&String> = run_status_set.iter().collect();
                v.sort();
                v
            };
            println!("Requeuing all tasks with run status among: {}", sorted_statuses.iter().map(|s| s.as_str()).collect::<Vec<_>>().join(", "));

            let (total_to_requeue, summary_by_status) = count_and_summarize(Some(&counts_map), &run_status_set);

            if total_to_requeue == 0 {
                println!("No tasks to requeue.");
                return Ok(());
            }

            if auto_accept {
                println!("Estimated {total_to_requeue} total tasks ({summary_by_status}) to requeue.");
            } else {
                println!("This action will requeue an estimated {total_to_requeue} total tasks ({summary_by_status})");
                eprint!("Are you sure you want to requeue these tasks? [y/n]: ");
                loop {
                    let mut input = String::new();
                    let bytes = std::io::stdin().read_line(&mut input).unwrap_or(0);
                    if bytes == 0 {
                        println!("No tasks were requeued.");
                        return Err(CliError::ExitCode { code: 1, message: String::new() });
                    }
                    match input.trim().to_lowercase().as_str() {
                        "y" | "yes" => break,
                        "n" | "no" => {
                            println!("No tasks were requeued.");
                            return Err(CliError::ExitCode { code: 1, message: String::new() });
                        }
                        _ => {
                            eprintln!("Error: invalid input");
                            eprint!("Are you sure you want to requeue these tasks? [y/n]: ");
                        }
                    }
                }
                println!("Requeuing tasks...");
            }

            let mut total_requeued: i64 = 0;

            let dl = session::deadline_client(Some(&config)).await;
            let steps_pages = client::collect_paginated(
                dl.list_steps().farm_id(&farm).queue_id(&queue).job_id(&job_id)
                    .into_paginator().send()
            ).await
                .map_err(|e| CliError::Operation(format!("Failed to list steps:\n{e}")))?;

            for page in &steps_pages {
                for step in page.steps() {
                    let step_id = step.step_id();
                    let step_name = step.name();
                    println!("\nStep: {step_name} ({step_id})");

                    let step_counts = step.task_run_status_counts();
                    let step_counts_map: serde_json::Map<String, serde_json::Value> = step_counts.iter()
                        .map(|(k, v)| (k.as_str().to_string(), serde_json::Value::Number((*v).into())))
                        .collect();
                    let (step_to_requeue, step_summary) = count_and_summarize(Some(&step_counts_map), &run_status_set);

                    if step_to_requeue == 0 {
                        println!("  Step has no tasks to requeue.");
                        continue;
                    }
                    println!("  Requeuing an estimated {step_to_requeue} total tasks ({step_summary})...");

                    let tasks_pages = client::collect_paginated(
                        dl.list_tasks().farm_id(&farm).queue_id(&queue).job_id(&job_id).step_id(step_id)
                            .into_paginator().send()
                    ).await
                        .map_err(|e| CliError::Operation(format!("Failed to list tasks:\n{e}")))?;

                    for tpage in &tasks_pages {
                        for task in tpage.tasks() {
                            let status = task.run_status().as_str();
                            if !run_status_set.contains(&status.to_uppercase()) {
                                continue;
                            }
                            let task_id = task.task_id();
                            let params = task.parameters();
                            let task_summary = if let Some(p) = params.filter(|p| !p.is_empty()) {
                                let mut param_pairs: Vec<_> = p.iter().map(|(name, val)| {
                                    let extracted = match val {
                                        aws_sdk_deadline::types::TaskParameterValue::Int(i) => i.clone(),
                                        aws_sdk_deadline::types::TaskParameterValue::Float(f) => f.clone(),
                                        aws_sdk_deadline::types::TaskParameterValue::String(s) => s.clone(),
                                        aws_sdk_deadline::types::TaskParameterValue::Path(p) => p.clone(),
                                        _ => String::new(),
                                    };
                                    format!("{name}={extracted}")
                                }).collect();
                                param_pairs.sort();
                                format!("{} ({task_id})", param_pairs.join(","))
                            } else {
                                task_id.to_string()
                            };
                            println!("    {status} {task_summary}");

                            session::deadline_client(Some(&config)).await
                                .update_task()
                                .farm_id(&farm).queue_id(&queue).job_id(&job_id)
                                .step_id(step_id).task_id(task_id)
                                .target_run_status(aws_sdk_deadline::types::TaskTargetRunStatus::Pending)
                                .customize()
                                .config_override(aws_sdk_deadline::config::Builder::default()
                                    .retry_config(aws_config::retry::RetryConfig::adaptive().with_max_attempts(5)))
                                .send().await
                                .map_err(|e| CliError::Operation(format!("Failed to update task:\n{}", client::format_sdk_error(&e))))?;
                            total_requeued += 1;
                        }
                    }
                }
            }

            println!("\nRequeued a total of {total_requeued} tasks.");
            Ok(())
        }
        JobAction::Search { profile, farm_id, queue_id, filter_expressions, sort_expressions, page_size, item_offset } => {
            let config = setup_config(profile, farm_id, queue_id, None, false, &["farm_id", "queue_id"])?;
            let farm = get(&config, "defaults.farm_id");
            let queue = get(&config, "defaults.queue_id");

            let filter_json = parse_json_or_file_arg(filter_expressions.as_deref())?;
            let sort_json = parse_json_or_file_arg(sort_expressions.as_deref())?;

            let resp = match search_jobs_call(
                &farm, &[queue.as_str()], item_offset, page_size,
                filter_json.as_ref(), sort_json.as_ref(),
                Some(&config),
            ).await {
                Ok(r) => r,
                Err(e) => {
                    let suggestion = suggest_resources_on_client_error(
                        &e.to_string(), "SearchJobs", Some(&farm), Some(&queue), None, Some(&config),
                    ).await;
                    return Err(CliError::Operation(format!(
                        "Failed to search Jobs from Deadline:\n{e}{suggestion}"
                    )));
                }
            };
            print_search_jobs_output(&resp, item_offset);
            Ok(())
        }
        JobAction::TraceSchedule {
            profile, farm_id, queue_id, job_id,
            verbose, trace_format, trace_file,
        } => {
            run_trace_schedule(profile, farm_id, queue_id, job_id, verbose, trace_format, trace_file).await
        }
        JobAction::DownloadOutput {
            profile, farm_id, queue_id, job_id, step_id, task_id,
            conflict_resolution, yes, output,
        } => {
            let is_json = output.eq_ignore_ascii_case("json");

            // Validate --task-id requires --step-id
            if task_id.is_some() && step_id.is_none() {
                return Err(CliError::ExitCode {
                    code: 2,
                    message: "Missing option '--step-id' required with '--task-id'".into(),
                });
            }

            let config = setup_config(profile, farm_id, queue_id, job_id, yes, &["farm_id", "queue_id", "job_id"])?;
            let farm = get(&config, "defaults.farm_id");
            let queue_id_val = get(&config, "defaults.queue_id");
            let job_id_val = get(&config, "defaults.job_id");

            let result = download_output_impl(
                &config, &farm, &queue_id_val, &job_id_val,
                step_id.as_deref(), task_id.as_deref(),
                conflict_resolution, is_json, is_auto_accept(&config),
            ).await;

            match result {
                Ok(()) => Ok(()),
                Err(e) if is_json => {
                    let error_one_liner = e.to_string().replace('\n', ". ");
                    println!("{}", serde_json::json!({"messageType": "error", "value": error_one_liner}));
                    std::process::exit(1);
                }
                Err(e) => Err(e),
            }
        }
    }
}

/// Read auto_accept from config (already set by apply_cli_options_to_config when --yes).
fn is_auto_accept(config: &deadline_config::ini::IniConfig) -> bool {
    config_file::get_setting("settings.auto_accept", config)
        .ok()
        .and_then(|v| config_file::str2bool(&v).ok())
        .unwrap_or(false)
}

/// Count matching tasks and build a summary string like "2 FAILED tasks, 1 CANCELED tasks".
fn count_and_summarize(
    counts: Option<&serde_json::Map<String, serde_json::Value>>,
    statuses: &std::collections::HashSet<String>,
) -> (i64, String) {
    let total = counts.map_or(0, |obj| {
        obj.iter()
            .filter(|(k, _)| statuses.contains(&k.to_uppercase()))
            .filter_map(|(_, v)| v.as_i64())
            .sum()
    });
    let summary = counts.map_or(String::new(), |obj| {
        let mut items: Vec<_> = obj.iter()
            .filter(|(k, v)| statuses.contains(&k.to_uppercase()) && v.as_i64().unwrap_or(0) != 0)
            .map(|(k, v)| format!("{} {} tasks", v.as_i64().unwrap_or(0), k.to_uppercase()))
            .collect();
        items.sort();
        items.join(", ")
    });
    (total, summary)
}

/// Estimate remaining job time from task progress and elapsed time.
/// Returns None if not computable (no startedAt, no completed tasks, or no remaining tasks).
/// Matches Python's `_estimate_remaining_time` in `_job_helpers.py`.
fn estimate_remaining_time(job: &serde_json::Value) -> Option<String> {
    let counts = job.get("taskRunStatusCounts")?.as_object()?;
    let started_at = job.get("startedAt").and_then(|v| v.as_str())?;

    let started = chrono::DateTime::parse_from_rfc3339(
        &started_at.replace(' ', "T").replace("+00:00", "Z").replace('Z', "+00:00"),
    ).ok()?;

    let completed = ["SUCCEEDED", "FAILED", "CANCELED"].iter()
        .filter_map(|s| counts.get(*s).and_then(|v| v.as_i64()))
        .sum::<i64>();
    let in_progress = ["RUNNING", "STARTING", "ASSIGNED"].iter()
        .filter_map(|s| counts.get(*s).and_then(|v| v.as_i64()))
        .sum::<i64>();
    let pending = ["PENDING", "READY", "SCHEDULED"].iter()
        .filter_map(|s| counts.get(*s).and_then(|v| v.as_i64()))
        .sum::<i64>();

    if completed == 0 || (pending == 0 && in_progress == 0) {
        return None;
    }

    let elapsed = (chrono::Utc::now() - started.with_timezone(&chrono::Utc)).num_seconds() as f64;
    if elapsed <= 0.0 {
        return None;
    }

    let remaining_secs = (elapsed / completed as f64) * (in_progress + pending) as f64;
    Some(format_duration(remaining_secs))
}

fn format_duration(seconds: f64) -> String {
    let secs = seconds as u64;
    if secs < 60 {
        return format!("{secs} seconds");
    }
    let minutes = secs / 60;
    if minutes < 60 {
        let s = if minutes != 1 { "s" } else { "" };
        return format!("{minutes} minute{s}");
    }
    let hours = minutes / 60;
    let mins = minutes % 60;
    let hs = if hours != 1 { "s" } else { "" };
    if mins == 0 {
        format!("{hours} hour{hs}")
    } else {
        let ms = if mins != 1 { "s" } else { "" };
        format!("{hours} hour{hs}, {mins} minute{ms}")
    }
}

/// Print full job details (used by `job get` in direct mode).
async fn print_job_details(farm: &str, queue: &str, job_id: &str, config: &IniConfig) -> Result<(), CliError> {
    match session::deadline_client(Some(config)).await
        .get_job().farm_id(farm).queue_id(queue).job_id(job_id)
        .send().await {
        Ok(output) => {
            let resp = JobResponse::from(output);
            let val = serde_json::to_value(&resp).map_err(|e| CliError::Operation(e.to_string()))?;
            println!("{}", crate::common::cli_object_repr(&val));
            let est = estimate_remaining_time(&val);
            println!("estimatedTimeRemaining: {}", est.as_deref().unwrap_or("N/A"));
            Ok(())
        }
        Err(e) => {
            let err_str = client::format_sdk_error(&e);
            let suggestion = suggest_resources_on_client_error(
                &err_str, "GetJob", Some(farm), Some(queue), None, Some(config),
            ).await;
            Err(CliError::Operation(format!(
                "Failed to get Job from Deadline:\n{err_str}{suggestion}"
            )))
        }
    }
}

/// Search for jobs matching a term. Single match → show details. Multiple → summary list.
async fn resolve_job_search(farm: &str, queue: &str, search_term: &str, config: &IniConfig) -> Result<(), CliError> {
    let filter = serde_json::json!({
        "filters": [{
            "searchTermFilter": {
                "searchTerm": search_term,
                "matchType": "CONTAINS"
            }
        }],
        "operator": "AND"
    });
    let resp = match search_jobs_call(
        farm, &[queue], 0, 5, Some(&filter), None, Some(config),
    ).await {
        Ok(r) => r,
        Err(e) => {
            return Err(CliError::Operation(format!("Failed to search jobs:\n{e}")));
        }
    };

    let jobs = resp.jobs();
    let total = resp.total_results() as i64;

    if jobs.is_empty() {
        println!("No jobs found matching \"{search_term}\"");
        return Ok(());
    }

    if total == 1 {
        let job_id = jobs[0].job_id().unwrap_or("");
        return print_job_details(farm, queue, job_id, config).await;
    }

    // Multiple results — show summary
    println!("Found {total} job(s) matching \"{search_term}\", showing most recent {}:\n", jobs.len());
    for job in jobs {
        let name = job.name().unwrap_or("");
        let name = truncate_middle(name, 80);
        let job_id = job.job_id().unwrap_or("");
        let status = job.task_run_status().map(|s| s.as_str()).unwrap_or("");
        let created = job.created_at().map(|dt| {
            let s = format_datetime(dt);
            chrono::DateTime::parse_from_rfc3339(&s.replace(' ', "T").replace("+00:00", "Z").replace('Z', "+00:00"))
                .map(|dt| dt.with_timezone(&chrono::Local).format("%Y-%m-%d %H:%M:%S %z").to_string())
                .unwrap_or_else(|_| s)
        }).unwrap_or_default();
        let counts = job.task_run_status_counts().map(|c| {
            c.iter().map(|(k, v)| (k.as_str().to_string(), serde_json::Value::Number((*v).into()))).collect::<serde_json::Map<String, serde_json::Value>>()
        });
        let task_summary = format_task_summary(counts.as_ref());

        println!("  {name}");
        println!("    {job_id}  {status:<12}  {created}");
        println!("    Tasks: {task_summary}");
        println!();
    }

    if total > jobs.len() as i64 {
        println!("  ... and {} more", total - jobs.len() as i64);
    }
    println!("\nTo get details, run: deadline job get --job-id <job-id>");
    Ok(())
}

fn truncate_middle(text: &str, max_length: usize) -> String {
    if text.len() <= max_length { return text.to_string(); }
    let keep = max_length - 3;
    let start = (keep * 2) / 3;
    let end = keep - start;
    format!("{}...{}", &text[..start], &text[text.len() - end..])
}

fn format_task_summary(counts: Option<&serde_json::Map<String, serde_json::Value>>) -> String {
    let Some(counts) = counts else { return "no tasks".into() };
    let get = |keys: &[&str]| -> i64 {
        keys.iter().filter_map(|k| counts.get(*k).and_then(|v| v.as_i64())).sum()
    };
    let mut parts = Vec::new();
    let ready = get(&["READY"]);
    let running = get(&["RUNNING", "STARTING", "ASSIGNED", "SCHEDULED"]);
    let interrupting = get(&["INTERRUPTING"]);
    let pending = get(&["PENDING"]);
    let succeeded = get(&["SUCCEEDED"]);
    let failed = get(&["FAILED"]);
    let canceled = get(&["CANCELED"]);
    let suspended = get(&["SUSPENDED"]);
    let not_compatible = get(&["NOT_COMPATIBLE"]);
    if ready > 0 { parts.push(format!("{ready} ready")); }
    if running > 0 { parts.push(format!("{running} running")); }
    if interrupting > 0 { parts.push(format!("{interrupting} interrupting")); }
    if pending > 0 { parts.push(format!("{pending} pending")); }
    if suspended > 0 { parts.push(format!("{suspended} suspended")); }
    if succeeded > 0 { parts.push(format!("{succeeded} succeeded")); }
    if failed > 0 { parts.push(format!("{failed} failed")); }
    if canceled > 0 { parts.push(format!("{canceled} canceled")); }
    if not_compatible > 0 { parts.push(format!("{not_compatible} not compatible")); }
    if parts.is_empty() { "no tasks".into() } else { parts.join(", ") }
}

/// Print job list output (shared between `job list` and `job search`).
/// Format an AWS DateTime to the display format matching Python CLI output.
fn format_datetime(dt: &aws_sdk_deadline::primitives::DateTime) -> String {
    // Format as "YYYY-MM-DD HH:MM:SS+00:00" to match Python/ResponseBodyCapture output
    dt.fmt(aws_sdk_deadline::primitives::DateTimeFormat::DateTimeWithOffset)
        .unwrap_or_default()
        .replace('T', " ")
        // Remove fractional seconds if present (e.g. ".000Z" → "+00:00")
        .replace('Z', "+00:00")
}

/// Call SearchJobs directly via the SDK. Returns typed output.
async fn search_jobs_call(
    farm_id: &str,
    queue_ids: &[&str],
    item_offset: i32,
    page_size: i32,
    filter_expressions: Option<&serde_json::Value>,
    sort_expressions: Option<&serde_json::Value>,
    config: Option<&IniConfig>,
) -> Result<aws_sdk_deadline::operation::search_jobs::SearchJobsOutput, deadline_api::errors::DeadlineError> {
    use deadline_api::errors::DeadlineError;

    let filter = filter_expressions.map(api::build_filter_expressions).transpose()?;
    let sort = sort_expressions.map(api::build_sort_expressions).transpose()?;
    let client = deadline_api::session::deadline_client(config).await;

    let mut req = client
        .search_jobs()
        .farm_id(farm_id)
        .set_queue_ids(Some(queue_ids.iter().map(|s| s.to_string()).collect()))
        .item_offset(item_offset)
        .page_size(page_size);

    if let Some(ref f) = filter {
        req = req.filter_expressions(f.clone());
    }

    if let Some(ref sorts) = sort {
        for s in sorts {
            req = req.sort_expressions(s.clone());
        }
    } else {
        req = req.sort_expressions(
            aws_sdk_deadline::types::SearchSortExpression::FieldSort(
                aws_sdk_deadline::types::FieldSortExpression::builder()
                    .name("CREATED_AT")
                    .sort_order(aws_sdk_deadline::types::SortOrder::Descending)
                    .build()
                    .unwrap(),
            ),
        );
    }

    req.send().await.map_err(|e| DeadlineError::OperationError(deadline_api::api::format_sdk_error(&e)))
}

/// Print SearchJobs output in the standard job list format.
fn print_search_jobs_output(resp: &aws_sdk_deadline::operation::search_jobs::SearchJobsOutput, item_offset: i32) {
    let total = resp.total_results() as i64;
    let jobs = resp.jobs();

    let structured: Vec<serde_json::Value> = jobs.iter().map(|j| {
        let mut m = serde_json::Map::new();
        m.insert("name".into(), serde_json::Value::String(j.name().unwrap_or("").to_string()));
        m.insert("jobId".into(), serde_json::Value::String(j.job_id().unwrap_or("").to_string()));
        m.insert("taskRunStatus".into(), serde_json::Value::String(j.task_run_status().map(|s| s.as_str()).unwrap_or("").to_string()));
        m.insert("startedAt".into(), serde_json::Value::String(j.started_at().map(|d| format_datetime(d)).unwrap_or_default()));
        m.insert("endedAt".into(), serde_json::Value::String(j.ended_at().map(|d| format_datetime(d)).unwrap_or_default()));
        m.insert("createdBy".into(), serde_json::Value::String(j.created_by().unwrap_or("").to_string()));
        m.insert("createdAt".into(), serde_json::Value::String(j.created_at().map(|d| format_datetime(d)).unwrap_or_default()));
        // Estimate remaining time from task run status counts
        let counts_val = j.task_run_status_counts().map(|counts| {
            let map: serde_json::Map<String, serde_json::Value> = counts.iter()
                .map(|(k, v)| (k.as_str().to_string(), serde_json::Value::Number((*v).into())))
                .collect();
            serde_json::Value::Object(map)
        }).unwrap_or(serde_json::Value::Null);
        let fake_job = serde_json::json!({"taskRunStatusCounts": counts_val, "startedAt": j.started_at().map(|d| format_datetime(d)).unwrap_or_default()});
        m.insert("estimatedTimeRemaining".into(),
            serde_json::Value::String(
                estimate_remaining_time(&fake_job).unwrap_or_else(|| "N/A".into())
            ));
        serde_json::Value::Object(m)
    }).collect();

    println!("Displaying {} of {} Jobs starting at {}", structured.len(), total, item_offset);
    println!();
    println!("{}", crate::common::cli_object_repr(&serde_json::json!(structured)));
}

/// Parse a CLI argument that can be inline JSON or `file://path`.
fn parse_json_or_file_arg(arg: Option<&str>) -> Result<Option<serde_json::Value>, CliError> {
    let Some(arg) = arg else { return Ok(None) };
    let content = if let Some(path) = arg.strip_prefix("file://") {
        std::fs::read_to_string(path)
            .map_err(|e| CliError::Operation(format!("Failed to read {path}: {e}")))?
    } else {
        arg.to_string()
    };
    let val: serde_json::Value = serde_json::from_str(&content)
        .map_err(|e| CliError::Operation(format!("Invalid JSON: {e}")))?;
    Ok(Some(val))
}

// ---------------------------------------------------------------------------
// download-output implementation
// ---------------------------------------------------------------------------

/// Format the start message for download-output.
fn download_start_message(
    job_name: &str,
    step_name: Option<&str>,
    task_parameters: Option<&serde_json::Value>,
    is_json: bool,
) -> String {
    if is_json {
        serde_json::json!({"messageType": "title", "value": job_name}).to_string()
    } else if let Some(sn) = step_name {
        if let Some(params) = task_parameters {
            let param_str = if let Some(obj) = params.as_object() {
                if obj.is_empty() {
                    "{}".to_string()
                } else {
                    let inner: Vec<String> = obj.iter().map(|(k, v)| {
                        let val = v.as_object()
                            .and_then(|m| m.values().next())
                            .and_then(|v| v.as_str())
                            .unwrap_or("");
                        format!("{k}={val}")
                    }).collect();
                    format!("{{{}}}", inner.join(","))
                }
            } else {
                "{}".to_string()
            };
            format!("Downloading output from Job '{job_name}' Step '{sn}' Task {param_str}")
        } else {
            format!("Downloading output from Job '{job_name}' Step '{sn}'")
        }
    } else {
        format!("Downloading output from Job '{job_name}'")
    }
}

/// Format the "no output" message.
fn no_output_message(is_json: bool) -> String {
    let msg = "There are no output files available for download at this moment. \
               Please verify that the Job/Step/Task you are trying to download \
               output from has completed successfully.";
    if is_json {
        serde_json::json!({"messageType": "summary", "value": msg}).to_string()
    } else {
        msg.to_string()
    }
}

/// Check if a path exceeds Windows MAX_PATH and warn.
#[cfg(windows)]
fn check_windows_long_paths(output_paths_by_root: &std::collections::HashMap<String, Vec<String>>) {
    const WINDOWS_MAX_PATH_LENGTH: usize = 260;
    // Check if LongPathsEnabled registry key is set
    let long_paths_enabled = (|| -> bool {
        let hklm = winreg::RegKey::predef(winreg::enums::HKEY_LOCAL_MACHINE);
        let key = hklm.open_subkey(r"SYSTEM\CurrentControlSet\Control\FileSystem").ok()?;
        let val: u32 = key.get_value("LongPathsEnabled").ok()?;
        Some(val != 0)
    })().unwrap_or(false);

    if long_paths_enabled {
        return;
    }
    for (root, paths) in output_paths_by_root {
        for path in paths {
            if root.len() + path.len() >= WINDOWS_MAX_PATH_LENGTH {
                eprintln!(
                    "\nWARNING: Found downloaded file paths that exceed Windows path length limit. \
                     This may cause unexpected issues.\n\
                     For details and a fix using the registry, see: \
                     https://learn.microsoft.com/en-us/windows/win32/fileio/maximum-file-path-limitation\n"
                );
                return;
            }
        }
    }
}

#[cfg(not(windows))]
fn check_windows_long_paths(_output_paths_by_root: &std::collections::HashMap<String, Vec<String>>) {
    // No-op on non-Windows
}

/// Core implementation of `job download-output`.
pub(crate) async fn download_output_impl(
    config: &IniConfig,
    farm_id: &str,
    queue_id: &str,
    job_id: &str,
    step_id: Option<&str>,
    task_id: Option<&str>,
    conflict_resolution: Option<deadline_job_attachments::models::FileConflictResolution>,
    is_json: bool,
    auto_accept: bool,
) -> Result<(), CliError> {
    use deadline_job_attachments::download::OutputDownloader;
    use deadline_job_attachments::models::{FileConflictResolution, JobAttachmentS3Settings, PathFormat};
    use deadline_job_attachments::s3;
    use deadline_api::path_utils::{human_readable_file_size, summarize_path_list};

    // Get job
    let dl = session::deadline_client(Some(config)).await;
    let job = dl.get_job().farm_id(farm_id).queue_id(queue_id).job_id(job_id)
        .send().await
        .map_err(|e| CliError::Operation(format!("Failed to download output:\n{}", client::format_sdk_error(&e))))?;
    let job_name = job.name().to_string();

    // Get optional step/task
    let step_name = if let Some(sid) = step_id {
        let step = dl.get_step().farm_id(farm_id).queue_id(queue_id).job_id(job_id).step_id(sid)
            .send().await
            .map_err(|e| CliError::Operation(format!("Failed to download output:\n{}", client::format_sdk_error(&e))))?;
        Some(step.name().to_string())
    } else {
        None
    };

    let task_params;
    let session_action_id;
    if let (Some(sid), Some(tid)) = (step_id, task_id) {
        let task = dl.get_task().farm_id(farm_id).queue_id(queue_id).job_id(job_id).step_id(sid).task_id(tid)
            .send().await
            .map_err(|e| CliError::Operation(format!("Failed to download output:\n{}", client::format_sdk_error(&e))))?;
        task_params = task.parameters.as_ref().map(|m| {
            let obj: serde_json::Map<String, serde_json::Value> = m.iter()
                .map(|(k, v)| (k.clone(), deadline_api::type_conversions::task_parameter_value_to_value(v)))
                .collect();
            serde_json::Value::Object(obj)
        });
        session_action_id = task.latest_session_action_id.clone();
    } else {
        task_params = None;
        session_action_id = None;
    }

    // Print start message
    let empty_params = serde_json::json!({});
    let task_params_for_msg = if task_id.is_some() {
        task_params.as_ref().or(Some(&empty_params))
    } else {
        None
    };
    println!("{}", download_start_message(
        &job_name,
        step_name.as_deref(),
        task_params_for_msg,
        is_json,
    ));

    // Get queue for jobAttachmentSettings
    let queue = deadline_api::session::deadline_client(Some(config)).await
        .get_queue().farm_id(farm_id).queue_id(queue_id)
        .send().await
        .map_err(|e| CliError::Operation(format!("Failed to download output:\n{}", deadline_api::client::format_sdk_error(&e))))?;

    let attachment_settings = queue.job_attachment_settings()
        .ok_or_else(|| CliError::Operation(format!(
            "Queue '{}' does not have job attachments configured.",
            queue.display_name()
        )))?;

    let bucket = attachment_settings.s3_bucket_name();
    let prefix = attachment_settings.root_prefix();
    let s3_settings = JobAttachmentS3Settings {
        s3_bucket_name: bucket.to_string(),
        root_prefix: prefix.to_string(),
    };

    // Build S3 client with queue-scoped credentials
    let sdk_config = deadline_api::session::get_queue_scoped_config(
        farm_id, queue_id, Some(config),
    ).await.map_err(|e| CliError::Operation(format!("Failed to download output:\n{e}")))?;

    let s3_client = s3::build_s3_client(&sdk_config, Some(config));
    let account_id = s3::get_account_id(&sdk_config)
        .await
        .map_err(|e| CliError::Operation(format!("Failed to download output:\n{e}")))?;

    // Create OutputDownloader
    let mut downloader = OutputDownloader::new(
        s3_settings, farm_id, queue_id, job_id,
        step_id, task_id, session_action_id.as_deref(),
        s3_client, account_id,
    ).await.map_err(|e| CliError::Operation(format!("Failed to download output:\n{e}")))?;

    let output_paths = downloader.get_output_paths_by_root();

    // No output available
    if output_paths.is_empty() {
        println!("{}", no_output_message(is_json));
        return Ok(());
    }

    check_windows_long_paths(&output_paths);

    // F7: Build root_path_format_mapping from job attachments
    let mut root_path_format_mapping: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    if let Some(attachments) = job.attachments() {
        for manifest in attachments.manifests() {
            let root = manifest.root_path();
            let fmt = manifest.root_path_format().as_str();
            root_path_format_mapping.insert(root.to_string(), fmt.to_string());
        }
    }

    // F7: Cross-OS mismatch prompt — always runs, even with auto_accept
    let host_format = PathFormat::get_host_path_format_string();
    let asset_roots: Vec<String> = output_paths.keys().cloned().collect();
    for asset_root in &asset_roots {
        let root_format = root_path_format_mapping.get(asset_root).map(|s| s.as_str()).unwrap_or("");
        if !root_format.is_empty() && host_format != root_format {
            if is_json {
                println!("{}", serde_json::json!({"messageType": "path", "value": [asset_root]}));
                let mut line = String::new();
                std::io::stdin().read_line(&mut line).unwrap_or(0);
                let line = line.trim();
                if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(line) {
                    if let Some(vals) = parsed.get("value").and_then(|v| v.as_array()) {
                        if let Some(new_root) = vals.first().and_then(|v| v.as_str()) {
                            downloader.set_root_path(asset_root, new_root);
                        }
                    }
                }
            } else {
                let fmt_cap = format!("{}{}", &root_format[..1].to_uppercase(), &root_format[1..]);
                println!(
                    "This root path format does not match the operating system you're using. \
                     Where would you like to save the files?\n\
                     The location was {asset_root}, on {fmt_cap}."
                );
                print!("> Please enter a new root path: ");
                use std::io::Write;
                std::io::stdout().flush().ok();
                let mut new_root = String::new();
                std::io::stdin().read_line(&mut new_root).unwrap_or(0);
                let new_root = new_root.trim();
                let new_root = expand_tilde(new_root);
                if !new_root.is_empty() {
                    downloader.set_root_path(asset_root, &new_root);
                }
            }
        }
    }

    let mut output_paths = downloader.get_output_paths_by_root();

    // F7: Root editing loop — skipped when auto_accept
    if !auto_accept && !output_paths.is_empty() {
        if !is_json {
            loop {
                // Show summary
                let summary_lines: Vec<String> = output_paths.iter().map(|(dir, paths)| {
                    let count = paths.len();
                    let s = if count > 1 { "s" } else { "" };
                    format!("    {dir} ({count} file{s})")
                }).collect();
                println!("\nSummary of files to download:\n{}", summary_lines.join("\n"));

                // Show roots with indices
                let roots: Vec<String> = output_paths.keys().cloned().collect();
                println!("You are about to download files which may come from multiple root directories. Here are a list of the current root directories:");
                for (i, root) in roots.iter().enumerate() {
                    println!("[{i}] {root}");
                }

                print!("> Please enter the index of root directory to edit, y to proceed without changes, or n to cancel the download: ");
                use std::io::Write;
                std::io::stdout().flush().ok();
                let mut choice = String::new();
                if std::io::stdin().read_line(&mut choice).unwrap_or(0) == 0 {
                    break; // EOF
                }
                let choice = choice.trim();
                if choice == "n" {
                    println!("Output download canceled.");
                    return Ok(());
                } else if choice == "y" || choice.is_empty() {
                    break;
                } else if let Ok(idx) = choice.parse::<usize>() {
                    if idx < roots.len() {
                        print!("> Please enter the new root directory path, or press Enter to keep it unchanged: ");
                        std::io::stdout().flush().ok();
                        let mut new_root = String::new();
                        std::io::stdin().read_line(&mut new_root).unwrap_or(0);
                        let new_root = new_root.trim();
                        if !new_root.is_empty() && new_root != roots[idx] {
                            downloader.set_root_path(&roots[idx], new_root);
                            output_paths = downloader.get_output_paths_by_root();
                        }
                    }
                }
            }
        } else {
            // JSON mode: emit paths, read pathConfirm response
            let roots: Vec<String> = output_paths.keys().cloned().collect();
            println!("{}", serde_json::json!({"messageType": "path", "value": roots}));
            let mut line = String::new();
            std::io::stdin().read_line(&mut line).unwrap_or(0);
            let line = line.trim();
            if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(line) {
                if let Some(vals) = parsed.get("value").and_then(|v| v.as_array()) {
                    for (i, val) in vals.iter().enumerate() {
                        if let Some(new_root) = val.as_str() {
                            if i < roots.len() {
                                downloader.set_root_path(&roots[i], new_root);
                            }
                        }
                    }
                    output_paths = downloader.get_output_paths_by_root();
                }
            }
        }
    }

    check_windows_long_paths(&output_paths);

    // Build path summary for verbose output
    if !is_json {
        let all_paths: Vec<String> = output_paths.iter()
            .flat_map(|(root, paths)| {
                paths.iter().map(move |p| {
                    let full = std::path::PathBuf::from(root).join(p);
                    full.to_string_lossy().to_string()
                })
            })
            .collect();
        let path_refs: Vec<&str> = all_paths.iter().map(|s| s.as_str()).collect();
        println!("\nSummary of file paths to download:");
        let summary = summarize_path_list(&path_refs, 10, None);
        for line in summary.lines() {
            println!("  {line}");
        }
    }

    // Resolve conflict resolution — check for existing files if not explicitly set
    let resolution = match conflict_resolution {
        Some(r) => r,
        None => {
            // Check for conflicting files
            let mut conflicting: Vec<String> = Vec::new();
            for (root, paths) in &output_paths {
                for p in paths {
                    let full = std::path::PathBuf::from(root).join(p);
                    if full.is_file() {
                        conflicting.push(full.to_string_lossy().to_string());
                    }
                }
            }
            if !conflicting.is_empty() && !is_json {
                println!("\nThe following files already exist in your local directory:");
                for f in conflicting.iter().take(10) {
                    println!("        {f}");
                }
                if conflicting.len() > 10 {
                    println!("        ... and {} more", conflicting.len() - 10);
                }
                println!("Defaulting to Create a copy (appending '(1)' to conflicting files).");
            }
            let setting = config_file::get_setting("settings.conflict_resolution", config)
                .unwrap_or_default();
            match setting.to_uppercase().as_str() {
                "SKIP" => FileConflictResolution::Skip,
                "OVERWRITE" => FileConflictResolution::Overwrite,
                _ => FileConflictResolution::CreateCopy,
            }
        }
    };

    // Download with progress
    let progress_mgr = std::sync::Mutex::new(
        crate::common::ProgressBarManager::new(100, "Downloading Outputs"),
    );

    let download_summary = downloader.download_job_output(
        resolution,
        Some(Box::new(move |meta| {
            let new_progress = meta.progress as u64;
            progress_mgr.lock().unwrap().callback(new_progress);
            crate::common::should_continue()
        })),
    ).await.map_err(|e| CliError::Operation(format!("Failed to download output:\n{e}")))?;

    // Print summary
    if is_json {
        println!("{}", serde_json::json!({
            "messageType": "summary",
            "value": format!("Downloaded {} files", download_summary.stats.processed_files),
            "fileCount": download_summary.stats.processed_files,
            "files": download_summary.downloaded_files,
        }));
    } else {
        let paths_joined: String = download_summary.file_counts_by_root_directory.iter()
            .map(|(dir, count)| {
                let file_word = if *count > 1 { "files" } else { "file" };
                format!("{dir} ({count} {file_word})")
            })
            .collect::<Vec<_>>()
            .join("\n        ");
        println!(
            "Download Summary:\n\
             \x20   Downloaded {} files totaling {}.\n\
             \x20   Total download time of {} seconds at {}/s.\n\
             \x20   Download locations (total file counts):\n\
             \x20       {}",
            download_summary.stats.processed_files,
            human_readable_file_size(download_summary.stats.processed_bytes),
            format!("{:.5}", download_summary.stats.total_time),
            human_readable_file_size(download_summary.stats.transfer_rate as u64),
            paths_joined,
        );
    }
    println!();

    Ok(())
}

// ---------------------------------------------------------------------------
// trace-schedule
// ---------------------------------------------------------------------------

/// Per-item error codes that are transient and worth retrying.
const TRANSIENT_CODES: &[&str] = &["InternalServerErrorException", "ThrottlingException"];
const MAX_BATCH_SIZE: usize = 100;

/// Generic batch-get with chunking and retry. Calls `send_batch` for each
/// chunk of up to 100 identifiers, retries transient per-item errors with
/// exponential backoff, and collects terminal errors.
async fn batch_get<F, Fut>(
    send_batch: F,
    identifiers: Vec<serde_json::Value>,
    key_fn: fn(&serde_json::Value) -> Option<String>,
    items_field: &str,
    id_fields: &[&str],
    max_attempts: usize,
) -> Result<(std::collections::HashMap<String, serde_json::Value>, Vec<serde_json::Value>), CliError>
where
    F: Fn(Vec<serde_json::Value>) -> Fut,
    Fut: std::future::Future<Output = Result<serde_json::Value, deadline_api::errors::DeadlineError>>,
{
    let mut remaining = identifiers;
    let mut results = std::collections::HashMap::new();
    let mut terminal = Vec::new();

    for attempt in 0..max_attempts {
        let mut next_round = Vec::new();
        for chunk in remaining.chunks(MAX_BATCH_SIZE) {
            let response = send_batch(chunk.to_vec()).await
                .map_err(|e| CliError::Operation(e.to_string()))?;
            if let Some(items) = response[items_field].as_array() {
                for item in items {
                    if let Some(key) = key_fn(item) {
                        results.insert(key, item.clone());
                    }
                }
            }
            if let Some(errors) = response["errors"].as_array() {
                for err in errors {
                    let code = err.get("code").and_then(|c| c.as_str()).unwrap_or("");
                    if TRANSIENT_CODES.contains(&code) {
                        let mut retry_id = serde_json::Map::new();
                        for &field in id_fields {
                            if let Some(v) = err.get(field) {
                                retry_id.insert(field.to_string(), v.clone());
                            }
                        }
                        next_round.push(serde_json::Value::Object(retry_id));
                    } else {
                        terminal.push(err.clone());
                    }
                }
            }
        }
        remaining = next_round;
        if remaining.is_empty() {
            break;
        }
        if attempt + 1 < max_attempts {
            let secs = 0.5 * (2.0_f64).powi(attempt as i32);
            tokio::time::sleep(std::time::Duration::from_secs_f64(secs)).await;
        }
    }
    for ident in &remaining {
        let mut err = ident.as_object().cloned().unwrap_or_default();
        err.insert("code".to_string(), serde_json::Value::String("ExhaustedRetries".to_string()));
        terminal.push(serde_json::Value::Object(err));
    }
    Ok((results, terminal))
}

fn warn_on_errors(resource_type: &str, errors: &[serde_json::Value]) {
    if errors.is_empty() {
        return;
    }
    eprintln!(
        "Warning: could not retrieve {} {}(s); the trace will exclude their details.",
        errors.len(), resource_type,
    );
    for err in errors.iter().take(5) {
        eprintln!("  {}: {}", err.get("code").and_then(|c| c.as_str()).unwrap_or("Unknown"),
            err.get("message").and_then(|m| m.as_str()).unwrap_or(""));
    }
    if errors.len() > 5 {
        eprintln!("  ... and {} more.", errors.len() - 5);
    }
}

/// Format microseconds as Python's `str(timedelta)`: `H:MM:SS` or `H:MM:SS.ffffff`.
fn format_timedelta(us: i64) -> String {
    let total_secs = us / 1_000_000;
    let frac_us = us % 1_000_000;
    let h = total_secs / 3600;
    let m = (total_secs % 3600) / 60;
    let s = total_secs % 60;
    if frac_us == 0 {
        format!("{h}:{m:02}:{s:02}")
    } else {
        format!("{h}:{m:02}:{s:02}.{frac_us:06}")
    }
}

fn parse_datetime(s: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    // Handle: "2025-01-27T07:37:53Z", "2025-01-27 07:37:53+00:00",
    // "2025-01-27 07:37:53.238+00:00" (fractional seconds from ResponseBodyCapture)
    chrono::DateTime::parse_from_rfc3339(s).ok()
        .or_else(|| chrono::DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f%:z").ok())
        .or_else(|| chrono::DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%:z").ok())
        .map(|dt| dt.with_timezone(&chrono::Utc))
}

async fn run_trace_schedule(
    profile: Option<String>,
    farm_id: Option<String>,
    queue_id: Option<String>,
    job_id: Option<String>,
    verbose: bool,
    trace_format: Option<String>,
    trace_file: Option<String>,
) -> Result<(), CliError> {
    use serde_json::json;

    let config = setup_config(profile, farm_id, queue_id, job_id, false,
        &["farm_id", "queue_id", "job_id"])?;
    let farm = get(&config, "defaults.farm_id");
    let queue = get(&config, "defaults.queue_id");
    let job = get(&config, "defaults.job_id");

    if trace_file.is_some() && trace_format.is_none() {
        return Err(CliError::Operation(
            "Error: Must provide --trace-format with --trace-file.".to_string()
        ));
    }

    println!("Getting the job...");
    let dl = session::deadline_client(Some(&config)).await;
    let job_data = dl.get_job().farm_id(&farm).queue_id(&queue).job_id(&job)
        .send().await
        .map_err(|e| CliError::Operation(format!("Failed to get job: {}", client::format_sdk_error(&e))))?;

    let started_at = match job_data.started_at() {
        Some(dt) => {
            let s = responses::format_datetime(dt);
            parse_datetime(&s).ok_or_else(|| {
                CliError::Operation(format!("Failed to parse job startedAt: {s}"))
            })?
        }
        None => return Err(CliError::Operation(
            "No trace available - Job hasn't started yet, exiting".to_string()
        )),
    };
    let trace_end_utc = chrono::Utc::now();

    // Fetch all sessions
    let sessions_pages = client::collect_paginated(
        dl.list_sessions().farm_id(&farm).queue_id(&queue).job_id(&job)
            .into_paginator().send()
    ).await
        .map_err(|e| CliError::Operation(format!("Failed to list sessions: {e}")))?;
    let mut sessions: Vec<serde_json::Value> = sessions_pages.iter()
        .flat_map(|p| p.sessions())
        .map(|s| {
            let mut m = serde_json::Map::new();
            m.insert("sessionId".into(), json!(s.session_id()));
            m.insert("workerId".into(), json!(s.worker_id()));
            m.insert("fleetId".into(), json!(s.fleet_id()));
            m.insert("lifecycleStatus".into(), json!(s.lifecycle_status().as_str()));
            m.insert("startedAt".into(), json!(format_datetime(s.started_at())));
            if let Some(ended) = s.ended_at() { m.insert("endedAt".into(), json!(format_datetime(ended))); }
            serde_json::Value::Object(m)
        })
        .collect();
    // Sort by startedAt
    sessions.sort_by(|a, b| {
        let a_t = a["startedAt"].as_str().unwrap_or("");
        let b_t = b["startedAt"].as_str().unwrap_or("");
        a_t.cmp(b_t)
    });

    // Fetch session actions for each session
    println!("Getting all the session actions for the job...");
    for i in 0..sessions.len() {
        let sid = sessions[i]["sessionId"].as_str().unwrap_or("").to_string();
        let action_pages = collect_paginated_session_actions(&dl, &farm, &queue, &job, &sid).await
            .map_err(|e| CliError::Operation(format!("Failed to list session actions: {e}")))?;
        let actions: Vec<serde_json::Value> = action_pages.iter()
            .flat_map(|p| p.session_actions())
            .map(|a| session_action_summary_to_value(a))
            .collect();
        sessions[i]["actions"] = json!(actions);
    }

    // Collect unique step IDs and (stepId, taskId) pairs from taskRun definitions
    let mut step_ids = std::collections::HashSet::new();
    let mut task_refs = std::collections::HashSet::new();
    for session in &sessions {
        for action in session["actions"].as_array().unwrap_or(&vec![]) {
            if let Some(task_run) = action.get("definition").and_then(|d| d.get("taskRun")) {
                if let (Some(sid), Some(tid)) = (
                    task_run.get("stepId").and_then(|v| v.as_str()),
                    task_run.get("taskId").and_then(|v| v.as_str()),
                ) {
                    step_ids.insert(sid.to_string());
                    task_refs.insert((sid.to_string(), tid.to_string()));
                }
            }
        }
    }

    // BatchGetStep
    println!("Getting {} step(s) via BatchGetStep...", step_ids.len());
    let step_identifiers: Vec<serde_json::Value> = step_ids.iter().map(|s| {
        json!({"farmId": farm, "queueId": queue, "jobId": job, "stepId": s})
    }).collect();
    let config_ref = &config;
    let (steps, step_errors) = batch_get(
        |chunk| async move { api::batch_get_steps_page(&chunk, Some(config_ref)).await },
        step_identifiers,
        |item| item.get("stepId").and_then(|v| v.as_str()).map(|s| s.to_string()),
        "steps",
        &["farmId", "queueId", "jobId", "stepId"],
        3,
    ).await?;
    warn_on_errors("step", &step_errors);

    // BatchGetTask
    println!("Getting {} task(s) via BatchGetTask...", task_refs.len());
    let task_identifiers: Vec<serde_json::Value> = task_refs.iter().map(|(sid, tid)| {
        json!({"farmId": farm, "queueId": queue, "jobId": job, "stepId": sid, "taskId": tid})
    }).collect();
    let (tasks, task_errors) = batch_get(
        |chunk| async move { api::batch_get_tasks_page(&chunk, Some(config_ref)).await },
        task_identifiers,
        |item| item.get("taskId").and_then(|v| v.as_str()).map(|s| s.to_string()),
        "tasks",
        &["farmId", "queueId", "jobId", "stepId", "taskId"],
        3,
    ).await?;
    warn_on_errors("task", &task_errors);

    // Attach step/task records to sessions and actions
    for (i, session) in sessions.iter_mut().enumerate() {
        session["index"] = json!(i);
        let actions = session["actions"].as_array().cloned().unwrap_or_default();
        let mut new_actions = Vec::new();
        for mut action in actions {
            if let Some(task_run) = action.get("definition").and_then(|d| d.get("taskRun")) {
                let step_id = task_run.get("stepId").and_then(|v| v.as_str()).unwrap_or("");
                let task_id = task_run.get("taskId").and_then(|v| v.as_str()).unwrap_or("");
                if let Some(step) = steps.get(step_id) {
                    if session.get("step").is_none() {
                        session["step"] = step.clone();
                    } else if session["step"]["stepId"].as_str() != Some(step_id) {
                        let sid = session.get("sessionId").and_then(|v| v.as_str()).unwrap_or("");
                        return Err(CliError::Operation(format!(
                            "Session {sid} ran more than one step! When this code was written that wasn't possible."
                        )));
                    }
                }
                if let Some(task) = tasks.get(task_id) {
                    action["task"] = task.clone();
                }
            }
            new_actions.push(action);
        }
        session["actions"] = json!(new_actions);
    }

    // Build worker index map (sorted for deterministic pid assignment)
    let mut worker_list: Vec<String> = sessions.iter()
        .filter_map(|s| s.get("workerId").and_then(|v| v.as_str()).map(|s| s.to_string()))
        .collect::<std::collections::HashSet<_>>().into_iter().collect();
    worker_list.sort();
    let workers: std::collections::HashMap<String, usize> = worker_list.iter()
        .enumerate().map(|(i, w)| (w.clone(), i)).collect();

    println!("Processing the trace data...");

    // Build trace events and accumulators
    let time_int = |ts: &str| -> i64 {
        parse_datetime(ts).map(|dt| (dt - started_at).num_microseconds().unwrap_or(0)).unwrap_or(0)
    };
    let trace_end_str = trace_end_utc.to_rfc3339();
    let duration_of = |resource: &serde_json::Value| -> i64 {
        let end = resource.get("endedAt").and_then(|v| v.as_str()).unwrap_or(&trace_end_str);
        match resource.get("startedAt").and_then(|v| v.as_str()) {
            Some(start) => time_int(end) - time_int(start),
            None => 0,
        }
    };

    let mut trace_events: Vec<serde_json::Value> = Vec::new();
    let mut acc = std::collections::HashMap::from([
        ("sessionCount", 0i64), ("sessionActionCount", 0), ("taskRunCount", 0),
        ("envActionCount", 0), ("syncJobAttachmentsCount", 0),
        ("sessionDuration", 0), ("sessionActionDuration", 0), ("taskRunDuration", 0),
        ("envActionDuration", 0), ("syncJobAttachmentsDuration", 0),
    ]);

    for session in &sessions {
        *acc.get_mut("sessionCount").unwrap() += 1;
        *acc.get_mut("sessionDuration").unwrap() += duration_of(session);

        let worker_id = session.get("workerId").and_then(|v| v.as_str()).unwrap_or("");
        let pid = workers.get(worker_id).copied().unwrap_or(0);
        let step_name = session.get("step").and_then(|s| s.get("name")).and_then(|n| n.as_str()).unwrap_or("Unknown");
        let index = session.get("index").and_then(|v| v.as_i64()).unwrap_or(0);
        let mut session_event_name = format!("{step_name} - {index}");
        if session.get("endedAt").is_none() {
            session_event_name = format!("{session_event_name} - In Progress");
        }

        trace_events.push(json!({
            "name": session_event_name,
            "cat": "SESSION",
            "ph": "B",
            "ts": time_int(session.get("startedAt").and_then(|v| v.as_str()).unwrap_or("")),
            "pid": pid,
            "tid": 0,
            "args": {
                "sessionId": session.get("sessionId").and_then(|v| v.as_str()).unwrap_or(""),
                "workerId": worker_id,
                "fleetId": session.get("fleetId").and_then(|v| v.as_str()).unwrap_or(""),
                "lifecycleStatus": session.get("lifecycleStatus").and_then(|v| v.as_str()).unwrap_or(""),
            }
        }));

        for action in session["actions"].as_array().unwrap_or(&vec![]) {
            *acc.get_mut("sessionActionCount").unwrap() += 1;
            *acc.get_mut("sessionActionDuration").unwrap() += duration_of(action);

            let empty_obj = json!({});
            let definition = action.get("definition").unwrap_or(&empty_obj);
            let action_type = definition.as_object()
                .and_then(|m| m.keys().next()).map(|s| s.as_str()).unwrap_or("");

            let mut name = action.get("sessionActionId").and_then(|v| v.as_str()).unwrap_or("").to_string();

            match action_type {
                "taskRun" => {
                    *acc.get_mut("taskRunCount").unwrap() += 1;
                    *acc.get_mut("taskRunDuration").unwrap() += duration_of(action);

                    let empty_task = json!({});
                    let task = action.get("task").unwrap_or(&empty_task);
                    let parameters = task.get("parameters").and_then(|p| p.as_object());
                    name = match parameters {
                        Some(params) if !params.is_empty() => {
                            params.iter().map(|(k, v)| {
                                let val = v.as_object()
                                    .and_then(|m| m.values().next())
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("");
                                format!("{k}={val}")
                            }).collect::<Vec<_>>().join(",")
                        }
                        _ => "<No Task Params>".to_string(),
                    };
                }
                "envEnter" | "envExit" => {
                    *acc.get_mut("envActionCount").unwrap() += 1;
                    *acc.get_mut("envActionDuration").unwrap() += duration_of(action);

                    let env_id = definition.get(action_type)
                        .and_then(|e| e.get("environmentId"))
                        .and_then(|v| v.as_str()).unwrap_or("");
                    name = env_id.rsplit(':').next().unwrap_or(env_id).to_string();
                }
                "syncInputJobAttachments" => {
                    *acc.get_mut("syncJobAttachmentsCount").unwrap() += 1;
                    *acc.get_mut("syncJobAttachmentsDuration").unwrap() += duration_of(action);

                    let empty_sync = json!({});
                    let sync_def = definition.get(action_type).unwrap_or(&empty_sync);
                    name = if sync_def.get("stepId").is_some() {
                        "Sync Job Attchmnt (Dependencies)".to_string()
                    } else {
                        "Sync Job Attchmnt (Submitted)".to_string()
                    };
                }
                _ => {}
            }

            if action.get("endedAt").is_none() {
                name = format!("{name} - In Progress");
            }

            if action.get("startedAt").is_some() {
                trace_events.push(json!({
                    "name": name,
                    "cat": action_type,
                    "ph": "X",
                    "ts": time_int(action.get("startedAt").and_then(|v| v.as_str()).unwrap_or("")),
                    "dur": duration_of(action),
                    "pid": pid,
                    "tid": 0,
                    "args": {
                        "sessionActionId": action.get("sessionActionId").and_then(|v| v.as_str()).unwrap_or(""),
                        "status": action.get("status").and_then(|v| v.as_str()).unwrap_or(""),
                        "stepName": step_name,
                    }
                }));
            }
        }

        let session_end = session.get("endedAt").and_then(|v| v.as_str()).unwrap_or(&trace_end_str);
        trace_events.push(json!({
            "name": session_event_name,
            "cat": "SESSION",
            "ph": "E",
            "ts": time_int(session_end),
            "pid": pid,
            "tid": 0,
        }));
    }

    if verbose {
        println!(" ==== TRACE DATA ====");
        let job_resp = JobResponse::from(dl.get_job().farm_id(&farm).queue_id(&queue).job_id(&job)
            .send().await
            .map_err(|e| CliError::Operation(format!("Failed to get job: {}", client::format_sdk_error(&e))))?);
        println!("{}", crate::common::cli_object_repr(&serde_json::to_value(&job_resp).unwrap()));
        println!("{}", crate::common::cli_object_repr(&json!(sessions)));
    }

    // Print summary
    let session_duration = acc["sessionDuration"];
    let session_action_duration = acc["sessionActionDuration"];
    let task_run_duration = acc["taskRunDuration"];
    let env_action_duration = acc["envActionDuration"];
    let sync_duration = acc["syncJobAttachmentsDuration"];
    let session_action_count = acc["sessionActionCount"];

    let pct = |part: i64| -> String {
        if session_duration == 0 { "0.0".to_string() }
        else { format!("{:.1}", 100.0 * part as f64 / session_duration as f64) }
    };

    println!();
    println!(" ==== SUMMARY ====");
    println!();
    println!("Session Count: {}", acc["sessionCount"]);
    println!("Session Total Duration: {}", format_timedelta(session_duration));
    println!("Session Action Count: {session_action_count}");
    println!("Session Action Total Duration: {}", format_timedelta(session_action_duration));
    println!("Task Run Count: {}", acc["taskRunCount"]);
    println!("Task Run Total Duration: {} ({}%)", format_timedelta(task_run_duration), pct(task_run_duration));
    let non_task_count = session_action_count - acc["taskRunCount"];
    let non_task_duration = session_action_duration - task_run_duration;
    println!("Non-Task Run Count: {non_task_count}");
    println!("Non-Task Run Total Duration: {} ({}%)", format_timedelta(non_task_duration), pct(non_task_duration));
    println!("Sync Job Attachments Count: {}", acc["syncJobAttachmentsCount"]);
    println!("Sync Job Attachments Total Duration: {} ({}%)", format_timedelta(sync_duration), pct(sync_duration));
    println!("Env Action Count: {}", acc["envActionCount"]);
    println!("Env Action Total Duration: {} ({}%)", format_timedelta(env_action_duration), pct(env_action_duration));
    println!();
    let overhead = session_duration - session_action_duration;
    println!("Within-session Overhead Duration: {} ({}%)", format_timedelta(overhead), pct(overhead));
    if session_action_count > 0 {
        println!("Within-session Overhead Duration Per Action: {}", format_timedelta((overhead as f64 / session_action_count as f64).round() as i64));
    }

    // Write trace file
    if let Some(ref trace_path) = trace_file {
        // Python uses datetime.isoformat(sep="T") for trace file timestamps
        let to_iso = |s: &str| -> String {
            parse_datetime(s).map(|dt| dt.to_rfc3339_opts(chrono::SecondsFormat::Micros, false))
                .unwrap_or_else(|| s.to_string())
        };
        let job_started = job_data.started_at().map(|d| responses::format_datetime(d)).unwrap_or_default();
        let mut other_data = json!({
            "farmId": farm,
            "queueId": queue,
            "jobId": job,
            "jobName": job_data.name(),
            "startedAt": to_iso(&job_started),
        });
        if let Some(dt) = job_data.ended_at() {
            other_data["endedAt"] = json!(to_iso(&responses::format_datetime(dt)));
        }
        // Add accumulators to otherData
        for (k, v) in &acc {
            other_data[k] = json!(v);
        }

        let tracing_data = json!({
            "traceEvents": trace_events,
            "otherData": other_data,
        });

        std::fs::write(trace_path, serde_json::to_string_pretty(&tracing_data)
            .map_err(|e| CliError::Operation(e.to_string()))?)
            .map_err(|e| CliError::Operation(format!("Failed to write trace file: {e}")))?;
    }

    Ok(())
}

/// Expand leading `~` to the user's home directory.
fn expand_tilde(path: &str) -> String {
    if path.starts_with('~') {
        if let Ok(home) = std::env::var("HOME") {
            return path.replacen('~', &home, 1);
        }
    }
    path.to_string()
}

/// Paginate ListSessionActions using the SDK paginator.
async fn collect_paginated_session_actions(
    client: &aws_sdk_deadline::Client,
    farm_id: &str,
    queue_id: &str,
    job_id: &str,
    session_id: &str,
) -> Result<Vec<aws_sdk_deadline::operation::list_session_actions::ListSessionActionsOutput>, deadline_api::errors::DeadlineError> {
    deadline_api::client::collect_paginated(
        client.list_session_actions()
            .farm_id(farm_id).queue_id(queue_id).job_id(job_id).session_id(session_id)
            .into_paginator().send()
    ).await
}

/// Convert a SessionActionSummary to a serde_json::Value for the trace-schedule consumer.
fn session_action_summary_to_value(a: &aws_sdk_deadline::types::SessionActionSummary) -> serde_json::Value {
    use serde_json::json;
    let mut m = serde_json::Map::new();
    m.insert("sessionActionId".into(), json!(a.session_action_id()));
    m.insert("status".into(), json!(a.status().as_str()));
    if let Some(dt) = a.started_at() { m.insert("startedAt".into(), json!(responses::format_datetime(dt))); }
    if let Some(dt) = a.ended_at() { m.insert("endedAt".into(), json!(responses::format_datetime(dt))); }
    // Convert definition to match the JSON structure callers expect
    if let Some(def) = a.definition() {
        use aws_sdk_deadline::types::SessionActionDefinitionSummary;
        match def {
            SessionActionDefinitionSummary::TaskRun(tr) => {
                m.insert("definition".into(), json!({
                    "taskRun": {
                        "stepId": tr.step_id(),
                        "taskId": tr.task_id(),
                    }
                }));
            }
            SessionActionDefinitionSummary::SyncInputJobAttachments(_) => {
                m.insert("definition".into(), json!({"syncInputJobAttachments": {}}));
            }
            SessionActionDefinitionSummary::EnvEnter(ee) => {
                m.insert("definition".into(), json!({"envEnter": {"environmentId": ee.environment_id()}}));
            }
            SessionActionDefinitionSummary::EnvExit(ee) => {
                m.insert("definition".into(), json!({"envExit": {"environmentId": ee.environment_id()}}));
            }
            _ => {}
        }
    }
    serde_json::Value::Object(m)
}
