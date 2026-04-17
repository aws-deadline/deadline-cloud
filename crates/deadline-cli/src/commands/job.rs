use clap::Subcommand;
use deadline_api::{api, job_monitoring, log_retrieval};
use deadline_api::log_retrieval::SessionAutoSelect;
use deadline_config::config_file;
use deadline_config::ini::IniConfig;

use super::config::CliError;
use super::helpers::suggest_resources_on_client_error;

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
        &crate::common::CliOptions { profile, farm_id, queue_id, job_id, yes },
        required,
    )?;
    Ok(config)
}

fn get(config: &IniConfig, setting: &str) -> String {
    config_file::get_setting_with_config(setting, config).unwrap_or_default()
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
            let resp = match api::search_jobs(&farm, &[&queue], item_offset, page_size, Some(&config), None).await {
                Ok(r) => r,
                Err(e) => {
                    let suggestion = suggest_resources_on_client_error(
                        &e.to_string(), Some(&farm), Some(&queue), None, Some(&config),
                    ).await;
                    return Err(CliError::Operation(format!(
                        "Failed to get Jobs from Deadline:\n{e}{suggestion}"
                    )));
                }
            };
            print_job_list(&resp, item_offset);
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
            let resp = api::get_session(&farm, &queue, &job, &session_id, Some(&config), None)
                .await
                .map_err(|e| CliError::Operation(format!("Failed to get Session from Deadline:\n{e}")))?;
            println!("{}", crate::common::cli_object_repr(&resp));
            Ok(())
        }
        JobAction::ListSessions { profile, farm_id, queue_id, job_id } => {
            let config = setup_config(profile, farm_id, queue_id, job_id, false, &["farm_id", "queue_id", "job_id"])?;
            let farm = get(&config, "defaults.farm_id");
            let queue = get(&config, "defaults.queue_id");
            let job = get(&config, "defaults.job_id");
            let resp = api::list_sessions(&farm, &queue, &job, Some(&config), None)
                .await
                .map_err(|e| CliError::Operation(format!("Failed to list Sessions from Deadline:\n{e}")))?;
            println!("{}", crate::common::cli_object_repr(&resp["sessions"]));
            Ok(())
        }
        JobAction::ListSteps { profile, farm_id, queue_id, job_id } => {
            let config = setup_config(profile, farm_id, queue_id, job_id, false, &["farm_id", "queue_id", "job_id"])?;
            let farm = get(&config, "defaults.farm_id");
            let queue = get(&config, "defaults.queue_id");
            let job = get(&config, "defaults.job_id");
            let resp = api::list_steps(&farm, &queue, &job, Some(&config), None)
                .await
                .map_err(|e| CliError::Operation(format!("Failed to list Steps from Deadline:\n{e}")))?;
            println!("{}", crate::common::cli_object_repr(&resp["steps"]));
            Ok(())
        }
        JobAction::ListTasks { profile, farm_id, queue_id, job_id, step_id } => {
            let config = setup_config(profile, farm_id, queue_id, job_id, false, &["farm_id", "queue_id", "job_id"])?;
            let farm = get(&config, "defaults.farm_id");
            let queue = get(&config, "defaults.queue_id");
            let job = get(&config, "defaults.job_id");
            let resp = api::list_tasks(&farm, &queue, &job, &step_id, Some(&config), None)
                .await
                .map_err(|e| CliError::Operation(format!("Failed to list Tasks from Deadline:\n{e}")))?;
            println!("{}", crate::common::cli_object_repr(&resp["tasks"]));
            Ok(())
        }
        JobAction::Wait { profile, farm_id, queue_id, job_id, max_poll_interval, timeout, output } => {
            let config = setup_config(profile, farm_id, queue_id, job_id, false, &["farm_id", "queue_id", "job_id"])?;
            let farm = get(&config, "defaults.farm_id");
            let queue = get(&config, "defaults.queue_id");
            let job = get(&config, "defaults.job_id");
            let is_json = output.eq_ignore_ascii_case("json");

            let job_resp = api::get_job(&farm, &queue, &job, Some(&config), None).await
                .map_err(|e| CliError::Operation(format!("Error waiting for job completion: {e}")))?;
            let job_name = job_resp["name"].as_str().unwrap_or("");

            let job_cb: Box<dyn Fn(&serde_json::Value, f64, u64)> = if is_json {
                Box::new(|_, _, _| {})
            } else {
                Box::new(|j: &serde_json::Value, elapsed: f64, t: u64| {
                    let c = &j["taskRunStatusCounts"];
                    let running = c.get("RUNNING").and_then(|v| v.as_i64()).unwrap_or(0)
                        + c.get("ASSIGNED").and_then(|v| v.as_i64()).unwrap_or(0)
                        + c.get("STARTING").and_then(|v| v.as_i64()).unwrap_or(0);
                    let ok = c.get("SUCCEEDED").and_then(|v| v.as_i64()).unwrap_or(0);
                    let total: i64 = c.as_object().map_or(0, |m| m.values().filter_map(|v| v.as_i64()).sum());
                    let s = j.get("taskRunStatus").and_then(|v| v.as_str()).unwrap_or("");
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
                Some(&config), None, None, Some(&*job_cb),
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
        JobAction::Logs { profile, farm_id, queue_id, job_id, session_id, limit, start_time, end_time, next_token, output, timestamp_format } => {
            let config = setup_config(profile, farm_id, queue_id, job_id, false, &["farm_id", "queue_id", "job_id"])?;
            let farm = get(&config, "defaults.farm_id");
            let queue = get(&config, "defaults.queue_id");
            let is_json = output.eq_ignore_ascii_case("json");

            let job = get(&config, "defaults.job_id");
            let job_resp = api::get_job(&farm, &queue, &job, Some(&config), None).await
                .map_err(|e| CliError::Operation(format!("Failed to get job: {e}")))?;
            let job_name = job_resp["name"].as_str().unwrap_or("");

            let sid = session_id.as_deref();

            let start = start_time.as_deref().and_then(|s| {
                chrono::DateTime::parse_from_rfc3339(&s.replace('Z', "+00:00"))
                    .ok()
                    .map(|dt| dt.with_timezone(&chrono::Utc))
            });
            let end = end_time.as_deref().and_then(|s| {
                chrono::DateTime::parse_from_rfc3339(&s.replace('Z', "+00:00"))
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
                let sess = api::get_session(&farm, &queue, &job, resolved_session_id, Some(&config), None).await
                    .map_err(|e| CliError::Operation(format!("Failed to get session: {e}")))?;
                sess["startedAt"].as_str().and_then(|t| {
                    chrono::DateTime::parse_from_rfc3339(&t.replace(' ', "T").replace("+00:00", "Z").replace('Z', "+00:00"))
                        .ok()
                })
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
                println!("Retrieving logs for session {} from log group /aws/deadline/{farm}/{queue}...",
                    result.log_stream);
                println!("Job ID: {job}");
                println!("Job Name: {job_name}");
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
                return Err(CliError::Operation(format!(
                    "Invalid value for --mark-as: {mark_as}. Valid values: {}",
                    VALID_MARK_AS.join(", ")
                )));
            }
            let auto_accept = is_auto_accept(&config);

            let job = match api::get_job(&farm, &queue, &job_id, Some(&config), None).await {
                Ok(j) => j,
                Err(e) => {
                    let suggestion = suggest_resources_on_client_error(
                        &e.to_string(), Some(&farm), Some(&queue), None, Some(&config),
                    ).await;
                    return Err(CliError::Operation(format!(
                        "Failed to get Job from Deadline:\n{e}{suggestion}"
                    )));
                }
            };

            // Filter taskRunStatusCounts to non-zero entries
            let mut counts = serde_json::Map::new();
            if let Some(obj) = job.get("taskRunStatusCounts").and_then(|v| v.as_object()) {
                for (k, v) in obj {
                    if v.as_i64().unwrap_or(0) != 0 {
                        counts.insert(k.clone(), v.clone());
                    }
                }
            }

            // Build filtered summary
            let mut summary = serde_json::Map::new();
            for &field in &["name", "jobId", "taskRunStatus"] {
                if let Some(v) = job.get(field) { summary.insert(field.into(), v.clone()); }
            }
            summary.insert("taskRunStatusCounts".into(), serde_json::Value::Object(counts));
            for &field in &["startedAt", "endedAt", "createdBy", "createdAt"] {
                let v = job.get(field).and_then(|v| v.as_str()).unwrap_or("");
                summary.insert(field.into(), serde_json::Value::String(v.to_string()));
            }
            println!("{}", crate::common::cli_object_repr(&serde_json::Value::Object(summary)));

            if !auto_accept {
                let msg = if mark_as == "CANCELED" {
                    "Are you sure you want to cancel this job?".to_string()
                } else {
                    format!("Are you sure you want to cancel this job and mark its taskRunStatus as {mark_as}?")
                };
                eprint!("{msg} ");
                let mut input = String::new();
                std::io::stdin().read_line(&mut input).ok();
                if !input.trim().eq_ignore_ascii_case("y") && !input.trim().eq_ignore_ascii_case("yes") {
                    println!("Job not canceled.");
                    return Err(CliError::ExitCode { code: 1, message: String::new() });
                }
            }

            if mark_as == "CANCELED" {
                println!("Canceling job...");
            } else {
                println!("Canceling job and marking as {mark_as}...");
            }
            api::update_job(&farm, &queue, &job_id, &mark_as, Some(&config), None).await
                .map_err(|e| CliError::Operation(format!("Failed to update job:\n{e}")))?;
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
                    return Err(CliError::Operation(format!(
                        "Invalid value for --run-status: {status}. Valid values: {}",
                        VALID_RUN_STATUSES.join(", ")
                    )));
                }
            }

            let job = match api::get_job(&farm, &queue, &job_id, Some(&config), None).await {
                Ok(j) => j,
                Err(e) => {
                    let suggestion = suggest_resources_on_client_error(
                        &e.to_string(), Some(&farm), Some(&queue), None, Some(&config),
                    ).await;
                    return Err(CliError::Operation(format!(
                        "Failed to get Job from Deadline:\n{e}{suggestion}"
                    )));
                }
            };

            println!("Job: {} ({})", job["name"].as_str().unwrap_or(""), job["jobId"].as_str().unwrap_or(""));

            let counts = job.get("taskRunStatusCounts").and_then(|v| v.as_object());
            // Print taskRunStatusCounts (non-zero, keys uppercased)
            let mut counts_map = serde_json::Map::new();
            if let Some(obj) = counts {
                for (k, v) in obj {
                    if v.as_i64().unwrap_or(0) != 0 {
                        counts_map.insert(k.to_uppercase(), v.clone());
                    }
                }
            }
            println!("{}", crate::common::cli_object_repr(&serde_json::json!({"taskRunStatusCounts": counts_map})));

            let sorted_statuses: Vec<&String> = {
                let mut v: Vec<&String> = run_status_set.iter().collect();
                v.sort();
                v
            };
            println!("Requeuing all tasks with run status among: {}", sorted_statuses.iter().map(|s| s.as_str()).collect::<Vec<_>>().join(", "));

            let (total_to_requeue, summary_by_status) = count_and_summarize(counts, &run_status_set);

            if total_to_requeue == 0 {
                println!("No tasks to requeue.");
                return Ok(());
            }

            if auto_accept {
                println!("Estimated {total_to_requeue} total tasks ({summary_by_status}) to requeue.");
            } else {
                println!("This action will requeue an estimated {total_to_requeue} total tasks ({summary_by_status})");
                eprint!("Are you sure you want to requeue these tasks? ");
                let mut input = String::new();
                std::io::stdin().read_line(&mut input).ok();
                if !input.trim().eq_ignore_ascii_case("y") && !input.trim().eq_ignore_ascii_case("yes") {
                    println!("No tasks were requeued.");
                    return Err(CliError::ExitCode { code: 1, message: String::new() });
                }
                println!("Requeuing tasks...");
            }

            let mut total_requeued: i64 = 0;

            let steps_resp = api::list_steps(&farm, &queue, &job_id, Some(&config), None).await
                .map_err(|e| CliError::Operation(format!("Failed to list steps:\n{e}")))?;
            let steps = steps_resp["steps"].as_array().map(|v| v.as_slice()).unwrap_or(&[]);

            for step in steps {
                let step_id = step["stepId"].as_str().unwrap_or("");
                let step_name = step["name"].as_str().unwrap_or("");
                println!("\nStep: {step_name} ({step_id})");

                let step_counts = step.get("taskRunStatusCounts").and_then(|v| v.as_object());
                let (step_to_requeue, step_summary) = count_and_summarize(step_counts, &run_status_set);

                if step_to_requeue == 0 {
                    println!("  Step has no tasks to requeue.");
                    continue;
                }
                println!("  Requeuing an estimated {step_to_requeue} total tasks ({step_summary})...");

                let tasks_resp = api::list_tasks(&farm, &queue, &job_id, step_id, Some(&config), None).await
                    .map_err(|e| CliError::Operation(format!("Failed to list tasks:\n{e}")))?;
                let tasks = tasks_resp["tasks"].as_array().map(|v| v.as_slice()).unwrap_or(&[]);

                for task in tasks {
                    let status = task.get("runStatus").and_then(|v| v.as_str()).unwrap_or("");
                    if !run_status_set.contains(&status.to_uppercase()) {
                        continue;
                    }
                    let task_id = task["taskId"].as_str().unwrap_or("");
                    let params = task.get("parameters").and_then(|v| v.as_object());
                    let task_summary = if let Some(p) = params.filter(|p| !p.is_empty()) {
                        let param_str: String = p.iter().map(|(name, val)| {
                            // Union type: {"Frame": {"int": "1"}} → extract first value of inner dict
                            let extracted = val.as_object()
                                .and_then(|inner| inner.values().next())
                                .and_then(|v| v.as_str())
                                .unwrap_or("");
                            format!("{name}={extracted}")
                        }).collect::<Vec<_>>().join(",");
                        format!("{param_str} ({task_id})")
                    } else {
                        task_id.to_string()
                    };
                    println!("    {status} {task_summary}");

                    api::update_task(&farm, &queue, &job_id, step_id, task_id, "PENDING", Some(&config), None).await
                        .map_err(|e| CliError::Operation(format!("Failed to update task:\n{e}")))?;
                    total_requeued += 1;
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

            let resp = match api::search_jobs_with_filters(
                &farm, &[queue.as_str()], item_offset, page_size,
                filter_json.as_ref(), sort_json.as_ref(),
                Some(&config), None,
            ).await {
                Ok(r) => r,
                Err(e) => {
                    let suggestion = suggest_resources_on_client_error(
                        &e.to_string(), Some(&farm), Some(&queue), None, Some(&config),
                    ).await;
                    return Err(CliError::Operation(format!(
                        "Failed to search Jobs from Deadline:\n{e}{suggestion}"
                    )));
                }
            };
            print_job_list(&resp, item_offset);
            Ok(())
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
                conflict_resolution, is_json,
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
    config_file::get_setting_with_config("settings.auto_accept", config)
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
        obj.iter()
            .filter(|(k, v)| statuses.contains(&k.to_uppercase()) && v.as_i64().unwrap_or(0) != 0)
            .map(|(k, v)| format!("{} {} tasks", v.as_i64().unwrap_or(0), k.to_uppercase()))
            .collect::<Vec<_>>()
            .join(", ")
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
    match api::get_job(farm, queue, job_id, Some(config), None).await {
        Ok(resp) => {
            println!("{}", crate::common::cli_object_repr(&resp));
            let est = estimate_remaining_time(&resp);
            println!("estimatedTimeRemaining: {}", est.as_deref().unwrap_or("N/A"));
            Ok(())
        }
        Err(e) => {
            let suggestion = suggest_resources_on_client_error(
                &e.to_string(), Some(farm), Some(queue), None, Some(config),
            ).await;
            Err(CliError::Operation(format!(
                "Failed to get Job from Deadline:\n{e}{suggestion}"
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
    let resp = match api::search_jobs_with_filters(
        farm, &[queue], 0, 5, Some(&filter), None, Some(config), None,
    ).await {
        Ok(r) => r,
        Err(e) => {
            return Err(CliError::Operation(format!("Failed to search jobs:\n{e}")));
        }
    };

    let empty = vec![];
    let jobs = resp["jobs"].as_array().unwrap_or(&empty);
    let total = resp["totalResults"].as_i64().unwrap_or(0);

    if jobs.is_empty() {
        println!("No jobs found matching \"{search_term}\"");
        return Ok(());
    }

    if total == 1 {
        let job_id = jobs[0]["jobId"].as_str().unwrap_or("");
        return print_job_details(farm, queue, job_id, config).await;
    }

    // Multiple results — show summary
    println!("Found {total} job(s) matching \"{search_term}\", showing most recent {}:\n", jobs.len());
    for job in jobs {
        let name = job.get("name").or(job.get("displayName"))
            .and_then(|v| v.as_str()).unwrap_or("");
        let name = truncate_middle(name, 80);
        let job_id = job["jobId"].as_str().unwrap_or("");
        let status = job["taskRunStatus"].as_str().unwrap_or("");
        let created = job["createdAt"].as_str().map(|s| {
            // Convert to local time like Python's _format_timestamp
            chrono::DateTime::parse_from_rfc3339(&s.replace(' ', "T").replace("+00:00", "Z").replace('Z', "+00:00"))
                .map(|dt| dt.with_timezone(&chrono::Local).format("%Y-%m-%d %H:%M:%S %z").to_string())
                .unwrap_or_else(|_| s.to_string())
        }).unwrap_or_default();
        let counts = job.get("taskRunStatusCounts").and_then(|v| v.as_object());
        let task_summary = format_task_summary(counts);

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
    let pending = get(&["PENDING"]);
    let succeeded = get(&["SUCCEEDED"]);
    let failed = get(&["FAILED"]);
    let canceled = get(&["CANCELED"]);
    let suspended = get(&["SUSPENDED"]);
    if ready > 0 { parts.push(format!("{ready} ready")); }
    if running > 0 { parts.push(format!("{running} running")); }
    if pending > 0 { parts.push(format!("{pending} pending")); }
    if suspended > 0 { parts.push(format!("{suspended} suspended")); }
    if succeeded > 0 { parts.push(format!("{succeeded} succeeded")); }
    if failed > 0 { parts.push(format!("{failed} failed")); }
    if canceled > 0 { parts.push(format!("{canceled} canceled")); }
    if parts.is_empty() { "no tasks".into() } else { parts.join(", ") }
}

/// Print job list output (shared between `job list` and `job search`).
fn print_job_list(resp: &serde_json::Value, item_offset: i32) {
    let total = resp["totalResults"].as_i64().unwrap_or(0);
    let empty = vec![];
    let jobs = resp["jobs"].as_array().unwrap_or(&empty);

    let name_field = if jobs.first().map_or(false, |j| j.get("name").is_some()) {
        "name"
    } else {
        "displayName"
    };

    let structured: Vec<serde_json::Value> = jobs.iter().map(|j| {
        let mut m = serde_json::Map::new();
        for &field in &[name_field, "jobId", "taskRunStatus", "startedAt", "endedAt", "createdBy", "createdAt"] {
            let v = j.get(field).and_then(|v| v.as_str()).unwrap_or("");
            m.insert(field.into(), serde_json::Value::String(v.to_string()));
        }
        m.insert("estimatedTimeRemaining".into(),
            serde_json::Value::String(
                estimate_remaining_time(j).unwrap_or_else(|| "N/A".into())
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
) -> Result<(), CliError> {
    use deadline_job_attachments::download::OutputDownloader;
    use deadline_job_attachments::models::{FileConflictResolution, JobAttachmentS3Settings};
    use deadline_job_attachments::s3;
    use deadline_api::path_utils::{human_readable_file_size, summarize_path_list};

    // Get job
    let job = api::get_job(farm_id, queue_id, job_id, Some(config), None)
        .await
        .map_err(|e| CliError::Operation(format!("Failed to download output:\n{e}")))?;
    let job_name = job["name"].as_str().unwrap_or("");

    // Get optional step/task
    let step_name = if let Some(sid) = step_id {
        let step = api::get_step(farm_id, queue_id, job_id, sid, Some(config), None)
            .await
            .map_err(|e| CliError::Operation(format!("Failed to download output:\n{e}")))?;
        Some(step["name"].as_str().unwrap_or("").to_string())
    } else {
        None
    };

    let task_params;
    let session_action_id;
    if let (Some(sid), Some(tid)) = (step_id, task_id) {
        let task = api::get_task(farm_id, queue_id, job_id, sid, tid, Some(config), None)
            .await
            .map_err(|e| CliError::Operation(format!("Failed to download output:\n{e}")))?;
        task_params = task.get("parameters").cloned();
        session_action_id = task.get("latestSessionActionId")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
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
        job_name,
        step_name.as_deref(),
        task_params_for_msg,
        is_json,
    ));

    // Get queue for jobAttachmentSettings
    let queue = api::get_queue(farm_id, queue_id, Some(config), None)
        .await
        .map_err(|e| CliError::Operation(format!("Failed to download output:\n{e}")))?;

    let attachment_settings = queue.get("jobAttachmentSettings")
        .ok_or_else(|| CliError::Operation(format!(
            "Queue '{}' does not have job attachments configured.",
            queue["displayName"].as_str().unwrap_or(queue_id)
        )))?;

    let bucket = attachment_settings["s3BucketName"].as_str().unwrap_or("");
    let prefix = attachment_settings["rootPrefix"].as_str().unwrap_or("");
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
    let downloader = OutputDownloader::new(
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
        let summary = summarize_path_list(&path_refs, 10);
        for line in summary.lines() {
            println!("  {line}");
        }
    }

    // Resolve conflict resolution
    let resolution = conflict_resolution.unwrap_or_else(|| {
        let setting = config_file::get_setting_with_config("settings.conflict_resolution", config)
            .unwrap_or_default();
        match setting.to_uppercase().as_str() {
            "SKIP" => FileConflictResolution::Skip,
            "OVERWRITE" => FileConflictResolution::Overwrite,
            _ => FileConflictResolution::CreateCopy,
        }
    });

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
