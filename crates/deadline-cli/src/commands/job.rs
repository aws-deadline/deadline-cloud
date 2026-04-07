use clap::Subcommand;
use deadline_client::{api, job_monitoring, log_retrieval};

use super::config::CliError;
use super::helpers::{apply_profile, require_setting, suggest_resources_on_client_error};

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
    /// Get details of a specific job
    Get {
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
}

pub fn run(action: JobAction) -> Result<(), CliError> {
    tokio::runtime::Runtime::new()
        .map_err(|e| CliError::Operation(e.to_string()))?
        .block_on(run_async(action))
}

async fn run_async(action: JobAction) -> Result<(), CliError> {
    match action {
        JobAction::List { profile, farm_id, queue_id, page_size, item_offset } => {
            let config = apply_profile(profile)?;
            let farm = require_setting("farm_id", farm_id, "defaults.farm_id", config.as_ref())?;
            let queue = require_setting("queue_id", queue_id, "defaults.queue_id", config.as_ref())?;
            let resp = match api::search_jobs(&farm, &[&queue], item_offset, page_size, config.as_ref(), None).await {
                Ok(r) => r,
                Err(e) => {
                    let suggestion = suggest_resources_on_client_error(
                        &e.to_string(), Some(&farm), Some(&queue), None, config.as_ref(),
                    ).await;
                    return Err(CliError::Operation(format!(
                        "Failed to get Jobs from Deadline:\n{e}{suggestion}"
                    )));
                }
            };
            let total = resp["totalResults"].as_i64().unwrap_or(0);
            let empty = vec![];
            let jobs = resp["jobs"].as_array().unwrap_or(&empty);

            // Python uses "name" if present, falls back to "displayName"
            let name_field = if jobs.first().map_or(false, |j| j.get("name").is_some()) {
                "name"
            } else {
                "displayName"
            };

            let structured: Vec<serde_json::Value> = jobs
                .iter()
                .map(|j| {
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
                })
                .collect();

            println!(
                "Displaying {} of {} Jobs starting at {}",
                structured.len(), total, item_offset
            );
            println!();
            println!("{}", crate::common::cli_object_repr(&serde_json::json!(structured)));
            Ok(())
        }
        JobAction::Get { profile, farm_id, queue_id, job_id } => {
            let config = apply_profile(profile)?;
            let farm = require_setting("farm_id", farm_id, "defaults.farm_id", config.as_ref())?;
            let queue = require_setting("queue_id", queue_id, "defaults.queue_id", config.as_ref())?;
            let job = require_setting("job_id", job_id, "defaults.job_id", config.as_ref())?;
            match api::get_job(&farm, &queue, &job, config.as_ref(), None).await {
                Ok(resp) => {
                    println!("{}", crate::common::cli_object_repr(&resp));
                    let est = estimate_remaining_time(&resp);
                    println!("estimatedTimeRemaining: {}", est.as_deref().unwrap_or("N/A"));
                    Ok(())
                }
                Err(e) => {
                    let suggestion = suggest_resources_on_client_error(
                        &e.to_string(),
                        Some(&farm),
                        Some(&queue),
                        None,
                        config.as_ref(),
                    )
                    .await;
                    Err(CliError::Operation(format!(
                        "Failed to get Job from Deadline:\n{e}{suggestion}"
                    )))
                }
            }
        }
        JobAction::GetSession { profile, farm_id, queue_id, job_id, session_id } => {
            let config = apply_profile(profile)?;
            let farm = require_setting("farm_id", farm_id, "defaults.farm_id", config.as_ref())?;
            let queue = require_setting("queue_id", queue_id, "defaults.queue_id", config.as_ref())?;
            let job = require_setting("job_id", job_id, "defaults.job_id", config.as_ref())?;
            let resp = api::get_session(&farm, &queue, &job, &session_id, config.as_ref(), None)
                .await
                .map_err(|e| CliError::Operation(format!("Failed to get Session from Deadline:\n{e}")))?;
            println!("{}", crate::common::cli_object_repr(&resp));
            Ok(())
        }
        JobAction::ListSessions { profile, farm_id, queue_id, job_id } => {
            let config = apply_profile(profile)?;
            let farm = require_setting("farm_id", farm_id, "defaults.farm_id", config.as_ref())?;
            let queue = require_setting("queue_id", queue_id, "defaults.queue_id", config.as_ref())?;
            let job = require_setting("job_id", job_id, "defaults.job_id", config.as_ref())?;
            let resp = api::list_sessions(&farm, &queue, &job, config.as_ref(), None)
                .await
                .map_err(|e| CliError::Operation(format!("Failed to list Sessions from Deadline:\n{e}")))?;
            println!("{}", crate::common::cli_object_repr(&resp["sessions"]));
            Ok(())
        }
        JobAction::ListSteps { profile, farm_id, queue_id, job_id } => {
            let config = apply_profile(profile)?;
            let farm = require_setting("farm_id", farm_id, "defaults.farm_id", config.as_ref())?;
            let queue = require_setting("queue_id", queue_id, "defaults.queue_id", config.as_ref())?;
            let job = require_setting("job_id", job_id, "defaults.job_id", config.as_ref())?;
            let resp = api::list_steps(&farm, &queue, &job, config.as_ref(), None)
                .await
                .map_err(|e| CliError::Operation(format!("Failed to list Steps from Deadline:\n{e}")))?;
            println!("{}", crate::common::cli_object_repr(&resp["steps"]));
            Ok(())
        }
        JobAction::ListTasks { profile, farm_id, queue_id, job_id, step_id } => {
            let config = apply_profile(profile)?;
            let farm = require_setting("farm_id", farm_id, "defaults.farm_id", config.as_ref())?;
            let queue = require_setting("queue_id", queue_id, "defaults.queue_id", config.as_ref())?;
            let job = require_setting("job_id", job_id, "defaults.job_id", config.as_ref())?;
            let resp = api::list_tasks(&farm, &queue, &job, &step_id, config.as_ref(), None)
                .await
                .map_err(|e| CliError::Operation(format!("Failed to list Tasks from Deadline:\n{e}")))?;
            println!("{}", crate::common::cli_object_repr(&resp["tasks"]));
            Ok(())
        }
        JobAction::Wait { profile, farm_id, queue_id, job_id, max_poll_interval, timeout, output } => {
            let config = apply_profile(profile)?;
            let farm = require_setting("farm_id", farm_id, "defaults.farm_id", config.as_ref())?;
            let queue = require_setting("queue_id", queue_id, "defaults.queue_id", config.as_ref())?;
            let job = require_setting("job_id", job_id, "defaults.job_id", config.as_ref())?;
            let is_json = output.eq_ignore_ascii_case("json");

            let job_resp = api::get_job(&farm, &queue, &job, config.as_ref(), None).await
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
                config.as_ref(), None, None, Some(&*job_cb),
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
                    let is_timeout = matches!(e, deadline_models::errors::DeadlineError::OperationTimedOut(_));
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
            let config = apply_profile(profile)?;
            let farm = require_setting("farm_id", farm_id, "defaults.farm_id", config.as_ref())?;
            let queue = require_setting("queue_id", queue_id, "defaults.queue_id", config.as_ref())?;
            let is_json = output.eq_ignore_ascii_case("json");

            let job = require_setting("job_id", job_id, "defaults.job_id", config.as_ref())?;
            let job_resp = api::get_job(&farm, &queue, &job, config.as_ref(), None).await
                .map_err(|e| CliError::Operation(format!("Failed to get job: {e}")))?;
            let job_name = job_resp["name"].as_str().unwrap_or("");

            let sid = session_id.as_deref();

            // Get session start time for timestamp formatting (needed for relative mode)
            let reference_start = if let Some(ref s) = session_id {
                let sess = api::get_session(&farm, &queue, &job, s, config.as_ref(), None).await
                    .map_err(|e| CliError::Operation(format!("Failed to get session: {e}")))?;
                sess["startedAt"].as_str().and_then(|t| {
                    chrono::DateTime::parse_from_rfc3339(&t.replace(' ', "T").replace("+00:00", "Z").replace('Z', "+00:00"))
                        .ok()
                })
            } else {
                None
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

            if !is_json {
                println!("Retrieving logs for session {} from log group /aws/deadline/{farm}/{queue}...",
                    session_id.as_deref().unwrap_or("(auto-selected)"));
                println!("Job ID: {job}");
                println!("Job Name: {job_name}");
            }

            let result = log_retrieval::get_session_logs(
                &farm, &queue, sid, Some(&job), limit, start, end,
                next_token.as_deref(), config.as_ref(),
            ).await.map_err(|e| CliError::Operation(format!("{e}")))?;

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
    }
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
