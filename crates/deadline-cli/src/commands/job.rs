use clap::Subcommand;
use deadline_client::{api, job_monitoring};

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
            let resp = api::search_jobs(&farm, &[&queue], item_offset, page_size, config.as_ref(), None)
                .await
                .map_err(|e| {
                    CliError::Operation(format!("Failed to get Jobs from Deadline:\n{e}"))
                })?;
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
                        serde_json::Value::String("N/A".into()));
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
    }
}
