use crate::api;
use crate::errors::DeadlineError;
use deadline_config::ini::IniConfig;
use serde_json::Value;
use std::time::Instant;

/// A task that failed during job execution.
#[derive(Debug)]
pub struct FailedTask {
    pub step_id: String,
    pub task_id: String,
    pub step_name: String,
    pub parameters: serde_json::Value,
    pub session_id: Option<String>,
}

/// Result of waiting for a job to complete.
#[derive(Debug)]
pub struct JobCompletionResult {
    pub status: String,
    pub failed_tasks: Vec<FailedTask>,
    pub elapsed_time: f64,
}

const TERMINAL_STATES: &[&str] = &[
    "SUCCEEDED", "FAILED", "CANCELED", "SUSPENDED", "NOT_COMPATIBLE",
];

fn extract_session_id(session_action_id: &str) -> Option<String> {
    let parts: Vec<&str> = session_action_id.split('-').collect();
    if parts.len() >= 3 && parts[0] == "sessionaction" {
        Some(format!("session-{}", parts[1]))
    } else {
        None
    }
}

async fn collect_failed_tasks(
    farm_id: &str,
    queue_id: &str,
    job_id: &str,
    config: Option<&IniConfig>,
) -> Result<Vec<FailedTask>, DeadlineError> {
    let mut failed_tasks = Vec::new();
    let empty = vec![];

    let steps_resp = api::list_steps(farm_id, queue_id, job_id, config).await?;
    let steps = steps_resp["steps"].as_array().unwrap_or(&empty);

    for step in steps {
        let step_id = step["stepId"].as_str().unwrap_or("");
        let step_name = step["name"].as_str().unwrap_or("");
        let failed_count = step
            .get("taskRunStatusCounts")
            .and_then(|c| c.get("FAILED"))
            .and_then(|v| v.as_i64())
            .unwrap_or(0);

        if failed_count == 0 {
            continue;
        }

        let tasks_resp = api::list_tasks(farm_id, queue_id, job_id, step_id, config).await?;
        let tasks = tasks_resp["tasks"].as_array().unwrap_or(&empty);

        for task in tasks {
            if task.get("runStatus").and_then(|s| s.as_str()) != Some("FAILED") {
                continue;
            }
            let session_id = task
                .get("latestSessionActionId")
                .and_then(|v| v.as_str())
                .and_then(extract_session_id);

            failed_tasks.push(FailedTask {
                step_id: step_id.to_string(),
                task_id: task["taskId"].as_str().unwrap_or("").to_string(),
                step_name: step_name.to_string(),
                parameters: task.get("parameters").cloned().unwrap_or(Value::Object(Default::default())),
                session_id,
            });
        }
    }
    Ok(failed_tasks)
}

pub async fn wait_for_job_completion(
    farm_id: &str,
    queue_id: &str,
    job_id: &str,
    max_poll_interval: u64,
    timeout: u64,
    config: Option<&IniConfig>,
    status_callback: Option<&dyn Fn(&str, f64, u64)>,
    job_callback: Option<&dyn Fn(&Value, f64, u64)>,
) -> Result<JobCompletionResult, DeadlineError> {
    let start = Instant::now();
    let mut interval_ms: u64 = 500;

    loop {
        let elapsed = start.elapsed().as_secs_f64();

        if timeout > 0 && elapsed > timeout as f64 {
            return Err(DeadlineError::OperationTimedOut(format!(
                "Timeout waiting for job {job_id} to complete after {elapsed:.1} seconds"
            )));
        }

        let job = api::get_job(farm_id, queue_id, job_id, config).await?;
        let status = job.get("taskRunStatus").and_then(|v| v.as_str()).unwrap_or("");

        if let Some(cb) = status_callback {
            cb(status, elapsed, timeout);
        }
        if let Some(cb) = job_callback {
            cb(&job, elapsed, timeout);
        }

        if TERMINAL_STATES.contains(&status) {
            let elapsed_time = start.elapsed().as_secs_f64();
            let failed_tasks = if status != "SUCCEEDED" {
                collect_failed_tasks(farm_id, queue_id, job_id, config).await?
            } else {
                Vec::new()
            };
            return Ok(JobCompletionResult {
                status: status.to_string(),
                failed_tasks,
                elapsed_time,
            });
        }

        tokio::time::sleep(std::time::Duration::from_millis(interval_ms)).await;
        interval_ms = (interval_ms * 2).min(max_poll_interval * 1000);
    }
}
