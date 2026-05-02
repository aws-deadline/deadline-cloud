use crate::errors::DeadlineError;
use aws_sdk_deadline::operation::get_job::GetJobOutput;
use deadline_config::ini::IniConfig;
use serde_json::Value;
use std::time::Instant;

/// A task that failed during job execution.
#[derive(Debug)]
pub struct FailedTask {
    pub step_id: String,
    pub task_id: String,
    pub step_name: String,
    pub parameters: Value,
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
    let client = crate::session::deadline_client(config).await;

    let steps_pages = crate::client::collect_paginated(
        client.list_steps().farm_id(farm_id).queue_id(queue_id).job_id(job_id)
            .into_paginator().send()
    ).await?;

    for page in &steps_pages {
        for step in page.steps() {
            let step_id = step.step_id();
            let step_name = step.name();
            let failed_count = step.task_run_status_counts()
                .get(&aws_sdk_deadline::types::TaskRunStatus::Failed)
                .copied()
                .unwrap_or(0);

            if failed_count == 0 {
                continue;
            }

            let tasks_pages = crate::client::collect_paginated(
                client.list_tasks().farm_id(farm_id).queue_id(queue_id).job_id(job_id).step_id(step_id)
                    .into_paginator().send()
            ).await?;

            for tpage in &tasks_pages {
                for task in tpage.tasks() {
                    if task.run_status() != &aws_sdk_deadline::types::TaskRunStatus::Failed {
                        continue;
                    }
                    let session_id = task.latest_session_action_id()
                        .and_then(extract_session_id);

                    // Convert parameters to Value for FailedTask
                    let parameters = match task.parameters() {
                        Some(params) => {
                            let map: serde_json::Map<String, Value> = params.iter()
                                .map(|(k, v)| {
                                    let inner = match v {
                                        aws_sdk_deadline::types::TaskParameterValue::Int(i) => serde_json::json!({"int": i}),
                                        aws_sdk_deadline::types::TaskParameterValue::Float(f) => serde_json::json!({"float": f}),
                                        aws_sdk_deadline::types::TaskParameterValue::String(s) => serde_json::json!({"string": s}),
                                        aws_sdk_deadline::types::TaskParameterValue::Path(p) => serde_json::json!({"path": p}),
                                        _ => serde_json::json!(null),
                                    };
                                    (k.clone(), inner)
                                })
                                .collect();
                            Value::Object(map)
                        }
                        None => Value::Object(Default::default()),
                    };

                    failed_tasks.push(FailedTask {
                        step_id: step_id.to_owned(),
                        task_id: task.task_id().to_owned(),
                        step_name: step_name.to_owned(),
                        parameters,
                        session_id,
                    });
                }
            }
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
    job_callback: Option<&dyn Fn(&GetJobOutput, f64, u64)>,
) -> Result<JobCompletionResult, DeadlineError> {
    let start = Instant::now();
    let mut interval_ms: u64 = 500;
    let client = crate::session::deadline_client(config).await;

    loop {
        let elapsed = start.elapsed().as_secs_f64();

        if timeout > 0 && elapsed > timeout as f64 {
            return Err(DeadlineError::OperationTimedOut(format!(
                "Timeout waiting for job {job_id} to complete after {elapsed:.1} seconds"
            )));
        }

        let job = client.get_job().farm_id(farm_id).queue_id(queue_id).job_id(job_id)
            .send().await.map_err(crate::client::deadline_error)?;
        let status = job.task_run_status.as_ref().map_or("", aws_sdk_deadline::types::TaskRunStatus::as_str);

        if let Some(cb) = status_callback {
            cb(status, elapsed, timeout);
        }
        if let Some(cb) = job_callback {
            cb(&job, elapsed, timeout);
        }

        if TERMINAL_STATES.contains(&status) {
            let elapsed_time = start.elapsed().as_secs_f64();
            let failed_tasks = if status == "SUCCEEDED" {
                Vec::new()
            } else {
                collect_failed_tasks(farm_id, queue_id, job_id, config).await?
            };
            return Ok(JobCompletionResult {
                status: status.to_owned(),
                failed_tasks,
                elapsed_time,
            });
        }

        tokio::time::sleep(std::time::Duration::from_millis(interval_ms)).await;
        interval_ms = (interval_ms * 2).min(max_poll_interval * 1000);
    }
}
