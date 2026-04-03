/// Result types for job monitoring operations.

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
