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
