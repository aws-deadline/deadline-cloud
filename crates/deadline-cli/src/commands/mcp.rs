use rmcp::handler::server::wrapper::Parameters;
use rmcp::{schemars, tool, tool_handler, tool_router, ServerHandler, ServiceExt, transport::stdio};
use rmcp::model::{ServerCapabilities, ServerInfo};
use serde::Deserialize;
use serde_json::{json, Value};

use super::config::CliError;

const INSTRUCTIONS: &str = r#"
# AWS Deadline Cloud MCP Server

This server provides tools for interacting with AWS Deadline Cloud render farm management service.

## Debugging Failed Jobs Workflow

When debugging failed Deadline Cloud jobs, follow this sequence:

1. **Find failed jobs**: Use `deadline_search_jobs` with `task_run_status="FAILED"` to find jobs in a failed state
2. **Get job details**: Use `deadline_get_job` to get full job info including taskRunStatusCounts
3. **List steps**: Use `deadline_list_steps` to identify which steps failed (look for FAILED in taskRunStatusCounts)
4. **List tasks**: Use `deadline_list_tasks` for the failed step to find specific failed task IDs
5. **List sessions**: Use `deadline_list_sessions` to get session IDs for the job
6. **Get logs**: Use `deadline_get_session_and_worker_logs` with the session_id — this fetches session details, session logs (task stdout/stderr), AND worker logs (infrastructure events) in one call, with correct worker-session pairing. If worker logs return a permissions error, fall back to `deadline_get_session_logs` for session logs only.

**CRITICAL**: Worker logs show WHY a task was killed — spot interruptions, instance termination, agent crashes, OOM kills, etc. Session logs alone cannot determine the root cause of cancelled or interrupted tasks. The `deadline_get_session_and_worker_logs` tool always returns both.

## Common Root Causes Only Visible in Worker Logs

- **Spot interruption**: Look for "Shutting down the host", "Received signal 15", or EC2 termination notices
- **Worker shutdown**: Status changes to STOPPING, shutdown signals
- **Agent crash**: Errors in worker agent before task failure
- **Environment setup failure**: Issues loading environments or dependencies
- **Resource exhaustion**: Memory, disk, or system limit errors

When session logs show SIGTERM (exit code -15) or unexplained cancellation, the worker logs will contain the actual reason.

## AWS CLI Fallback

If no logs are returned, logs may be at a different position. Use `--start-from-head`:
```
aws logs get-log-events --log-group-name "/aws/deadline/{farm_id}/{queue_id}" --log-stream-name "{session_id}" --start-from-head --limit 100
```

For worker logs:
```
aws logs get-log-events --log-group-name "/aws/deadline/{farm_id}/{fleet_id}" --log-stream-name "{worker_id}" --start-from-head --limit 100
```

## Key Concepts

- **Farm**: Top-level resource containing queues and fleets
- **Queue**: Where jobs are submitted and scheduled
- **Fleet**: A group of workers that can process jobs (may use EC2 spot instances)
- **Worker**: A compute instance that runs job tasks
- **Job**: A unit of work containing steps and tasks
- **Step**: A stage within a job (e.g., render, composite)
- **Task**: Individual work item within a step
- **Session**: Worker execution context with associated logs

## Configuration

This server uses the Deadline Cloud configuration from `~/.deadline/config`, NOT the standard AWS credential chain. The config file specifies:
- `defaults.aws_profile_name`: The AWS profile used for all API calls (e.g., a Deadline Cloud Monitor profile)
- `defaults.farm_id`: Default farm ID
- `defaults.queue_id`: Default queue ID

When asked about authentication or which profile/credentials are being used, refer to the Deadline Cloud config file (`~/.deadline/config`) and the `aws_profile_name` setting, not the standard AWS credential chain.
"#;

fn ok_result(value: Value) -> String {
    serde_json::to_string(&value).unwrap_or_else(|e| error_json("SerializationError", &e.to_string()))
}

fn error_json(error_type: &str, message: &str) -> String {
    json!({"error": message, "type": error_type}).to_string()
}

// --- Parameter structs ---

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct ListFarmsParams { next_token: Option<String> }

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct ListQueuesParams { farm_id: String, next_token: Option<String> }

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct ListJobsParams { farm_id: String, queue_id: String, next_token: Option<String> }

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct ListFleetsParams { farm_id: String, next_token: Option<String> }

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct ListStorageProfilesParams { farm_id: String, queue_id: String, next_token: Option<String> }

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct GetJobParams { farm_id: String, queue_id: String, job_id: String }

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct GetSessionParams { farm_id: String, queue_id: String, job_id: String, session_id: String }

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct ListSessionsParams { farm_id: String, queue_id: String, job_id: String }

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct ListStepsParams { farm_id: String, queue_id: String, job_id: String }

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct ListTasksParams { farm_id: String, queue_id: String, job_id: String, step_id: String }

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct SearchJobsParams {
    farm_id: String,
    queue_ids: Vec<String>,
    task_run_status: Option<String>,
    name_contains: Option<String>,
    page_size: Option<i32>,
    item_offset: Option<i32>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct GetSessionLogsParams {
    farm_id: String, queue_id: String, session_id: String,
    job_id: Option<String>, limit: Option<i32>, next_token: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct SubmitJobParams {
    job_bundle_dir: String, job_parameters: Option<String>, name: Option<String>,
    farm_id: Option<String>, queue_id: Option<String>, storage_profile_id: Option<String>,
    priority: Option<i32>, max_failed_tasks_count: Option<i32>, max_retries_per_task: Option<i32>,
    max_worker_count: Option<i32>, job_attachments_file_system: Option<String>,
    require_paths_exist: Option<bool>, submitter_name: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct DownloadJobOutputParams {
    farm_id: Option<String>, queue_id: Option<String>, job_id: Option<String>,
    step_id: Option<String>, task_id: Option<String>, conflict_resolution: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct GetSessionAndWorkerLogsParams {
    farm_id: String, queue_id: String, job_id: String, session_id: String, limit: Option<i32>,
}

// --- Server ---

#[derive(Clone)]
pub struct DeadlineServer;

#[tool_router]
impl DeadlineServer {
    /// List all farms accessible to the current user.
    #[tool(name = "deadline_list_farms")]
    async fn list_farms(&self, Parameters(_p): Parameters<ListFarmsParams>) -> String {
        match deadline_api::api::list_farms(None, None).await {
            Ok(v) => ok_result(v),
            Err(e) => error_json("DeadlineError", &e.to_string()),
        }
    }

    /// List all queues in a farm.
    #[tool(name = "deadline_list_queues")]
    async fn list_queues(&self, Parameters(p): Parameters<ListQueuesParams>) -> String {
        match deadline_api::api::list_queues(&p.farm_id, None, None).await {
            Ok(v) => ok_result(v),
            Err(e) => error_json("DeadlineError", &e.to_string()),
        }
    }

    /// List all jobs in a queue.
    #[tool(name = "deadline_list_jobs")]
    async fn list_jobs(&self, Parameters(p): Parameters<ListJobsParams>) -> String {
        match deadline_api::api::list_jobs(&p.farm_id, &p.queue_id, None, None).await {
            Ok(v) => ok_result(v),
            Err(e) => error_json("DeadlineError", &e.to_string()),
        }
    }

    /// List all fleets in a farm.
    #[tool(name = "deadline_list_fleets")]
    async fn list_fleets(&self, Parameters(p): Parameters<ListFleetsParams>) -> String {
        match deadline_api::api::list_fleets(&p.farm_id, None, None).await {
            Ok(v) => ok_result(v),
            Err(e) => error_json("DeadlineError", &e.to_string()),
        }
    }

    /// List storage profiles for a queue.
    #[tool(name = "deadline_list_storage_profiles_for_queue")]
    async fn list_storage_profiles_for_queue(&self, Parameters(p): Parameters<ListStorageProfilesParams>) -> String {
        match deadline_api::api::list_storage_profiles_for_queue(&p.farm_id, &p.queue_id, None, None).await {
            Ok(v) => ok_result(v),
            Err(e) => error_json("DeadlineError", &e.to_string()),
        }
    }

    /// Check authentication status and API availability.
    #[tool(name = "deadline_check_authentication_status")]
    async fn check_authentication_status(&self) -> String {
        let source = deadline_api::auth::get_credentials_source(None);
        let status = deadline_api::auth::check_authentication_status(None).await;
        let api_available = deadline_api::auth::check_deadline_api_available(None).await;
        ok_result(json!({
            "source": format!("{source:?}"),
            "status": format!("{status:?}"),
            "api_available": api_available,
        }))
    }

    /// Get log events for a session.
    #[tool(name = "deadline_get_session_logs")]
    async fn get_session_logs(&self, Parameters(p): Parameters<GetSessionLogsParams>) -> String {
        match deadline_api::log_retrieval::get_session_logs(
            &p.farm_id, &p.queue_id, Some(&p.session_id), p.job_id.as_deref(),
            p.limit.unwrap_or(100), None, None, p.next_token.as_deref(), None,
        ).await {
            Ok((r, _)) => ok_result(json!({
                "log_group": r.log_group,
                "events": r.events.iter().map(|e| json!({
                    "timestamp": e.timestamp.to_string(), "message": e.message,
                })).collect::<Vec<_>>(),
                "count": r.count,
            })),
            Err(e) => error_json("DeadlineError", &e.to_string()),
        }
    }

    /// Get detailed information about a specific job.
    #[tool(name = "deadline_get_job")]
    async fn get_job(&self, Parameters(p): Parameters<GetJobParams>) -> String {
        match deadline_api::api::get_job(&p.farm_id, &p.queue_id, &p.job_id, None, None).await {
            Ok(v) => ok_result(v),
            Err(e) => error_json("DeadlineError", &e.to_string()),
        }
    }

    /// Get detailed information about a specific session.
    #[tool(name = "deadline_get_session")]
    async fn get_session(&self, Parameters(p): Parameters<GetSessionParams>) -> String {
        match deadline_api::api::get_session(&p.farm_id, &p.queue_id, &p.job_id, &p.session_id, None, None).await {
            Ok(v) => ok_result(v),
            Err(e) => error_json("DeadlineError", &e.to_string()),
        }
    }

    /// List all sessions for a job.
    #[tool(name = "deadline_list_sessions")]
    async fn list_sessions(&self, Parameters(p): Parameters<ListSessionsParams>) -> String {
        match deadline_api::api::list_sessions(&p.farm_id, &p.queue_id, &p.job_id, None, None).await {
            Ok(v) => ok_result(v),
            Err(e) => error_json("DeadlineError", &e.to_string()),
        }
    }

    /// List all steps for a job.
    #[tool(name = "deadline_list_steps")]
    async fn list_steps(&self, Parameters(p): Parameters<ListStepsParams>) -> String {
        match deadline_api::api::list_steps(&p.farm_id, &p.queue_id, &p.job_id, None, None).await {
            Ok(v) => ok_result(v),
            Err(e) => error_json("DeadlineError", &e.to_string()),
        }
    }

    /// List all tasks for a step.
    #[tool(name = "deadline_list_tasks")]
    async fn list_tasks(&self, Parameters(p): Parameters<ListTasksParams>) -> String {
        match deadline_api::api::list_tasks(&p.farm_id, &p.queue_id, &p.job_id, &p.step_id, None, None).await {
            Ok(v) => ok_result(v),
            Err(e) => error_json("DeadlineError", &e.to_string()),
        }
    }

    /// Search for jobs with optional filters.
    #[tool(name = "deadline_search_jobs")]
    async fn search_jobs(&self, Parameters(p): Parameters<SearchJobsParams>) -> String {
        let queue_id_refs: Vec<&str> = p.queue_ids.iter().map(|s| s.as_str()).collect();
        let page_size = p.page_size.unwrap_or(25).clamp(1, 100);
        let item_offset = p.item_offset.unwrap_or(0).clamp(0, 10000);

        let mut filters = Vec::new();
        if let Some(ref status) = p.task_run_status {
            filters.push(json!({"stringFilter": {"name": "TASK_RUN_STATUS", "operator": "EQUAL", "value": status}}));
        }
        if let Some(ref name) = p.name_contains {
            filters.push(json!({"searchTermFilter": {"searchTerm": name}}));
        }
        let filter_expr = if filters.is_empty() { None }
        else { Some(json!({"filters": filters, "operator": "AND"})) };

        match deadline_api::api::search_jobs_with_filters(
            &p.farm_id, &queue_id_refs, item_offset, page_size,
            filter_expr.as_ref(), None, None, None,
        ).await {
            Ok(v) => ok_result(v),
            Err(e) => error_json("DeadlineError", &e.to_string()),
        }
    }

    /// Submit an Open Job Description job bundle to AWS Deadline Cloud.
    #[tool(name = "deadline_submit_job")]
    async fn submit_job(&self, Parameters(_p): Parameters<SubmitJobParams>) -> String {
        error_json("NotImplementedError", "submit_job not yet implemented")
    }

    /// Download job output files from AWS Deadline Cloud.
    #[tool(name = "deadline_download_job_output")]
    async fn download_job_output(&self, Parameters(_p): Parameters<DownloadJobOutputParams>) -> String {
        error_json("NotImplementedError", "download_job_output not yet implemented")
    }

    /// Get both session logs AND worker logs for a session in one call.
    #[tool(name = "deadline_get_session_and_worker_logs")]
    async fn get_session_and_worker_logs(&self, Parameters(_p): Parameters<GetSessionAndWorkerLogsParams>) -> String {
        error_json("NotImplementedError", "get_session_and_worker_logs not yet implemented")
    }
}

#[tool_handler]
impl ServerHandler for DeadlineServer {
    fn get_info(&self) -> ServerInfo {
        let mut info = ServerInfo::default();
        info.instructions = Some(INSTRUCTIONS.into());
        info.capabilities = ServerCapabilities::builder().enable_tools().build();
        info
    }
}

pub fn run() -> Result<(), CliError> {
    tokio::runtime::Runtime::new()
        .map_err(|e| CliError::Operation(format!("Failed to start async runtime: {e}")))?
        .block_on(async {
            let service = DeadlineServer.serve(stdio()).await.map_err(|e| {
                CliError::Operation(format!("MCP server failed to start: {e}"))
            })?;
            service.waiting().await.map_err(|e| {
                CliError::Operation(format!("MCP server error: {e}"))
            })?;
            Ok(())
        })
}
