use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{ServerCapabilities, ServerInfo};
use rmcp::{
    ServerHandler, ServiceExt, schemars, tool, tool_handler, tool_router, transport::stdio,
};
use serde::Deserialize;
use serde_json::{Value, json};

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
    serde_json::to_string(&value)
        .unwrap_or_else(|e| error_json("SerializationError", &e.to_string()))
}

fn error_json(error_type: &str, message: &str) -> String {
    json!({"error": message, "type": error_type}).to_string()
}

/// Record MCP per-tool telemetry: latency and usage events.
fn record_mcp_tool_telemetry(tool_name: &str, start: std::time::Instant, result: &str) {
    let tc = deadline_api::telemetry::create_telemetry(None);
    let latency = start.elapsed().as_nanos() as u64;
    let parsed = serde_json::from_str::<Value>(result).ok();
    let is_success = parsed.as_ref().is_none_or(|v| v.get("error").is_none());
    let error_type: Option<String> = if is_success {
        None
    } else {
        parsed.and_then(|v| v.get("type").and_then(|t| t.as_str().map(String::from)))
    };

    let mut latency_details = std::collections::HashMap::new();
    latency_details.insert("latency".into(), json!(latency));
    latency_details.insert("tool_name".into(), json!(tool_name));
    latency_details.insert("usage_mode".into(), json!("MCP"));
    tc.record_event(
        "com.amazon.rum.deadline.mcp.latency",
        latency_details,
        false,
    );

    let mut usage_details = std::collections::HashMap::new();
    usage_details.insert("tool_name".into(), json!(tool_name));
    usage_details.insert("is_success".into(), json!(is_success));
    usage_details.insert("error_type".into(), json!(error_type));
    usage_details.insert("usage_mode".into(), json!("MCP"));
    tc.record_event("com.amazon.rum.deadline.mcp.usage", usage_details, false);
}

macro_rules! with_mcp_telemetry {
    ($name:expr, $body:expr) => {{
        let start = std::time::Instant::now();
        let result = $body;
        record_mcp_tool_telemetry($name, start, &result);
        result
    }};
}

// --- Parameter structs ---

#[derive(Debug, Deserialize, schemars::JsonSchema)]
#[allow(
    clippy::empty_structs_with_brackets,
    reason = "serde requires braced struct to deserialize from JSON {}"
)]
struct ListFarmsParams {}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct ListQueuesParams {
    farm_id: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct ListJobsParams {
    farm_id: String,
    queue_id: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct ListFleetsParams {
    farm_id: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct ListStorageProfilesParams {
    farm_id: String,
    queue_id: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
#[allow(
    clippy::struct_field_names,
    reason = "field names match the Deadline API JSON schema"
)]
struct GetJobParams {
    farm_id: String,
    queue_id: String,
    job_id: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
#[allow(
    clippy::struct_field_names,
    reason = "field names match the Deadline API JSON schema"
)]
struct GetSessionParams {
    farm_id: String,
    queue_id: String,
    job_id: String,
    session_id: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
#[allow(
    clippy::struct_field_names,
    reason = "field names match the Deadline API JSON schema"
)]
struct ListSessionsParams {
    farm_id: String,
    queue_id: String,
    job_id: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
#[allow(
    clippy::struct_field_names,
    reason = "field names match the Deadline API JSON schema"
)]
struct ListStepsParams {
    farm_id: String,
    queue_id: String,
    job_id: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
#[allow(
    clippy::struct_field_names,
    reason = "field names match the Deadline API JSON schema"
)]
struct ListTasksParams {
    farm_id: String,
    queue_id: String,
    job_id: String,
    step_id: String,
}

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
    farm_id: String,
    queue_id: String,
    session_id: String,
    job_id: Option<String>,
    limit: Option<i32>,
    next_token: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct SubmitJobParams {
    job_bundle_dir: String,
    job_parameters: Option<String>,
    name: Option<String>,
    farm_id: Option<String>,
    queue_id: Option<String>,
    storage_profile_id: Option<String>,
    priority: Option<i32>,
    max_failed_tasks_count: Option<i32>,
    max_retries_per_task: Option<i32>,
    max_worker_count: Option<i32>,
    job_attachments_file_system: Option<String>,
    require_paths_exist: Option<bool>,
    submitter_name: Option<String>,
    known_asset_paths: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct DownloadJobOutputParams {
    farm_id: Option<String>,
    queue_id: Option<String>,
    job_id: Option<String>,
    step_id: Option<String>,
    task_id: Option<String>,
    conflict_resolution: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct GetSessionAndWorkerLogsParams {
    farm_id: String,
    queue_id: String,
    job_id: String,
    session_id: String,
    limit: Option<i32>,
}

// --- Server ---

#[derive(Clone)]
pub(crate) struct DeadlineServer;

#[tool_router]
impl DeadlineServer {
    /// List all farms accessible to the current user.
    #[tool(name = "deadline_list_farms")]
    async fn list_farms(&self, Parameters(_p): Parameters<ListFarmsParams>) -> String {
        with_mcp_telemetry!("deadline_list_farms", {
            let dl = deadline_api::session::deadline_client(None).await;
            let builder = deadline_api::client::apply_dcm_principal(dl.list_farms(), None);
            let resp =
                deadline_api::client::collect_paginated(builder.into_paginator().send()).await;
            match resp {
                Ok(pages) => {
                    let farms: Vec<Value> = pages
                        .iter()
                        .flat_map(
                            aws_sdk_deadline::operation::list_farms::ListFarmsOutput::farms,
                        )
                        .map(|f| json!({"farmId": f.farm_id(), "displayName": f.display_name(), "createdAt": f.created_at().to_string(), "createdBy": f.created_by()}))
                        .collect();
                    ok_result(json!({"farms": farms}))
                }
                Err(e) => error_json("DeadlineError", &e.to_string()),
            }
        })
    }

    /// List all queues in a farm.
    #[tool(name = "deadline_list_queues")]
    async fn list_queues(&self, Parameters(p): Parameters<ListQueuesParams>) -> String {
        with_mcp_telemetry!("deadline_list_queues", {
            let dl = deadline_api::session::deadline_client(None).await;
            let builder = deadline_api::client::apply_dcm_principal(
                dl.list_queues().farm_id(&p.farm_id),
                None,
            );
            let resp =
                deadline_api::client::collect_paginated(builder.into_paginator().send()).await;
            match resp {
                Ok(pages) => {
                    let queues: Vec<Value> = pages
                        .iter()
                        .flat_map(
                            aws_sdk_deadline::operation::list_queues::ListQueuesOutput::queues,
                        )
                        .map(|q| json!({"queueId": q.queue_id(), "displayName": q.display_name(), "status": q.status().as_str(), "createdAt": q.created_at().to_string(), "createdBy": q.created_by()}))
                        .collect();
                    ok_result(json!({"queues": queues}))
                }
                Err(e) => error_json("DeadlineError", &e.to_string()),
            }
        })
    }

    /// List all jobs in a queue.
    #[tool(name = "deadline_list_jobs")]
    async fn list_jobs(&self, Parameters(p): Parameters<ListJobsParams>) -> String {
        with_mcp_telemetry!("deadline_list_jobs", {
            let dl = deadline_api::session::deadline_client(None).await;
            let builder = deadline_api::client::apply_dcm_principal(
                dl.list_jobs().farm_id(&p.farm_id).queue_id(&p.queue_id),
                None,
            );
            match deadline_api::client::collect_paginated(builder.into_paginator().send()).await {
                Ok(pages) => {
                    let jobs: Vec<Value> = pages.iter()
                        .flat_map(aws_sdk_deadline::operation::list_jobs::ListJobsOutput::jobs)
                        .map(|j| json!({"jobId": j.job_id(), "name": j.name(), "taskRunStatus": j.task_run_status().map(aws_sdk_deadline::types::TaskRunStatus::as_str), "lifecycleStatus": j.lifecycle_status().as_str(), "createdAt": j.created_at().to_string()}))
                        .collect();
                    ok_result(json!({"jobs": jobs}))
                }
                Err(e) => error_json("DeadlineError", &e.to_string()),
            }
        })
    }

    /// List all fleets in a farm.
    #[tool(name = "deadline_list_fleets")]
    async fn list_fleets(&self, Parameters(p): Parameters<ListFleetsParams>) -> String {
        with_mcp_telemetry!("deadline_list_fleets", {
            let dl = deadline_api::session::deadline_client(None).await;
            let builder = deadline_api::client::apply_dcm_principal(
                dl.list_fleets().farm_id(&p.farm_id),
                None,
            );
            let resp =
                deadline_api::client::collect_paginated(builder.into_paginator().send()).await;
            match resp {
                Ok(pages) => {
                    let fleets: Vec<Value> = pages
                        .iter()
                        .flat_map(
                            aws_sdk_deadline::operation::list_fleets::ListFleetsOutput::fleets,
                        )
                        .map(|f| json!({"fleetId": f.fleet_id(), "displayName": f.display_name(), "status": f.status().as_str(), "createdAt": f.created_at().to_string(), "createdBy": f.created_by()}))
                        .collect();
                    ok_result(json!({"fleets": fleets}))
                }
                Err(e) => error_json("DeadlineError", &e.to_string()),
            }
        })
    }

    /// List storage profiles for a queue.
    #[tool(name = "deadline_list_storage_profiles_for_queue")]
    async fn list_storage_profiles_for_queue(
        &self,
        Parameters(p): Parameters<ListStorageProfilesParams>,
    ) -> String {
        with_mcp_telemetry!("deadline_list_storage_profiles_for_queue", {
            let client = deadline_api::session::deadline_client(None).await;
            match deadline_api::client::collect_paginated(
                client
                    .list_storage_profiles_for_queue()
                    .farm_id(&p.farm_id)
                    .queue_id(&p.queue_id)
                    .into_paginator()
                    .send(),
            )
            .await
            {
                Ok(pages) => {
                    let profiles: Vec<Value> = pages.iter()
                        .flat_map(aws_sdk_deadline::operation::list_storage_profiles_for_queue::ListStorageProfilesForQueueOutput::storage_profiles)
                        .map(|sp| json!({
                            "storageProfileId": sp.storage_profile_id(),
                            "displayName": sp.display_name(),
                            "osFamily": sp.os_family().as_str(),
                        }))
                        .collect();
                    ok_result(json!({"storageProfiles": profiles}))
                }
                Err(e) => error_json("DeadlineError", &e.to_string()),
            }
        })
    }

    /// Check authentication status and API availability.
    #[tool(name = "deadline_check_authentication_status")]
    async fn check_authentication_status(&self) -> String {
        with_mcp_telemetry!("deadline_check_authentication_status", {
            let source = deadline_api::auth::get_credentials_source(None);
            let status = deadline_api::auth::check_authentication_status(None).await;
            let api_available =
                status == deadline_api::auth::AwsAuthenticationStatus::Authenticated;
            ok_result(json!({
                "source": source.to_string(),
                "status": status.to_string(),
                "api_available": api_available,
            }))
        })
    }

    /// Get log events for a session.
    #[tool(name = "deadline_get_session_logs")]
    async fn get_session_logs(&self, Parameters(p): Parameters<GetSessionLogsParams>) -> String {
        with_mcp_telemetry!("deadline_get_session_logs", {
            match deadline_api::log_retrieval::get_session_logs(
                &p.farm_id,
                &p.queue_id,
                Some(&p.session_id),
                p.job_id.as_deref(),
                p.limit.unwrap_or(100),
                None,
                None,
                p.next_token.as_deref(),
                None,
            )
            .await
            {
                Ok((r, _)) => ok_result(json!({
                    "log_group": r.log_group,
                    "events": r.events.iter().map(|e| json!({
                        "timestamp": e.timestamp.to_string(), "message": e.message,
                    })).collect::<Vec<_>>(),
                    "count": r.count,
                })),
                Err(e) => error_json("DeadlineError", &e.to_string()),
            }
        })
    }

    /// Get detailed information about a specific job.
    #[tool(name = "deadline_get_job")]
    async fn get_job(&self, Parameters(p): Parameters<GetJobParams>) -> String {
        with_mcp_telemetry!("deadline_get_job", {
            match deadline_api::session::deadline_client(None)
                .await
                .get_job()
                .farm_id(&p.farm_id)
                .queue_id(&p.queue_id)
                .job_id(&p.job_id)
                .send()
                .await
            {
                Ok(output) => {
                    let resp = deadline_api::responses::JobResponse::from(output);
                    ok_result(serde_json::to_value(&resp).unwrap_or_default())
                }
                Err(e) => error_json("DeadlineError", &deadline_api::client::format_sdk_error(&e)),
            }
        })
    }

    /// Get detailed information about a specific session.
    #[tool(name = "deadline_get_session")]
    async fn get_session(&self, Parameters(p): Parameters<GetSessionParams>) -> String {
        with_mcp_telemetry!("deadline_get_session", {
            match deadline_api::session::deadline_client(None)
                .await
                .get_session()
                .farm_id(&p.farm_id)
                .queue_id(&p.queue_id)
                .job_id(&p.job_id)
                .session_id(&p.session_id)
                .send()
                .await
            {
                Ok(output) => {
                    let resp = deadline_api::responses::SessionResponse::from(output);
                    ok_result(serde_json::to_value(&resp).unwrap_or_default())
                }
                Err(e) => error_json("DeadlineError", &deadline_api::client::format_sdk_error(&e)),
            }
        })
    }

    /// List all sessions for a job.
    #[tool(name = "deadline_list_sessions")]
    async fn list_sessions(&self, Parameters(p): Parameters<ListSessionsParams>) -> String {
        with_mcp_telemetry!("deadline_list_sessions", {
            let client = deadline_api::session::deadline_client(None).await;
            match deadline_api::client::collect_paginated(
                client
                    .list_sessions()
                    .farm_id(&p.farm_id)
                    .queue_id(&p.queue_id)
                    .job_id(&p.job_id)
                    .into_paginator()
                    .send(),
            )
            .await
            {
                Ok(pages) => {
                    let sessions: Vec<Value> = pages.iter()
                    .flat_map(aws_sdk_deadline::operation::list_sessions::ListSessionsOutput::sessions)
                    .map(|s| json!({"sessionId": s.session_id(), "fleetId": s.fleet_id(), "workerId": s.worker_id(), "startedAt": s.started_at().to_string(), "lifecycleStatus": s.lifecycle_status().as_str()}))
                    .collect();
                    ok_result(json!({"sessions": sessions}))
                }
                Err(e) => error_json("DeadlineError", &e.to_string()),
            }
        })
    }

    /// List all steps for a job.
    #[tool(name = "deadline_list_steps")]
    async fn list_steps(&self, Parameters(p): Parameters<ListStepsParams>) -> String {
        with_mcp_telemetry!("deadline_list_steps", {
            let client = deadline_api::session::deadline_client(None).await;
            match deadline_api::client::collect_paginated(
                client
                    .list_steps()
                    .farm_id(&p.farm_id)
                    .queue_id(&p.queue_id)
                    .job_id(&p.job_id)
                    .into_paginator()
                    .send(),
            )
            .await
            {
                Ok(pages) => {
                    let steps: Vec<Value> = pages.iter()
                    .flat_map(aws_sdk_deadline::operation::list_steps::ListStepsOutput::steps)
                    .map(|s| json!({"stepId": s.step_id(), "name": s.name(), "lifecycleStatus": s.lifecycle_status().as_str(), "createdAt": s.created_at.to_string()}))
                    .collect();
                    ok_result(json!({"steps": steps}))
                }
                Err(e) => error_json("DeadlineError", &e.to_string()),
            }
        })
    }

    /// List all tasks for a step.
    #[tool(name = "deadline_list_tasks")]
    async fn list_tasks(&self, Parameters(p): Parameters<ListTasksParams>) -> String {
        with_mcp_telemetry!("deadline_list_tasks", {
            let client = deadline_api::session::deadline_client(None).await;
            match deadline_api::client::collect_paginated(
                client
                    .list_tasks()
                    .farm_id(&p.farm_id)
                    .queue_id(&p.queue_id)
                    .job_id(&p.job_id)
                    .step_id(&p.step_id)
                    .into_paginator()
                    .send(),
            )
            .await
            {
                Ok(pages) => {
                    let tasks: Vec<Value> = pages.iter()
                    .flat_map(aws_sdk_deadline::operation::list_tasks::ListTasksOutput::tasks)
                    .map(|t| json!({"taskId": t.task_id(), "runStatus": t.run_status().as_str(), "createdAt": t.created_at.to_string(), "createdBy": t.created_by()}))
                    .collect();
                    ok_result(json!({"tasks": tasks}))
                }
                Err(e) => error_json("DeadlineError", &e.to_string()),
            }
        })
    }

    /// Search for jobs with optional filters.
    #[tool(name = "deadline_search_jobs")]
    async fn search_jobs(&self, Parameters(p): Parameters<SearchJobsParams>) -> String {
        with_mcp_telemetry!("deadline_search_jobs", {
            let page_size = p.page_size.unwrap_or(25).clamp(1, 100);
            let item_offset = p.item_offset.unwrap_or(0).clamp(0, 10000);

            let mut filters = Vec::new();
            if let Some(ref status) = p.task_run_status {
                filters.push(json!({"stringFilter": {"name": "TASK_RUN_STATUS", "operator": "EQUAL", "value": status}}));
            }
            if let Some(ref name) = p.name_contains {
                filters.push(json!({"searchTermFilter": {"searchTerm": name}}));
            }
            let filter_expr = if filters.is_empty() {
                None
            } else {
                Some(json!({"filters": filters, "operator": "AND"}))
            };

            let filter = filter_expr
                .as_ref()
                .map(deadline_api::api::build_filter_expressions);
            let filter = match filter {
                Some(Ok(f)) => Some(f),
                Some(Err(e)) => return error_json("DeadlineError", &e.to_string()),
                None => None,
            };

            let dl = deadline_api::session::deadline_client(None).await;
            let mut req = dl
                .search_jobs()
                .farm_id(&p.farm_id)
                .set_queue_ids(Some(p.queue_ids.clone()))
                .item_offset(item_offset)
                .page_size(page_size);

            if let Some(ref f) = filter {
                req = req.filter_expressions(f.clone());
            }

            req = req.sort_expressions(aws_sdk_deadline::types::SearchSortExpression::FieldSort(
                aws_sdk_deadline::types::FieldSortExpression::builder()
                    .name("CREATED_AT")
                    .sort_order(aws_sdk_deadline::types::SortOrder::Descending)
                    .build()
                    .expect("required fields set"),
            ));

            match req.send().await {
                Ok(output) => {
                    let jobs: Vec<Value> = output.jobs().iter()
                    .map(|j| json!({"jobId": j.job_id(), "name": j.name(), "taskRunStatus": j.task_run_status().map(aws_sdk_deadline::types::TaskRunStatus::as_str), "createdAt": j.created_at().map(ToString::to_string), "createdBy": j.created_by()}))
                    .collect();
                    ok_result(json!({"jobs": jobs, "totalResults": output.total_results()}))
                }
                Err(e) => error_json("DeadlineError", &deadline_api::client::format_sdk_error(&e)),
            }
        })
    }

    /// Submit an Open Job Description job bundle to AWS Deadline Cloud.
    #[tool(name = "deadline_submit_job")]
    async fn submit_job(&self, Parameters(p): Parameters<SubmitJobParams>) -> String {
        with_mcp_telemetry!("deadline_submit_job", {
            let start = std::time::Instant::now();

            // Validate directory
            let path = std::path::Path::new(&p.job_bundle_dir);
            if !path.exists() {
                return error_json(
                    "ValueError",
                    &format!("Job bundle directory does not exist: {}", p.job_bundle_dir),
                );
            }
            if !path.is_dir() {
                return error_json(
                    "ValueError",
                    &format!("Path is not a directory: {}", p.job_bundle_dir),
                );
            }

            // Parse job_parameters
            let parsed_params: Vec<Value> = if let Some(ref params_str) = p.job_parameters {
                match serde_json::from_str::<Value>(params_str) {
                    Ok(Value::Array(arr)) => arr,
                    Ok(_) => {
                        return error_json("ValueError", "job_parameters must be a JSON array");
                    }
                    Err(e) => {
                        return error_json(
                            "ValueError",
                            &format!("job_parameters is not valid JSON: {e}"),
                        );
                    }
                }
            } else {
                Vec::new()
            };

            // Parse known_asset_paths
            let parsed_known_asset_paths: Vec<String> =
                if let Some(ref paths_str) = p.known_asset_paths {
                    match serde_json::from_str::<Value>(paths_str) {
                        Ok(Value::Array(arr)) => arr
                            .into_iter()
                            .filter_map(|v| v.as_str().map(String::from))
                            .collect(),
                        Ok(_) => {
                            return error_json(
                                "ValueError",
                                "known_asset_paths must be a JSON array of strings",
                            );
                        }
                        Err(e) => {
                            return error_json(
                                "ValueError",
                                &format!("known_asset_paths is not valid JSON: {e}"),
                            );
                        }
                    }
                } else {
                    Vec::new()
                };

            // Resolve farm_id / queue_id from params or config
            let farm_id = p.farm_id.unwrap_or_else(|| {
                deadline_config::config_file::get_setting_from_disk("defaults.farm_id")
                    .unwrap_or_default()
            });
            if farm_id.is_empty() {
                return error_json("ValueError", "farm_id is required");
            }
            let queue_id = p.queue_id.unwrap_or_else(|| {
                deadline_config::config_file::get_setting_from_disk("defaults.queue_id")
                    .unwrap_or_default()
            });
            if queue_id.is_empty() {
                return error_json("ValueError", "queue_id is required");
            }

            // Build config with overrides
            let mut config = deadline_config::config_file::read_config().unwrap_or_default();
            deadline_config::config_file::set_setting("defaults.farm_id", &farm_id, &mut config)
                .expect("known valid setting");
            deadline_config::config_file::set_setting("defaults.queue_id", &queue_id, &mut config)
                .expect("known valid setting");
            if let Some(ref sp) = p.storage_profile_id {
                deadline_config::config_file::set_setting(
                    "defaults.storage_profile_id",
                    sp,
                    &mut config,
                )
                .expect("known valid setting");
            }

            let bundle_dir = p.job_bundle_dir.clone();
            let job_params = parsed_params;
            let name = p.name;
            let priority = p.priority;
            let max_failed = p.max_failed_tasks_count;
            let max_retries = p.max_retries_per_task;
            let max_workers = p.max_worker_count;
            let fs_type = p.job_attachments_file_system;
            let require_paths = p.require_paths_exist.unwrap_or(false);
            let submitter = p.submitter_name.unwrap_or_else(|| "MCP".to_owned());

            let handle = tokio::runtime::Handle::current();
            let result = std::thread::spawn(move || {
                handle.block_on(async {
                    let submit_params = deadline_job_bundle::submission::SubmitJobParams {
                        job_bundle_dir: bundle_dir,
                        job_parameters: job_params,
                        name,
                        priority,
                        max_failed_tasks_count: max_failed,
                        max_retries_per_task: max_retries,
                        max_worker_count: max_workers,
                        target_task_run_status: None,
                        job_attachments_file_system: fs_type,
                        require_paths_exist: require_paths,
                        submitter_name: Some(submitter),
                        known_asset_paths: parsed_known_asset_paths,
                        auto_accept: true,
                        force_s3_check: None,
                        debug_snapshot_dir: None,
                        config: Some(&config),
                        print_callback: Box::new(|_| {}),
                        hashing_progress_callback: None,
                        upload_progress_callback: None,
                        continue_callback: None,
                        interactive_confirmation_callback: None,
                        telemetry: None,
                    };
                    deadline_job_bundle::submission::create_job_from_job_bundle(submit_params).await
                })
            })
            .join();

            match result {
                Ok(Ok(Some(job_id))) => {
                    let elapsed = start.elapsed().as_secs_f64();
                    ok_result(json!({
                        "status": "success",
                        "job_id": job_id,
                        "message": format!("Successfully submitted job bundle from {}", p.job_bundle_dir),
                        "total_time_seconds": (elapsed * 10.0).round() / 10.0,
                    }))
                }
                Ok(Ok(None)) => error_json("SubmissionError", "Job submission returned no job ID"),
                Ok(Err(e)) => error_json("DeadlineError", &e.to_string()),
                Err(_) => error_json("DeadlineError", "Job submission task panicked"),
            }
        })
    }

    /// Download job output files from AWS Deadline Cloud.
    #[tool(name = "deadline_download_job_output")]
    async fn download_job_output(
        &self,
        Parameters(p): Parameters<DownloadJobOutputParams>,
    ) -> String {
        with_mcp_telemetry!("deadline_download_job_output", {
            let start = std::time::Instant::now();

            // Validation
            if p.task_id.is_some() && p.step_id.is_none() {
                return error_json("ValueError", "step_id is required when task_id is provided");
            }
            let job_id = match p.job_id {
                Some(ref id) if !id.is_empty() => id.clone(),
                _ => return error_json("ValueError", "job_id is required"),
            };
            if let Some(ref cr) = p.conflict_resolution {
                let upper = cr.to_uppercase();
                if upper
                    .parse::<deadline_job_attachments::models::FileConflictResolution>()
                    .is_err()
                {
                    return error_json(
                        "ValueError",
                        &format!(
                            "Invalid conflict_resolution: {cr}. Must be SKIP, OVERWRITE, or CREATE_COPY"
                        ),
                    );
                }
            }

            // Resolve farm/queue from params or config
            let farm_id = p.farm_id.unwrap_or_else(|| {
                deadline_config::config_file::get_setting_from_disk("defaults.farm_id")
                    .unwrap_or_default()
            });
            let queue_id = p.queue_id.unwrap_or_else(|| {
                deadline_config::config_file::get_setting_from_disk("defaults.queue_id")
                    .unwrap_or_default()
            });

            if farm_id.is_empty() {
                return error_json("ValueError", "farm_id is required");
            }
            if queue_id.is_empty() {
                return error_json("ValueError", "queue_id is required");
            }

            let mut config = deadline_config::config_file::read_config().unwrap_or_default();
            deadline_config::config_file::set_setting("defaults.farm_id", &farm_id, &mut config)
                .expect("known valid setting");
            deadline_config::config_file::set_setting("defaults.queue_id", &queue_id, &mut config)
                .expect("known valid setting");
            deadline_config::config_file::set_setting("settings.auto_accept", "true", &mut config)
                .expect("known valid setting");
            if let Some(ref cr) = p.conflict_resolution {
                deadline_config::config_file::set_setting(
                    "settings.conflict_resolution",
                    &cr.to_uppercase(),
                    &mut config,
                )
                .expect("known valid setting");
            }

            let conflict = p.conflict_resolution.as_deref().and_then(|cr| {
                cr.parse::<deadline_job_attachments::models::FileConflictResolution>()
                    .ok()
            });

            let step_id = p.step_id;
            let task_id = p.task_id;
            let job_id_for_result = job_id.clone();
            let step_id_for_result = step_id.clone();
            let task_id_for_result = task_id.clone();
            let handle = tokio::runtime::Handle::current();
            let result = std::thread::spawn(move || {
                handle.block_on(async {
                    super::job::download_output_impl(
                        &config,
                        &farm_id,
                        &queue_id,
                        &job_id,
                        step_id.as_deref(),
                        task_id.as_deref(),
                        conflict,
                        false,
                        true,
                        None,
                        "LOCAL",
                    )
                    .await
                })
            })
            .join();

            match result {
                Ok(Ok(())) => {
                    let elapsed = start.elapsed().as_secs_f64();
                    ok_result(json!({
                        "status": "success",
                        "job_id": job_id_for_result,
                        "step_id": step_id_for_result,
                        "task_id": task_id_for_result,
                        "total_time_seconds": (elapsed * 10.0).round() / 10.0,
                    }))
                }
                Ok(Err(e)) => error_json("DeadlineError", &e.to_string()),
                Err(_) => error_json("DeadlineError", "Download task panicked"),
            }
        })
    }

    /// Get both session logs AND worker logs for a session in one call.
    #[tool(name = "deadline_get_session_and_worker_logs")]
    async fn get_session_and_worker_logs(
        &self,
        Parameters(p): Parameters<GetSessionAndWorkerLogsParams>,
    ) -> String {
        with_mcp_telemetry!("deadline_get_session_and_worker_logs", {
            let limit = p.limit.unwrap_or(100);

            // Get session details
            let session = match deadline_api::session::deadline_client(None)
                .await
                .get_session()
                .farm_id(&p.farm_id)
                .queue_id(&p.queue_id)
                .job_id(&p.job_id)
                .session_id(&p.session_id)
                .send()
                .await
            {
                Ok(v) => v,
                Err(e) => {
                    return error_json(
                        "DeadlineError",
                        &deadline_api::client::format_sdk_error(&e),
                    );
                }
            };

            let worker_id = {
                let wid = session.worker_id();
                if wid.is_empty() {
                    None
                } else {
                    Some(wid.to_owned())
                }
            };
            let fleet_id = {
                let fid = session.fleet_id();
                if fid.is_empty() {
                    None
                } else {
                    Some(fid.to_owned())
                }
            };

            let host_props = session
                .host_properties()
                .map(deadline_api::type_conversions::host_properties_to_value);

            let mut result = json!({
                "session_id": p.session_id,
                "worker_id": worker_id,
                "fleet_id": fleet_id,
                "lifecycle_status": session.lifecycle_status().as_str(),
                "host_properties": host_props,
            });

            // Get session logs
            match deadline_api::log_retrieval::get_session_logs(
                &p.farm_id,
                &p.queue_id,
                Some(&p.session_id),
                None,
                limit,
                None,
                None,
                None,
                None,
            )
            .await
            {
                Ok((r, _)) => {
                    result["session_logs"] = json!({
                        "log_group": r.log_group,
                        "events": r.events.iter().map(|e| json!({
                            "timestamp": e.timestamp.to_string(),
                            "message": e.message,
                        })).collect::<Vec<_>>(),
                        "count": r.count,
                    });
                }
                Err(e) => {
                    result["session_logs"] =
                        json!({"events": [], "count": 0, "error": e.to_string()});
                }
            }

            // Get worker logs if worker_id and fleet_id are present
            if let (Some(wid), Some(fid)) = (&worker_id, &fleet_id) {
                match deadline_api::log_retrieval::get_worker_logs(
                    &p.farm_id, fid, wid, limit, None, None, None, None,
                )
                .await
                {
                    Ok(r) => {
                        result["worker_logs"] = json!({
                            "log_group": r.log_group,
                            "events": r.events.iter().map(|e| json!({
                                "timestamp": e.timestamp.to_string(),
                                "message": e.message,
                            })).collect::<Vec<_>>(),
                            "count": r.count,
                        });
                    }
                    Err(e) => {
                        result["worker_logs"] =
                            json!({"events": [], "count": 0, "error": e.to_string()});
                    }
                }
            } else {
                result["worker_logs"] = json!({"events": [], "count": 0});
            }

            ok_result(result)
        })
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

pub(crate) fn run() -> Result<(), CliError> {
    tokio::runtime::Runtime::new()
        .map_err(|e| CliError::Operation(format!("Failed to start async runtime: {e}")))?
        .block_on(async {
            // Emit server startup telemetry
            let tc = deadline_api::telemetry::create_telemetry(None);
            let mut details = std::collections::HashMap::new();
            details.insert("usage_mode".into(), json!("MCP"));
            details.insert("startup_method".into(), json!("cli"));
            tc.record_event("com.amazon.rum.deadline.mcp.server_startup", details, false);

            let service = DeadlineServer
                .serve(stdio())
                .await
                .map_err(|e| CliError::Operation(format!("MCP server failed to start: {e}")))?;
            service
                .waiting()
                .await
                .map_err(|e| CliError::Operation(format!("MCP server error: {e}")))?;
            Ok(())
        })
}
