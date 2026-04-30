//! Level 2 tests for `deadline mcp-server`.
//!
//! These tests spawn the MCP server as a child process and connect an rmcp
//! client over stdio.

use deadline_test_server::deadline_api::{cloudwatch, errors, farms, fleets, jobs, queues, sessions, sts};
use deadline_test_server::TestHarness;
use rmcp::model::CallToolRequestParams;
use rmcp::transport::{ConfigureCommandExt, TokioChildProcess};
use rmcp::ServiceExt;
use serde_json::Value;
use tokio::process::Command;

/// Helper: spawn the MCP server as a child process, return an rmcp client.
async fn mcp_client(harness: &TestHarness) -> rmcp::service::RunningService<rmcp::RoleClient, ()> {
    let bin = assert_cmd::cargo::cargo_bin("deadline");
    let ep = harness.endpoint_url();
    let config_path = harness.config_path.clone();
    let home = harness.config_dir.path().to_str().unwrap().to_string();

    let transport = TokioChildProcess::new(Command::new(&bin).configure(move |cmd| {
        cmd.arg("mcp-server");
        cmd.env("AWS_ENDPOINT_URL_DEADLINE", &ep);
        cmd.env("AWS_ENDPOINT_URL_STS", &ep);
        cmd.env("AWS_ENDPOINT_URL_CLOUDWATCHLOGS", &ep);
        cmd.env("AWS_ENDPOINT_URL_S3", &ep);
        cmd.env("AWS_ACCESS_KEY_ID", "AKIAIOSFODNN7EXAMPLE");
        cmd.env("AWS_SECRET_ACCESS_KEY", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
        cmd.env("AWS_DEFAULT_REGION", "us-west-2");
        cmd.env("DEADLINE_CONFIG_FILE_PATH", &config_path);
        cmd.env("HOME", &home);
        for var in &[
            "AWS_PROFILE", "AWS_DEFAULT_PROFILE", "AWS_CONFIG_FILE",
            "AWS_SHARED_CREDENTIALS_FILE", "AWS_SESSION_TOKEN",
            "AWS_SECURITY_TOKEN", "AWS_ENDPOINT_URL",
        ] {
            cmd.env_remove(var);
        }
    }))
    .expect("failed to spawn mcp-server");

    ().serve(transport).await.expect("failed to initialize MCP client")
}

fn result_json(result: &rmcp::model::CallToolResult) -> Value {
    let text = result.content.first().and_then(|c| c.raw.as_text())
        .expect("expected text content in tool result");
    serde_json::from_str(&text.text).expect("expected valid JSON in tool result")
}

// ---------------------------------------------------------------------------
// Server exposes all 16 expected tools with deadline_ prefix
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mcp_server_lists_all_expected_tools() {
    let harness = TestHarness::new().await;
    let client = mcp_client(&harness).await;

    let tools = client.list_all_tools().await.expect("list_all_tools failed");
    let mut tool_names: Vec<String> = tools.iter().map(|t| t.name.to_string()).collect();
    tool_names.sort();

    let mut expected = vec![
        "deadline_list_farms", "deadline_list_queues", "deadline_list_jobs",
        "deadline_list_fleets", "deadline_list_storage_profiles_for_queue",
        "deadline_check_authentication_status", "deadline_get_session_logs",
        "deadline_get_job", "deadline_get_session", "deadline_list_sessions",
        "deadline_list_steps", "deadline_list_tasks", "deadline_search_jobs",
        "deadline_submit_job", "deadline_download_job_output",
        "deadline_get_session_and_worker_logs",
    ];
    expected.sort();

    assert_eq!(tool_names, expected);
    client.cancel().await.unwrap();
}

// ---------------------------------------------------------------------------
// Server instructions contain debugging workflow and key concepts
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mcp_server_instructions_contain_debugging_workflow() {
    let harness = TestHarness::new().await;
    let client = mcp_client(&harness).await;

    let info = client.peer_info();
    let server_info = info.expect("server should have peer info");
    let instructions = server_info.instructions.as_deref()
        .expect("server should have instructions");

    assert!(instructions.contains("Debugging Failed Jobs Workflow"));
    assert!(instructions.contains("Key Concepts"));
    assert!(instructions.contains("Configuration"));
    client.cancel().await.unwrap();
}

// ---------------------------------------------------------------------------
// Auto-paginating list tools do NOT expose maxResults parameter
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mcp_list_tools_do_not_expose_max_results_param() {
    let harness = TestHarness::new().await;
    let client = mcp_client(&harness).await;

    let tools = client.list_all_tools().await.expect("list_all_tools failed");
    let auto_paginating = [
        "deadline_list_farms", "deadline_list_queues", "deadline_list_jobs",
        "deadline_list_fleets", "deadline_list_storage_profiles_for_queue",
        "deadline_list_sessions", "deadline_list_steps", "deadline_list_tasks",
    ];

    let tool_names: Vec<String> = tools.iter().map(|t| t.name.to_string()).collect();
    for expected in &auto_paginating {
        assert!(tool_names.contains(&expected.to_string()), "Expected tool {expected} not found");
    }

    for tool in &tools {
        let name = tool.name.to_string();
        if auto_paginating.contains(&name.as_str()) {
            let schema_str = serde_json::to_string(&tool.input_schema).unwrap_or_default();
            assert!(
                !schema_str.contains("max_results") && !schema_str.contains("maxResults"),
                "{name} should not expose maxResults parameter"
            );
        }
    }
    client.cancel().await.unwrap();
}

// ---------------------------------------------------------------------------
// Tool descriptions are present
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mcp_tools_have_descriptions() {
    let harness = TestHarness::new().await;
    let client = mcp_client(&harness).await;

    let tools = client.list_all_tools().await.expect("list_all_tools failed");
    assert!(!tools.is_empty(), "Server should have tools registered");

    for tool in &tools {
        assert!(
            tool.description.is_some() && !tool.description.as_ref().unwrap().is_empty(),
            "Tool {} should have a non-empty description", tool.name
        );
    }
    client.cancel().await.unwrap();
}

// ---------------------------------------------------------------------------
// check_authentication_status returns status
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mcp_check_authentication_status_returns_status() {
    let harness = TestHarness::new().await;
    sts::mock_get_caller_identity(&harness.server).await;
    farms::mock_list_farms(&harness.server, &[]).await;

    let client = mcp_client(&harness).await;
    let result = client
        .call_tool(CallToolRequestParams::new("deadline_check_authentication_status"))
        .await.expect("call_tool failed");

    let json = result_json(&result);
    assert!(json["status"].is_string(), "expected status field: {json}");
    client.cancel().await.unwrap();
}

// ---------------------------------------------------------------------------
// Calling an unregistered tool returns an error
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mcp_call_unregistered_tool_returns_error() {
    let harness = TestHarness::new().await;
    let client = mcp_client(&harness).await;

    let result = client
        .call_tool(CallToolRequestParams::new("deadline_nonexistent_tool"))
        .await;

    assert!(result.is_err(), "expected error for unregistered tool");
    client.cancel().await.unwrap();
}

// ---------------------------------------------------------------------------
// list_farms returns farm data
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mcp_list_farms_returns_farm_data() {
    let harness = TestHarness::new().await;
    farms::mock_list_farms(&harness.server, &[
        serde_json::json!({"farmId": "farm-aaa", "displayName": "Test Farm"}),
    ]).await;

    let client = mcp_client(&harness).await;
    let result = client
        .call_tool(CallToolRequestParams::new("deadline_list_farms"))
        .await.expect("call_tool should succeed at protocol level");

    let json = result_json(&result);
    let farms = json["farms"].as_array().expect("expected farms array");
    assert_eq!(farms.len(), 1);
    assert_eq!(farms[0]["farmId"], "farm-aaa");
    assert_eq!(farms[0]["displayName"], "Test Farm");
    client.cancel().await.unwrap();
}

// ---------------------------------------------------------------------------
// list_queues returns queue data
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mcp_list_queues_returns_queue_data() {
    let harness = TestHarness::new().await;
    queues::mock_list_queues(&harness.server, "farm-aaa", &[
        serde_json::json!({"queueId": "queue-bbb", "displayName": "Render Queue"}),
    ]).await;

    let client = mcp_client(&harness).await;

    let mut args = serde_json::Map::new();
    args.insert("farm_id".into(), Value::String("farm-aaa".into()));

    let result = client
        .call_tool(CallToolRequestParams::new("deadline_list_queues").with_arguments(args))
        .await.expect("call_tool should succeed at protocol level");

    let json = result_json(&result);
    let queues = json["queues"].as_array().expect("expected queues array");
    assert_eq!(queues.len(), 1);
    assert_eq!(queues[0]["queueId"], "queue-bbb");
    client.cancel().await.unwrap();
}

// ---------------------------------------------------------------------------
// get_job returns job details
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mcp_get_job_returns_job_details() {
    let harness = TestHarness::new().await;
    jobs::mock_get_job(&harness.server, "farm-aaa", "queue-bbb", serde_json::json!({
        "jobId": "job-ccc",
        "name": "My Render",
        "lifecycleStatus": "CREATE_COMPLETE",
    })).await;

    let client = mcp_client(&harness).await;

    let mut args = serde_json::Map::new();
    args.insert("farm_id".into(), Value::String("farm-aaa".into()));
    args.insert("queue_id".into(), Value::String("queue-bbb".into()));
    args.insert("job_id".into(), Value::String("job-ccc".into()));

    let result = client
        .call_tool(CallToolRequestParams::new("deadline_get_job").with_arguments(args))
        .await.expect("call_tool should succeed at protocol level");

    let json = result_json(&result);
    assert_eq!(json["jobId"], "job-ccc");
    assert_eq!(json["name"], "My Render");
    client.cancel().await.unwrap();
}

// ---------------------------------------------------------------------------
// API error → tool returns error dict (not protocol error)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mcp_list_farms_api_error_returns_error_dict() {
    let harness = TestHarness::new().await;
    errors::mock_list_farms_access_denied(&harness.server).await;

    let client = mcp_client(&harness).await;
    let result = client
        .call_tool(CallToolRequestParams::new("deadline_list_farms"))
        .await.expect("call_tool should succeed at protocol level");

    let json = result_json(&result);
    assert!(json["error"].is_string(), "expected error field: {json}");
    assert!(json["type"].is_string(), "expected type field: {json}");
    client.cancel().await.unwrap();
}

// ---------------------------------------------------------------------------
// Missing required params → protocol error
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mcp_get_job_missing_required_params_returns_error() {
    let harness = TestHarness::new().await;
    let client = mcp_client(&harness).await;

    let result = client
        .call_tool(CallToolRequestParams::new("deadline_get_job"))
        .await;

    assert!(result.is_err(), "expected error for missing required params");
    let err_msg = format!("{}", result.unwrap_err());
    assert!(
        err_msg.contains("farm_id") || err_msg.contains("deserialize"),
        "error should mention missing field: {err_msg}"
    );
    client.cancel().await.unwrap();
}

// ---------------------------------------------------------------------------
// search_jobs with status filter
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mcp_search_jobs_with_status_filter() {
    let harness = TestHarness::new().await;
    jobs::mock_search_jobs(&harness.server, "farm-aaa", &[
        serde_json::json!({"jobId": "job-fail", "name": "Failed Job"}),
    ], 1).await;

    let client = mcp_client(&harness).await;

    let mut args = serde_json::Map::new();
    args.insert("farm_id".into(), Value::String("farm-aaa".into()));
    args.insert("queue_ids".into(), serde_json::json!(["queue-bbb"]).into());
    args.insert("task_run_status".into(), Value::String("FAILED".into()));

    let result = client
        .call_tool(CallToolRequestParams::new("deadline_search_jobs").with_arguments(args))
        .await.expect("call_tool should succeed at protocol level");

    let json = result_json(&result);
    let jobs = json["jobs"].as_array().expect("expected jobs array");
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0]["jobId"], "job-fail");
    client.cancel().await.unwrap();
}

// ---------------------------------------------------------------------------
// null param filtering — list_farms with null next_token still works
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mcp_list_farms_with_null_params_still_works() {
    let harness = TestHarness::new().await;
    farms::mock_list_farms(&harness.server, &[
        serde_json::json!({"farmId": "farm-xxx", "displayName": "Null Param Farm"}),
    ]).await;

    let client = mcp_client(&harness).await;

    let mut args = serde_json::Map::new();
    args.insert("next_token".into(), Value::Null);

    let result = client
        .call_tool(CallToolRequestParams::new("deadline_list_farms").with_arguments(args))
        .await.expect("call_tool should succeed at protocol level");

    let json = result_json(&result);
    let farms = json["farms"].as_array().expect("expected farms array");
    assert_eq!(farms.len(), 1);
    assert_eq!(farms[0]["farmId"], "farm-xxx");
    client.cancel().await.unwrap();
}

// ---------------------------------------------------------------------------
// submit_job: directory does not exist
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mcp_submit_job_dir_not_exist_returns_error() {
    let harness = TestHarness::new().await;
    let client = mcp_client(&harness).await;

    let mut args = serde_json::Map::new();
    args.insert("job_bundle_dir".into(), Value::String("/nonexistent/path".into()));

    let result = client
        .call_tool(CallToolRequestParams::new("deadline_submit_job").with_arguments(args))
        .await.expect("call_tool should succeed at protocol level");

    let json = result_json(&result);
    assert!(json["error"].as_str().unwrap().contains("does not exist"), "expected 'does not exist': {json}");
    client.cancel().await.unwrap();
}

// ---------------------------------------------------------------------------
// submit_job: path is a file, not a directory
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mcp_submit_job_path_is_file_returns_error() {
    let harness = TestHarness::new().await;
    let client = mcp_client(&harness).await;

    let mut args = serde_json::Map::new();
    args.insert("job_bundle_dir".into(), Value::String(harness.config_path.clone().into()));

    let result = client
        .call_tool(CallToolRequestParams::new("deadline_submit_job").with_arguments(args))
        .await.expect("call_tool should succeed at protocol level");

    let json = result_json(&result);
    assert!(json["error"].as_str().unwrap().contains("not a directory"), "expected 'not a directory': {json}");
    client.cancel().await.unwrap();
}

// ---------------------------------------------------------------------------
// submit_job: job_parameters is not a JSON array
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mcp_submit_job_params_not_array_returns_error() {
    let harness = TestHarness::new().await;
    let client = mcp_client(&harness).await;

    let dir = harness.config_dir.path().to_str().unwrap().to_string();
    let mut args = serde_json::Map::new();
    args.insert("job_bundle_dir".into(), Value::String(dir));
    args.insert("job_parameters".into(), Value::String(r#"{"not": "array"}"#.into()));

    let result = client
        .call_tool(CallToolRequestParams::new("deadline_submit_job").with_arguments(args))
        .await.expect("call_tool should succeed at protocol level");

    let json = result_json(&result);
    assert!(json["error"].as_str().unwrap().contains("JSON array"), "expected 'JSON array': {json}");
    client.cancel().await.unwrap();
}

// ---------------------------------------------------------------------------
// submit_job: farm_id not provided and not in config
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mcp_submit_job_no_farm_id_returns_error() {
    let harness = TestHarness::new().await;
    let client = mcp_client(&harness).await;

    let dir = harness.config_dir.path().to_str().unwrap().to_string();
    let mut args = serde_json::Map::new();
    args.insert("job_bundle_dir".into(), Value::String(dir));

    let result = client
        .call_tool(CallToolRequestParams::new("deadline_submit_job").with_arguments(args))
        .await.expect("call_tool should succeed at protocol level");

    let json = result_json(&result);
    assert!(json["error"].as_str().unwrap().contains("farm_id"), "expected 'farm_id': {json}");
    client.cancel().await.unwrap();
}

// ---------------------------------------------------------------------------
// submit_job: queue_id not provided and not in config
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mcp_submit_job_no_queue_id_returns_error() {
    let harness = TestHarness::new().await;
    let client = mcp_client(&harness).await;

    let dir = harness.config_dir.path().to_str().unwrap().to_string();
    let mut args = serde_json::Map::new();
    args.insert("job_bundle_dir".into(), Value::String(dir));
    args.insert("farm_id".into(), Value::String("farm-aaa".into()));

    let result = client
        .call_tool(CallToolRequestParams::new("deadline_submit_job").with_arguments(args))
        .await.expect("call_tool should succeed at protocol level");

    let json = result_json(&result);
    assert!(json["error"].as_str().unwrap().contains("queue_id"), "expected 'queue_id': {json}");
    client.cancel().await.unwrap();
}

// ---------------------------------------------------------------------------
// download_job_output: task_id without step_id
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mcp_download_task_without_step_returns_error() {
    let harness = TestHarness::new().await;
    let client = mcp_client(&harness).await;

    let mut args = serde_json::Map::new();
    args.insert("job_id".into(), Value::String("job-aaa".into()));
    args.insert("task_id".into(), Value::String("task-bbb".into()));

    let result = client
        .call_tool(CallToolRequestParams::new("deadline_download_job_output").with_arguments(args))
        .await.expect("call_tool should succeed at protocol level");

    let json = result_json(&result);
    assert!(json["error"].as_str().unwrap().contains("step_id"), "expected 'step_id required': {json}");
    client.cancel().await.unwrap();
}

// ---------------------------------------------------------------------------
// download_job_output: no job_id
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mcp_download_no_job_id_returns_error() {
    let harness = TestHarness::new().await;
    let client = mcp_client(&harness).await;

    let result = client
        .call_tool(CallToolRequestParams::new("deadline_download_job_output"))
        .await.expect("call_tool should succeed at protocol level");

    let json = result_json(&result);
    assert!(json["error"].as_str().unwrap().contains("job_id"), "expected 'job_id required': {json}");
    client.cancel().await.unwrap();
}

// ---------------------------------------------------------------------------
// download_job_output: invalid conflict_resolution
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mcp_download_invalid_conflict_resolution_returns_error() {
    let harness = TestHarness::new().await;
    let client = mcp_client(&harness).await;

    let mut args = serde_json::Map::new();
    args.insert("job_id".into(), Value::String("job-aaa".into()));
    args.insert("conflict_resolution".into(), Value::String("INVALID".into()));

    let result = client
        .call_tool(CallToolRequestParams::new("deadline_download_job_output").with_arguments(args))
        .await.expect("call_tool should succeed at protocol level");

    let json = result_json(&result);
    let err = json["error"].as_str().unwrap();
    assert!(err.contains("SKIP") || err.contains("OVERWRITE") || err.contains("CREATE_COPY"),
        "expected valid options in error: {json}");
    client.cancel().await.unwrap();
}

// ---------------------------------------------------------------------------
// get_session_and_worker_logs: happy path with worker and fleet IDs
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mcp_session_and_worker_logs_returns_both() {
    let harness = TestHarness::new().await;

    sessions::mock_get_session(&harness.server, "farm-aaa", "queue-bbb", "job-ccc",
        serde_json::json!({
            "sessionId": "session-ddd",
            "workerId": "worker-eee",
            "fleetId": "fleet-fff",
            "lifecycleStatus": "ENDED",
        }),
    ).await;

    cloudwatch::mock_get_log_events(&harness.server, &[
        serde_json::json!({"timestamp": 1700000000000_i64, "message": "Rendering frame 1"}),
    ], None).await;

    fleets::mock_assume_fleet_role_for_read(&harness.server, "farm-aaa", "fleet-fff",
        serde_json::json!({
            "credentials": {
                "accessKeyId": "AKIAIOSFODNN7EXAMPLE",
                "secretAccessKey": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
                "sessionToken": "FakeSessionToken",
                "expiration": "2099-01-01T00:00:00Z"
            }
        }),
    ).await;

    let client = mcp_client(&harness).await;

    let mut args = serde_json::Map::new();
    args.insert("farm_id".into(), Value::String("farm-aaa".into()));
    args.insert("queue_id".into(), Value::String("queue-bbb".into()));
    args.insert("job_id".into(), Value::String("job-ccc".into()));
    args.insert("session_id".into(), Value::String("session-ddd".into()));

    let result = client
        .call_tool(CallToolRequestParams::new("deadline_get_session_and_worker_logs").with_arguments(args))
        .await.expect("call_tool should succeed at protocol level");

    let json = result_json(&result);
    assert_eq!(json["session_id"], "session-ddd");
    assert_eq!(json["worker_id"], "worker-eee");
    assert_eq!(json["fleet_id"], "fleet-fff");
    assert!(json["session_logs"]["events"].is_array());
    assert!(json["worker_logs"].is_object());
    client.cancel().await.unwrap();
}

// ---------------------------------------------------------------------------
// get_session_and_worker_logs: no worker_id — worker logs empty
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mcp_session_and_worker_logs_no_worker_id() {
    let harness = TestHarness::new().await;

    sessions::mock_get_session(&harness.server, "farm-aaa", "queue-bbb", "job-ccc",
        serde_json::json!({
            "sessionId": "session-ddd",
            "lifecycleStatus": "STARTED",
            "fleetId": "", "workerId": "",
            "startedAt": "2024-01-15T10:30:00Z",
        }),
    ).await;

    cloudwatch::mock_get_log_events(&harness.server, &[
        serde_json::json!({"timestamp": 1700000000000_i64, "message": "Starting"}),
    ], None).await;

    let client = mcp_client(&harness).await;

    let mut args = serde_json::Map::new();
    args.insert("farm_id".into(), Value::String("farm-aaa".into()));
    args.insert("queue_id".into(), Value::String("queue-bbb".into()));
    args.insert("job_id".into(), Value::String("job-ccc".into()));
    args.insert("session_id".into(), Value::String("session-ddd".into()));

    let result = client
        .call_tool(CallToolRequestParams::new("deadline_get_session_and_worker_logs").with_arguments(args))
        .await.expect("call_tool should succeed at protocol level");

    let json = result_json(&result);
    assert_eq!(json["worker_logs"]["count"], 0);
    assert!(json["worker_logs"]["events"].as_array().unwrap().is_empty());
    assert!(json["session_logs"]["events"].is_array());
    client.cancel().await.unwrap();
}

// ---------------------------------------------------------------------------
// get_session_and_worker_logs: worker log permission error — graceful
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mcp_session_and_worker_logs_permission_error() {
    let harness = TestHarness::new().await;

    sessions::mock_get_session(&harness.server, "farm-aaa", "queue-bbb", "job-ccc",
        serde_json::json!({
            "sessionId": "session-ddd",
            "workerId": "worker-eee",
            "fleetId": "fleet-fff",
            "lifecycleStatus": "ENDED",
        }),
    ).await;

    // CloudWatch returns access denied — both session and worker logs fail,
    // but the tool should return gracefully with error fields.
    cloudwatch::mock_get_log_events_access_denied(&harness.server).await;

    let client = mcp_client(&harness).await;

    let mut args = serde_json::Map::new();
    args.insert("farm_id".into(), Value::String("farm-aaa".into()));
    args.insert("queue_id".into(), Value::String("queue-bbb".into()));
    args.insert("job_id".into(), Value::String("job-ccc".into()));
    args.insert("session_id".into(), Value::String("session-ddd".into()));

    let result = client
        .call_tool(CallToolRequestParams::new("deadline_get_session_and_worker_logs").with_arguments(args))
        .await.expect("call_tool should succeed at protocol level");

    let json = result_json(&result);
    assert_eq!(json["session_id"], "session-ddd");
    assert!(json["session_logs"]["error"].is_string(), "expected session_logs.error: {json}");
    assert!(json["worker_logs"]["error"].is_string(), "expected worker_logs.error: {json}");
    assert_eq!(json["worker_logs"]["count"], 0);
    client.cancel().await.unwrap();
}

// ---------------------------------------------------------------------------
// get_session_and_worker_logs: custom limit parameter
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mcp_session_and_worker_logs_custom_limit() {
    let harness = TestHarness::new().await;

    sessions::mock_get_session(&harness.server, "farm-aaa", "queue-bbb", "job-ccc",
        serde_json::json!({
            "sessionId": "session-ddd",
            "lifecycleStatus": "ENDED",
        }),
    ).await;

    cloudwatch::mock_get_log_events(&harness.server, &[], None).await;

    let client = mcp_client(&harness).await;

    let mut args = serde_json::Map::new();
    args.insert("farm_id".into(), Value::String("farm-aaa".into()));
    args.insert("queue_id".into(), Value::String("queue-bbb".into()));
    args.insert("job_id".into(), Value::String("job-ccc".into()));
    args.insert("session_id".into(), Value::String("session-ddd".into()));
    args.insert("limit".into(), Value::Number(10.into()));

    let result = client
        .call_tool(CallToolRequestParams::new("deadline_get_session_and_worker_logs").with_arguments(args))
        .await.expect("call_tool should succeed at protocol level");

    let json = result_json(&result);
    assert!(json["session_logs"].is_object(), "expected session_logs: {json}");
    client.cancel().await.unwrap();
}

// ---------------------------------------------------------------------------
// submit_job: job_parameters is invalid JSON
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mcp_submit_job_params_invalid_json_returns_error() {
    let harness = TestHarness::new().await;
    let client = mcp_client(&harness).await;

    let dir = harness.config_dir.path().to_str().unwrap().to_string();
    let mut args = serde_json::Map::new();
    args.insert("job_bundle_dir".into(), Value::String(dir));
    args.insert("job_parameters".into(), Value::String("not valid json".into()));

    let result = client
        .call_tool(CallToolRequestParams::new("deadline_submit_job").with_arguments(args))
        .await.expect("call_tool should succeed at protocol level");

    let json = result_json(&result);
    assert!(json["error"].as_str().unwrap().contains("JSON") || json["error"].as_str().unwrap().contains("parse"),
        "expected JSON parse error: {json}");
    client.cancel().await.unwrap();
}

// ---------------------------------------------------------------------------
// download_job_output: valid conflict_resolution accepted
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mcp_download_valid_conflict_resolution_passes_validation() {
    let harness = TestHarness::new().await;
    let client = mcp_client(&harness).await;

    let mut args = serde_json::Map::new();
    args.insert("job_id".into(), Value::String("job-aaa".into()));
    args.insert("conflict_resolution".into(), Value::String("SKIP".into()));

    let result = client
        .call_tool(CallToolRequestParams::new("deadline_download_job_output").with_arguments(args))
        .await.expect("call_tool should succeed at protocol level");

    let json = result_json(&result);
    // Should pass validation — error (if any) should NOT be about conflict_resolution
    // but could be about missing farm_id/queue_id or actual download failure
    assert!(json.get("error").is_none() || !json["error"].as_str().unwrap_or("").contains("conflict"),
        "SKIP should pass validation, got conflict error: {json}");
    // Must not be the NotImplementedError stub
    assert_ne!(json["error"].as_str().unwrap_or(""), "download_job_output not yet implemented",
        "tool should be implemented, not a stub");
    client.cancel().await.unwrap();
}

// ---------------------------------------------------------------------------
// get_session_and_worker_logs: response includes lifecycle_status
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mcp_session_and_worker_logs_includes_session_details() {
    let harness = TestHarness::new().await;

    sessions::mock_get_session(&harness.server, "farm-aaa", "queue-bbb", "job-ccc",
        serde_json::json!({
            "sessionId": "session-ddd",
            "lifecycleStatus": "ENDED",
            "fleetId": "fleet-aaa", "workerId": "worker-aaa",
            "startedAt": "2024-01-15T10:30:00Z",
            "hostProperties": {"ipAddresses": {"ipV4Addresses": ["10.0.0.1"]}},
        }),
    ).await;

    cloudwatch::mock_get_log_events(&harness.server, &[], None).await;

    let client = mcp_client(&harness).await;

    let mut args = serde_json::Map::new();
    args.insert("farm_id".into(), Value::String("farm-aaa".into()));
    args.insert("queue_id".into(), Value::String("queue-bbb".into()));
    args.insert("job_id".into(), Value::String("job-ccc".into()));
    args.insert("session_id".into(), Value::String("session-ddd".into()));

    let result = client
        .call_tool(CallToolRequestParams::new("deadline_get_session_and_worker_logs").with_arguments(args))
        .await.expect("call_tool should succeed at protocol level");

    let json = result_json(&result);
    assert_eq!(json["lifecycle_status"], "ENDED");
    assert!(json["host_properties"].is_object(), "expected host_properties: {json}");
    client.cancel().await.unwrap();
}
