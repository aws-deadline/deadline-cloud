//! Level 2 tests for `deadline mcp-server`.
//!
//! These tests spawn the MCP server as a child process and connect an rmcp
//! client over stdio.

use deadline_test_server::deadline_api::{errors, farms, jobs, queues, sts};
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
// §50 #4, #13, #38: Server exposes all 16 expected tools with deadline_ prefix
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
// §50 #39: Server instructions contain debugging workflow and key concepts
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
// §50 #4: Auto-paginating list tools do NOT expose maxResults parameter
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
// §50 #5: Tool descriptions are present
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
// §50 #5: check_authentication_status returns status
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
// §50 #2: Calling an unregistered tool returns an error
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
// §50 #5: list_farms returns farm data
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
// §50 #5: list_queues returns queue data
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
// §50 #5: get_job returns job details
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
// §50 #7: API error → tool returns error dict (not protocol error)
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
// §50 #7: Missing required params → protocol error
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
// §50 #5: search_jobs with status filter
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
// §50 #6: null param filtering — list_farms with null next_token still works
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
