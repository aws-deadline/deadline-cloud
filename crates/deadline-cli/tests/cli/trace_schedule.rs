//! Level 2 tests for `deadline job trace-schedule`.

use deadline_test_server::deadline_api::{jobs, sessions};
use deadline_test_server::TestHarness;
use insta_cmd::assert_cmd_snapshot;
use serde_json::json;

const FARM: &str = "farm-abc";
const QUEUE: &str = "queue-abc";
const JOB: &str = "job-aaf4cdf8aae242f58fb84c5bb19f199b";
const STEP: &str = "step-00000000000000000000000000000001";
const WORKER0: &str = "worker-0000000000000000000000000001";
const WORKER1: &str = "worker-0000000000000000000000000002";
const FLEET: &str = "fleet-00000000000000000000000000000001";

async fn setup(harness: &TestHarness) {
    harness.cli(&["config", "set", "defaults.farm_id", FARM]).assert().success();
    harness.cli(&["config", "set", "defaults.queue_id", QUEUE]).assert().success();
}

fn make_step(step_id: &str, name: &str) -> serde_json::Value {
    json!({
        "farmId": FARM,
        "queueId": QUEUE,
        "jobId": JOB,
        "stepId": step_id,
        "name": name,
        "lifecycleStatus": "SUCCEEDED",
        "createdAt": "2025-01-27T07:34:41Z",
        "startedAt": "2025-01-27T07:37:53Z",
        "endedAt": "2025-01-27T07:39:17Z",
    })
}

fn make_task(step_id: &str, task_id: &str, params: serde_json::Value) -> serde_json::Value {
    json!({
        "farmId": FARM,
        "queueId": QUEUE,
        "jobId": JOB,
        "stepId": step_id,
        "taskId": task_id,
        "parameters": params,
        "createdAt": "2025-01-27T07:34:41Z",
        "runStatus": "SUCCEEDED",
    })
}

fn make_session(
    session_id: &str,
    worker_id: &str,
    started_at: &str,
    ended_at: Option<&str>,
) -> serde_json::Value {
    let mut s = json!({
        "sessionId": session_id,
        "workerId": worker_id,
        "fleetId": FLEET,
        "lifecycleStatus": "ENDED",
        "startedAt": started_at,
    });
    if let Some(e) = ended_at {
        s["endedAt"] = json!(e);
    }
    s
}

fn make_task_run_action(
    action_id: &str,
    step_id: &str,
    task_id: &str,
    started_at: &str,
    ended_at: &str,
) -> serde_json::Value {
    json!({
        "sessionActionId": action_id,
        "status": "SUCCEEDED",
        "startedAt": started_at,
        "endedAt": ended_at,
        "definition": {
            "taskRun": {
                "stepId": step_id,
                "taskId": task_id,
            }
        }
    })
}

fn make_env_action(
    action_id: &str,
    env_type: &str,
    env_id: &str,
    started_at: &str,
    ended_at: &str,
) -> serde_json::Value {
    json!({
        "sessionActionId": action_id,
        "status": "SUCCEEDED",
        "startedAt": started_at,
        "endedAt": ended_at,
        "definition": {
            env_type: {
                "environmentId": env_id,
            }
        }
    })
}

// --- Happy path: single session with one task ---

#[tokio::test]
async fn trace_schedule_single_session_prints_summary() {
    let harness = TestHarness::new().await;
    setup(&harness).await;

    // GetJob
    jobs::mock_get_job(&harness.server, FARM, QUEUE, json!({
        "jobId": JOB,
        "name": "Render Job",
        "startedAt": "2025-01-27T07:37:53Z",
        "endedAt": "2025-01-27T07:39:53Z",
        "createdAt": "2025-01-27T07:34:41Z",
    })).await;

    // ListSessions
    let session = make_session("session-001", WORKER0, "2025-01-27T07:37:53Z", Some("2025-01-27T07:38:53Z"));
    sessions::mock_list_sessions(&harness.server, FARM, QUEUE, JOB, &[session]).await;

    // ListSessionActions for session-001
    let action = make_task_run_action(
        "sessionaction-001", STEP, "task-001",
        "2025-01-27T07:37:53Z", "2025-01-27T07:38:53Z",
    );
    sessions::mock_list_session_actions(&harness.server, FARM, QUEUE, JOB, "session-001", &[action]).await;

    // BatchGetStep
    sessions::mock_batch_get_steps(&harness.server, &[make_step(STEP, "Render")], &[]).await;

    // BatchGetTask
    let task = make_task(STEP, "task-001", json!({"Frame": {"int": "1"}}));
    sessions::mock_batch_get_tasks(&harness.server, &[task], &[]).await;

    assert_cmd_snapshot!(harness.cmd(&["job", "trace-schedule", "--job-id", JOB]));
}

// --- Happy path: multiple sessions across workers ---

#[tokio::test]
async fn trace_schedule_multiple_workers_prints_summary() {
    let harness = TestHarness::new().await;
    setup(&harness).await;

    jobs::mock_get_job(&harness.server, FARM, QUEUE, json!({
        "jobId": JOB,
        "name": "Render Job",
        "startedAt": "2025-01-27T07:37:53Z",
        "endedAt": "2025-01-27T07:40:53Z",
        "createdAt": "2025-01-27T07:34:41Z",
    })).await;

    let s1 = make_session("session-001", WORKER0, "2025-01-27T07:37:53Z", Some("2025-01-27T07:38:53Z"));
    let s2 = make_session("session-002", WORKER1, "2025-01-27T07:37:53Z", Some("2025-01-27T07:39:53Z"));
    sessions::mock_list_sessions(&harness.server, FARM, QUEUE, JOB, &[s1, s2]).await;

    // Session 1: 1 task
    let a1 = make_task_run_action("sessionaction-001", STEP, "task-001",
        "2025-01-27T07:37:53Z", "2025-01-27T07:38:53Z");
    sessions::mock_list_session_actions(&harness.server, FARM, QUEUE, JOB, "session-001", &[a1]).await;

    // Session 2: 2 tasks
    let a2 = make_task_run_action("sessionaction-002", STEP, "task-002",
        "2025-01-27T07:37:53Z", "2025-01-27T07:38:23Z");
    let a3 = make_task_run_action("sessionaction-003", STEP, "task-003",
        "2025-01-27T07:38:23Z", "2025-01-27T07:39:53Z");
    sessions::mock_list_session_actions(&harness.server, FARM, QUEUE, JOB, "session-002", &[a2, a3]).await;

    sessions::mock_batch_get_steps(&harness.server, &[make_step(STEP, "Render")], &[]).await;

    let tasks = vec![
        make_task(STEP, "task-001", json!({"Frame": {"int": "1"}})),
        make_task(STEP, "task-002", json!({"Frame": {"int": "2"}})),
        make_task(STEP, "task-003", json!({"Frame": {"int": "3"}})),
    ];
    sessions::mock_batch_get_tasks(&harness.server, &tasks, &[]).await;

    assert_cmd_snapshot!(harness.cmd(&["job", "trace-schedule", "--job-id", JOB]));
}

// --- Happy path: env enter/exit actions ---

#[tokio::test]
async fn trace_schedule_with_env_actions_prints_summary() {
    let harness = TestHarness::new().await;
    setup(&harness).await;

    jobs::mock_get_job(&harness.server, FARM, QUEUE, json!({
        "jobId": JOB,
        "name": "Render Job",
        "startedAt": "2025-01-27T07:37:53Z",
        "endedAt": "2025-01-27T07:39:53Z",
        "createdAt": "2025-01-27T07:34:41Z",
    })).await;

    let session = make_session("session-001", WORKER0, "2025-01-27T07:37:53Z", Some("2025-01-27T07:39:53Z"));
    sessions::mock_list_sessions(&harness.server, FARM, QUEUE, JOB, &[session]).await;

    let env_enter = make_env_action("sessionaction-001", "envEnter",
        "queue-abc:queue-env-001", "2025-01-27T07:37:53Z", "2025-01-27T07:37:58Z");
    let task_run = make_task_run_action("sessionaction-002", STEP, "task-001",
        "2025-01-27T07:37:58Z", "2025-01-27T07:39:48Z");
    let env_exit = make_env_action("sessionaction-003", "envExit",
        "queue-abc:queue-env-001", "2025-01-27T07:39:48Z", "2025-01-27T07:39:53Z");
    sessions::mock_list_session_actions(&harness.server, FARM, QUEUE, JOB, "session-001",
        &[env_enter, task_run, env_exit]).await;

    sessions::mock_batch_get_steps(&harness.server, &[make_step(STEP, "Render")], &[]).await;
    sessions::mock_batch_get_tasks(&harness.server,
        &[make_task(STEP, "task-001", json!({"Frame": {"int": "1"}}))], &[]).await;

    assert_cmd_snapshot!(harness.cmd(&["job", "trace-schedule", "--job-id", JOB]));
}

// --- Error: job hasn't started ---

#[tokio::test]
async fn trace_schedule_job_not_started_errors() {
    let harness = TestHarness::new().await;
    setup(&harness).await;

    jobs::mock_get_job(&harness.server, FARM, QUEUE, json!({
        "jobId": JOB,
        "name": "Render Job",
        "createdAt": "2025-01-27T07:34:41Z",
        "lifecycleStatus": "CREATE_COMPLETE",
    })).await;

    assert_cmd_snapshot!(harness.cmd(&["job", "trace-schedule", "--job-id", JOB]));
}

// --- Error: --trace-file without --trace-format ---

#[tokio::test]
async fn trace_schedule_trace_file_without_format_errors() {
    let harness = TestHarness::new().await;
    setup(&harness).await;

    jobs::mock_get_job(&harness.server, FARM, QUEUE, json!({
        "jobId": JOB,
        "name": "Render Job",
        "startedAt": "2025-01-27T07:37:53Z",
        "createdAt": "2025-01-27T07:34:41Z",
    })).await;

    assert_cmd_snapshot!(harness.cmd(&[
        "job", "trace-schedule", "--job-id", JOB,
        "--trace-file", "/tmp/trace.json",
    ]));
}

// --- Happy path: chrome trace file written ---

#[tokio::test]
async fn trace_schedule_chrome_trace_file_written() {
    let harness = TestHarness::new().await;
    setup(&harness).await;

    jobs::mock_get_job(&harness.server, FARM, QUEUE, json!({
        "jobId": JOB,
        "name": "Render Job",
        "startedAt": "2025-01-27T07:37:53Z",
        "endedAt": "2025-01-27T07:39:53Z",
        "createdAt": "2025-01-27T07:34:41Z",
    })).await;

    let session = make_session("session-001", WORKER0, "2025-01-27T07:37:53Z", Some("2025-01-27T07:38:53Z"));
    sessions::mock_list_sessions(&harness.server, FARM, QUEUE, JOB, &[session]).await;

    let action = make_task_run_action("sessionaction-001", STEP, "task-001",
        "2025-01-27T07:37:53Z", "2025-01-27T07:38:53Z");
    sessions::mock_list_session_actions(&harness.server, FARM, QUEUE, JOB, "session-001", &[action]).await;

    sessions::mock_batch_get_steps(&harness.server, &[make_step(STEP, "Render")], &[]).await;
    sessions::mock_batch_get_tasks(&harness.server,
        &[make_task(STEP, "task-001", json!({"Frame": {"int": "1"}}))], &[]).await;

    let trace_file = harness.config_dir.path().join("trace.json");
    let trace_path = trace_file.to_str().unwrap();

    let mut cmd = harness.cmd(&[
        "job", "trace-schedule", "--job-id", JOB,
        "--trace-format", "chrome",
        "--trace-file", trace_path,
    ]);
    let output = cmd.output().expect("failed to run");
    assert!(output.status.success(), "stderr: {}", String::from_utf8_lossy(&output.stderr));

    // Verify trace file was written and contains valid JSON with expected structure
    let content = std::fs::read_to_string(&trace_file).expect("trace file not written");
    let trace: serde_json::Value = serde_json::from_str(&content).expect("invalid JSON in trace file");
    assert!(trace["traceEvents"].is_array());
    assert_eq!(trace["otherData"]["jobId"], JOB);
    assert_eq!(trace["otherData"]["farmId"], FARM);
    assert!(!trace["traceEvents"].as_array().unwrap().is_empty());
}

// --- Warning: terminal batch error warns but continues ---

#[tokio::test]
async fn trace_schedule_terminal_batch_error_warns_and_continues() {
    let harness = TestHarness::new().await;
    setup(&harness).await;

    jobs::mock_get_job(&harness.server, FARM, QUEUE, json!({
        "jobId": JOB,
        "name": "Render Job",
        "startedAt": "2025-01-27T07:37:53Z",
        "endedAt": "2025-01-27T07:39:53Z",
        "createdAt": "2025-01-27T07:34:41Z",
    })).await;

    let session = make_session("session-001", WORKER0, "2025-01-27T07:37:53Z", Some("2025-01-27T07:38:53Z"));
    sessions::mock_list_sessions(&harness.server, FARM, QUEUE, JOB, &[session]).await;

    let action = make_task_run_action("sessionaction-001", STEP, "task-001",
        "2025-01-27T07:37:53Z", "2025-01-27T07:38:53Z");
    sessions::mock_list_session_actions(&harness.server, FARM, QUEUE, JOB, "session-001", &[action]).await;

    sessions::mock_batch_get_steps(&harness.server, &[make_step(STEP, "Render")], &[]).await;

    // Task batch returns a terminal error instead of the task
    sessions::mock_batch_get_tasks(&harness.server, &[], &[json!({
        "code": "ResourceNotFoundException",
        "taskId": "task-001",
        "stepId": STEP,
        "farmId": FARM,
        "queueId": QUEUE,
        "jobId": JOB,
        "message": "Task not found",
    })]).await;

    assert_cmd_snapshot!(harness.cmd(&["job", "trace-schedule", "--job-id", JOB]));
}

// --- Duration and overhead reporting with controlled timestamps ---

#[tokio::test]
async fn trace_schedule_durations_and_overhead_reported() {
    let harness = TestHarness::new().await;
    setup(&harness).await;

    jobs::mock_get_job(&harness.server, FARM, QUEUE, json!({
        "jobId": JOB,
        "name": "Render Job",
        "startedAt": "2025-01-27T07:37:53Z",
        "endedAt": "2025-01-27T07:39:53Z",
        "createdAt": "2025-01-27T07:34:41Z",
    })).await;

    // Session: 70s total (07:37:53 → 07:39:03)
    // 20s overhead before first action
    let session = make_session("session-001", WORKER0,
        "2025-01-27T07:37:53Z", Some("2025-01-27T07:39:03Z"));
    sessions::mock_list_sessions(&harness.server, FARM, QUEUE, JOB, &[session]).await;

    // env enter: 5s, task run: 40s, env exit: 5s = 50s actions, 20s overhead
    let env_enter = make_env_action("sessionaction-001", "envEnter",
        "queue-abc:Conda", "2025-01-27T07:38:13Z", "2025-01-27T07:38:18Z");
    let task_run = make_task_run_action("sessionaction-002", STEP, "task-001",
        "2025-01-27T07:38:18Z", "2025-01-27T07:38:58Z");
    let env_exit = make_env_action("sessionaction-003", "envExit",
        "queue-abc:Conda", "2025-01-27T07:38:58Z", "2025-01-27T07:39:03Z");
    sessions::mock_list_session_actions(&harness.server, FARM, QUEUE, JOB, "session-001",
        &[env_enter, task_run, env_exit]).await;

    sessions::mock_batch_get_steps(&harness.server, &[make_step(STEP, "Render")], &[]).await;
    sessions::mock_batch_get_tasks(&harness.server,
        &[make_task(STEP, "task-001", json!({"Frame": {"int": "1"}}))], &[]).await;

    assert_cmd_snapshot!(harness.cmd(&["job", "trace-schedule", "--job-id", JOB]));
}

// --- Zero counts for unused action types ---

#[tokio::test]
async fn trace_schedule_zero_counts_for_unused_action_types() {
    let harness = TestHarness::new().await;
    setup(&harness).await;

    jobs::mock_get_job(&harness.server, FARM, QUEUE, json!({
        "jobId": JOB,
        "name": "Render Job",
        "startedAt": "2025-01-27T07:37:53Z",
        "endedAt": "2025-01-27T07:38:53Z",
        "createdAt": "2025-01-27T07:34:41Z",
    })).await;

    // Single session, single task, no env actions
    let session = make_session("session-001", WORKER0,
        "2025-01-27T07:37:53Z", Some("2025-01-27T07:38:53Z"));
    sessions::mock_list_sessions(&harness.server, FARM, QUEUE, JOB, &[session]).await;

    let action = make_task_run_action("sessionaction-001", STEP, "task-001",
        "2025-01-27T07:37:53Z", "2025-01-27T07:38:53Z");
    sessions::mock_list_session_actions(&harness.server, FARM, QUEUE, JOB, "session-001", &[action]).await;

    sessions::mock_batch_get_steps(&harness.server, &[make_step(STEP, "Render")], &[]).await;
    sessions::mock_batch_get_tasks(&harness.server,
        &[make_task(STEP, "task-001", json!({"Frame": {"int": "1"}}))], &[]).await;

    assert_cmd_snapshot!(harness.cmd(&["job", "trace-schedule", "--job-id", JOB]));
}

// --- Task parameters displayed in verbose output ---

#[tokio::test]
async fn trace_schedule_verbose_prints_trace_data() {
    let harness = TestHarness::new().await;
    setup(&harness).await;

    jobs::mock_get_job(&harness.server, FARM, QUEUE, json!({
        "jobId": JOB,
        "name": "Render Job",
        "startedAt": "2025-01-27T07:37:53Z",
        "endedAt": "2025-01-27T07:38:53Z",
        "createdAt": "2025-01-27T07:34:41Z",
    })).await;

    let session = make_session("session-001", WORKER0,
        "2025-01-27T07:37:53Z", Some("2025-01-27T07:38:53Z"));
    sessions::mock_list_sessions(&harness.server, FARM, QUEUE, JOB, &[session]).await;

    let action = make_task_run_action("sessionaction-001", STEP, "task-001",
        "2025-01-27T07:37:53Z", "2025-01-27T07:38:53Z");
    sessions::mock_list_session_actions(&harness.server, FARM, QUEUE, JOB, "session-001", &[action]).await;

    sessions::mock_batch_get_steps(&harness.server, &[make_step(STEP, "Render")], &[]).await;
    sessions::mock_batch_get_tasks(&harness.server,
        &[make_task(STEP, "task-001", json!({"Frame": {"int": "42"}, "Camera": {"string": "main"}}))], &[]).await;

    assert_cmd_snapshot!(harness.cmd(&["job", "trace-schedule", "--job-id", JOB, "-v"]));
}

// --- Task with no parameters shows placeholder ---

#[tokio::test]
async fn trace_schedule_task_with_no_params_shows_placeholder() {
    let harness = TestHarness::new().await;
    setup(&harness).await;

    jobs::mock_get_job(&harness.server, FARM, QUEUE, json!({
        "jobId": JOB,
        "name": "Render Job",
        "startedAt": "2025-01-27T07:37:53Z",
        "endedAt": "2025-01-27T07:38:53Z",
        "createdAt": "2025-01-27T07:34:41Z",
    })).await;

    let session = make_session("session-001", WORKER0,
        "2025-01-27T07:37:53Z", Some("2025-01-27T07:38:53Z"));
    sessions::mock_list_sessions(&harness.server, FARM, QUEUE, JOB, &[session]).await;

    let action = make_task_run_action("sessionaction-001", STEP, "task-001",
        "2025-01-27T07:37:53Z", "2025-01-27T07:38:53Z");
    sessions::mock_list_session_actions(&harness.server, FARM, QUEUE, JOB, "session-001", &[action]).await;

    sessions::mock_batch_get_steps(&harness.server, &[make_step(STEP, "Render")], &[]).await;
    // Task with empty parameters
    sessions::mock_batch_get_tasks(&harness.server,
        &[make_task(STEP, "task-001", json!({}))], &[]).await;

    assert_cmd_snapshot!(harness.cmd(&["job", "trace-schedule", "--job-id", JOB,
        "--trace-format", "chrome", "--trace-file",
        harness.config_dir.path().join("trace.json").to_str().unwrap()]));

    // Verify the trace file contains "<No Task Params>" as event name
    let content = std::fs::read_to_string(harness.config_dir.path().join("trace.json")).unwrap();
    let trace: serde_json::Value = serde_json::from_str(&content).unwrap();
    let events = trace["traceEvents"].as_array().unwrap();
    let task_event = events.iter().find(|e| e["cat"] == "taskRun").unwrap();
    assert_eq!(task_event["name"], "<No Task Params>");
}

// --- Error: invalid --trace-format value ---

#[tokio::test]
async fn trace_schedule_invalid_trace_format_errors() {
    let harness = TestHarness::new().await;
    setup(&harness).await;

    assert_cmd_snapshot!(harness.cmd(&[
        "job", "trace-schedule", "--job-id", JOB,
        "--trace-format", "banana",
    ]));
}
