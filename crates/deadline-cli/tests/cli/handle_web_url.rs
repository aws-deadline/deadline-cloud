//! Level 2 tests for `deadline handle-web-url`.
//!
//! Test spec reference: `specs/test_specs/cli.md`, Section 48, cases 1-14.
//! Python test reference: `test/unit/deadline_client/cli/test_cli_handle_web_url.py`

use deadline_test_server::TestHarness;
use deadline_test_server::deadline_api::{jobs, queues, s3, sts};
use insta_cmd::assert_cmd_snapshot;
use serde_json::json;

const FARM: &str = "farm-0123456789abcdef0123456789abcdef";
const QUEUE: &str = "queue-0123456789abcdef0123456789abcdef";
const JOB: &str = "job-0123456789abcdef0123456789abcdef";
const STEP: &str = "step-0123456789abcdef0123456789abcdef";
const TASK: &str = "task-0123456789abcdef0123456789abcdef-99";

fn job_with_attachments() -> serde_json::Value {
    json!({
        "jobId": JOB,
        "name": "Render Job",
        "lifecycleStatus": "CREATE_COMPLETE",
        "taskRunStatus": "SUCCEEDED",
        "taskRunStatusCounts": { "SUCCEEDED": 10 },
        "attachments": {
            "manifests": [{
                "rootPath": "/tmp/outputs",
                "rootPathFormat": "posix",
                "inputManifestPath": "Manifests/input.manifest",
                "inputManifestHash": "abc123",
                "outputRelativeDirectories": ["outputs"]
            }],
            "fileSystem": "COPIED"
        }
    })
}

fn queue_response() -> serde_json::Value {
    json!({
        "queueId": QUEUE,
        "displayName": "Test Queue",
        "jobAttachmentSettings": {
            "s3BucketName": "test-bucket",
            "rootPrefix": "root-prefix"
        }
    })
}

// =====================================================================
// Wrong URL scheme
// =====================================================================

#[tokio::test]
async fn handle_web_url_wrong_scheme_errors() {
    let harness = TestHarness::new().await;
    assert_cmd_snapshot!(harness.cmd(&["handle-web-url", "https://sketchy-website.com"]));
}

// =====================================================================
// Unsupported command in URL
// =====================================================================

#[tokio::test]
async fn handle_web_url_unsupported_command_errors() {
    let harness = TestHarness::new().await;
    assert_cmd_snapshot!(harness.cmd(&["handle-web-url", "deadline://config"]));
}

// =====================================================================
// Missing required URL parameters
// =====================================================================

#[tokio::test]
async fn handle_web_url_missing_farm_id_errors() {
    let harness = TestHarness::new().await;
    assert_cmd_snapshot!(harness.cmd(&[
        "handle-web-url",
        &format!("deadline://download-output?queue-id={QUEUE}&job-id={JOB}")
    ]));
}

#[tokio::test]
async fn handle_web_url_missing_queue_id_errors() {
    let harness = TestHarness::new().await;
    assert_cmd_snapshot!(harness.cmd(&[
        "handle-web-url",
        &format!("deadline://download-output?farm-id={FARM}&job-id={JOB}")
    ]));
}

#[tokio::test]
async fn handle_web_url_missing_job_id_errors() {
    let harness = TestHarness::new().await;
    assert_cmd_snapshot!(harness.cmd(&[
        "handle-web-url",
        &format!("deadline://download-output?farm-id={FARM}&queue-id={QUEUE}")
    ]));
}

#[tokio::test]
async fn handle_web_url_missing_all_params_errors() {
    let harness = TestHarness::new().await;
    assert_cmd_snapshot!(harness.cmd(&["handle-web-url", "deadline://download-output"]));
}

// =====================================================================
// URL provided with --install or --uninstall
// =====================================================================

#[tokio::test]
async fn handle_web_url_url_with_install_flag_errors() {
    let harness = TestHarness::new().await;
    assert_cmd_snapshot!(harness.cmd(&["handle-web-url", "deadline://config", "--install"]));
}

#[tokio::test]
async fn handle_web_url_url_with_uninstall_flag_errors() {
    let harness = TestHarness::new().await;
    assert_cmd_snapshot!(harness.cmd(&["handle-web-url", "deadline://config", "--uninstall"]));
}

#[tokio::test]
async fn handle_web_url_url_with_all_users_flag_errors() {
    let harness = TestHarness::new().await;
    assert_cmd_snapshot!(harness.cmd(&["handle-web-url", "deadline://config", "--all-users"]));
}

// =====================================================================
// Both --install and --uninstall
// =====================================================================

#[tokio::test]
async fn handle_web_url_both_install_uninstall_errors() {
    let harness = TestHarness::new().await;
    assert_cmd_snapshot!(harness.cmd(&["handle-web-url", "--install", "--uninstall"]));
}

// =====================================================================
// No URL, no --install, no --uninstall
// =====================================================================

#[tokio::test]
async fn handle_web_url_no_args_errors() {
    let harness = TestHarness::new().await;
    assert_cmd_snapshot!(harness.cmd(&["handle-web-url"]));
}

// =====================================================================
// Happy path — required params only
// =====================================================================

#[tokio::test]
async fn handle_web_url_download_output_required_params_succeeds() {
    let harness = TestHarness::new().await;

    jobs::mock_get_job(&harness.server, FARM, QUEUE, job_with_attachments()).await;
    queues::mock_get_queue(&harness.server, FARM, queue_response()).await;
    sts::mock_get_caller_identity(&harness.server).await;
    s3::mock_s3_list_empty(&harness.server).await;

    let url = format!("deadline://download-output?farm-id={FARM}&queue-id={QUEUE}&job-id={JOB}");
    assert_cmd_snapshot!(harness.cmd(&["handle-web-url", &url]));
}

// =====================================================================
// Happy path — with step-id and task-id
// =====================================================================

#[tokio::test]
async fn handle_web_url_download_output_with_step_and_task_succeeds() {
    let harness = TestHarness::new().await;

    jobs::mock_get_job(&harness.server, FARM, QUEUE, job_with_attachments()).await;
    queues::mock_get_queue(&harness.server, FARM, queue_response()).await;
    sts::mock_get_caller_identity(&harness.server).await;
    s3::mock_s3_list_empty(&harness.server).await;

    // Mock step and task
    jobs::mock_get_step(
        &harness.server,
        FARM,
        QUEUE,
        JOB,
        json!({"stepId": STEP, "name": "Render Step"}),
    )
    .await;
    jobs::mock_get_task(
        &harness.server,
        FARM,
        QUEUE,
        JOB,
        STEP,
        json!({"taskId": TASK, "parameters": {"Frame": {"int": "1"}}}),
    )
    .await;

    let url = format!(
        "deadline://download-output?farm-id={FARM}&queue-id={QUEUE}&job-id={JOB}&step-id={STEP}&task-id={TASK}"
    );
    assert_cmd_snapshot!(harness.cmd(&["handle-web-url", &url]));
}
