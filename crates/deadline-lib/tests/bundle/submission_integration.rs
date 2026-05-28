//! Level 1 tests for `create_job_from_job_bundle`.
//!
//! These test the orchestration function directly (not via CLI) to assert on
//! precision the CLI can't expose: exact `CreateJob` request body, hook metadata
//! construction, and telemetry event payloads.
#![allow(unsafe_code, reason = "env var manipulation in serialized tests")]
//!
//! Uses wiremock + real temp dirs + #[serial] (env var mutation).

use deadline_lib::bundle::submission::{
    SubmissionHandler, SubmitJobParams, create_job_from_job_bundle,
};
use serde_json::json;
use serial_test::serial;
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tempfile::TempDir;
use wiremock::matchers::{method, path, path_regex};
use wiremock::{Mock, MockServer, ResponseTemplate};

const FARM: &str = "farm-0123456789abcdef0123456789abcdef";
const QUEUE: &str = "queue-0123456789abcdef0123456789abcdef";
const JOB: &str = "job-0123456789abcdef0123456789abcdef";

// ── Test handler ────────────────────────────────────────────────────

struct TestHandler {
    messages: Arc<Mutex<Vec<String>>>,
}

impl TestHandler {
    fn new() -> Self {
        Self {
            messages: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn messages(&self) -> Vec<String> {
        self.messages.lock().unwrap().clone()
    }
}

impl SubmissionHandler for TestHandler {
    fn on_message(&self, msg: &str) {
        self.messages.lock().unwrap().push(msg.to_owned());
    }
    fn confirm(&self, _msg: &str, _default: bool) -> bool {
        true
    }
    fn should_continue(&self) -> bool {
        true
    }
}

// ── Helpers ─────────────────────────────────────────────────────────

fn setup_env(server: &MockServer) {
    // SAFETY: tests are serialized via #[serial] — no concurrent env mutation.
    unsafe {
        std::env::set_var(
            "AWS_ENDPOINT_URL_DEADLINE",
            format!("http://localhost:{}", server.address().port()),
        );
        std::env::set_var(
            "AWS_ENDPOINT_URL_STS",
            format!("http://localhost:{}", server.address().port()),
        );
        std::env::set_var("AWS_ACCESS_KEY_ID", "AKIAIOSFODNN7EXAMPLE");
        std::env::set_var(
            "AWS_SECRET_ACCESS_KEY",
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        );
        std::env::set_var("AWS_DEFAULT_REGION", "us-west-2");
        std::env::set_var("AWS_CONFIG_FILE", "/dev/null");
    }
}

fn default_submit_params(
    bundle_dir: PathBuf,
    handler: &dyn SubmissionHandler,
) -> SubmitJobParams<'_> {
    SubmitJobParams {
        job_bundle_dir: bundle_dir,
        job_parameters: vec![],
        name: None,
        priority: None,
        max_failed_tasks_count: None,
        max_retries_per_task: None,
        max_worker_count: None,
        target_task_run_status: None,
        require_paths_exist: false,
        submitter_name: None,
        known_asset_paths: vec![],
        auto_accept: true,
        debug_snapshot_dir: None,
        handler,
        hashing_progress_callback: None,
        upload_progress_callback: None,
        telemetry: None,
        farm_id: FARM.to_owned(),
        queue_id: QUEUE.to_owned(),
        profile: None,
        storage_profile_id: None,
        job_attachments_file_system: String::new(),
        force_s3_check: false,
        allow_bundle_hooks: false,
        allow_environment_hooks: false,
        known_config_paths: vec![],
        s3_max_pool_connections: None,
    }
}

fn create_bundle(dir: &TempDir, name: &str) -> PathBuf {
    let bundle_dir = dir.path().join(name);
    fs::create_dir_all(&bundle_dir).unwrap();
    fs::write(
        bundle_dir.join("template.yaml"),
        "\
specificationVersion: jobtemplate-2023-09
name: TestJob
steps:
  - name: Step1
    script:
      actions:
        onRun:
          command: echo
          args: ['hello']
",
    )
    .unwrap();
    bundle_dir
}

async fn mock_get_queue(server: &MockServer) {
    Mock::given(method("GET"))
        .and(path(format!("/2023-10-12/farms/{FARM}/queues/{QUEUE}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "queueId": QUEUE,
            "displayName": "Test Queue",
            "farmId": FARM,
            "status": "ACTIVE",
            "defaultBudgetAction": "NONE",
            "createdAt": "2024-01-01T00:00:00Z",
            "createdBy": "user",
            "updatedAt": "2024-01-01T00:00:00Z",
            "updatedBy": "user",
        })))
        .mount(server)
        .await;
}

async fn mock_list_queue_environments_empty(server: &MockServer) {
    Mock::given(method("GET"))
        .and(path(format!(
            "/2023-10-12/farms/{FARM}/queues/{QUEUE}/environments"
        )))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"environments": []})))
        .mount(server)
        .await;
}

async fn mock_create_job(server: &MockServer) {
    Mock::given(method("POST"))
        .and(path(format!(
            "/2023-10-12/farms/{FARM}/queues/{QUEUE}/jobs"
        )))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobId": JOB,
        })))
        .mount(server)
        .await;
}

async fn mock_get_job_complete(server: &MockServer) {
    Mock::given(method("GET"))
        .and(path_regex(format!(
            "/2023-10-12/farms/{FARM}/queues/{QUEUE}/jobs/{JOB}"
        )))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobId": JOB,
            "name": "TestJob",
            "lifecycleStatus": "CREATE_COMPLETE",
            "lifecycleStatusMessage": "Job created successfully",
            "priority": 50,
            "createdAt": "2024-01-01T00:00:00Z",
            "createdBy": "user",
            "updatedAt": "2024-01-01T00:00:00Z",
            "updatedBy": "user",
        })))
        .mount(server)
        .await;
}

async fn mock_standard_submission(server: &MockServer) {
    mock_get_queue(server).await;
    mock_list_queue_environments_empty(server).await;
    mock_create_job(server).await;
    mock_get_job_complete(server).await;
}

// ── Tests ───────────────────────────────────────────────────────────

/// Happy path: no attachments, returns job ID.
#[tokio::test]
#[serial]
async fn submit_no_attachments_returns_job_id() {
    let server = MockServer::start().await;
    setup_env(&server);

    mock_standard_submission(&server).await;

    let dir = TempDir::new().unwrap();
    let bundle_dir = create_bundle(&dir, "bundle");
    let handler = TestHandler::new();

    let result = create_job_from_job_bundle(default_submit_params(bundle_dir, &handler)).await;

    let job_id = result.unwrap().unwrap();
    assert_eq!(job_id, JOB);
}

/// `CreateJob` API failure returns error with message.
#[tokio::test]
#[serial]
async fn submit_create_job_api_error_returns_error() {
    let server = MockServer::start().await;
    setup_env(&server);

    mock_get_queue(&server).await;
    mock_list_queue_environments_empty(&server).await;

    // CreateJob returns 400
    Mock::given(method("POST"))
        .and(path(format!(
            "/2023-10-12/farms/{FARM}/queues/{QUEUE}/jobs"
        )))
        .respond_with(ResponseTemplate::new(400).set_body_json(json!({
            "__type": "ValidationException",
            "message": "Template validation failed",
        })))
        .mount(&server)
        .await;

    let dir = TempDir::new().unwrap();
    let bundle_dir = create_bundle(&dir, "bundle");
    let handler = TestHandler::new();

    let result = create_job_from_job_bundle(default_submit_params(bundle_dir, &handler)).await;

    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("ValidationException"),
        "expected ValidationException in error, got: {err}"
    );
}

/// Polling returns `CREATE_FAILED` → error with job ID.
#[tokio::test]
#[serial]
async fn submit_create_failed_status_returns_error() {
    let server = MockServer::start().await;
    setup_env(&server);

    mock_get_queue(&server).await;
    mock_list_queue_environments_empty(&server).await;
    mock_create_job(&server).await;

    // GetJob returns CREATE_FAILED
    Mock::given(method("GET"))
        .and(path_regex(format!(
            "/2023-10-12/farms/{FARM}/queues/{QUEUE}/jobs/{JOB}"
        )))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobId": JOB,
            "name": "TestJob",
            "lifecycleStatus": "CREATE_FAILED",
            "lifecycleStatusMessage": "Template has errors",
            "priority": 50,
            "createdAt": "2024-01-01T00:00:00Z",
            "createdBy": "user",
            "updatedAt": "2024-01-01T00:00:00Z",
            "updatedBy": "user",
        })))
        .mount(&server)
        .await;

    let dir = TempDir::new().unwrap();
    let bundle_dir = create_bundle(&dir, "bundle");
    let handler = TestHandler::new();

    let result = create_job_from_job_bundle(default_submit_params(bundle_dir, &handler)).await;

    let err = result.unwrap_err().to_string();
    assert!(
        err.contains(JOB) && err.contains("failed"),
        "expected job ID and 'failed' in error, got: {err}"
    );
}

/// Missing template file returns error.
#[tokio::test]
#[serial]
async fn submit_missing_template_returns_error() {
    let server = MockServer::start().await;
    setup_env(&server);

    let dir = TempDir::new().unwrap();
    // Empty bundle dir — no template file
    let bundle_dir = dir.path().join("empty_bundle");
    fs::create_dir_all(&bundle_dir).unwrap();
    let handler = TestHandler::new();

    let result = create_job_from_job_bundle(default_submit_params(bundle_dir, &handler)).await;

    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("template"),
        "expected 'template' in error, got: {err}"
    );
}

/// Handler receives queue name message during submission.
#[tokio::test]
#[serial]
async fn submit_handler_receives_queue_name_message() {
    let server = MockServer::start().await;
    setup_env(&server);

    mock_standard_submission(&server).await;

    let dir = TempDir::new().unwrap();
    let bundle_dir = create_bundle(&dir, "bundle");
    let handler = TestHandler::new();

    let _ = create_job_from_job_bundle(default_submit_params(bundle_dir, &handler))
        .await
        .unwrap();

    let msgs = handler.messages();
    assert!(
        msgs.iter().any(|m| m.contains("Test Queue")),
        "expected queue name in messages, got: {msgs:?}"
    );
    assert!(
        msgs.iter().any(|m| m.contains(JOB)),
        "expected job ID in messages, got: {msgs:?}"
    );
}
