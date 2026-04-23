//! Level 2 tests for submission hooks integration in `deadline bundle submit`.
//!
//! Tests §58 from the submission_hooks test spec: config gating,
//! environment hooks, confirmation prompts, payload modification.

use deadline_test_server::TestHarness;
use deadline_test_server::deadline_api::{bundle, jobs, queues, queue_resources};
use insta_cmd::assert_cmd_snapshot;
use serde_json::json;
use std::fs;

const FARM: &str = "farm-0123456789abcdef0123456789abcdef";
const QUEUE: &str = "queue-0123456789abcdef0123456789abcdef";
const JOB: &str = "job-0123456789abcdef0123456789abcdef";

fn bundle_hooks_settings() -> insta::Settings {
    let mut settings = insta::Settings::clone_current();
    settings.add_filter(r"/var/folders/[^\s]+", "[TEMP_PATH]");
    settings.add_filter(r"/tmp/[^\s]+", "[TEMP_PATH]");
    settings.add_filter(r"[\d.]+ seconds at .*/s", "[TIME] seconds at [RATE]/s");
    settings
}

fn setup_config(harness: &TestHarness) {
    harness.cli(&["config", "set", "defaults.farm_id", FARM]).assert().success();
    harness.cli(&["config", "set", "defaults.queue_id", QUEUE]).assert().success();
}

/// Minimal valid YAML template.
fn create_bundle(harness: &TestHarness, name: &str) -> String {
    let dir = harness.config_dir.path().join(name);
    fs::create_dir_all(&dir).unwrap();
    fs::write(dir.join("template.yaml"), "\
specificationVersion: jobtemplate-2023-09
name: TestJob
steps:
  - name: Step1
    script:
      actions:
        onRun:
          command: echo
          args: ['hello']
").unwrap();
    dir.to_str().unwrap().to_string()
}

/// Create a bundle with hooks.yaml containing a noop pre-submission hook.
fn create_bundle_with_hooks(harness: &TestHarness, name: &str) -> String {
    let dir = create_bundle(harness, name);
    fs::write(
        std::path::Path::new(&dir).join("hooks.yaml"),
        "preSubmission:\n  - command: sh\n    args: [\"-c\", \"exit 0\"]\n",
    ).unwrap();
    dir
}

/// Create a bundle with a pre-hook that outputs JSON to modify priority.
fn create_bundle_with_modifying_hook(harness: &TestHarness, name: &str) -> String {
    let dir = create_bundle(harness, name);
    fs::write(
        std::path::Path::new(&dir).join("hooks.yaml"),
        "preSubmission:\n  - command: sh\n    args: [\"-c\", \"echo '{\\\"priority\\\": 100}'\"]\n",
    ).unwrap();
    dir
}

/// Create a bundle with a pre-hook that fails.
fn create_bundle_with_failing_hook(harness: &TestHarness, name: &str) -> String {
    let dir = create_bundle(harness, name);
    fs::write(
        std::path::Path::new(&dir).join("hooks.yaml"),
        "preSubmission:\n  - command: sh\n    args: [\"-c\", \"exit 1\"]\n",
    ).unwrap();
    dir
}

/// Create a hooks directory for environment hooks.
fn create_env_hooks_dir(harness: &TestHarness, name: &str) -> String {
    let dir = harness.config_dir.path().join(name);
    fs::create_dir_all(&dir).unwrap();
    fs::write(
        dir.join("hooks.yaml"),
        "preSubmission:\n  - command: sh\n    args: [\"-c\", \"exit 0\"]\n",
    ).unwrap();
    dir.to_str().unwrap().to_string()
}

/// Create a bundle with a post-submission hook that writes a marker file.
fn create_bundle_with_post_hook(harness: &TestHarness, name: &str) -> (String, String) {
    let dir = create_bundle(harness, name);
    let marker = harness.config_dir.path().join(format!("{name}_marker"));
    let marker_escaped = marker.to_str().unwrap().replace('\\', "\\\\");
    fs::write(
        std::path::Path::new(&dir).join("hooks.yaml"),
        format!("postSubmission:\n  - command: sh\n    args: [\"-c\", \"touch '{marker_escaped}'\"]\n"),
    ).unwrap();
    (dir, marker.to_str().unwrap().to_string())
}

/// Mock the standard APIs for a no-attachment submission.
async fn mock_submit_no_attachments(harness: &TestHarness) {
    queues::mock_get_queue(&harness.server, FARM, json!({
        "queueId": QUEUE,
        "displayName": "Test Queue",
    })).await;
    queue_resources::mock_list_queue_environments(
        &harness.server, FARM, QUEUE, &[],
    ).await;
    bundle::mock_create_job(&harness.server, FARM, QUEUE, JOB).await;
    jobs::mock_get_job(&harness.server, FARM, QUEUE, json!({
        "jobId": JOB,
        "lifecycleStatus": "CREATE_COMPLETE",
        "lifecycleStatusMessage": "Job created successfully",
    })).await;
}

// =====================================================================
// §58 #59: Bundle hooks enabled — pre-hooks run, submission proceeds
// =====================================================================

#[tokio::test]
async fn bundle_submit_hooks_enabled() {
    let harness = TestHarness::new().await;
    setup_config(&harness);
    harness.cli(&["config", "set", "settings.allow_bundle_hooks", "true"]).assert().success();
    mock_submit_no_attachments(&harness).await;
    let bundle_dir = create_bundle_with_hooks(&harness, "hooks_enabled");
    let _guard = bundle_hooks_settings().bind_to_scope();
    assert_cmd_snapshot!(harness.cmd(&["bundle", "submit", &bundle_dir, "--yes"]));
}

// =====================================================================
// §58 #60: Bundle hooks disabled — note printed, hooks skipped
// =====================================================================

#[tokio::test]
async fn bundle_submit_hooks_disabled_note() {
    let harness = TestHarness::new().await;
    setup_config(&harness);
    // allow_bundle_hooks defaults to false
    mock_submit_no_attachments(&harness).await;
    let bundle_dir = create_bundle_with_hooks(&harness, "hooks_disabled");
    let _guard = bundle_hooks_settings().bind_to_scope();
    assert_cmd_snapshot!(harness.cmd(&["bundle", "submit", &bundle_dir, "--yes"]));
}

// =====================================================================
// §58 #61: Environment hooks enabled
// =====================================================================

#[tokio::test]
async fn bundle_submit_env_hooks_enabled() {
    let harness = TestHarness::new().await;
    setup_config(&harness);
    harness.cli(&["config", "set", "settings.allow_environment_hooks", "true"]).assert().success();
    mock_submit_no_attachments(&harness).await;
    let bundle_dir = create_bundle(&harness, "env_hooks_enabled");
    let env_hooks_dir = create_env_hooks_dir(&harness, "env_hooks");
    let _guard = bundle_hooks_settings().bind_to_scope();
    assert_cmd_snapshot!(harness.cmd(&["bundle", "submit", &bundle_dir, "--yes"])
        .env("DEADLINE_HOOKS_DIR", &env_hooks_dir));
}

// =====================================================================
// §58 #62: Environment hooks disabled — warning printed
// =====================================================================

#[tokio::test]
async fn bundle_submit_env_hooks_disabled_warning() {
    let harness = TestHarness::new().await;
    setup_config(&harness);
    // allow_environment_hooks defaults to false
    mock_submit_no_attachments(&harness).await;
    let bundle_dir = create_bundle(&harness, "env_hooks_disabled");
    let env_hooks_dir = create_env_hooks_dir(&harness, "env_hooks_warn");
    let _guard = bundle_hooks_settings().bind_to_scope();
    assert_cmd_snapshot!(harness.cmd(&["bundle", "submit", &bundle_dir, "--yes"])
        .env("DEADLINE_HOOKS_DIR", &env_hooks_dir));
}

// =====================================================================
// §58 #63: Both bundle and env hooks enabled — env runs first
// =====================================================================

#[tokio::test]
async fn bundle_submit_both_hook_sources() {
    let harness = TestHarness::new().await;
    setup_config(&harness);
    harness.cli(&["config", "set", "settings.allow_bundle_hooks", "true"]).assert().success();
    harness.cli(&["config", "set", "settings.allow_environment_hooks", "true"]).assert().success();
    mock_submit_no_attachments(&harness).await;
    let bundle_dir = create_bundle_with_hooks(&harness, "both_sources");
    let env_hooks_dir = create_env_hooks_dir(&harness, "env_hooks_both");
    let _guard = bundle_hooks_settings().bind_to_scope();
    assert_cmd_snapshot!(harness.cmd(&["bundle", "submit", &bundle_dir, "--yes"])
        .env("DEADLINE_HOOKS_DIR", &env_hooks_dir));
}

// =====================================================================
// §58 #64: Pre-hook modifies payload with --yes
// =====================================================================

#[tokio::test]
async fn bundle_submit_pre_hook_modifies_payload() {
    let harness = TestHarness::new().await;
    setup_config(&harness);
    harness.cli(&["config", "set", "settings.allow_bundle_hooks", "true"]).assert().success();
    mock_submit_no_attachments(&harness).await;
    let bundle_dir = create_bundle_with_modifying_hook(&harness, "hook_modifies");
    let _guard = bundle_hooks_settings().bind_to_scope();
    assert_cmd_snapshot!(harness.cmd(&["bundle", "submit", &bundle_dir, "--yes"]));
}

// =====================================================================
// §58 #65: Pre-hook fails — submission canceled
// =====================================================================

#[tokio::test]
async fn bundle_submit_pre_hook_failure_cancels() {
    let harness = TestHarness::new().await;
    setup_config(&harness);
    harness.cli(&["config", "set", "settings.allow_bundle_hooks", "true"]).assert().success();
    mock_submit_no_attachments(&harness).await;
    let bundle_dir = create_bundle_with_failing_hook(&harness, "hook_fails");
    let _guard = bundle_hooks_settings().bind_to_scope();
    assert_cmd_snapshot!(harness.cmd(&["bundle", "submit", &bundle_dir, "--yes"]));
}

// =====================================================================
// §58 #66: Post-hook runs after successful submission
// =====================================================================

#[tokio::test]
async fn bundle_submit_post_hook_after_success() {
    let harness = TestHarness::new().await;
    setup_config(&harness);
    harness.cli(&["config", "set", "settings.allow_bundle_hooks", "true"]).assert().success();
    mock_submit_no_attachments(&harness).await;
    let (bundle_dir, marker_path) = create_bundle_with_post_hook(&harness, "post_hook");
    harness.cli(&["bundle", "submit", &bundle_dir, "--yes"])
        .assert()
        .success();
    assert!(
        std::path::Path::new(&marker_path).exists(),
        "Post-submission hook should have created marker file"
    );
}

// =====================================================================
// §58 #67: Hooks confirmation prompt shown (not --yes)
// =====================================================================

#[tokio::test]
async fn bundle_submit_hooks_confirmation_prompt() {
    let harness = TestHarness::new().await;
    setup_config(&harness);
    harness.cli(&["config", "set", "settings.allow_bundle_hooks", "true"]).assert().success();
    mock_submit_no_attachments(&harness).await;
    let bundle_dir = create_bundle_with_hooks(&harness, "hooks_prompt");
    let _guard = bundle_hooks_settings().bind_to_scope();
    // Without --yes, hooks should trigger confirmation prompt.
    // Since stdin is not a TTY, submission should be canceled.
    assert_cmd_snapshot!(harness.cmd(&["bundle", "submit", &bundle_dir]));
}
