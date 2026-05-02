//! Level 2 tests for `deadline auth` subcommands.

use deadline_test_server::TestHarness;
use deadline_test_server::deadline_api::farms;
use insta_cmd::assert_cmd_snapshot;
use serde_json::json;
use std::os::unix::fs::PermissionsExt;

/// Set up a fake DCM environment in the harness temp dir:
/// - AWS config with a DCM profile (has `monitor_id`)
/// - Deadline config pointing to the DCM profile and fake monitor binary
/// - A fake monitor shell script at the given path
fn setup_dcm_env(harness: &TestHarness, monitor_script: &str) {
    let dir = harness.config_dir.path();

    // Fake AWS config with DCM profile
    let aws_config_path = dir.join("aws_config");
    std::fs::write(&aws_config_path, "\
[profile test-dcm]
monitor_id = mon-fake123
user_id = user-fake456
identity_store_id = d-fake789
").unwrap();

    // Fake monitor binary
    let monitor_path = dir.join("fake-monitor");
    std::fs::write(&monitor_path, monitor_script).unwrap();
    std::fs::set_permissions(&monitor_path, std::fs::Permissions::from_mode(0o755)).unwrap();

    // Deadline config pointing to DCM profile and fake monitor
    std::fs::write(&harness.config_path, format!("\
[defaults]
aws_profile_name = test-dcm

[deadline-cloud-monitor]
path = {}
", monitor_path.display())).unwrap();
}

/// Build a command with the fake AWS config file set.
fn dcm_cmd(harness: &TestHarness, args: &[&str]) -> std::process::Command {
    let mut cmd = harness.cmd(args);
    let aws_config_path = harness.config_dir.path().join("aws_config");
    cmd.env("AWS_CONFIG_FILE", aws_config_path);
    cmd
}

// --- auth status (verbose) ---

// Auth check uses ListFarms, not STS. No separate "API availability"
// field — if ListFarms succeeds, status is AUTHENTICATED (implies API available).

// ListFarms succeeds → AUTHENTICATED
#[tokio::test]
async fn auth_status_authenticated() {
    let harness = TestHarness::new().await;
    // No STS mock — auth check uses ListFarms only
    farms::mock_list_farms(&harness.server, &[json!({"farmId": "farm-abc", "displayName": "F"})]).await;

    assert_cmd_snapshot!(harness.cmd(&["auth", "status"]));
}

// ListFarms fails (no mock) → CONFIGURATION_ERROR
#[tokio::test]
async fn auth_status_configuration_error() {
    let harness = TestHarness::new().await;
    // No ListFarms mock → request fails → CONFIGURATION_ERROR

    assert_cmd_snapshot!(harness.cmd(&["auth", "status"]));
}

// --profile option uses specified profile name
#[tokio::test]
async fn auth_status_with_profile_option() {
    let harness = TestHarness::new().await;
    farms::mock_list_farms(&harness.server, &[]).await;

    assert_cmd_snapshot!(harness.cmd(&["auth", "status", "--profile", "custom-profile"]));
}

// "AUTHENTICATED but API unavailable" state no longer exists.
// Tests auth_status_api_unavailable and auth_status_json_api_unavailable removed.

// --- auth status (JSON) ---

#[tokio::test]
async fn auth_status_json_authenticated() {
    let harness = TestHarness::new().await;
    farms::mock_list_farms(&harness.server, &[]).await;

    assert_cmd_snapshot!(harness.cmd(&["auth", "status", "--output", "json"]));
}

#[tokio::test]
async fn auth_status_json_configuration_error() {
    let harness = TestHarness::new().await;
    // No ListFarms mock → CONFIGURATION_ERROR

    assert_cmd_snapshot!(harness.cmd(&["auth", "status", "--output", "json"]));
}

// --- auth login ---

// Non-DCM profile → error (no monitor_id in profile)
#[tokio::test]
async fn auth_login_non_dcm_profile_prints_error() {
    let harness = TestHarness::new().await;
    assert_cmd_snapshot!(harness.cmd(&["auth", "login"]));
}

// --- auth logout ---

// Non-DCM profile → error
#[tokio::test]
async fn auth_logout_non_dcm_profile_prints_error() {
    let harness = TestHarness::new().await;
    assert_cmd_snapshot!(harness.cmd(&["auth", "logout"]));
}

// --- auth login (DCM profile) ---

// Login polling now uses ListFarms instead of STS.

// DCM login happy path — monitor starts, ListFarms succeeds
#[tokio::test]
async fn auth_login_dcm_profile_succeeds() {
    let harness = TestHarness::new().await;
    // Login polls check_authentication_status which now uses ListFarms
    farms::mock_list_farms(&harness.server, &[]).await;

    // Fake monitor that exits immediately (login is non-blocking, polling handles auth)
    setup_dcm_env(&harness, "#!/bin/bash\nexit 0\n");

    assert_cmd_snapshot!(dcm_cmd(&harness, &["auth", "login"]));
}

// monitor exits before auth succeeds
#[tokio::test]
async fn auth_login_dcm_monitor_exits_with_error() {
    let harness = TestHarness::new().await;
    // No ListFarms mock → auth check fails → login fails

    // Fake monitor that prints an error and exits non-zero
    setup_dcm_env(&harness, "#!/bin/bash\necho 'Monitor login failed'\nexit 1\n");

    assert_cmd_snapshot!(dcm_cmd(&harness, &["auth", "login"]));
}

// monitor executable not found
#[tokio::test]
async fn auth_login_dcm_monitor_not_found() {
    let harness = TestHarness::new().await;

    // Set up DCM env but with a nonexistent monitor path
    let dir = harness.config_dir.path();
    let aws_config_path = dir.join("aws_config");
    std::fs::write(&aws_config_path, "\
[profile test-dcm]
monitor_id = mon-fake123
").unwrap();
    std::fs::write(&harness.config_path, "\
[defaults]
aws_profile_name = test-dcm

[deadline-cloud-monitor]
path = /nonexistent/path/to/monitor
").unwrap();

    let mut cmd = harness.cmd(&["auth", "login"]);
    cmd.env("AWS_CONFIG_FILE", aws_config_path);
    assert_cmd_snapshot!(cmd);
}

// --- auth logout (DCM profile) ---

// DCM logout happy path
#[tokio::test]
async fn auth_logout_dcm_profile_succeeds() {
    let harness = TestHarness::new().await;

    // Fake monitor that prints success and exits 0
    setup_dcm_env(&harness, "#!/bin/bash\necho 'Logged out'\nexit 0\n");

    assert_cmd_snapshot!(dcm_cmd(&harness, &["auth", "logout"]));
}

// logout subprocess returns non-zero
#[tokio::test]
async fn auth_logout_dcm_monitor_fails() {
    let harness = TestHarness::new().await;

    setup_dcm_env(&harness, "#!/bin/bash\necho 'Logout error'\nexit 1\n");

    assert_cmd_snapshot!(dcm_cmd(&harness, &["auth", "logout"]));
}

// --output JSON (uppercase) should produce JSON, not verbose
#[tokio::test]
async fn auth_status_output_json_uppercase_produces_json() {
    let harness = TestHarness::new().await;
    farms::mock_list_farms(&harness.server, &[]).await;

    let output = harness.cli(&["auth", "status", "--output", "JSON"])
        .output()
        .expect("failed to run");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let parsed: serde_json::Value = serde_json::from_str(&stdout)
        .unwrap_or_else(|e| panic!("Expected JSON output for --output JSON (uppercase), got: {stdout}\nError: {e}"));
    assert_eq!(parsed["status"], "AUTHENTICATED");
}

// B-1: Non-existent profile should show Source: NOT_VALID, not HOST_PROVIDED
#[tokio::test]
async fn auth_status_nonexistent_profile_shows_not_valid() {
    let harness = TestHarness::new().await;

    // Write an AWS config with only one profile — "existing-profile"
    let aws_config_path = harness.config_dir.path().join("aws_config");
    std::fs::write(&aws_config_path, "\
[profile existing-profile]
region = us-west-2
").unwrap();

    let mut cmd = harness.cmd(&["auth", "status", "--profile", "nonexistent-profile"]);
    cmd.env("AWS_CONFIG_FILE", &aws_config_path);
    assert_cmd_snapshot!(cmd);
}
