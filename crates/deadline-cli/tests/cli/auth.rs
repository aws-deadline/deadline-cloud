//! Level 2 tests for `deadline auth` subcommands.

use deadline_test_server::TestHarness;
use deadline_test_server::deadline_api::farms;
use insta_cmd::assert_cmd_snapshot;
use serde_json::json;

/// Make a file executable. On Unix, sets the executable permission bit.
/// On Windows, this is a no-op since `.cmd` files are inherently executable.
#[cfg(unix)]
fn make_executable(path: &std::path::Path) {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
}

#[cfg(windows)]
fn make_executable(_path: &std::path::Path) {}

/// Set up a fake DCM environment in the harness temp dir:
/// - AWS config with a DCM profile (has `monitor_id`)
/// - Deadline config pointing to the DCM profile and fake monitor binary
/// - A fake monitor script at the given path
fn setup_dcm_env(harness: &TestHarness, monitor_script: &str) {
    let dir = harness.config_dir.path();

    // Fake AWS config with DCM profile
    let aws_config_path = dir.join("aws_config");
    std::fs::write(
        &aws_config_path,
        "\
[profile test-dcm]
monitor_id = mon-fake123
user_id = user-fake456
identity_store_id = d-fake789
",
    )
    .unwrap();

    // Fake monitor binary
    let monitor_name = if cfg!(windows) {
        "fake-monitor.cmd"
    } else {
        "fake-monitor"
    };
    let monitor_path = dir.join(monitor_name);
    std::fs::write(&monitor_path, monitor_script).unwrap();
    make_executable(&monitor_path);

    // Deadline config pointing to DCM profile and fake monitor
    std::fs::write(
        &harness.config_path,
        format!(
            "\
[defaults]
aws_profile_name = test-dcm

[deadline-cloud-monitor]
path = {}
",
            monitor_path.display()
        ),
    )
    .unwrap();
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
    farms::mock_list_farms(
        &harness.server,
        &[json!({"farmId": "farm-abc", "displayName": "F"})],
    )
    .await;

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
// DCM login happy path — monitor starts, ListFarms succeeds
#[tokio::test]
async fn auth_login_dcm_profile_succeeds() {
    let harness = TestHarness::new().await;
    // Login polls check_authentication_status which now uses ListFarms
    farms::mock_list_farms(&harness.server, &[]).await;

    // Fake monitor that exits immediately (login is non-blocking, polling handles auth)
    let script = if cfg!(windows) {
        "@echo off\nexit /b 0\n"
    } else {
        "#!/bin/bash\nexit 0\n"
    };
    setup_dcm_env(&harness, script);

    assert_cmd_snapshot!(dcm_cmd(&harness, &["auth", "login"]));
}

// monitor exits before auth succeeds
#[tokio::test]
async fn auth_login_dcm_monitor_exits_with_error() {
    let harness = TestHarness::new().await;
    // No ListFarms mock → auth check fails → login fails

    // Fake monitor that prints an error and exits non-zero
    let script = if cfg!(windows) {
        "@echo off\necho Monitor login failed\nexit /b 1\n"
    } else {
        "#!/bin/bash\necho 'Monitor login failed'\nexit 1\n"
    };
    setup_dcm_env(&harness, script);

    assert_cmd_snapshot!(dcm_cmd(&harness, &["auth", "login"]));
}

// monitor executable not found
#[tokio::test]
async fn auth_login_dcm_monitor_not_found() {
    let harness = TestHarness::new().await;

    // Set up DCM env but with a nonexistent monitor path
    let dir = harness.config_dir.path();
    let aws_config_path = dir.join("aws_config");
    std::fs::write(
        &aws_config_path,
        "\
[profile test-dcm]
monitor_id = mon-fake123
",
    )
    .unwrap();
    std::fs::write(
        &harness.config_path,
        "\
[defaults]
aws_profile_name = test-dcm

[deadline-cloud-monitor]
path = /nonexistent/path/to/monitor
",
    )
    .unwrap();

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
    let script = if cfg!(windows) {
        "@echo off\necho Logged out\nexit /b 0\n"
    } else {
        "#!/bin/bash\necho 'Logged out'\nexit 0\n"
    };
    setup_dcm_env(&harness, script);

    assert_cmd_snapshot!(dcm_cmd(&harness, &["auth", "logout"]));
}

// logout subprocess returns non-zero
#[tokio::test]
async fn auth_logout_dcm_monitor_fails() {
    let harness = TestHarness::new().await;

    let script = if cfg!(windows) {
        "@echo off\necho Logout error\nexit /b 1\n"
    } else {
        "#!/bin/bash\necho 'Logout error'\nexit 1\n"
    };
    setup_dcm_env(&harness, script);

    assert_cmd_snapshot!(dcm_cmd(&harness, &["auth", "logout"]));
}

// --output JSON (uppercase) should produce JSON, not verbose
#[tokio::test]
async fn auth_status_output_json_uppercase_produces_json() {
    let harness = TestHarness::new().await;
    farms::mock_list_farms(&harness.server, &[]).await;

    let output = harness
        .cli(&["auth", "status", "--output", "JSON"])
        .output()
        .expect("failed to run");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let parsed: serde_json::Value = serde_json::from_str(&stdout).unwrap_or_else(|e| {
        panic!("Expected JSON output for --output JSON (uppercase), got: {stdout}\nError: {e}")
    });
    assert_eq!(parsed["status"], "AUTHENTICATED");
}

// --- auth login: session refresh in polling loop ---

// GAP-1: Login must invalidate the session cache each iteration so that
// credentials written by DCM mid-login are picked up. Without invalidation,
// the cached SdkConfig retains stale (empty) credentials and the auth probe
// never succeeds.
//
// This test uses credential_process in the AWS profile (no env-var credentials)
// to prove that cache invalidation is required. The monitor script writes
// credential_process to the profile after a delay, simulating DCM's behavior.
#[tokio::test]
async fn auth_login_dcm_picks_up_credentials_written_mid_login() {
    let harness = TestHarness::new().await;
    let dir = harness.config_dir.path();

    // ListFarms always succeeds (the issue is credentials, not server response)
    farms::mock_list_farms_with_principal_id(&harness.server, "user-fake456", &[]).await;

    // Credential helper script that outputs valid AWS credentials JSON
    let (cred_helper_name, cred_helper_content) = if cfg!(windows) {
        ("cred-helper.cmd", "@echo off\necho {\"Version\": 1, \"AccessKeyId\": \"AKIAIOSFODNN7EXAMPLE\", \"SecretAccessKey\": \"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY\", \"SessionToken\": \"token\", \"Expiration\": \"2099-01-01T00:00:00Z\"}\n".to_owned())
    } else {
        ("cred-helper", "#!/bin/bash\necho '{\"Version\": 1, \"AccessKeyId\": \"AKIAIOSFODNN7EXAMPLE\", \"SecretAccessKey\": \"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY\", \"SessionToken\": \"token\", \"Expiration\": \"2099-01-01T00:00:00Z\"}'\n".to_owned())
    };
    let cred_helper = dir.join(cred_helper_name);
    std::fs::write(&cred_helper, cred_helper_content).unwrap();
    make_executable(&cred_helper);

    // AWS config: has monitor_id and user_id but NO credential_process initially.
    // Without credentials, the SDK cannot sign requests → ListFarms fails.
    let aws_config_path = dir.join("aws_config");
    std::fs::write(
        &aws_config_path,
        "[profile test-dcm]\nregion = us-west-2\nmonitor_id = mon-fake123\nuser_id = user-fake456\nidentity_store_id = d-fake789\n",
    )
    .unwrap();

    // Monitor script: waits briefly, then writes credential_process to the
    // AWS config (simulating DCM completing login and writing credentials).
    let (monitor_name, monitor_script) = if cfg!(windows) {
        (
            "fake-monitor.cmd",
            format!(
                "@echo off\ntimeout /t 1 /nobreak >nul\necho credential_process = {} >> {}\ntimeout /t 10 /nobreak >nul\n",
                cred_helper.display(),
                aws_config_path.display()
            ),
        )
    } else {
        (
            "fake-monitor",
            format!(
                "#!/bin/bash\nsleep 0.3\necho \"credential_process = {}\" >> {}\nsleep 10\n",
                cred_helper.display(),
                aws_config_path.display()
            ),
        )
    };
    let monitor_path = dir.join(monitor_name);
    std::fs::write(&monitor_path, &monitor_script).unwrap();
    make_executable(&monitor_path);

    // Deadline config
    std::fs::write(
        &harness.config_path,
        format!(
            "[defaults]\naws_profile_name = test-dcm\n\n[deadline-cloud-monitor]\npath = {}\n",
            monitor_path.display()
        ),
    )
    .unwrap();

    // Build command WITHOUT env-var credentials — forces SDK to use profile
    let mut cmd = std::process::Command::new(assert_cmd::cargo::cargo_bin("deadline"));
    cmd.args(["auth", "login"]);
    cmd.env("AWS_ENDPOINT_URL_DEADLINE", harness.endpoint_url());
    cmd.env("AWS_CONFIG_FILE", &aws_config_path);
    cmd.env("AWS_DEFAULT_REGION", "us-west-2");
    cmd.env("DEADLINE_CONFIG_FILE_PATH", &harness.config_path);
    cmd.env("HOME", dir);
    #[cfg(windows)]
    cmd.env("USERPROFILE", dir);
    // Remove credential env vars so SDK must use credential_process from profile
    cmd.env_remove("AWS_ACCESS_KEY_ID");
    cmd.env_remove("AWS_SECRET_ACCESS_KEY");
    cmd.env_remove("AWS_SESSION_TOKEN");
    cmd.env_remove("AWS_SECURITY_TOKEN");
    cmd.env_remove("AWS_PROFILE");
    cmd.env_remove("AWS_DEFAULT_PROFILE");
    cmd.env_remove("AWS_SHARED_CREDENTIALS_FILE");
    // Disable IMDS/ECS credential providers to avoid timeouts
    cmd.env("AWS_EC2_METADATA_DISABLED", "true");
    cmd.env("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "");

    let output = cmd.output().expect("failed to run");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "Login should succeed after credentials are written mid-login.\n\
         stdout: {stdout}\nstderr: {stderr}"
    );
    assert!(
        stdout.contains("Deadline Cloud monitor profile"),
        "Expected success message, got stdout: {stdout}"
    );
}

// B-1: Non-existent profile should show Source: NOT_VALID, not HOST_PROVIDED
#[tokio::test]
async fn auth_status_nonexistent_profile_shows_not_valid() {
    let harness = TestHarness::new().await;

    // Write an AWS config with only one profile — "existing-profile"
    let aws_config_path = harness.config_dir.path().join("aws_config");
    std::fs::write(
        &aws_config_path,
        "\
[profile existing-profile]
region = us-west-2
",
    )
    .unwrap();

    let mut cmd = harness.cmd(&["auth", "status", "--profile", "nonexistent-profile"]);
    cmd.env("AWS_CONFIG_FILE", &aws_config_path);
    assert_cmd_snapshot!(cmd);
}
