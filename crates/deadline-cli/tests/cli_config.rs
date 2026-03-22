//! Level 2 tests for `deadline config` subcommands.

use deadline_test_server::TestHarness;
use predicates::prelude::*;

// config show — verbose (default) output
#[tokio::test]
async fn config_show_verbose_prints_file_path_and_settings() {
    let harness = TestHarness::new().await;

    harness
        .cli(&["config", "show"])
        .assert()
        .success()
        .stdout(predicate::str::contains("AWS Deadline Cloud configuration file:"))
        .stdout(predicate::str::contains("defaults.farm_id:"))
        .stdout(predicate::str::contains("defaults.aws_profile_name:"));
}

// config show --output json
#[tokio::test]
async fn config_show_json_prints_all_settings_as_json() {
    let harness = TestHarness::new().await;

    let output = harness
        .cli(&["config", "show", "--output", "json"])
        .output()
        .expect("failed to run CLI");

    assert!(output.status.success());

    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value = serde_json::from_str(stdout.trim())
        .expect("output should be valid JSON");

    let obj = json.as_object().expect("output should be a JSON object");
    assert!(obj.contains_key("settings.config_file_path"));
    assert!(obj.contains_key("defaults.farm_id"));
    assert!(obj.contains_key("defaults.aws_profile_name"));
}

// config show — non-default value has no "(default)" suffix
#[tokio::test]
async fn config_show_non_default_value_has_no_default_suffix() {
    let harness = TestHarness::with_config(
        "[defaults]\naws_profile_name = custom-profile\n",
    )
    .await;

    let output = harness
        .cli(&["config", "show"])
        .output()
        .expect("failed to run CLI");

    let stdout = String::from_utf8_lossy(&output.stdout);

    // Find the line for aws_profile_name — it should NOT have "(default)"
    let profile_line = stdout
        .lines()
        .find(|l| l.starts_with("defaults.aws_profile_name:"))
        .expect("should contain aws_profile_name line");

    assert!(
        !profile_line.contains("(default)"),
        "non-default value should not show (default), got: {profile_line}"
    );
}

// config show — default value shows "(default)" suffix
#[tokio::test]
async fn config_show_default_value_shows_default_suffix() {
    let harness = TestHarness::new().await;

    let output = harness
        .cli(&["config", "show"])
        .output()
        .expect("failed to run CLI");

    let stdout = String::from_utf8_lossy(&output.stdout);

    let profile_line = stdout
        .lines()
        .find(|l| l.starts_with("defaults.aws_profile_name:"))
        .expect("should contain aws_profile_name line");

    assert!(
        profile_line.contains("(default)"),
        "default value should show (default), got: {profile_line}"
    );
}

// config get — prints current value
#[tokio::test]
async fn config_get_prints_current_value() {
    let harness = TestHarness::with_config(
        "[defaults]\naws_profile_name = my-profile\n",
    )
    .await;

    harness
        .cli(&["config", "get", "defaults.aws_profile_name"])
        .assert()
        .success()
        .stdout(predicate::str::is_match(r"^my-profile\n$").unwrap());
}

// config get — unset setting prints default
#[tokio::test]
async fn config_get_unset_setting_prints_default() {
    let harness = TestHarness::new().await;

    harness
        .cli(&["config", "get", "defaults.aws_profile_name"])
        .assert()
        .success()
        .stdout(predicate::str::is_match(r"^\(default\)\n$").unwrap());
}

// config set — persists value to disk
#[tokio::test]
async fn config_set_persists_value() {
    let harness = TestHarness::new().await;

    harness
        .cli(&["config", "set", "defaults.farm_id", "farm-0123456789abcdef0123456789abcdef"])
        .assert()
        .success();

    harness
        .cli(&["config", "get", "defaults.farm_id"])
        .assert()
        .success()
        .stdout(predicate::str::contains("farm-0123456789abcdef0123456789abcdef"));
}

// config clear — reverts to default
#[tokio::test]
async fn config_clear_reverts_to_default() {
    let harness = TestHarness::with_config(
        "[defaults]\naws_profile_name = custom\n",
    )
    .await;

    // Verify it's set
    harness
        .cli(&["config", "get", "defaults.aws_profile_name"])
        .assert()
        .success()
        .stdout(predicate::str::contains("custom"));

    // Clear it
    harness
        .cli(&["config", "clear", "defaults.aws_profile_name"])
        .assert()
        .success();

    // Should be back to default
    harness
        .cli(&["config", "get", "defaults.aws_profile_name"])
        .assert()
        .success()
        .stdout(predicate::str::is_match(r"^\(default\)\n$").unwrap());
}
