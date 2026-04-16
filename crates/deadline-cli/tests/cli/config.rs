//! Level 2 tests for `deadline config` subcommands.

use deadline_test_server::TestHarness;
use insta_cmd::assert_cmd_snapshot;

fn config_show_settings() -> insta::Settings {
    let mut settings = insta::Settings::clone_current();
    settings.add_filter(r"(?m)^   /.*config$", "   [CONFIG_PATH]");
    settings
}

// config show — verbose (default) output
#[tokio::test]
async fn config_show_verbose_prints_file_path_and_settings() {
    let harness = TestHarness::new().await;
    let _guard = config_show_settings().bind_to_scope();
    assert_cmd_snapshot!(harness.cmd(&["config", "show"]));
}

// config show --output json
#[tokio::test]
async fn config_show_json_prints_all_settings_as_json() {
    let harness = TestHarness::new().await;

    let mut settings = insta::Settings::clone_current();
    settings.add_filter(r#""settings\.config_file_path": "[^"]*""#, r#""settings.config_file_path": "[CONFIG_PATH]""#);
    let _guard = settings.bind_to_scope();

    assert_cmd_snapshot!(harness.cmd(&["config", "show", "--output", "json"]));
}

// config show — non-default value has no "(default)" suffix
#[tokio::test]
async fn config_show_non_default_value_has_no_default_suffix() {
    let harness = TestHarness::with_config(
        "[defaults]\naws_profile_name = custom-profile\n",
    )
    .await;
    let _guard = config_show_settings().bind_to_scope();
    assert_cmd_snapshot!(harness.cmd(&["config", "show"]));
}

// config show — default value shows "(default)" suffix
// (covered by config_show_verbose snapshot — all defaults show "(default)")

// config get — prints current value
#[tokio::test]
async fn config_get_prints_current_value() {
    let harness = TestHarness::with_config(
        "[defaults]\naws_profile_name = my-profile\n",
    )
    .await;
    assert_cmd_snapshot!(harness.cmd(&["config", "get", "defaults.aws_profile_name"]));
}

// config get — unset setting prints default
#[tokio::test]
async fn config_get_unset_setting_prints_default() {
    let harness = TestHarness::new().await;
    assert_cmd_snapshot!(harness.cmd(&["config", "get", "defaults.aws_profile_name"]));
}

// config set — persists value to disk
#[tokio::test]
async fn config_set_persists_value() {
    let harness = TestHarness::new().await;

    harness
        .cli(&["config", "set", "defaults.farm_id", "farm-0123456789abcdef0123456789abcdef"])
        .assert()
        .success();

    assert_cmd_snapshot!(harness.cmd(&["config", "get", "defaults.farm_id"]));
}

// config clear — reverts to default
#[tokio::test]
async fn config_clear_reverts_to_default() {
    let harness = TestHarness::with_config(
        "[defaults]\naws_profile_name = custom\n",
    )
    .await;

    harness
        .cli(&["config", "clear", "defaults.aws_profile_name"])
        .assert()
        .success();

    assert_cmd_snapshot!(harness.cmd(&["config", "get", "defaults.aws_profile_name"]));
}

// A-2: Error messages should use single quotes around setting names
// A-3: Known-section unknown-setting should use same message as unknown-section
#[tokio::test]
async fn config_get_unknown_setting_uses_single_quotes() {
    let harness = TestHarness::new().await;
    // "settings" is a known section, "nonexistent" is not a known key
    assert_cmd_snapshot!(harness.cmd(&["config", "get", "settings.nonexistent"]));
}

#[tokio::test]
async fn config_get_no_dot_setting_uses_single_quotes() {
    let harness = TestHarness::new().await;
    assert_cmd_snapshot!(harness.cmd(&["config", "get", "bad_name"]));
}
