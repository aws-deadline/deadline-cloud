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
    settings.add_filter(
        r#""settings\.config_file_path": "[^"]*""#,
        r#""settings.config_file_path": "[CONFIG_PATH]""#,
    );
    let _guard = settings.bind_to_scope();

    assert_cmd_snapshot!(harness.cmd(&["config", "show", "--output", "json"]));
}

// config show — non-default value has no "(default)" suffix
#[tokio::test]
async fn config_show_non_default_value_has_no_default_suffix() {
    let harness = TestHarness::with_config("[defaults]\naws_profile_name = custom-profile\n").await;
    let _guard = config_show_settings().bind_to_scope();
    assert_cmd_snapshot!(harness.cmd(&["config", "show"]));
}

// config show — default value shows "(default)" suffix
// (covered by config_show_verbose snapshot — all defaults show "(default)")

// config get — prints current value
#[tokio::test]
async fn config_get_prints_current_value() {
    let harness = TestHarness::with_config("[defaults]\naws_profile_name = my-profile\n").await;
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
        .cli(&[
            "config",
            "set",
            "defaults.farm_id",
            "farm-0123456789abcdef0123456789abcdef",
        ])
        .assert()
        .success();

    assert_cmd_snapshot!(harness.cmd(&["config", "get", "defaults.farm_id"]));
}

// config clear — reverts to default
#[tokio::test]
async fn config_clear_reverts_to_default() {
    let harness = TestHarness::with_config("[defaults]\naws_profile_name = custom\n").await;

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

// --- Config hierarchy tests ---
// Port of Python test_config_settings_hierarchy: settings are stored
// hierarchically (aws_profile → farm_id → queue_id). Switching a parent
// scope resets child settings to defaults.

#[tokio::test]
async fn config_hierarchy_profile_farm_queue_scoping() {
    let harness = TestHarness::new().await;

    // Set settings from innermost to outermost, then switch profile
    harness
        .cli(&[
            "config",
            "set",
            "settings.storage_profile_id",
            "sp-for-farm-default",
        ])
        .assert()
        .success();
    harness
        .cli(&[
            "config",
            "set",
            "defaults.queue_id",
            "queue-for-farm-default",
        ])
        .assert()
        .success();
    harness
        .cli(&[
            "config",
            "set",
            "defaults.farm_id",
            "farm-for-profile-default",
        ])
        .assert()
        .success();
    harness
        .cli(&[
            "config",
            "set",
            "defaults.aws_profile_name",
            "NonDefaultProfile",
        ])
        .assert()
        .success();

    // After switching profile, child settings should be defaults (empty)
    assert_cmd_snapshot!(
        "hierarchy_after_profile_switch",
        harness.cmd(&["config", "get", "defaults.farm_id"])
    );

    // Switch back to default profile — farm should be restored
    harness
        .cli(&["config", "clear", "defaults.aws_profile_name"])
        .assert()
        .success();
    assert_cmd_snapshot!(
        "hierarchy_farm_restored",
        harness.cmd(&["config", "get", "defaults.farm_id"])
    );

    // Queue should still be default (farm scope hasn't been cleared)
    assert_cmd_snapshot!(
        "hierarchy_queue_still_default",
        harness.cmd(&["config", "get", "defaults.queue_id"])
    );

    // Clear farm — queue should be restored
    harness
        .cli(&["config", "clear", "defaults.farm_id"])
        .assert()
        .success();
    assert_cmd_snapshot!(
        "hierarchy_queue_restored",
        harness.cmd(&["config", "get", "defaults.queue_id"])
    );

    // Storage profile should also be restored
    assert_cmd_snapshot!(
        "hierarchy_storage_profile_restored",
        harness.cmd(&["config", "get", "settings.storage_profile_id"])
    );
}

// --- Config roundtrip tests ---
// Port of Python test_config_settings_via_cli_roundtrip: each setting
// roundtrips through get→set→get→clear→get via the CLI.

#[tokio::test]
async fn config_roundtrip_farm_id() {
    let harness = TestHarness::new().await;

    // Default is empty
    assert_cmd_snapshot!(
        "roundtrip_farm_id_default",
        harness.cmd(&["config", "get", "defaults.farm_id"])
    );

    // Set and verify
    harness
        .cli(&[
            "config",
            "set",
            "defaults.farm_id",
            "farm-0123456789abcdef0123456789abcdef",
        ])
        .assert()
        .success();
    assert_cmd_snapshot!(
        "roundtrip_farm_id_set",
        harness.cmd(&["config", "get", "defaults.farm_id"])
    );

    // Clear and verify default restored
    harness
        .cli(&["config", "clear", "defaults.farm_id"])
        .assert()
        .success();
    assert_cmd_snapshot!(
        "roundtrip_farm_id_cleared",
        harness.cmd(&["config", "get", "defaults.farm_id"])
    );
}

#[tokio::test]
async fn config_roundtrip_aws_profile_name() {
    let harness = TestHarness::new().await;

    assert_cmd_snapshot!(
        "roundtrip_profile_default",
        harness.cmd(&["config", "get", "defaults.aws_profile_name"])
    );

    harness
        .cli(&[
            "config",
            "set",
            "defaults.aws_profile_name",
            "AnotherProfileName",
        ])
        .assert()
        .success();
    assert_cmd_snapshot!(
        "roundtrip_profile_set",
        harness.cmd(&["config", "get", "defaults.aws_profile_name"])
    );

    harness
        .cli(&["config", "clear", "defaults.aws_profile_name"])
        .assert()
        .success();
    assert_cmd_snapshot!(
        "roundtrip_profile_cleared",
        harness.cmd(&["config", "get", "defaults.aws_profile_name"])
    );
}

#[tokio::test]
async fn config_roundtrip_auto_accept() {
    let harness = TestHarness::new().await;

    assert_cmd_snapshot!(
        "roundtrip_auto_accept_default",
        harness.cmd(&["config", "get", "settings.auto_accept"])
    );

    harness
        .cli(&["config", "set", "settings.auto_accept", "true"])
        .assert()
        .success();
    assert_cmd_snapshot!(
        "roundtrip_auto_accept_set",
        harness.cmd(&["config", "get", "settings.auto_accept"])
    );

    harness
        .cli(&["config", "clear", "settings.auto_accept"])
        .assert()
        .success();
    assert_cmd_snapshot!(
        "roundtrip_auto_accept_cleared",
        harness.cmd(&["config", "get", "settings.auto_accept"])
    );
}

#[tokio::test]
async fn config_roundtrip_conflict_resolution() {
    let harness = TestHarness::new().await;

    assert_cmd_snapshot!(
        "roundtrip_conflict_default",
        harness.cmd(&["config", "get", "settings.conflict_resolution"])
    );

    harness
        .cli(&[
            "config",
            "set",
            "settings.conflict_resolution",
            "CREATE_COPY",
        ])
        .assert()
        .success();
    assert_cmd_snapshot!(
        "roundtrip_conflict_set",
        harness.cmd(&["config", "get", "settings.conflict_resolution"])
    );

    harness
        .cli(&["config", "clear", "settings.conflict_resolution"])
        .assert()
        .success();
    assert_cmd_snapshot!(
        "roundtrip_conflict_cleared",
        harness.cmd(&["config", "get", "settings.conflict_resolution"])
    );
}

#[tokio::test]
async fn config_roundtrip_log_level() {
    let harness = TestHarness::new().await;

    assert_cmd_snapshot!(
        "roundtrip_log_level_default",
        harness.cmd(&["config", "get", "settings.log_level"])
    );

    harness
        .cli(&["config", "set", "settings.log_level", "DEBUG"])
        .assert()
        .success();
    assert_cmd_snapshot!(
        "roundtrip_log_level_set",
        harness.cmd(&["config", "get", "settings.log_level"])
    );

    harness
        .cli(&["config", "clear", "settings.log_level"])
        .assert()
        .success();
    assert_cmd_snapshot!(
        "roundtrip_log_level_cleared",
        harness.cmd(&["config", "get", "settings.log_level"])
    );
}

// --- Config error path tests ---
// Verify set/clear with unknown settings produce correct error messages.

#[tokio::test]
async fn config_set_unknown_setting_exits_with_error() {
    let harness = TestHarness::new().await;
    assert_cmd_snapshot!(harness.cmd(&["config", "set", "settings.nonexistent", "value"]));
}

#[tokio::test]
async fn config_clear_unknown_setting_exits_with_error() {
    let harness = TestHarness::new().await;
    assert_cmd_snapshot!(harness.cmd(&["config", "clear", "settings.nonexistent"]));
}

#[tokio::test]
async fn config_set_no_dot_setting_exits_with_error() {
    let harness = TestHarness::new().await;
    assert_cmd_snapshot!(harness.cmd(&["config", "set", "bad_name", "value"]));
}

#[tokio::test]
async fn config_clear_no_dot_setting_exits_with_error() {
    let harness = TestHarness::new().await;
    assert_cmd_snapshot!(harness.cmd(&["config", "clear", "bad_name"]));
}

// ── config set preserves existing key ordering ───────────

#[tokio::test]
async fn config_set_preserves_existing_key_order() {
    // Write a config with keys in a specific non-alphabetical order,
    // then set one value. The file should preserve the original order.
    let harness = TestHarness::with_config(
        "[profile-(default) defaults]\nqueue_id = queue-first\nfarm_id = farm-second\njob_id = job-third\n",
    ).await;

    // Update farm_id — should stay in its original position (second)
    harness
        .cli(&["config", "set", "defaults.farm_id", "farm-updated"])
        .assert()
        .success();

    // Verify the value was updated
    let output = harness
        .cli(&["config", "get", "defaults.farm_id"])
        .output()
        .expect("failed to run");
    assert_eq!(
        String::from_utf8_lossy(&output.stdout).trim(),
        "farm-updated"
    );

    // Read the config file directly to check key ordering
    let content = std::fs::read_to_string(&harness.config_path).unwrap();
    let queue_pos = content.find("queue_id").expect("queue_id not found");
    let farm_pos = content.find("farm_id").expect("farm_id not found");
    let job_pos = content.find("job_id").expect("job_id not found");
    assert!(queue_pos < farm_pos, "queue_id should come before farm_id");
    assert!(farm_pos < job_pos, "farm_id should come before job_id");
}

// ── config file with colon delimiter is read correctly ────

#[tokio::test]
async fn config_get_reads_colon_delimited_values() {
    let harness =
        TestHarness::with_config("[profile-(default) defaults]\nfarm_id : farm-from-colon\n").await;

    let output = harness
        .cli(&["config", "get", "defaults.farm_id"])
        .output()
        .expect("failed to run");
    assert_eq!(
        String::from_utf8_lossy(&output.stdout).trim(),
        "farm-from-colon"
    );
}
