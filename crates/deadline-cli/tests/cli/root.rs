//! Level 2 tests for `deadline` root command options.

use deadline_test_server::TestHarness;
use insta_cmd::assert_cmd_snapshot;

fn config_show_settings() -> insta::Settings {
    let mut settings = insta::Settings::clone_current();
    settings.add_filter(r"(?m)^   /.*config$", "   [CONFIG_PATH]");
    settings.add_filter(
        r"/.*/\.deadline/job_history/",
        "[HOME]/.deadline/job_history/",
    );
    settings
}

// --log-level DEBUG enables debug logging output
#[tokio::test]
async fn log_level_debug_flag_enables_debug_output() {
    let harness = TestHarness::new().await;
    let _guard = config_show_settings().bind_to_scope();
    assert_cmd_snapshot!(harness.cmd(&["--log-level", "DEBUG", "config", "show"]));
}

// No --log-level flag; config has settings.log_level=WARNING → defaults from config
#[tokio::test]
async fn log_level_defaults_to_config_value() {
    let harness = TestHarness::with_config("[defaults]\n\n[settings]\nlog_level = WARNING\n").await;
    let _guard = config_show_settings().bind_to_scope();
    assert_cmd_snapshot!(harness.cmd(&["config", "show"]));
}

// Config has invalid log level → falls back to WARNING with a warning message
#[tokio::test]
async fn log_level_invalid_config_value_falls_back_to_warning() {
    let harness = TestHarness::with_config("[defaults]\n\n[settings]\nlog_level = TRACE\n").await;
    let _guard = config_show_settings().bind_to_scope();
    assert_cmd_snapshot!(harness.cmd(&["config", "show"]));
}

// --redirect-output redirects stdout to file in append mode
#[tokio::test]
async fn redirect_output_appends_to_file() {
    let harness = TestHarness::new().await;
    let out_file = harness.config_dir.path().join("output.log");

    std::fs::write(&out_file, "existing\n").unwrap();

    harness
        .cli(&[
            "--redirect-output",
            out_file.to_str().unwrap(),
            "config",
            "show",
        ])
        .assert()
        .success();

    let content = std::fs::read_to_string(&out_file).unwrap();
    assert!(
        content.starts_with("existing\n"),
        "should preserve existing content"
    );
    assert!(
        content.contains("AWS Deadline Cloud configuration file:"),
        "should contain config show output"
    );
}

// --redirect-output with --redirect-mode replace truncates file
#[tokio::test]
async fn redirect_output_replace_mode_truncates_file() {
    let harness = TestHarness::new().await;
    let out_file = harness.config_dir.path().join("output.log");

    std::fs::write(&out_file, "this should be gone\n").unwrap();

    harness
        .cli(&[
            "--redirect-output",
            out_file.to_str().unwrap(),
            "--redirect-mode",
            "replace",
            "config",
            "show",
        ])
        .assert()
        .success();

    let content = std::fs::read_to_string(&out_file).unwrap();
    assert!(
        !content.contains("this should be gone"),
        "replace mode should truncate"
    );
    assert!(
        content.contains("AWS Deadline Cloud configuration file:"),
        "should contain config show output"
    );
}

// Help text with markdown links displays as "text (url)" in terminal
#[tokio::test]
async fn help_text_markdown_link_stripped_to_text_and_url() {
    let harness = TestHarness::new().await;
    assert_cmd_snapshot!(harness.cmd(&["--help"]));
}

// Help text with bold markers displays without the ** markers
// (covered by the help snapshot above — no ** in output)

// Known operation error prints message to stdout and exits 1
#[tokio::test]
async fn known_error_prints_message_to_stdout_and_exits_one() {
    let harness = TestHarness::new().await;
    assert_cmd_snapshot!(harness.cmd(&["config", "get", "nonexistent.setting"]));
}

// Known error output does NOT contain "CLI encountered the following exception"
// (covered by the error snapshot above — no "exception" in output)
