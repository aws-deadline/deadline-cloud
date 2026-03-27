//! Level 2 tests for `deadline` root command options.

use deadline_test_server::TestHarness;
use predicates::prelude::*;

// --log-level DEBUG enables debug logging output
#[tokio::test]
async fn log_level_debug_flag_enables_debug_output() {
    let harness = TestHarness::new().await;

    harness
        .cli(&["--log-level", "DEBUG", "config", "show"])
        .assert()
        .success()
        .stderr(predicate::str::contains("Debug logging is on"));
}

// No --log-level flag; config has settings.log_level=WARNING → defaults from config
#[tokio::test]
async fn log_level_defaults_to_config_value() {
    let harness =
        TestHarness::with_config("[defaults]\n\n[settings]\nlog_level = WARNING\n").await;

    // With WARNING level, debug messages should NOT appear
    harness
        .cli(&["config", "show"])
        .assert()
        .success()
        .stderr(predicate::str::contains("Debug logging is on").not());
}

// Config has invalid log level → falls back to WARNING with a warning message
#[tokio::test]
async fn log_level_invalid_config_value_falls_back_to_warning() {
    let harness =
        TestHarness::with_config("[defaults]\n\n[settings]\nlog_level = TRACE\n").await;

    harness
        .cli(&["config", "show"])
        .assert()
        .success()
        .stderr(predicate::str::contains("TRACE").and(predicate::str::contains("WARNING")));
}

// --redirect-output redirects stdout to file in append mode
#[tokio::test]
async fn redirect_output_appends_to_file() {
    let harness = TestHarness::new().await;
    let out_file = harness.config_dir.path().join("output.log");

    // Write some pre-existing content
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
    // Should contain both the pre-existing content and the new output
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

    harness
        .cli(&["--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Deadline Cloud (https://docs.aws.amazon.com"))
        .stdout(predicate::str::contains("[Deadline Cloud]").not());
}

// Help text with bold markers displays without the ** markers
#[tokio::test]
async fn help_text_bold_markers_stripped() {
    let harness = TestHarness::new().await;

    // The help text should not contain literal ** markers
    harness
        .cli(&["--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("**").not());
}
