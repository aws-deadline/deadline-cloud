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
