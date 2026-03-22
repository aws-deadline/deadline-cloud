use deadline_test_server::TestHarness;
use predicates::prelude::*;

#[tokio::test]
async fn version_prints_name_and_semver() {
    let harness = TestHarness::new().await;

    harness
        .cli(&["--version"])
        .assert()
        .success()
        .stdout(predicate::str::is_match(r"^deadline \d+\.\d+\.\d+\n$").unwrap());
}

#[tokio::test]
async fn help_long_flag_prints_usage() {
    let harness = TestHarness::new().await;

    harness
        .cli(&["--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Usage:"));
}

#[tokio::test]
async fn help_short_flag_prints_usage() {
    let harness = TestHarness::new().await;

    harness
        .cli(&["-h"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Usage:"));
}
