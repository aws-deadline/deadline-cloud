//! Smoke test to verify the test harness works.
//! This will be replaced by real CLI tests as commands are implemented.

use deadline_test_server::TestHarness;

#[tokio::test]
async fn harness_starts_and_cli_binary_runs() {
    let harness = TestHarness::new().await;

    // The CLI currently just prints a placeholder message.
    // This test verifies the full pipeline works:
    // 1. TestHarness starts a wiremock server
    // 2. TestHarness creates an isolated config directory
    // 3. assert_cmd finds and runs the compiled `deadline` binary
    // 4. Environment variables are correctly injected
    let output = harness
        .cli(&["--help"])
        .output()
        .expect("failed to run CLI binary");

    // The binary ran — that's all we need to verify for now.
    // It may exit 0 (if --help works) or non-zero (if not implemented yet).
    assert!(
        output.status.success() || !output.stdout.is_empty() || !output.stderr.is_empty(),
        "CLI binary produced no output at all"
    );
}
