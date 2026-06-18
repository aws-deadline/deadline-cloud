use deadline_test_server::TestHarness;
use insta_cmd::assert_cmd_snapshot;

/// Redact Windows temp-directory paths to `[TEMP_PATH]` so snapshots generated
/// on Unix also match Windows CI output. `tempfile` on Windows places dirs under
/// `%LOCALAPPDATA%\Temp` (e.g. `C:\Users\RUNNER~1\AppData\Local\Temp\.tmpXXXX`).
/// Unix temp redaction (`/var/folders`, `/tmp`) is added by each module's own
/// settings; this only adds the Windows-shaped pattern (no-op on Unix output).
pub(crate) fn add_windows_temp_filters(settings: &mut insta::Settings) {
    settings.add_filter(
        r"[A-Za-z]:\\Users\\[^\\]+\\AppData\\Local\\Temp\\[^\s]*",
        "[TEMP_PATH]",
    );
}

#[tokio::test]
async fn version_prints_name_and_semver() {
    let harness = TestHarness::new().await;

    let mut settings = insta::Settings::clone_current();
    settings.add_filter(r"deadline \d+\.\d+\.\d+", "deadline [VERSION]");
    let _guard = settings.bind_to_scope();

    assert_cmd_snapshot!(harness.cmd(&["--version"]));
}

#[tokio::test]
async fn help_long_flag_prints_usage() {
    let harness = TestHarness::new().await;
    assert_cmd_snapshot!(harness.cmd(&["--help"]));
}

#[tokio::test]
async fn help_short_flag_prints_usage() {
    let harness = TestHarness::new().await;
    assert_cmd_snapshot!(harness.cmd(&["-h"]));
}
