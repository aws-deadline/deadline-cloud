//! Level 2 tests for GUI CLI commands: `bundle gui-submit` and `config gui`.
//!
//! These commands spawn a Python subprocess to run Qt dialogs. The tests
//! cover the Rust side of the pipeline: arg parsing, validation, Python
//! discovery, and error paths. They do NOT test the actual GUI rendering
//! because that requires a display server (X11/Wayland/macOS `WindowServer`)
//! and PySide6 installed — conditions not guaranteed in CI.
//!
//! The Python GUI code itself is tested separately by ~228 pytest-qt tests
//! in `gui/tests/ui/` which use `qtbot` to exercise widgets headlessly.
//!
//! What these tests verify:
//! - Help text and subcommand registration (snapshot)
//! - `--submitter-info` field validation (unknown fields, missing required)
//! - `--output` case insensitivity, `--install-gui` flag acceptance
//! - `--submitter-name` deprecation warning
//! - Python-not-found error path (`DEADLINE_PYTHON` → nonexistent path)
//! - Missing required args (no dir + no --browse)

use deadline_test_server::TestHarness;
use insta_cmd::assert_cmd_snapshot;

// ── bundle gui-submit ──────────────────────────────────────────────

// Help text matches expected options
#[tokio::test]
async fn bundle_gui_submit_help_shows_all_options() {
    let harness = TestHarness::new().await;
    assert_cmd_snapshot!(harness.cmd(&["bundle", "gui-submit", "--help"]));
}

// Python not found → clear error message
#[tokio::test]
async fn bundle_gui_submit_no_python_returns_error() {
    let harness = TestHarness::new().await;
    let mut cmd = harness.cmd(&["bundle", "gui-submit", "/tmp/fake-bundle"]);
    cmd.env("DEADLINE_PYTHON", "/nonexistent/python3");
    // Remove PATH so system python isn't found as fallback
    cmd.env("PATH", "");
    assert_cmd_snapshot!(cmd);
}

// No bundle dir and no --browse → error
#[tokio::test]
async fn bundle_gui_submit_no_dir_no_browse_returns_error() {
    let harness = TestHarness::new().await;
    let mut cmd = harness.cmd(&["bundle", "gui-submit"]);
    cmd.env("DEADLINE_PYTHON", "/nonexistent/python3");
    cmd.env("PATH", "");
    assert_cmd_snapshot!(cmd);
}

// --output accepts case-insensitive values
#[tokio::test]
async fn bundle_gui_submit_output_case_insensitive() {
    let harness = TestHarness::new().await;
    // JSON uppercase should be accepted (not rejected by clap)
    let mut cmd = harness.cmd(&["bundle", "gui-submit", "--output", "JSON", "/tmp/fake"]);
    cmd.env("DEADLINE_PYTHON", "/nonexistent/python3");
    cmd.env("PATH", "");
    // Should fail with exit code 1 (Python-not-found), NOT exit code 2 (clap arg error)
    let output = cmd.output().unwrap();
    assert_eq!(
        output.status.code(),
        Some(1),
        "should exit 1 (operation error), not 2 (arg error). stderr: {}",
        String::from_utf8_lossy(&output.stderr),
    );
}

// --submitter-info with invalid field name → error
#[tokio::test]
async fn bundle_gui_submit_submitter_info_unknown_field_errors() {
    let harness = TestHarness::new().await;
    assert_cmd_snapshot!(harness.cmd(&[
        "bundle", "gui-submit",
        "--submitter-info", "bogus_field=value",
        "/tmp/fake-bundle",
    ]));
}

// --submitter-info without submitter_name → error
#[tokio::test]
async fn bundle_gui_submit_submitter_info_missing_name_errors() {
    let harness = TestHarness::new().await;
    assert_cmd_snapshot!(harness.cmd(&[
        "bundle", "gui-submit",
        "--submitter-info", "host_application_name=Maya",
        "/tmp/fake-bundle",
    ]));
}

// --submitter-info key=value parsing works
#[tokio::test]
async fn bundle_gui_submit_submitter_info_key_value_accepted() {
    let harness = TestHarness::new().await;
    let mut cmd = harness.cmd(&[
        "bundle", "gui-submit",
        "--submitter-info", "submitter_name=MyApp",
        "--submitter-info", "host_application_name=Maya",
        "/tmp/fake-bundle",
    ]);
    cmd.env("DEADLINE_PYTHON", "/nonexistent/python3");
    cmd.env("PATH", "");
    // Should fail with exit code 1 (Python-not-found), NOT exit code 2 (arg error)
    let output = cmd.output().unwrap();
    assert_eq!(
        output.status.code(),
        Some(1),
        "should exit 1 (operation error), not 2 (arg error). stderr: {}",
        String::from_utf8_lossy(&output.stderr),
    );
}

// --submitter-info inline JSON parsing works
#[tokio::test]
async fn bundle_gui_submit_submitter_info_json_accepted() {
    let harness = TestHarness::new().await;
    let mut cmd = harness.cmd(&[
        "bundle", "gui-submit",
        "--submitter-info", r#"{"submitter_name": "MyApp", "host_application_name": "Blender"}"#,
        "/tmp/fake-bundle",
    ]);
    cmd.env("DEADLINE_PYTHON", "/nonexistent/python3");
    cmd.env("PATH", "");
    // Should fail with exit code 1 (Python-not-found), NOT exit code 2 (arg error)
    let output = cmd.output().unwrap();
    assert_eq!(
        output.status.code(),
        Some(1),
        "should exit 1 (operation error), not 2 (arg error). stderr: {}",
        String::from_utf8_lossy(&output.stderr),
    );
}

// --submitter-name (deprecated) still accepted with warning
#[tokio::test]
async fn bundle_gui_submit_deprecated_submitter_name_warns() {
    let harness = TestHarness::new().await;
    let mut cmd = harness.cmd(&[
        "bundle", "gui-submit",
        "--submitter-name", "OldApp",
        "/tmp/fake-bundle",
    ]);
    cmd.env("DEADLINE_PYTHON", "/nonexistent/python3");
    cmd.env("PATH", "");
    assert_cmd_snapshot!(cmd);
}

// ── config gui ─────────────────────────────────────────────────────

// Help text matches expected options
#[tokio::test]
async fn config_gui_help_shows_options() {
    let harness = TestHarness::new().await;
    assert_cmd_snapshot!(harness.cmd(&["config", "gui", "--help"]));
}

// Python not found → clear error message
#[tokio::test]
async fn config_gui_no_python_returns_error() {
    let harness = TestHarness::new().await;
    let mut cmd = harness.cmd(&["config", "gui"]);
    cmd.env("DEADLINE_PYTHON", "/nonexistent/python3");
    cmd.env("PATH", "");
    assert_cmd_snapshot!(cmd);
}

// --install-gui flag is accepted
#[tokio::test]
async fn config_gui_install_gui_flag_accepted() {
    let harness = TestHarness::new().await;
    let mut cmd = harness.cmd(&["config", "gui", "--install-gui"]);
    cmd.env("DEADLINE_PYTHON", "/nonexistent/python3");
    cmd.env("PATH", "");
    // Should fail with exit code 1 (Python-not-found), NOT exit code 2 (unknown flag)
    let output = cmd.output().unwrap();
    assert_eq!(
        output.status.code(),
        Some(1),
        "should exit 1 (operation error), not 2 (arg error). stderr: {}",
        String::from_utf8_lossy(&output.stderr),
    );
}

// ── bundle help includes gui-submit ────────────────────────────────

// `deadline bundle --help` lists gui-submit as a subcommand
#[tokio::test]
async fn bundle_help_lists_gui_submit() {
    let harness = TestHarness::new().await;
    assert_cmd_snapshot!(harness.cmd(&["bundle", "--help"]));
}

// ── config help includes gui ───────────────────────────────────────

// `deadline config --help` lists gui as a subcommand
#[tokio::test]
async fn config_help_lists_gui() {
    let harness = TestHarness::new().await;
    assert_cmd_snapshot!(harness.cmd(&["config", "--help"]));
}
