//! Level 2 tests for the `deadlinew` windowless binary.
//!
//! `deadlinew` is identical to `deadline` except it uses
//! `#![windows_subsystem = "windows"]` to suppress the console window.
//! On all platforms, it must produce the same output as `deadline`.

use assert_cmd::Command;

/// `deadlinew --version` produces the same output as `deadline --version`.
#[test]
fn deadlinew_version_matches_deadline_version() {
    let deadline_output = Command::cargo_bin("deadline")
        .unwrap()
        .arg("--version")
        .output()
        .expect("deadline binary should exist");

    let deadlinew_output = Command::cargo_bin("deadlinew")
        .unwrap()
        .arg("--version")
        .output()
        .expect("deadlinew binary should exist");

    assert_eq!(deadline_output.status, deadlinew_output.status);
    assert_eq!(
        String::from_utf8_lossy(&deadline_output.stdout),
        String::from_utf8_lossy(&deadlinew_output.stdout),
    );
}

/// `deadlinew --help` produces the same output as `deadline --help`.
#[test]
fn deadlinew_help_matches_deadline_help() {
    let deadline_output = Command::cargo_bin("deadline")
        .unwrap()
        .arg("--help")
        .output()
        .expect("deadline binary should exist");

    let deadlinew_output = Command::cargo_bin("deadlinew")
        .unwrap()
        .arg("--help")
        .output()
        .expect("deadlinew binary should exist");

    assert_eq!(deadline_output.status, deadlinew_output.status);
    assert_eq!(
        String::from_utf8_lossy(&deadline_output.stdout),
        String::from_utf8_lossy(&deadlinew_output.stdout),
    );
}
