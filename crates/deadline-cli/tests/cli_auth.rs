//! Level 2 tests for `deadline auth` subcommands.

use deadline_test_server::TestHarness;
use deadline_test_server::deadline_api::{farms, sts};
use predicates::prelude::*;
use serde_json::json;

// --- auth status (verbose) ---

// auth status with valid creds shows AUTHENTICATED
#[tokio::test]
async fn auth_status_authenticated_shows_status() {
    let harness = TestHarness::new().await;
    sts::mock_get_caller_identity(&harness.server).await;
    farms::mock_list_farms(&harness.server, &[json!({"farmId": "farm-abc", "displayName": "F"})]).await;

    harness
        .cli(&["auth", "status"])
        .assert()
        .success()
        .stdout(predicate::str::contains("AUTHENTICATED"));
}

// auth status verbose output shows profile name, source, status, api availability
#[tokio::test]
async fn auth_status_verbose_shows_all_fields() {
    let harness = TestHarness::new().await;
    sts::mock_get_caller_identity(&harness.server).await;
    farms::mock_list_farms(&harness.server, &[]).await;

    let output = harness
        .cli(&["auth", "status"])
        .output()
        .expect("failed to run CLI");

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Profile Name:"), "should show Profile Name");
    assert!(stdout.contains("Source:"), "should show Source");
    assert!(stdout.contains("Status:"), "should show Status");
    assert!(stdout.contains("API Availability:"), "should show API Availability");
}

// auth status with failed STS and host-provided creds shows CONFIGURATION_ERROR
#[tokio::test]
async fn auth_status_failed_sts_host_provided_shows_configuration_error() {
    let harness = TestHarness::new().await;
    sts::mock_get_caller_identity_failure(&harness.server).await;

    harness
        .cli(&["auth", "status"])
        .assert()
        .success()
        .stdout(predicate::str::contains("CONFIGURATION_ERROR"));
}

// auth status shows HOST_PROVIDED for non-DCM profiles
#[tokio::test]
async fn auth_status_host_provided_source() {
    let harness = TestHarness::new().await;
    sts::mock_get_caller_identity(&harness.server).await;
    farms::mock_list_farms(&harness.server, &[]).await;

    harness
        .cli(&["auth", "status"])
        .assert()
        .success()
        .stdout(predicate::str::contains("HOST_PROVIDED"));
}

// auth status with --profile option uses specified profile
#[tokio::test]
async fn auth_status_with_profile_option() {
    let harness = TestHarness::new().await;
    sts::mock_get_caller_identity(&harness.server).await;
    farms::mock_list_farms(&harness.server, &[]).await;

    harness
        .cli(&["auth", "status", "--profile", "custom-profile"])
        .assert()
        .success()
        .stdout(predicate::str::contains("custom-profile"));
}

// --- auth status (JSON) ---

// auth status --output json returns valid JSON with expected keys
#[tokio::test]
async fn auth_status_json_output_has_expected_keys() {
    let harness = TestHarness::new().await;
    sts::mock_get_caller_identity(&harness.server).await;
    farms::mock_list_farms(&harness.server, &[]).await;

    let output = harness
        .cli(&["auth", "status", "--output", "json"])
        .output()
        .expect("failed to run CLI");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value = serde_json::from_str(stdout.trim())
        .expect("output should be valid JSON");

    let obj = json.as_object().expect("should be a JSON object");
    assert!(obj.contains_key("profile_name"));
    assert!(obj.contains_key("source"));
    assert!(obj.contains_key("status"));
    assert!(obj.contains_key("api_availability"));
}

// auth status --output json shows AUTHENTICATED when STS succeeds
#[tokio::test]
async fn auth_status_json_authenticated() {
    let harness = TestHarness::new().await;
    sts::mock_get_caller_identity(&harness.server).await;
    farms::mock_list_farms(&harness.server, &[]).await;

    let output = harness
        .cli(&["auth", "status", "--output", "json"])
        .output()
        .expect("failed to run CLI");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value = serde_json::from_str(stdout.trim()).unwrap();
    assert_eq!(json["status"], "AUTHENTICATED");
    assert_eq!(json["source"], "HOST_PROVIDED");
}

// --- auth status: API availability ---

// auth status shows API availability true when ListFarms succeeds
#[tokio::test]
async fn auth_status_api_available_shows_true() {
    let harness = TestHarness::new().await;
    sts::mock_get_caller_identity(&harness.server).await;
    farms::mock_list_farms(&harness.server, &[]).await;

    harness
        .cli(&["auth", "status"])
        .assert()
        .success()
        .stdout(predicate::str::contains("API Availability:").and(predicate::str::contains("True")));
}

// auth status shows API availability false when ListFarms fails
#[tokio::test]
async fn auth_status_api_unavailable_shows_false() {
    let harness = TestHarness::new().await;
    sts::mock_get_caller_identity(&harness.server).await;
    // No ListFarms mock mounted — SDK will get no response / error

    harness
        .cli(&["auth", "status"])
        .assert()
        .success()
        .stdout(predicate::str::contains("API Availability:").and(predicate::str::contains("False")));
}

// auth status JSON shows api_availability as boolean true
#[tokio::test]
async fn auth_status_json_api_availability_true() {
    let harness = TestHarness::new().await;
    sts::mock_get_caller_identity(&harness.server).await;
    farms::mock_list_farms(&harness.server, &[]).await;

    let output = harness
        .cli(&["auth", "status", "--output", "json"])
        .output()
        .expect("failed to run CLI");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value = serde_json::from_str(stdout.trim()).unwrap();
    assert_eq!(json["api_availability"], true);
}

// auth status JSON shows api_availability as boolean false when API unavailable
#[tokio::test]
async fn auth_status_json_api_availability_false() {
    let harness = TestHarness::new().await;
    sts::mock_get_caller_identity(&harness.server).await;
    // No ListFarms mock

    let output = harness
        .cli(&["auth", "status", "--output", "json"])
        .output()
        .expect("failed to run CLI");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value = serde_json::from_str(stdout.trim()).unwrap();
    assert_eq!(json["api_availability"], false);
}
