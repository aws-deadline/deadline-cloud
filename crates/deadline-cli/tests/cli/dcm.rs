//! Level 2 tests for DCM credential detection and principalId injection
//! (auth status, list farms with/without DCM, auth CLI commands).

use deadline_test_server::TestHarness;
use deadline_test_server::deadline_api::{farms, fleets, queues};
use insta_cmd::assert_cmd_snapshot;
use serde_json::json;

/// Write a fake ~/.aws/config with a DCM profile into the harness temp dir.
fn write_dcm_aws_config(harness: &TestHarness, profile_name: &str) {
    let aws_dir = harness.config_dir.path().join(".aws");
    std::fs::create_dir_all(&aws_dir).unwrap();
    std::fs::write(
        aws_dir.join("config"),
        format!(
            "[profile {profile_name}]\n\
             region=us-west-2\n\
             credential_process=echo dummy\n\
             monitor_id=monitor-abc123\n\
             user_id=user-dcm-test-id\n\
             identity_store_id=d-teststore\n"
        ),
    )
    .unwrap();
}

// auth status with DCM profile shows DEADLINE_CLOUD_MONITOR_LOGIN source
#[tokio::test]
async fn auth_status_dcm_profile_shows_monitor_login_source() {
    let harness = TestHarness::new().await;
    write_dcm_aws_config(&harness, "dcm-profile");
    harness
        .cli(&["config", "set", "defaults.aws_profile_name", "dcm-profile"])
        .assert()
        .success();

    assert_cmd_snapshot!(harness.cmd(&["auth", "status", "--output", "json"]));
}

// auth status with non-DCM profile shows HOST_PROVIDED source
#[tokio::test]
async fn auth_status_non_dcm_profile_shows_host_provided_source() {
    let harness = TestHarness::new().await;
    assert_cmd_snapshot!(harness.cmd(&["auth", "status", "--output", "json"]));
}

// farm list with DCM user injects principalId (mock only matches with correct principalId)
#[tokio::test]
async fn farm_list_dcm_user_injects_principal_id() {
    let harness = TestHarness::new().await;
    write_dcm_aws_config(&harness, "dcm-profile");
    harness
        .cli(&["config", "set", "defaults.aws_profile_name", "dcm-profile"])
        .assert()
        .success();

    farms::mock_list_farms_with_principal_id(
        &harness.server,
        "user-dcm-test-id",
        &[json!({"farmId": "farm-dcm", "displayName": "DCM Farm"})],
    )
    .await;

    assert_cmd_snapshot!(harness.cmd(&["farm", "list"]));
}

// farm list without DCM does NOT inject principalId
#[tokio::test]
async fn farm_list_non_dcm_does_not_inject_principal_id() {
    let harness = TestHarness::new().await;
    farms::mock_list_farms(
        &harness.server,
        &[json!({"farmId": "farm-all", "displayName": "All Farm"})],
    )
    .await;

    assert_cmd_snapshot!(harness.cmd(&["farm", "list"]));
}

// DCM credentials in [default] profile should be detected
#[tokio::test]
async fn auth_status_default_profile_dcm_shows_monitor_login_source() {
    let harness = TestHarness::new().await;
    // Write AWS config with [default] section containing monitor_id
    let aws_dir = harness.config_dir.path().join(".aws");
    std::fs::create_dir_all(&aws_dir).unwrap();
    std::fs::write(
        aws_dir.join("config"),
        "[default]\n\
         region=us-west-2\n\
         monitor_id=monitor-default123\n\
         user_id=user-default-dcm\n\
         identity_store_id=d-defaultstore\n",
    )
    .unwrap();
    // Use (default) profile — no explicit profile name set
    harness
        .cli(&["config", "set", "defaults.aws_profile_name", "(default)"])
        .assert()
        .success();

    let output = harness
        .cli(&["auth", "status", "--output", "json"])
        .output()
        .expect("failed to run");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let parsed: serde_json::Value = serde_json::from_str(&stdout)
        .unwrap_or_else(|e| panic!("Expected JSON, got: {stdout}\nError: {e}"));
    assert_eq!(
        parsed["source"], "DEADLINE_CLOUD_MONITOR_LOGIN",
        "Default profile with monitor_id should show DEADLINE_CLOUD_MONITOR_LOGIN, got: {}",
        parsed["source"]
    );
}

// queue list with DCM user injects principalId
#[tokio::test]
async fn queue_list_dcm_user_injects_principal_id() {
    let harness = TestHarness::new().await;
    write_dcm_aws_config(&harness, "dcm-profile");
    harness
        .cli(&["config", "set", "defaults.aws_profile_name", "dcm-profile"])
        .assert()
        .success();
    harness
        .cli(&["config", "set", "defaults.farm_id", "farm-abc"])
        .assert()
        .success();

    queues::mock_list_queues_with_principal_id(
        &harness.server,
        "farm-abc",
        "user-dcm-test-id",
        &[json!({"queueId": "queue-dcm", "displayName": "DCM Queue"})],
    )
    .await;

    assert_cmd_snapshot!(harness.cmd(&["queue", "list"]));
}

// fleet list with DCM user injects principalId
#[tokio::test]
async fn fleet_list_dcm_user_injects_principal_id() {
    let harness = TestHarness::new().await;
    write_dcm_aws_config(&harness, "dcm-profile");
    harness
        .cli(&["config", "set", "defaults.aws_profile_name", "dcm-profile"])
        .assert()
        .success();
    harness
        .cli(&["config", "set", "defaults.farm_id", "farm-abc"])
        .assert()
        .success();

    fleets::mock_list_fleets_with_principal_id(
        &harness.server,
        "farm-abc",
        "user-dcm-test-id",
        &[json!({"fleetId": "fleet-dcm", "displayName": "DCM Fleet"})],
    )
    .await;

    assert_cmd_snapshot!(harness.cmd(&["fleet", "list"]));
}
