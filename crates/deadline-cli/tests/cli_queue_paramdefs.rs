//! Level 2 tests for `deadline queue paramdefs`.

use deadline_test_server::TestHarness;
use deadline_test_server::deadline_api::{queue_resources, telemetry};
use insta_cmd::assert_cmd_snapshot;
use serde_json::json;

fn setup_config(harness: &TestHarness) {
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.queue_id", "queue-abc"]).assert().success();
}

fn env_template(name: &str, params: &[serde_json::Value]) -> String {
    let template = serde_json::json!({
        "specificationVersion": "environment-2023-09",
        "environment": {
            "name": name,
        },
        "parameterDefinitions": params,
    });
    serde_yaml::to_string(&template).unwrap()
}

// ---------------------------------------------------------------------------
// Happy path: one environment with one parameter (§8 case 1)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn queue_paramdefs_one_env_one_param() {
    let harness = TestHarness::new().await;
    setup_config(&harness);
    telemetry::mock_telemetry_endpoint(&harness.server).await;

    queue_resources::mock_list_queue_environments(
        &harness.server, "farm-abc", "queue-abc",
        &[json!({"queueEnvironmentId": "env-001", "name": "Render Env", "priority": 10})],
    ).await;

    queue_resources::mock_get_queue_environment(
        &harness.server, "farm-abc", "queue-abc", "env-001",
        json!({
            "queueEnvironmentId": "env-001",
            "name": "Render Env",
            "priority": 10,
            "templateType": "YAML",
            "template": env_template("Render Env", &[json!({
                "name": "Cores",
                "type": "INT",
                "default": "4",
            })]),
        }),
    ).await;

    assert_cmd_snapshot!(harness.cmd(&["queue", "paramdefs"]));
}

// ---------------------------------------------------------------------------
// Happy path: multiple environments sorted by priority (§8 case 2)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn queue_paramdefs_multiple_envs_sorted_by_priority() {
    let harness = TestHarness::new().await;
    setup_config(&harness);
    telemetry::mock_telemetry_endpoint(&harness.server).await;

    queue_resources::mock_list_queue_environments(
        &harness.server, "farm-abc", "queue-abc",
        &[
            json!({"queueEnvironmentId": "env-low", "name": "Low Priority", "priority": 50}),
            json!({"queueEnvironmentId": "env-high", "name": "High Priority", "priority": 10}),
        ],
    ).await;

    queue_resources::mock_get_queue_environment(
        &harness.server, "farm-abc", "queue-abc", "env-high",
        json!({
            "queueEnvironmentId": "env-high",
            "name": "High Priority",
            "priority": 10,
            "templateType": "YAML",
            "template": env_template("High Priority", &[json!({
                "name": "Memory",
                "type": "INT",
                "default": "8",
            })]),
        }),
    ).await;

    queue_resources::mock_get_queue_environment(
        &harness.server, "farm-abc", "queue-abc", "env-low",
        json!({
            "queueEnvironmentId": "env-low",
            "name": "Low Priority",
            "priority": 50,
            "templateType": "YAML",
            "template": env_template("Low Priority", &[json!({
                "name": "GPU",
                "type": "STRING",
                "default": "nvidia",
            })]),
        }),
    ).await;

    assert_cmd_snapshot!(harness.cmd(&["queue", "paramdefs"]));
}

// ---------------------------------------------------------------------------
// Happy path: no environments returns empty list (§8 case 5)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn queue_paramdefs_no_environments_returns_empty() {
    let harness = TestHarness::new().await;
    setup_config(&harness);
    telemetry::mock_telemetry_endpoint(&harness.server).await;

    queue_resources::mock_list_queue_environments(
        &harness.server, "farm-abc", "queue-abc", &[],
    ).await;

    assert_cmd_snapshot!(harness.cmd(&["queue", "paramdefs"]));
}

// ---------------------------------------------------------------------------
// Happy path: duplicate param name with identical definition (§8 case 7)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn queue_paramdefs_duplicate_identical_keeps_one() {
    let harness = TestHarness::new().await;
    setup_config(&harness);
    telemetry::mock_telemetry_endpoint(&harness.server).await;

    let shared_param = json!({"name": "Cores", "type": "INT", "default": "4"});

    queue_resources::mock_list_queue_environments(
        &harness.server, "farm-abc", "queue-abc",
        &[
            json!({"queueEnvironmentId": "env-a", "name": "Env A", "priority": 10}),
            json!({"queueEnvironmentId": "env-b", "name": "Env B", "priority": 20}),
        ],
    ).await;

    queue_resources::mock_get_queue_environment(
        &harness.server, "farm-abc", "queue-abc", "env-a",
        json!({
            "queueEnvironmentId": "env-a", "name": "Env A", "priority": 10,
            "templateType": "YAML",
            "template": env_template("Env A", &[shared_param.clone()]),
        }),
    ).await;

    queue_resources::mock_get_queue_environment(
        &harness.server, "farm-abc", "queue-abc", "env-b",
        json!({
            "queueEnvironmentId": "env-b", "name": "Env B", "priority": 20,
            "templateType": "YAML",
            "template": env_template("Env B", &[shared_param]),
        }),
    ).await;

    assert_cmd_snapshot!(harness.cmd(&["queue", "paramdefs"]));
}

// ---------------------------------------------------------------------------
// Error: duplicate param name with different definitions (§8 case 6)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn queue_paramdefs_duplicate_different_errors() {
    let harness = TestHarness::new().await;
    setup_config(&harness);
    telemetry::mock_telemetry_endpoint(&harness.server).await;

    queue_resources::mock_list_queue_environments(
        &harness.server, "farm-abc", "queue-abc",
        &[
            json!({"queueEnvironmentId": "env-a", "name": "Env A", "priority": 10}),
            json!({"queueEnvironmentId": "env-b", "name": "Env B", "priority": 20}),
        ],
    ).await;

    queue_resources::mock_get_queue_environment(
        &harness.server, "farm-abc", "queue-abc", "env-a",
        json!({
            "queueEnvironmentId": "env-a", "name": "Env A", "priority": 10,
            "templateType": "YAML",
            "template": env_template("Env A", &[json!({"name": "Cores", "type": "INT", "default": "4"})]),
        }),
    ).await;

    queue_resources::mock_get_queue_environment(
        &harness.server, "farm-abc", "queue-abc", "env-b",
        json!({
            "queueEnvironmentId": "env-b", "name": "Env B", "priority": 20,
            "templateType": "YAML",
            "template": env_template("Env B", &[json!({"name": "Cores", "type": "FLOAT", "default": "4.0"})]),
        }),
    ).await;

    assert_cmd_snapshot!(harness.cmd(&["queue", "paramdefs"]));
}

// ---------------------------------------------------------------------------
// Happy path: parameter with existing groupLabel preserved (§8 case 3)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn queue_paramdefs_existing_group_label_preserved() {
    let harness = TestHarness::new().await;
    setup_config(&harness);
    telemetry::mock_telemetry_endpoint(&harness.server).await;

    queue_resources::mock_list_queue_environments(
        &harness.server, "farm-abc", "queue-abc",
        &[json!({"queueEnvironmentId": "env-001", "name": "My Env", "priority": 10})],
    ).await;

    queue_resources::mock_get_queue_environment(
        &harness.server, "farm-abc", "queue-abc", "env-001",
        json!({
            "queueEnvironmentId": "env-001", "name": "My Env", "priority": 10,
            "templateType": "YAML",
            "template": env_template("My Env", &[json!({
                "name": "OutputDir",
                "type": "PATH",
                "default": "/tmp/output",
                "userInterface": {
                    "control": "CHOOSE_DIRECTORY",
                    "groupLabel": "Custom Group",
                },
            })]),
        }),
    ).await;

    assert_cmd_snapshot!(harness.cmd(&["queue", "paramdefs"]));
}

// ---------------------------------------------------------------------------
// Happy path: env template with no parameterDefinitions (§8 case 10)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn queue_paramdefs_env_with_no_params_contributes_nothing() {
    let harness = TestHarness::new().await;
    setup_config(&harness);
    telemetry::mock_telemetry_endpoint(&harness.server).await;

    // Template with no parameterDefinitions field
    let template_no_params = serde_yaml::to_string(&serde_json::json!({
        "specificationVersion": "environment-2023-09",
        "environment": {"name": "Empty Env"},
    })).unwrap();

    queue_resources::mock_list_queue_environments(
        &harness.server, "farm-abc", "queue-abc",
        &[
            json!({"queueEnvironmentId": "env-empty", "name": "Empty Env", "priority": 10}),
            json!({"queueEnvironmentId": "env-real", "name": "Real Env", "priority": 20}),
        ],
    ).await;

    queue_resources::mock_get_queue_environment(
        &harness.server, "farm-abc", "queue-abc", "env-empty",
        json!({
            "queueEnvironmentId": "env-empty", "name": "Empty Env", "priority": 10,
            "templateType": "YAML",
            "template": template_no_params,
        }),
    ).await;

    queue_resources::mock_get_queue_environment(
        &harness.server, "farm-abc", "queue-abc", "env-real",
        json!({
            "queueEnvironmentId": "env-real", "name": "Real Env", "priority": 20,
            "templateType": "YAML",
            "template": env_template("Real Env", &[json!({
                "name": "Threads",
                "type": "INT",
                "default": "2",
            })]),
        }),
    ).await;

    assert_cmd_snapshot!(harness.cmd(&["queue", "paramdefs"]));
}
