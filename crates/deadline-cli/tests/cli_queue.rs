//! Level 2 tests for `deadline queue` subcommands.

use deadline_test_server::TestHarness;
use deadline_test_server::deadline_api::queues;
use insta_cmd::assert_cmd_snapshot;
use serde_json::json;

#[tokio::test]
async fn queue_list_prints_queue_ids_and_names() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    queues::mock_list_queues(&harness.server, "farm-abc", &[
        json!({"queueId": "queue-aaa", "displayName": "Queue A", "createdAt": "2024-01-01T00:00:00Z", "createdBy": "user"}),
        json!({"queueId": "queue-bbb", "displayName": "Queue B", "createdAt": "2024-01-02T00:00:00Z", "createdBy": "user"}),
    ]).await;

    assert_cmd_snapshot!(harness.cmd(&["queue", "list"]));
}

#[tokio::test]
async fn queue_list_no_farm_id_exits_with_error() {
    let harness = TestHarness::new().await;
    assert_cmd_snapshot!(harness.cmd(&["queue", "list"]));
}

#[tokio::test]
async fn queue_get_prints_all_fields() {
    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.queue_id", "queue-aaa"]).assert().success();
    queues::mock_get_queue(&harness.server, "farm-abc", json!({
        "queueId": "queue-aaa",
        "displayName": "Production Queue",
        "description": "The production queue.",
        "farmId": "farm-abc",
        "status": "SCHEDULING",
        "defaultBudgetAction": "NONE",
        "jobAttachmentSettings": {
            "s3BucketName": "my-bucket",
            "rootPrefix": "DeadlineCloud"
        },
        "roleArn": "arn:aws:iam::123456789012:role/QueueRole",
        "jobRunAsUser": {
            "windows": {
                "user": "job-user",
                "passwordArn": "arn:aws:secretsmanager:us-west-2:123456789012:secret:pw"
            },
            "runAs": "QUEUE_CONFIGURED_USER"
        },
        "createdAt": "2024-06-15T10:30:00Z",
        "createdBy": "arn:aws:sts::123456789012:assumed-role/Admin/user",
        "updatedAt": "2024-07-01T12:00:00Z",
        "updatedBy": "arn:aws:sts::123456789012:assumed-role/Admin/user"
    })).await;

    assert_cmd_snapshot!(harness.cmd(&["queue", "get"]));
}
