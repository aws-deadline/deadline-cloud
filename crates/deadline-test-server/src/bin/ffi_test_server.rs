//! Standalone stub server for Python FFI tests.
//!
//! Starts a wiremock server with canned API responses, prints connection
//! info as JSON to stdout, then waits for SIGTERM.
//!
//! Usage: cargo run --bin ffi-test-server

use deadline_test_server::TestHarness;
use deadline_test_server::deadline_api::{farms, queue_resources, queues, sts};
use serde_json::json;

#[tokio::main]
async fn main() {
    let harness = TestHarness::new().await;

    // STS — needed for auth status checks
    sts::mock_get_caller_identity(&harness.server).await;

    // Farms
    farms::mock_list_farms(
        &harness.server,
        &[json!({
            "farmId": "farm-abc123def4567890abc123def4567890",
            "displayName": "Test Farm",
        })],
    )
    .await;

    farms::mock_get_farm(
        &harness.server,
        json!({
            "farmId": "farm-abc123def4567890abc123def4567890",
            "displayName": "Test Farm",
            "description": "A test farm",
        }),
    )
    .await;

    // Queues
    queues::mock_list_queues(
        &harness.server,
        "farm-abc123def4567890abc123def4567890",
        &[json!({
            "queueId": "queue-abc123def4567890abc123def4567890",
            "displayName": "Test Queue",
        })],
    )
    .await;

    queues::mock_get_queue(
        &harness.server,
        "farm-abc123def4567890abc123def4567890",
        json!({
            "queueId": "queue-abc123def4567890abc123def4567890",
            "displayName": "Test Queue",
            "description": "A test queue",
        }),
    )
    .await;

    // Storage profiles
    queue_resources::mock_list_storage_profiles_for_queue(
        &harness.server,
        "farm-abc123def4567890abc123def4567890",
        "queue-abc123def4567890abc123def4567890",
        &[json!({
            "storageProfileId": "sp-abc123def4567890abc123def4567890",
            "displayName": "Test Storage Profile",
            "osFamily": "linux",
        })],
    )
    .await;

    // Queue environments (for queue parameter definitions)
    queue_resources::mock_list_queue_environments(
        &harness.server,
        "farm-abc123def4567890abc123def4567890",
        "queue-abc123def4567890abc123def4567890",
        &[], // empty — no queue parameters
    )
    .await;

    // Print connection info as JSON — use localhost (not 127.0.0.1)
    // because the Deadline SDK prepends "management." to the hostname,
    // and "management.localhost" resolves to 127.0.0.1 per RFC 6761,
    // while "management.127.0.0.1" is not a valid hostname.
    let port = harness.server.address().port();
    let info = json!({
        "endpoint": format!("http://localhost:{port}"),
        "config_path": harness.config_path,
    });
    #[allow(
        clippy::print_stdout,
        reason = "test binary outputs server info for harness to read"
    )]
    {
        println!("{info}");
    }

    // Wait for termination
    tokio::signal::ctrl_c().await.ok();
}
