//! Level 2 tests for `deadline job download-input`.
//!
//! Test spec reference: GAP-2 in `specs/HANDOFF.md`.
//! Python reference: `test/cli_e2e/test_job_download_input.py`.

use deadline_test_server::TestHarness;
use deadline_test_server::deadline_api::{errors, jobs, queues, s3, sts};
use insta_cmd::assert_cmd_snapshot;
use serde_json::json;

const FARM: &str = "farm-0123456789abcdef0123456789abcdef";
const QUEUE: &str = "queue-0123456789abcdef0123456789abcdef";
const JOB: &str = "job-0123456789abcdef0123456789abcdef";

fn job_with_input_attachments(asset_root: &str) -> serde_json::Value {
    json!({
        "jobId": JOB,
        "name": "Render Job",
        "lifecycleStatus": "CREATE_COMPLETE",
        "taskRunStatus": "SUCCEEDED",
        "taskRunStatusCounts": { "SUCCEEDED": 10 },
        "attachments": {
            "manifests": [
                {
                    "rootPath": asset_root,
                    "rootPathFormat": "posix",
                    "inputManifestPath": "farm-abc/queue-abc/Inputs/manifest123.manifest",
                    "inputManifestHash": "abc123"
                }
            ],
            "fileSystem": "COPIED"
        }
    })
}

fn job_without_attachments() -> serde_json::Value {
    json!({
        "jobId": JOB,
        "name": "No Attachments Job",
        "lifecycleStatus": "CREATE_COMPLETE",
        "taskRunStatus": "SUCCEEDED",
        "taskRunStatusCounts": { "SUCCEEDED": 5 },
    })
}

fn queue_with_attachment_settings() -> serde_json::Value {
    json!({
        "queueId": QUEUE,
        "displayName": "Test Queue",
        "jobAttachmentSettings": {
            "s3BucketName": "test-bucket",
            "rootPrefix": "root-prefix"
        }
    })
}

fn input_manifest_json() -> String {
    json!({
        "manifestVersion": "2023-03-03",
        "hashAlg": "xxh128",
        "totalSize": 200,
        "paths": [
            {"path": "scene.ma", "hash": "aaa111bbb222ccc333ddd444eee55566", "size": 100, "mtime": 1_700_000_000},
            {"path": "textures/brick.png", "hash": "fff666eee555ddd444ccc333bbb22211", "size": 100, "mtime": 1_700_000_000}
        ]
    })
    .to_string()
}

/// Set up mocks for a download-input test with an input manifest in S3.
async fn setup_input_manifest_mocks(harness: &TestHarness, asset_root: &str) {
    jobs::mock_get_job(
        &harness.server,
        FARM,
        QUEUE,
        job_with_input_attachments(asset_root),
    )
    .await;
    queues::mock_get_queue(&harness.server, FARM, queue_with_attachment_settings()).await;
    sts::mock_get_caller_identity(&harness.server).await;

    // The input manifest is fetched via GetObject at the inputManifestPath key
    let manifest_key = "root-prefix/Manifests/farm-abc/queue-abc/Inputs/manifest123.manifest";
    s3::mock_s3_get_object_with_metadata(
        &harness.server,
        &format!("test-bucket/{manifest_key}"),
        input_manifest_json().as_bytes(),
        &[("asset-root", asset_root)],
    )
    .await;
}

// =====================================================================
// Missing required args
// =====================================================================

#[tokio::test]
async fn job_download_input_missing_farm_id_exits_with_error() {
    let harness = TestHarness::new().await;
    assert_cmd_snapshot!(harness.cmd(&[
        "job",
        "download-input",
        "--queue-id",
        QUEUE,
        "--job-id",
        JOB,
    ]));
}

#[tokio::test]
async fn job_download_input_missing_queue_id_exits_with_error() {
    let harness = TestHarness::new().await;
    assert_cmd_snapshot!(harness.cmd(&[
        "job",
        "download-input",
        "--farm-id",
        FARM,
        "--job-id",
        JOB,
    ]));
}

#[tokio::test]
async fn job_download_input_missing_job_id_exits_with_error() {
    let harness = TestHarness::new().await;
    assert_cmd_snapshot!(harness.cmd(&[
        "job",
        "download-input",
        "--farm-id",
        FARM,
        "--queue-id",
        QUEUE,
    ]));
}

// =====================================================================
// No attachments → "No input attachments found"
// =====================================================================

#[tokio::test]
async fn job_download_input_no_attachments_message() {
    let harness = TestHarness::new().await;
    jobs::mock_get_job(&harness.server, FARM, QUEUE, job_without_attachments()).await;
    queues::mock_get_queue(&harness.server, FARM, queue_with_attachment_settings()).await;
    sts::mock_get_caller_identity(&harness.server).await;

    assert_cmd_snapshot!(harness.cmd(&[
        "job",
        "download-input",
        "--farm-id",
        FARM,
        "--queue-id",
        QUEUE,
        "--job-id",
        JOB,
        "--yes",
    ]));
}

// =====================================================================
// GetJob API error
// =====================================================================

#[tokio::test]
async fn job_download_input_get_job_error_exits_with_error() {
    let harness = TestHarness::new().await;
    errors::mock_get_job_not_found(&harness.server, FARM, QUEUE, JOB).await;

    assert_cmd_snapshot!(harness.cmd(&[
        "job",
        "download-input",
        "--farm-id",
        FARM,
        "--queue-id",
        QUEUE,
        "--job-id",
        JOB,
        "--yes",
    ]));
}

// =====================================================================
// --output json with error → JSON error line
// =====================================================================

#[tokio::test]
async fn job_download_input_json_mode_error() {
    let harness = TestHarness::new().await;
    errors::mock_get_job_not_found(&harness.server, FARM, QUEUE, JOB).await;

    assert_cmd_snapshot!(harness.cmd(&[
        "job",
        "download-input",
        "--farm-id",
        FARM,
        "--queue-id",
        QUEUE,
        "--job-id",
        JOB,
        "--output",
        "json",
    ]));
}

// =====================================================================
// --output json with no attachments → JSON summary line
// =====================================================================

#[tokio::test]
async fn job_download_input_json_mode_no_attachments() {
    let harness = TestHarness::new().await;
    jobs::mock_get_job(&harness.server, FARM, QUEUE, job_without_attachments()).await;
    queues::mock_get_queue(&harness.server, FARM, queue_with_attachment_settings()).await;
    sts::mock_get_caller_identity(&harness.server).await;

    assert_cmd_snapshot!(harness.cmd(&[
        "job",
        "download-input",
        "--farm-id",
        FARM,
        "--queue-id",
        QUEUE,
        "--job-id",
        JOB,
        "--output",
        "json",
        "--yes",
    ]));
}

// =====================================================================
// --include with no match → "No input files match the provided filters"
// =====================================================================

#[tokio::test]
async fn job_download_input_include_no_match() {
    let harness = TestHarness::new().await;

    let output_dir = tempfile::TempDir::new().unwrap();
    let output_root = output_dir.path().to_str().unwrap();
    setup_input_manifest_mocks(&harness, output_root).await;

    // The manifest has "scene.ma" and "textures/brick.png" but we filter for "*.nonexistent"
    assert_cmd_snapshot!(harness.cmd(&[
        "job",
        "download-input",
        "--farm-id",
        FARM,
        "--queue-id",
        QUEUE,
        "--job-id",
        JOB,
        "--include",
        "*.nonexistent",
        "--yes",
    ]));
}

// =====================================================================
// --include with matching glob downloads only matching files
// =====================================================================

#[tokio::test]
async fn job_download_input_include_glob_filters() {
    let harness = TestHarness::new().await;

    let output_dir = tempfile::TempDir::new().unwrap();
    let output_root = output_dir.path().to_str().unwrap();
    setup_input_manifest_mocks(&harness, output_root).await;

    // Mock S3 GetObject for the actual file download (CAS path)
    s3::mock_s3_get_object_catchall(&harness.server, b"png-content", &[]).await;

    let output = harness
        .cli(&[
            "job",
            "download-input",
            "--farm-id",
            FARM,
            "--queue-id",
            QUEUE,
            "--job-id",
            JOB,
            "--include",
            "*.png",
            "--conflict-resolution",
            "OVERWRITE",
            "--yes",
        ])
        .output()
        .expect("failed to run");

    let stdout = String::from_utf8_lossy(&output.stdout);

    // Should download only the .png file, not the .ma file
    assert!(
        stdout.contains("Downloaded 1 file"),
        "Expected 1 file downloaded (only .png), got: {stdout}"
    );
    // The .png file should exist
    assert!(
        output_dir.path().join("textures/brick.png").exists(),
        "Expected textures/brick.png to be downloaded"
    );
    // The .ma file should NOT exist
    assert!(
        !output_dir.path().join("scene.ma").exists(),
        "Expected scene.ma to NOT be downloaded"
    );
}

// =====================================================================
// --match-paths-by JOB filters at construction time
// =====================================================================

#[tokio::test]
async fn job_download_input_match_paths_by_job() {
    let harness = TestHarness::new().await;

    let output_dir = tempfile::TempDir::new().unwrap();
    let output_root = output_dir.path().to_str().unwrap();
    setup_input_manifest_mocks(&harness, output_root).await;

    // Mock S3 GetObject for the actual file download (CAS path)
    s3::mock_s3_get_object_catchall(&harness.server, b"png-content", &[]).await;

    let output = harness
        .cli(&[
            "job",
            "download-input",
            "--farm-id",
            FARM,
            "--queue-id",
            QUEUE,
            "--job-id",
            JOB,
            "--include",
            "*.png",
            "--match-paths-by",
            "JOB",
            "--conflict-resolution",
            "OVERWRITE",
            "--yes",
        ])
        .output()
        .expect("failed to run");

    let stdout = String::from_utf8_lossy(&output.stdout);

    // Should download only the .png file
    assert!(
        stdout.contains("Downloaded 1 file"),
        "Expected 1 file downloaded with --match-paths-by JOB, got: {stdout}"
    );
    assert!(
        output_dir.path().join("textures/brick.png").exists(),
        "Expected textures/brick.png to be downloaded"
    );
    assert!(
        !output_dir.path().join("scene.ma").exists(),
        "Expected scene.ma to NOT be downloaded"
    );
}

// =====================================================================
// No inputManifestPath in attachments → "No input files available"
// =====================================================================

#[tokio::test]
async fn job_download_input_no_input_manifest_path_shows_no_files() {
    let harness = TestHarness::new().await;

    // Job has attachments but the manifest entry has no inputManifestPath
    let job = json!({
        "jobId": JOB,
        "name": "Output Only Job",
        "lifecycleStatus": "CREATE_COMPLETE",
        "taskRunStatus": "SUCCEEDED",
        "taskRunStatusCounts": { "SUCCEEDED": 1 },
        "attachments": {
            "manifests": [
                {
                    "rootPath": "/tmp/outputs",
                    "rootPathFormat": "posix",
                    "outputRelativeDirectories": ["outputs"]
                }
            ],
            "fileSystem": "COPIED"
        }
    });
    jobs::mock_get_job(&harness.server, FARM, QUEUE, job).await;
    queues::mock_get_queue(&harness.server, FARM, queue_with_attachment_settings()).await;
    sts::mock_get_caller_identity(&harness.server).await;

    assert_cmd_snapshot!(harness.cmd(&[
        "job",
        "download-input",
        "--farm-id",
        FARM,
        "--queue-id",
        QUEUE,
        "--job-id",
        JOB,
        "--yes",
    ]));
}

// =====================================================================
// Multiple --include values are OR'd
// =====================================================================

#[tokio::test]
async fn job_download_input_multiple_include_patterns_ored() {
    let harness = TestHarness::new().await;

    let output_dir = tempfile::TempDir::new().unwrap();
    let output_root = output_dir.path().to_str().unwrap();

    // Set up manifest with 3 files
    jobs::mock_get_job(
        &harness.server,
        FARM,
        QUEUE,
        json!({
            "jobId": JOB,
            "name": "Multi-file Job",
            "lifecycleStatus": "CREATE_COMPLETE",
            "taskRunStatus": "SUCCEEDED",
            "taskRunStatusCounts": { "SUCCEEDED": 1 },
            "attachments": {
                "manifests": [{
                    "rootPath": output_root,
                    "rootPathFormat": "posix",
                    "inputManifestPath": "farm-abc/queue-abc/Inputs/manifest123.manifest",
                    "inputManifestHash": "abc123"
                }],
                "fileSystem": "COPIED"
            }
        }),
    )
    .await;
    queues::mock_get_queue(&harness.server, FARM, queue_with_attachment_settings()).await;
    sts::mock_get_caller_identity(&harness.server).await;

    let manifest_key = "root-prefix/Manifests/farm-abc/queue-abc/Inputs/manifest123.manifest";
    let manifest_json = json!({
        "manifestVersion": "2023-03-03",
        "hashAlg": "xxh128",
        "totalSize": 300,
        "paths": [
            {"path": "scene.ma", "hash": "aaa111bbb222ccc333ddd444eee55566", "size": 100, "mtime": 1_700_000_000},
            {"path": "textures/brick.png", "hash": "fff666eee555ddd444ccc333bbb22211", "size": 100, "mtime": 1_700_000_000},
            {"path": "data/cache.bin", "hash": "111222333444555666777888999aaabbb", "size": 100, "mtime": 1_700_000_000}
        ]
    })
    .to_string();
    s3::mock_s3_get_object_with_metadata(
        &harness.server,
        &format!("test-bucket/{manifest_key}"),
        manifest_json.as_bytes(),
        &[("asset-root", output_root)],
    )
    .await;

    // Mock S3 GetObject for the actual file downloads
    s3::mock_s3_get_object_catchall(&harness.server, b"content", &[]).await;

    let output = harness
        .cli(&[
            "job",
            "download-input",
            "--farm-id",
            FARM,
            "--queue-id",
            QUEUE,
            "--job-id",
            JOB,
            "--include",
            "*.ma",
            "--include",
            "*.png",
            "--conflict-resolution",
            "OVERWRITE",
            "--yes",
        ])
        .output()
        .expect("failed to run");

    let stdout = String::from_utf8_lossy(&output.stdout);

    // Should download .ma and .png (2 files), but NOT .bin
    assert!(
        stdout.contains("Downloaded 2 file"),
        "Expected 2 files downloaded (*.ma + *.png OR'd), got: {stdout}"
    );
    assert!(output_dir.path().join("scene.ma").exists());
    assert!(output_dir.path().join("textures/brick.png").exists());
    assert!(
        !output_dir.path().join("data/cache.bin").exists(),
        "Expected data/cache.bin to NOT be downloaded"
    );
}

// =====================================================================
// Basic download-input downloads all input files
// =====================================================================

#[tokio::test]
async fn job_download_input_downloads_all_input_files() {
    let harness = TestHarness::new().await;

    let output_dir = tempfile::TempDir::new().unwrap();
    let output_root = output_dir.path().to_str().unwrap();
    setup_input_manifest_mocks(&harness, output_root).await;

    // Mock S3 GetObject for the actual file downloads (CAS path)
    s3::mock_s3_get_object_catchall(&harness.server, b"file-content", &[]).await;

    let output = harness
        .cli(&[
            "job",
            "download-input",
            "--farm-id",
            FARM,
            "--queue-id",
            QUEUE,
            "--job-id",
            JOB,
            "--conflict-resolution",
            "OVERWRITE",
            "--yes",
        ])
        .output()
        .expect("failed to run");

    let stdout = String::from_utf8_lossy(&output.stdout);

    // Should download both files
    assert!(
        stdout.contains("Downloaded 2 file"),
        "Expected 2 files downloaded, got: {stdout}"
    );
    assert!(
        output_dir.path().join("scene.ma").exists(),
        "Expected scene.ma to be downloaded"
    );
    assert!(
        output_dir.path().join("textures/brick.png").exists(),
        "Expected textures/brick.png to be downloaded"
    );
}
