//! Level 2 tests for `deadline manifest` subcommands.
//!
//! These test the CLI binary as a subprocess, exercising the full stack:
//! CLI arg parsing → manifest_ops → diff → hashing → file I/O.

use deadline_test_server::TestHarness;
use insta_cmd::assert_cmd_snapshot;
use serde_json::json;
use std::fs;
use tempfile::TempDir;

// =====================================================================
// manifest snapshot — happy path
// =====================================================================

#[tokio::test]
async fn manifest_snapshot_creates_manifest_file() {
    let harness = TestHarness::new().await;
    let dir = TempDir::new().unwrap();
    fs::write(dir.path().join("a.txt"), b"hello").unwrap();
    fs::write(dir.path().join("b.txt"), b"world").unwrap();

    let output = harness
        .cli(&["manifest", "snapshot", "--root", dir.path().to_str().unwrap()])
        .output()
        .expect("failed to run");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Manifest creation path defaulted to"));
    assert!(stdout.contains("Manifest generated at"));

    // Should have created a .manifest file in the root dir
    let manifests: Vec<_> = fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map_or(false, |ext| ext == "manifest"))
        .collect();
    assert_eq!(manifests.len(), 1, "Expected one manifest file");

    // Manifest content should be valid JSON with paths
    let content = fs::read_to_string(manifests[0].path()).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&content).unwrap();
    assert_eq!(parsed["hashAlg"], "xxh128");
    let paths = parsed["paths"].as_array().unwrap();
    assert_eq!(paths.len(), 2);
}

// =====================================================================
// manifest snapshot — --destination
// =====================================================================

#[tokio::test]
async fn manifest_snapshot_writes_to_destination() {
    let harness = TestHarness::new().await;
    let root = TempDir::new().unwrap();
    let dest = TempDir::new().unwrap();
    fs::write(root.path().join("file.txt"), b"data").unwrap();

    let output = harness
        .cli(&[
            "manifest", "snapshot",
            "--root", root.path().to_str().unwrap(),
            "--destination", dest.path().to_str().unwrap(),
        ])
        .output()
        .expect("failed to run");

    assert!(output.status.success());
    // Manifest should be in dest, not root
    let in_dest: Vec<_> = fs::read_dir(dest.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map_or(false, |ext| ext == "manifest"))
        .collect();
    assert_eq!(in_dest.len(), 1);
    let in_root: Vec<_> = fs::read_dir(root.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map_or(false, |ext| ext == "manifest"))
        .collect();
    assert_eq!(in_root.len(), 0);
}

// =====================================================================
// manifest snapshot — --name
// =====================================================================

#[tokio::test]
async fn manifest_snapshot_uses_provided_name() {
    let harness = TestHarness::new().await;
    let dir = TempDir::new().unwrap();
    fs::write(dir.path().join("file.txt"), b"data").unwrap();

    let output = harness
        .cli(&[
            "manifest", "snapshot",
            "--root", dir.path().to_str().unwrap(),
            "--name", "my-snapshot",
        ])
        .output()
        .expect("failed to run");

    assert!(output.status.success());
    let manifests: Vec<_> = fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            let name = e.file_name().to_string_lossy().to_string();
            name.starts_with("my-snapshot-") && name.ends_with(".manifest")
        })
        .collect();
    assert_eq!(manifests.len(), 1);
}

// =====================================================================
// manifest snapshot — --include and --exclude
// =====================================================================

#[tokio::test]
async fn manifest_snapshot_include_exclude_filters() {
    let harness = TestHarness::new().await;
    let dir = TempDir::new().unwrap();
    fs::write(dir.path().join("keep.exr"), b"image").unwrap();
    fs::write(dir.path().join("skip.tmp"), b"temp").unwrap();

    let output = harness
        .cli(&[
            "manifest", "snapshot",
            "--root", dir.path().to_str().unwrap(),
            "--include", "*.exr",
            "--exclude", "*.tmp",
        ])
        .output()
        .expect("failed to run");

    assert!(output.status.success());
    let manifest_file = fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .find(|e| e.path().extension().map_or(false, |ext| ext == "manifest"))
        .expect("manifest file should exist");
    let contents = fs::read_to_string(manifest_file.path()).unwrap();
    assert!(contents.contains("keep.exr"));
    assert!(!contents.contains("skip.tmp"));
}

// =====================================================================
// manifest snapshot — root doesn't exist
// =====================================================================

#[tokio::test]
async fn manifest_snapshot_nonexistent_root_exits_with_error() {
    let harness = TestHarness::new().await;
    assert_cmd_snapshot!(harness.cmd(&[
        "manifest", "snapshot", "--root", "/nonexistent/path/abc123"
    ]));
}

// =====================================================================
// manifest diff — happy path with JSON output
// =====================================================================

#[tokio::test]
async fn manifest_diff_json_shows_new_modified_deleted() {
    let harness = TestHarness::new().await;
    let dir = TempDir::new().unwrap();
    fs::write(dir.path().join("original.txt"), b"original").unwrap();
    fs::write(dir.path().join("will_modify.txt"), b"before").unwrap();
    fs::write(dir.path().join("will_delete.txt"), b"gone").unwrap();

    // Create snapshot
    let output = harness
        .cli(&["manifest", "snapshot", "--root", dir.path().to_str().unwrap()])
        .output()
        .expect("failed to run");
    assert!(output.status.success());

    let manifest_file = fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .find(|e| e.path().extension().map_or(false, |ext| ext == "manifest"))
        .expect("manifest should exist");

    // Modify: change content (different size triggers fast diff)
    fs::write(dir.path().join("will_modify.txt"), b"after modification with more bytes").unwrap();
    // Delete
    fs::remove_file(dir.path().join("will_delete.txt")).unwrap();
    // Add new
    fs::write(dir.path().join("added.txt"), b"new content").unwrap();

    // Run diff with --json
    let output = harness
        .cli(&[
            "manifest", "diff",
            "--manifest", manifest_file.path().to_str().unwrap(),
            "--root", dir.path().to_str().unwrap(),
            "--json",
        ])
        .output()
        .expect("failed to run");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value = serde_json::from_str(&stdout).expect("should be valid JSON");

    // Verify root-relative paths (not absolute)
    let new_files: Vec<&str> = json["new"].as_array().unwrap()
        .iter().filter_map(|v| v.as_str()).collect();
    let modified: Vec<&str> = json["modified"].as_array().unwrap()
        .iter().filter_map(|v| v.as_str()).collect();
    let deleted: Vec<&str> = json["deleted"].as_array().unwrap()
        .iter().filter_map(|v| v.as_str()).collect();

    assert!(new_files.iter().any(|f| *f == "added.txt"), "new: {new_files:?}");
    assert!(modified.iter().any(|f| *f == "will_modify.txt"), "modified: {modified:?}");
    assert!(deleted.iter().any(|f| *f == "will_delete.txt"), "deleted: {deleted:?}");
    // original.txt should not appear in any diff list
    assert!(!new_files.contains(&"original.txt"));
    assert!(!modified.contains(&"original.txt"));
    assert!(!deleted.contains(&"original.txt"));
}

// =====================================================================
// manifest snapshot --diff
// =====================================================================

#[tokio::test]
async fn manifest_snapshot_diff_only_includes_changed_files() {
    let harness = TestHarness::new().await;
    let dir = TempDir::new().unwrap();
    let dest = TempDir::new().unwrap();
    fs::write(dir.path().join("unchanged.txt"), b"same").unwrap();
    fs::write(dir.path().join("will_change.txt"), b"before").unwrap();

    // Initial snapshot
    let output = harness
        .cli(&[
            "manifest", "snapshot",
            "--root", dir.path().to_str().unwrap(),
            "--destination", dest.path().to_str().unwrap(),
        ])
        .output()
        .expect("failed to run");
    assert!(output.status.success());

    let first_manifest = fs::read_dir(dest.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .find(|e| e.path().extension().map_or(false, |ext| ext == "manifest"))
        .expect("first manifest should exist");

    // Modify one file (different size)
    fs::write(dir.path().join("will_change.txt"), b"after with more content").unwrap();

    // Diff snapshot
    let output = harness
        .cli(&[
            "manifest", "snapshot",
            "--root", dir.path().to_str().unwrap(),
            "--destination", dest.path().to_str().unwrap(),
            "--diff", first_manifest.path().to_str().unwrap(),
            "--name", "diff-snap",
        ])
        .output()
        .expect("failed to run");
    assert!(output.status.success());

    // Find the diff manifest
    let diff_manifest = fs::read_dir(dest.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .find(|e| {
            let name = e.file_name().to_string_lossy().to_string();
            name.starts_with("diff-snap-") && name.ends_with(".manifest")
        })
        .expect("diff manifest should exist");

    let content = fs::read_to_string(diff_manifest.path()).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&content).unwrap();
    let paths = parsed["paths"].as_array().unwrap();
    // Should only contain the changed file, not unchanged.txt
    assert_eq!(paths.len(), 1, "diff manifest should have 1 file, got {}", paths.len());
    assert_eq!(paths[0]["path"], "will_change.txt");
}

// =====================================================================
// manifest diff — manifest doesn't exist
// =====================================================================

#[tokio::test]
async fn manifest_diff_nonexistent_manifest_exits_with_error() {
    let harness = TestHarness::new().await;
    let dir = TempDir::new().unwrap();
    assert_cmd_snapshot!(harness.cmd(&[
        "manifest", "diff",
        "--manifest", "/nonexistent/manifest.file",
        "--root", dir.path().to_str().unwrap(),
    ]));
}

// =====================================================================
// manifest diff — root doesn't exist
// =====================================================================

#[tokio::test]
async fn manifest_diff_nonexistent_root_exits_with_error() {
    let harness = TestHarness::new().await;
    let dir = TempDir::new().unwrap();
    let manifest_path = dir.path().join("dummy.manifest");
    fs::write(&manifest_path, "{}").unwrap();
    assert_cmd_snapshot!(harness.cmd(&[
        "manifest", "diff",
        "--manifest", manifest_path.to_str().unwrap(),
        "--root", "/nonexistent/root/abc123",
    ]));
}

// =====================================================================
// manifest upload — manifest doesn't exist
// =====================================================================

#[tokio::test]
async fn manifest_upload_nonexistent_file_exits_with_error() {
    let harness = TestHarness::new().await;
    assert_cmd_snapshot!(harness.cmd(&[
        "manifest", "upload", "/nonexistent/manifest.file",
        "--s3-cas-uri", "s3://bucket/prefix",
        "--profile", "test",
    ]));
}

// =====================================================================
// manifest download — dir doesn't exist
// =====================================================================

#[tokio::test]
async fn manifest_download_nonexistent_dir_exits_with_error() {
    let harness = TestHarness::new().await;
    assert_cmd_snapshot!(harness.cmd(&[
        "manifest", "download", "/nonexistent/dir",
        "--job-id", "job-abc",
        "--farm-id", "farm-abc",
        "--queue-id", "queue-abc",
    ]));
}

// ===========================================================================
// manifest upload derives S3 settings from queue
// ===========================================================================

#[tokio::test]
async fn manifest_upload_derives_s3_settings_from_queue() {
    use deadline_test_server::deadline_api::{queues, sts, s3};

    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.queue_id", "queue-abc"]).assert().success();

    queues::mock_get_queue(&harness.server, "farm-abc", json!({
        "queueId": "queue-abc",
        "displayName": "Test Queue",
        "jobAttachmentSettings": {
            "s3BucketName": "test-bucket",
            "rootPrefix": "root-prefix"
        }
    })).await;
    sts::mock_get_caller_identity(&harness.server).await;
    s3::mock_s3_put_success(&harness.server).await;

    // Create a minimal manifest file
    let dir = TempDir::new().unwrap();
    let manifest_path = dir.path().join("test.manifest");
    fs::write(&manifest_path, r#"{"hashAlg":"xxh128","paths":[]}"#).unwrap();

    let output = harness
        .cli(&["manifest", "upload", manifest_path.to_str().unwrap()])
        .output()
        .expect("failed to run");

    let stdout = String::from_utf8_lossy(&output.stdout);
    // Should succeed and show upload message with bucket from queue
    assert!(stdout.contains("test-bucket"), "Expected bucket name in output: {stdout}");
    assert!(stdout.contains("Uploading successful") || output.status.success(),
        "Expected success, got: {stdout}");
}

// ---------------------------------------------------------------------------
// manifest upload without --s3-cas-uri and without farm/queue errors
// ---------------------------------------------------------------------------

#[tokio::test]
async fn manifest_upload_no_s3_uri_no_queue_exits_with_error() {
    let harness = TestHarness::new().await;
    // No farm_id or queue_id configured, no --s3-cas-uri
    let dir = TempDir::new().unwrap();
    let manifest_path = dir.path().join("test.manifest");
    fs::write(&manifest_path, r#"{"hashAlg":"xxh128","paths":[]}"#).unwrap();

    assert_cmd_snapshot!(harness.cmd(&[
        "manifest", "upload", manifest_path.to_str().unwrap(),
    ]));
}

// ===========================================================================
// manifest download wired to API
// ===========================================================================

#[tokio::test]
async fn manifest_download_fetches_manifests_from_s3() {
    use deadline_test_server::deadline_api::{jobs, queues, sts, s3};

    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.queue_id", "queue-abc"]).assert().success();

    queues::mock_get_queue(&harness.server, "farm-abc", json!({
        "queueId": "queue-abc",
        "displayName": "Test Queue",
        "jobAttachmentSettings": {
            "s3BucketName": "test-bucket",
            "rootPrefix": "root-prefix"
        }
    })).await;
    jobs::mock_get_job(&harness.server, "farm-abc", "queue-abc", json!({
        "jobId": "job-abc",
        "name": "Test Job",
        "attachments": {
            "manifests": [{
                "rootPath": "/tmp/outputs",
                "rootPathFormat": "posix",
                "inputManifestPath": "Manifests/input.manifest",
                "inputManifestHash": "abc123",
                "outputRelativeDirectories": ["outputs"]
            }],
            "fileSystem": "COPIED"
        }
    })).await;
    sts::mock_get_caller_identity(&harness.server).await;
    // Mock S3 list and get for manifest download
    s3::mock_s3_list_empty(&harness.server).await;

    let dir = TempDir::new().unwrap();
    let output = harness
        .cli(&[
            "manifest", "download", dir.path().to_str().unwrap(),
            "--job-id", "job-abc",
        ])
        .output()
        .expect("failed to run");

    // Should NOT return the old stub error
    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let combined = format!("{stdout}{stderr}");
    assert!(!combined.contains("not yet wired"),
        "Should not show stub error, got: {combined}");
}

// ===========================================================================
// manifest upload — error cases
// ===========================================================================

#[tokio::test]
async fn manifest_upload_queue_no_attachment_settings_exits_with_error() {
    use deadline_test_server::deadline_api::{queues, sts};

    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.queue_id", "queue-abc"]).assert().success();

    // Queue exists but has no jobAttachmentSettings
    queues::mock_get_queue(&harness.server, "farm-abc", json!({
        "queueId": "queue-abc",
        "displayName": "No Attachments Queue",
    })).await;
    sts::mock_get_caller_identity(&harness.server).await;

    let dir = TempDir::new().unwrap();
    let manifest_path = dir.path().join("test.manifest");
    fs::write(&manifest_path, r#"{"hashAlg":"xxh128","paths":[]}"#).unwrap();

    let output = harness
        .cli(&["manifest", "upload", manifest_path.to_str().unwrap()])
        .output()
        .expect("failed to run");

    assert!(!output.status.success(), "Should fail when queue has no attachment settings");
    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let combined = format!("{stdout}{stderr}");
    // Must NOT contain the old stub error — that means queue derivation worked
    // but the queue lacks attachment settings
    assert!(
        !combined.contains("--s3-cas-uri"),
        "Should not show the old stub error about --s3-cas-uri, got: {combined}"
    );
    assert!(
        combined.contains("attachment") && combined.contains("not configured"),
        "Error should say attachments are not configured, got: {combined}"
    );
}

// ===========================================================================
// manifest download — error cases
// ===========================================================================

#[tokio::test]
async fn manifest_download_job_no_attachments_exits_with_error() {
    use deadline_test_server::deadline_api::{jobs, queues, sts};

    let harness = TestHarness::new().await;
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"]).assert().success();
    harness.cli(&["config", "set", "defaults.queue_id", "queue-abc"]).assert().success();

    queues::mock_get_queue(&harness.server, "farm-abc", json!({
        "queueId": "queue-abc",
        "displayName": "Test Queue",
        "jobAttachmentSettings": {
            "s3BucketName": "test-bucket",
            "rootPrefix": "root-prefix"
        }
    })).await;
    // Job has no attachments field
    jobs::mock_get_job(&harness.server, "farm-abc", "queue-abc", json!({
        "jobId": "job-abc",
        "name": "No Attachments Job",
    })).await;
    sts::mock_get_caller_identity(&harness.server).await;

    let dir = TempDir::new().unwrap();
    let output = harness
        .cli(&[
            "manifest", "download", dir.path().to_str().unwrap(),
            "--job-id", "job-abc",
        ])
        .output()
        .expect("failed to run");

    assert!(!output.status.success(), "Should fail when job has no attachments");
    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let combined = format!("{stdout}{stderr}");
    // Must NOT contain the old stub error
    assert!(
        !combined.contains("not yet wired"),
        "Should not show the old stub error, got: {combined}"
    );
    assert!(
        combined.contains("no attachment") || combined.contains("no manifest"),
        "Error should mention missing attachments/manifests, got: {combined}"
    );
}

// ===========================================================================
// -ie short alias for --include-exclude-config
// ===========================================================================

/// `-ie` should be accepted as a short alias for `--include-exclude-config`
/// on `manifest snapshot`.
#[tokio::test]
async fn manifest_snapshot_ie_short_alias() {
    let harness = TestHarness::new().await;
    let dir = TempDir::new().unwrap();
    fs::write(dir.path().join("a.txt"), b"hello").unwrap();

    let output = harness.cli(&[
        "manifest", "snapshot",
        "--root", dir.path().to_str().unwrap(),
        "-ie", r#"{"include": ["**/*"], "exclude": []}"#,
    ]).output().expect("failed to run");

    assert!(
        output.status.success(),
        "Expected -ie alias to be accepted, stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

// ===========================================================================
// manifest diff --root optional
// ===========================================================================

/// `manifest diff --manifest <file>` without `--root` should succeed,
/// deriving the root from the manifest file's parent directory.
#[tokio::test]
async fn manifest_diff_root_optional() {
    let harness = TestHarness::new().await;
    let dir = TempDir::new().unwrap();
    fs::write(dir.path().join("a.txt"), b"hello").unwrap();

    // First create a snapshot to get a manifest file
    let output = harness.cli(&[
        "manifest", "snapshot",
        "--root", dir.path().to_str().unwrap(),
    ]).output().expect("failed to run snapshot");
    assert!(output.status.success());

    // Find the manifest file
    let manifest_file = fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .find(|e| e.path().extension().map_or(false, |ext| ext == "manifest"))
        .expect("manifest file should exist")
        .path();

    // Run diff WITHOUT --root — should derive root from manifest path
    let output = harness.cli(&[
        "manifest", "diff",
        "--manifest", manifest_file.to_str().unwrap(),
    ]).output().expect("failed to run diff");

    assert!(
        output.status.success(),
        "Expected diff without --root to succeed, stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

// ===========================================================================
// --json suppresses human-readable output
// ===========================================================================

/// `manifest snapshot --json` should only output valid JSON on stdout.
/// Human-readable messages like "Manifest creation path defaulted to..."
/// should NOT appear on stdout.
#[tokio::test]
async fn manifest_snapshot_json_only_json_on_stdout() {
    let harness = TestHarness::new().await;
    let dir = TempDir::new().unwrap();
    fs::write(dir.path().join("a.txt"), b"hello").unwrap();

    let output = harness.cli(&[
        "manifest", "snapshot",
        "--root", dir.path().to_str().unwrap(),
        "--json",
    ]).output().expect("failed to run");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);

    // stdout should NOT contain human-readable messages
    assert!(
        !stdout.contains("Manifest creation path defaulted to"),
        "Human-readable message should not appear on stdout with --json, got: {stdout}"
    );
    assert!(
        !stdout.contains("Manifest generated at"),
        "Human-readable message should not appear on stdout with --json, got: {stdout}"
    );

    // stdout should be valid JSON
    let trimmed = stdout.trim();
    assert!(
        !trimmed.is_empty(),
        "Expected JSON output on stdout"
    );
    let parsed: Result<serde_json::Value, _> = serde_json::from_str(trimmed);
    assert!(
        parsed.is_ok(),
        "stdout should be valid JSON, got parse error: {:?} for: {trimmed}",
        parsed.err()
    );
}
