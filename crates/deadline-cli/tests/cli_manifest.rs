//! Level 2 tests for `deadline manifest` subcommands (§47, batch 9e-3).
//!
//! These test the CLI binary as a subprocess, exercising the full stack:
//! CLI arg parsing → manifest_ops → diff → hashing → file I/O.

use deadline_test_server::TestHarness;
use insta_cmd::assert_cmd_snapshot;
use std::fs;
use tempfile::TempDir;

// =====================================================================
// §47 case 1: manifest snapshot — happy path
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
// §47 case 2: manifest snapshot — --destination
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
// §47 case 4: manifest snapshot — --name
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
// §47 case 7: manifest snapshot — --include and --exclude
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
// §47 case 8: manifest snapshot — root doesn't exist
// =====================================================================

#[tokio::test]
async fn manifest_snapshot_nonexistent_root_exits_with_error() {
    let harness = TestHarness::new().await;
    assert_cmd_snapshot!(harness.cmd(&[
        "manifest", "snapshot", "--root", "/nonexistent/path/abc123"
    ]));
}

// =====================================================================
// §47 case 12: manifest diff — happy path with JSON output
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
// §47 case 5: manifest snapshot --diff
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
// §47 case 15: manifest diff — manifest doesn't exist
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
// §47 case 16: manifest diff — root doesn't exist
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
// §47 case 26: manifest upload — manifest doesn't exist
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
// §47 case 22: manifest download — dir doesn't exist
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
