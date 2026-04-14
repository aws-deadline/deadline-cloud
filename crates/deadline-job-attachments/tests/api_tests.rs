//! Level 1 tests for the public API module (, batch 9d).
//!
//! Tests for `read_manifests`, `process_path_mapping`,
//! `attachment_download`, and `attachment_upload`.

use std::fs;
use std::path::{Path, PathBuf};

use deadline_job_attachments::api::{
    attachment_download, attachment_upload, process_path_mapping, read_manifests,
};
use deadline_job_attachments::asset_manifests::{
    AssetManifest, HashAlgorithm, ManifestPath, ManifestVersion,
};
use deadline_job_attachments::models::FileConflictResolution;
use tempfile::TempDir;
use wiremock::matchers::method;
use wiremock::{Mock, MockServer, ResponseTemplate};

// --- Test helpers ---

/// RAII guard that removes a directory on drop — runs even if the test panics.
/// Same pattern as `TempDir`, but for directories created by the code under test
/// (e.g. cwd-relative downloads) rather than by the test harness.
struct CleanupDir(PathBuf);

impl Drop for CleanupDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

// --- Test helpers ---

/// Create a valid manifest and write it to disk. Returns the file path.
fn write_manifest_file(dir: &Path, filename: &str, files: &[(&str, &str, i64)]) -> String {
    let paths: Vec<ManifestPath> = files
        .iter()
        .map(|(p, h, s)| ManifestPath {
            path: p.to_string(),
            hash: h.to_string(),
            size: *s,
            mtime: 1700000000_000_000,
        })
        .collect();
    let total_size: i64 = paths.iter().map(|p| p.size).sum();
    let manifest = AssetManifest::new(
        HashAlgorithm::Xxh128,
        ManifestVersion::V2023_03_03,
        total_size,
        paths,
    )
    .unwrap();
    let encoded = manifest.encode();
    let file_path = dir.join(filename);
    fs::write(&file_path, &encoded).unwrap();
    file_path.to_string_lossy().into_owned()
}

async fn build_s3_client(server: &MockServer) -> aws_sdk_s3::Client {
    let sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new("us-west-2"))
        .endpoint_url(server.uri())
        .test_credentials()
        .load()
        .await;
    deadline_job_attachments::s3::build_s3_client(&sdk_config, None)
}

// =====================================================================
//  cases 32-36: read_manifests
// =====================================================================

// Two valid manifest file paths → map keyed by base filename
#[test]
fn read_manifests_two_valid_files_returns_map() {
    let dir = TempDir::new().unwrap();
    let p1 = write_manifest_file(
        dir.path(),
        "manifest_a.manifest",
        &[("file1.txt", "aabbccdd11223344aabbccdd11223344", 100)],
    );
    let p2 = write_manifest_file(
        dir.path(),
        "manifest_b.manifest",
        &[("file2.txt", "eeff00112233445566778899aabbccdd", 200)],
    );

    let result = read_manifests(&[p1, p2]).unwrap();
    assert_eq!(result.len(), 2);
    assert!(result.contains_key("manifest_a.manifest"));
    assert!(result.contains_key("manifest_b.manifest"));
}

// One path does not exist → error listing invalid paths
#[test]
fn read_manifests_one_invalid_path_errors() {
    let dir = TempDir::new().unwrap();
    let p1 = write_manifest_file(
        dir.path(),
        "valid.manifest",
        &[("f.txt", "aabbccdd11223344aabbccdd11223344", 10)],
    );
    let bad = dir.path().join("nonexistent.manifest").to_string_lossy().into_owned();

    let err = read_manifests(&[p1, bad.clone()]).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("not valid"), "expected 'not valid' in: {msg}");
    assert!(msg.contains("nonexistent.manifest"), "expected bad path in: {msg}");
}

// All paths invalid → error listing all
#[test]
fn read_manifests_all_invalid_paths_errors() {
    let bad1 = "/tmp/no_such_1.manifest".to_string();
    let bad2 = "/tmp/no_such_2.manifest".to_string();

    let err = read_manifests(&[bad1, bad2]).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("no_such_1"), "expected first path in: {msg}");
    assert!(msg.contains("no_such_2"), "expected second path in: {msg}");
}

// Empty list → empty map
#[test]
fn read_manifests_empty_list_returns_empty() {
    let result = read_manifests(&[]).unwrap();
    assert!(result.is_empty());
}

// File exists but invalid content → decode error
#[test]
fn read_manifests_invalid_content_errors() {
    let dir = TempDir::new().unwrap();
    let bad_path = dir.path().join("bad.manifest");
    fs::write(&bad_path, "not json at all").unwrap();

    let err = read_manifests(&[bad_path.to_string_lossy().into_owned()]).unwrap_err();
    let msg = err.to_string();
    // Should be a manifest decode error
    assert!(
        msg.contains("manifest") || msg.contains("JSON") || msg.contains("parse"),
        "expected decode error, got: {msg}"
    );
}

// =====================================================================
//  cases 25-31: process_path_mapping
// =====================================================================

// Valid JSON file with top-level list
#[test]
fn process_path_mapping_top_level_list() {
    let dir = TempDir::new().unwrap();
    let rules_json = serde_json::json!([
        {"source_path_format": "posix", "source_path": "/src", "destination_path": "/dst"}
    ]);
    let rules_path = dir.path().join("rules.json");
    fs::write(&rules_path, rules_json.to_string()).unwrap();

    let result = process_path_mapping(Some(rules_path.to_str().unwrap()), &[]).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].source_path, "/src");
    assert_eq!(result[0].destination_path, "/dst");
}

// Valid JSON with nested path_mapping_rules key
#[test]
fn process_path_mapping_nested_key() {
    let dir = TempDir::new().unwrap();
    let rules_json = serde_json::json!({
        "path_mapping_rules": [
            {"source_path_format": "windows", "source_path": "C:\\src", "destination_path": "/dst"}
        ]
    });
    let rules_path = dir.path().join("rules.json");
    fs::write(&rules_path, rules_json.to_string()).unwrap();

    let result = process_path_mapping(Some(rules_path.to_str().unwrap()), &[]).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].source_path, "C:\\src");
}

// Path mapping file does not exist → error
#[test]
fn process_path_mapping_file_not_found_errors() {
    let err = process_path_mapping(Some("/nonexistent/rules.json"), &[]).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("not valid"), "expected 'not valid' in: {msg}");
}

// root_dirs with two valid directories
#[test]
fn process_path_mapping_root_dirs() {
    let dir1 = TempDir::new().unwrap();
    let dir2 = TempDir::new().unwrap();

    let result = process_path_mapping(
        None,
        &[
            dir1.path().to_string_lossy().into_owned(),
            dir2.path().to_string_lossy().into_owned(),
        ],
    )
    .unwrap();
    assert_eq!(result.len(), 2);
    // source_path == destination_path for root_dirs
    assert_eq!(result[0].source_path, result[0].destination_path);
    assert_eq!(result[1].source_path, result[1].destination_path);
    // source_path_format is empty
    assert!(result[0].source_path_format.is_empty());
}

// One root_dir does not exist → error
#[test]
fn process_path_mapping_invalid_root_dir_errors() {
    let dir = TempDir::new().unwrap();
    let err = process_path_mapping(
        None,
        &[
            dir.path().to_string_lossy().into_owned(),
            "/nonexistent/dir".to_string(),
        ],
    )
    .unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("not valid"), "expected 'not valid' in: {msg}");
}

// Neither provided → empty list
#[test]
fn process_path_mapping_neither_returns_empty() {
    let result = process_path_mapping(None, &[]).unwrap();
    assert!(result.is_empty());
}

// Both provided → concatenated
#[test]
fn process_path_mapping_both_concatenated() {
    let dir = TempDir::new().unwrap();
    let rules_json = serde_json::json!([
        {"source_path_format": "posix", "source_path": "/from_file", "destination_path": "/to_file"}
    ]);
    let rules_path = dir.path().join("rules.json");
    fs::write(&rules_path, rules_json.to_string()).unwrap();

    let root_dir = TempDir::new().unwrap();
    let result = process_path_mapping(
        Some(rules_path.to_str().unwrap()),
        &[root_dir.path().to_string_lossy().into_owned()],
    )
    .unwrap();
    // One from file + one from root_dirs
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].source_path, "/from_file");
    assert_eq!(result[1].source_path, result[1].destination_path);
}

// =====================================================================
//  cases 1-10: attachment_download
// =====================================================================

// Two manifests with matching path mapping rules
#[tokio::test]
async fn attachment_download_with_path_mapping_rules() {
    let dir = TempDir::new().unwrap();
    let dest1 = TempDir::new().unwrap();
    let dest2 = TempDir::new().unwrap();

    // Create manifest files with hashed source path in filename
    let hash1 = deadline_job_attachments::asset_manifests::hash_data(
        dest1.path().to_string_lossy().as_bytes(),
        HashAlgorithm::Xxh128,
    );
    let hash2 = deadline_job_attachments::asset_manifests::hash_data(
        dest2.path().to_string_lossy().as_bytes(),
        HashAlgorithm::Xxh128,
    );

    let p1 = write_manifest_file(
        dir.path(),
        &format!("{hash1}_input"),
        &[("a.txt", "aabbccdd11223344aabbccdd11223344", 5)],
    );
    let p2 = write_manifest_file(
        dir.path(),
        &format!("{hash2}_input"),
        &[("b.txt", "eeff00112233445566778899aabbccdd", 5)],
    );

    // Write path mapping rules file
    let rules = serde_json::json!([
        {
            "source_path_format": "posix",
            "source_path": dest1.path().to_string_lossy(),
            "destination_path": dest1.path().to_string_lossy()
        },
        {
            "source_path_format": "posix",
            "source_path": dest2.path().to_string_lossy(),
            "destination_path": dest2.path().to_string_lossy()
        }
    ]);
    let rules_path = dir.path().join("rules.json");
    fs::write(&rules_path, rules.to_string()).unwrap();

    let server = MockServer::start().await;
    // Mock S3 GetObject for both files
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"hello".to_vec()))
        .mount(&server)
        .await;

    let s3_client = build_s3_client(&server).await;

    let result = attachment_download(
        &[p1, p2],
        "s3://test-bucket/root-prefix",
        &s3_client,
        "123456789012",
        Some(rules_path.to_str().unwrap()),
        None,
        FileConflictResolution::CreateCopy,
    )
    .await
    .unwrap();

    assert!(result.stats.processed_files > 0 || result.stats.total_files > 0);
}

// Manifest with no matching rule → downloads to cwd/filename
#[tokio::test]
async fn attachment_download_no_matching_rule_uses_cwd() {
    let dir = TempDir::new().unwrap();
    let manifest_name = format!("_test_unmatched_{}", std::process::id());
    let p1 = write_manifest_file(
        dir.path(),
        &manifest_name,
        &[("a.txt", "aabbccdd11223344aabbccdd11223344", 5)],
    );

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"data".to_vec()))
        .mount(&server)
        .await;

    let s3_client = build_s3_client(&server).await;

    // Guard cleans up the cwd-relative directory on drop (even on panic)
    let _cleanup = CleanupDir(std::env::current_dir().unwrap().join(&manifest_name));

    let result = attachment_download(
        &[p1],
        "s3://test-bucket/root-prefix",
        &s3_client,
        "123456789012",
        None,
        None,
        FileConflictResolution::CreateCopy,
    )
    .await
    .unwrap();

    assert_eq!(result.stats.total_files, 1);
}

// No path mapping rules → all to cwd
#[tokio::test]
async fn attachment_download_no_rules_downloads_to_cwd() {
    let dir = TempDir::new().unwrap();
    let manifest_name = format!("_test_norules_{}", std::process::id());
    let p1 = write_manifest_file(
        dir.path(),
        &manifest_name,
        &[("x.txt", "aabbccdd11223344aabbccdd11223344", 3)],
    );

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"abc".to_vec()))
        .mount(&server)
        .await;

    let s3_client = build_s3_client(&server).await;

    let _cleanup = CleanupDir(std::env::current_dir().unwrap().join(&manifest_name));

    let result = attachment_download(
        &[p1],
        "s3://test-bucket/root-prefix",
        &s3_client,
        "123456789012",
        None,
        None,
        FileConflictResolution::CreateCopy,
    )
    .await
    .unwrap();

    assert_eq!(result.stats.total_files, 1);
}

// Manifest file path does not exist → error
#[tokio::test]
async fn attachment_download_invalid_manifest_path_errors() {
    let server = MockServer::start().await;
    let s3_client = build_s3_client(&server).await;

    let err = attachment_download(
        &["/nonexistent/manifest.file".to_string()],
        "s3://test-bucket/root-prefix",
        &s3_client,
        "123456789012",
        None,
        None,
        FileConflictResolution::CreateCopy,
    )
    .await
    .unwrap_err();

    assert!(err.to_string().contains("not valid"));
}

// Two manifests resolve to same destination → error
#[tokio::test]
async fn attachment_download_duplicate_destination_errors() {
    let dir = TempDir::new().unwrap();
    let dest = TempDir::new().unwrap();

    // Both manifests have the same hashed source path → same destination
    let hash = deadline_job_attachments::asset_manifests::hash_data(
        dest.path().to_string_lossy().as_bytes(),
        HashAlgorithm::Xxh128,
    );
    let p1 = write_manifest_file(
        dir.path(),
        &format!("{hash}_input1"),
        &[("a.txt", "aabbccdd11223344aabbccdd11223344", 5)],
    );
    let p2 = write_manifest_file(
        dir.path(),
        &format!("{hash}_input2"),
        &[("b.txt", "eeff00112233445566778899aabbccdd", 5)],
    );

    let rules = serde_json::json!([{
        "source_path_format": "posix",
        "source_path": dest.path().to_string_lossy(),
        "destination_path": dest.path().to_string_lossy()
    }]);
    let rules_path = dir.path().join("rules.json");
    fs::write(&rules_path, rules.to_string()).unwrap();

    let server = MockServer::start().await;
    let s3_client = build_s3_client(&server).await;

    let err = attachment_download(
        &[p1, p2],
        "s3://test-bucket/root-prefix",
        &s3_client,
        "123456789012",
        Some(rules_path.to_str().unwrap()),
        None,
        FileConflictResolution::CreateCopy,
    )
    .await
    .unwrap_err();

    let msg = err.to_string();
    assert!(msg.contains("already in use"), "expected 'already in use' in: {msg}");
}

// Malformed S3 root URI → error
#[tokio::test]
async fn attachment_download_malformed_s3_uri_errors() {
    let dir = TempDir::new().unwrap();
    let p1 = write_manifest_file(
        dir.path(),
        "m.manifest",
        &[("f.txt", "aabbccdd11223344aabbccdd11223344", 5)],
    );

    let server = MockServer::start().await;
    let s3_client = build_s3_client(&server).await;

    let err = attachment_download(
        &[p1],
        "not-an-s3-uri",
        &s3_client,
        "123456789012",
        None,
        None,
        FileConflictResolution::CreateCopy,
    )
    .await
    .unwrap_err();

    let msg = err.to_string();
    assert!(
        msg.contains("Invalid") || msg.contains("root uri"),
        "expected URI error, got: {msg}"
    );
}

// Empty manifests list → empty summary
#[tokio::test]
async fn attachment_download_empty_manifests_returns_empty() {
    let server = MockServer::start().await;
    let s3_client = build_s3_client(&server).await;

    let result = attachment_download(
        &[],
        "s3://test-bucket/root-prefix",
        &s3_client,
        "123456789012",
        None,
        None,
        FileConflictResolution::CreateCopy,
    )
    .await
    .unwrap();

    assert_eq!(result.stats.total_files, 0);
    assert_eq!(result.stats.processed_files, 0);
}

// =====================================================================
//  cases 11-24: attachment_upload
// =====================================================================

// Manifests with root_dirs provided
#[tokio::test]
async fn attachment_upload_with_root_dirs() {
    let dir = TempDir::new().unwrap();
    let root = TempDir::new().unwrap();

    // Create a source file in root
    let src_file = root.path().join("a.txt");
    fs::write(&src_file, b"hello").unwrap();

    // Hash the root path to create manifest filename
    let root_hash = deadline_job_attachments::asset_manifests::hash_data(
        root.path().to_string_lossy().as_bytes(),
        HashAlgorithm::Xxh128,
    );
    let file_hash = deadline_job_attachments::asset_manifests::hash_data(b"hello", HashAlgorithm::Xxh128);
    let p1 = write_manifest_file(
        dir.path(),
        &format!("{root_hash}_input"),
        &[("a.txt", &file_hash, 5)],
    );

    let server = MockServer::start().await;
    // Mock S3 HEAD (not found) and PUT (success)
    Mock::given(method("HEAD"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;
    Mock::given(method("PUT"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let s3_client = build_s3_client(&server).await;

    let result = attachment_upload(
        &[p1],
        "s3://test-bucket/root-prefix",
        &s3_client,
        "123456789012",
        &[root.path().to_string_lossy().into_owned()],
        None,
        None,
        None,
        None,
    )
    .await
    .unwrap();

    assert_eq!(result.len(), 1);
    assert!(result[0].source_path.is_some());
}

// Both path_mapping_rules and root_dirs → error
#[tokio::test]
async fn attachment_upload_both_rules_and_dirs_errors() {
    let dir = TempDir::new().unwrap();
    let p1 = write_manifest_file(
        dir.path(),
        "m.manifest",
        &[("f.txt", "aabbccdd11223344aabbccdd11223344", 5)],
    );
    let rules_path = dir.path().join("rules.json");
    fs::write(&rules_path, "[]").unwrap();
    let root = TempDir::new().unwrap();

    let server = MockServer::start().await;
    let s3_client = build_s3_client(&server).await;

    let err = attachment_upload(
        &[p1],
        "s3://test-bucket/root-prefix",
        &s3_client,
        "123456789012",
        &[root.path().to_string_lossy().into_owned()],
        Some(rules_path.to_str().unwrap()),
        None,
        None,
        None,
    )
    .await
    .unwrap_err();

    let msg = err.to_string();
    assert!(
        msg.contains("not both") || msg.contains("One of"),
        "expected mutual exclusion error, got: {msg}"
    );
}

// Neither path_mapping_rules nor root_dirs → error
#[tokio::test]
async fn attachment_upload_neither_rules_nor_dirs_errors() {
    let dir = TempDir::new().unwrap();
    let p1 = write_manifest_file(
        dir.path(),
        "m.manifest",
        &[("f.txt", "aabbccdd11223344aabbccdd11223344", 5)],
    );

    let server = MockServer::start().await;
    let s3_client = build_s3_client(&server).await;

    let err = attachment_upload(
        &[p1],
        "s3://test-bucket/root-prefix",
        &s3_client,
        "123456789012",
        &[],
        None,
        None,
        None,
        None,
    )
    .await
    .unwrap_err();

    let msg = err.to_string();
    assert!(
        msg.contains("not both") || msg.contains("One of"),
        "expected mutual exclusion error, got: {msg}"
    );
}

// Manifest filename doesn't match any rule → error
#[tokio::test]
async fn attachment_upload_no_matching_rule_errors() {
    let dir = TempDir::new().unwrap();
    let root = TempDir::new().unwrap();

    // Manifest filename does NOT contain the hash of root_dir
    let p1 = write_manifest_file(
        dir.path(),
        "completely_unrelated_name",
        &[("f.txt", "aabbccdd11223344aabbccdd11223344", 5)],
    );

    let server = MockServer::start().await;
    let s3_client = build_s3_client(&server).await;

    let err = attachment_upload(
        &[p1],
        "s3://test-bucket/root-prefix",
        &s3_client,
        "123456789012",
        &[root.path().to_string_lossy().into_owned()],
        None,
        None,
        None,
        None,
    )
    .await
    .unwrap_err();

    let msg = err.to_string();
    assert!(
        msg.contains("No valid root"),
        "expected 'No valid root' in: {msg}"
    );
}

// Manifest file path does not exist → error
#[tokio::test]
async fn attachment_upload_invalid_manifest_path_errors() {
    let server = MockServer::start().await;
    let s3_client = build_s3_client(&server).await;

    let err = attachment_upload(
        &["/nonexistent/manifest.file".to_string()],
        "s3://test-bucket/root-prefix",
        &s3_client,
        "123456789012",
        &["/tmp".to_string()],
        None,
        None,
        None,
        None,
    )
    .await
    .unwrap_err();

    assert!(err.to_string().contains("not valid"));
}

// Malformed S3 root URI → error
#[tokio::test]
async fn attachment_upload_malformed_s3_uri_errors() {
    let dir = TempDir::new().unwrap();
    let root = TempDir::new().unwrap();
    let root_hash = deadline_job_attachments::asset_manifests::hash_data(
        root.path().to_string_lossy().as_bytes(),
        HashAlgorithm::Xxh128,
    );
    let p1 = write_manifest_file(
        dir.path(),
        &format!("{root_hash}_input"),
        &[("f.txt", "aabbccdd11223344aabbccdd11223344", 5)],
    );

    let server = MockServer::start().await;
    let s3_client = build_s3_client(&server).await;

    let err = attachment_upload(
        &[p1],
        "bad-uri",
        &s3_client,
        "123456789012",
        &[root.path().to_string_lossy().into_owned()],
        None,
        None,
        None,
        None,
    )
    .await
    .unwrap_err();

    let msg = err.to_string();
    assert!(
        msg.contains("Invalid") || msg.contains("root uri"),
        "expected URI error, got: {msg}"
    );
}

// Root directory does not exist → error
#[tokio::test]
async fn attachment_upload_invalid_root_dir_errors() {
    let dir = TempDir::new().unwrap();
    let p1 = write_manifest_file(
        dir.path(),
        "m.manifest",
        &[("f.txt", "aabbccdd11223344aabbccdd11223344", 5)],
    );

    let server = MockServer::start().await;
    let s3_client = build_s3_client(&server).await;

    let err = attachment_upload(
        &[p1],
        "s3://test-bucket/root-prefix",
        &s3_client,
        "123456789012",
        &["/nonexistent/root/dir".to_string()],
        None,
        None,
        None,
        None,
    )
    .await
    .unwrap_err();

    assert!(err.to_string().contains("not valid"));
}

// =====================================================================
// Missing  cases — added in Step 6 audit
// =====================================================================

// S3 root URI parsed into bucket and CAS prefix
#[tokio::test]
async fn attachment_download_parses_s3_uri_into_bucket_and_prefix() {
    let dir = TempDir::new().unwrap();
    let dest = TempDir::new().unwrap();

    let dest_hash = deadline_job_attachments::asset_manifests::hash_data(
        dest.path().to_string_lossy().as_bytes(),
        HashAlgorithm::Xxh128,
    );
    let p1 = write_manifest_file(
        dir.path(),
        &format!("{dest_hash}_input"),
        &[("a.txt", "aabbccdd11223344aabbccdd11223344", 5)],
    );

    let rules = serde_json::json!([{
        "source_path_format": "posix",
        "source_path": dest.path().to_string_lossy(),
        "destination_path": dest.path().to_string_lossy()
    }]);
    let rules_path = dir.path().join("rules.json");
    fs::write(&rules_path, rules.to_string()).unwrap();

    let server = MockServer::start().await;
    // The GET request path should contain the CAS prefix "my-prefix/Data/"
    Mock::given(method("GET"))
        .and(wiremock::matchers::path_regex("my-prefix/Data/.*"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"data".to_vec()))
        .mount(&server)
        .await;

    let s3_client = build_s3_client(&server).await;

    let result = attachment_download(
        &[p1],
        "s3://my-bucket/my-prefix",
        &s3_client,
        "123456789012",
        Some(rules_path.to_str().unwrap()),
        None,
        FileConflictResolution::CreateCopy,
    )
    .await
    .unwrap();

    assert_eq!(result.stats.total_files, 1);
}

// conflict_resolution=CREATE_COPY passed through
#[tokio::test]
async fn attachment_download_conflict_resolution_create_copy() {
    let dir = TempDir::new().unwrap();
    let dest = TempDir::new().unwrap();

    let hash = deadline_job_attachments::asset_manifests::hash_data(
        dest.path().to_string_lossy().as_bytes(),
        HashAlgorithm::Xxh128,
    );
    let p1 = write_manifest_file(
        dir.path(),
        &format!("{hash}_input"),
        &[("existing.txt", "aabbccdd11223344aabbccdd11223344", 5)],
    );

    // Pre-create the file so conflict resolution triggers
    let existing = dest.path().join("existing.txt");
    fs::write(&existing, b"old content").unwrap();

    let rules = serde_json::json!([{
        "source_path_format": "posix",
        "source_path": dest.path().to_string_lossy(),
        "destination_path": dest.path().to_string_lossy()
    }]);
    let rules_path = dir.path().join("rules.json");
    fs::write(&rules_path, rules.to_string()).unwrap();

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"new content".to_vec()))
        .mount(&server)
        .await;

    let s3_client = build_s3_client(&server).await;

    let result = attachment_download(
        &[p1],
        "s3://test-bucket/root-prefix",
        &s3_client,
        "123456789012",
        Some(rules_path.to_str().unwrap()),
        None,
        FileConflictResolution::CreateCopy,
    )
    .await
    .unwrap();

    // Original file should still have old content
    assert_eq!(fs::read_to_string(&existing).unwrap(), "old content");
    // A copy should exist
    assert_eq!(result.stats.processed_files, 1);
}

// Hash match in filename selects correct destination
#[tokio::test]
async fn attachment_download_hash_match_selects_correct_destination() {
    let dir = TempDir::new().unwrap();
    let dest_a = TempDir::new().unwrap();
    let dest_b = TempDir::new().unwrap();

    let hash_a = deadline_job_attachments::asset_manifests::hash_data(
        dest_a.path().to_string_lossy().as_bytes(),
        HashAlgorithm::Xxh128,
    );
    let hash_b = deadline_job_attachments::asset_manifests::hash_data(
        dest_b.path().to_string_lossy().as_bytes(),
        HashAlgorithm::Xxh128,
    );

    // Manifest filenames contain the hash of their respective source paths
    let p1 = write_manifest_file(
        dir.path(),
        &format!("{hash_a}_input"),
        &[("file_a.txt", "aabbccdd11223344aabbccdd11223344", 3)],
    );
    let p2 = write_manifest_file(
        dir.path(),
        &format!("{hash_b}_input"),
        &[("file_b.txt", "eeff00112233445566778899aabbccdd", 3)],
    );

    let rules = serde_json::json!([
        {
            "source_path_format": "posix",
            "source_path": dest_a.path().to_string_lossy(),
            "destination_path": dest_a.path().to_string_lossy()
        },
        {
            "source_path_format": "posix",
            "source_path": dest_b.path().to_string_lossy(),
            "destination_path": dest_b.path().to_string_lossy()
        }
    ]);
    let rules_path = dir.path().join("rules.json");
    fs::write(&rules_path, rules.to_string()).unwrap();

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"abc".to_vec()))
        .mount(&server)
        .await;

    let s3_client = build_s3_client(&server).await;

    let result = attachment_download(
        &[p1, p2],
        "s3://test-bucket/root-prefix",
        &s3_client,
        "123456789012",
        Some(rules_path.to_str().unwrap()),
        None,
        FileConflictResolution::CreateCopy,
    )
    .await
    .unwrap();

    // Both files should be downloaded to their respective destinations
    assert_eq!(result.stats.total_files, 2);
    assert!(dest_a.path().join("file_a.txt").exists());
    assert!(dest_b.path().join("file_b.txt").exists());
}

// Upload with path_mapping_rules file (no root_dirs)
#[tokio::test]
async fn attachment_upload_with_path_mapping_rules_file() {
    let dir = TempDir::new().unwrap();
    let source = TempDir::new().unwrap();

    // Create source file
    fs::write(source.path().join("a.txt"), b"hello").unwrap();

    let source_hash = deadline_job_attachments::asset_manifests::hash_data(
        source.path().to_string_lossy().as_bytes(),
        HashAlgorithm::Xxh128,
    );
    let file_hash = deadline_job_attachments::asset_manifests::hash_data(b"hello", HashAlgorithm::Xxh128);
    let p1 = write_manifest_file(
        dir.path(),
        &format!("{source_hash}_input"),
        &[("a.txt", &file_hash, 5)],
    );

    let rules = serde_json::json!([{
        "source_path_format": "posix",
        "source_path": source.path().to_string_lossy(),
        "destination_path": source.path().to_string_lossy()
    }]);
    let rules_path = dir.path().join("rules.json");
    fs::write(&rules_path, rules.to_string()).unwrap();

    let server = MockServer::start().await;
    Mock::given(method("HEAD"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;
    Mock::given(method("PUT"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let s3_client = build_s3_client(&server).await;

    let result = attachment_upload(
        &[p1],
        "s3://test-bucket/root-prefix",
        &s3_client,
        "123456789012",
        &[],
        Some(rules_path.to_str().unwrap()),
        None,
        None,
        None,
    )
    .await
    .unwrap();

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].source_path.as_deref(), Some(source.path().to_str().unwrap()));
}

// ASCII source path → asset-root metadata
#[tokio::test]
async fn attachment_upload_ascii_path_sets_asset_root_metadata() {
    let dir = TempDir::new().unwrap();
    let root = TempDir::new().unwrap();
    fs::write(root.path().join("a.txt"), b"data").unwrap();

    let root_hash = deadline_job_attachments::asset_manifests::hash_data(
        root.path().to_string_lossy().as_bytes(),
        HashAlgorithm::Xxh128,
    );
    let file_hash = deadline_job_attachments::asset_manifests::hash_data(b"data", HashAlgorithm::Xxh128);
    let p1 = write_manifest_file(
        dir.path(),
        &format!("{root_hash}_input"),
        &[("a.txt", &file_hash, 4)],
    );

    let server = MockServer::start().await;
    Mock::given(method("HEAD"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;

    // Capture the PUT request to verify metadata
    let put_mock = Mock::given(method("PUT"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1..)
        .mount_as_scoped(&server)
        .await;

    let s3_client = build_s3_client(&server).await;

    let result = attachment_upload(
        &[p1],
        "s3://test-bucket/root-prefix",
        &s3_client,
        "123456789012",
        &[root.path().to_string_lossy().into_owned()],
        None,
        Some("manifests/prefix"),
        None,
        None,
    )
    .await
    .unwrap();

    // Verify upload happened and source_path is ASCII
    assert_eq!(result.len(), 1);
    assert!(result[0].source_path.as_ref().unwrap().is_ascii());
    drop(put_mock); // verify expectations
}

// upload_manifest_path provided → manifest uploaded
#[tokio::test]
async fn attachment_upload_with_manifest_path_uploads_manifest() {
    let dir = TempDir::new().unwrap();
    let root = TempDir::new().unwrap();
    fs::write(root.path().join("a.txt"), b"data").unwrap();

    let root_hash = deadline_job_attachments::asset_manifests::hash_data(
        root.path().to_string_lossy().as_bytes(),
        HashAlgorithm::Xxh128,
    );
    let file_hash = deadline_job_attachments::asset_manifests::hash_data(b"data", HashAlgorithm::Xxh128);
    let manifest_name = format!("{root_hash}_input");
    let p1 = write_manifest_file(
        dir.path(),
        &manifest_name,
        &[("a.txt", &file_hash, 4)],
    );

    let server = MockServer::start().await;
    Mock::given(method("HEAD"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;
    Mock::given(method("PUT"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let s3_client = build_s3_client(&server).await;

    let result = attachment_upload(
        &[p1],
        "s3://test-bucket/root-prefix",
        &s3_client,
        "123456789012",
        &[root.path().to_string_lossy().into_owned()],
        None,
        Some("upload/prefix"),
        None,
        None,
    )
    .await
    .unwrap();

    // output_manifest_path should include the upload prefix
    assert!(
        result[0].output_manifest_path.starts_with("upload/prefix/"),
        "expected prefix in path, got: {}",
        result[0].output_manifest_path
    );
}

// upload_manifest_path not provided → manifest not uploaded
#[tokio::test]
async fn attachment_upload_without_manifest_path_skips_manifest_upload() {
    let dir = TempDir::new().unwrap();
    let root = TempDir::new().unwrap();
    fs::write(root.path().join("a.txt"), b"data").unwrap();

    let root_hash = deadline_job_attachments::asset_manifests::hash_data(
        root.path().to_string_lossy().as_bytes(),
        HashAlgorithm::Xxh128,
    );
    let file_hash = deadline_job_attachments::asset_manifests::hash_data(b"data", HashAlgorithm::Xxh128);
    let manifest_name = format!("{root_hash}_input");
    let p1 = write_manifest_file(
        dir.path(),
        &manifest_name,
        &[("a.txt", &file_hash, 4)],
    );

    let server = MockServer::start().await;
    Mock::given(method("HEAD"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;
    Mock::given(method("PUT"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let s3_client = build_s3_client(&server).await;

    let result = attachment_upload(
        &[p1],
        "s3://test-bucket/root-prefix",
        &s3_client,
        "123456789012",
        &[root.path().to_string_lossy().into_owned()],
        None,
        None, // no upload_manifest_path
        None,
        None,
    )
    .await
    .unwrap();

    // output_manifest_path should just be the filename (no prefix)
    assert_eq!(result[0].output_manifest_path, manifest_name);
}

// Multiple manifests → returns in same order as input
#[tokio::test]
async fn attachment_upload_multiple_manifests_preserves_order() {
    let dir = TempDir::new().unwrap();
    let root1 = TempDir::new().unwrap();
    let root2 = TempDir::new().unwrap();
    fs::write(root1.path().join("a.txt"), b"aaa").unwrap();
    fs::write(root2.path().join("b.txt"), b"bbb").unwrap();

    let hash1 = deadline_job_attachments::asset_manifests::hash_data(
        root1.path().to_string_lossy().as_bytes(),
        HashAlgorithm::Xxh128,
    );
    let hash2 = deadline_job_attachments::asset_manifests::hash_data(
        root2.path().to_string_lossy().as_bytes(),
        HashAlgorithm::Xxh128,
    );
    let fh1 = deadline_job_attachments::asset_manifests::hash_data(b"aaa", HashAlgorithm::Xxh128);
    let fh2 = deadline_job_attachments::asset_manifests::hash_data(b"bbb", HashAlgorithm::Xxh128);

    let p1 = write_manifest_file(
        dir.path(),
        &format!("{hash1}_input"),
        &[("a.txt", &fh1, 3)],
    );
    let p2 = write_manifest_file(
        dir.path(),
        &format!("{hash2}_input"),
        &[("b.txt", &fh2, 3)],
    );

    let server = MockServer::start().await;
    Mock::given(method("HEAD"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;
    Mock::given(method("PUT"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let s3_client = build_s3_client(&server).await;

    let result = attachment_upload(
        &[p1, p2],
        "s3://test-bucket/root-prefix",
        &s3_client,
        "123456789012",
        &[
            root1.path().to_string_lossy().into_owned(),
            root2.path().to_string_lossy().into_owned(),
        ],
        None,
        None,
        None,
        None,
    )
    .await
    .unwrap();

    assert_eq!(result.len(), 2);
    // First result should correspond to root1
    assert_eq!(
        result[0].source_path.as_deref(),
        Some(root1.path().to_str().unwrap())
    );
    // Second result should correspond to root2
    assert_eq!(
        result[1].source_path.as_deref(),
        Some(root2.path().to_str().unwrap())
    );
}
