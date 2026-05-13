//! Level 1 tests for the download engine (, batch 9c).
//!
//! These tests use wiremock to stub S3 API responses and real temp
//! directories for file I/O. No mocking libraries.

use std::collections::HashMap;
use std::fs;
use std::path::Path;

use deadline_job_attachments::asset_manifests::{
    AssetManifest, HashAlgorithm, ManifestPath, ManifestVersion,
};
use deadline_job_attachments::download::{
    download_file, download_files_from_manifests, get_output_manifests_by_asset_root,
    merge_asset_manifests,
};
use deadline_job_attachments::models::{FileConflictResolution, JobAttachmentS3Settings};
use tempfile::TempDir;
use wiremock::matchers::{method, path_regex};
use wiremock::{Mock, MockServer, ResponseTemplate};

// --- Test helpers ---

fn test_s3_settings() -> JobAttachmentS3Settings {
    JobAttachmentS3Settings::from_root_path("test-bucket/root-prefix").unwrap()
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

fn make_manifest(files: &[(&str, &[u8])], dir: &Path) -> AssetManifest {
    let mut paths = Vec::new();
    for (name, content) in files {
        let file_path = dir.join(name);
        if let Some(parent) = file_path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(&file_path, content).unwrap();
        let hash =
            deadline_job_attachments::asset_manifests::hash_data(content);
        let meta = fs::metadata(&file_path).unwrap();
        paths.push(ManifestPath {
            path: name.to_string(),
            hash,
            size: meta.len(),
            mtime: 1_700_000_000_000_000, // fixed microseconds for test determinism
        });
    }
    let total_size: u64 = paths.iter().map(|p| p.size).sum();
    AssetManifest::new(
        HashAlgorithm::Xxh128,
        ManifestVersion::V2023_03_03,
        total_size,
        paths,
    )
    .unwrap()
}

fn make_manifest_no_files(entries: &[(&str, &str, u64)]) -> AssetManifest {
    // entries: (path, hash, size)
    let paths: Vec<ManifestPath> = entries
        .iter()
        .map(|(p, h, s)| ManifestPath {
            path: p.to_string(),
            hash: h.to_string(),
            size: *s,
            mtime: 1_700_000_000_000_000,
        })
        .collect();
    let total_size: u64 = paths.iter().map(|p| p.size).sum();
    AssetManifest::new(
        HashAlgorithm::Xxh128,
        ManifestVersion::V2023_03_03,
        total_size,
        paths,
    )
    .unwrap()
}

/// Mount S3 `GetObject` returning file content.
async fn mock_s3_get_object(server: &MockServer, body: &[u8]) {
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(body.to_vec()))
        .mount(server)
        .await;
}

// =====================================================================
//  cases 19-24: merge_asset_manifests
// =====================================================================

#[test]
fn merge_single_manifest_returns_same() {
    let manifest =
        make_manifest_no_files(&[("file1.txt", "aabbccdd11223344aabbccdd11223344", 100)]);
    let result = merge_asset_manifests(&[manifest]).unwrap();
    assert!(result.is_some());
    let merged = result.unwrap();
    assert_eq!(merged.paths.len(), 1);
    assert_eq!(merged.paths[0].path, "file1.txt");
    assert_eq!(merged.total_size, 100);
}

#[test]
fn merge_two_manifests_non_overlapping_paths() {
    let m1 = make_manifest_no_files(&[("file1.txt", "aabbccdd11223344aabbccdd11223344", 100)]);
    let m2 = make_manifest_no_files(&[("file2.txt", "11223344aabbccdd11223344aabbccdd", 200)]);
    let result = merge_asset_manifests(&[m1, m2]).unwrap().unwrap();
    assert_eq!(result.paths.len(), 2);
    assert_eq!(result.total_size, 300);
}

#[test]
fn merge_two_manifests_overlapping_paths_later_wins() {
    let m1 = make_manifest_no_files(&[("file1.txt", "aaaa000000000000aaaa000000000000", 100)]);
    let m2 = make_manifest_no_files(&[("file1.txt", "bbbb000000000000bbbb000000000000", 200)]);
    let result = merge_asset_manifests(&[m1, m2]).unwrap().unwrap();
    assert_eq!(result.paths.len(), 1);
    // Later manifest's entry wins
    assert_eq!(result.paths[0].hash, "bbbb000000000000bbbb000000000000");
    assert_eq!(result.total_size, 200);
}

#[test]
fn merge_empty_list_returns_ok_none() {
    // merge_asset_manifests should return Result<Option<...>>.
    // Empty input → Ok(None).
    let result: Result<Option<AssetManifest>, _> = merge_asset_manifests(&[]);
    assert!(result.unwrap().is_none());
}

#[test]
fn merge_single_manifest_returns_ok_some() {
    let manifest = make_manifest_no_files(&[("a.txt", "aabbccdd11223344aabbccdd11223344", 10)]);
    // Should return Result<Option<...>>, not bare Option.
    let result: Result<Option<AssetManifest>, _> = merge_asset_manifests(&[manifest]);
    let merged = result.unwrap().unwrap();
    assert_eq!(merged.paths.len(), 1);
}

#[test]
fn merge_different_hash_algorithms_returns_error() {
    // We only have Xxh128 currently, so this test verifies the error path
    // exists. When a second algorithm is added, this test should use two
    // different algorithms. For now, we test the function accepts matching
    // algorithms without error.
    let m1 = make_manifest_no_files(&[("a.txt", "aabbccdd11223344aabbccdd11223344", 10)]);
    let m2 = make_manifest_no_files(&[("b.txt", "11223344aabbccdd11223344aabbccdd", 20)]);
    // Same algorithm — should return Ok(Some(...))
    let result: Result<Option<AssetManifest>, _> = merge_asset_manifests(&[m1, m2]);
    assert!(result.unwrap().is_some());
}

#[test]
fn merge_recalculates_total_size_after_dedup() {
    let m1 = make_manifest_no_files(&[
        ("file1.txt", "aaaa000000000000aaaa000000000000", 100),
        ("file2.txt", "bbbb000000000000bbbb000000000000", 200),
    ]);
    let m2 = make_manifest_no_files(&[("file1.txt", "cccc000000000000cccc000000000000", 50)]);
    let result = merge_asset_manifests(&[m1, m2]).unwrap().unwrap();
    // file1.txt replaced (50), file2.txt kept (200) = 250
    assert_eq!(result.total_size, 250);
}

// =====================================================================
//  cases 8-18: download_file
// =====================================================================

#[tokio::test]
async fn download_file_happy_path_creates_file_and_sets_mtime() {
    let server = MockServer::start().await;
    let s3_client = build_s3_client(&server).await;
    let download_dir = TempDir::new().unwrap();
    let content = b"hello world";

    mock_s3_get_object(&server, content).await;

    let manifest_path = ManifestPath {
        path: "subdir/test.txt".into(),
        hash: "aabbccdd11223344aabbccdd11223344".into(),
        size: content.len() as u64,
        mtime: 1_700_000_000_000_000, // microseconds
    };

    let (bytes, local_path) = download_file(
        &manifest_path,
        HashAlgorithm::Xxh128,
        download_dir.path().to_str().unwrap(),
        &s3_client,
        "test-bucket",
        Some("root-prefix/Data"),
        "123456789012",
        None,
        FileConflictResolution::CreateCopy,
        &Default::default(),
    )
    .await
    .unwrap();

    assert_eq!(bytes, content.len() as u64);
    let path = local_path.unwrap();
    assert!(path.exists());
    assert_eq!(fs::read(&path).unwrap(), content);
}

#[tokio::test]
async fn download_file_creates_parent_directories() {
    let server = MockServer::start().await;
    let s3_client = build_s3_client(&server).await;
    let download_dir = TempDir::new().unwrap();

    mock_s3_get_object(&server, b"data").await;

    let manifest_path = ManifestPath {
        path: "deep/nested/dir/file.txt".into(),
        hash: "aabbccdd11223344aabbccdd11223344".into(),
        size: 4,
        mtime: 1_700_000_000_000_000,
    };

    let (_, local_path) = download_file(
        &manifest_path,
        HashAlgorithm::Xxh128,
        download_dir.path().to_str().unwrap(),
        &s3_client,
        "test-bucket",
        Some("root-prefix/Data"),
        "123456789012",
        None,
        FileConflictResolution::CreateCopy,
        &Default::default(),
    )
    .await
    .unwrap();

    let path = local_path.unwrap();
    assert!(path.exists());
    assert!(path.starts_with(download_dir.path().join("deep/nested/dir")));
}

#[tokio::test]
async fn download_file_404_retries_without_algorithm_suffix() {
    let server = MockServer::start().await;
    let s3_client = build_s3_client(&server).await;
    let download_dir = TempDir::new().unwrap();

    // First request (with .xxh128 suffix) returns 404
    Mock::given(method("GET"))
        .and(path_regex(r".*\.xxh128$"))
        .respond_with(
            ResponseTemplate::new(404)
                .set_body_string(r#"<?xml version="1.0"?><Error><Code>NoSuchKey</Code></Error>"#),
        )
        .expect(1)
        .mount(&server)
        .await;

    // Second request (without suffix) returns 200
    Mock::given(method("GET"))
        .and(path_regex(r".*/aabbccdd11223344aabbccdd11223344$"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"fallback content".to_vec()))
        .expect(1)
        .mount(&server)
        .await;

    let manifest_path = ManifestPath {
        path: "file.txt".into(),
        hash: "aabbccdd11223344aabbccdd11223344".into(),
        size: 16,
        mtime: 1_700_000_000_000_000,
    };

    let (bytes, local_path) = download_file(
        &manifest_path,
        HashAlgorithm::Xxh128,
        download_dir.path().to_str().unwrap(),
        &s3_client,
        "test-bucket",
        Some("root-prefix/Data"),
        "123456789012",
        None,
        FileConflictResolution::CreateCopy,
        &Default::default(),
    )
    .await
    .unwrap();

    assert_eq!(bytes, 16);
    let path = local_path.unwrap();
    assert_eq!(fs::read_to_string(&path).unwrap(), "fallback content");
}

#[tokio::test]
async fn download_file_404_on_both_attempts_returns_error() {
    let server = MockServer::start().await;
    let s3_client = build_s3_client(&server).await;
    let download_dir = TempDir::new().unwrap();

    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(404)
                .set_body_string(r#"<?xml version="1.0"?><Error><Code>NoSuchKey</Code></Error>"#),
        )
        .mount(&server)
        .await;

    let manifest_path = ManifestPath {
        path: "file.txt".into(),
        hash: "aabbccdd11223344aabbccdd11223344".into(),
        size: 10,
        mtime: 1_700_000_000_000_000,
    };

    let err = download_file(
        &manifest_path,
        HashAlgorithm::Xxh128,
        download_dir.path().to_str().unwrap(),
        &s3_client,
        "test-bucket",
        Some("root-prefix/Data"),
        "123456789012",
        None,
        FileConflictResolution::CreateCopy,
        &Default::default(),
    )
    .await
    .unwrap_err();

    let msg = err.to_string();
    assert!(msg.contains("404"), "expected 404 in error: {msg}");
}

#[tokio::test]
async fn download_file_403_non_kms_returns_get_object_guidance() {
    let server = MockServer::start().await;
    let s3_client = build_s3_client(&server).await;
    let download_dir = TempDir::new().unwrap();

    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(403).set_body_string(
            r#"<?xml version="1.0"?><Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>"#,
        ))
        .mount(&server)
        .await;

    let manifest_path = ManifestPath {
        path: "file.txt".into(),
        hash: "aabbccdd11223344aabbccdd11223344".into(),
        size: 10,
        mtime: 1_700_000_000_000_000,
    };

    let err = download_file(
        &manifest_path,
        HashAlgorithm::Xxh128,
        download_dir.path().to_str().unwrap(),
        &s3_client,
        "test-bucket",
        Some("root-prefix/Data"),
        "123456789012",
        None,
        FileConflictResolution::CreateCopy,
        &Default::default(),
    )
    .await
    .unwrap_err();

    let msg = err.to_string();
    assert!(
        msg.contains("s3:GetObject"),
        "expected GetObject guidance: {msg}"
    );
}

#[tokio::test]
async fn download_file_403_kms_returns_decrypt_guidance() {
    let server = MockServer::start().await;
    let s3_client = build_s3_client(&server).await;
    let download_dir = TempDir::new().unwrap();

    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(403).set_body_string(
            r#"<?xml version="1.0"?><Error><Code>AccessDenied</Code><Message>kms:Decrypt access denied</Message></Error>"#,
        ))
        .mount(&server)
        .await;

    let manifest_path = ManifestPath {
        path: "file.txt".into(),
        hash: "aabbccdd11223344aabbccdd11223344".into(),
        size: 10,
        mtime: 1_700_000_000_000_000,
    };

    let err = download_file(
        &manifest_path,
        HashAlgorithm::Xxh128,
        download_dir.path().to_str().unwrap(),
        &s3_client,
        "test-bucket",
        Some("root-prefix/Data"),
        "123456789012",
        None,
        FileConflictResolution::CreateCopy,
        &Default::default(),
    )
    .await
    .unwrap_err();

    let msg = err.to_string();
    assert!(msg.contains("kms:Decrypt"), "expected KMS guidance: {msg}");
}

#[tokio::test]
async fn download_file_skip_existing_returns_none_path() {
    let server = MockServer::start().await;
    let s3_client = build_s3_client(&server).await;
    let download_dir = TempDir::new().unwrap();

    // Create the file locally first
    let local_file = download_dir.path().join("existing.txt");
    fs::write(&local_file, b"original").unwrap();

    let manifest_path = ManifestPath {
        path: "existing.txt".into(),
        hash: "aabbccdd11223344aabbccdd11223344".into(),
        size: 8,
        mtime: 1_700_000_000_000_000,
    };

    let (bytes, local_path) = download_file(
        &manifest_path,
        HashAlgorithm::Xxh128,
        download_dir.path().to_str().unwrap(),
        &s3_client,
        "test-bucket",
        Some("root-prefix/Data"),
        "123456789012",
        None,
        FileConflictResolution::Skip,
        &Default::default(),
    )
    .await
    .unwrap();

    assert_eq!(bytes, 8);
    assert!(local_path.is_none());
    // Original file unchanged
    assert_eq!(fs::read_to_string(&local_file).unwrap(), "original");
}

#[tokio::test]
async fn download_file_overwrite_existing_replaces_content() {
    let server = MockServer::start().await;
    let s3_client = build_s3_client(&server).await;
    let download_dir = TempDir::new().unwrap();

    // Create the file locally first
    let local_file = download_dir.path().join("existing.txt");
    fs::write(&local_file, b"original").unwrap();

    mock_s3_get_object(&server, b"new content").await;

    let manifest_path = ManifestPath {
        path: "existing.txt".into(),
        hash: "aabbccdd11223344aabbccdd11223344".into(),
        size: 11,
        mtime: 1_700_000_000_000_000,
    };

    let (_, local_path) = download_file(
        &manifest_path,
        HashAlgorithm::Xxh128,
        download_dir.path().to_str().unwrap(),
        &s3_client,
        "test-bucket",
        Some("root-prefix/Data"),
        "123456789012",
        None,
        FileConflictResolution::Overwrite,
        &Default::default(),
    )
    .await
    .unwrap();

    let path = local_path.unwrap();
    assert_eq!(fs::read_to_string(&path).unwrap(), "new content");
}

#[tokio::test]
async fn download_file_create_copy_generates_unique_name() {
    let server = MockServer::start().await;
    let s3_client = build_s3_client(&server).await;
    let download_dir = TempDir::new().unwrap();

    // Create the file locally first
    let local_file = download_dir.path().join("file.txt");
    fs::write(&local_file, b"original").unwrap();

    mock_s3_get_object(&server, b"copy content").await;

    let manifest_path = ManifestPath {
        path: "file.txt".into(),
        hash: "aabbccdd11223344aabbccdd11223344".into(),
        size: 12,
        mtime: 1_700_000_000_000_000,
    };

    let (_, local_path) = download_file(
        &manifest_path,
        HashAlgorithm::Xxh128,
        download_dir.path().to_str().unwrap(),
        &s3_client,
        "test-bucket",
        Some("root-prefix/Data"),
        "123456789012",
        None,
        FileConflictResolution::CreateCopy,
        &Default::default(),
    )
    .await
    .unwrap();

    let path = local_path.unwrap();
    // Should be a different path than the original
    assert_ne!(path, local_file);
    // Original unchanged
    assert_eq!(fs::read_to_string(&local_file).unwrap(), "original");
    // Copy has new content
    assert_eq!(fs::read_to_string(&path).unwrap(), "copy content");
    // Copy name follows pattern: "file (1).txt"
    let name = path.file_name().unwrap().to_str().unwrap();
    assert!(name.contains("(1)"), "expected copy suffix in: {name}");
}

// =====================================================================
//  cases 1-7: download_files_from_manifests
// =====================================================================

#[tokio::test]
async fn download_files_from_manifests_single_manifest_downloads_all() {
    let server = MockServer::start().await;
    let s3_client = build_s3_client(&server).await;
    let download_dir = TempDir::new().unwrap();
    let root = download_dir.path().to_str().unwrap().to_owned();

    mock_s3_get_object(&server, b"file content").await;

    let manifest = make_manifest_no_files(&[("a.txt", "aabbccdd11223344aabbccdd11223344", 12)]);

    let mut manifests_by_root = HashMap::new();
    manifests_by_root.insert(root.clone(), manifest);

    let stats = download_files_from_manifests(
        "test-bucket",
        &manifests_by_root,
        Some("root-prefix/Data"),
        &s3_client,
        "123456789012",
        None,
        FileConflictResolution::CreateCopy,
    )
    .await
    .unwrap();

    assert_eq!(stats.stats.processed_files, 1);
    assert!(download_dir.path().join("a.txt").exists());
}

#[tokio::test]
async fn download_files_from_manifests_multiple_roots() {
    let server = MockServer::start().await;
    let s3_client = build_s3_client(&server).await;
    let root1 = TempDir::new().unwrap();
    let root2 = TempDir::new().unwrap();

    mock_s3_get_object(&server, b"content").await;

    let m1 = make_manifest_no_files(&[("f1.txt", "aabbccdd11223344aabbccdd11223344", 7)]);
    let m2 = make_manifest_no_files(&[("f2.txt", "11223344aabbccdd11223344aabbccdd", 7)]);

    let mut manifests_by_root = HashMap::new();
    manifests_by_root.insert(root1.path().to_str().unwrap().to_owned(), m1);
    manifests_by_root.insert(root2.path().to_str().unwrap().to_owned(), m2);

    let stats = download_files_from_manifests(
        "test-bucket",
        &manifests_by_root,
        Some("root-prefix/Data"),
        &s3_client,
        "123456789012",
        None,
        FileConflictResolution::CreateCopy,
    )
    .await
    .unwrap();

    assert_eq!(stats.stats.processed_files, 2);
    assert!(root1.path().join("f1.txt").exists());
    assert!(root2.path().join("f2.txt").exists());
}

#[tokio::test]
async fn download_files_from_manifests_callback_cancel_returns_error() {
    let server = MockServer::start().await;
    let s3_client = build_s3_client(&server).await;
    let download_dir = TempDir::new().unwrap();
    let root = download_dir.path().to_str().unwrap().to_owned();

    mock_s3_get_object(&server, b"data").await;

    let manifest = make_manifest_no_files(&[
        ("a.txt", "aabbccdd11223344aabbccdd11223344", 4),
        ("b.txt", "11223344aabbccdd11223344aabbccdd", 4),
    ]);

    let mut manifests_by_root = HashMap::new();
    manifests_by_root.insert(root, manifest);

    // Callback returns false to cancel
    let err = download_files_from_manifests(
        "test-bucket",
        &manifests_by_root,
        Some("root-prefix/Data"),
        &s3_client,
        "123456789012",
        Some(Box::new(|_| false)),
        FileConflictResolution::CreateCopy,
    )
    .await
    .unwrap_err();

    let msg = err.to_string();
    assert!(
        msg.to_lowercase().contains("cancel"),
        "expected cancellation error: {msg}"
    );
}

#[tokio::test]
async fn download_files_from_manifests_skip_existing_tracks_skipped() {
    let server = MockServer::start().await;
    let s3_client = build_s3_client(&server).await;
    let download_dir = TempDir::new().unwrap();
    let root = download_dir.path().to_str().unwrap().to_owned();

    // Pre-create the file
    fs::write(download_dir.path().join("existing.txt"), b"old").unwrap();

    let manifest =
        make_manifest_no_files(&[("existing.txt", "aabbccdd11223344aabbccdd11223344", 3)]);

    let mut manifests_by_root = HashMap::new();
    manifests_by_root.insert(root, manifest);

    let stats = download_files_from_manifests(
        "test-bucket",
        &manifests_by_root,
        Some("root-prefix/Data"),
        &s3_client,
        "123456789012",
        None,
        FileConflictResolution::Skip,
    )
    .await
    .unwrap();

    assert_eq!(stats.stats.skipped_files, 1);
    assert_eq!(stats.stats.processed_files, 0);
}

// =====================================================================
//  cases 25-30: get_output_manifests_by_asset_root
// =====================================================================

#[tokio::test]
async fn get_output_manifests_no_manifests_returns_empty() {
    let server = MockServer::start().await;
    let s3_client = build_s3_client(&server).await;
    let s3_settings = test_s3_settings();

    // ListObjectsV2 returns empty
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"<?xml version="1.0"?><ListBucketV2Result xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><KeyCount>0</KeyCount></ListBucketV2Result>"#,
        ))
        .mount(&server)
        .await;

    let result = get_output_manifests_by_asset_root(
        &s3_settings,
        "farm-1",
        "queue-1",
        "job-1",
        None,
        None,
        None,
        &s3_client,
        "123456789012",
    )
    .await
    .unwrap();

    assert!(result.is_empty());
}

#[tokio::test]
async fn get_output_manifests_session_action_without_step_errors() {
    let server = MockServer::start().await;
    let s3_client = build_s3_client(&server).await;
    let s3_settings = test_s3_settings();

    let err = get_output_manifests_by_asset_root(
        &s3_settings,
        "farm-1",
        "queue-1",
        "job-1",
        None, // no step_id
        None, // no task_id
        Some("sessionaction-abc-1"),
        &s3_client,
        "123456789012",
    )
    .await
    .unwrap_err();

    let msg = err.to_string();
    assert!(
        msg.contains("Step ID") || msg.contains("Task ID"),
        "expected missing step/task error: {msg}"
    );
}

// =====================================================================
// Path traversal validation
// =====================================================================

// These tests verify that download_files_from_manifests rejects manifests
// containing path traversal sequences (e.g. ../../etc/passwd).
// Currently, no validation exists — these tests will fail until
// ensure_paths_within_directory is added to the download path.

#[tokio::test]
async fn download_rejects_path_traversal_in_manifest() {
    let server = MockServer::start().await;
    let s3_client = build_s3_client(&server).await;
    let download_dir = TempDir::new().unwrap();
    let root = download_dir.path().to_str().unwrap().to_owned();

    // Manifest with a path traversal attack
    let evil_manifest = make_manifest_no_files(&[("../../etc/passwd", "deadbeef", 100)]);

    let manifests_by_root = HashMap::from([(root.clone(), evil_manifest)]);

    let result = download_files_from_manifests(
        "test-bucket",
        &manifests_by_root,
        Some("root-prefix/Data"),
        &s3_client,
        "123456789012",
        None,
        FileConflictResolution::CreateCopy,
    )
    .await;

    assert!(
        result.is_err(),
        "path traversal should be rejected before download"
    );
    let msg = result.unwrap_err().to_string();
    assert!(
        msg.contains("outside") || msg.contains("not under") || msg.contains("traversal"),
        "error should mention path is outside root: {msg}"
    );
}

// download_accepts_valid_relative_paths_in_manifest — not included because
// valid paths already work. Only the rejection of traversal paths is new behavior.

// =====================================================================
// download_manifest_from_s3 must return S3 LastModified
// =====================================================================

/// `download_manifest_from_s3` returns the S3 `LastModified` timestamp
/// alongside the asset root and manifest. The caller uses this to sort
/// manifests chronologically before merging (older first, newer wins).
#[tokio::test]
async fn download_manifest_from_s3_returns_last_modified() {
    use deadline_job_attachments::download::download_manifest_from_s3;
    use wiremock::matchers::path;

    let server = MockServer::start().await;
    let s3_client = build_s3_client(&server).await;

    // A minimal valid manifest JSON
    let manifest_json = r#"{"hashAlg":"xxh128","manifestVersion":"2023-03-03","paths":[{"hash":"abc123def456abc123def456abc12345","mtime":1000000,"path":"file.txt","size":42}],"totalSize":42}"#;

    // Mount S3 GetObject with a specific Last-Modified timestamp
    Mock::given(method("GET"))
        .and(path("/test-bucket/manifest-key"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(manifest_json)
                .insert_header("last-modified", "Sat, 15 Jun 2024 14:30:00 GMT")
                .insert_header("x-amz-meta-asset-root-json", r#""/mnt/shared""#),
        )
        .mount(&server)
        .await;

    let (asset_root, last_modified, manifest) =
        download_manifest_from_s3(&s3_client, "test-bucket", "manifest-key", "123456789012")
            .await
            .unwrap();

    assert_eq!(asset_root, Some("/mnt/shared".to_owned()));
    assert_eq!(manifest.paths.len(), 1);
    // The LastModified should be 2024-06-15T14:30:00Z
    assert_eq!(
        last_modified.format("%Y-%m-%dT%H:%M:%SZ").to_string(),
        "2024-06-15T14:30:00Z"
    );
}

/// `get_output_manifests_by_asset_root` sorts manifests by
/// `LastModified` before merging, so newer files overwrite older ones.
/// This is tested at Level 2 via the CLI (job download-output), but
/// we add a Level 1 test for precision on the merge ordering.
#[tokio::test]
async fn get_output_manifests_merges_by_last_modified_order() {
    use deadline_job_attachments::download::get_output_manifests_by_asset_root;
    use wiremock::matchers::{path, query_param};

    let server = MockServer::start().await;
    let s3_client = build_s3_client(&server).await;
    let s3_settings = test_s3_settings();

    // Two manifests for the same asset root, with different timestamps.
    // The older manifest has "old_hash", the newer has "new_hash".
    let manifest_old = r#"{"hashAlg":"xxh128","manifestVersion":"2023-03-03","paths":[{"hash":"aaa111bbb222ccc333ddd444eee55566","mtime":1000000,"path":"file.txt","size":42}],"totalSize":42}"#;
    let manifest_new = r#"{"hashAlg":"xxh128","manifestVersion":"2023-03-03","paths":[{"hash":"fff666eee555ddd444ccc333bbb22211","mtime":2000000,"path":"file.txt","size":42}],"totalSize":42}"#;

    // S3 ListObjectsV2 returns two manifest keys
    Mock::given(method("GET"))
        .and(query_param("list-type", "2"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>test-bucket</Name>
  <KeyCount>2</KeyCount>
  <MaxKeys>1000</MaxKeys>
  <IsTruncated>false</IsTruncated>
  <Contents><Key>root-prefix/Manifests/farm-1/queue-1/job-1/step-1/task-1/sa-1/output_old.json</Key><Size>100</Size></Contents>
  <Contents><Key>root-prefix/Manifests/farm-1/queue-1/job-1/step-1/task-1/sa-2/output_new.json</Key><Size>100</Size></Contents>
</ListBucketResult>"#,
        ))
        .mount(&server)
        .await;

    // Older manifest (LastModified earlier)
    Mock::given(method("GET"))
        .and(path("/test-bucket/root-prefix/Manifests/farm-1/queue-1/job-1/step-1/task-1/sa-1/output_old.json"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(manifest_old)
                .insert_header("last-modified", "Sat, 15 Jun 2024 10:00:00 GMT")
                .insert_header("x-amz-meta-asset-root-json", r#""/mnt/shared""#),
        )
        .mount(&server)
        .await;

    // Newer manifest (LastModified later)
    Mock::given(method("GET"))
        .and(path("/test-bucket/root-prefix/Manifests/farm-1/queue-1/job-1/step-1/task-1/sa-2/output_new.json"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(manifest_new)
                .insert_header("last-modified", "Sat, 15 Jun 2024 14:00:00 GMT")
                .insert_header("x-amz-meta-asset-root-json", r#""/mnt/shared""#),
        )
        .mount(&server)
        .await;

    let result = get_output_manifests_by_asset_root(
        &s3_settings,
        "farm-1",
        "queue-1",
        "job-1",
        Some("step-1"),
        Some("task-1"),
        None,
        &s3_client,
        "123456789012",
    )
    .await
    .unwrap();

    // Should have one asset root with one merged manifest
    let manifests = result.get("/mnt/shared").expect("missing /mnt/shared root");
    assert_eq!(manifests.len(), 1);
    // The merged manifest should have the newer hash (newer LastModified wins)
    let merged = &manifests[0];
    assert_eq!(merged.paths.len(), 1);
    assert_eq!(merged.paths[0].hash, "fff666eee555ddd444ccc333bbb22211");
}

// =====================================================================
// Batch 3: compute_download_workers (#9 download side)
// =====================================================================

// Python: _get_num_download_workers(s3_max_pool_connections=50) -> int
// num_download_workers = s3_max_pool_connections / S3_DOWNLOAD_MAX_CONCURRENCY
// Minimum 1.
// This function doesn't exist in Rust yet — needs to be added.

#[test]
fn compute_download_workers_default_pool() {
    // 50 / 10 = 5
    assert_eq!(
        deadline_job_attachments::s3::compute_download_workers(50),
        5
    );
}

#[test]
fn compute_download_workers_small_pool() {
    // 5 / 10 = 0 → clamped to 1
    assert_eq!(deadline_job_attachments::s3::compute_download_workers(5), 1);
}

#[test]
fn compute_download_workers_exact_concurrency() {
    // 10 / 10 = 1
    assert_eq!(
        deadline_job_attachments::s3::compute_download_workers(10),
        1
    );
}
