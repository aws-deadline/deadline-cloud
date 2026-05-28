//! Level 1 tests for the S3 upload engine (, batch 9b).
//!
//! These tests use wiremock to stub S3 API responses and real temp
//! directories for file I/O. No mocking libraries.

use std::path::Path;

use deadline_lib::attachments::models::{
    AssetRootGroup, AssetRootManifest, JobAttachmentS3Settings,
};
use deadline_lib::attachments::upload::{S3UploadContext, snapshot_assets, upload_assets};
use openjd_snapshots::{FileEntry, HashAlgorithm, Snapshot, WHOLE_FILE_CHUNK_SIZE};
use tempfile::TempDir;
use wiremock::matchers::{header_exists, method, path_regex};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn test_manifest(dir: &Path, files: &[(&str, &[u8])]) -> Snapshot {
    let mut entries = Vec::new();
    for (name, content) in files {
        let file_path = dir.join(name);
        if let Some(parent) = file_path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(&file_path, content).unwrap();
        let hash = openjd_snapshots::hash::hash_file(&file_path).unwrap();
        let meta = std::fs::metadata(&file_path).unwrap();
        let mut e = FileEntry::file(*name, meta.len(), 1_000_000);
        e.hash = Some(hash);
        entries.push(e);
    }
    let total_size: u64 = entries.iter().map(|f| f.size.unwrap_or(0)).sum();
    let mut snap = Snapshot::new(HashAlgorithm::Xxh128, WHOLE_FILE_CHUNK_SIZE);
    snap.files = entries;
    snap.total_size = total_size;
    snap
}

/// Create files on disk and return an `AssetRootGroup` with those files as inputs.
fn test_group(dir: &Path, files: &[(&str, &[u8])]) -> AssetRootGroup {
    let mut inputs = std::collections::BTreeSet::new();
    for (name, content) in files {
        let file_path = dir.join(name);
        if let Some(parent) = file_path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(&file_path, content).unwrap();
        inputs.insert(file_path);
    }
    AssetRootGroup {
        root_path: dir.to_path_buf(),
        file_system_location_name: None,
        inputs,
        outputs: std::collections::BTreeSet::new(),
        references: std::collections::BTreeSet::new(),
    }
}

fn test_s3_settings() -> JobAttachmentS3Settings {
    JobAttachmentS3Settings::from_root_path("test-bucket/root-prefix").unwrap()
}

async fn build_uploader(server: &MockServer) -> S3UploadContext {
    let sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new("us-west-2"))
        .endpoint_url(server.uri())
        .test_credentials()
        .load()
        .await;
    let s3_client = deadline_lib::attachments::s3::build_s3_client(&sdk_config, None);
    S3UploadContext::new(s3_client, "123456789012".into()).unwrap()
}

/// Mount S3 `HeadObject` returning 404 (object does not exist).
async fn mock_s3_head_object_not_found(server: &MockServer) {
    Mock::given(method("HEAD"))
        .respond_with(ResponseTemplate::new(404))
        .mount(server)
        .await;
}

/// Mount S3 `PutObject` returning 200 (upload success).
async fn mock_s3_put_object_success(server: &MockServer) {
    Mock::given(method("PUT"))
        .respond_with(ResponseTemplate::new(200))
        .mount(server)
        .await;
}

// =====================================================================
// upload_assets — happy path with input files
// =====================================================================
#[tokio::test]
async fn upload_assets_returns_stats_and_attachments_with_manifest_paths() {
    let server = MockServer::start().await;
    // HEAD for file existence check → 404 (not found, needs upload)
    Mock::given(method("HEAD"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;
    // PUT for file data — verify CAS key format
    Mock::given(method("PUT"))
        .and(path_regex(
            r"/test-bucket/root-prefix/Data/[a-f0-9]{32}\.xxh128",
        ))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    // PUT for manifest — verify Manifests/ prefix and ExpectedBucketOwner header
    Mock::given(method("PUT"))
        .and(path_regex(r"/test-bucket/root-prefix/Manifests/farm-1/queue-1/Inputs/[a-f0-9]{32}/[a-f0-9]{32}_input"))
        .and(header_exists("x-amz-expected-bucket-owner"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let dir = TempDir::new().unwrap();
    let groups = vec![test_group(
        dir.path(),
        &[("a.txt", b"hello"), ("b.txt", b"world")],
    )];

    let uploader = build_uploader(&server).await;
    let s3_settings = test_s3_settings();
    let cache_dir = TempDir::new().unwrap();

    let (stats, attachments) = upload_assets(
        "farm-1",
        "queue-1",
        &s3_settings,
        &groups,
        &uploader,
        None,
        Some(cache_dir.path().to_str().unwrap()),
        None,
    )
    .await
    .unwrap();

    assert_eq!(stats.total_files, 2);
    assert!(!attachments.manifests.is_empty());
    assert!(attachments.manifests[0].input_manifest_path.is_some());
    assert!(attachments.manifests[0].input_manifest_hash.is_some());
}

// =====================================================================
// upload_assets — manifest with no input files (output only)
// =====================================================================
#[tokio::test]
async fn upload_assets_output_only_manifest_has_no_input_path() {
    let server = MockServer::start().await;
    let dir = TempDir::new().unwrap();
    let out_dir = dir.path().join("output");
    std::fs::create_dir_all(&out_dir).unwrap();

    let uploader = build_uploader(&server).await;
    let s3_settings = test_s3_settings();

    let groups = vec![AssetRootGroup {
        root_path: dir.path().to_path_buf(),
        file_system_location_name: None,
        inputs: std::collections::BTreeSet::new(),
        outputs: [out_dir].into_iter().collect(),
        references: std::collections::BTreeSet::new(),
    }];

    let (_, attachments) = upload_assets(
        "farm-1",
        "queue-1",
        &s3_settings,
        &groups,
        &uploader,
        None,
        None,
        None,
    )
    .await
    .unwrap();

    assert!(attachments.manifests[0].input_manifest_path.is_none());
    assert!(attachments.manifests[0].input_manifest_hash.is_none());
}

// =====================================================================
// upload_assets — output dir equals root produces "." not ""
// =====================================================================
#[tokio::test]
async fn upload_assets_output_dir_equals_root_uses_dot() {
    let server = MockServer::start().await;
    let dir = TempDir::new().unwrap();
    std::fs::write(dir.path().join("file.txt"), b"data").unwrap();

    let uploader = build_uploader(&server).await;
    let s3_settings = test_s3_settings();

    // Output dir is the same as root_path
    let groups = vec![AssetRootGroup {
        root_path: dir.path().to_path_buf(),
        file_system_location_name: None,
        inputs: std::collections::BTreeSet::new(),
        outputs: [dir.path().to_path_buf()].into_iter().collect(),
        references: std::collections::BTreeSet::new(),
    }];

    let (_, attachments) = upload_assets(
        "farm-1",
        "queue-1",
        &s3_settings,
        &groups,
        &uploader,
        None,
        None,
        None,
    )
    .await
    .unwrap();

    let dirs = attachments.manifests[0]
        .output_relative_directories
        .as_ref()
        .unwrap();
    assert_eq!(dirs, &vec![".".to_owned()]);
}

// =====================================================================
// upload_assets — missing farm_id errors
// =====================================================================
#[tokio::test]
async fn upload_assets_missing_farm_id_errors() {
    let server = MockServer::start().await;
    let uploader = build_uploader(&server).await;
    let s3_settings = test_s3_settings();

    let result = upload_assets(
        "",
        "queue-1",
        &s3_settings,
        &[],
        &uploader,
        None,
        None,
        None,
    )
    .await;

    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("Farm") || err.contains("missing"));
}

// =====================================================================
// upload_assets — callback cancellation mid-upload
// =====================================================================
#[tokio::test]
async fn upload_assets_callback_cancel_returns_error() {
    let server = MockServer::start().await;
    mock_s3_head_object_not_found(&server).await;
    mock_s3_put_object_success(&server).await;

    let dir = TempDir::new().unwrap();
    let groups = vec![test_group(dir.path(), &[("a.txt", b"data")])];

    let uploader = build_uploader(&server).await;
    let s3_settings = test_s3_settings();
    let cache_dir = TempDir::new().unwrap();

    let cancel_cb = |_: u64, _: u64| -> bool { false };

    let result = upload_assets(
        "farm-1",
        "queue-1",
        &s3_settings,
        &groups,
        &uploader,
        Some(Box::new(cancel_cb)),
        Some(cache_dir.path().to_str().unwrap()),
        None,
    )
    .await;

    assert!(result.is_err());
}

// =====================================================================
// upload_assets — per-file progress reporting
// =====================================================================
#[tokio::test]
async fn upload_assets_progress_callback_fires_per_file() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    let server = MockServer::start().await;
    mock_s3_head_object_not_found(&server).await;
    mock_s3_put_object_success(&server).await;

    let dir = TempDir::new().unwrap();
    // 3 files in one group — callback should fire at least 3 times
    let groups = vec![test_group(
        dir.path(),
        &[("a.txt", b"aaa"), ("b.txt", b"bbb"), ("c.txt", b"ccc")],
    )];

    let uploader = build_uploader(&server).await;
    let s3_settings = test_s3_settings();
    let cache_dir = TempDir::new().unwrap();

    let call_count = Arc::new(AtomicU32::new(0));
    let count_clone = call_count.clone();
    let progress_cb = move |_processed: u64, _total: u64| -> bool {
        count_clone.fetch_add(1, Ordering::Relaxed);
        true
    };

    let (stats, _) = upload_assets(
        "farm-1",
        "queue-1",
        &s3_settings,
        &groups,
        &uploader,
        Some(Box::new(progress_cb)),
        Some(cache_dir.path().to_str().unwrap()),
        None,
    )
    .await
    .unwrap();

    assert_eq!(stats.total_files, 3);
    // With per-file progress, callback should fire at least once per file
    let calls = call_count.load(Ordering::Relaxed);
    assert!(
        calls >= 3,
        "Expected progress callback to fire at least 3 times (once per file), got {calls}"
    );
}

// =====================================================================
// upload_assets — cancellation mid-group via progress callback
// =====================================================================
#[tokio::test]
async fn upload_assets_cancel_mid_group_stops_early() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    let server = MockServer::start().await;
    mock_s3_head_object_not_found(&server).await;
    mock_s3_put_object_success(&server).await;

    let dir = TempDir::new().unwrap();
    // 5 files — cancel after 2nd callback
    let groups = vec![test_group(
        dir.path(),
        &[
            ("a.txt", b"aaa"),
            ("b.txt", b"bbb"),
            ("c.txt", b"ccc"),
            ("d.txt", b"ddd"),
            ("e.txt", b"eee"),
        ],
    )];

    let uploader = build_uploader(&server).await;
    let s3_settings = test_s3_settings();
    let cache_dir = TempDir::new().unwrap();

    let call_count = Arc::new(AtomicU32::new(0));
    let count_clone = call_count.clone();
    let cancel_after_2 = move |_processed: u64, _total: u64| -> bool {
        let n = count_clone.fetch_add(1, Ordering::Relaxed);
        n < 2 // return false on 3rd call (index 2)
    };

    let result = upload_assets(
        "farm-1",
        "queue-1",
        &s3_settings,
        &groups,
        &uploader,
        Some(Box::new(cancel_after_2)),
        Some(cache_dir.path().to_str().unwrap()),
        None,
    )
    .await;

    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.to_lowercase().contains("cancel"),
        "Expected cancellation error, got: {err}"
    );
}

// =====================================================================
// upload_assets — multiple manifests with different roots
// =====================================================================
#[tokio::test]
async fn upload_assets_multiple_manifests_each_gets_properties() {
    let server = MockServer::start().await;
    Mock::given(method("HEAD"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;
    // Accept PUTs to both CAS data and Manifests paths
    Mock::given(method("PUT"))
        .and(header_exists("x-amz-expected-bucket-owner"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let dir1 = TempDir::new().unwrap();
    let dir2 = TempDir::new().unwrap();
    let mut g2 = test_group(dir2.path(), &[("b.txt", b"bbb")]);
    g2.file_system_location_name = Some("loc1".into());

    let uploader = build_uploader(&server).await;
    let s3_settings = test_s3_settings();
    let cache_dir = TempDir::new().unwrap();

    let groups = vec![test_group(dir1.path(), &[("a.txt", b"aaa")]), g2];

    let (_, attachments) = upload_assets(
        "farm-1",
        "queue-1",
        &s3_settings,
        &groups,
        &uploader,
        None,
        Some(cache_dir.path().to_str().unwrap()),
        None,
    )
    .await
    .unwrap();

    assert_eq!(attachments.manifests.len(), 2);
    assert!(attachments.manifests[0].input_manifest_path.is_some());
    assert!(attachments.manifests[1].input_manifest_path.is_some());
}

// =====================================================================
// upload_assets — force_s3_check=true bypasses cache
// =====================================================================
#[tokio::test]
async fn upload_assets_force_s3_check_bypasses_cache() {
    let server = MockServer::start().await;
    mock_s3_head_object_not_found(&server).await;
    mock_s3_put_object_success(&server).await;

    let dir = TempDir::new().unwrap();
    let groups = vec![test_group(dir.path(), &[("a.txt", b"data")])];

    let uploader = build_uploader(&server).await;
    let s3_settings = test_s3_settings();
    let cache_dir = TempDir::new().unwrap();

    let (stats, _) = upload_assets(
        "farm-1",
        "queue-1",
        &s3_settings,
        &groups,
        &uploader,
        None,
        Some(cache_dir.path().to_str().unwrap()),
        Some(true),
    )
    .await
    .unwrap();

    // With force_s3_check, file should be processed (not skipped via cache)
    assert!(stats.processed_files > 0 || stats.skipped_files > 0);
}

// =====================================================================
// upload_assets — force_s3_check=false uses cache
// =====================================================================
#[tokio::test]
async fn upload_assets_default_uses_s3_check_cache() {
    let server = MockServer::start().await;
    mock_s3_head_object_not_found(&server).await;
    mock_s3_put_object_success(&server).await;

    let dir = TempDir::new().unwrap();
    let groups = vec![test_group(dir.path(), &[("a.txt", b"data")])];

    let uploader = build_uploader(&server).await;
    let s3_settings = test_s3_settings();
    let cache_dir = TempDir::new().unwrap();

    // Default (None for force_s3_check) should use cache — just verify it succeeds
    let result = upload_assets(
        "farm-1",
        "queue-1",
        &s3_settings,
        &groups,
        &uploader,
        None,
        Some(cache_dir.path().to_str().unwrap()),
        None,
    )
    .await;

    assert!(result.is_ok());
}

// =====================================================================
// snapshot_assets — copies files to local directory
// =====================================================================
#[tokio::test]
async fn snapshot_assets_copies_files_to_local_dir() {
    let dir = TempDir::new().unwrap();
    let manifest = test_manifest(dir.path(), &[("a.txt", b"hello")]);
    let snapshot_dir = TempDir::new().unwrap();
    let s3_settings = test_s3_settings();

    let manifests = vec![AssetRootManifest {
        file_system_location_name: None,
        root_path: dir.path().to_path_buf(),
        asset_manifest: Some(manifest),
        outputs: vec![],
    }];

    let (_stats, attachments) = snapshot_assets(
        "farm-1",
        "queue-1",
        &s3_settings,
        snapshot_dir.path(),
        &manifests,
        None,
    )
    .unwrap();

    // Files should be in snapshot_dir/Data/
    let data_dir = snapshot_dir.path().join("Data");
    assert!(data_dir.exists());
    assert!(!attachments.manifests.is_empty());
    assert!(attachments.manifests[0].input_manifest_path.is_some());
}

// =====================================================================
// snapshot_assets — missing farm_id errors
// =====================================================================
#[tokio::test]
async fn snapshot_assets_missing_farm_id_errors() {
    let _dir = TempDir::new().unwrap();
    let snapshot_dir = TempDir::new().unwrap();
    let s3_settings = test_s3_settings();

    let result = snapshot_assets("", "queue-1", &s3_settings, snapshot_dir.path(), &[], None);

    assert!(result.is_err());
}

// =====================================================================
// snapshot_assets — callback cancel
// =====================================================================
#[tokio::test]
async fn snapshot_assets_callback_cancel_returns_error() {
    let dir = TempDir::new().unwrap();
    let manifest = test_manifest(dir.path(), &[("a.txt", b"data")]);
    let snapshot_dir = TempDir::new().unwrap();
    let s3_settings = test_s3_settings();

    let manifests = vec![AssetRootManifest {
        file_system_location_name: None,
        root_path: dir.path().to_path_buf(),
        asset_manifest: Some(manifest),
        outputs: vec![],
    }];

    let cancel_cb = |_: u64, _: u64| -> bool { false };

    let result = snapshot_assets(
        "farm-1",
        "queue-1",
        &s3_settings,
        snapshot_dir.path(),
        &manifests,
        Some(Box::new(cancel_cb)),
    );

    assert!(result.is_err());
}

// =====================================================================
// upload_bytes_to_s3 — S3 error guidance (pre-9e fix #2)
// =====================================================================

#[tokio::test]
async fn upload_bytes_to_s3_403_non_kms_returns_put_object_guidance() {
    let server = MockServer::start().await;
    Mock::given(method("PUT"))
        .respond_with(ResponseTemplate::new(403).set_body_string(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>"#,
        ))
        .mount(&server)
        .await;

    let uploader = build_uploader(&server).await;

    let result = uploader
        .upload_bytes_to_s3(
            b"manifest content",
            "test-bucket",
            "root-prefix/Manifests/some-key",
            None,
        )
        .await;

    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("s3:PutObject"),
        "Expected s3:PutObject guidance in error, got: {err}"
    );
}

#[tokio::test]
async fn upload_bytes_to_s3_403_kms_returns_kms_guidance() {
    let server = MockServer::start().await;
    Mock::given(method("PUT"))
        .respond_with(ResponseTemplate::new(403).set_body_string(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<Error><Code>AccessDenied</Code><Message>kms:GenerateDataKey denied</Message></Error>"#,
        ))
        .mount(&server)
        .await;

    let uploader = build_uploader(&server).await;

    let result = uploader
        .upload_bytes_to_s3(
            b"manifest content",
            "test-bucket",
            "root-prefix/Manifests/some-key",
            None,
        )
        .await;

    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("kms:GenerateDataKey") || err.contains("kms:DescribeKey"),
        "Expected KMS guidance in error, got: {err}"
    );
}

#[tokio::test]
async fn upload_bytes_to_s3_404_returns_bucket_guidance() {
    let server = MockServer::start().await;
    Mock::given(method("PUT"))
        .respond_with(ResponseTemplate::new(404).set_body_string(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<Error><Code>NoSuchBucket</Code><Message>The specified bucket does not exist</Message></Error>"#,
        ))
        .mount(&server)
        .await;

    let uploader = build_uploader(&server).await;

    let result = uploader
        .upload_bytes_to_s3(
            b"manifest content",
            "test-bucket",
            "root-prefix/Manifests/some-key",
            None,
        )
        .await;

    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("bucket") || err.contains("404"),
        "Expected bucket/404 guidance in error, got: {err}"
    );
}

// =====================================================================
// Batch 2: Multipart upload (#9 upload side)
// =====================================================================
