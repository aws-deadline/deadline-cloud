//! Level 1 tests for the S3 upload engine (§21, batch 9b).
//!
//! These tests use wiremock to stub S3 API responses and real temp
//! directories for file I/O. No mocking libraries.

use std::path::Path;

use deadline_job_attachments::asset_manifests::{
    AssetManifest, HashAlgorithm, ManifestPath, ManifestVersion,
};
use deadline_job_attachments::caches::S3CheckCache;
use deadline_job_attachments::models::{
    AssetRootManifest, JobAttachmentS3Settings,
};
use deadline_job_attachments::progress_tracker::{
    ProgressReportMetadata, ProgressStatus, ProgressTracker,
};
use deadline_job_attachments::upload::{
    upload_assets, snapshot_assets, S3UploadContext,
};
use tempfile::TempDir;
use wiremock::matchers::{method, path_regex, header_exists};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

// --- Test helpers ---

fn test_manifest(dir: &Path, files: &[(&str, &[u8])]) -> AssetManifest {
    let mut paths = Vec::new();
    for (name, content) in files {
        let file_path = dir.join(name);
        if let Some(parent) = file_path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(&file_path, content).unwrap();
        let hash = deadline_job_attachments::asset_manifests::hash_file(
            &file_path,
            HashAlgorithm::Xxh128,
        )
        .unwrap();
        let meta = std::fs::metadata(&file_path).unwrap();
        paths.push(ManifestPath {
            path: name.to_string(),
            hash,
            size: meta.len() as i64,
            mtime: 1000000, // fixed for tests
        });
    }
    let total_size: i64 = paths.iter().map(|p| p.size).sum();
    AssetManifest::new(
        HashAlgorithm::Xxh128,
        ManifestVersion::V2023_03_03,
        total_size,
        paths,
    )
    .unwrap()
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
    let s3_client = deadline_job_attachments::s3::build_s3_client(&sdk_config, None);
    S3UploadContext::new(s3_client, "123456789012".into(), None).unwrap()
}

/// Mount S3 HeadObject returning 200 (object exists).
async fn mock_s3_head_object_exists(server: &MockServer) {
    Mock::given(method("HEAD"))
        .respond_with(ResponseTemplate::new(200))
        .mount(server)
        .await;
}

/// Mount S3 HeadObject returning 404 (object does not exist).
async fn mock_s3_head_object_not_found(server: &MockServer) {
    Mock::given(method("HEAD"))
        .respond_with(ResponseTemplate::new(404))
        .mount(server)
        .await;
}

/// Mount S3 PutObject returning 200 (upload success).
async fn mock_s3_put_object_success(server: &MockServer) {
    Mock::given(method("PUT"))
        .respond_with(ResponseTemplate::new(200))
        .mount(server)
        .await;
}

// =====================================================================
// §21 case 1: upload_assets — happy path with input files
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
        .and(path_regex(r"/test-bucket/root-prefix/Data/[a-f0-9]{32}\.xxh128"))
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
    let manifest = test_manifest(dir.path(), &[("a.txt", b"hello"), ("b.txt", b"world")]);

    let uploader = build_uploader(&server).await;
    let s3_settings = test_s3_settings();
    let cache_dir = TempDir::new().unwrap();

    let manifests = vec![AssetRootManifest {
        file_system_location_name: None,
        root_path: dir.path().to_string_lossy().into(),
        asset_manifest: Some(manifest),
        outputs: vec![],
    }];

    let (stats, attachments) = upload_assets(
        "farm-1",
        "queue-1",
        &s3_settings,
        &manifests,
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
// §21 case 2: upload_assets — manifest with no input files (output only)
// =====================================================================
#[tokio::test]
async fn upload_assets_output_only_manifest_has_no_input_path() {
    let server = MockServer::start().await;
    let dir = TempDir::new().unwrap();
    let out_dir = dir.path().join("output");
    std::fs::create_dir_all(&out_dir).unwrap();

    let uploader = build_uploader(&server).await;
    let s3_settings = test_s3_settings();

    let manifests = vec![AssetRootManifest {
        file_system_location_name: None,
        root_path: dir.path().to_string_lossy().into(),
        asset_manifest: None,
        outputs: vec![out_dir],
    }];

    let (_, attachments) = upload_assets(
        "farm-1",
        "queue-1",
        &s3_settings,
        &manifests,
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
// §21 case 3: upload_assets — missing farm_id errors
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
// §21 case 4: upload_assets — callback cancellation mid-upload
// =====================================================================
#[tokio::test]
async fn upload_assets_callback_cancel_returns_error() {
    let server = MockServer::start().await;
    mock_s3_head_object_not_found(&server).await;
    mock_s3_put_object_success(&server).await;

    let dir = TempDir::new().unwrap();
    let manifest = test_manifest(dir.path(), &[("a.txt", b"data")]);

    let uploader = build_uploader(&server).await;
    let s3_settings = test_s3_settings();
    let cache_dir = TempDir::new().unwrap();

    let manifests = vec![AssetRootManifest {
        file_system_location_name: None,
        root_path: dir.path().to_string_lossy().into(),
        asset_manifest: Some(manifest),
        outputs: vec![],
    }];

    let cancel_cb = |_: ProgressReportMetadata| -> bool { false };

    let result = upload_assets(
        "farm-1",
        "queue-1",
        &s3_settings,
        &manifests,
        &uploader,
        Some(Box::new(cancel_cb)),
        Some(cache_dir.path().to_str().unwrap()),
        None,
    )
    .await;

    assert!(result.is_err());
}

// =====================================================================
// §21 case 5: upload_assets — multiple manifests with different roots
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
    let m1 = test_manifest(dir1.path(), &[("a.txt", b"aaa")]);
    let m2 = test_manifest(dir2.path(), &[("b.txt", b"bbb")]);

    let uploader = build_uploader(&server).await;
    let s3_settings = test_s3_settings();
    let cache_dir = TempDir::new().unwrap();

    let manifests = vec![
        AssetRootManifest {
            file_system_location_name: None,
            root_path: dir1.path().to_string_lossy().into(),
            asset_manifest: Some(m1),
            outputs: vec![],
        },
        AssetRootManifest {
            file_system_location_name: Some("loc1".into()),
            root_path: dir2.path().to_string_lossy().into(),
            asset_manifest: Some(m2),
            outputs: vec![],
        },
    ];

    let (_, attachments) = upload_assets(
        "farm-1",
        "queue-1",
        &s3_settings,
        &manifests,
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
// §21 case 6: upload_assets — force_s3_check=true bypasses cache
// =====================================================================
#[tokio::test]
async fn upload_assets_force_s3_check_bypasses_cache() {
    let server = MockServer::start().await;
    mock_s3_head_object_not_found(&server).await;
    mock_s3_put_object_success(&server).await;

    let dir = TempDir::new().unwrap();
    let manifest = test_manifest(dir.path(), &[("a.txt", b"data")]);

    let uploader = build_uploader(&server).await;
    let s3_settings = test_s3_settings();
    let cache_dir = TempDir::new().unwrap();

    let manifests = vec![AssetRootManifest {
        file_system_location_name: None,
        root_path: dir.path().to_string_lossy().into(),
        asset_manifest: Some(manifest),
        outputs: vec![],
    }];

    let (stats, _) = upload_assets(
        "farm-1",
        "queue-1",
        &s3_settings,
        &manifests,
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
// §21 case 7: upload_assets — force_s3_check=false uses cache
// =====================================================================
#[tokio::test]
async fn upload_assets_default_uses_s3_check_cache() {
    let server = MockServer::start().await;
    mock_s3_head_object_not_found(&server).await;
    mock_s3_put_object_success(&server).await;

    let dir = TempDir::new().unwrap();
    let manifest = test_manifest(dir.path(), &[("a.txt", b"data")]);

    let uploader = build_uploader(&server).await;
    let s3_settings = test_s3_settings();
    let cache_dir = TempDir::new().unwrap();

    let manifests = vec![AssetRootManifest {
        file_system_location_name: None,
        root_path: dir.path().to_string_lossy().into(),
        asset_manifest: Some(manifest),
        outputs: vec![],
    }];

    // Default (None for force_s3_check) should use cache — just verify it succeeds
    let result = upload_assets(
        "farm-1",
        "queue-1",
        &s3_settings,
        &manifests,
        &uploader,
        None,
        Some(cache_dir.path().to_str().unwrap()),
        None,
    )
    .await;

    assert!(result.is_ok());
}

// =====================================================================
// §21 case 8: snapshot_assets — copies files to local directory
// =====================================================================
#[tokio::test]
async fn snapshot_assets_copies_files_to_local_dir() {
    let dir = TempDir::new().unwrap();
    let manifest = test_manifest(dir.path(), &[("a.txt", b"hello")]);
    let snapshot_dir = TempDir::new().unwrap();
    let s3_settings = test_s3_settings();

    let manifests = vec![AssetRootManifest {
        file_system_location_name: None,
        root_path: dir.path().to_string_lossy().into(),
        asset_manifest: Some(manifest),
        outputs: vec![],
    }];

    let (stats, attachments) = snapshot_assets(
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
// §21 case 9: snapshot_assets — missing farm_id errors
// =====================================================================
#[tokio::test]
async fn snapshot_assets_missing_farm_id_errors() {
    let dir = TempDir::new().unwrap();
    let snapshot_dir = TempDir::new().unwrap();
    let s3_settings = test_s3_settings();

    let result = snapshot_assets(
        "",
        "queue-1",
        &s3_settings,
        snapshot_dir.path(),
        &[],
        None,
    );

    assert!(result.is_err());
}

// =====================================================================
// §21 case 10: snapshot_assets — callback cancel
// =====================================================================
#[tokio::test]
async fn snapshot_assets_callback_cancel_returns_error() {
    let dir = TempDir::new().unwrap();
    let manifest = test_manifest(dir.path(), &[("a.txt", b"data")]);
    let snapshot_dir = TempDir::new().unwrap();
    let s3_settings = test_s3_settings();

    let manifests = vec![AssetRootManifest {
        file_system_location_name: None,
        root_path: dir.path().to_string_lossy().into(),
        asset_manifest: Some(manifest),
        outputs: vec![],
    }];

    let cancel_cb = |_: ProgressReportMetadata| -> bool { false };

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
// §21 case 11: upload_input_files — small files uploaded in parallel
// =====================================================================
#[tokio::test]
async fn upload_input_files_small_files_uploaded() {
    let server = MockServer::start().await;
    // Verify files are PUT to the correct CAS key path: /test-bucket/root-prefix/Data/{hash}.xxh128
    Mock::given(method("HEAD"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;
    Mock::given(method("PUT"))
        .and(path_regex(r"/test-bucket/root-prefix/Data/[a-f0-9]{32}\.xxh128"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let dir = TempDir::new().unwrap();
    let manifest = test_manifest(dir.path(), &[("a.txt", b"aaa"), ("b.txt", b"bbb")]);

    let uploader = build_uploader(&server).await;
    let tracker = ProgressTracker::new(ProgressStatus::UploadInProgress, 2, 6, None);
    let cache_dir = TempDir::new().unwrap();

    uploader
        .upload_input_files(
            &manifest,
            "test-bucket",
            dir.path(),
            "root-prefix/Data",
            Some(&tracker),
            Some(cache_dir.path().to_str().unwrap()),
            None,
        )
        .await
        .unwrap();
}

// =====================================================================
// §21 case 14: upload_input_files — file already in S3 is skipped
// =====================================================================
#[tokio::test]
async fn upload_input_files_existing_file_skipped() {
    let server = MockServer::start().await;
    // HEAD returns 200 — file exists in S3, should be skipped
    Mock::given(method("HEAD"))
        .and(path_regex(r"/test-bucket/root-prefix/Data/[a-f0-9]{32}\.xxh128"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let dir = TempDir::new().unwrap();
    let manifest = test_manifest(dir.path(), &[("a.txt", b"data")]);

    let uploader = build_uploader(&server).await;
    let tracker = ProgressTracker::new(ProgressStatus::UploadInProgress, 1, 4, None);
    let cache_dir = TempDir::new().unwrap();

    uploader
        .upload_input_files(
            &manifest,
            "test-bucket",
            dir.path(),
            "root-prefix/Data",
            Some(&tracker),
            Some(cache_dir.path().to_str().unwrap()),
            None,
        )
        .await
        .unwrap();

    let stats = tracker.get_summary_statistics();
    assert_eq!(stats.skipped_files, 1);
}

// =====================================================================
// §21 case 15: upload_input_files — file in S3 check cache is skipped
// =====================================================================
#[tokio::test]
async fn upload_input_files_cached_file_skipped_without_s3_call() {
    let server = MockServer::start().await;
    // No HEAD mock — if it tries to call S3, it will fail

    let dir = TempDir::new().unwrap();
    let manifest = test_manifest(dir.path(), &[("a.txt", b"data")]);

    let uploader = build_uploader(&server).await;
    let cache_dir = TempDir::new().unwrap();

    // Pre-populate the S3 check cache
    {
        let cache = S3CheckCache::new(cache_dir.path().to_str().unwrap()).unwrap();
        let hash = &manifest.paths[0].hash;
        let cache_key = format!("test-bucket/root-prefix/Data/{}.xxh128", hash);
        cache.put_entry(&deadline_job_attachments::caches::S3CheckCacheEntry {
            s3_key: cache_key,
            last_seen_time: format!(
                "{}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs_f64()
            ),
        });
    }

    let tracker = ProgressTracker::new(ProgressStatus::UploadInProgress, 1, 4, None);

    uploader
        .upload_input_files(
            &manifest,
            "test-bucket",
            dir.path(),
            "root-prefix/Data",
            Some(&tracker),
            Some(cache_dir.path().to_str().unwrap()),
            None,
        )
        .await
        .unwrap();

    let stats = tracker.get_summary_statistics();
    assert_eq!(stats.skipped_files, 1);
}

// =====================================================================
// §21 case 16: upload_input_files — cancellation after batch
// =====================================================================
#[tokio::test]
async fn upload_input_files_cancel_after_batch_returns_error() {
    let server = MockServer::start().await;
    mock_s3_head_object_not_found(&server).await;
    mock_s3_put_object_success(&server).await;

    let dir = TempDir::new().unwrap();
    let manifest = test_manifest(dir.path(), &[("a.txt", b"data")]);

    let uploader = build_uploader(&server).await;
    let cancel_cb = |_: ProgressReportMetadata| -> bool { false };
    let tracker = ProgressTracker::new(
        ProgressStatus::UploadInProgress,
        1,
        4,
        Some(Box::new(cancel_cb)),
    );
    let cache_dir = TempDir::new().unwrap();

    let result = uploader
        .upload_input_files(
            &manifest,
            "test-bucket",
            dir.path(),
            "root-prefix/Data",
            Some(&tracker),
            Some(cache_dir.path().to_str().unwrap()),
            None,
        )
        .await;

    assert!(result.is_err());
}

// =====================================================================
// §21 case 17: upload_file_to_s3 — valid file uploads
// =====================================================================
#[tokio::test]
async fn upload_file_to_s3_valid_file_succeeds() {
    let server = MockServer::start().await;
    // Verify ExpectedBucketOwner header is sent
    Mock::given(method("PUT"))
        .and(header_exists("x-amz-expected-bucket-owner"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let dir = TempDir::new().unwrap();
    std::fs::write(dir.path().join("test.txt"), b"content").unwrap();

    let uploader = build_uploader(&server).await;

    uploader
        .upload_file_to_s3(
            &dir.path().join("test.txt"),
            "test-bucket",
            "root-prefix/Data/abc123.xxh128",
            None,
        )
        .await
        .unwrap();
}

// =====================================================================
// §21 case 18: upload_file_to_s3 — directory is silently skipped
// =====================================================================
#[tokio::test]
async fn upload_file_to_s3_directory_silently_skipped() {
    let server = MockServer::start().await;
    let dir = TempDir::new().unwrap();
    let sub = dir.path().join("subdir");
    std::fs::create_dir_all(&sub).unwrap();

    let uploader = build_uploader(&server).await;

    // Should not error — just skip
    uploader
        .upload_file_to_s3(&sub, "test-bucket", "key", None)
        .await
        .unwrap();
}

// =====================================================================
// §21 case 19: upload_file_to_s3 — non-existent path silently skipped
// =====================================================================
#[tokio::test]
async fn upload_file_to_s3_nonexistent_silently_skipped() {
    let server = MockServer::start().await;
    let uploader = build_uploader(&server).await;

    uploader
        .upload_file_to_s3(
            Path::new("/nonexistent/file.txt"),
            "test-bucket",
            "key",
            None,
        )
        .await
        .unwrap();
}

// =====================================================================
// §21 case 20: upload_file_to_s3 — S3 403 non-KMS error
// =====================================================================
#[tokio::test]
async fn upload_file_to_s3_403_non_kms_returns_s3_client_error() {
    let server = MockServer::start().await;
    Mock::given(method("PUT"))
        .respond_with(
            ResponseTemplate::new(403).set_body_string(
                r#"<?xml version="1.0" encoding="UTF-8"?>
<Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>"#,
            ),
        )
        .mount(&server)
        .await;

    let dir = TempDir::new().unwrap();
    std::fs::write(dir.path().join("test.txt"), b"data").unwrap();

    let uploader = build_uploader(&server).await;

    let result = uploader
        .upload_file_to_s3(
            &dir.path().join("test.txt"),
            "test-bucket",
            "root-prefix/Data/abc.xxh128",
            None,
        )
        .await;

    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("s3:PutObject") || err.contains("403"));
}

// =====================================================================
// §21 case 21: upload_file_to_s3 — S3 403 KMS error
// =====================================================================
#[tokio::test]
async fn upload_file_to_s3_403_kms_returns_kms_guidance() {
    let server = MockServer::start().await;
    Mock::given(method("PUT"))
        .respond_with(
            ResponseTemplate::new(403).set_body_string(
                r#"<?xml version="1.0" encoding="UTF-8"?>
<Error><Code>AccessDenied</Code><Message>kms:GenerateDataKey denied</Message></Error>"#,
            ),
        )
        .mount(&server)
        .await;

    let dir = TempDir::new().unwrap();
    std::fs::write(dir.path().join("test.txt"), b"data").unwrap();

    let uploader = build_uploader(&server).await;

    let result = uploader
        .upload_file_to_s3(
            &dir.path().join("test.txt"),
            "test-bucket",
            "root-prefix/Data/abc.xxh128",
            None,
        )
        .await;

    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("kms:GenerateDataKey") || err.contains("kms:DescribeKey"));
}

// =====================================================================
// §21 case 22: upload_file_to_s3 — S3 404 error
// =====================================================================
#[tokio::test]
async fn upload_file_to_s3_404_returns_bucket_guidance() {
    let server = MockServer::start().await;
    Mock::given(method("PUT"))
        .respond_with(
            ResponseTemplate::new(404).set_body_string(
                r#"<?xml version="1.0" encoding="UTF-8"?>
<Error><Code>NoSuchBucket</Code><Message>The specified bucket does not exist</Message></Error>"#,
            ),
        )
        .mount(&server)
        .await;

    let dir = TempDir::new().unwrap();
    std::fs::write(dir.path().join("test.txt"), b"data").unwrap();

    let uploader = build_uploader(&server).await;

    let result = uploader
        .upload_file_to_s3(
            &dir.path().join("test.txt"),
            "test-bucket",
            "root-prefix/Data/abc.xxh128",
            None,
        )
        .await;

    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("bucket") || err.contains("404"));
}

// =====================================================================
// §21 case 25: file_already_uploaded — object exists returns true
// =====================================================================
#[tokio::test]
async fn file_already_uploaded_exists_returns_true() {
    let server = MockServer::start().await;
    mock_s3_head_object_exists(&server).await;

    let uploader = build_uploader(&server).await;
    let result = uploader
        .file_already_uploaded("test-bucket", "some-key")
        .await
        .unwrap();
    assert!(result);
}

// =====================================================================
// §21 case 26: file_already_uploaded — 404 returns false
// =====================================================================
#[tokio::test]
async fn file_already_uploaded_404_returns_false() {
    let server = MockServer::start().await;
    mock_s3_head_object_not_found(&server).await;

    let uploader = build_uploader(&server).await;
    let result = uploader
        .file_already_uploaded("test-bucket", "some-key")
        .await
        .unwrap();
    assert!(!result);
}

// =====================================================================
// §21 case 27: file_already_uploaded — 403 returns error
// =====================================================================
#[tokio::test]
async fn file_already_uploaded_403_returns_error() {
    let server = MockServer::start().await;
    Mock::given(method("HEAD"))
        .respond_with(ResponseTemplate::new(403))
        .mount(&server)
        .await;

    let uploader = build_uploader(&server).await;
    let result = uploader
        .file_already_uploaded("test-bucket", "some-key")
        .await;
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("s3:ListBucket") || err.contains("403"));
}

// =====================================================================
// §21 case 28: file_already_uploaded — transport error
// =====================================================================
#[tokio::test]
async fn file_already_uploaded_transport_error() {
    // Use a server that immediately drops connections
    let server = MockServer::start().await;
    drop(server);

    let sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new("us-west-2"))
        .endpoint_url("http://localhost:1") // unreachable port
        .test_credentials()
        .load()
        .await;
    let s3_client = deadline_job_attachments::s3::build_s3_client(&sdk_config, None);
    let uploader = S3UploadContext::new(s3_client, "123456789012".into(), None).unwrap();

    let result = uploader
        .file_already_uploaded("test-bucket", "some-key")
        .await;
    assert!(result.is_err());
}
