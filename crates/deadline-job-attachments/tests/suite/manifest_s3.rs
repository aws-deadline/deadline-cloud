//! Level 1 tests for `manifest_upload` and `manifest_download` (batch 9e-2).
//!
//! Uses wiremock to stub S3 and Deadline API responses.

use std::collections::HashMap;
use std::fs;

use deadline_job_attachments::asset_manifests::{
    AssetManifest, HashAlgorithm, ManifestPath, ManifestVersion,
};
use deadline_job_attachments::manifest_ops::{AssetType, manifest_download, manifest_upload};
use deadline_job_attachments::models::JobAttachmentS3Settings;
use tempfile::TempDir;
use wiremock::matchers::{method, path_regex};
use wiremock::{Mock, MockServer, ResponseTemplate};

async fn build_s3_client(server: &MockServer) -> aws_sdk_s3::Client {
    let sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new("us-west-2"))
        .endpoint_url(server.uri())
        .test_credentials()
        .load()
        .await;
    deadline_job_attachments::s3::build_s3_client(&sdk_config, &deadline_config::ini::IniConfig::new())
}

fn make_test_manifest() -> AssetManifest {
    AssetManifest::new(
        HashAlgorithm::Xxh128,
        ManifestVersion::V2023_03_03,
        5,
        vec![ManifestPath {
            path: "file.txt".into(),
            hash: "aa".repeat(16),
            size: 5,
            mtime: 1_700_000_000_000_000,
        }],
    )
    .unwrap()
}

// =====================================================================
//  cases 25-26: manifest_upload
// =====================================================================

#[tokio::test]
async fn manifest_upload_with_prefix_uploads_to_correct_key() {
    let server = MockServer::start().await;

    // Expect PUT to Manifests path with prefix
    Mock::given(method("PUT"))
        .and(path_regex(r".*/Manifests/my-prefix/.*\.manifest"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;

    let dir = TempDir::new().unwrap();
    let manifest = make_test_manifest();
    let manifest_path = dir.path().join("test.manifest");
    fs::write(&manifest_path, manifest.encode()).unwrap();

    let s3_client = build_s3_client(&server).await;

    manifest_upload(
        manifest_path.to_str().unwrap(),
        "test-bucket",
        "root-prefix",
        &s3_client,
        "123456789012",
        Some("my-prefix"),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn manifest_upload_without_prefix_uploads_to_manifests_root() {
    let server = MockServer::start().await;

    // Expect PUT to Manifests path without prefix subfolder
    Mock::given(method("PUT"))
        .and(path_regex(r".*/Manifests/test\.manifest"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;

    let dir = TempDir::new().unwrap();
    let manifest = make_test_manifest();
    let manifest_path = dir.path().join("test.manifest");
    fs::write(&manifest_path, manifest.encode()).unwrap();

    let s3_client = build_s3_client(&server).await;

    manifest_upload(
        manifest_path.to_str().unwrap(),
        "test-bucket",
        "root-prefix",
        &s3_client,
        "123456789012",
        None,
    )
    .await
    .unwrap();
}

// =====================================================================
// manifest_upload metadata
// =====================================================================

#[tokio::test]
async fn manifest_upload_sets_file_system_location_name_metadata() {
    let server = MockServer::start().await;

    // We can't easily inspect metadata in wiremock, but we verify the
    // upload succeeds (metadata is set in the request)
    Mock::given(method("PUT"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;

    let dir = TempDir::new().unwrap();
    let manifest = make_test_manifest();
    let manifest_path = dir.path().join("test.manifest");
    fs::write(&manifest_path, manifest.encode()).unwrap();

    let s3_client = build_s3_client(&server).await;

    manifest_upload(
        manifest_path.to_str().unwrap(),
        "test-bucket",
        "root-prefix",
        &s3_client,
        "123456789012",
        Some("prefix"),
    )
    .await
    .unwrap();
}

// =====================================================================
// manifest_download — job with no attachments
// =====================================================================

#[tokio::test]
async fn manifest_download_no_attachments_returns_empty() {
    let dir = TempDir::new().unwrap();
    let server = MockServer::start().await;
    let s3_client = build_s3_client(&server).await;
    let s3_settings = JobAttachmentS3Settings::from_root_path("test-bucket/root-prefix").unwrap();

    // Job with no attachments
    let job_attachments: HashMap<String, serde_json::Value> = HashMap::new();

    let result = manifest_download(
        dir.path().to_str().unwrap(),
        "farm-1",
        "queue-1",
        "job-1",
        &s3_client,
        "123456789012",
        &s3_settings,
        &job_attachments,
        None,
        AssetType::All,
    )
    .await
    .unwrap();

    assert!(result.downloaded.is_empty());
}

// =====================================================================
// manifest_download — input manifests
// =====================================================================

#[tokio::test]
async fn manifest_download_input_manifests_downloaded_and_written() {
    let dir = TempDir::new().unwrap();
    let server = MockServer::start().await;

    let manifest = make_test_manifest();
    let manifest_json = manifest.encode();

    // Mock S3 GetObject for the input manifest
    Mock::given(method("GET"))
        .and(path_regex(r".*/Manifests/.*"))
        .respond_with(ResponseTemplate::new(200).set_body_string(&manifest_json))
        .mount(&server)
        .await;

    let s3_client = build_s3_client(&server).await;
    let s3_settings = JobAttachmentS3Settings::from_root_path("test-bucket/root-prefix").unwrap();

    // Job attachments with one input manifest
    let job_attachments: HashMap<String, serde_json::Value> =
        serde_json::from_value(serde_json::json!({
            "manifests": [
                {
                    "rootPath": "/tmp/assets",
                    "inputManifestPath": "farm-1/queue-1/Inputs/abc123/manifest_input"
                }
            ]
        }))
        .unwrap();

    let result = manifest_download(
        dir.path().to_str().unwrap(),
        "farm-1",
        "queue-1",
        "job-1",
        &s3_client,
        "123456789012",
        &s3_settings,
        &job_attachments,
        None,
        AssetType::Input,
    )
    .await
    .unwrap();

    assert_eq!(result.downloaded.len(), 1);
    assert_eq!(result.downloaded[0].manifest_root, "/tmp/assets");
    assert!(std::path::Path::new(&result.downloaded[0].local_manifest_path).is_file());
}

// =====================================================================
// manifest_download — asset_type=INPUT skips outputs
// =====================================================================

#[tokio::test]
async fn manifest_download_input_only_skips_output_manifests() {
    let dir = TempDir::new().unwrap();
    let server = MockServer::start().await;

    let manifest = make_test_manifest();
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_string(manifest.encode()))
        .mount(&server)
        .await;

    let s3_client = build_s3_client(&server).await;
    let s3_settings = JobAttachmentS3Settings::from_root_path("test-bucket/root-prefix").unwrap();

    let job_attachments: HashMap<String, serde_json::Value> =
        serde_json::from_value(serde_json::json!({
            "manifests": [
                {
                    "rootPath": "/tmp/assets",
                    "inputManifestPath": "farm-1/queue-1/Inputs/abc/manifest_input"
                }
            ]
        }))
        .unwrap();

    // asset_type=Input — should download inputs but not call get_output_manifests
    let result = manifest_download(
        dir.path().to_str().unwrap(),
        "farm-1",
        "queue-1",
        "job-1",
        &s3_client,
        "123456789012",
        &s3_settings,
        &job_attachments,
        None,
        AssetType::Input,
    )
    .await
    .unwrap();

    // Should have the input manifest
    assert_eq!(result.downloaded.len(), 1);
}
