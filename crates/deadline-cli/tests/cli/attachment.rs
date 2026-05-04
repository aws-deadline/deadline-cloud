//! Level 2 tests for `deadline attachment` subcommands.

use deadline_test_server::TestHarness;
use insta_cmd::assert_cmd_snapshot;

// =====================================================================
// attachment download — no S3 root URI
// =====================================================================

#[tokio::test]
async fn attachment_download_no_s3_uri_exits_with_error() {
    let harness = TestHarness::new().await;
    // No --s3-root-uri and no config → error
    assert_cmd_snapshot!(harness.cmd(&[
        "attachment",
        "download",
        "--manifests",
        "/some/manifest.file",
    ]));
}

// =====================================================================
// attachment upload — missing required args
// =====================================================================

#[tokio::test]
async fn attachment_upload_missing_root_dirs_and_rules_exits_with_error() {
    let harness = TestHarness::new().await;
    // Neither --root-dirs nor --path-mapping-rules → error
    assert_cmd_snapshot!(harness.cmd(&[
        "attachment",
        "upload",
        "--manifests",
        "/some/manifest.file",
        "--s3-root-uri",
        "s3://bucket/prefix",
        "--profile",
        "test",
    ]));
}
