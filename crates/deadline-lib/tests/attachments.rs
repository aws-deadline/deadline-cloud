// Integration tests — unwrap/expect are the standard way to assert in tests.
// clippy's allow-unwrap-in-tests only covers #[test] fns, not helper functions
// in integration test crates. See specs/testing.md for details.
#![allow(
    clippy::unwrap_used,
    reason = "test code — unwrap is acceptable for test assertions"
)]
#[path = "attachments/api.rs"]
mod api;
#[path = "attachments/diff.rs"]
mod diff;
#[path = "attachments/download.rs"]
mod download;
#[path = "attachments/download_filters.rs"]
mod download_filters;
#[path = "attachments/download_perf.rs"]
mod download_perf;
#[path = "attachments/manifest_ops.rs"]
mod manifest_ops;
#[path = "attachments/manifest_s3.rs"]
mod manifest_s3;
#[path = "attachments/openjd_contract.rs"]
mod openjd_contract;
#[path = "attachments/upload_s3.rs"]
mod upload_s3;
