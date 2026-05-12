// Integration tests — unwrap/expect are the standard way to assert in tests.
// clippy's allow-unwrap-in-tests only covers #[test] fns, not helper functions
// in integration test crates. See specs/testing.md for details.
#![allow(clippy::unwrap_used)]
#[path = "suite/api.rs"]
mod api;
#[path = "suite/diff.rs"]
mod diff;
#[path = "suite/download.rs"]
mod download;
#[path = "suite/download_filters.rs"]
mod download_filters;
#[path = "suite/manifest_ops.rs"]
mod manifest_ops;
#[path = "suite/manifest_s3.rs"]
mod manifest_s3;
#[path = "suite/upload_s3.rs"]
mod upload_s3;
