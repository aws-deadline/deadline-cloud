// Integration tests — unwrap/expect are the standard way to assert in tests.
// clippy's allow-unwrap-in-tests only covers #[test] fns, not helper functions
// in integration test crates. See specs/testing.md for details.
#![allow(clippy::unwrap_used)]
#[path = "bundle/history.rs"]
mod history;
#[path = "bundle/hooks.rs"]
mod hooks;
#[path = "bundle/loader.rs"]
mod loader;
#[path = "bundle/param_apply_merge.rs"]
mod param_apply_merge;
#[path = "bundle/param_read.rs"]
mod param_read;
#[path = "bundle/param_validation.rs"]
mod param_validation;
#[path = "bundle/param_value.rs"]
mod param_value;
#[path = "bundle/submission.rs"]
mod submission;
