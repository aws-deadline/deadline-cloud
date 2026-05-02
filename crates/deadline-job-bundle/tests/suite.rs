// Integration tests — unwrap/expect are the standard way to assert in tests.
// clippy's allow-unwrap-in-tests only covers #[test] fns, not helper functions
// in integration test crates. See specs/testing.md for details.
#![allow(clippy::unwrap_used)]
#[path = "suite/history.rs"]
mod history;
#[path = "suite/loader.rs"]
mod loader;
#[path = "suite/param_apply_merge.rs"]
mod param_apply_merge;
#[path = "suite/param_read.rs"]
mod param_read;
#[path = "suite/param_validation.rs"]
mod param_validation;
#[path = "suite/param_value.rs"]
mod param_value;
#[path = "suite/submission.rs"]
mod submission;
