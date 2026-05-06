// Integration tests — unwrap/expect are the standard way to assert in tests.
// clippy's allow-unwrap-in-tests only covers #[test] fns, not helper functions
// in integration test crates. See specs/testing.md for details.
#![allow(clippy::unwrap_used)]
#[path = "cli/attachment.rs"]
mod attachment;
#[path = "cli/auth.rs"]
mod auth;
#[path = "cli/bundle.rs"]
mod bundle;
#[path = "cli/bundle_hooks.rs"]
mod bundle_hooks;
#[path = "cli/common.rs"]
mod common;
#[path = "cli/config.rs"]
mod config;
#[path = "cli/credential_scoping.rs"]
mod credential_scoping;
#[path = "cli/dcm.rs"]
mod dcm;
#[path = "cli/deadlinew.rs"]
mod deadlinew;
#[path = "cli/farm.rs"]
mod farm;
#[path = "cli/fleet.rs"]
mod fleet;
#[path = "cli/gui.rs"]
mod gui;
#[path = "cli/handle_web_url.rs"]
mod handle_web_url;
#[path = "cli/job.rs"]
mod job;
#[path = "cli/job_actions.rs"]
mod job_actions;
#[path = "cli/job_download.rs"]
mod job_download;
#[path = "cli/job_logs.rs"]
mod job_logs;
#[path = "cli/job_wait.rs"]
mod job_wait;
#[path = "cli/manifest.rs"]
mod manifest;
#[path = "cli/mcp.rs"]
mod mcp;
#[path = "cli/queue.rs"]
mod queue;
#[path = "cli/queue_paramdefs.rs"]
mod queue_paramdefs;
#[path = "cli/queue_resources.rs"]
mod queue_resources;
#[path = "cli/queue_sync_output.rs"]
mod queue_sync_output;
#[path = "cli/root.rs"]
mod root;
#[path = "cli/smoke.rs"]
mod smoke;
#[path = "cli/suggest.rs"]
mod suggest;
#[path = "cli/telemetry.rs"]
mod telemetry;
#[path = "cli/telemetry_parity.rs"]
mod telemetry_parity;
#[path = "cli/trace_schedule.rs"]
mod trace_schedule;
#[path = "cli/worker.rs"]
mod worker;
