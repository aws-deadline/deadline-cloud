# Handoff

Current in-flight work. Read this at the start of every session before
consulting the Work Items table in `specs/progress.md`.

## Active Work Item

None — ready for next work item.

## Recently Completed — #19 Update Checker

New `update_checker` module in `deadline-api`. Fetches remote manifest,
compares versions via `semver`, returns structured result. Never panics.
Config opt-out via `settings.submitter_update_notification`. 23 Level 1
tests. Also fixed pre-existing gui-ffi test failures (13 tests) caused
by missing tokio runtime context in `make_stub`.

#20 (Batch get API helper) deferred — only consumer is `trace-schedule`
(AUDIT-041, EXPERIMENTAL) which doesn't exist in Rust.
