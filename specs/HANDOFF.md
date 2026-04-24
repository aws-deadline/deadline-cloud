# Handoff

Current in-flight work. Read this at the start of every session before
consulting the Work Items table in `specs/progress.md`.

## Active Work Item

None — ready for next work item.

## Recently Completed — #20 + AUDIT-041: Batch get API + trace-schedule

SDK upgrade (`aws-sdk-deadline` 1.94.0 → 1.98.0) to get `BatchGetStep`
and `BatchGetTask` APIs. New `batch_get_steps_page` and
`batch_get_tasks_page` functions in `api.rs`. New `deadline job
trace-schedule` CLI command with batch-get chunking/retry, Chrome trace
format output, and summary statistics. 12 Level 2 tests, zero-diff CLI
comparison against Python on real API. Closes AUDIT-041.
