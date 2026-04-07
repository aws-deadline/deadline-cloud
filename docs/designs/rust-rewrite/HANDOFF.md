# Handoff

Current in-flight work for the rust-rewrite. Read this at the start of
every session before consulting the Work Items table in `README.md`.

## Active Work Item

None. Consult the Work Items table in `README.md` for the next item.

## Recently Completed

**#12 — Job cancel** and **#15 — Job requeue-tasks** (marked ✅ Done)

- `deadline job cancel` with `--mark-as` and `--yes`
- `deadline job requeue-tasks` with `--run-status` and `--yes`
- `update_job` and `update_task` API functions in `deadline-client`
- 9 Level 2 tests, verified byte-for-byte against Python CLI on real API
- Also fixed: `job list` error suggestions, `job get` estimatedTimeRemaining
- Refactored to use `apply_cli_options_to_config`, extracted helpers

**#4 — Queue parameters** (marked ✅ Done)

- `get_queue_parameter_definitions` in `queue_parameters.rs`
- `deadline queue paramdefs` CLI command
- 7 Level 2 tests
- Verified byte-for-byte match with Python CLI output

**#6 — Job monitoring & logs** (marked ✅ Done)

- `deadline job wait` (14 tests), `deadline job logs` (10 tests)
- `get_worker_logs` library function (2 Level 1 tests)
