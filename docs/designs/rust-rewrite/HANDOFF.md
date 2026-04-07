# Handoff

Current in-flight work for the rust-rewrite. Read this at the start of
every session before consulting the Work Items table in `README.md`.

## Active Work Item

**#12 — Job action commands** and **#15 — Job requeue-tasks**

These are being done together as one batch since they're small,
self-contained CLI commands.

### Workflow Step

Step 1 (review existing implementation for improvements) — in progress.
Fixing behavior gaps found during Step 0 audit:
1. `job list` missing `suggest_resources_on_client_error` on API failure
2. `job list` and `job get` missing `estimatedTimeRemaining` computation
3. `job get` missing `estimatedTimeRemaining` line after YAML output

Then proceeding to Steps 2-7 for cancel/requeue implementation.

### What's Being Implemented

**`deadline job cancel`** (§44 cases 25-26):
- Calls `GetJob` for a summary, prints it, asks for confirmation,
  then calls `UpdateJob` with `targetTaskRunStatus`.
- Options: `--mark-as` (CANCELED|SUSPENDED|FAILED|SUCCEEDED, default
  CANCELED), `--yes` (skip confirmation).
- Confirmation skipped if `--yes` or `settings.auto_accept` is true.
- On decline: prints "Job not canceled." and exits 1.
- Filters `taskRunStatusCounts` to non-zero entries in the summary.

**`deadline job requeue-tasks`** (§44 cases 29-42):
- Calls `GetJob` for summary, iterates `ListSteps` → `ListTasks`,
  calls `UpdateTask(targetRunStatus=PENDING)` for matching tasks.
- Options: `--run-status` (repeatable, default FAILED+CANCELED+SUSPENDED),
  `--yes` (skip confirmation).
- Prints per-step progress with task details (param=value format).
- Uses adaptive retry config for `UpdateTask` calls.
- "No tasks to requeue." when count is 0.

### New API Functions Needed

- `update_job(farm_id, queue_id, job_id, target_task_run_status, config, telemetry)`
  — PATCH to `/farms/{farmId}/queues/{queueId}/jobs/{jobId}`
- `update_task(farm_id, queue_id, job_id, step_id, task_id, target_run_status, config, telemetry)`
  — PATCH to `/farms/{farmId}/queues/{queueId}/jobs/{jobId}/steps/{stepId}/tasks/{taskId}`

Both are simple `capture_send` calls, no pagination.

### Key Design Notes from Python Source

- `job cancel` prints the job summary YAML before asking confirmation.
  The summary uses fields: name, jobId, taskRunStatus,
  taskRunStatusCounts (non-zero only), startedAt, endedAt, createdBy,
  createdAt.
- `job cancel` confirmation message varies: "Are you sure you want to
  cancel this job?" vs "...cancel this job and mark its taskRunStatus
  as {mark_as}?" depending on whether mark_as is CANCELED or not.
- `job requeue-tasks` prints task parameters in `param=value (taskId)`
  format. Parameters use union type format: `{"Frame": {"int": "1"}}`
  → extract first value of inner dict.
- `job requeue-tasks` uses an adaptive retry client for UpdateTask.
  In Rust, the SDK's default retry is sufficient — no special config
  needed.
- Both commands use `click.confirm(default=None)` which requires
  explicit y/n. For Level 2 tests, pipe "n\n" to stdin for decline
  tests, or use `--yes` flag.

### Test Plan

**`job cancel` tests (Level 2):**
1. Happy path with `--yes` — prints summary, cancels, exits 0
2. Happy path with `--mark-as SUSPENDED --yes` — different message
3. Error: GetJob fails — prints error with suggestions, exits 1

**`job requeue-tasks` tests (Level 2):**
1. Happy path with `--yes` — FAILED tasks requeued, per-step output
2. No matching tasks — "No tasks to requeue.", exits 0
3. Custom `--run-status FAILED --run-status SUCCEEDED --yes`
4. Task with parameters — shows param=value format
5. Error: GetJob fails — prints error, exits 1

### Mock Helpers Needed

- `jobs::mock_update_job` — PATCH matcher
- `jobs::mock_update_task` — PATCH matcher

### What's NOT in scope

- `job search` as a separate command — Python doesn't have it either.
  The test spec cases 6-9 are about `job list` filter/sort which
  Python also doesn't support. Skip.
- `job download-output` — blocked on job attachments (#10).
- Interactive confirmation tests (piping stdin) — test with `--yes`
  flag only. Interactive decline can be a follow-up.

## Recently Completed

**#4 — Queue parameters** (marked ✅ Done)

- `get_queue_parameter_definitions` in `queue_parameters.rs`
- `deadline queue paramdefs` CLI command
- 7 Level 2 tests
- Verified byte-for-byte match with Python CLI output

**#6 — Job monitoring & logs** (marked ✅ Done)

- `deadline job wait` (14 tests), `deadline job logs` (10 tests)
- `get_worker_logs` library function (2 Level 1 tests)
