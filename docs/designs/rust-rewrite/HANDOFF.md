# Handoff

Current in-flight work for the rust-rewrite. Read this at the start of
every session before consulting the Work Items table in `README.md`.

## Active Work Item

**#6 — Job monitoring & logs**

### Workflow Step

Step 0 (planning). The remaining scope has been analyzed but not yet
approved for implementation.

### What's Done

- `wait_for_job_completion` in `crates/deadline-client/src/job_monitoring.rs`
  — §12 cases 1-15 (polling, exponential backoff, timeout, failed task
  collection, session ID extraction from `latestSessionActionId`)
- `deadline job wait` CLI in `crates/deadline-cli/src/commands/job.rs`
  — §44 cases 22-24 (verbose/json output, exit codes 0-5, status
  updates via `\r` overwrite)

### What's Next

Two batches remain:

**Batch 1 — API layer (session/worker logs + supporting functions):**
- `get_session_logs` (§12 cases 16-30) — CloudWatch log retrieval,
  session auto-selection from job_id, queue-role credentials for DCM
  users, log group `/aws/deadline/{farm_id}/{queue_id}`
- `get_worker_logs` (§12 cases 31-35) — CloudWatch log retrieval,
  fleet-role credentials, log group `/aws/deadline/{farm_id}/{fleet_id}`
- New API functions needed in `api.rs`: `assume_fleet_role_for_read`,
  `list_session_actions`, `get_session_action`
- New dependency: `aws-sdk-cloudwatchlogs` (not yet in workspace
  `Cargo.toml`)

**Batch 2 — CLI layer:**
- `deadline job logs` (§44 cases 17-21) — `--session-id`,
  `--session-action-id`, `--limit`, `--start-time`, `--end-time`,
  `--next-token`, `--output verbose|json`,
  `--timestamp-format utc|local|relative`, deprecated `--timezone`
- `deadline job trace-schedule` (§44 cases 27-28) — fetches all
  sessions + session actions + steps/tasks, computes schedule
  statistics, optional Chrome trace format output via `--trace-format`
  and `--trace-file`

### Key Context

- Python source: `../deadline-cloud-python/src/deadline/client/api/_job_monitoring.py`
  has `get_session_logs` and `get_worker_logs`. The CLI is in
  `../deadline-cloud-python/src/deadline/client/cli/_groups/job_group.py`
  (lines 1171-1700).
- `get_session_logs` uses `get_queue_user_boto3_session` for DCM users
  (queue-role credentials). `get_worker_logs` uses
  `assume_fleet_role_for_read` (fleet-role credentials). These are
  different credential paths.
- Session auto-selection: prioritizes ongoing sessions (no `endedAt`),
  then most recently ended. Uses `list_sessions` paginator.
- `job logs` with `--session-action-id` derives session ID from the
  action ID, fetches the action to get time bounds, intersects with
  user-provided `--start-time`/`--end-time`.
- `job trace-schedule` is marked EXPERIMENTAL in Python. It fetches
  all sessions, all session actions per session, caches steps/tasks,
  and computes timing statistics. Chrome trace format output is
  optional.
- CloudWatch `ResourceNotFoundException` returns empty result (count=0),
  not an error.
- Log messages have trailing whitespace stripped.

### Open Questions

None currently.
