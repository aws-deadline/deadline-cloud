# job Commands

The largest command group. Manages job lifecycle: listing, inspection,
monitoring, log retrieval, cancellation, and task requeuing.

## Subcommands

| Subcommand | Status | Description |
|------------|--------|-------------|
| `job list` | ✅ | List jobs in a queue (paginated search) |
| `job get` | ✅ | Get job details by ID, or search by name |
| `job get-session` | ✅ | Get details of a specific session |
| `job list-sessions` | ✅ | List sessions for a job |
| `job list-steps` | ✅ | List steps for a job |
| `job list-tasks` | ✅ | List tasks for a step |
| `job search` | ✅ | Search jobs with filter/sort expressions |
| `job wait` | ✅ | Wait for a job to reach a terminal state |
| `job logs` | ✅ | Retrieve session logs from CloudWatch |
| `job cancel` | ✅ | Cancel a running job |
| `job requeue-tasks` | ✅ | Requeue failed/canceled/suspended tasks |
| `job download-output` | ✅ | Download job output attachments from S3 |

All accept `--profile`, `--farm-id`, `--queue-id`. Most also accept `--job-id`.

## `job list`

Options: `--page-size` (default 5), `--item-offset` (default 0).

Requires: farm_id, queue_id. Calls `SearchJobs` API (not `ListJobs`).

Output: summary line ("Displaying N of M Jobs starting at offset") followed
by YAML list. Each entry shows: name (or displayName, whichever field exists),
jobId, taskRunStatus, startedAt, endedAt, createdBy, createdAt,
estimatedTimeRemaining. The name field detection checks `name` first, falls
back to `displayName`.

On error, `suggest_resources_on_client_error` lists available jobs/queues.

## `job get`

Accepts an optional positional `search_term` and `--job-id`.

Resolution logic:
1. If `--job-id` provided → direct GetJob call (search_term ignored)
2. If positional arg matches `job-[0-9a-f]{32}` → treat as job ID
3. Otherwise → search by name (CONTAINS match)

**Direct mode:** Calls GetJob, prints full response via `cli_object_repr`,
then appends `estimatedTimeRemaining: {value}` on a separate line (computed
from task progress and elapsed time, or "N/A" if not computable).

**Search mode:** Calls SearchJobs with a `searchTermFilter` (matchType: CONTAINS).
- Exactly one result → shows full details (same as direct mode)
- Multiple results → shows summary list:
  ```
  Found 3 job(s) matching "render", showing most recent 3:

    My Render Job
      job-abc123  SUCCEEDED     2024-01-15 10:30:00 -0700
      Tasks: 50 succeeded, 2 failed

  To get details, run: deadline job get --job-id <job-id>
  ```
  Job names longer than 80 characters are truncated with `...` in the middle.
  Creation times are converted to local timezone.
- Zero results → "No jobs found matching \"{term}\""

### Estimated Time Remaining

Computed from `taskRunStatusCounts` and `startedAt`:
- completed = SUCCEEDED + FAILED + CANCELED
- in_progress = RUNNING + STARTING + ASSIGNED
- pending = PENDING + READY + SCHEDULED
- remaining_secs = (elapsed / completed) × (in_progress + pending)

Returns None if: no startedAt, no completed tasks, or no remaining tasks.
Duration formatted as: "N seconds", "N minute(s)", "N hour(s), M minute(s)".

## `job get-session`

Options: `--session-id` (required). Requires: farm_id, queue_id, job_id.

Calls `GetSession`. Full response via `cli_object_repr`.

## `job list-sessions`

Requires: farm_id, queue_id, job_id. Calls `ListSessions`.

Output: the `sessions` array from the response via `cli_object_repr`.

## `job list-steps`

Requires: farm_id, queue_id, job_id. Calls `ListSteps`.

Output: the `steps` array from the response via `cli_object_repr`.

## `job list-tasks`

Options: `--step-id` (required). Requires: farm_id, queue_id, job_id.

Calls `ListTasks`. Output: the `tasks` array from the response via `cli_object_repr`.

## `job wait`

Options: `--max-poll-interval` (default 120s), `--timeout` (default 0 = no timeout),
`--output verbose|json`.

Requires: farm_id, queue_id, job_id.

First calls GetJob to retrieve the job name for display. Then delegates to
`job_monitoring::wait_for_job_completion` which polls GetJob until a terminal
state is reached.

**Verbose mode progress (stderr):**
```
Waiting for job job-abc123 to complete...
Job Name: My Render Job
Current status: RUNNING (50/100 tasks succeeded, 3 workers running). [42.1s elapsed, 38.5s remaining]
```

The progress line updates in-place via `\r`. Shows running count (RUNNING +
ASSIGNED + STARTING), succeeded count, total count, elapsed time, and remaining
time (if `--timeout` > 0).

**JSON mode:** No progress output. Final result printed as pretty JSON.

### Exit Codes (public contract — scripts depend on these)

| Status | Exit Code |
|--------|-----------|
| SUCCEEDED (no failed tasks) | 0 |
| FAILED or SUCCEEDED with failed tasks | 2 |
| CANCELED | 3 |
| SUSPENDED or ARCHIVED | 4 |
| NOT_COMPATIBLE | 5 |
| Timeout | 1 |
| Other error | 2 |

On FAILED or SUCCEEDED-with-failures, prints failed task details (stepId,
taskId, stepName, sessionId) via `cli_object_repr`.

Terminal states: SUCCEEDED, FAILED, CANCELED, SUSPENDED, NOT_COMPATIBLE.

## `job logs`

Options: `--session-id` (optional), `--session-action-id` (optional),
`--limit` (default 100), `--start-time`, `--end-time`, `--next-token`,
`--output verbose|json`, `--timestamp-format utc|local|relative` (default utc).

Requires: farm_id, queue_id, job_id.

### `--session-action-id`

When provided, derives the session ID from the action ID format
`sessionaction-{uuid}-{number}` → `session-{uuid}`. Calls
`GetSessionAction` to get `startedAt`/`endedAt` timestamps and uses
them to scope the CloudWatch log query to that action's time window.

- Validated before any API calls (fast failure on bad format)
- If both `--session-id` and `--session-action-id` are given, validates
  they are consistent (error on mismatch)
- Displays "session action {id}" instead of "session {id}" in output
- Shows session action start, end, and duration in header

### Execution Flow

1. Validate `--session-action-id` format (if provided)
2. Validate `--session-id` / `--session-action-id` consistency
3. GetJob to retrieve job name
4. GetSessionAction for time bounds (if `--session-action-id`)
5. Call `log_retrieval::get_session_logs` (handles session auto-selection)
6. Resolve the actual session ID (explicit, derived, or auto-selected)
7. GetSession to retrieve `startedAt` for timestamp formatting
8. Build timestamp formatter (utc/local/relative)
9. Print auto-selection message (if applicable)
10. Print header: log group, job ID, job name, session action times
11. Print events or "No logs found"
12. Print pagination hint if more logs available

### Session Auto-Selection

When `--session-id` is omitted, the library selects automatically. The CLI
prints how the selection was made:
- "Using the only available session: session-xxx"
- "Using the latest session: session-xxx"

### Timestamp Formatting

- `utc` — RFC 3339 in UTC
- `local` — RFC 3339 in local timezone
- `relative` — time delta from session start time (e.g., `0:01:23.456789`)

For relative mode, the reference time comes from the session's `startedAt`.
The session details are always fetched after session resolution (whether
explicitly provided or auto-selected), so relative timestamps are correct
in both cases.

### Verbose Output Format

```
Retrieving logs for session session-xxx from log group /aws/deadline/{farm}/{queue}...
Job ID: job-abc123
Job Name: My Render Job
Logs relative to start time: 2024-01-15T10:30:00+00:00

[0:00:01] Worker started
[0:00:05] Running task Frame=1

Retrieved 2 log events.
More logs are available. Use --next-token "xxx" to retrieve the next page.
```

### Credential Scoping

Uses queue-scoped credentials via `get_session_logs` (which internally calls
`get_queue_scoped_config` for DCM users).

## `job cancel`

Options: `--mark-as` (default CANCELED), `--yes`.

Requires: farm_id, queue_id, job_id. The `--mark-as` value is uppercased
and validated against `[SUSPENDED, CANCELED, FAILED, SUCCEEDED]`. Invalid
values produce an error listing the valid choices (matching Python's
`click.Choice` behavior). Exit code is 1 (Python uses 2 for Click usage
errors — accepted difference since Rust validates post-parse).

Flow:
1. GetJob → print filtered summary (name, jobId, taskRunStatus, non-zero
   taskRunStatusCounts, startedAt, endedAt, createdBy, createdAt)
2. Confirmation prompt: "Are you sure you want to cancel this job?" (or
   "...and mark its taskRunStatus as {mark_as}?" if not CANCELED)
3. Unless `--yes` or `settings.auto_accept` is set
4. Declined → "Job not canceled." → exit 1
5. Prints "Canceling job..." (or "Canceling job and marking as {mark_as}...")
6. UpdateJob with target status

On GetJob error, `suggest_resources_on_client_error` lists available jobs.

## `job requeue-tasks`

Options: `--run-status` (repeatable), `--yes`.

Requires: farm_id, queue_id, job_id.

Default statuses when `--run-status` not provided: SUSPENDED, CANCELED, FAILED.
All `--run-status` values are uppercased and validated against
`[SUSPENDED, CANCELED, FAILED, SUCCEEDED, NOT_COMPATIBLE]`. Invalid values
produce an error listing the valid choices. Exit code is 1 (Python uses 2
— same accepted difference as `--mark-as`).

Flow:
1. GetJob → print job name and ID
2. Print non-zero taskRunStatusCounts (keys uppercased)
3. Print "Requeuing all tasks with run status among: CANCELED, FAILED, SUSPENDED"
   (sorted alphabetically)
4. Count tasks matching target statuses across all steps
5. If zero → "No tasks to requeue." → exit 0
6. Confirmation prompt with estimated count:
   "This action will requeue an estimated N total tasks (2 FAILED tasks, 1 CANCELED tasks)"
7. Iterate steps → iterate tasks → UpdateTask to PENDING for matching tasks
8. Per-step output:
   ```
   Step: Render (step-abc123)
     Requeuing an estimated 3 total tasks (2 FAILED tasks, 1 CANCELED tasks)...
       FAILED Frame=1 (task-001)
       FAILED Frame=2 (task-002)
       CANCELED Frame=3 (task-003)
   ```
9. Print "Requeued a total of N tasks."

Task parameter display extracts the first value from the union-typed
parameter object (e.g., `{"Frame": {"int": "1"}}` → `Frame=1`). Tasks
with no parameters show only the task ID.

On GetJob error, `suggest_resources_on_client_error` lists available jobs.

## `job search`

Options: `--filter-expressions` (JSON or file://), `--sort-expressions`
(JSON or file://), `--page-size` (default 5), `--item-offset` (default 0).

Requires: farm_id, queue_id.

Calls `SearchJobs` with caller-provided filter/sort expressions. Both
`--filter-expressions` and `--sort-expressions` accept inline JSON or
`file://path` (parsed by `parse_json_or_file_arg`). Same output format
as `job list`.

On error, `suggest_resources_on_client_error` lists available jobs/queues.

## `job download-output`

Options: `--step-id`, `--task-id`, `--conflict-resolution` (SKIP/OVERWRITE/CREATE_COPY),
`--yes`, `--output verbose|json`.

Requires: farm_id, queue_id, job_id. `--task-id` requires `--step-id`.

### Execution Flow

1. Validate `--task-id` requires `--step-id` (exit 2 if missing)
2. GetJob to retrieve job name and attachments
3. If `--step-id`: GetStep to retrieve step name
4. If `--task-id`: GetTask to retrieve task parameters and `latestSessionActionId`
5. Print start message
6. GetQueue for `jobAttachmentSettings` (S3 bucket + root prefix)
7. Build S3 client with queue-scoped credentials (`get_queue_scoped_config`:
   DCM users get queue role, non-DCM users use base credentials)
8. Create `OutputDownloader` → fetches output manifests from S3
9. If no output paths → print "no output" message and return
10. On Windows: check `LongPathsEnabled` registry key, warn if paths exceed 260 chars
11. Print path summary (files grouped by directory with sequence detection)
12. Resolve conflict resolution: CLI flag > config setting > default (CREATE_COPY)
13. Download with progress bar, print summary

### Start Message

- Job only: `Downloading output from Job 'X'`
- With step: `Downloading output from Job 'X' Step 'Y'`
- With task: `Downloading output from Job 'X' Step 'Y' Task {Frame=1}`
- Task with no params: `...Task {}`

### No Output Message

```
There are no output files available for download at this moment. Please
verify that the Job/Step/Task you are trying to download output from has
completed successfully.
```

### Download Summary

```
Download Summary:
    Downloaded 7 files totaling 700.91 KB.
    Total download time of 0.27198 seconds at 2.58 MB/s.
    Download locations (total file counts):
        /path/to/root (7 files)
```

### JSON Output Mode (`--output json`)

Uses JSON line protocol with `messageType` field:
- `{"messageType": "title", "value": "job name"}` — start
- `{"messageType": "summary", "value": "...", "fileCount": N, "files": [...]}` — completion
- `{"messageType": "error", "value": "..."}` — error (exit 1)

### Credential Scoping

Uses `get_queue_scoped_config` for the S3 client. DCM users get queue
role credentials via `AssumeQueueRoleForUser`. Non-DCM users use their
base AWS credentials. Same pattern as `attachment download`.

### Not Yet Implemented

- Interactive root-editing loop (users can only use `--yes` path for now)
- Cross-OS root path mismatch interactive prompt
- Conflict resolution interactive prompt (when neither `--conflict-resolution`
  nor `--yes` is provided)

## Differences from Python CLI

| Aspect | Python | Rust |
|--------|--------|------|
| `job list` | Uses `ListJobs` API | Uses `SearchJobs` API |
| `job get` with search | Not supported | Positional arg searches by name |
| Estimated time | Separate helper function | Inline in `print_job_details` |
| `job wait` progress | Callback-based | Closure passed to `wait_for_job_completion` |
| `job requeue-tasks` | `--step-id`, `--task-ids` filters | `--run-status` filter only |
| `job download-output` | Interactive root-editing loop, conflict prompt | `--yes` path only (interactive prompts deferred) |
