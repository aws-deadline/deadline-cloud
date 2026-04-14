# API — Job Lifecycle

> Part of [AWS Deadline Cloud Client — Behavioral Test Specification](index.md)
>
> This document describes observable behavior through public interfaces.
> Error descriptions use category labels (e.g., "returns error") rather than
> language-specific exception types. The Rust implementation should produce
> equivalent observable outcomes, not necessarily identical internal mechanics.

---

## Section 11: API — submit job bundle

> **Rust crate:** `deadline-api` · **Module:** `api`
>
> **Logic under test:** The full job submission pipeline: read template, merge parameters,
> resolve PATH defaults, validate symlink containment, hash and upload attachments,
> call `CreateJob`, and poll for completion. Includes known-path filtering (TRIE-based),
> interactive confirmation for unknown paths, and debug snapshot mode.

### `create_job_from_job_bundle(job_bundle_dir, job_parameters, ...) -> optional string`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 1 | Happy path | Submit a valid job bundle with template.yaml, no attachments | Returns job ID string; prints "Submitted job bundle" message | |
| 2 | Happy path | Submit a valid job bundle with template.json | Returns job ID; template type is `"JSON"` in CreateJob args | |
| 3 | Happy path | Submit with `name` parameter to override template name | Template object's `name` field is replaced before submission | |
| 4 | Happy path | Submit with explicit `priority` parameter | `priority` in CreateJob args is set to the provided value | |
| 5 | Happy path | Submit with `max_failed_tasks_count`, `max_retries_per_task`, `max_worker_count` | Each maps to corresponding CreateJob arg (`maxFailedTasksCount`, etc.) | |
| 6 | Happy path | Submit with `target_task_run_status="SUSPENDED"` | Job is created in SUSPENDED state | |
| 7 | Happy path | Default priority is 50 when not explicitly provided | `create_job_args["priority"]` defaults to 50 | |
| 8 | Happy path | `submitter_name` is provided | `session_context["submitter-name"]` is set; telemetry records it | |
| 9 | Happy path | `submitter_name` is not provided | Defaults to `"Custom"` | |
| 10 | Config interaction | `job_attachments_file_system` is not provided | Falls back to `get_setting("defaults.job_attachments_file_system")` | |
| 11 | Config interaction | `force_s3_check` is not provided | Falls back to `str2bool(get_setting("settings.force_s3_check"))` | |
| 12 | Config interaction | No config override provided, job succeeds | `defaults.job_id` is updated to the new job ID | Only when using default config |
| 13 | Config interaction | Explicit config override is provided, job succeeds | `defaults.job_id` is NOT updated | |
| 14 | Config interaction | `storage_profile_id` is set in config | `storageProfileId` is added to CreateJob args; storage profile is fetched | |
| 15 | Config interaction | `storage_profile_id` is empty | No `storageProfileId` in CreateJob args; no storage profile fetched | |

### Symlink containment validation

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 16 | Error handling | Job bundle directory contains a symlink pointing outside the bundle | Returns error before any API calls | `validate_directory_symlink_containment` |

### Job attachments handling

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 17 | Happy path | Bundle has asset_references with input files, queue has `jobAttachmentSettings` | Files are hashed, uploaded to S3, `attachments` added to CreateJob args | |
| 18 | Happy path | Bundle has no asset_references | No hashing or upload; callbacks called once with 100% progress | |
| 19 | Happy path | Queue has no `jobAttachmentSettings` | No hashing or upload even if asset_references exist | |
| 20 | Happy path | Input directory contains files | Files are walked recursively and added to `input_filenames` | |
| 21 | Happy path | Input directory is empty | Directory is moved to `referenced_paths` (logged as info) | Manifest spec can't represent empty dirs |
| 22 | Error handling | Input directory does not exist AND `require_paths_exist=true` | Returns error listing missing directories | |
| 23 | Happy path | Input directory does not exist AND `require_paths_exist=false` | Directory is moved to `referenced_paths` with a warning | |
| 24 | Happy path | `debug_snapshot_dir` is provided | No job is submitted; snapshot files are saved; returns none | |

### Interactive confirmation for unknown paths

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 25 | Interactive vs scripted | Files are outside known paths, `interactive_confirmation_callback` is not provided | Prints warning and returns canceled error | No user prompt possible |
| 26 | Interactive vs scripted | Files are outside known paths, `auto_accept=true`, not from GUI | Prints warning and returns canceled error | Auto-accept doesn't accept unknown paths |
| 27 | Interactive vs scripted | Files are outside known paths, `auto_accept=true`, from GUI | Presents prompt via callback; returns user-canceled error if user declines | GUI gets a prompt even with auto_accept |
| 28 | Interactive vs scripted | Files are outside known paths, user confirms via callback | Submission proceeds normally | |
| 29 | Interactive vs scripted | Files are outside known paths, user declines via callback | Returns user-initiated cancel error | |
| 30 | Interactive vs scripted | All files are within known paths | No warning; submission proceeds without prompt | `default_prompt_response` is true |
| 31 | Config interaction | `settings.known_asset_paths` has paths configured | Those paths are added to known paths list | Split by OS-specific path separator |
| 32 | Config interaction | Storage profile has LOCAL file system locations | Those paths are added to known paths list | |
| 33 | Happy path | Job bundle directory itself is always added to known paths | Bundle dir is included in known paths | |
| 34 | Happy path | PATH-type job parameters with values from `job_parameters` arg are added to known paths | Parameter values from the caller are trusted | |
| 35 | Happy path | PATH-type parameter with `objectType=FILE` | Parent directory (not file path) is added to known paths | |

### Known path filtering

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 36 | Happy path | `/mnt/prod` and `/mnt/prod/project` are both in known paths | `/mnt/prod/project` is filtered out (redundant) | TRIE-based prefix filtering |
| 37 | Boundary values | Single known path | Returned as-is | |

### `wait_for_create_job_to_complete(farm_id, queue_id, job_id, deadline_api, continue_callback) -> (bool, string)`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 38 | Happy path | Job transitions from `CREATE_IN_PROGRESS` to `CREATE_COMPLETE` | Returns `(true, lifecycle_status_message)` | Polls with exponential backoff |
| 39 | Error handling | Job transitions to `CREATE_FAILED` | Returns `(false, lifecycle_status_message)` | |
| 40 | Concurrency/cancellation | `continue_callback` returns false during polling | Returns waiter-canceled error | |
| 41 | Error handling | Job stays in `CREATE_IN_PROGRESS` for 300 seconds | Returns timeout error | |
| 42 | Happy path | Polling uses exponential backoff from 0.3s to max 5s | Delay doubles each iteration, capped at 5 seconds | |

---

## Section 12: API — job monitoring & logs

> **Rust crate:** `deadline-api` · **Module:** `api`
>
> **Logic under test:** Polling a job until it reaches a terminal state (SUCCEEDED,
> FAILED, CANCELED, SUSPENDED, NOT_COMPATIBLE) with exponential backoff. Collecting
> failed task details. Retrieving CloudWatch logs for sessions and workers, using
> queue-role or fleet-role credentials when logged in via DCM.

### `wait_for_job_completion(farm_id, queue_id, job_id, ...) -> JobCompletionResult`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 1 | Happy path | Job completes with `SUCCEEDED` status | Returns result with `status="SUCCEEDED"`, empty `failed_tasks`, and elapsed time | |
| 2 | Happy path | Job completes with `FAILED` status | Returns result with `status="FAILED"` and populated `failed_tasks` list | |
| 3 | Happy path | Job completes with `CANCELED` status | Returns result with `status="CANCELED"` | |
| 4 | Happy path | Job completes with `SUSPENDED` status | Returns result with `status="SUSPENDED"` | |
| 5 | Happy path | Job completes with `NOT_COMPATIBLE` status | Returns result with `status="NOT_COMPATIBLE"` | |
| 6 | Happy path | Polling uses exponential backoff from 0.5s to `max_poll_interval` | Interval doubles each iteration, capped at `max_poll_interval` (default 120s) | |
| 7 | Error handling | `timeout` is set and exceeded | Returns timed-out error with elapsed time in message | |
| 8 | Happy path | `timeout=0` (default) | No timeout; polls indefinitely until terminal state | |
| 9 | Happy path | `status_callback` is provided | Called each poll with `(status, elapsed, timeout)` | |
| 10 | Happy path | `job_callback` is provided | Called each poll with `(job, elapsed, timeout)` | |
| 11 | Error handling | `get_job` raises an AWS service error | Returns wrapped error | |
| 12 | Happy path | Failed job has steps with FAILED tasks | `failed_tasks` contains entries with step_id, task_id, step_name, parameters, session_id | |
| 13 | Happy path | `latestSessionActionId` format `sessionaction-{id}-{num}` | `session_id` is extracted as `session-{id}` | |
| 14 | Boundary values | `latestSessionActionId` is absent | `session_id` is none in the failed task entry | |
| 15 | Happy path | Step has `taskRunStatusCounts.FAILED = 0` | Tasks for that step are not queried (optimization) | |

### `get_session_logs(farm_id, queue_id, session_id?, job_id?, ...) -> SessionLogResult`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 16 | Happy path | Provide `session_id` directly | Returns result with events from CloudWatch | |
| 17 | Happy path | Provide `job_id` without `session_id`, job has ongoing sessions | Auto-selects the most recently started ongoing session | |
| 18 | Happy path | Provide `job_id` without `session_id`, no ongoing sessions | Auto-selects the most recently ended session | |
| 19 | Error handling | Neither `session_id` nor `job_id` provided | Returns error "Either session_id or job_id must be provided" | |
| 20 | Error handling | `job_id` provided but no sessions found | Returns error "No sessions found for job" | |
| 21 | Happy path | Log group is `/aws/deadline/{farm_id}/{queue_id}`, stream is `session_id` | Correct CloudWatch log group/stream pattern | |
| 22 | Auth/credential states | User is logged in via DCM (has user_id) | Uses queue user credentials for CloudWatch access | |
| 23 | Auth/credential states | User is NOT logged in via DCM | Uses direct client for CloudWatch | |
| 24 | Happy path | `start_time` and `end_time` provided as datetime values | Converted to milliseconds since epoch for CloudWatch API | |
| 25 | Happy path | `next_token` provided | Passed to CloudWatch for pagination | |
| 26 | Error handling | Log group or stream does not exist (resource not found) | Returns empty result with `count=0` | Not an error |
| 27 | Error handling | Other CloudWatch error | Returns wrapped error | |
| 28 | Happy path | Log events have `ingestionTime` | Converted to datetime in result | |
| 29 | Happy path | Log events lack `ingestionTime` | Ingestion time is none in result | |
| 30 | Happy path | Log message has trailing whitespace | Trailing whitespace is stripped | |

### `get_worker_logs(farm_id, fleet_id, worker_id, ...) -> WorkerLogResult`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 31 | Happy path | Valid fleet_id and worker_id | Returns result with events from `/aws/deadline/{farm_id}/{fleet_id}` log group | |
| 32 | Auth/credential states | User is logged in via DCM | Uses `assume_fleet_role_for_read` credentials for CloudWatch | Different from session logs which use queue role |
| 33 | Auth/credential states | User is NOT logged in via DCM | Uses direct client | |
| 34 | Error handling | Log group/stream not found | Returns empty result with `count=0` | |
| 35 | Error handling | `assume_fleet_role_for_read` fails | Returns error "Failed to get fleet credentials" | |

---

## Section 13: API — diagnostics (get/list/search)

> **Rust crate:** `deadline-api` · **Module:** `api`
>
> **Logic under test:** Thin wrappers around Deadline API calls for retrieving job,
> session, step, and task details. `search_jobs` builds filter expressions from
> optional status and name filters. Falls back to config defaults for farm_id and
> queue_ids when not provided.

### `get_job(farm_id, queue_id, job_id, config?) -> dict`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 1 | Happy path | Valid IDs | Returns full job details from the API | |
| 2 | Error handling | Invalid job_id | Propagates AWS service error | |

### `get_session(farm_id, queue_id, job_id, session_id, config?) -> dict`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 3 | Happy path | Valid IDs | Returns session details | |
| 4 | Error handling | Invalid session_id | Propagates AWS service error | |

### `list_sessions(farm_id, queue_id, job_id, max_results?, config?) -> dict`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 5 | Happy path | Job has sessions | Returns `{"sessions": [...]}` with all sessions (paginated) | |
| 6 | Happy path | `max_results` is provided | Passed as `maxResults` to API | |
| 7 | Happy path | `max_results` is not provided | `maxResults` is not included in API call | |
| 8 | Pagination/batching | Multiple pages | All sessions concatenated | |

### `list_steps(farm_id, queue_id, job_id, max_results?, config?) -> dict`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 9 | Happy path | Job has steps | Returns `{"steps": [...]}` | |
| 10 | Pagination/batching | Multiple pages | All steps concatenated | |

### `list_tasks(farm_id, queue_id, job_id, step_id, max_results?, config?) -> dict`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 11 | Happy path | Step has tasks | Returns `{"tasks": [...]}` | |
| 12 | Pagination/batching | Multiple pages | All tasks concatenated | |

### `search_jobs(farm_id?, queue_ids?, task_run_status?, name_contains?, page_size?, item_offset?, config?) -> dict`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 13 | Happy path | Search with `task_run_status="FAILED"` | Returns jobs filtered by FAILED status | Uses `stringFilter` with `TASK_RUN_STATUS` |
| 14 | Happy path | Search with `name_contains="render"` | Returns jobs matching name substring | Uses `searchTermFilter` |
| 15 | Happy path | Both filters provided | Combined with AND operator | |
| 16 | Happy path | No filters provided | Returns all jobs (no `filterExpressions` in request) | |
| 17 | Config interaction | `farm_id` is not provided | Falls back to `get_setting("defaults.farm_id")` | |
| 18 | Config interaction | `queue_ids` is not provided | Falls back to default queue_id from config (wrapped in list) | |
| 19 | Error handling | `farm_id` is not provided and not in config | Returns error "farm_id is required" | |
| 20 | Error handling | `queue_ids` is not provided and not in config | Returns error "queue_ids is required" | |

---

## Section 14: API — telemetry

> **Rust crate:** `deadline-api` · **Module:** `telemetry`
>
> **Logic under test:** Telemetry client that queues events and sends them asynchronously
> via HTTP to a management-prefixed endpoint. Supports opt-out via env var or config.
> Retries on 429/500 with exponential backoff and jitter. Silently drops events when
> not initialized, opted out, or queue is full.

### `TelemetryClient.new(package_name, package_ver, config?)`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 1 | Happy path | Create with `"deadline-cloud-library"` package name | Common details do NOT include `deadline-cloud-version` | Same package, no supplementary info |
| 2 | Happy path | Create with different package name (e.g., `"deadline-cloud-for-blender"`) | Common details include `deadline-cloud-version` | Supplementary info for non-library packages |
| 3 | Happy path | `package_ver` is `"1.2.3.4.5"` | Truncated to `"1.2.3"` (first 3 components) | |
| 4 | Happy path | `telemetry.identifier` is a valid UUID4 in config | Uses the existing identifier | |
| 5 | Happy path | `telemetry.identifier` is empty or invalid | Generates a new UUID4 and saves it to config | |

### `TelemetryClient.set_opt_out(config?)`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 6 | Config interaction | `DEADLINE_CLOUD_TELEMETRY_OPT_OUT` env var is `"true"` | `telemetry_opted_out` is true | Env var supersedes config |
| 7 | Config interaction | `DEADLINE_CLOUD_TELEMETRY_OPT_OUT` env var is `"false"` | `telemetry_opted_out` is false | |
| 8 | Config interaction | Env var not set, `telemetry.opt_out` config is `"true"` | `telemetry_opted_out` is true | |
| 9 | Config interaction | Env var not set, `telemetry.opt_out` config is `"false"` (default) | `telemetry_opted_out` is false | |
| 10 | Config interaction | Env var is set to empty string | Falls through to config setting | Empty string is falsy |

### `TelemetryClient.initialize(config?)`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 11 | Happy path | Telemetry not opted out, AWS SDK configured | `_initialized` is true; background send thread started | |
| 12 | Happy path | Telemetry opted out | Returns immediately; `_initialized` remains false | |
| 13 | Error handling | AWS SDK not configured (error during init) | Silently fails; `_initialized` remains false | |
| 14 | Happy path | User has `user_id` from DCM | `user_id` added to system metadata | |
| 15 | Happy path | User has `monitor_id` from DCM | `monitor_id` added to system metadata | |
| 16 | Happy path | Endpoint URL has `management.` prefix inserted after `https://` | Telemetry endpoint is correctly prefixed | |

### `TelemetryClient.put_telemetry_record(event)`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 17 | Happy path | Client is initialized and not opted out | Event is added to the queue | |
| 18 | Happy path | Client is not initialized | Event is silently dropped | |
| 19 | Happy path | Client is opted out | Event is silently dropped | |
| 20 | Boundary values | Event queue is full (25 items) | Event is silently dropped (no error) | Queue full condition caught |

### `TelemetryClient.send_request(req)`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 21 | Happy path | Request succeeds on first attempt | Returns normally | |
| 22 | Error handling | HTTP 429 (throttled) then succeeds | Retries with exponential backoff and jitter | |
| 23 | Error handling | HTTP 500 then succeeds | Retries with exponential backoff | |
| 24 | Error handling | 4 consecutive 429/500 errors | Returns error "Max retries reached" | MAX_RETRY_ATTEMPTS = 4 |

---
