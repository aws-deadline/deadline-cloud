# Job Monitoring

## Overview

`job_monitoring.rs` provides `wait_for_job_completion` — a polling loop that
watches a job until it reaches a terminal state, then collects failed task
details.

## Terminal States

```
SUCCEEDED, FAILED, CANCELED, SUSPENDED, NOT_COMPATIBLE
```

## wait_for_job_completion

Polls `GetJob` in a loop with exponential backoff:
- Initial interval: 500ms
- Doubles each iteration
- Capped at `max_poll_interval × 1000` ms (default: 120s → 120,000ms)
- Timeout: if `timeout > 0` and elapsed exceeds it, returns `OperationTimedOut`

On each poll:
1. Call `api::get_job`
2. Extract `taskRunStatus` from response
3. Fire `status_callback` and/or `job_callback` with current status, elapsed time, timeout
4. If terminal state → collect failed tasks → return `JobCompletionResult`
5. Otherwise → sleep → double interval → loop

## Failed Task Collection

When the job reaches a non-SUCCEEDED terminal state, `collect_failed_tasks`:
1. Lists all steps for the job
2. For each step with `FAILED` count > 0, lists all tasks
3. Filters to tasks with `runStatus == "FAILED"`
4. Extracts session ID from `latestSessionActionId` by parsing the
   `sessionaction-{session_hash}-{action_id}` format → `session-{session_hash}`

Returns `Vec<FailedTask>` with step_id, task_id, step_name, parameters, session_id.

Note: for SUCCEEDED jobs, failed task collection is skipped (returns empty vec).
The CLI handles the case where SUCCEEDED jobs still have failed tasks by checking
the exit code logic separately.

## Callbacks

Two optional callbacks:
- `status_callback: Option<&dyn Fn(&str, f64, u64)>` — receives (status, elapsed_secs, timeout_secs)
- `job_callback: Option<&dyn Fn(&Value, f64, u64)>` — receives (full job JSON, elapsed_secs, timeout_secs)

The CLI uses `job_callback` to print the progress line with task counts.
`status_callback` exists for simpler consumers that only need the status string.
