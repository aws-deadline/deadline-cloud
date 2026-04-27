# Job Monitoring

## Overview

Job monitoring provides a polling loop that watches a submitted job until
it reaches a terminal state, then collects details about any failed tasks.
Used by `bundle submit` (with `--wait`) and `job wait`.

## Terminal States

A job is considered complete when its status is one of:
`SUCCEEDED`, `FAILED`, `CANCELED`, `SUSPENDED`, or `NOT_COMPATIBLE`.

## Polling Behavior

The monitor polls GetJob in a loop with exponential backoff:
- Initial interval: 500ms
- Doubles each iteration
- Capped at the configured max poll interval (default 120s)
- If a timeout is set and elapsed time exceeds it, returns a timeout error

On each poll, the monitor fires optional callbacks with the current status,
elapsed time, and timeout. The CLI uses these to print a progress line
with task counts.

## Failed Task Collection

When a job reaches a non-SUCCEEDED terminal state, the monitor collects
details about failed tasks:

1. Lists all steps for the job
2. For each step with failed tasks, lists all tasks
3. Filters to tasks with `runStatus == "FAILED"`
4. Extracts the session ID from the session action ID format
   (`sessionaction-{hash}-{index}` → `session-{hash}`)

For SUCCEEDED jobs, failed task collection is skipped.

## Callbacks

Two optional callbacks allow consumers to react to status changes:

- **Status callback** — receives the status string, elapsed seconds, and
  timeout. Used by simple consumers that only need the status.
- **Job callback** — receives the full job JSON response, elapsed seconds,
  and timeout. Used by the CLI to print detailed progress with task counts.
