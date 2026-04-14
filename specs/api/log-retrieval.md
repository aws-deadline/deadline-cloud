# Log Retrieval

## Overview

`log_retrieval.rs` retrieves session and worker logs from CloudWatch Logs.
Separate from the main API module because it talks to a different AWS service
and has different credential requirements.

## Session Logs (`get_session_logs`)

Retrieves logs from CloudWatch log group `/aws/deadline/{farm_id}/{queue_id}`,
log stream = session ID.

Uses queue-scoped credentials via `get_queue_scoped_config` — DCM users get
queue role credentials, non-DCM users use base credentials.

### Session Auto-Selection

When no session ID is provided, `auto_select_session` resolves one:

1. Lists all sessions for the job via `api::list_sessions`
2. If zero sessions → error: "No sessions found for job {job_id}"
3. If exactly one → `SessionAutoSelect::OnlySession(id)`
4. If multiple:
   - Filter to ongoing sessions (no `endedAt` field)
   - If any ongoing → pick the most recently started (by `startedAt` string sort)
   - If none ongoing → pick the most recently ended (by `endedAt` string sort)
   - Returns `SessionAutoSelect::LatestSession(id)`

### Error Handling

- `ResourceNotFoundException` from CloudWatch → returns empty result (not an error).
  The log group/stream may not exist yet if the session hasn't produced output.
- Other CloudWatch errors → propagated as `DeadlineError::OperationError`

### Pagination

Returns `next_forward_token` from CloudWatch. The CLI prints a message telling
the user to pass `--next-token` for the next page.

## Worker Logs (`get_worker_logs`)

Retrieves logs from CloudWatch log group `/aws/deadline/{farm_id}/{fleet_id}`,
log stream = worker ID.

Uses fleet-scoped credentials via `get_fleet_scoped_config`:
- DCM user → calls `AssumeFleetRoleForRead` API, builds one-shot `SdkConfig`
  from returned credentials (not cached)
- Non-DCM user → uses base `SdkConfig`

Same error handling as session logs: `ResourceNotFoundException` → empty result.

## CloudWatch Client Construction

Built from the scoped `SdkConfig`. Respects `AWS_ENDPOINT_URL_CLOUDWATCHLOGS`
env var for test endpoint override (applied in `logs_client()` builder).

## Log Event Parsing

CloudWatch `OutputLogEvent` fields mapped to `LogEvent`:
- `timestamp` (milliseconds since epoch) → `DateTime<Utc>`
- `message` → trimmed of trailing whitespace
- `ingestion_time` → `Option<DateTime<Utc>>`
- `event_id` → always `None` (CloudWatch GetLogEvents doesn't return event IDs)
