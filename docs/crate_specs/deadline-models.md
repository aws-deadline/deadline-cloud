# deadline-models

Shared data types and error types used across crates. No I/O, no business
logic beyond construction and display.

## Error Types

### `DeadlineError`

CLI-facing errors. The CLI error handler prints the message verbatim.

Variants: `OperationError`, `OperationCanceled`, `OperationTimedOut`,
`CreateJobWaiterCanceled`, `UserInitiatedCancel`, `NonValidInput`.

Cancel/timeout variants have default messages via constructor functions.

### `JobAttachmentsError`

Errors from the job attachments subsystem. Covers S3 operations, manifest
parsing, asset validation, VFS operations.

## Data Types

### `job_monitoring`

Result types for job monitoring operations:

- `FailedTask` — a task that failed during job execution. Contains
  `step_id`, `task_id`, `step_name`, `parameters` (as `serde_json::Value`),
  and optional `session_id`.
- `JobCompletionResult` — result of waiting for a job to complete.
  Contains `status` (terminal state string), `failed_tasks` list, and
  `elapsed_time` in seconds.

## Dependencies

- `serde_json` — `Value` type used in `FailedTask.parameters`
