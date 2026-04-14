# deadline-models Architecture

## Crate Position in the Workspace

```
deadline-cli ──► deadline-models
deadline-api ──► deadline-models
deadline-job-bundle ──► deadline-models
deadline-job-attachments ──► deadline-models
deadline-gui-ffi ──► deadline-models
deadline-common ──► deadline-models
```

Leaf dependency — depends on nothing in the workspace. Every other crate
depends on it for shared types.

## Module Layout

```
src/
├── lib.rs              # Re-exports: errors, submitter_info, job_attachments,
│                       #   job_monitoring, path_format
├── errors.rs           # DeadlineError (6 variants), JobAttachmentsError (18 variants)
├── submitter_info.rs   # SubmitterInfo struct, YamlValue enum
├── job_attachments.rs  # Attachment-related types (PathFormat, ManifestProperties)
├── job_monitoring.rs   # Job monitoring types (JobLifecycleStatus)
└── path_format.rs      # PathFormat enum (POSIX, WINDOWS)
```

## Public Types

### errors.rs

`DeadlineError` — 6 variants, all carrying a `String` message:
- `OperationError` — generic, printed verbatim by CLI
- `OperationCanceled` — default: "Operation canceled"
- `OperationTimedOut` — default: "Operation timed out"
- `CreateJobWaiterCanceled` — default: "Operation canceled while waiting for CreateJob to finish"
- `UserInitiatedCancel` — default: "Operation canceled by user"
- `NonValidInput` — user input validation failure

Constructor helpers: `operation_canceled()`, `operation_timed_out()`,
`create_job_waiter_canceled()`, `user_initiated_cancel()`.

`JobAttachmentsError` — 18 variants covering S3 errors (with structured
`action/status_code/bucket/key/message` fields), transport errors (with
actionable guidance), and domain errors (missing settings, malformed
attachments, path escapes, VFS failures, hash algorithm mismatches).

### submitter_info.rs

`SubmitterInfo` — metadata about the submitting application. Fields:
`submitter_name` (required), `submitter_package_name`, `submitter_package_version`,
`host_application_name`, `host_application_version`, `additional_info` (nested
`HashMap<String, YamlValue>`).

`YamlValue` — recursive enum: `String`, `Int(i64)`, `Float(f64)`, `Bool`, `Null`,
`Map(HashMap)`, `List(Vec)`. Used for arbitrary nested metadata from DCC integrations.

### job_attachments.rs

Types shared between `deadline-job-attachments` and `deadline-job-bundle`:
- `FileConflictResolution` — enum: `CreateCopy`, `Overwrite`, `Skip`
- `FileSystemLocationType` — enum: `Shared`, `Local`
- `JobAttachmentsFileSystem` — enum: `Copied`, `Virtual`

### job_monitoring.rs

Types used by the job monitoring/wait logic in `deadline-api`:
- `FailedTask` — `{ step_id, task_id, step_name, parameters: Value, session_id: Option }`
- `JobCompletionResult` — `{ status, failed_tasks: Vec<FailedTask>, elapsed_time: f64 }`
- `LogEvent` — `{ timestamp: DateTime<Utc>, message, ingestion_time: Option, event_id: Option }`
- `SessionLogResult` — `{ events: Vec<LogEvent>, next_token, log_group, log_stream, count }`
- `WorkerLogResult` — `{ events, next_token, log_group, log_stream, worker_id, fleet_id, count }`

### path_format.rs

`PathFormat` — enum: `Windows`, `Posix`. Used for cross-platform path handling
in job attachments.

**Manual `Display` + `Error` impls instead of `thiserror`.** The error types
use hand-written `Display` implementations because `JobAttachmentsError::S3Client`
has structured formatting that doesn't fit `thiserror`'s `#[error(...)]` macro
cleanly. Consistency across both error enums was preferred over mixing approaches.

**`YamlValue` instead of `serde_yaml::Value`.** Avoids coupling the models crate
to serde_yaml. The type is simple enough that a custom enum is lighter than a
dependency.
