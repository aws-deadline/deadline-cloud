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

## Dependencies

None.
