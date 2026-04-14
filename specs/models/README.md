# deadline-models Crate Specifications

Shared data types and error types used across the workspace. No I/O, no
business logic beyond construction and display. The lightest crate in the
workspace — exists to break circular dependencies between crates that need
to share types.

Consumers: every other crate in the workspace.

## Document Index

| Document | Description |
|----------|-------------|
| [architecture.md](architecture.md) | Module layout, public types, error enums, design decisions |

## Status

Fully implemented. The VFS-related error variants (`VfsExecutableMissing`,
`VfsFailedToMount`, `VfsOsUserNotSet`) exist but the VFS feature itself
is deferred.

## Gotchas & Constraints

- Adding a new `DeadlineError` variant changes the user-facing error
  contract. The CLI prints these verbatim — test the message reads well
  to a human.

- `JobAttachmentsError` variants are matched by the CLI for specific
  handling (e.g., `Cancelled` triggers a different exit path than
  `S3Client`). Adding variants may require updating match arms in
  `deadline-cli`.

- `FailedTask.parameters` is `serde_json::Value` (not typed). This is
  intentional — task parameters are user-defined and their schema varies
  per job template.

- `PathFormat::host()` is compile-time (`cfg!(target_os)`). Cross-compilation
  produces the correct format for the target, not the build host.
