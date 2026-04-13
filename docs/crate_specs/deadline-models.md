# deadline-models

Shared data types and error types used across all crates. No I/O, no
business logic beyond construction and display.

## Role in the System

The leaf crate in the dependency graph — everything depends on it, it
depends on nothing (except `serde_json` and `chrono` for field types).
Defines the error contracts and shared domain types that cross crate
boundaries.

Consumers: every crate in the workspace.

## Key Concepts

**Two error hierarchies.** `DeadlineError` covers CLI-facing operations
(API calls, timeouts, cancellations). `JobAttachmentsError` covers the
attachment subsystem (S3, manifests, caches, VFS). They're separate
because the attachment subsystem has fundamentally different failure modes
(network transport, bucket permissions, file system issues) that need
structured fields rather than just a message string.

**Domain enums live here, not in their consuming crate.** Types like
`FileConflictResolution`, `PathFormat`, `JobAttachmentsFileSystem`, and
`FileSystemLocationType` are defined here so multiple crates can share
them without creating circular dependencies.

**`SubmitterInfo` carries DCC plugin metadata.** When a job is submitted
from a DCC plugin (Blender, Maya, etc.), this struct carries the
submitter identity and arbitrary nested metadata. The `YamlValue` enum
exists because this metadata can be arbitrarily nested maps/lists that
need to round-trip through YAML serialization.

## Behavior & Contracts

**`DeadlineError` display is the user-facing message.** The CLI error
handler calls `.to_string()` and prints it verbatim. This means the
message in each variant IS the user experience — it must be
human-readable and actionable.

**Cancel/timeout variants have default constructors.** Calling
`DeadlineError::operation_canceled()` gives you the standard message.
You can also construct with a custom message for context-specific
cancellations.

**`JobAttachmentsError::S3Client` is structured.** It carries action,
status code, bucket, key, and optional message — formatted into a
consistent pattern: `"Error {action} in bucket '{bucket}', Target key
or prefix: '{key}', HTTP Status Code: {status_code}"`. This format is
relied upon by error handling in the CLI layer.

**`JobAttachmentsError::S3BotoCore` appends credential/network guidance.**
The display impl always appends a multi-line help message about verifying
credentials and network. This is intentional — transport errors are the
most common user-facing issue and the guidance reduces support tickets.

**`PathFormat::host()` is compile-time.** Uses `cfg!(target_os)`, not
runtime detection. This means cross-compilation produces the correct
format for the target, not the build host.

## Design Decisions

**No trait hierarchy for errors.** Both error enums are standalone — no
shared `BaseError` trait. Each crate that needs to convert between them
uses `From` impls at the boundary. This keeps the error types simple and
avoids trait object overhead.

**`JobAttachmentsFileSystem` is a typed enum, not a string.** Invalid
values are rejected at deserialization time. This catches malformed API
responses or config values early rather than propagating garbage strings
through the system.

**`YamlValue` instead of `serde_yaml::Value`.** The submitter info needs
to serialize to YAML but also needs to be constructable without pulling
in `serde_yaml` as a dependency of this leaf crate. The custom enum
keeps `deadline-models` dependency-light.

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

## Status & Gaps

Fully implemented. The VFS-related error variants (`VfsExecutableMissing`,
`VfsFailedToMount`, `VfsOsUserNotSet`) exist but the VFS feature itself
is deferred.
