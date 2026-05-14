# attachments Module

Manages the data plane for job attachments — everything that moves files
between the user's machine and S3 content-addressed storage.

## What It Does

- Groups input/output files by storage profile location
- Hashes files and builds manifests (via `openjd-snapshots`)
- Uploads file data to S3 CAS at `{rootPrefix}/Data/{hash}.xxh128`
- Downloads file data from S3 CAS with conflict resolution
- Caches hashes and S3 existence checks to avoid redundant work
- Manages incremental download checkpoints for `queue sync-output`
- Applies cross-OS path mapping rules (via `openjd-expr`)

## Key Concepts

**Content-Addressed Storage:** Files are stored by hash, not by name.
Identical files across jobs are stored once. Deduplication is automatic.

**Independent credentials:** Owns its own S3/STS clients, separate from
the Deadline API client. Attachment operations use queue-scoped credentials
when the user is logged in via DCM.

**openjd-snapshots delegation:** Hashing, manifest encoding/decoding,
hash cache, and the S3 upload/download engine are all provided by
`openjd-snapshots`. This module owns the Deadline-specific orchestration
layer on top.

## Consumers

- `deadline-cli` — attachment/manifest commands
- `deadline-python-bindings` — submission flow
- `deadline-lib::bundle` — submission orchestration

## Document Index

| Document | Description |
|----------|-------------|
| [architecture.md](architecture.md) | Module layout, design decisions, S3 config, caches, path grouping, error mapping |
| [manifest-format.md](manifest-format.md) | Canonical JSON wire format, S3 storage layout, versioning |
| [incremental-download.md](incremental-download.md) | Checkpoint persistence for `queue sync-output` |

## Status

Fully implemented. Upload/download engine delegated to openjd-snapshots.

Gaps:
- VFS (virtual filesystem) — deferred (worker-agent scope)
- File permission management on download — deferred (worker-agent scope)

## Gotchas

- Manifest path entries use POSIX-style relative paths regardless of host OS.
- File mtime in manifests is microseconds since epoch, truncated from nanoseconds.
- The `partial_manifest_prefix` includes a random GUID per upload operation.
- `attachment_download` and `attachment_upload` accept pre-built S3 clients —
  the CLI layer builds those with appropriate credentials.
- Hash cache format changed from Python's `hashesV4` (datetime string mtime)
  to openjd's `hashesV5` (nanosecond u64 mtime). One-time cache invalidation
  on first use after switching from Python CLI.
