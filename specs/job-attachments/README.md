# deadline-job-attachments Crate Specifications

Asset manifest handling, S3 upload/download, hash cache, content-addressed
storage, and manifest lifecycle operations. Manages the data plane for job
attachments — everything that moves files between the user's machine and S3.

Owns its own S3 and STS clients independently from `deadline-api` because
the attachment subsystem has different credential lifecycles, timeout
requirements, and caching semantics.

Consumers: `deadline-cli` (attachment/manifest commands), `deadline-gui-ffi`
(submission flow), `deadline-job-bundle` (submission orchestration).

## Document Index

| Document | Description |
|----------|-------------|
| [architecture.md](architecture.md) | Module layout, public API surface, design decisions |
| [manifest-format.md](manifest-format.md) | Canonical JSON encoding, entry types, versioning |
| [hash-cache.md](hash-cache.md) | SQLite hash cache, S3 check cache, integrity verification |
| [s3-transfer.md](s3-transfer.md) | Upload/download engine, CAS addressing, progress tracking |
| [path-grouping.md](path-grouping.md) | Storage profiles, asset root resolution, symlink rejection |
| [path-mapping.md](path-mapping.md) | Path mapping rules from storage profiles, trie-based transform |

## Status

Implemented: hashing, manifest encode/decode, hash cache, S3 check cache,
progress tracking, path grouping, manifest creation, S3 upload/download,
manifest merging, output manifest retrieval, attachment download/upload API,
manifest snapshot/diff/merge/upload/download, OutputDownloader (job output
download orchestration), path mapping from storage profiles.

Gaps:
- Job attachments orchestration (`submit_job_attachments` full flow)
- VFS (virtual filesystem) — deferred
- Parallel S3 transfer — proven in spike, not yet in production paths
- File permission management on download

## Gotchas & Constraints

- The hash cache table is `hashesV5` — it will NOT read entries from the
  legacy `hashesV4` table. First run after migration starts with a cold cache.

- Manifest path entries use POSIX-style relative paths regardless of the
  host OS. Windows paths are converted to POSIX at manifest creation time.

- File mtime in manifests is microseconds since epoch, truncated (not
  rounded) from nanoseconds: `trunc(mtime_ns / 1000)`. The download side
  restores mtime from this value.

- The `partial_manifest_prefix` includes a random GUID. Re-uploading the
  same files produces a different S3 prefix each time. Deduplication
  happens at the file level (CAS), not the manifest level.

- The `attachment_download` and `attachment_upload` library functions
  accept pre-built S3 clients. The CLI layer is responsible for building
  those clients with appropriate credentials (queue-scoped for DCM users,
  profile-based otherwise).

- `manifest_download` name derivation only replaces `/` with `_` (not `\`
  or `:`), which differs from `write_manifest`. Known inconsistency
  preserved for compatibility.

## Relationship to the Python Library

Mirrors the Python `deadline.job_attachments` package. Key differences:
- Python uses `boto3.s3.transfer.TransferManager` for parallel uploads;
  Rust is currently sequential (parallel proven in spike, not yet wired)
- Python uses `hashlib` for xxh128; Rust uses the `xxhash-rust` crate
- Python's `HashCache` uses `hashesV4` table with formatted datetime
  strings; Rust uses `hashesV5` with integer nanosecond mtime (not compatible)
- Python uses `concurrent.futures.ThreadPoolExecutor` for parallel hashing;
  Rust hashes sequentially (fast enough due to xxh128 speed)
