# deadline-job-attachments Crate Specifications

Asset manifest handling, S3 upload/download, hash cache, content-addressed
storage, and manifest lifecycle operations. Manages the data plane for job
attachments — everything that moves files between the user's machine and S3.

Owns its own S3 and STS clients independently from `deadline-api` because
the attachment subsystem has different credential lifecycles, timeout
requirements, and caching semantics.

Consumers: `deadline-cli` (attachment/manifest commands), `deadline-python-bindings`
(submission flow), `deadline-job-bundle` (submission orchestration).

## Content-Addressed Storage

Files are stored at `{rootPrefix}/Data/{hash}.{algorithm}` in S3. Identical
files are stored once regardless of how many jobs reference them —
deduplication happens at the file level, not the manifest level.

Every S3 call includes `ExpectedBucketOwner` (from STS GetCallerIdentity)
to prevent confused deputy attacks where a misconfigured bucket policy
could route requests to the wrong account.

The hash cache (`hashesV4` SQLite table) is shared with the Python CLI —
both tools use the same cache with zero re-hashing when switching between
them. An S3 check cache (30-day expiry) avoids redundant HEAD requests
for files already known to exist in S3.

## Document Index

| Document | Description |
|----------|-------------|
| [architecture.md](architecture.md) | Module layout, public API surface, design decisions |
| [manifest-format.md](manifest-format.md) | Canonical JSON encoding, entry types, versioning |
| [hash-cache.md](hash-cache.md) | SQLite hash cache, S3 check cache, integrity verification |
| [s3-transfer.md](s3-transfer.md) | Upload/download engine, CAS addressing, progress tracking |
| [path-grouping.md](path-grouping.md) | Storage profiles, asset root resolution, symlink rejection |
| [path-mapping.md](path-mapping.md) | Path mapping rules from storage profiles, trie-based transform |
| [incremental-download.md](incremental-download.md) | Incremental download state checkpoint persistence |

## Status

Implemented: hashing, manifest encode/decode, hash cache, S3 check cache,
progress tracking, path grouping, manifest creation, parallel S3
upload/download, manifest merging, output manifest retrieval, attachment
download/upload API, manifest snapshot/diff/merge/upload/download,
OutputDownloader (job output download orchestration with include-path
filtering and root path mapping), path mapping from storage profiles,
incremental download state and checkpoint persistence.

Gaps:
- VFS (virtual filesystem) — deferred (worker-agent scope)
- File permission management on download — deferred (worker-agent scope)

## Gotchas & Constraints

- The hash cache table is `hashesV4` — compatible with Python's hash cache.
  Both CLIs share one cache with zero re-hashing when switching between tools.

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
  Rust uses `futures::stream::buffer_unordered` for parallel small-file
  upload/download and concurrent `UploadPart` for large-file multipart
- Python uses `hashlib` for xxh128; Rust uses the `xxhash-rust` crate
- Python uses `concurrent.futures.ThreadPoolExecutor` for parallel hashing;
  Rust hashes sequentially (fast enough due to xxh128 speed)
