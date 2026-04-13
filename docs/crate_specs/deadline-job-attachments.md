# deadline-job-attachments

Asset manifest handling, S3 upload/download, hash cache,
content-addressed storage, and manifest lifecycle operations.

## Role in the System

Manages the data plane for job attachments — everything that moves files
between the user's machine and S3. This includes hashing files, creating
manifests, uploading/downloading via content-addressed storage (CAS),
caching results locally, and tracking progress. Owns its own S3 and STS
clients independently from `deadline-client` because the attachment
subsystem has different credential lifecycles, timeout requirements, and
caching semantics.

Consumers: `deadline-cli` (attachment/manifest commands, future bundle
submit), `deadline-gui-ffi` (submission flow).

## Key Concepts

**Content-Addressed Storage (CAS).** Files are stored in S3 at
`{rootPrefix}/Data/{hash}.{algorithm}`. The hash is the file's content
hash (xxh128), so identical files are stored once regardless of how many
jobs reference them. Manifests describe which files a job needs by listing
their hashes.

**Asset manifests.** A JSON document (version `2023-03-03`) listing all
files for one asset root: each entry has a relative path, content hash,
file size, and modification time (microseconds since epoch). Manifests
are the unit of transfer — upload uploads the files a manifest describes,
download downloads them.

**Canonical JSON encoding.** Manifests are serialized with paths sorted by
UTF-16 BE byte ordering, object keys sorted lexicographically, compact
format (no whitespace), and non-ASCII characters escaped to `\uXXXX`.
This ensures byte-identical output regardless of platform or insertion
order.

**Two local caches (SQLite).** The hash cache avoids re-hashing unchanged
files (keyed by path + mtime). The S3 check cache avoids redundant
HeadObject calls to verify a file exists in S3 (keyed by S3 key, expires
after 30 days). Both use WAL journal mode and retry with jitter on lock
contention.

**Storage profiles and path grouping.** Files are grouped by asset root
based on storage profile locations (LOCAL vs SHARED). SHARED locations
are skipped (they're already accessible to workers). LOCAL locations
define the grouping boundaries. Files outside any known location group
under the filesystem root.

**Progress tracking with cancellation.** All long-running operations
(hashing, upload, download) report progress via callbacks. The callback
return value controls cancellation — returning `false` stops the
operation. Progress reports fire based on time elapsed (≥1s) or chunk
completion, whichever comes first.

## Behavior & Contracts

**Hash cache uses integer nanosecond mtime.** Stored in a `hashesV5`
table (not compatible with the legacy `hashesV4` table which used
formatted datetime strings with precision loss). Cache hit requires exact
mtime match — any change triggers a rehash.

**S3 check cache expires after 30 days.** Evaluated at lookup time.
Invalid timestamps are treated as cache misses with a warning log.

**S3 error messages include actionable guidance.** Every S3 error maps to
a specific help message: 403 → which IAM permission is needed (including
KMS permissions when the error mentions `kms:`), 404 → bucket/key
existence check, 408/500/503 → retry/network guidance. Transport errors
append credential and network verification guidance.

**`ExpectedBucketOwner` on every S3 call.** The account ID (from STS
GetCallerIdentity) is passed to every PutObject/GetObject/HeadObject.
This prevents confused deputy attacks where a misconfigured bucket policy
could route requests to the wrong account's bucket.

**Symlinks are rejected on upload.** Files are checked via
`symlink_metadata()` before upload. Symlinks are skipped with a warning
— they're a security risk (could point outside the asset root after
submission).

**Download conflict resolution:** Three modes — `Skip` (keep existing),
`Overwrite` (replace), `CreateCopy` (write as `filename (N).ext` with
atomic create-new to avoid races). Collision counters are shared across
concurrent downloads via `Arc<Mutex<HashMap>>`.

**Manifest merging:** Multiple manifests for the same asset root are
merged by path — later entries overwrite earlier ones. Different hash
algorithms across manifests being merged is an error.

**Output manifest retrieval from S3:** Lists manifest objects under the
job's output prefix, selects the latest per task (by alphabetical sort of
timestamp folders), downloads each, extracts asset root from S3 object
metadata (`asset-root-json` preferred, `asset-root` fallback), and groups
by root.

## Design Decisions

**Independent client caching from `deadline-client`.** The S3 client has
different configuration (custom timeouts, s3v4 signature, pool size,
user agent) and different credential lifecycles. Unifying with
`deadline-client`'s session cache would create coupling without benefit.
Credential changes (e.g., after login) don't propagate to attachment
clients — acceptable because attachments run within a single submission
flow.

**Free functions for orchestration, struct for S3 operations.**
`S3UploadContext` holds the S3 client and account ID. Orchestration
functions (`upload_assets`, `snapshot_assets`, `attachment_download`,
`attachment_upload`) are stateless free functions that take dependencies
as parameters. No god-class that accumulates farm/queue/session state.

**No stat cache.** File metadata calls are direct syscalls (~1-2μs in
Rust). The overhead of maintaining a cache would exceed the syscall cost.

**Paths sorted once at manifest construction.** The canonical UTF-16 BE
sort is applied when the manifest is built, not deferred to encoding.
This eliminates a class of bugs where the manifest's path order depends
on whether `encode()` has been called.

**Sequential S3 operations (for now).** Upload and download are currently
sequential per file. The S3 transfer spike proved that concurrent
operations via `JoinSet` + `Semaphore` match boto3 throughput. Parallel
transfer will be added when performance testing requires it — the
`small_file_threshold` and `num_upload_workers` fields are already stored
for this purpose.

**`IndexMap` for manifest reading order.** `read_manifests` returns an
`IndexMap` (preserves insertion order) because `attachment_upload`
iterates manifests in the order they were specified on the command line.

## Gotchas & Constraints

- The hash cache table is `hashesV5` — it will NOT read entries from the
  legacy `hashesV4` table. First run after migration starts with a cold
  cache.

- Manifest path entries use POSIX-style relative paths regardless of the
  host OS. Windows paths are converted to POSIX at manifest creation time.

- File mtime in manifests is microseconds since epoch, truncated (not
  rounded) from nanoseconds: `trunc(mtime_ns / 1000)`. The download side
  restores mtime from this value.

- The `partial_manifest_prefix` includes a random GUID. This means
  re-uploading the same files produces a different S3 prefix each time.
  Deduplication happens at the file level (CAS), not the manifest level.

- `verify_hash_cache_integrity` samples up to 30 random entries. If any
  sampled file is missing from S3, the entire cache is reset. This is a
  heuristic — it won't catch every stale entry but catches bulk
  invalidation (e.g., bucket was cleared).

- The `attachment_download` and `attachment_upload` library functions
  accept pre-built S3 clients. The CLI layer is responsible for building
  those clients with appropriate credentials (queue-scoped for DCM users,
  profile-based otherwise).

- `manifest_download` name derivation only replaces `/` with `_` (not `\`
  or `:`), which differs from `write_manifest`. This is a known
  inconsistency preserved for compatibility.

## Status & Gaps

Implemented: hashing, manifest encode/decode, hash cache, S3 check cache,
progress tracking, path grouping, manifest creation, S3 upload engine,
S3 download engine, manifest merging, output manifest retrieval,
attachment download/upload API, manifest snapshot/diff/merge/upload/download.

Gaps:
- Job attachments orchestration (work item #10) — the full
  `submit_job_attachments` flow that ties path grouping → hashing →
  upload → manifest upload → API attachment metadata together
- VFS (virtual filesystem) — deferred, Linux-only FUSE mount
- Parallel S3 transfer — proven in spike, not yet wired into production
  upload/download paths
- File permission management on download — not yet ported
- Cross-OS path remapping on download — not yet ported
