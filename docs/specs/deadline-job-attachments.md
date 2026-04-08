# deadline-job-attachments

Asset manifest handling, S3 upload/download, hash cache, content-addressed
storage.

## Status: Complete

Batch 8a: foundational types, hashing, manifest encode/decode, caches.
Batch 8b: progress tracking. Batch 8c: path grouping and manifest
creation. Upload, download, and orchestration follow in work items #9–#10.

## Modules

### `models` — Data types for job attachments

Types that represent S3 settings, manifest metadata, path grouping, and
storage profile concepts. No I/O — these are pure data with construction
and serialization logic.

#### `JobAttachmentS3Settings`

Holds the S3 bucket name and root prefix for a queue's job attachment
storage. Constructed from either a root path string (`bucket/prefix`) or
an S3 URI (`s3://bucket/prefix`).

| Method | Behavior |
|--------|----------|
| `from_root_path(s)` | Splits on first `/`. Error if fewer than 2 parts. |
| `from_s3_root_uri(s)` | Strips `s3://`, validates scheme. Error if no prefix. |
| `to_root_path()` | Returns `"bucket/prefix"`. |
| `to_s3_root_uri()` | Returns `"s3://bucket/prefix"`. |
| `full_cas_prefix()` | `"prefix/Data"`. Error if prefix is empty. |
| `full_job_output_prefix(farm, queue, job)` | `"prefix/Manifests/farm/queue/job"`. |
| `full_step_output_prefix(...)` | Extends with step. |
| `full_task_output_prefix(...)` | Extends with task. |
| `full_output_prefix(...)` | Full hierarchical path including session action. |
| `partial_manifest_prefix(farm, queue)` | `"farm/queue/Inputs/{random_guid}"`. |
| `partial_session_action_manifest_prefix(farm, queue, job, step, task, session_action, time)` | `"farm/queue/job/step/task/{iso_time}_{session_action}"`. Static method. |
| `partial_session_action_manifest_prefix_without_task(farm, queue, job, step, session_action, time)` | Same but without task_id. For task chunking. Static method. |
| `add_root_and_manifest_folder_prefix(path)` | `"prefix/Manifests/path"`. |

The `s3://` URI is parsed with `strip_prefix` + `split_once` — no URL
parsing crate needed since the format is always `s3://bucket/prefix`.

#### `ManifestProperties`

Metadata for one asset root within a job's attachments. Fields:

- `root_path: String` — asset root on the submitting machine
- `root_path_format: PathFormat` — posix or windows
- `file_system_location_name: Option<String>` — storage profile location
- `input_manifest_path: Option<String>` — S3 key for the input manifest
- `input_manifest_hash: Option<String>` — hash of the manifest content
- `output_relative_directories: Option<Vec<String>>` — output dirs relative to root

`to_json()` returns `serde_json::Value` with camelCase keys matching the
API format (`rootPath`, `rootPathFormat`, etc.). Optional fields are omitted
when `None`.

`as_output_metadata()` returns S3 object metadata for manifest uploads.
ASCII root paths set `asset-root` directly. Non-ASCII paths JSON-encode
the root path and set both `asset-root` and `asset-root-json` for backward
compatibility.

#### `Attachments`

Container for a job's attachment metadata: a list of `ManifestProperties`
and a `JobAttachmentsFileSystem` enum (not a raw string — invalid values
are rejected at deserialization time, unlike Python which stores a string).

#### `PathFormat`

Enum: `Posix`, `Windows`. `host()` returns the current platform's format
using `cfg!(target_os)` (compile-time, not runtime `sys.platform` check).
Serializes to lowercase `"posix"` / `"windows"`.

#### `StorageProfileOperatingSystemFamily`

Enum: `Windows`, `Linux`, `Macos`. Case-insensitive `FromStr`. `host()`
returns the current platform's OS family.

#### `PathMappingRule`

Struct with `source_path_format`, `source_path`, `destination_path`.
`get_hashed_source_path(hash_alg)` returns the xxh128 hash of the
source path encoded as UTF-8.

#### `AssetRootGroup`

Groups input files, output directories, and referenced paths under a
common root. Uses `BTreeSet<PathBuf>` for deterministic ordering (Python
uses `set` then sorts later — Rust maintains order throughout).

#### `AssetUploadGroup`

Aggregates `AssetRootGroup`s with total file count and byte count.

#### `AssetRootManifest`

Associates an optional `AssetManifest` with a root path and output
directory list.

#### Helper functions

- `join_s3_paths(parts)` — joins with `/`. Replaces Python's `_join_s3_paths`.
- `generate_random_guid()` — `Uuid::new_v4().simple().to_string()`.
- `float_to_iso_datetime_string(time)` — converts a Unix timestamp (f64)
  to ISO 8601 datetime string. Used by session action manifest prefix
  methods.

---

### `asset_manifests` — Manifest encode/decode and hashing

Handles the asset manifest JSON format (version `2023-03-03`), xxh128
hashing, and canonical JSON serialization.

#### Design: no registry, no inheritance hierarchy

Python uses `BaseAssetManifest` → `AssetManifest`, `BaseManifestModel` →
`ManifestModel`, and a `ManifestModelRegistry` with explicit registration.
This exists because Python needs runtime dispatch for `decode()`/`encode()`
based on version.

Rust uses a single `AssetManifest` struct and a single `ManifestPath`
struct. `decode_manifest` matches on the version string directly. No
registry, no base types, no initialization order dependency. If a v2
manifest version appears, we add a match arm.

#### `HashAlgorithm`

Enum with one variant: `Xxh128`. Serializes to `"xxh128"`. `FromStr`
accepts `"xxh128"` only.

#### `hash_file(path, algorithm) -> Result<String>`

Reads the file in chunks using `BufReader` (default buffer size), feeds
each chunk to `xxh3_128`, returns the hex digest as a zero-padded 32-char
lowercase string. Matches Python's `xxh3_128().hexdigest()` output.

#### `hash_data(data, algorithm) -> String`

Hashes a byte slice. Returns the same hex format.

#### `ManifestVersion`

Enum: `V2023_03_03`. Serializes to `"2023-03-03"`. No `UNDEFINED` variant
(that was Python test scaffolding).

#### `ManifestPath`

Struct: `path: String`, `hash: String`, `size: i64`, `mtime: i64`.
The `mtime` is microseconds since epoch (Python truncates nanoseconds
to microseconds via `trunc(st_mtime_ns // 1000)`).

#### `AssetManifest`

Struct: `hash_alg: HashAlgorithm`, `manifest_version: ManifestVersion`,
`total_size: i64`, `paths: Vec<ManifestPath>`.

Construction validates that `hash_alg` is in the supported set (currently
only `Xxh128`). Paths are sorted by the canonical UTF-16 BE comparator at
construction time — not deferred to `encode()`. This fixes a Python bug
where `_create_manifest_file` sorts in reverse lexicographic order, then
`encode()` re-sorts by UTF-16 BE, leaving the manifest's path order
dependent on whether `encode()` has been called.

#### `decode_manifest(json_str) -> Result<AssetManifest>`

1. Parse JSON
2. Check `manifestVersion` — missing → error "missing the required
   'manifestVersion' field"; unknown → error listing supported versions
3. Validate required fields (`hashAlg`, `paths`, `totalSize`), types,
   and constraints (`paths` non-empty, `hashAlg` is `"xxh128"`,
   `totalSize` is integer)
4. Validate each path entry has required fields with correct types
5. Validate each hash is alphanumeric (`[a-zA-Z0-9]+`)
6. Construct `AssetManifest`

Error messages match Python's `ManifestDecodeValidationError` wording.

#### `AssetManifest::encode() -> String`

Canonical JSON per RFC 8785 subset:

1. Paths sorted by UTF-16 BE byte ordering (already sorted at
   construction)
2. Object keys sorted lexicographically (automatic — `serde_json::Value`
   uses `BTreeMap`)
3. Compact format: no whitespace between tokens
4. ASCII output: non-ASCII characters escaped to `\uXXXX`

The UTF-16 BE sort comparator:
```
fn utf16_be_sort_key(s: &str) -> Vec<u8> {
    s.encode_utf16().flat_map(|c| c.to_be_bytes()).collect()
}
```

This matches Python's `path.path.encode("utf-16_be", errors="surrogatepass")`.

---

### `caches` — Hash cache and S3 check cache

SQLite-backed caches for file hashes and S3 existence checks. Both use
WAL journal mode and retry with jitter on lock contention.

#### Design: no base type, no `enabled` flag

Python has a `CacheDB` ABC and an `enabled` flag for when SQLite is
unavailable. Rust uses `rusqlite` with the `bundled` feature, which
compiles SQLite in — it's always available. Each cache is a standalone
struct with its own `new()` that handles WAL mode and table creation.
No shared base type (the two caches share very little behavior).

#### `HashCache`

Wraps a `rusqlite::Connection` behind a `Mutex` for thread safety.

**Schema: `hashesV5`** (new version — see improvement note below)

| Column | Type | Description |
|--------|------|-------------|
| `file_path` | BLOB | UTF-8 encoded absolute path |
| `hash_algorithm` | TEXT | e.g. `"xxh128"` |
| `range_start` | INTEGER | 0 for whole-file |
| `range_end` | INTEGER | -1 for whole-file |
| `file_hash` | TEXT | hex digest |
| `last_modified_time` | INTEGER | nanoseconds since epoch |

Primary key: `(file_path, hash_algorithm, range_start, range_end)`.

**Improvement over Python (`hashesV4`):** `last_modified_time` is stored
as an integer (nanoseconds since epoch) instead of a formatted datetime
string. This fixes a precision bug where Python's
`str(datetime.fromtimestamp(mtime))` loses sub-microsecond precision,
causing the cache to miss modifications that change the mtime by less
than a microsecond. Integer comparison is also faster than string
comparison.

Since the Rust CLI fully replaces the Python CLI (not coexisting), a new
table version is safe. The old `hashesV4` table is ignored — a fresh
`hashesV5` table is created on first use.

| Method | Behavior |
|--------|----------|
| `new(cache_dir)` | Opens/creates DB, sets WAL mode, creates table if missing |
| `get_entry(path, alg, range_start, range_end)` | Lookup by composite key. Returns `None` if not found. |
| `put_entry(entry)` | Insert or replace (upsert). |

File paths are encoded as UTF-8 bytes and stored as BLOBs, matching
Python's `surrogatepass` encoding for the common case (valid UTF-8 paths).

No eviction policy — entries persist indefinitely (intentional, matches
Python).

#### `S3CheckCache`

Same `Mutex<Connection>` pattern.

**Schema: `s3checkV1`** (same as Python)

| Column | Type | Description |
|--------|------|-------------|
| `s3_key` | TEXT | Full S3 object key |
| `last_seen_time` | TEXT | Unix timestamp as float string |

Primary key: `(s3_key)`.

Entries expire after 30 days, evaluated at lookup time. Expired entries
are not proactively deleted. Invalid timestamps are logged as warnings
and treated as cache misses.

The `last_seen_time` format is kept as a float string for compatibility
with the Python S3 check cache (unlike the hash cache, the S3 check
cache doesn't have a precision bug — the 30-day granularity makes float
precision irrelevant).

#### Retry with jitter

Both caches retry up to 3 times on `rusqlite::Error` (database locked),
with random delay between 0.5 and 1.5 seconds. After 3 failures, the
error propagates.

#### `Drop`

`rusqlite::Connection` closes automatically on drop. No explicit cleanup
needed.

---

### `progress_tracker` — Progress reporting

Tracks file processing progress (hashing, upload, download) and reports
to callers via callbacks.

#### Design: struct with methods, not closure-on-self

Python's `ProgressTracker.__init__` creates a `track_progress_callback`
closure that captures `self`. Rust uses a method
`track_progress(&self, bytes: u64, file_done: bool) -> bool` instead.
The caller invokes the method directly — no stored closure needed.

#### `ProgressStatus`

Enum representing the current processing stage.

| Variant | `title()` | `verb_in_message()` |
|---------|-----------|---------------------|
| `None` | `"NONE"` | `""` |
| `PreparingInProgress` | `"PREPARING_IN_PROGRESS"` | `"Processed"` |
| `UploadInProgress` | `"UPLOAD_IN_PROGRESS"` | `"Uploaded"` |
| `DownloadInProgress` | `"DOWNLOAD_IN_PROGRESS"` | `"Downloaded"` |
| `SnapshotInProgress` | `"SNAPSHOT_IN_PROGRESS"` | `"Snapshotted"` |

#### `ProgressReportMetadata`

Struct passed to the progress callback on each report.

| Field | Type | Description |
|-------|------|-------------|
| `status` | `ProgressStatus` | Current processing stage |
| `progress` | `f64` | Percentage (0.0–100.0), one decimal place |
| `transfer_rate` | `f64` | Bytes/second since last report |
| `progress_message` | `String` | Human-readable message |
| `processed_files` | `u64` | Files completed so far |

The `progress_message` format matches Python exactly:
`"{verb} {completed_bytes} / {total_bytes} of {N} file(s) ({rate_label}: {rate}/s)"`

Where `rate_label` is `"Hashing speed"` for `PreparingInProgress`,
`"Transfer rate"` for all others. Byte sizes use `human_readable_file_size`
from `deadline-common`.

#### `SummaryStatistics`

| Field | Type | Description |
|-------|------|-------------|
| `total_time` | `f64` | Seconds (fractional) |
| `total_files` | `u64` | Total files to process |
| `total_bytes` | `u64` | Total bytes to process |
| `processed_files` | `u64` | Files processed |
| `processed_bytes` | `u64` | Bytes processed |
| `skipped_files` | `u64` | Files skipped (cached) |
| `skipped_bytes` | `u64` | Bytes skipped |
| `transfer_rate` | `f64` | Bytes/second overall |

| Method | Behavior |
|--------|----------|
| `aggregate(&mut self, other: &SummaryStatistics)` | Sums all fields; recalculates `transfer_rate` as `processed_bytes / total_time`. |
| `Display` | Matches Python's `__str__` exactly: processed count, skipped count, total time, rate. Singular "file" when count is 1. |

`Display` output format (matches Python):
```
Processed {N} file(s) totaling {size}.
Skipped re-processing {N} files totaling {size}.
Total processing time of {time} seconds at {rate}/s.
```

#### `DownloadSummaryStatistics`

Composition over `SummaryStatistics` (not inheritance — Rust doesn't have it).

| Field | Type | Description |
|-------|------|-------------|
| `stats` | `SummaryStatistics` | Base statistics |
| `file_counts_by_root_directory` | `BTreeMap<String, usize>` | Download count per root |
| `downloaded_files` | `Vec<String>` | All downloaded file paths, sorted |

`aggregate` sums the base stats and merges the root directory counts.

#### `ProgressTracker`

| Field | Type | Description |
|-------|------|-------------|
| `status` | `ProgressStatus` | Current stage |
| `total_files` | `u64` | Total files to process |
| `total_bytes` | `u64` | Total bytes to process |
| `callback` | `Option<Box<dyn Fn(ProgressReportMetadata) -> bool + Send>>` | Progress callback |
| `continue_reporting` | `bool` | `false` after cancellation |
| `processed_files` | `u64` | Files processed |
| `processed_bytes` | `u64` | Bytes processed |
| `skipped_files` | `u64` | Files skipped |
| `skipped_bytes` | `u64` | Bytes skipped |
| `total_time` | `f64` | Set by caller after processing completes |
| `completed_files_in_chunk` | `u64` | Files since last report |
| `reporting_files_per_chunk` | `u64` | Chunk threshold (1 or 50) |
| `last_report_time` | `Option<Instant>` | Monotonic timestamp of last report |
| `last_report_processed_bytes` | `u64` | Bytes at last report (for rate calc) |

All mutable state is behind a `Mutex` for thread safety.

| Method | Behavior |
|--------|----------|
| `new(status, total_files, total_bytes, callback)` | Sets `reporting_files_per_chunk` to 50 if `total_files >= 50`, else 1. |
| `set_total_files(files, bytes)` | Updates totals and recalculates chunk threshold. |
| `increase_processed(num_files, file_bytes)` | Increments processed counters and `completed_files_in_chunk`. |
| `increase_skipped(num_files, file_bytes)` | Increments skipped counters and `completed_files_in_chunk`. |
| `report_progress() -> bool` | Fires callback if: time elapsed ≥ 1s, OR chunk complete, OR 100% done. Returns `false` if cancelled. |
| `track_progress(bytes, file_done) -> bool` | Increments `processed_bytes`; if `file_done`, increments `processed_files` and chunk counter. Calls `report_progress`. |
| `get_summary_statistics() -> SummaryStatistics` | Snapshot of current state. `transfer_rate = processed_bytes / total_time`. |

**Cancellation:** When the callback returns `false`, `continue_reporting`
is set to `false`. All subsequent calls to `report_progress()` and
`track_progress()` return `false` without invoking the callback.

**Transfer rate in reports:** Computed as bytes-since-last-report divided
by seconds-since-last-report (instantaneous rate, not cumulative). This
matches Python's `_get_progress_report_metadata`.

---

### `upload` — Path grouping, manifest creation, and S3 upload

Batch 8c implements path grouping and manifest creation as free functions.
S3 upload logic follows in work item #9.

#### Design: free functions, not a god-class

Python uses `S3AssetManager` as a class holding farm_id, queue_id,
session, and job_attachment_settings, with methods for path grouping,
hashing, uploading, and snapshotting. The path grouping and hashing
methods don't use any S3 state — they only need filesystem access and
the hash cache.

Rust implements path grouping and manifest creation as free functions.
No struct is needed until S3 upload arrives in #9 (Principle 3 — every
abstraction must earn its keep). The functions take their dependencies
as parameters.

#### Storage profile types

`StorageProfile` and `FileSystemLocation` are added to `models.rs`:

`FileSystemLocationType` — enum: `Local`, `Shared`.

`FileSystemLocation` — struct with `name: String`, `path: String`,
`location_type: FileSystemLocationType`.

`StorageProfile` — struct with `storage_profile_id: String`,
`display_name: String`,
`os_family: StorageProfileOperatingSystemFamily`, and
`file_system_locations: Vec<FileSystemLocation>`. The first three fields
are used by the CLI to match storage profiles to queues. Python's
`StorageProfile` uses camelCase field names (`storageProfileId`, etc.)
because it deserializes directly from API JSON; Rust uses snake_case
with serde rename if needed.

#### `prepare_paths_for_upload`

```
fn prepare_paths_for_upload(
    input_paths: &[String],
    output_paths: &[String],
    referenced_paths: &[String],
    storage_profile: Option<&StorageProfile>,
    require_paths_exist: bool,
) -> Result<AssetUploadGroup, JobAttachmentsError>
```

Groups input/output/referenced paths by asset root, respecting storage
profile LOCAL and SHARED locations.

Behavior:
1. Filter out empty strings from all path lists.
2. For each input path, resolve to absolute (without following symlinks,
   using `std::path::absolute` + normalize).
3. If the path is relative to a SHARED location, skip it.
4. If the path is relative to a LOCAL location, group under that
   location's root (most specific match wins — longest path).
5. Otherwise, group under the filesystem root (top-level directory
   component).
6. Non-existent input files: if `require_paths_exist` is true, collect
   and return error. If false, move to referenced paths with a warning.
7. Directories classified as input files → error.
8. Same SHARED/LOCAL logic applies to output and referenced paths.
9. After grouping, compute `root_path` as `common_path` of all paths
   in each group. If the common path is a file, use its parent.
10. Return `AssetUploadGroup` with groups sorted by `(root_path,
    file_system_location_name)`, plus total file count and byte count.

#### `hash_assets_and_create_manifest`

```
fn hash_assets_and_create_manifest(
    asset_groups: &[AssetRootGroup],
    total_input_files: u64,
    total_input_bytes: u64,
    hash_cache_dir: Option<&str>,
    on_preparing_to_submit: Option<Box<dyn Fn(ProgressReportMetadata) -> bool + Send>>,
) -> Result<(SummaryStatistics, Vec<AssetRootManifest>), JobAttachmentsError>
```

For each `AssetRootGroup`:
1. If the group has input files, hash each file using the hash cache.
   - Cache hit (same mtime): use cached hash, mark as skipped.
   - Cache miss or mtime changed: hash file, update cache, mark as
     processed.
2. Create an `AssetManifest` from the hashed files. Paths are stored
   as POSIX-style relative paths (`path.as_posix()` equivalent).
   File mtime is `trunc(st_mtime_ns / 1000)` (microseconds).
3. If the group has no input files (only outputs), the manifest is
   `None`.
4. Build `AssetRootManifest` with the manifest, root path, location
   name, and sorted output list.
5. Report progress via `ProgressTracker` after each file.
6. If the callback returns `false`, return `Cancelled` error.

Returns `(SummaryStatistics, Vec<AssetRootManifest>)`.

**No stat cache.** Python uses `_FileStatCache` with `@lru_cache` to
avoid redundant `stat()` calls. Rust's `fs::metadata` is a direct
syscall (~1-2μs) vs Python's FFI overhead (~10-50μs). The cache
overhead would exceed the syscall cost.

#### S3 upload engine (batch 9b)

`S3AssetUploader` — struct holding `aws_sdk_s3::Client` and config.

| Method | Behavior |
|--------|----------|
| `upload_input_files(manifest, bucket, root, cas_prefix, ...)` | Splits files into small (≤ threshold) and large queues. Small files uploaded in parallel, large files serially with multipart. S3 check cache used to skip already-uploaded files. |
| `upload_object_to_cas(file, alg, bucket, root, prefix, cache, ...)` | Checks S3 check cache → HeadObject → upload if needed. Updates cache. Returns `(was_uploaded, file_size)`. |
| `file_already_uploaded(bucket, key) -> bool` | HeadObject check. 404 → false. 403 → error with `s3:ListBucket` guidance. |
| `upload_file_to_s3(path, bucket, key, ...)` | Upload via multipart with progress callback. Rejects symlinks (`O_NOFOLLOW`). Skips directories and non-existent files. |
| `upload_bytes_to_s3(bytes, bucket, key, ...)` | Upload raw bytes (for manifests). Includes `ExpectedBucketOwner`. |
| `snapshot_assets(dir, manifest, root, ...)` | Local copy to `dir/Data/` instead of S3 (debug mode). |

`S3AssetManager.upload_assets` and `snapshot_assets` orchestrate the
uploader with progress tracking, building `ManifestProperties` and
`Attachments` from the results. Error if `farm_id` or `queue_id` is
missing.

S3 error handling matches Python's guidance messages:
- 403 (non-KMS): `s3:PutObject` permission guidance
- 403 (KMS): `kms:GenerateDataKey` and `kms:DescribeKey` guidance
- 404: bucket/key existence guidance
- Transport errors: credential/network guidance

### `download` — File download from S3 CAS (work item #9)

Downloads files from S3 content-addressed storage by hash, with conflict
resolution, manifest merging, and output manifest retrieval.

#### S3 interaction model

All S3 operations use queue-scoped credentials (assumed via
`GetQueueUserBoto3Session`). Every S3 call includes
`ExpectedBucketOwner` set to the caller's account ID (from STS
`GetCallerIdentity`) to prevent confused deputy attacks.

Files are stored in CAS at `{rootPrefix}/Data/{hash}.{algorithm}`.
Download constructs the S3 key from the manifest entry's hash and
algorithm, then calls `GetObject`. If 404, retries without the
`.{algorithm}` suffix for backward compatibility with pre-CAS objects.

#### `download_file`

Downloads a single file from CAS to a local path.

1. Construct S3 key: `{cas_prefix}/{hash}.{algorithm}`
2. Resolve local path: `{local_download_dir}/{manifest_path}`
3. If file exists locally, apply conflict resolution:
   - `Skip` → return `(file_bytes, None)`
   - `Overwrite` → proceed (overwrite)
   - `CreateCopy` → generate unique copy name with collision tracking
4. Create parent directories
5. Download via S3 `GetObject` with progress callback
6. On 404: retry with key `{cas_prefix}/{hash}` (no algorithm suffix)
7. On 403: error with `s3:GetObject` or `kms:Decrypt` guidance
8. Set file mtime from manifest (microseconds → seconds)
9. Return `(file_bytes, local_path)`

Progress callback integration: the download handler calls
`progress_tracker.track_progress(bytes, false)` for each chunk, and
the tracker can cancel by returning `false`.

#### `download_files_from_manifests`

Parallel download of all files across multiple manifests.

1. Compute total files and bytes across all manifests
2. Create `ProgressTracker` with `DownloadInProgress` status
3. For each `(local_root, manifest)` pair:
   - Download all files in parallel (bounded concurrency)
   - Track downloaded file paths per root
4. Return `DownloadSummaryStatistics` with per-root file counts

#### `merge_asset_manifests`

Merges multiple manifests into one. Later manifests' paths win on
conflict (used for output-over-input merging).

- Empty list → `None`
- Single manifest → return as-is
- Multiple: collect paths into a map keyed by path string; later
  entries overwrite earlier ones. Recalculate `total_size`.
- Error if manifests have different hash algorithms.

#### `get_output_manifests_by_asset_root`

Lists and downloads output manifests from S3, grouped by asset root.

1. Build S3 prefix from farm/queue/job (optionally step/task)
2. `ListObjectsV2` to find all manifest keys under the prefix
3. Download each manifest in parallel, extracting asset root from
   S3 object metadata (`asset-root` or `asset-root-json`)
4. Group by asset root, merge chronologically (oldest first, so
   newer files overwrite older ones)
5. Return `{asset_root: [merged_manifest]}`

Output manifests are per-root-path, not per-output-directory. Even
if a manifest entry has multiple `outputRelativeDirectories`, they
all collect into one manifest per root path.

#### `OutputDownloader`

Orchestrates output download with root path remapping for cross-OS
scenarios.

- Constructed with S3 settings, farm/queue/job/step/task IDs
- `get_output_paths_by_root()` → paths grouped by asset root
- `set_root_path(original, new)` → remaps download destination
- `download_job_output()` → downloads all outputs with progress

---

### `s3` — S3 client infrastructure (batch 9a)

Factory functions for creating and configuring S3 clients, account
identity retrieval, and S3-specific constants.

#### Design: explicit parameters, no event hooks

Python uses boto3 event hooks to inject `ExpectedBucketOwner` on
every S3 call. The Rust SDK doesn't have event hooks. Instead, every
S3 call explicitly passes the account ID. This is more verbose but
more transparent — no hidden parameter injection.

#### S3 client configuration

| Setting | Value | Source |
|---------|-------|--------|
| Signature version | `s3v4` | Hardcoded |
| Connect timeout | 30s | Constant |
| Read timeout | 30s | Constant |
| Retry mode | `standard` | Constant |
| Max pool connections | From config `settings.s3_max_pool_connections` | Runtime |
| User agent | `S3A/Deadline/NA/JobAttachments/{version}` | Hardcoded |

#### `get_account_id`

Calls STS `GetCallerIdentity`, returns the `Account` field. Does not
cache — creates a fresh STS client per call. Callers should cache the
result when they need the account ID for multiple S3 operations (e.g.
as `ExpectedBucketOwner` on every PutObject/GetObject).

#### Upload/download concurrency

No boto3 `TransferManager` equivalent in Rust. Use
`tokio::task::JoinSet` + `tokio::sync::Semaphore` for bounded
concurrent multipart upload/download (proven in S3 spike).

- Small file threshold: `8MB * small_file_threshold_multiplier`
  (from config)
- Upload workers: `s3_max_pool_connections / min(multiplier, 10)`
- Download workers: `s3_max_pool_connections / 10`

### `vfs` — Virtual filesystem (deferred)

Deferred per migration strategy. Linux-only FUSE, platform-specific.

## Dependencies

| Crate | Purpose |
|-------|---------|
| `deadline-models` | Shared types and error types |
| `deadline-common` | Utility functions |
| `deadline-config` | Config file access (for S3 pool size, etc.) |
| `xxhash-rust` (feature `xxh3`) | xxh128 hashing |
| `rusqlite` (feature `bundled`) | SQLite caches |
| `serde` + `serde_json` | Manifest JSON encode/decode |
| `uuid` (feature `v4`) | Random GUID generation |
| `chrono` | Datetime formatting for manifest S3 paths |
| `aws-sdk-s3` | S3 client for upload/download (work item #9) |
| `aws-sdk-sts` | Account ID retrieval (work item #9) |
| `tokio` | Async runtime for concurrent S3 operations (work item #9) |

## S3 Transfer Spike

Proved that `aws-sdk-s3` matches Python/boto3 throughput for multipart
upload and download of large files. This was a required risk spike before
bulk implementation (see `../designs/rust-rewrite/migration_strategy.md`
§ "Fail-Fast Strategy").

### Results

1 GB file, 8 MB parts, 10 concurrent tasks, us-west-2, three runs
back-to-back with alternating order to control for network variance.

| Language | Upload (MB/s) | Download (MB/s) |
|----------|--------------|-----------------|
| Python (boto3 `upload_file`/`download_file`) | 93–106 | 112–114 |
| Rust (sequential parts) | 26 | 74 |
| Rust (concurrent parts, semaphore-bounded) | 91–105 | 107–111 |

### Findings

- **Concurrency is mandatory.** Sequential part upload is ~3.5× slower
  than boto3 because boto3's `TransferManager` uses 10 concurrent
  threads by default. Rust must do the same with async tasks.
- **Concurrent Rust matches Python.** With `JoinSet` +
  `Semaphore(10)`, throughput is within ~5% of boto3 — both are
  network-bound at ~100–110 MB/s.
- **Standard SDK is sufficient.** No need for the CRT-based transfer
  manager or raw HTTP. `aws-sdk-s3` with concurrent multipart is
  enough.

### Design constraints for implementation

- Use `tokio::task::JoinSet` + `tokio::sync::Semaphore` for bounded
  concurrent part upload and download.
- Default part size: 8 MB. Default concurrency: 10. Both should be
  configurable (matching boto3's `TransferConfig`).
- Download should use concurrent range-GET requests, not a single
  `GetObject` call.
- The `ResponseBodyCapture` pattern from `deadline-client` does not
  apply to S3 data transfer — use the SDK directly.

## Improvements Over Python

### Bug fixes

1. **Hash cache mtime precision.** Python stores `last_modified_time` as
   `str(datetime.fromtimestamp(mtime))`, which loses sub-microsecond
   precision. Rust stores nanoseconds as an integer in `hashesV5`.

2. **Manifest path sort-once.** Python sorts paths in reverse
   lexicographic order during creation, then re-sorts by UTF-16 BE
   during `encode()`, leaving the manifest's path order dependent on
   whether `encode()` has been called. Rust sorts once at construction
   using the canonical comparator.

### Type safety

3. **`Attachments.file_system`** is a typed enum, not a raw string.
   Invalid values rejected at deserialization.

4. **`ManifestProperties.root_path_format`** is a `PathFormat` enum.
   Invalid values rejected at parse time.

5. **No `ManifestModelRegistry` initialization dependency.** Decoding
   works via direct match, not a registry that must be populated first.

### Performance

6. **No stat cache.** Rust's `fs::metadata` is a direct syscall (~1-2μs)
   vs Python's FFI overhead (~10-50μs). Cache overhead would exceed the
   syscall cost.

7. **Integer mtime comparison** in hash cache instead of string
   comparison. Faster and more correct.

## Known Gaps (to address in future work items)

Types and functions that exist in Python but are not yet ported. Each
gap lists which work item will address it.

| Gap | Python location | Needed by | Work item |
|-----|----------------|-----------|-----------|
| `UploadManifestInfo` struct | `models.py` | Worker agent upload script | #18 |
| `FileStatus` enum (NEW/MODIFIED/UNCHANGED/DELETED) | `models.py` | Internal to upload, manifest diff | #10 |
| `FileConflictResolution` enum | `models.py` | Download conflict handling | #10 |
| `GlobConfig` struct | `models.py` | Manifest CLI commands | #10 |
| `ManifestSnapshot`, `ManifestDiff`, `ManifestMerge`, `ManifestDownload` | `models.py` | Manifest CLI commands, worker agent | #10 |
| `ManifestPathGroup` | `models.py` | Download path grouping | #9 |
| `OutputFile` | `models.py` | Download output tracking | #9 |
| `_manifest_snapshot`, `_manifest_merge` functions | `api/manifest.py` | Worker agent, manifest CLI | #10 |
| `_path_mapping`, `_PathMappingRuleApplier` | `_path_mapping.py` | Cross-OS download path remapping | #10 |
| `os_file_permission` module | `os_file_permission.py` | File permission management on download | #10 |
| `_float_to_iso_datetime_string` | `_utils.py` | Output manifest S3 paths | #9 |
| `_get_unique_dest_dir_name` | `_utils.py` | Download directory naming | #9 |
