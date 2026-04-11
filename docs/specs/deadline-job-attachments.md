# deadline-job-attachments

Asset manifest handling, S3 upload/download, hash cache, content-addressed
storage.

## Status: In Progress

Batch 8a: foundational types, hashing, manifest encode/decode, caches.
Batch 8b: progress tracking. Batch 8c: path grouping and manifest
creation. Batch 9a: S3 client infrastructure. Batch 9b: upload engine
(`S3UploadContext`, `upload_assets`, `snapshot_assets`). Batch 9c:
download engine (`download_file`, `download_files_from_manifests`,
`merge_asset_manifests`, `get_output_manifests_by_asset_root`). Batch 9d:
public API (`attachment_download`, `attachment_upload`,
`process_path_mapping`, `read_manifests`).
CLI commands follow in batch 9e and work item #10.

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

##### Design: struct with owned client, free functions for orchestration

Python splits upload across two classes: `S3AssetUploader` (S3 operations)
and `S3AssetManager` (orchestration with farm/queue context). Rust uses
`S3UploadContext` for S3 operations and free functions for orchestration. The
orchestration functions take farm/queue IDs as parameters rather than
storing them on a struct — they're only needed at the top level, not
threaded through every S3 call (Principle 3).

##### `S3UploadContext`

Struct holding the S3 client, account ID, and computed config values.

```
pub struct S3UploadContext {
    s3_client: aws_sdk_s3::Client,
    account_id: String,
    small_file_threshold: usize,
    num_upload_workers: usize,
}
```

Construction: `new(s3_client, account_id, config)`. Reads
`settings.small_file_threshold_multiplier` and
`settings.s3_max_pool_connections` via `compute_upload_config()` from
`s3.rs`. The caller builds the S3 client (via `build_s3_client`) and
gets the account ID (via `get_account_id`) before constructing the
uploader. This avoids the uploader needing `SdkConfig` or STS access.

| Method | Behavior |
|--------|----------|
| `upload_input_files(manifest, s3_bucket, source_root, s3_cas_prefix, progress_tracker, s3_check_cache_dir, force_s3_check)` | Async. For each manifest path: check S3 check cache (unless `force_s3_check=true`) → `file_already_uploaded` → upload if needed → update cache. Skipped files tracked via `increase_skipped`. After all files, calls `report_progress()` and checks cancellation. Currently sequential; parallel small/large split deferred to when performance testing requires it. |
| `file_already_uploaded(bucket, key) -> bool` | Async. `HeadObject`. HTTP status extracted from `raw_response()`. 404 → false. 403 → `S3Client` error with `s3:ListBucket` guidance including account ID. Other → `S3BotoCore` transport error. |
| `upload_file_to_s3(local_path, s3_bucket, s3_upload_key, progress_tracker)` | Async. Checks symlink via `symlink_metadata()` (rejects), skips directories and non-existent files silently. Uploads via `PutObject` with `ExpectedBucketOwner`. On completion, calls `progress_tracker.increase_processed(1, 0)`. |
| `upload_bytes_to_s3(bytes, bucket, key, metadata)` | Async. `PutObject` with `expected_bucket_owner` set to `self.account_id`. Used for manifest uploads. Optional metadata map passed as S3 object metadata. Uses the same S3 error handling as `upload_file_to_s3` (403 KMS/non-KMS, 404, 408, 500, 503 guidance). |
| `verify_hash_cache_integrity(s3_check_cache_dir, manifest, s3_cas_prefix, s3_bucket) -> bool` | Async. Samples up to 30 S3 check cache entries for manifest files, verifies each exists in S3 via `file_already_uploaded`. Returns false if any missing. |
| `reset_s3_check_cache(s3_check_cache_dir)` | Deletes the S3 check cache database file. |

**Symlink rejection:** Rust checks `fs::symlink_metadata(path).file_type().is_symlink()`
before opening. This matches the intent of Python's `O_NOFOLLOW` without
needing raw file descriptors. On failure (permission error, etc.), the
file is skipped with a warning log — matching Python's
`_open_non_symlink_file_binary` yielding `None`.

**Parallel upload and multipart:** Currently sequential single-PutObject.
The `small_file_threshold` and `num_upload_workers` fields are stored on
`S3UploadContext` for future use when parallel upload via `JoinSet` +
`Semaphore` and multipart for large files are added. The S3 spike proved
concurrent parts match boto3 throughput — this optimization will be added
when performance testing requires it.

**S3 error handling** matches Python's guidance messages exactly:

| HTTP Status | Context | Guidance |
|-------------|---------|----------|
| 403 (non-KMS) | `upload_file_to_s3` | `s3:PutObject` permission |
| 403 (KMS) | `upload_file_to_s3` | `kms:GenerateDataKey` and `kms:DescribeKey` |
| 403 | `file_already_uploaded` | `s3:ListBucket` permission + account ID |
| 403 (non-KMS) | `upload_bytes_to_s3` | `s3:PutObject` permission |
| 403 (KMS) | `upload_bytes_to_s3` | `kms:GenerateDataKey` and `kms:DescribeKey` |
| 404 | `upload_file_to_s3` | bucket/key existence |
| 404 | `upload_bytes_to_s3` | bucket existence |
| 408, 500, 503 | all | retry/network guidance (from `COMMON_ERROR_GUIDANCE_FOR_S3`) |
| Transport | all | `S3BotoCore` with credential/network guidance |

KMS detection: check if the error message string contains `"kms:"` —
same heuristic as Python.

##### `upload_assets` (free function)

Orchestrates the full upload flow for a list of `AssetRootManifest`s.

```
pub async fn upload_assets(
    farm_id: &str,
    queue_id: &str,
    job_attachment_settings: &JobAttachmentS3Settings,
    manifests: &[AssetRootManifest],
    uploader: &S3UploadContext,
    on_uploading_assets: Option<Box<dyn Fn(ProgressReportMetadata) -> bool + Send>>,
    s3_check_cache_dir: Option<&str>,
    force_s3_check: Option<bool>,
) -> Result<(SummaryStatistics, Attachments), JobAttachmentsError>
```

Behavior:
1. Validate `farm_id` and `queue_id` are non-empty. Error:
   `"upload_assets: Farm or Fleet ID is missing."`.
2. Compute total files/bytes from manifests (only those with
   `asset_manifest.is_some()`).
3. Create `ProgressTracker` with `UploadInProgress` status.
4. For each `AssetRootManifest`:
   a. Build `ManifestProperties` with `root_path`, `root_path_format`
      (host format), `file_system_location_name`, and
      `output_relative_directories` (relative to `root_path`).
   b. If `asset_manifest` is `Some`:
      - Generate `partial_manifest_prefix` (random GUID path).
      - Encode manifest to bytes, compute manifest name as
        `{hash_of_root_path_str}_input`.
      - Call `ctx.upload_bytes_to_s3` for the manifest (uploaded
        first so it's available even if file upload fails partway).
      - Verify S3 check cache integrity (sample up to 30 entries,
        reset cache if any missing). Skip if `force_s3_check=true`.
      - Call `ctx.upload_input_files` for the file data.
      - Set `input_manifest_path` and `input_manifest_hash` on the
        `ManifestProperties`.
5. Return `(SummaryStatistics, Attachments)`.

##### `snapshot_assets` (free function)

Same signature pattern as `upload_assets` but copies files locally
instead of uploading to S3.

```
pub fn snapshot_assets(
    farm_id: &str,
    queue_id: &str,
    job_attachment_settings: &JobAttachmentS3Settings,
    snapshot_dir: &Path,
    manifests: &[AssetRootManifest],
    on_snapshotting_assets: Option<Box<dyn Fn(ProgressReportMetadata) -> bool + Send>>,
) -> Result<(SummaryStatistics, Attachments), JobAttachmentsError>
```

Behavior:
1. Same farm/queue validation.
2. For each manifest with files: copy each file to
   `snapshot_dir/Data/{hash}.{algorithm}` using `std::fs::copy`.
   Write manifest to `snapshot_dir/Manifests/{partial_prefix}/{name}`.
3. Progress tracking with `SnapshotInProgress` status.
4. Cancellation check after each file.

##### `verify_hash_cache_integrity`

Method on `S3UploadContext`. See method table above. The sample is
randomized (Fisher-Yates shuffle using time-seeded hash, take first 30
that have cache entries). This matches Python's `random.shuffle` +
`break at 30` pattern.

### `download` — File download from S3 CAS (work item #9)

Downloads files from S3 content-addressed storage by hash, with conflict
resolution, manifest merging, and output manifest retrieval.

#### Design: free functions + `FileConflictResolution` enum

Python splits download across `download_file` (free function),
`download_files_from_manifests` (free function), and `OutputDownloader`
(class). Rust uses free functions for the core engine (batch 9c) and
defers `OutputDownloader` to batch 9d where the CLI consumer needs it.

`FileConflictResolution` is an enum in `models.rs`: `Skip`, `Overwrite`,
`CreateCopy`. Python uses integer-valued enum members (0-3) with a
`NOT_SELECTED` variant; Rust omits `NOT_SELECTED` (callers pass
`Option<FileConflictResolution>` if selection is optional).

Collision tracking for `CreateCopy` uses `Arc<Mutex<HashMap<String, i32>>>`
shared across concurrent downloads. Python uses `threading.Lock` +
`DefaultDict[str, int]` — same semantics.

#### S3 interaction model

Every S3 call includes `ExpectedBucketOwner` set to the caller's
account ID (from STS `GetCallerIdentity`). The S3 client and account
ID are passed as parameters — no struct needed (Principle 3).

Files are stored in CAS at `{rootPrefix}/Data/{hash}.{algorithm}`.
Download constructs the S3 key from the manifest entry's hash and
algorithm, then calls `GetObject`. If 404, retries without the
`.{algorithm}` suffix for backward compatibility with pre-CAS objects.

#### `download_file`

Async. Downloads a single file from CAS to a local path.

```
pub async fn download_file(
    file: &ManifestPath,
    hash_algorithm: HashAlgorithm,
    local_download_dir: &str,
    s3_client: &aws_sdk_s3::Client,
    s3_bucket: &str,
    cas_prefix: Option<&str>,
    account_id: &str,
    progress_tracker: Option<&ProgressTracker>,
    file_conflict_resolution: FileConflictResolution,
    collision_state: &CollisionState,
) -> Result<(i64, Option<PathBuf>), JobAttachmentsError>
```

Behavior:
1. Construct S3 key: `{cas_prefix}/{hash}.{algorithm}`
2. Resolve local path: `{local_download_dir}/{manifest_path}`
3. If file exists locally, apply conflict resolution:
   - `Skip` → return `(file_bytes, None)`
   - `Overwrite` → proceed
   - `CreateCopy` → generate unique copy name via atomic
     `OpenOptions::create_new(true)` in a loop with `(N)` suffix,
     tracked in shared `CollisionState`
4. Create parent directories
5. Download via S3 `GetObject` with `ExpectedBucketOwner`
6. Write content directly to target path
7. On 404: retry with key `{cas_prefix}/{hash}` (no algorithm suffix)
8. On 403: S3Client error with `s3:GetObject` or `kms:Decrypt` guidance
9. On 408/500/503: S3Client error with retry/network guidance
10. Set file mtime from manifest (microseconds → seconds) via `filetime`
11. Return `(file_bytes, local_path)`

Progress: calls `progress_tracker.increase_processed` after each file.
Cancellation checked via `report_progress` return value.

S3 error handling matches upload patterns exactly (same guidance
messages for 403/404/408/500/503).

#### `download_files_from_manifests`

Async. Downloads all files across multiple manifests.

```
pub async fn download_files_from_manifests(
    s3_bucket: &str,
    manifests_by_root: &HashMap<String, AssetManifest>,
    cas_prefix: Option<&str>,
    s3_client: &aws_sdk_s3::Client,
    account_id: &str,
    on_downloading_files: Option<Box<dyn Fn(ProgressReportMetadata) -> bool + Send>>,
    conflict_resolution: FileConflictResolution,
) -> Result<DownloadSummaryStatistics, JobAttachmentsError>
```

Behavior:
1. Compute total files and bytes across all manifests
2. Create `ProgressTracker` with `DownloadInProgress` status
3. For each `(local_root, manifest)` pair:
   - Download each file via `download_file`
   - Track downloaded file paths per root
   - On cancellation: return `Cancelled` error
4. Final progress report at 100%
5. Return `DownloadSummaryStatistics` with per-root file counts

Currently sequential per file. Parallel download via `JoinSet` +
`Semaphore` deferred to performance optimization (same pattern as
upload engine).

#### `merge_asset_manifests`

Pure function. Merges multiple manifests into one. Returns
`Result<Option<AssetManifest>>` — `Ok(None)` for empty input,
`Err` for mismatched hash algorithms.

- Empty list → `Ok(None)`
- Single manifest → `Ok(Some(clone))`
- Multiple: collect paths into `HashMap` keyed by path string;
  later entries overwrite earlier ones. Recalculate `total_size`.
- Different hash algorithms → `Err(JobAttachmentsError::AssetSync)`.

#### `get_output_manifests_by_asset_root`

Async. Lists and downloads output manifests from S3, grouped by root.

```
pub async fn get_output_manifests_by_asset_root(
    s3_settings: &JobAttachmentS3Settings,
    farm_id: &str,
    queue_id: &str,
    job_id: &str,
    step_id: Option<&str>,
    task_id: Option<&str>,
    session_action_id: Option<&str>,
    s3_client: &aws_sdk_s3::Client,
    account_id: &str,
) -> Result<HashMap<String, Vec<AssetManifest>>, JobAttachmentsError>
```

Behavior:
1. If `session_action_id` provided: require `step_id` and `task_id`,
   search by regex in task then step prefix
2. Otherwise: build S3 prefix from farm/queue/job (optionally step/task)
3. `ListObjectsV2` (paginated) to find manifest keys
4. `_get_tasks_manifests_keys_from_s3` selects latest per task
   (alphabetical sort of `timestamp_sessionaction_id` folders)
5. Download each manifest via `GetObject`, extract asset root from
   S3 object metadata (`asset-root-json` preferred, `asset-root` fallback)
6. Group by asset root, merge chronologically (oldest first by
   `LastModified`, so newer files overwrite older ones)
7. Return `{asset_root: [merged_manifest]}`

Missing asset root in metadata → `MissingAssetRoot` error.

#### `OutputDownloader` (batch 9d)

Deferred. Orchestrates output download with root path remapping for
cross-OS scenarios. Needed by CLI `job download-output` command.

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

### `api` — Public API for attachment download/upload (batch 9d)

Stateless orchestration functions that compose the lower-level upload and
download engines. These are the entry points that the CLI `attachment`
commands call. Corresponds to Python's `deadline.job_attachments.api.attachment`
and `deadline.job_attachments.api._utils`.

#### Design: free functions, not a class

Python uses module-level functions (`_attachment_download`,
`_attachment_upload`, `_process_path_mapping`, `_read_manifests`). Rust
does the same — no struct needed since these functions are stateless
orchestrators that take all dependencies as parameters (Principle 3).

#### New type: `UploadManifestInfo`

Added to `models.rs`:

```
pub struct UploadManifestInfo {
    pub output_manifest_path: String,
    pub output_manifest_hash: String,
    pub source_path: Option<String>,
}
```

Returned by `attachment_upload` — one entry per input manifest, in the
same order as the input list.

#### `read_manifests(manifest_paths) -> IndexMap<String, AssetManifest>`

Reads and decodes manifest files from disk. Returns an `IndexMap` keyed
by base filename (preserves insertion order for `attachment_upload`
iteration — Python dicts preserve insertion order in 3.7+).

- Validates all paths exist upfront; collects invalid ones into a single
  error: `"Specified manifests [path1, path2] are not valid."`
- Empty list → empty map
- Invalid manifest content → propagates decode error

#### `process_path_mapping(path_mapping_rules?, root_dirs) -> Vec<PathMappingRule>`

Builds a list of path mapping rules from a JSON file and/or root
directories.

- If `path_mapping_rules` is `Some`: validates file exists, parses JSON.
  Handles both top-level list format and `path_mapping_rules` nested key.
  Error if file doesn't exist: `"Specified path mapping file {path} is
  not valid."`
- If `root_dirs` non-empty: validates all dirs exist. Creates rules with
  `source_path = destination_path = dir`, empty `source_path_format`.
  Error if any dir doesn't exist: `"Specified root dir [dirs] are not
  valid."`
- Both can be provided (this helper concatenates them). The caller
  `attachment_upload` enforces mutual exclusion separately.
- Neither → empty list

#### `attachment_download`

```
pub async fn attachment_download(
    manifests: &[String],
    s3_root_uri: &str,
    s3_client: &aws_sdk_s3::Client,
    account_id: &str,
    path_mapping_rules: Option<&str>,
    on_progress: Option<Box<dyn Fn(ProgressReportMetadata) -> bool + Send>>,
    conflict_resolution: FileConflictResolution,
) -> Result<DownloadSummaryStatistics, JobAttachmentsError>
```

Orchestrates downloading files from S3 CAS based on manifest files and
optional path mapping rules.

Behavior:
1. Call `read_manifests` to decode all manifest files
2. Call `process_path_mapping` if rules file provided
3. For each manifest: find matching rule by checking if
   `rule.get_hashed_source_path()` appears in the manifest filename
   (substring match via `.contains()`). If no match, fall back to
   `{cwd}/{filename}` as destination.
4. Reject duplicate destinations: `"{dest} is already in use, one
   destination path maps to one manifest file only."`
5. Parse `s3_root_uri` via `JobAttachmentS3Settings::from_s3_root_uri`
6. Delegate to `download_files_from_manifests` with the resolved
   `manifests_by_root` map

#### `attachment_upload`

```
pub async fn attachment_upload(
    manifests: &[String],
    s3_root_uri: &str,
    s3_client: &aws_sdk_s3::Client,
    account_id: &str,
    root_dirs: &[String],
    path_mapping_rules: Option<&str>,
    upload_manifest_path: Option<&str>,
    on_progress: Option<Box<dyn Fn(ProgressReportMetadata) -> bool + Send>>,
    config: Option<&IniConfig>,
) -> Result<Vec<UploadManifestInfo>, JobAttachmentsError>
```

Orchestrates uploading files to S3 CAS based on manifest files and
path mapping rules or root directories.

Behavior:
1. Call `read_manifests` to decode all manifest files
2. Validate exactly one of `path_mapping_rules` / `root_dirs` is
   provided. Error: `"One of path mapping rule and root dir must exist,
   and not both."`
3. Call `process_path_mapping`
4. Parse `s3_root_uri` via `JobAttachmentS3Settings::from_s3_root_uri`
5. Build `S3UploadContext` from `s3_client`, `account_id`, `config`
6. For each manifest (in original input order):
   a. Find matching rule by hashed source path in filename
   b. Error if no match: `"No valid root defined for given manifest
      {filename}, please check input root dirs and path mapping rule."`
   c. Build S3 metadata: ASCII paths set `asset-root` directly;
      non-ASCII paths JSON-encode and set `asset-root-json` (plus
      `asset-root` with JSON value for backward compatibility).
      If rule has `source_path_format`, set `file-system-location-name`.
   d. Upload files via `S3UploadContext.upload_input_files`
   e. If `upload_manifest_path` provided, upload manifest to S3 via
      `upload_bytes_to_s3` with metadata
   f. Collect `UploadManifestInfo` with manifest S3 key, content hash,
      and source path
7. Return `Vec<UploadManifestInfo>` in same order as input

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

### `diff` — Manifest comparison (batch 9e-1)

Compares files on disk against a manifest to detect changes. Two
strategies: fast (mtime + size) and thorough (hash-based).

#### New type: `FileStatus`

Enum in `models.rs`: `Unchanged`, `New`, `Modified`, `Deleted`.
Represents the status of a local file relative to a manifest entry.

#### `fast_diff(root, current_files, reference_manifest) -> Vec<(String, FileStatus)>`

Compares files by size and modification time against a reference
manifest. Returns root-relative POSIX paths with their status.

Behavior:
- For each file in `current_files`: compute root-relative path, look
  up in manifest by path.
- Not in manifest → `New`.
- In manifest, size differs → `Modified`.
- In manifest, size matches but mtime differs by more than 1
  microsecond → `Modified`. The 1μs tolerance accounts for rounding
  when setting mtime from microsecond-precision manifest values.
  Comparison: `abs(trunc(file_mtime_ns / 1000) - manifest_mtime) > 1`.
- For each manifest entry not in `current_files` → `Deleted`.

#### `hash_diff(reference, compare) -> Vec<(FileStatus, ManifestPath)>`

Compares two manifests by hash. Returns status for every path in
either manifest.

Behavior:
- Path in `compare` but not `reference` → `New`.
- Path in both, hashes differ → `Modified`.
- Path in both, hashes match → `Unchanged`.
- Path in `reference` but not `compare` → `Deleted`.

---

### `manifest_ops` — Manifest lifecycle operations (batch 9e-1)

High-level operations for creating, comparing, merging, uploading, and
downloading manifests. These are the functions the CLI `manifest`
commands call.

#### New types in `models.rs`

```
GlobConfig { include: Vec<String>, exclude: Vec<String> }
ManifestSnapshot { root: String, manifest: String }
ManifestDiffResult { new: Vec<String>, modified: Vec<String>, deleted: Vec<String> }
ManifestMergeResult { manifest_root: String, local_manifest_path: String }
ManifestDownloadEntry { manifest_root: String, local_manifest_path: String }
ManifestDownloadResponse { downloaded: Vec<ManifestDownloadEntry> }
AssetType enum: Input, Output, All
```

All are simple data containers with `Serialize` for JSON output.

#### `resolve_glob_config(include, exclude, include_exclude_config) -> GlobConfig`

Resolves glob configuration from CLI arguments. Pure function.

Behavior:
- If `include` or `exclude` is non-empty, use them (config ignored).
- Else if `include_exclude_config` is provided: try reading as file
  path first, fall back to parsing as JSON string. Extract `include`
  and `exclude` keys, defaulting to `["**/*"]` and `[]`.
- Else: default config (`include: ["**/*"]`, `exclude: []`).

#### `glob_files(root, config) -> Vec<String>`

Returns absolute normalized paths of all files matching the glob config
under `root`. Directories are excluded from results.

Uses the `glob` crate for pattern matching. Each include pattern is
joined with the root path. Exclude patterns are subtracted from the
include results.

#### `write_manifest(root, manifest, destination, name?) -> String`

Writes a manifest to disk and returns the file path.

Behavior:
- Filename: `{name}-{root_hash}-{timestamp}.manifest`
  - `root_hash`: xxh128 of root path as UTF-8 bytes
  - `timestamp`: `YYYY-MM-DDTHH-MM-SS` (local time)
- Name derivation when not provided: replace `/`, `\`, `:` with `_`,
  strip leading `_`.
- Creates parent directories if needed.

#### `manifest_snapshot(root, destination, name, config, diff?, force_rehash?, callback?) -> Option<ManifestSnapshot>`

Creates a manifest of files in a directory, optionally diffing against
a prior manifest.

Behavior:
- Glob files in root using config.
- If no diff manifest: hash all files via `hash_assets_and_create_manifest`,
  write via `write_manifest`.
- If diff manifest provided and `force_rehash=false`: fast diff
  (mtime/size), then hash only new/modified files.
- If diff manifest provided and `force_rehash=true`: hash all files,
  compare hashes, include only new/modified.
- If no files match or diff produces zero changes: return `None`.

#### `manifest_diff(manifest_path, root, config, force_rehash?, callback?) -> ManifestDiffResult`

Computes file differences between a manifest and a directory.

Behavior:
- Glob files in root using config.
- If `force_rehash=false`: use `fast_diff`.
- If `force_rehash=true`: hash all files, use `hash_diff`.
- Returns three lists: new, modified, deleted (root-relative paths).

#### `manifest_merge(root, manifest_files, destination, name?, callback?) -> Option<ManifestMergeResult>`

Merges multiple manifest files into one.

Behavior:
- Read and decode all manifest files via `read_manifests`.
- Merge via `merge_asset_manifests`.
- If merge produces a result, write via `write_manifest`.
- Return `None` if merge produces empty result.

#### `manifest_upload` and `manifest_download`

Deferred to batch 9e-2 (S3 + Deadline API interaction).

---

## Known Gaps (to address in future work items)

Types and functions that exist in Python but are not yet ported. Each
gap lists which work item will address it.

| Gap | Python location | Needed by | Work item |
|-----|----------------|-----------|-----------|
| `_path_mapping`, `_PathMappingRuleApplier` | `_path_mapping.py` | Cross-OS download path remapping | #10 |
| `os_file_permission` module | `os_file_permission.py` | File permission management on download | #10 |
| `_get_unique_dest_dir_name` | `_utils.py` | Download directory naming | #10 |
