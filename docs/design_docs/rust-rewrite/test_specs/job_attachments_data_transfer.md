# Job Attachments — Data & Transfer

> Part of [AWS Deadline Cloud Client — Behavioral Test Specification](index.md)
>
> This document describes observable behavior through public interfaces.
> Error descriptions use category labels (e.g., "returns error") rather than
> language-specific exception types. The Rust implementation should produce
> equivalent observable outcomes, not necessarily identical internal mechanics.

---

## Section 19: Job attachments — models & data classes

> **Rust crate:** `deadline-models` + `deadline-job-attachments` · **Modules:** `job_attachments` (models), `models` (job-attachments crate)
>
> **Logic under test:** Data structures for S3 settings (bucket/prefix parsing from
> root paths and URIs), manifest properties (S3 metadata generation with ASCII/non-ASCII
> handling), storage profile OS family (case-insensitive enum), path format detection,
> attachments serialization, and path mapping rule hashing.
> See [data_flow.md § Asset Manifest Format](data_flow.md#asset-manifest-format) for S3 layout.

### `JobAttachmentS3Settings`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 1 | Happy path | `from_root_path("my-bucket/my-prefix")` | Returns `JobAttachmentS3Settings(s3BucketName="my-bucket", rootPrefix="my-prefix")` | |
| 2 | Happy path | `from_root_path("my-bucket/deep/nested/prefix")` | `rootPrefix` is `"deep/nested/prefix"` | Splits on first `/` only for bucket |
| 3 | Error handling | `from_root_path("no-slash")` | Returns error (malformed attachment setting) | Less than 2 parts after split |
| 4 | Happy path | `from_s3_root_uri("s3://my-bucket/my-prefix")` | Returns correct settings | |
| 5 | Error handling | `from_s3_root_uri("https://my-bucket/my-prefix")` | Returns error (malformed attachment setting, scheme != s3) | |
| 6 | Error handling | `from_s3_root_uri("s3://my-bucket")` (no prefix) | Returns error (malformed attachment setting) | |
| 7 | Happy path | `to_root_path()` | Returns `"s3BucketName/rootPrefix"` | |
| 8 | Happy path | `to_s3_root_uri()` | Returns `"s3://s3BucketName/rootPrefix"` | |
| 9 | Happy path | `full_cas_prefix()` | Returns `"rootPrefix/Data"` | |
| 10 | Error handling | `full_cas_prefix()` with empty rootPrefix | Returns error (missing S3 root prefix) | |
| 11 | Happy path | `full_job_output_prefix(farm, queue, job)` | Returns `"rootPrefix/Manifests/farm/queue/job"` | |
| 12 | Happy path | `full_step_output_prefix(farm, queue, job, step)` | Returns `"rootPrefix/Manifests/farm/queue/job/step"` | |
| 13 | Happy path | `full_task_output_prefix(farm, queue, job, step, task)` | Returns `"rootPrefix/Manifests/farm/queue/job/step/task"` | |
| 14 | Happy path | `full_output_prefix(farm, queue, job, step, task, session_action)` | Returns full hierarchical path | |
| 15 | Happy path | `partial_manifest_prefix(farm, queue)` | Returns `"farm/queue/Inputs/{random_guid}"` | GUID is random |
| 16 | Happy path | `add_root_and_manifest_folder_prefix(path)` | Returns `"rootPrefix/Manifests/path"` | |

### `ManifestProperties`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 17 | Happy path | `to_dict()` with all fields set | Returns dict with all fields | |
| 18 | Happy path | `to_dict()` with optional fields as None | Optional fields omitted from dict | |
| 19 | Happy path | `from_dict(data)` with all fields | Returns correct `ManifestProperties` | |
| 20 | Happy path | `as_output_metadata()` with ASCII rootPath | Returns `{"Metadata": {"asset-root": path}}` | |
| 21 | Happy path | `as_output_metadata()` with non-ASCII rootPath | Returns `{"Metadata": {"asset-root-json": json_encoded, "asset-root": json_encoded}}` | Both fields for backward compat |
| 22 | Happy path | `as_output_metadata()` with `fileSystemLocationName` set | Includes `"file-system-location-name"` in metadata | |

### `StorageProfileOperatingSystemFamily`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 23 | Happy path | `StorageProfileOperatingSystemFamily("windows")` | Returns `WINDOWS` | |
| 24 | Happy path | `StorageProfileOperatingSystemFamily("WINDOWS")` | Returns `WINDOWS` (case-insensitive) | `_missing_` method |
| 25 | Happy path | `get_host_os_family()` on macOS | Returns `MACOS` | |

### `PathFormat`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 26 | Happy path | `get_host_path_format()` on macOS/Linux | Returns `POSIX` | |
| 27 | Happy path | `get_host_path_format()` on Windows | Returns `WINDOWS` | |

### `Attachments`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 28 | Happy path | `to_dict()` with manifests | Returns dict with `manifests` list and `fileSystem` string | |
| 29 | Happy path | Default `fileSystem` is `"COPIED"` | `JobAttachmentsFileSystem.COPIED.value` | |

### `PathMappingRule`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 30 | Happy path | `get_hashed_source_path(HashAlgorithm.XXH128)` | Returns xxh128 hash of source_path encoded as UTF-8 | |

---

## Section 20: Job attachments — hashing & manifest creation

> **Rust crate:** `deadline-job-attachments` · **Module:** `asset_manifests`, `upload`
>
> **Logic under test:** Grouping input/output/referenced paths by asset root (with
> storage profile awareness for SHARED vs LOCAL locations), hashing files with xxh128
> (using hash cache for dedup), creating manifests with POSIX-style relative paths,
> and reporting progress via callbacks. File mtime stored as microseconds since epoch.

### `S3AssetManager.prepare_paths_for_upload(input_paths, output_paths, referenced_paths, storage_profile=None, require_paths_exist=False) -> AssetUploadGroup`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 1 | Happy path | Provide input files, output dirs, and referenced paths | Returns `AssetUploadGroup` with grouped paths, total file count, and total bytes | |
| 2 | Happy path | Input path is relative to a SHARED storage profile location | Path is excluded from the returned groups | |
| 3 | Happy path | Input path is relative to a LOCAL storage profile location | Path is grouped under that location's root | |
| 4 | Happy path | Multiple input files share a common parent directory | Grouped into a single `AssetRootGroup` with that common path as root | |
| 5 | Happy path | Input files have no common parent except filesystem root | Root path is the filesystem root | |
| 6 | Error handling | Input file does not exist and `require_paths_exist=true` | Returns error listing missing paths | |
| 7 | Happy path | Input file does not exist and `require_paths_exist=false` | Path is moved to referenced paths with a warning | |
| 8 | Error handling | Input path is a directory (classified as file) | Returns error listing misconfigured directories | |
| 9 | Boundary values | Empty input_paths, output_paths, and referenced_paths | Returns `AssetUploadGroup` with empty groups and zero totals | |
| 10 | Boundary values | Input paths contain empty strings | Empty strings are filtered out before processing | |
| 11 | Happy path | Output path is relative to a SHARED location | Output path is excluded from groups | |
| 12 | Happy path | Referenced path is relative to a SHARED location | Referenced path is excluded from groups | |

### `S3AssetManager.hash_assets_and_create_manifest(asset_groups, total_input_files, total_input_bytes, ...) -> tuple[SummaryStatistics, list[AssetRootManifest]]`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 13 | Happy path | Single asset group with input files | Returns summary statistics and one `AssetRootManifest` with a manifest | |
| 14 | Happy path | Asset group has output directories but no input files | Returns `AssetRootManifest` with `asset_manifest=None` and outputs listed | |
| 15 | Happy path | Multiple asset groups | Returns one `AssetRootManifest` per group | |
| 16 | Happy path | File is new (not in hash cache) | File is hashed, added to cache, and included in manifest | |
| 17 | Happy path | File is in hash cache and unmodified | Cached hash is used; file is not re-hashed | |
| 18 | Happy path | File is in hash cache but modified (different mtime) | File is re-hashed and cache is updated | |
| 19 | Concurrency/cancellation | `on_preparing_to_submit` callback returns False | Returns sync-canceled error with summary statistics | |
| 20 | Happy path | Progress tracker reports progress for each file | `on_preparing_to_submit` is called with progress updates | |
| 21 | Happy path | Manifest paths are stored as POSIX-style relative paths | Even on Windows, manifest paths use forward slashes | |
| 22 | Happy path | File mtime is stored as microseconds since epoch (integer) | Nanosecond mtime is truncated to microseconds | |

### `create_manifest_for_single_root(files, root, print_function_callback) -> optional manifest`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 23 | Happy path | Valid files and root directory | Returns a single `BaseAssetManifest` | |
| 24 | Boundary values | Empty file list | Returns none and calls callback with "No manifest generated" | |

---

## Section 21: Job attachments — upload

> **Rust crate:** `deadline-job-attachments` · **Module:** `upload`
>
> **Logic under test:** Uploading file data and manifests to S3 content-addressed storage.
> Small files uploaded in parallel, large files serially with multi-part upload.
> S3 check cache used to skip already-uploaded files. Snapshot mode copies to local dir
> instead of S3.

### `S3AssetManager.upload_assets(manifests, on_uploading_assets=None, s3_check_cache_dir=None, manifest_write_dir=None, force_s3_check=None) -> tuple[SummaryStatistics, Attachments]`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 1 | Happy path | Upload manifests with input files | Returns summary statistics and `Attachments` with manifest properties including S3 paths | |
| 2 | Happy path | Manifest has no input files (only output directories) | `inputManifestPath` and `inputManifestHash` are not set on the manifest properties | |
| 3 | Error handling | `farm_id` or `queue_id` is missing on the S3AssetManager | Returns error (job attachments) "Farm or Fleet ID is missing" | |
| 4 | Concurrency/cancellation | `on_uploading_assets` callback returns False mid-upload | Upload is cancelled; returns sync-canceled error with summary statistics | |
| 5 | Happy path | Multiple manifests with different root paths | Each manifest gets its own `ManifestProperties` entry in the returned `Attachments` | |
| 6 | Happy path | `force_s3_check=True` | S3 check cache is bypassed; all files are checked against S3 directly | |
| 7 | Happy path | `force_s3_check=False` (default) | S3 check cache is used to skip already-uploaded files | |

### `S3AssetManager.snapshot_assets(snapshot_dir, manifests, on_snapshotting_assets=None) -> tuple[SummaryStatistics, Attachments]`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 8 | Happy path | Snapshot manifests to a local directory | Files are copied to `snapshot_dir/Data/` and manifest is written to `snapshot_dir/Manifests/` | |
| 9 | Error handling | `farm_id` or `queue_id` is missing | Returns error (job attachments) | |
| 10 | Concurrency/cancellation | Callback returns False mid-snapshot | Returns sync-canceled error | |

### `S3AssetUploader.upload_input_files(manifest, s3_bucket, source_root, s3_cas_prefix, ...)`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 11 | Happy path | Manifest has small files only | All files uploaded in parallel | |
| 12 | Happy path | Manifest has large files only | Large files uploaded serially with parallel multi-part upload | |
| 13 | Happy path | Manifest has mix of small and large files | Small files uploaded in parallel first, then large files serially | |
| 14 | Happy path | File already exists in S3 (head_object succeeds) | File is skipped; progress tracker records it as skipped | |
| 15 | Happy path | File exists in S3 check cache | File is skipped without S3 API call | |
| 16 | Concurrency/cancellation | Progress tracker signals cancellation after last batch | Returns sync-canceled error | |

### `S3AssetUploader.upload_file_to_s3(local_path, s3_bucket, s3_upload_key, ...)`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 17 | Happy path | Valid file path | File is uploaded via TransferManager | |
| 18 | Boundary values | Path resolves to a directory | Upload is silently skipped | |
| 19 | Boundary values | Path does not exist | Upload is silently skipped | |
| 20 | Error handling | S3 returns 403 Forbidden (non-KMS) | Returns S3 client error with s3:PutObject guidance | |
| 21 | Error handling | S3 returns 403 Forbidden (KMS-related) | Returns S3 client error with KMS permission guidance | |
| 22 | Error handling | S3 returns 404 Not Found | Returns S3 client error with bucket/key guidance | |
| 23 | Error handling | Transport error during upload | Returns S3 transport error with credential/network guidance | |
| 24 | Concurrency/cancellation | Progress tracker signals cancellation mid-upload | Upload future is cancelled; returns sync-canceled error | |

### `S3AssetUploader.file_already_uploaded(bucket, key) -> bool`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 25 | Happy path | Object exists in S3 | Returns true | |
| 26 | Happy path | Object does not exist (404) | Returns false | |
| 27 | Error handling | S3 returns 403 | Returns S3 client error with s3:ListBucket guidance | |
| 28 | Error handling | BotoCoreError | Returns S3 transport error | |

---

## Section 22: Job attachments — download

> **Rust crate:** `deadline-job-attachments` · **Module:** `download`
>
> **Logic under test:** Downloading files from S3 CAS by hash, with conflict resolution
> (skip/overwrite/create copy), manifest merging, and output manifest retrieval organized
> by asset root. Retry logic for 404 with fallback key format (backward compat).

### `download_files_from_manifests(s3_bucket, manifests_by_root, cas_prefix=None, ...) -> DownloadSummaryStatistics`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 1 | Happy path | Single manifest with files | All files downloaded to local root; returns download summary statistics | |
| 2 | Happy path | Multiple manifests with different root paths | Files downloaded to their respective local root directories | |
| 3 | Happy path | `fs_permission_settings` is provided | Downloaded files have filesystem permissions applied | |
| 4 | Concurrency/cancellation | `on_downloading_files` callback returns False | Download is cancelled; returns sync-canceled error | |
| 5 | Happy path | `conflict_resolution` is `CREATE_COPY` and file exists | A copy with a new name is created | |
| 6 | Happy path | `conflict_resolution` is `SKIP` and file exists | File is skipped; returns `(file_bytes, None)` | |
| 7 | Happy path | `conflict_resolution` is `OVERWRITE` and file exists | Existing file is overwritten | |

### `download_file(file, hash_algorithm, local_download_dir, ...) -> (int, optional path)`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 8 | Happy path | File does not exist locally | File downloaded from S3; mtime set from manifest; returns `(file_bytes, local_path)` | |
| 9 | Happy path | S3 key uses format `{cas_prefix}/{hash}.{algorithm}` | Correct S3 key constructed for download | |
| 10 | Happy path | Parent directories don't exist | Created automatically before download | |
| 11 | Error handling | S3 returns 404 on first attempt | Retries with key without hash algorithm suffix (backward compatibility) | |
| 12 | Error handling | S3 returns 404 on both attempts | Returns S3 client error | |
| 13 | Error handling | S3 returns 403 (non-KMS) | Returns S3 client error with s3:GetObject guidance | |
| 14 | Error handling | S3 returns 403 (KMS-related) | Returns S3 client error with kms:Decrypt guidance | |
| 15 | Error handling | Transport error during download | Returns S3 transport error | |
| 16 | Concurrency/cancellation | Progress tracker signals cancellation mid-download | Returns sync-canceled error | |
| 17 | Error handling | Unknown `file_conflict_resolution` value | Returns error "Unknown choice" | |
| 18 | Happy path | File mtime from manifest (microseconds) is applied to downloaded file | Downloaded file's modification time matches the manifest value | |

### `merge_asset_manifests(manifests) -> BaseAssetManifest | None`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 19 | Happy path | Single manifest | Returns the same manifest | |
| 20 | Happy path | Two manifests with non-overlapping paths | Returns manifest with all paths combined | |
| 21 | Happy path | Two manifests with overlapping paths | Later manifest's version of the path wins | |
| 22 | Boundary values | Empty manifest list | Returns none | |
| 23 | Error handling | Manifests have different hash algorithms | Returns error (not implemented) | |
| 24 | Happy path | Merged manifest's `totalSize` is sum of unique paths' sizes | Correct total after deduplication | |

### `get_output_manifests_by_asset_root(s3_settings, farm_id, queue_id, job_id, ...) -> dict[str, list[BaseAssetManifest]]`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 25 | Happy path | Job has output manifests in S3 | Returns manifests grouped by asset root | |
| 26 | Happy path | `session_action_id` is provided with step_id and task_id | Returns manifests for that specific session action | |
| 27 | Error handling | `session_action_id` provided without step_id or task_id | Returns error (job attachments) "missing Step ID or Task ID" | |
| 28 | Happy path | No output manifests found in S3 | Returns empty dict | |
| 29 | Error handling | Manifest in S3 has no asset root metadata | Returns error (missing asset root) | |
| 30 | Happy path | Multiple manifests for same asset root | Merged chronologically by last modified time | |

### `get_job_input_output_paths_by_asset_root(s3_settings, attachments, farm_id, queue_id, job_id, ...) -> dict[str, ManifestPathGroup]`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 31 | Happy path | Job has both input and output files | Returns combined `ManifestPathGroup` per asset root | |
| 32 | Happy path | Input and output share the same asset root | Groups are combined into a single `ManifestPathGroup` | |
| 33 | Happy path | Input and output have different asset roots | Separate entries in the returned dict | |
| 34 | Boundary values | No inputs and no outputs | Returns empty dict | |

---

## Section 24: Job attachments — caches (hash & S3 check)

> **Rust crate:** `deadline-job-attachments` · **Module:** `caches`
>
> **Logic under test:** SQLite-backed caches for file hashes and S3 existence checks.
> Hash cache uses composite key (path, algorithm, range_start, range_end) with no eviction.
> S3 check cache uses 30-day expiry evaluated at lookup time. Both use WAL journal mode
> and retry with jitter on lock contention. Graceful degradation when SQLite unavailable.
> See [data_flow.md § Cache Structures](data_flow.md#cache-structures).

### `HashCache` (context manager)

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 1 | Happy path | Enter context manager with default cache dir | SQLite database created at `~/.deadline/cache/hash_cache.db` | |
| 2 | Happy path | Enter context manager with custom cache dir | Database created at the specified directory | |
| 3 | Happy path | Exit context manager | Database connection is closed | |
| 4 | Error handling | SQLite not available (ImportError) | Cache is disabled; all operations are no-ops | |
| 5 | Error handling | Database file is locked after retries | Returns error (job attachments) "Could not access cache file" | |
| 6 | Happy path | Database file exists but table doesn't | Table is created automatically | |

### `HashCache.put_entry(entry)`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 7 | Happy path | Insert a new whole-file hash entry | Entry is stored with `range_start=0`, `range_end=-1` | |
| 8 | Happy path | Insert a byte-range hash entry | Entry stored with specified range_start and range_end | |
| 9 | Happy path | Insert entry with same key (file_path + algorithm + range) | Existing entry is replaced | |
| 10 | Happy path | Cache is disabled | No-op; no error raised | |
| 11 | Happy path | File path contains surrogate characters | Path is encoded with surrogate pass handler and stored as blob | |

### `HashCache.get_entry(file_path_key, hash_algorithm, range_start=0, range_end=-1) -> optional cache entry`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 12 | Happy path | Entry exists for the given key | Returns `HashCacheEntry` with all fields | |
| 13 | Happy path | No entry for the given key | Returns none | |
| 14 | Happy path | Entry exists for whole-file but queried with byte range | Returns none (different composite key) | |
| 15 | Happy path | Cache is disabled | Returns none | |

### `HashCacheEntry`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 16 | Error handling | Byte-range entry with `range_end <= range_start` | Returns error (invalid value) | |

### `S3CheckCache` (context manager)

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 17 | Happy path | Enter context manager | SQLite database created at `~/.deadline/cache/s3_check_cache.db` | |
| 18 | Happy path | Exit context manager | Database connection is closed | |

### `S3CheckCache.put_entry(entry)`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 19 | Happy path | Insert a new entry | Entry is stored with s3_key and last_seen_time | |
| 20 | Happy path | Insert entry with same s3_key | Existing entry is replaced | |
| 21 | Happy path | Cache is disabled | No-op | |

### `S3CheckCache.get_entry(s3_key) -> optional cache entry`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 22 | Happy path | Entry exists and is less than 30 days old | Returns `S3CheckCacheEntry` | |
| 23 | Happy path | Entry exists but is older than 30 days | Returns none (expired) | |
| 24 | Happy path | No entry for the given key | Returns none | |
| 25 | Happy path | Cache is disabled | Returns none | |
| 26 | Error handling | Entry has invalid (non-numeric) timestamp | Returns none with a warning logged | |

### `CacheDB.remove_cache()`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 27 | Happy path | Cache file exists | File is deleted from disk; connections are closed | |
| 28 | Error handling | Cache file does not exist or cannot be removed | Raises the underlying OS exception | |

### `CacheDB.get_default_cache_db_file_dir() -> optional string`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 29 | Happy path | `HOME` env var is set | Returns `$HOME/.deadline/cache` | |

---

## Section 25: Job attachments — manifest formats & decode

> **Rust crate:** `deadline-job-attachments` · **Module:** `asset_manifests`
>
> **Logic under test:** xxh128 hashing, manifest version registry, JSON decode with
> validation (version, hashAlg, totalSize, paths), canonical JSON encoding (RFC 8785
> subset: compact, sorted keys, UTF-16 BE path sort, ASCII output), and manifest
> construction.
> See [data_flow.md § Asset Manifest Format](data_flow.md#asset-manifest-format).

### `HashAlgorithm` enum

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 1 | Happy path | `HashAlgorithm("xxh128")` | Returns `HashAlgorithm.XXH128` | |
| 2 | Error handling | `HashAlgorithm("sha256")` | Returns error (invalid value) (not a valid enum member) | |

### `hash_file(file_path, hash_alg) -> str`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 3 | Happy path | Hash a file with `XXH128` | Returns hexadecimal xxh3_128 hash string | |
| 4 | Happy path | Hash an empty file | Returns the xxh128 hash of empty input | |
| 5 | Happy path | Hash a large file (multi-chunk) | Correct hash computed across all chunks | |
| 6 | Error handling | Unsupported hash algorithm | Returns error (unsupported hash algorithm) | |

### `hash_data(data, hash_alg) -> str`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 7 | Happy path | Hash bytes with `XXH128` | Returns hexadecimal xxh3_128 hash string | |
| 8 | Happy path | Hash empty bytes `b""` | Returns hash of empty input | |
| 9 | Error handling | Unsupported hash algorithm | Returns error (unsupported hash algorithm) | |

### `ManifestVersion` enum

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 10 | Happy path | `ManifestVersion("2023-03-03")` | Returns `ManifestVersion.v2023_03_03` | |
| 11 | Happy path | `ManifestVersion("UNDEFINED")` | Returns `ManifestVersion.UNDEFINED` | For internal testing only |

### `ManifestModelRegistry`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 12 | Happy path | `get_manifest_model(version=ManifestVersion.v2023_03_03)` | Returns the v2023_03_03 ManifestModel | |
| 13 | Error handling | `get_manifest_model(version=ManifestVersion.UNDEFINED)` | Returns error "No model for asset manifest version" | |

### `decode_manifest(manifest_str) -> BaseAssetManifest`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 14 | Happy path | Valid JSON manifest with version `"2023-03-03"` | Returns decoded `AssetManifest` object | |
| 15 | Error handling | Missing `manifestVersion` field | Returns manifest decode error "missing the required 'manifestVersion' field" | |
| 16 | Error handling | Unknown `manifestVersion` value (e.g., `"2099-01-01"`) | Returns manifest decode error listing supported versions | |
| 17 | Error handling | Missing `hashAlg` field | Returns manifest decode error "missing required field(s)" | |
| 18 | Error handling | Missing `paths` field | Returns manifest decode error | |
| 19 | Error handling | Missing `totalSize` field | Returns manifest decode error | |
| 20 | Error handling | `paths` is empty list | Returns manifest decode error "must have at least one item" | |
| 21 | Error handling | `paths` is not a list | Returns manifest decode error "must be a list" | |
| 22 | Error handling | Path entry missing `hash` field | Returns manifest decode error "missing required field(s)" | |
| 23 | Error handling | Path entry `size` is not an integer | Returns manifest decode error "size must be an integer" | |
| 24 | Error handling | Path entry `hash` is not alphanumeric (e.g., contains `/`) | Returns manifest decode error "not alphanumeric" | |
| 25 | Error handling | `hashAlg` is not `"xxh128"` | Returns manifest decode error | |
| 26 | Error handling | `totalSize` is not an integer | Returns manifest decode error | |

### `AssetManifest.encode() -> str` (v2023_03_03)

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 27 | Happy path | Encode a manifest with multiple paths | Returns canonical JSON: no whitespace, sorted keys, compact separators | |
| 28 | Happy path | Paths are sorted by UTF-16 big-endian byte ordering | Paths with non-ASCII characters sort correctly | |
| 29 | Happy path | Output uses `ensure_ascii=True` | All non-ASCII characters are escaped | |
| 30 | Happy path | Encode a manifest with ASCII-only paths | Paths are sorted and encoded correctly | |
| 31 | Happy path | Encode a manifest with paths containing surrogate characters | Surrogate characters are handled during sort and encoding | |

### `AssetManifest.new(hash_alg, ...)` (v2023_03_03)

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 32 | Error handling | `hash_alg` not in `SUPPORTED_HASH_ALGS` | Returns manifest decode error | |

---

## Section 30: Job attachments: public API (attachment)

> **Rust crate:** `deadline-job-attachments` · **Module:** `api`
>
> **Logic under test:** Public API for downloading and uploading job attachments
> given pre-built manifest files, an S3 root URI, and optional path mapping rules.
> Download resolves each manifest's destination via hashed source path lookup in
> path mapping rules (falling back to current working directory). Upload resolves
> each manifest's source root the same way, uploads file content to a content-
> addressable store, optionally uploads the manifest itself, and returns per-manifest
> metadata. A helper reads and validates path mapping rules from a JSON file or
> from a list of root directories (but not both).

### `attachment_download(manifests, s3_root_uri, session, path_mapping_rules?, callback?, conflict_resolution?) -> download summary`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 1 | Happy path | Two manifests with matching path mapping rules | Files downloaded to each rule's destination path; returns download summary statistics | |
| 2 | Happy path | Manifest with no matching path mapping rule | Files downloaded to `<current_working_directory>/<manifest_filename>` | |
| 3 | Happy path | No path mapping rules provided (none) | All manifests download to current working directory partitioned by manifest filename | |
| 4 | Error handling | Manifest file path does not exist on disk | Returns error (invalid input: manifests not valid) | |
| 5 | Error handling | Two manifests resolve to the same destination path | Returns error (invalid input: destination already in use) | |
| 6 | Happy path | S3 root URI parsed into bucket name and root prefix | Download uses extracted bucket and CAS prefix | |
| 7 | Error handling | S3 root URI is malformed (not `s3://bucket/prefix`) | Returns error (malformed attachment setting) | |
| 8 | Happy path | conflict_resolution set to CREATE_COPY | Passed through to download; conflicting local files get a copy suffix | |
| 9 | Boundary values | Empty manifests list | No files downloaded; returns empty summary | |
| 10 | Happy path | Path mapping rule matched by hashed source path appearing in manifest filename | Correct destination selected based on hash match | |

### `attachment_upload(manifests, s3_root_uri, session, root_dirs?, path_mapping_rules?, manifest_path_mapping?, upload_manifest_path?, callback?) -> list of upload manifest info`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 11 | Happy path | Manifests with root_dirs provided (no path mapping) | Files uploaded to content-addressable store; returns list of upload info with manifest path, hash, and source path | |
| 12 | Happy path | Manifests with path_mapping_rules file provided (no root_dirs) | Source and destination resolved from mapping rules; files uploaded | |
| 13 | Error handling | Both path_mapping_rules and root_dirs provided | Returns error (invalid input: one must exist, not both) | |
| 14 | Error handling | Neither path_mapping_rules nor root_dirs provided | Returns error (invalid input: one must exist, not both) | |
| 15 | Error handling | Manifest filename does not match any rule's hashed source path | Returns error (invalid input: no valid root for manifest) | |
| 16 | Error handling | Manifest file path does not exist on disk | Returns error (invalid input: manifests not valid) | |
| 17 | Happy path | Source path is pure ASCII | S3 object metadata includes `asset-root` key with the raw path | |
| 18 | Happy path | Source path contains non-ASCII characters | S3 object metadata uses `asset-root-json` key with JSON-encoded ASCII value instead of `asset-root` | |
| 19 | Happy path | Rule has a source_path_format value | S3 object metadata includes `file-system-location-name` key | |
| 20 | Happy path | upload_manifest_path provided | Manifest file itself uploaded to S3 under the given prefix | |
| 21 | Happy path | upload_manifest_path not provided | Manifest file upload skipped | |
| 22 | Happy path | Multiple manifests provided | Returns one upload info entry per manifest in the same order as input | |
| 23 | Error handling | S3 root URI is malformed | Returns error (malformed attachment setting) | |
| 24 | Error handling | Root directory in root_dirs does not exist on disk | Returns error (invalid input: root dirs not valid) | |

### `process_path_mapping(path_mapping_rules?, root_dirs?) -> list of path mapping rules`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 25 | Happy path | Valid JSON file with top-level list of rule objects | Returns list of path mapping rules | |
| 26 | Happy path | Valid JSON file with rules nested under `path_mapping_rules` key | Extracts the nested list; returns path mapping rules | |
| 27 | Error handling | Path mapping file does not exist on disk | Returns error (invalid input: file not valid) | |
| 28 | Happy path | root_dirs provided with two valid directories | Returns two rules with source_path = destination_path = each directory, empty source_path_format | |
| 29 | Error handling | One of the root_dirs does not exist on disk | Returns error (invalid input: root dirs not valid) | |
| 30 | Boundary values | Neither path_mapping_rules nor root_dirs provided | Returns empty list | |
| 31 | Happy path | Both path_mapping_rules and root_dirs provided | Rules from file and rules from root_dirs are concatenated | ⚠️ Caller (`attachment_upload`) rejects this combination, but this helper allows it. Verify intent. |

### `read_manifests(manifest_paths) -> map of filename to decoded manifest`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 32 | Happy path | Two valid manifest file paths | Returns map keyed by base filename, values are decoded manifests | |
| 33 | Error handling | One path in the list does not exist | Returns error (invalid input: manifests not valid); error message includes the invalid paths | |
| 34 | Error handling | All paths in the list do not exist | Returns error listing all invalid paths | |
| 35 | Boundary values | Empty manifest_paths list | Returns empty map | |
| 36 | Error handling | File exists but contains invalid manifest content | Returns error from manifest decoding | |

> ✅ Complete (36 cases)

---

## Section 31: Job attachments: public API (manifest)

> **Rust crate:** `deadline-job-attachments` · **Module:** `api`
>
> **Logic under test:** Public API for manifest lifecycle operations: glob files from
> a directory, create a snapshot manifest (full or diff), diff a directory against a
> prior manifest, upload a manifest to S3, download manifests for a job (inputs,
> outputs, step dependencies), merge multiple manifests, and write a manifest to disk.
> All operations are session-agnostic and accept an explicit cloud session.

### `glob_files(root, include?, exclude?, include_exclude_config?) -> list of file paths`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 1 | Happy path | Root directory with files, no filters | Returns all files recursively (default include `**/*`) | |
| 2 | Happy path | Include pattern `*.exr` provided | Returns only `.exr` files | |
| 3 | Happy path | Include and exclude both provided | Returns files matching include but not matching exclude | |
| 4 | Happy path | include_exclude_config JSON string provided instead of include/exclude | Config parsed and used for glob | |
| 5 | Happy path | include/exclude provided alongside include_exclude_config | include/exclude take precedence; config is ignored | |
| 6 | Boundary values | Root directory is empty | Returns empty list | |

### `manifest_snapshot(root, destination, name, include?, exclude?, include_exclude_config?, diff?, force_rehash?, callback?) -> optional snapshot`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 7 | Happy path | Root with files, no diff manifest | Full manifest created from all matched files; written to destination; returns snapshot with root and manifest path | |
| 8 | Happy path | diff manifest path provided, force_rehash=false | Fast diff using file timestamps and sizes; only new/modified files included in output manifest | |
| 9 | Happy path | diff manifest path provided, force_rehash=true | Hash-based diff; only new/modified files included in output manifest | |
| 10 | Boundary values | No files match glob patterns | Returns none (no manifest generated) | |
| 11 | Boundary values | Diff produces zero changed files | Returns none | |
| 12 | Happy path | Manifest filename format | Written as `<name>-<root_hash>-<timestamp>.manifest` | |
| 13 | Happy path | Destination directory does not exist | Directory created before writing manifest | |

### `write_manifest(root, manifest, destination, name?) -> file path string`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 14 | Happy path | Name provided | Manifest written as `<name>-<root_hash>-<timestamp>.manifest` in destination | |
| 15 | Happy path | Name not provided | Name derived from root path with path separators and colons replaced by underscores, leading underscore stripped | |
| 16 | Happy path | Destination directory does not exist | Parent directories created before writing | |
| 17 | Happy path | Root path contains forward slashes, backslashes, and colons | All replaced with underscores in the derived name | |
| 18 | Happy path | On Windows, path exceeding max length limit | Long-path-compatible prefix applied to output path | |

### `manifest_diff(manifest, root, include?, exclude?, include_exclude_config?, force_rehash?, callback?) -> diff result`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 19 | Happy path | Files added since manifest was created, force_rehash=false | Fast diff returns new files in the `new` list | |
| 20 | Happy path | Files modified since manifest, force_rehash=false | Modified files in the `modified` list | |
| 21 | Happy path | Files deleted since manifest, force_rehash=false | Deleted files in the `deleted` list | |
| 22 | Happy path | force_rehash=true | Hash-based comparison; modified files detected by content hash change | |
| 23 | Boundary values | No differences between manifest and directory | All lists (new, modified, deleted) are empty | |
| 24 | Happy path | Include/exclude filters applied | Only files matching filters are considered in the diff | |

### `manifest_upload(manifest_file, s3_bucket_name, s3_cas_prefix, session, s3_key_prefix?, callback?)`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 25 | Happy path | s3_key_prefix provided | Manifest uploaded to `<cas_prefix>/Manifests/<key_prefix>/<filename>` | |
| 26 | Happy path | s3_key_prefix not provided | Manifest uploaded to `<cas_prefix>/Manifests/<filename>` | |
| 27 | Happy path | S3 metadata includes `file-system-location-name` set to the manifest file path | Metadata attached to the uploaded object | |
| 28 | Happy path | On Windows, manifest path exceeding max length | Long-path-compatible prefix applied before reading file | |

### `manifest_download(download_dir, farm_id, queue_id, job_id, session, step_id?, asset_type?, callback?) -> download response`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 29 | Happy path | Job with input manifests, asset_type=ALL, no step_id | Input manifests downloaded, merged by root, written to download_dir; returns download response | |
| 30 | Happy path | Job with output manifests, asset_type=ALL, no step_id | Output manifests for all steps downloaded, merged by root, written to download_dir | |
| 31 | Happy path | asset_type=INPUT | Only input manifests downloaded; output manifests skipped | |
| 32 | Happy path | asset_type=OUTPUT | Only output manifests downloaded; input manifests skipped | |
| 33 | Happy path | step_id provided with asset_type=ALL | Input manifests downloaded; step-step dependency manifests for the given step downloaded; output manifests for only that step downloaded | |
| 34 | Happy path | Step has multiple step-step dependencies | Dependency manifests from all dependent steps collected and merged by root | |
| 35 | Pagination/batching | Step dependencies response is paginated | All pages followed via next token until exhausted | |
| 36 | Happy path | Multiple manifests share the same root path | Manifests merged into a single manifest per root before writing to disk | |
| 37 | Boundary values | Job has no attachments | No manifests downloaded; returns empty download response | |
| 38 | Happy path | Merged manifest written with filename `<root>-<hash>-<timestamp>.manifest` | Root path separators replaced with underscores; leading underscore stripped | |
| 39 | Happy path | Queue role assumed before accessing S3 | Download uses queue-scoped credentials, not the caller's session directly | |
| 40 | Happy path | Download response contains one entry per unique root | Each entry has manifest_root and local_manifest_path | |

### `manifest_merge(root, manifest_files, destination, name?, callback?) -> optional merge result`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 41 | Happy path | Two valid manifest files | Manifests merged; result written to destination; returns merge result with root and local path | |
| 42 | Boundary values | Merge produces an empty manifest (no paths) | Returns none | |
| 43 | Error handling | One manifest file does not exist | Returns error (invalid input: manifests not valid) | |
| 44 | Happy path | Name provided | Output filename uses provided name | |
| 45 | Happy path | Name not provided | Output filename derived from root path | |

> ✅ Complete (45 cases)

---

## Section 34: Job attachments: AWS client helpers

> **Rust crate:** `deadline-job-attachments` · **Module:** (new) `aws_clients` or inline in `upload`/`download`
>
> **Logic under test:** Factory functions for creating cloud service clients (S3,
> STS, Deadline) with correct configuration, session management with caching,
> S3 client configuration (timeouts, retries, signature version, connection pool
> size from config, expected-bucket-owner injection), caller identity retrieval,
> and Deadline API wrappers for fetching queue and job resources with attachment
> metadata parsing.

### Configuration constants

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 1 | Happy path | S3 connect timeout constant | Value is 30 seconds | |
| 2 | Happy path | S3 read timeout constant | Value is 30 seconds | |
| 3 | Happy path | S3 retries mode constant | Value is `"standard"` | |

### `get_botocore_session() -> session`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 4 | Happy path | Called with defaults | Returns a session with STS regional endpoints set to `"regional"` | |
| 5 | Happy path | Called with defaults | Returns a session with S3 us-east-1 regional endpoint set to `"regional"` | |

### `get_boto3_session(botocore_session?) -> session`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 6 | Happy path | Called without arguments | Returns a session wrapping the default botocore session | |
| 7 | Happy path | Called twice with same arguments | Returns the same cached session instance | |

### `get_deadline_client(session?, endpoint_url?) -> client`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 8 | Happy path | Session and endpoint_url provided | Returns a Deadline service client using the given session and endpoint | |
| 9 | Happy path | Session is none | Uses the default cached session | |
| 10 | Happy path | Called twice with same arguments | Returns the same cached client instance | |

### `get_s3_client(session?) -> client`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 11 | Happy path | Session provided | Returns an S3 client with signature version `s3v4`, configured timeouts, and standard retry mode | |
| 12 | Happy path | Session is none | Uses the default cached session | |
| 13 | Config interaction | `s3_max_pool_connections` config setting is 20 | Client created with max pool connections of 20 | |
| 14 | Happy path | S3 client user agent | User agent string includes `S3A/Deadline/NA/JobAttachments/<version>` | |
| 15 | Happy path | Any S3 API call that supports `ExpectedBucketOwner` | Parameter automatically injected with the caller's account ID | |
| 16 | Happy path | S3 API call that does not support `ExpectedBucketOwner` | Parameter not injected; call proceeds normally | |

### `get_s3_max_pool_connections() -> integer`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 17 | Happy path | Config setting `s3_max_pool_connections` is `"10"` | Returns 10 | |
| 18 | Error handling | Config setting is not a valid integer (e.g., `"abc"`) | Returns error (asset sync: failed to parse config) | |
| 19 | Error handling | Config setting is `"0"` | Returns error (asset sync: value must be positive) | |
| 20 | Error handling | Config setting is `"-5"` | Returns error (asset sync: value must be positive) | |

### `get_s3_transfer_manager(s3_client) -> transfer manager`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 21 | Happy path | Valid S3 client provided | Returns a transfer manager with default transfer config | |
| 22 | Happy path | Called twice with same client | Returns the same cached transfer manager | |

### `get_sts_client(session?) -> client`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 23 | Happy path | Session provided | Returns an STS client for the given session | |
| 24 | Happy path | Session is none | Uses the default cached session | |

### `get_caller_identity(session?) -> identity map`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 25 | Happy path | Valid credentials | Returns map containing Account, Arn, and UserId | |
| 26 | Happy path | Called twice with same session | Returns the same cached result | |

### `get_account_id(session?) -> string`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 27 | Happy path | Valid credentials | Returns the Account field from caller identity | |

### `get_queue(farm_id, queue_id, session?, endpoint_url?) -> Queue`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 28 | Happy path | Queue has job attachment settings with s3BucketName and rootPrefix | Returns Queue with populated job attachment settings | |
| 29 | Happy path | Queue has empty job attachment settings (no s3BucketName) | Returns Queue with job attachment settings as none | |
| 30 | Happy path | API response uses `name` key instead of `displayName` | Display name read from `name` field | ⚠️ Code handles both key names; verify which API version uses which. |
| 31 | Happy path | API response uses `state` key instead of `status` | Status read from `state` field | ⚠️ Same backward-compatibility handling as above. |
| 32 | Error handling | Deadline API call fails (service error) | Returns error (job attachments: failed to get queue) | |

### `get_job(farm_id, queue_id, job_id, session?, endpoint_url?) -> Job`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 33 | Happy path | Job has attachments with manifests | Returns Job with Attachments containing ManifestProperties list | |
| 34 | Happy path | Job has no attachments | Returns Job with attachments as none | |
| 35 | Happy path | Attachment manifest has optional fields (fileSystemLocationName, outputRelativeDirectories, inputManifestPath) | Optional fields populated when present, none when absent | |
| 36 | Happy path | Attachments missing `fileSystem` key | Defaults to COPIED file system mode | |
| 37 | Error handling | Deadline API call fails (service error) | Returns error (job attachments: failed to get job) | |

> ✅ Complete (37 cases)

---

## Section 35: Job attachments: incremental downloads

> **Rust crate:** `deadline-job-attachments` · **Module:** (new) `incremental_download`
>
> **Logic under test:** Incremental download state tracking (persisted to a JSON
> file) and the S3 manifest download pipeline that supports it. State tracks per-job
> download progress at three levels: job → session → session action index. The
> download pipeline retrieves output manifests from S3, resolves absolute local
> paths (with optional path mapping), merges manifests by timestamp order, downloads
> files from a content-addressable store with conflict resolution, and reports
> progress with cancellation support.

### `IncrementalDownloadJob(job, session_ended_timestamp, session_completed_indexes)`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 1 | Happy path | Construct with job dict, timestamp, and indexes | All fields stored; datetime values in job dict converted to ISO strings | |
| 2 | Happy path | session_completed_indexes is none | Stored as empty map | |
| 3 | Happy path | `job_id` property | Returns the `jobId` field from the stored job dict | |

### `IncrementalDownloadJob.from_dict(data) / to_dict()`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 4 | Happy path | Round-trip: construct, to_dict, from_dict | Produces equivalent object | |
| 5 | Error handling | Input is not a dict | Returns error (input must be a dict) | |
| 6 | Error handling | Input dict missing required `job` field | Returns error listing missing fields | |
| 7 | Happy path | Input dict missing optional `sessionEndedTimestamp` | Field set to none | |
| 8 | Happy path | Input dict missing optional `sessionCompletedIndexes` | Field set to empty map | |
| 9 | Happy path | `to_dict` with session_ended_timestamp as none | Output dict omits `sessionEndedTimestamp` key | |
| 10 | Happy path | `to_dict` with empty session_completed_indexes | Output dict omits `sessionCompletedIndexes` key | |

### `IncrementalDownloadState(local_storage_profile_id, downloads_started_timestamp, ...)`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 11 | Happy path | Construct with only required fields | `downloads_completed_timestamp` defaults to `downloads_started_timestamp`; jobs defaults to empty list | |
| 12 | Happy path | Construct with all fields | All fields stored as provided | |
| 13 | Happy path | `local_storage_profile_id` is none | Stored as none (ignore-storage-profiles mode) | |

### `IncrementalDownloadState.from_dict(data) / to_dict()`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 14 | Happy path | Round-trip: construct, to_dict, from_dict | Produces equivalent object | |
| 15 | Error handling | Input is not a dict | Returns error (input must be a dict) | |
| 16 | Error handling | Input dict missing required fields | Returns error listing all missing field names | |
| 17 | Happy path | Jobs list contains multiple entries | Each entry deserialized as IncrementalDownloadJob | |

### `IncrementalDownloadState.from_file(file_path) / save_file(file_path)`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 18 | Happy path | Save then load from same path | Loaded state matches saved state | |
| 19 | Happy path | Save to path whose parent directory does not exist | Parent directories created before writing | |
| 20 | Happy path | File write is atomic | Writes to a temporary file first, then atomically replaces the target | |
| 21 | Error handling | Load from nonexistent file | Returns OS-level error (file not found) | |
| 22 | Error handling | File contains invalid JSON | Returns error (JSON parse failure) | |

### `add_output_manifests_from_s3(farm_id, queue, job, session, session_action_list)`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 23 | Happy path | Session actions lack manifest fields | Manifest S3 keys retrieved from S3 and matched to session actions by session action ID and root path hash | |
| 24 | Happy path | Session action already has manifests field | That session action is skipped | |
| 25 | Happy path | Job has no attachments | Returns immediately without modifying session actions | |
| 26 | Happy path | All session actions already have manifests | Returns immediately after filtering | |
| 27 | Error handling | Manifest S3 key does not contain a session action ID | Returns error (key lacks session action ID) | |
| 28 | Error handling | Manifest S3 key does not match any root path hash | Returns error (key does not contain any root path hashes) | |
| 29 | Happy path | No manifests found in S3 for the job | Returns without error; session actions unchanged | |

### `download_all_manifests_with_absolute_paths(queue, jobs, job_sessions, path_mapping_rule_appliers, output_unmapped_paths, session, callback?) -> list of (timestamp, manifest)`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 30 | Happy path | Multiple manifests across multiple jobs | All manifests downloaded concurrently; paths made absolute by joining with root path | |
| 31 | Happy path | Path mapping rule applier provided | Manifest paths transformed through path mapping; unmapped paths recorded in output_unmapped_paths | |
| 32 | Happy path | No path mapping (empty appliers map) | Paths joined with root path using host OS path conventions | |
| 33 | Happy path | Source path format is Windows | Paths joined and normalized using Windows path conventions | |
| 34 | Happy path | Path mapping fails for some paths (no matching rule) | Those paths excluded from manifest; recorded as unmapped | |

### `merge_absolute_path_manifest_list(downloaded_manifests) -> list of manifest paths`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 35 | Happy path | Two manifests with non-overlapping files | All paths included in merged result | |
| 36 | Happy path | Two manifests with same file path, different timestamps | Later manifest's version of the file wins | |
| 37 | Happy path | File paths differing only in case | Treated as same file (case-insensitive merge key) | |
| 38 | Boundary values | Empty manifest list | Returns empty list | |
| 39 | Happy path | Manifests sorted by last-modified timestamp before merging | Earlier manifests processed first so later ones overwrite | |

### `download_file(file, hash_algorithm, collision_lock, collision_file_dict, s3_bucket, cas_prefix, s3_client, session, progress_tracker, file_conflict_resolution)`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 40 | Happy path | File does not exist locally | File downloaded from `<cas_prefix>/<hash>.<algorithm>`; modified time set from manifest | |
| 41 | Happy path | File larger than 1 MB | Downloaded using multi-part transfer manager | |
| 42 | Happy path | File 1 MB or smaller | Downloaded using single get-object call | |
| 43 | Happy path | File exists locally, conflict_resolution=SKIP | File not downloaded; returns immediately | |
| 44 | Happy path | File exists locally, conflict_resolution=OVERWRITE | Existing file overwritten with downloaded content | |
| 45 | Happy path | File exists locally, conflict_resolution=CREATE_COPY | New file created with a copy suffix; collision tracked in shared dict | |
| 46 | Error handling | Unknown conflict_resolution value | Returns error (unknown choice) | |
| 47 | Happy path | Parent directory does not exist | Created recursively before download | |
| 48 | Happy path | After download, file modified time set from manifest's mtime field | mtime is in microseconds; converted to seconds for the OS call | |
| 49 | Error handling | Downloaded file size does not match manifest size | Returns error (incorrect size) | |
| 50 | Error handling | S3 returns 403 (non-KMS related) | Returns error with guidance about GetObject permission | |
| 51 | Error handling | S3 returns 403 (KMS related) | Returns error with guidance about Decrypt and DescribeKey permissions | |
| 52 | Error handling | S3 returns 404 | Returns error with guidance about bucket name and object key | |
| 53 | Error handling | S3 returns 408, 500, or 503 | Returns error with appropriate retry/network guidance | |
| 54 | Error handling | Generic cloud SDK transport error (not a client error) | Returns error with credentials/network guidance | |
| 55 | Concurrency/cancellation | Progress callback returns false during multi-part download | Download future cancelled; returns error (download cancelled) | |
| 56 | Concurrency/cancellation | Progress callback returns false during single get-object download | Partial file deleted; returns error (download cancelled) | |

### `download_manifest_paths(manifest_paths, hash_algorithm, queue, session, file_conflict_resolution, on_downloading_files, callback?)`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 57 | Happy path | List of manifest paths provided | All files downloaded concurrently; progress reported after each file | |
| 58 | Happy path | Progress reported at 100% after all files complete | Final progress report always fires | |
| 59 | Concurrency/cancellation | on_downloading_files callback returns false | Downloads cancelled via progress tracker | |
| 60 | Boundary values | Empty manifest_paths list | No downloads attempted; progress reports 100% immediately | |

### Constants

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 61 | Happy path | Eventual consistency max seconds | Value is 120 | |

> ✅ Complete (61 cases)

---
