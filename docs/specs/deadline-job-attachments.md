# deadline-job-attachments

Asset manifest handling, S3 upload/download, hash cache, content-addressed
storage.

## Status: In Progress (batch 8a)

Batch 8a implements the foundational types, hashing, manifest encode/decode,
and caches. Upload, download, and orchestration follow in work items #9–#10.

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

#### `SummaryStatistics`

Struct with: `total_time`, `total_files`, `total_bytes`, `processed_files`,
`processed_bytes`, `skipped_files`, `skipped_bytes`, `transfer_rate`.
`aggregate()` combines two instances.

#### `DownloadSummaryStatistics`

Extends `SummaryStatistics` (via composition) with
`file_counts_by_root_directory` and `downloaded_files`.

#### `ProgressTracker`

Struct holding:
- Callback: `Option<Box<dyn Fn(ProgressReportMetadata) -> bool + Send>>`
- Counters: processed/skipped files and bytes
- Timing: uses `Instant` (monotonic) instead of Python's `time.perf_counter()`
- Thread safety: `Mutex` around mutable state

The callback returns `false` to signal cancellation. The tracker sets
`continue_reporting = false` and subsequent calls to `report_progress()`
return `false` immediately.

Reporting fires when: (a) the time interval has elapsed (1s default),
(b) a chunk of files has completed (50 default), or (c) progress reaches
100%.

---

### `upload` — Path grouping and manifest creation (batch 8c)

Not yet implemented. Will contain `prepare_paths_for_upload`,
`hash_assets_and_create_manifest`, and S3 upload logic.

### `download` — File download from S3 CAS (work item #9)

Not yet implemented.

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
