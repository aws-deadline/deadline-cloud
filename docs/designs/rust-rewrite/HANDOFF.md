# Handoff

Current in-flight work for the rust-rewrite. Read this at the start of
every session before consulting the Work Items table in `README.md`.

## Active Work Item

**#9 — Job attachments: transfer** (pre-9e fixes)

### Current Step

Pre-9e fixes complete. Proceeding to batch 9e.

**Fix #1 — `merge_asset_manifests` returns `Result`:**
Changed return type from `Option<AssetManifest>` to
`Result<Option<AssetManifest>, JobAttachmentsError>`. Mismatched hash
algorithms now return `Err` instead of silent `None`. Updated internal
caller in `get_output_manifests_by_asset_root` and all tests. Added
two new tests (`merge_empty_list_returns_ok_none`,
`merge_single_manifest_returns_ok_some`).

**Fix #2 — `upload_bytes_to_s3` S3 error guidance:**
Extracted `s3_upload_error` shared helper with HTTP status-specific
guidance (403 KMS/non-KMS, 404, 408, 500, 503). Refactored
`upload_file_to_s3` to use it (dedup ~50 lines). Updated
`upload_bytes_to_s3` to use the same helper instead of generic
`S3BotoCore`. Added 3 new tests asserting guidance messages.

**Files changed:**
- `crates/deadline-job-attachments/src/download.rs` — `merge_asset_manifests` signature + caller
- `crates/deadline-job-attachments/src/upload.rs` — `s3_upload_error` helper, refactored both upload functions
- `crates/deadline-job-attachments/tests/download_tests.rs` — updated 6 existing + added 2 new merge tests
- `crates/deadline-job-attachments/tests/upload_s3_tests.rs` — added 3 new upload_bytes error tests
- `docs/specs/deadline-job-attachments.md` — updated merge and upload_bytes specs
- `docs/designs/rust-rewrite/README.md` — added #15e behavioral parity audit work item
- `docs/designs/rust-rewrite/HANDOFF.md` — this file

**Test count:** 231 tests in deadline-job-attachments (228 existing + 2 merge + 3 upload_bytes error guidance). 0 failures.

### What's Next

Batch 9e: CLI commands (`deadline attachment download/upload`,
`deadline manifest snapshot/diff/download/upload`) and manifest API
library functions (`glob_files`, `manifest_snapshot`, `manifest_diff`,
`manifest_upload`, `manifest_download`, `manifest_merge`,
`write_manifest`). §31: 45 cases, §46: 15 cases, §47: 26 cases.

### Batch 9d — Step 6 Audit

**Test gap analysis (§30):** Found 10 missing test cases. Added 8 new
tests covering cases 6, 8, 10, 12, 17, 20, 21, 22. Cases 18-19
(non-ASCII metadata, file-system-location-name) are metadata assertions
that require wiremock request inspection — deferred to CLI-level tests
in batch 9e where the full flow is exercised.

**Bug fix:** `attachment_upload` was passing `rule.source_path` to
`upload_input_files` as the local root. Python passes
`rule.destination_path` (the local machine path after mapping).
`source_path` is the original submitting machine's path. Fixed.

**Test cleanup fix:** Tests using the cwd fallback path (cases 2, 3)
were creating directories in the crate root (`m.manifest/`,
`m1.manifest/`, `unmatched_manifest/`). Fixed by:
- Using unique PID-based manifest names to avoid parallel test collisions
- Adding explicit cleanup (`fs::remove_dir_all`) after each test
- Restructuring case 6 to use path mapping rules (it tests S3 URI
  parsing, not the cwd fallback)

**Test count:** 26 → 34 tests. Full crate: 228 tests, 0 failures,
0 leftover files.

### Batch 9c — Step 4 Implementation

Added `download.rs` module to `deadline-job-attachments` with:

**Public functions:**
- `merge_asset_manifests(&[AssetManifest]) -> Option<AssetManifest>` —
  merges manifests, later paths win, recalculates total_size.
- `download_file(...)` — async, downloads single file from CAS with
  conflict resolution (Skip/Overwrite/CreateCopy), 404 retry without
  algorithm suffix, KMS/non-KMS error guidance, mtime from manifest.
- `download_files_from_manifests(...)` — async, iterates manifests by
  root, downloads all files, returns DownloadSummaryStatistics.
- `get_output_manifests_by_asset_root(...)` — async, lists S3 objects,
  downloads manifests, groups by asset root, merges chronologically.

**Public types:**
- `CollisionState` — `Arc<Mutex<HashMap<String, i32>>>` for CreateCopy
  collision tracking.
- `FileConflictResolution` enum added to `models.rs`.

**Internal helpers:**
- `s3_download_error` — builds S3Client/S3BotoCore errors matching
  upload patterns exactly (403/404/408/500/503 guidance).
- `s3_get_object_bytes` — GetObject with KMS detection via both
  Display and message() (matching upload's ProvideErrorMetadata pattern).
- `get_new_copy_file_path` — atomic CreateCopy via OpenOptions::create_new.
- `get_asset_root_from_metadata` — prefers asset-root-json, falls back
  to asset-root.
- `get_output_manifest_prefix` — builds S3 prefix from IDs.
- `list_manifest_keys_from_s3` — paginated ListObjectsV2.
- `select_latest_manifests_per_task` — alphabetical sort of
  timestamp_sessionaction_id folders.
- `download_manifest_from_s3` — GetObject + decode + metadata extraction.
- `get_manifests_by_session_action_id` — regex search in task then step
  prefix.

**Other changes:**
- `HashAlgorithm::as_str()` added to `asset_manifests.rs`.
- `ProgressTracker::processed_files()` accessor added.
- Dependencies: `filetime`, `regex` added to Cargo.toml.

All 21 download tests pass. 149 lib tests pass. 24 upload tests pass.
Full workspace green (0 failures).

### Batching Plan

| Batch | Scope | Test Spec | Cases |
|-------|-------|-----------|-------|
| 9a | S3 client infrastructure + aws-sdk-s3 | §34 | 37 |
| 9b | Upload engine (S3UploadContext, upload_assets) | §21 | 28 |
| 9c | Download engine (download_file, merge, output manifests) | §22 | 34 |
| 9d | Public API (attachment_download/upload, path mapping) | §30 | 36 |
| 9e | CLI commands + manifest API | §31, §46 | 60 |

### What's Next

Batch 9d complete. Proceed to batch 9e: CLI commands
(`deadline attachment download`, `deadline attachment upload`) and
manifest API functions (`glob_files`, `manifest_snapshot`,
`manifest_diff`, `manifest_upload`, `manifest_download`,
`manifest_merge`, `write_manifest`). §31: 45 cases, §46: 15 cases.

### Batch 9d — Complete Summary

**New files:**
- `crates/deadline-job-attachments/src/api.rs` — public API module
- `crates/deadline-job-attachments/tests/api_tests.rs` — 34 Level 1 tests

**Modified files:**
- `crates/deadline-job-attachments/src/lib.rs` — added `pub mod api`
- `crates/deadline-job-attachments/src/models.rs` — added `UploadManifestInfo`
- `docs/specs/deadline-job-attachments.md` — added `api` module section,
  updated status, updated Known Gaps
- `docs/designs/rust-rewrite/HANDOFF.md` — updated throughout

**Public API:**
- `read_manifests(manifest_paths) -> HashMap<String, AssetManifest>`
- `process_path_mapping(path_mapping_rules?, root_dirs) -> Vec<PathMappingRule>`
- `attachment_download(manifests, s3_root_uri, s3_client, account_id,
  path_mapping_rules?, callback?, conflict_resolution) -> DownloadSummaryStatistics`
- `attachment_upload(manifests, s3_root_uri, s3_client, account_id,
  root_dirs, path_mapping_rules?, upload_manifest_path?, callback?,
  config?) -> Vec<UploadManifestInfo>`

**Test coverage:** 34 tests covering §30 cases 1-17, 20-36.
Cases 18-19 (non-ASCII metadata, file-system-location-name wiremock
request inspection) deferred to CLI-level tests in batch 9e.

**Bug fixed:** `attachment_upload` was passing `rule.source_path` to
`upload_input_files` instead of `rule.destination_path`.

**Test cleanup:** Added `CleanupDir` RAII guard for tests that write
to cwd (Drop runs on panic, like pytest fixtures). Audited all test
files across workspace — no other leaks found.

Full crate: 228 tests, 0 failures, 0 leftover files.

### Batch 9d — Step 2 Crate Spec Update

Updated `docs/specs/deadline-job-attachments.md`:
- Status line updated to include batch 9d
- Added `api` module section with design rationale, `UploadManifestInfo`
  type, and all four function signatures with behavior descriptions
- Removed `UploadManifestInfo`, `ManifestPathGroup`, `OutputFile`, and
  `_get_unique_dest_dir_name` from Known Gaps (addressed by 9d or no
  longer needed as separate types)

### Batch 9d — Step 1 Review

Audited existing `upload.rs`, `download.rs`, `models.rs` against Python
source for the functions batch 9d builds on. No behavior gaps found:

- `upload_assets` correctly passes `None` for manifest metadata (matches
  Python's `S3AssetManager.upload_assets` which also omits metadata;
  metadata is only passed in the `_attachment_upload` path, which is
  batch 9d scope)
- `download_files_from_manifests` signature and behavior match Python
- `merge_asset_manifests` correctly handles empty/single/multiple cases
- `PathMappingRule.get_hashed_source_path` matches Python's UTF-8 hash
- `JobAttachmentS3Settings.from_s3_root_uri` parsing matches Python
- `ManifestProperties.as_output_metadata` handles ASCII/non-ASCII correctly
- S3 error guidance messages match Python
- `FileConflictResolution` all three variants correct

No improvements needed. Proceeding to Step 2.

### Batch 9b — Steps 5-6: Python Verification + Refactor

**Step 5 — Python behavioral comparison:**

Systematically compared each function against Python source. Found
three behavioral gaps:

1. **Upload order wrong** — Rust uploaded files before manifest. Python
   uploads manifest first so it's available in S3 even if file upload
   fails partway. Fixed: reordered.

2. **Missing `verify_hash_cache_integrity`** — Python samples up to 30
   S3 check cache entries before uploading, resets cache if any are
   missing from S3. Rust had no integrity check. Fixed: added methods
   and called from `upload_assets`.

3. **Missing 408/500/503 error guidance** — Python's
   `COMMON_ERROR_GUIDANCE_FOR_S3` provides specific messages. Fixed:
   added status-specific S3Client errors matching Python's wording.

**Step 6 — Tightened wiremock matchers on 5 tests:**

- `upload_assets` happy path: manifest PUT path regex, requires
  `x-amz-expected-bucket-owner` header
- `upload_input_files` small files: PUT path matches CAS key format
- `upload_input_files` existing file: HEAD path matches CAS key format
- `upload_file_to_s3` valid file: requires `x-amz-expected-bucket-owner`
- `upload_assets` multiple manifests: requires
  `x-amz-expected-bucket-owner`

### Batch 9b — Step 4 Implementation

Added to `upload.rs`:

- `S3UploadContext` struct — holds `s3_client`, `account_id`,
  `small_file_threshold`, `num_upload_workers`. Constructed via `new()`
  which reads config via `compute_upload_config()`.
- `S3UploadContext::file_already_uploaded` — async HeadObject with
  HTTP status extraction from `raw_response()`. 404 → false, 403 →
  S3Client error with ListBucket guidance, other → S3BotoCore.
- `S3UploadContext::upload_file_to_s3` — async PutObject. Rejects
  symlinks via `symlink_metadata()`. Skips dirs/nonexistent silently.
  403 error checks both Display and `message()` for KMS detection.
- `S3UploadContext::upload_bytes_to_s3` — async PutObject for manifests
  with ExpectedBucketOwner and optional metadata.
- `S3UploadContext::upload_input_files` — iterates manifest paths,
  checks S3 check cache, HeadObject, uploads, updates cache. Final
  progress report + cancellation check.
- `upload_assets` free function — validates farm/queue, iterates
  manifests, builds ManifestProperties, calls uploader, returns
  (SummaryStatistics, Attachments).
- `snapshot_assets` free function — same pattern with local fs::copy.

Key implementation decisions:
- HTTP status extracted from `sdk_err.raw_response().status()` instead
  of string matching — reliable across SDK versions.
- KMS detection checks both `format!("{service_err}")` and
  `service_err.message()` since SDK Display may not include the raw
  XML message body.
- `ProvideErrorMetadata` trait imported for `.message()` access.

All 24 integration tests pass. 143 existing lib tests pass. Full
workspace green (0 failures, 0 warnings).

### Batch 9b — Step 1 Improvements

Audited existing `upload.rs`, `s3.rs`, and `errors.rs` against Python
source. Applied three improvements:

1. **Fixed S3Client error Display format** — was `"S3 {action} failed
   ({status_code}) bucket={bucket} key={key}: {msg}"`, now matches
   Python: `"Error {action} in bucket '{bucket}', Target key or prefix:
   '{key}', HTTP Status Code: {status_code}, {msg}"`.

2. **Fixed S3BotoCore error Display format** — was `"S3 {action}:
   {details}"`, now matches Python with full guidance text about
   credentials, network, and retry.

3. **Added `get_small_file_threshold_multiplier` and
   `compute_upload_config` to `s3.rs`** — the upload engine needs both
   `small_file_threshold` and `num_upload_workers` computed from config.
   Added helpers with validation (positive integer check) and 5 tests.

### Batch 9a — What Was Done

Added `s3.rs` module to `deadline-job-attachments` with S3 client
infrastructure:
- Constants: timeouts (30s), retry mode (standard), chunk size (8MB),
  concurrency limits (10), user agent string
- `build_s3_client(sdk_config, config)` — builds S3 client with
  job-attachments-specific config on top of caller's credentials.
  Validates pool connections at build time (warns on misconfiguration).
- `get_s3_max_pool_connections(config)` — reads and validates config.
  Propagates config read errors (does not silently default).
- `get_account_id(sdk_config)` — STS GetCallerIdentity wrapper.
  No caching — callers should cache the result.
- 14 Level 1 tests, all passing

Refactors applied:
- Removed dead `pool_connections` computation from `build_s3_client`
  (was computed then discarded; callers use `get_s3_max_pool_connections`
  directly for worker counts)
- Fixed `get_s3_max_pool_connections` to propagate config read errors
  instead of silently falling back to "50"
- Added doc comments about caching responsibility on `get_account_id`
- Clarified `S3_RETRIES_MODE` constant purpose (behavioral parity
  verification, not used in implementation)

New dependencies: `aws-sdk-s3`, `aws-sdk-sts`, `aws-config`, `tokio`.

## Recently Completed

**#8 — Job attachments: core** (all batches complete, marked ✅ Done)

Batch 8a: foundational types, hashing, manifest encode/decode, caches
(80 Level 1 tests). Batch 8b: progress tracker (17 Level 1 tests).
Batch 8c: path grouping and manifest creation (21 Level 1 tests).
Post-batch improvements: session action manifest prefix methods,
StorageProfile full fields, float_to_iso_datetime_string (6 tests).
Total: 124 tests.

**Files changed (8b/8c batch):**
- `crates/deadline-job-attachments/src/progress_tracker.rs` — ProgressStatus,
  ProgressReportMetadata, SummaryStatistics (with Display),
  DownloadSummaryStatistics, ProgressTracker with callback-based reporting
  and cancellation
- `crates/deadline-job-attachments/src/upload.rs` — prepare_paths_for_upload
  (path grouping with storage profile support),
  hash_assets_and_create_manifest (hash cache integration, progress
  reporting, cancellation)
- `crates/deadline-job-attachments/src/models.rs` — added
  FileSystemLocationType, FileSystemLocation, StorageProfile,
  AssetRootGroup, AssetUploadGroup, AssetRootManifest
- `docs/specs/deadline-job-attachments.md` — updated status, expanded
  progress_tracker and upload module specs
- `docs/designs/rust-rewrite/README.md` — #8 marked ✅ Done

**Test spec coverage:** §19 (30), §20 (21), §24 (20), §25 (30), §32 (17)

**#12b — Job search**, **#15b — Job get search term**,
**#15c — Job logs auto-selection messages** (batched, marked ✅ Done)

**#7 — Job bundle** (marked ✅ Done)

**#12 — Job cancel** and **#15 — Job requeue-tasks** (marked ✅ Done)

**#4 — Queue parameters** (marked ✅ Done)

**#6 — Job monitoring & logs** (marked ✅ Done)
