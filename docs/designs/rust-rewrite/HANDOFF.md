# Handoff

Current in-flight work for the rust-rewrite. Read this at the start of
every session before consulting the Work Items table in `README.md`.

## Active Work Item

**#9 — Job attachments: transfer** (batch 9a starting)

### Current Step

Batch 9b complete. Proceed to batch 9c (download engine, §22: 34 cases).

### Batching Plan

| Batch | Scope | Test Spec | Cases |
|-------|-------|-----------|-------|
| 9a | S3 client infrastructure + aws-sdk-s3 | §34 | 37 |
| 9b | Upload engine (S3UploadContext, upload_assets) | §21 | 28 |
| 9c | Download engine (download_file, merge, output manifests) | §22 | 34 |
| 9d | Public API (attachment_download/upload, path mapping) | §30 | 36 |
| 9e | CLI commands + manifest API | §31, §46 | 60 |

### What's Next

Batch 9b complete. Proceed to batch 9c: download engine
(`download_file`, `download_files_from_manifests`,
`merge_asset_manifests`, `get_output_manifests_by_asset_root`).
§22: 34 cases.

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
