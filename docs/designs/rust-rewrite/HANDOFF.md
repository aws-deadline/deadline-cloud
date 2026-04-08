# Handoff

Current in-flight work for the rust-rewrite. Read this at the start of
every session before consulting the Work Items table in `README.md`.

## Active Work Item

**#9 — Job attachments: transfer** (batch 9a starting)

### Current Step

Step 0 complete (plan approved). Starting batch 9a (S3 client
infrastructure, §34: 37 cases).

### Batching Plan

| Batch | Scope | Test Spec | Cases |
|-------|-------|-----------|-------|
| 9a | S3 client infrastructure + aws-sdk-s3 | §34 | 37 |
| 9b | Upload engine (S3AssetUploader, upload_assets) | §21 | 28 |
| 9c | Download engine (download_file, merge, output manifests) | §22 | 34 |
| 9d | Public API (attachment_download/upload, path mapping) | §30 | 36 |
| 9e | CLI commands + manifest API | §31, §46 | 60 |

### What's Next

Batch 9a complete. Proceed to batch 9b (upload engine, §21: 28 cases).

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
