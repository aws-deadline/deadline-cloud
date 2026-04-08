# Handoff

Current in-flight work for the rust-rewrite. Read this at the start of
every session before consulting the Work Items table in `README.md`.

## Active Work Item

None. Work item #8 is complete. Next session should consult the Work
Items table in `README.md` and pick the first row with status
"Not started" whose dependencies are all "✅ Done".

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
