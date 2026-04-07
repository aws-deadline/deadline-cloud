# Handoff

Current in-flight work for the rust-rewrite. Read this at the start of
every session before consulting the Work Items table in `README.md`.

## Active Work Item

**#8 — Job attachments: core** (batch 8a complete, 8b/8c not started)

### Current Step

Batch 8a complete (Steps 0–7). Next session should start batch 8b or 8c.

### Batch 8a — What Was Done

Implemented foundational types, hashing, manifest encode/decode, and
caches in `deadline-job-attachments`. 80 Level 1 tests, all passing.

**Files changed:**
- `crates/deadline-job-attachments/Cargo.toml` — added xxhash-rust,
  rusqlite, uuid, serde, serde_json, log dependencies
- `crates/deadline-job-attachments/src/asset_manifests.rs` — HashAlgorithm,
  hash_file, hash_data, ManifestVersion, ManifestPath, AssetManifest
  (canonical JSON encode, UTF-16 BE path sort, ensure_ascii),
  decode_manifest with full validation
- `crates/deadline-job-attachments/src/models.rs` — JobAttachmentS3Settings,
  ManifestProperties, Attachments, PathMappingRule,
  StorageProfileOperatingSystemFamily, PathFormat extensions, helpers
- `crates/deadline-job-attachments/src/caches.rs` — HashCache (hashesV5),
  S3CheckCache (s3checkV1), retry with jitter, WAL mode
- `docs/specs/deadline-job-attachments.md` — full design spec
- `docs/designs/rust-rewrite/README.md` — status updated to In progress

**Bug fixes over Python:**
1. Hash cache mtime precision — integer nanoseconds in hashesV5 instead
   of lossy datetime string in hashesV4
2. Manifest path double-sort — sorted once at construction by canonical
   UTF-16 BE comparator

**Test spec coverage:** §19 (30 cases), §24 (20 cases), §25 (30 cases)

### Remaining Batches

| Batch | Scope | Test Spec Sections | Status |
|-------|-------|--------------------|--------|
| 8a | Types, hashing, manifest encode/decode, caches | §19, §24, §25 | ✅ Done |
| 8b | Progress tracker | §33 | Not started |
| 8c | Path grouping, manifest creation | §20 | Not started |

### What's Next

Pick up batch 8b (progress tracker) or 8c (path grouping & manifest
creation). Both are independent — can be done in either order. Batch 8c
depends on 8b for the progress callback during hashing.

## Recently Completed

**#12b — Job search**, **#15b — Job get search term**,
**#15c — Job logs auto-selection messages** (batched, marked ✅ Done)

**#7 — Job bundle** (marked ✅ Done)

**#12 — Job cancel** and **#15 — Job requeue-tasks** (marked ✅ Done)

**#4 — Queue parameters** (marked ✅ Done)

**#6 — Job monitoring & logs** (marked ✅ Done)
