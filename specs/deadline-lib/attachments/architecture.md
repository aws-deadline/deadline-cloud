# attachments Module Architecture

## Module Position

```
deadline-lib
├── attachments  ← this module
│   └── config
└── ...

deadline-cli ──► deadline-lib::attachments
deadline-python-bindings ──► deadline-lib::attachments
deadline-lib::bundle ──► deadline-lib::attachments
```

Independent from `deadline-lib::api` for AWS operations — owns its own S3 and
STS clients with custom timeouts, signatures, and pool sizes.

## Module Layout

```
src/attachments/
├── mod.rs                  # Re-exports all public modules
├── models.rs               # Data types: storage profiles, attachment settings, conflict resolution
├── upload.rs               # Path grouping, manifest creation, S3 CAS upload orchestration
├── download.rs             # S3 CAS download orchestration, conflict resolution, mtime restoration
├── caches.rs               # Re-exports openjd_snapshots::{HashCache, S3CheckCache}, default_cache_dir
├── s3.rs                   # S3 client builder, STS account ID lookup
├── api.rs                  # Public API: read_manifests, attachment_download, attachment_upload
├── manifest_ops.rs         # CLI-facing: snapshot, diff, merge, upload, download
├── diff.rs                 # Manifest diffing via openjd_snapshots::diff_snapshots
├── progress_tracker.rs     # Time-based progress reporting, cancellation, SummaryStatistics
├── path_mapping.rs         # Rule generation from storage profiles, delegates to openjd_expr
├── errors.rs               # JobAttachmentsError enum
└── incremental_download.rs # Checkpoint persistence for resumable downloads
```

Types used throughout: `openjd_snapshots::Snapshot` (manifest), `FileEntry`
(file metadata), `HashAlgorithm`. No local wrapper types — openjd types flow
directly through the system.

## Key Design Decisions

**Content-Addressed Storage (CAS).** Files are stored at
`{rootPrefix}/Data/{hash}.{algorithm}`. Identical files are stored once
regardless of how many jobs reference them. Deduplication happens at the
file level, not the manifest level.

**openjd-snapshots for heavy lifting.** Hashing, manifest codec, hash
cache, and the S3 upload/download engine are all delegated to
`openjd-snapshots`. This module owns the Deadline-specific orchestration:
path grouping by storage profile, manifest S3 key construction, conflict
resolution, incremental download checkpoints, and progress reporting.

**Independent S3 client.** The attachment subsystem builds its own S3
clients with custom timeouts (30s connect, 30s read), retry config,
and a custom user agent. This is separate from the Deadline API client
because attachments have different credential lifecycles and performance
requirements.

**ExpectedBucketOwner on every S3 call.** The account ID from STS
GetCallerIdentity is passed to every PutObject/GetObject/HeadObject.
This prevents confused deputy attacks where a misconfigured bucket
policy could route requests to the wrong account.

**Canonical JSON encoding.** Manifests are serialized with paths sorted
by UTF-16 BE byte ordering, keys sorted lexicographically, compact
format, and non-ASCII escaped to `\uXXXX`. Encoding/decoding delegates
to `openjd_snapshots::encode_snapshot_v2023` / `decode_v2023`.

**Symlinks rejected on upload.** Files are checked before upload and
symlinks are skipped with a warning — a security measure to prevent
path traversal after submission.

## S3 Client Configuration

`build_s3_client` in `s3.rs` applies:
- Connect timeout: 30s
- Read timeout: 30s
- Retry: standard (SDK default backoff)
- User agent: `S3A/Deadline/NA/JobAttachments/{version}`
- `AWS_ENDPOINT_URL_S3` → endpoint override + force path style

## S3 Error Mapping

Every S3 error maps to a specific help message:

| Status | Guidance |
|--------|----------|
| 403 | Check credentials + IAM permissions. If error mentions `kms:`, also check KMS Decrypt/DescribeKey permissions |
| 404 | Check bucket name and object key existence |
| 408 | Request timeout — retry or check network stability |
| 500 | Internal server error — retry later |
| 503 | Service unavailable — retry later |
| Transport | Check credentials and network connection |

## Conflict Resolution (Download)

- `Skip` — if file exists, skip it
- `Overwrite` — replace existing file
- `CreateCopy` — write as `filename (N).ext` where N is the next available
  number. Uses atomic `OpenOptions::create_new(true)` to avoid races.
  Collision counters shared across concurrent downloads via `Arc<Mutex<HashMap>>`.

## Caches

**Hash cache:** Re-exports `openjd_snapshots::HashCache`. SQLite-based,
stores file hashes keyed by path + mtime (nanoseconds). Avoids re-hashing
unchanged files. See openjd-rs documentation for schema details.

**S3 check cache:** SQLite at `~/.deadline/job_attachments/s3_check_cache.db`.
Table `s3checkV1`, primary key `s3_key`. Entries expire after 30 days.
Avoids redundant HeadObject calls for files already known to exist in S3.

**Integrity verification:** `verify_hash_cache_integrity` samples up to 30
random entries from the hash cache, calls HeadObject on each. If any sampled
file is missing from S3, the entire cache is reset. Skipped when
`settings.force_s3_check` is `true` (every file verified individually).

## Path Grouping

`prepare_paths_for_upload` groups input/output/referenced paths by asset root
based on storage profile locations:

- **LOCAL locations** define grouping boundaries — files under a LOCAL
  location are grouped into one manifest with that location as asset root.
- **SHARED locations** are filtered out — files are already accessible to
  workers via network mounts.
- Files outside any known location group under the filesystem root.

Symlinks are rejected via `symlink_metadata()` check before grouping.

## Path Mapping

Cross-OS path remapping for downloads. Rules are generated from source and
destination storage profiles (each shared location name produces one rule).
Implementation delegates to `openjd_expr::apply_rules()`.

## Progress Tracking

`ProgressTracker` fires progress callbacks based on:
- Time elapsed (≥1s since last report), OR
- Chunk completion (≥50 files processed), OR
- 100% completion

Callback type: `ProgressFn = Box<dyn Fn(u64, u64) -> bool + Send>` where
args are `(processed_bytes, total_bytes)`. Returning `false` cancels.

`SummaryStatistics` tracks total files, bytes, skipped files, elapsed time,
and transfer rate.
