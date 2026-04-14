# deadline-job-attachments Architecture

## Crate Position in the Workspace

```
deadline-cli ──► deadline-job-attachments    ← this crate
deadline-gui-ffi ──► deadline-job-attachments
deadline-job-bundle ──► deadline-job-attachments
```

Independent from `deadline-client` for AWS operations — owns its own S3 and
STS clients with custom timeouts, s3v4 signatures, and pool sizes.

## Module Layout

```
src/
├── lib.rs              # Re-exports all public modules
├── models.rs           # StorageProfile, FileSystemLocation, AssetRootGroup,
│                       #   AssetUploadGroup, Attachments, ManifestProperties,
│                       #   JobAttachmentS3Settings, FileConflictResolution,
│                       #   PathFormatExt, helper functions (join_s3_paths,
│                       #   generate_random_guid, float_to_iso_datetime_string)
├── asset_manifests.rs  # AssetManifest, ManifestEntry, ManifestPath, ManifestVersion,
│                       #   HashAlgorithm, canonical JSON encoding, manifest read/write,
│                       #   merging, output manifest retrieval from S3
├── upload.rs           # prepare_paths_for_upload (path grouping by storage profile),
│                       #   hash_assets_and_create_manifest (walk + hash + manifest),
│                       #   upload_assets (S3 CAS upload with ExpectedBucketOwner),
│                       #   symlink rejection, progress callbacks
├── download.rs         # attachment_download (S3 CAS download), conflict resolution
│                       #   (Skip/Overwrite/CreateCopy), mtime restoration,
│                       #   collision state tracking, S3 error mapping
├── caches.rs           # HashCache (SQLite hashesV5, keyed by path+mtime_ns),
│                       #   S3CheckCache (SQLite, 30-day expiry), WAL journal mode,
│                       #   lock retry with jitter (3 attempts, 0.5-1.5s delay)
├── s3.rs               # build_s3_client (custom timeouts, retries, user agent),
│                       #   get_account_id (STS GetCallerIdentity),
│                       #   get_s3_max_pool_connections, constants
├── api.rs              # Deadline attachment API calls: get_job (for attachment settings),
│                       #   get_queue (for jobAttachmentSettings), storage profile parsing
├── manifest_ops.rs     # CLI-facing operations: manifest_snapshot, manifest_diff,
│                       #   manifest_upload, manifest_download, resolve_glob_config
├── diff.rs             # Manifest diffing: compare two manifests or manifest vs directory,
│                       #   returns DiffResult { new, modified, deleted }
├── progress_tracker.rs # ProgressTracker: time-based reporting (fires when ≥1s elapsed),
│                       #   SummaryStatistics, DownloadSummaryStatistics,
│                       #   ProgressReportMetadata, cancellation via callback return
└── vfs.rs              # Placeholder (empty) — virtual filesystem, deferred
```

## Key Design Decisions

**Content-Addressed Storage (CAS).** Files stored at `{rootPrefix}/Data/{hash}.{algorithm}`.
Identical files stored once regardless of how many jobs reference them. Deduplication
happens at the file level, not the manifest level.

**Independent S3 client.** `build_s3_client` in `s3.rs` creates clients with:
- 30s connect timeout, 30s read timeout
- Standard retry config
- Custom user agent: `S3A/Deadline/NA/JobAttachments/{version}`
- `AWS_ENDPOINT_URL_S3` env var for test endpoint override (forces path-style)
- Pool size from `settings.s3_max_pool_connections` (default 50)

**`ExpectedBucketOwner` on every S3 call.** Account ID from STS GetCallerIdentity
passed to every PutObject/GetObject/HeadObject. Prevents confused deputy attacks
where a misconfigured bucket policy could route requests to the wrong account.

**Canonical JSON encoding.** Manifests serialized with paths sorted by UTF-16 BE
byte ordering, keys sorted lexicographically, compact format, non-ASCII escaped
to `\uXXXX`. Sort applied at manifest construction time, not deferred to encoding.

**Free functions for orchestration.** `upload_assets`, `attachment_download`,
`hash_assets_and_create_manifest` are stateless free functions that take S3 client
and account ID as parameters. No god-class accumulating state.

**Sequential S3 operations (for now).** Upload and download are currently sequential
per file. Parallel transfer proven in spike but not yet wired into production paths.
The `s3_max_pool_connections` and `num_upload_workers` fields are stored for future use.

**Symlinks rejected on upload.** Files checked via `symlink_metadata()` before upload.
Symlinks skipped with a warning — security measure to prevent path traversal after
submission.
