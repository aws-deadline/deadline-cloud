# deadline-job-attachments Architecture

## Crate Position

```
deadline-cli ──► deadline-job-attachments    ← this crate
deadline-python-bindings ──► deadline-job-attachments
deadline-job-bundle ──► deadline-job-attachments
```

Independent from `deadline-api` for AWS operations — owns its own S3 and
STS clients with custom timeouts, signatures, and pool sizes.

## Module Layout

The crate covers the full attachment data plane: hashing files, building
manifests, uploading to and downloading from S3 content-addressed
storage, and caching results to avoid redundant work.

Upload and download are orchestrated by stateless free functions that
take an S3 client and account ID as parameters — no god-class
accumulating state. The hash cache and S3 check cache use SQLite with
WAL journal mode for concurrent access.

CLI-facing operations (snapshot, diff, upload, download) live in
`manifest_ops.rs`, which is the bridge between the CLI command layer
and the core attachment logic.

```
src/
├── lib.rs                  # Re-exports all public modules
├── models.rs               # Data types: storage profiles, attachment settings, conflict resolution
├── asset_manifests.rs      # Manifest parsing, canonical JSON encoding, merging
├── upload.rs               # Path grouping, hashing, manifest creation, S3 CAS upload
├── download.rs             # S3 CAS download, conflict resolution, mtime restoration
├── caches.rs               # Hash cache and S3 check cache (SQLite)
├── s3.rs                   # S3 client builder, STS account ID lookup
├── api.rs                  # Deadline API calls for attachment/queue settings
├── manifest_ops.rs         # CLI-facing: snapshot, diff, upload, download
├── diff.rs                 # Manifest diffing (new, modified, deleted)
├── progress_tracker.rs     # Time-based progress reporting, cancellation
├── path_mapping.rs         # Path mapping rules from storage profiles, trie-based transform
├── incremental_download.rs # Checkpoint persistence for resumable downloads
└── vfs.rs                  # Placeholder — virtual filesystem, deferred
```

## Key Design Decisions

**Content-Addressed Storage (CAS).** Files are stored at
`{rootPrefix}/Data/{hash}.{algorithm}`. Identical files are stored once
regardless of how many jobs reference them. Deduplication happens at the
file level, not the manifest level.

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
format, and non-ASCII escaped to `\uXXXX`. This ensures byte-identical
output regardless of platform.

**Sequential S3 operations (for now).** Upload and download are
currently sequential per file. Parallel transfer was proven in a spike
but is not yet wired into production paths.

**Symlinks rejected on upload.** Files are checked before upload and
symlinks are skipped with a warning — a security measure to prevent
path traversal after submission.
