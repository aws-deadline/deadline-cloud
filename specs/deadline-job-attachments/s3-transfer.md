# S3 Transfer

## Upload (`upload_assets`)

Uploads files described by a manifest to S3 CAS at `{rootPrefix}/Data/{hash}.xxh128`.

Flow per file:
1. Check S3 check cache — if hit and not force_s3_check, skip
2. If force_s3_check or cache miss → HeadObject to verify existence
3. If file exists in S3 → update S3 check cache, skip upload
4. If file doesn't exist → PutObject with `ExpectedBucketOwner`
5. Update S3 check cache on success
6. Report progress via callback; returning `false` cancels

`prepare_paths_for_upload` groups input/output/referenced paths by asset root
based on storage profile locations. SHARED locations are filtered out. Files
outside any known location group under the filesystem root.

`hash_assets_and_create_manifest` walks directories, hashes files (using hash
cache for unchanged files), and creates a manifest. Symlinks are rejected.

## Download (`attachment_download`)

Downloads files from S3 CAS to local filesystem.

Flow per file:
1. Compute local path from manifest entry's relative path + output directory
2. Check conflict resolution mode
3. Download from S3 via GetObject
4. Write to local file
5. Restore mtime from manifest entry (microseconds since epoch)

### Conflict Resolution

- `Skip` — if file exists, skip it
- `Overwrite` — replace existing file
- `CreateCopy` — write as `filename (N).ext` where N is the next available
  number. Uses atomic `OpenOptions::create_new(true)` to avoid races.
  Collision counters shared across concurrent downloads via `Arc<Mutex<HashMap>>`.

## S3 Error Mapping

Every S3 error maps to a specific help message in both upload and download:

| Status | Guidance |
|--------|----------|
| 403 | Check credentials + IAM permissions. If error mentions `kms:`, also check KMS Decrypt/DescribeKey permissions |
| 404 | Check bucket name and object key existence |
| 408 | Request timeout — retry or check network stability |
| 500 | Internal server error — retry later |
| 503 | Service unavailable — retry later |
| Transport | Check credentials and network connection |

## S3 Client Configuration

`build_s3_client` in `s3.rs` applies:
- Connect timeout: 30s
- Read timeout: 30s
- Retry: standard (SDK default backoff)
- User agent: `S3A/Deadline/NA/JobAttachments/{version}`
- `AWS_ENDPOINT_URL_S3` → endpoint override + force path style
- Multipart chunk size: 8 MB
- Max upload concurrency: 10 (stored, not yet used)
- Max download concurrency: 10 (stored, not yet used)

## Progress Tracking

`ProgressTracker` fires progress callbacks based on time elapsed (≥1s since
last report) or operation completion. The callback receives
`ProgressReportMetadata` with progress percentage and status. Returning
`false` from the callback cancels the operation.

`SummaryStatistics` tracks total files, bytes, skipped files, and elapsed time.
Implements `Display` for human-readable output.
