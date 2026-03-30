# deadline-job-attachments

Asset manifest handling, S3 upload/download, hash cache, content-addressed storage.

## Status: Stub

Not yet implemented.

## Dependencies

| Crate | Purpose |
|-------|---------|
| `deadline-models` | Shared types |

## S3 Transfer Spike

Proved that `aws-sdk-s3` matches Python/boto3 throughput for multipart
upload and download of large files. This was a required risk spike before
bulk implementation (see `migration_strategy.md` § "Fail-Fast Strategy").

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
