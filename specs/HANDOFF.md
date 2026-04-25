# Handoff

Current in-flight work. Read this at the start of every session before
consulting the Work Items table in `specs/progress.md`.

## Active Work Item

None — ready for next work item.

## Recently Completed — #21: Python bug-fix parity sweep

All 5 Python bug fixes (#1098, #1005, #1013, #1032, #1008) verified as
already correct in Rust. Implemented from fixed Python source — no code
changes needed.

## Previously Completed — #9 + #11: Parallel/multipart S3 transfer + bundle submit --json

Parallel upload for small files via `buffer_unordered(num_upload_workers)`.
Multipart upload for large files (>threshold) via
`CreateMultipartUpload`/`UploadPart`/`CompleteMultipartUpload` with abort
on error. Parallel download via `buffer_unordered(num_download_workers)`.
Streaming download to file via `tokio::io::copy(body.into_async_read())`.
`bundle submit --json` flag for machine-readable output. Closes
AUDIT-011, AUDIT-012, AUDIT-014, AUDIT-049. All audit findings resolved
(0 remaining).
