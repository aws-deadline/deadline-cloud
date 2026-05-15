# attachments/upload Evaluation Report

**Date:** 2026-05-14
**Module:** `attachments/upload`
**Files reviewed:** `src/attachments/upload.rs`, `tests/attachments/upload_s3.rs`

## Summary
Upload engine (37KB) for hashing and uploading job attachment files to S3 CAS. Clean delegation to `openjd-snapshots` for the heavy lifting (hash+upload pipeline). The path grouping logic is well-structured. Grade: **B+**.

## Findings

### Critical (must fix)
- None identified.

### Important (should fix)

1. **`prepare_paths_for_upload` does filesystem I/O (existence checks, metadata)**: This makes it untestable without real files. The function mixes path classification logic with filesystem validation. Consider separating validation from grouping.

2. **`find_group_key` uses `eq_ignore_ascii_case` for group lookup but exact match for insertion**: If two paths differ only in case (e.g., `/Users/foo` vs `/users/foo` on case-insensitive macOS), they'll match the same group on lookup but could create duplicates on insertion. This is unlikely to cause bugs in practice but is inconsistent.

3. **`s3_upload_error` has repetitive match arms**: Each HTTP status code arm constructs a similar `JobAttachmentsError::S3Client` with different guidance text. Consider a table-driven approach or at minimum extract the guidance strings as constants.

4. **`upload_assets` doesn't report per-file progress**: The progress tracker is updated in bulk after each group completes. For large groups with many files, the progress bar appears stuck. The `openjd-snapshots` engine likely supports per-file callbacks.

### Minor (nice to have)

5. **`common_path` doesn't handle empty path components gracefully**: If any path in the input is empty, the function returns an empty `PathBuf`. This is handled upstream (empty strings are filtered), but the function itself isn't defensive.

6. **`top_directory` returns empty string for empty paths**: Edge case that shouldn't occur in practice but could cause confusing group keys.

7. **`snapshot_assets` is synchronous**: Unlike `upload_assets` which is async, `snapshot_assets` does blocking file I/O. This is fine since it's only used for debug snapshots, but it blocks the tokio runtime if called from an async context without `spawn_blocking`.

## Test Coverage Assessment

| Function/Area | Happy path | Error cases | Edge cases |
|---|---|---|---|
| `prepare_paths_for_upload` | ✅ | ✅ | ✅ |
| `upload_assets` | ✅ (integration) | ❌ | ❌ |
| `snapshot_assets` | ✅ (via CLI) | ❌ | ❌ |
| `S3UploadContext::upload_bytes_to_s3` | ✅ (integration) | ❌ | ❌ |
| `common_path` | ❌ | ❌ | ❌ |
| `find_group_key` | ❌ (tested indirectly) | ❌ | ❌ |

## Recommended Changes

1. [S] Extract guidance strings in `s3_upload_error` to named constants
2. [M] Add unit tests for `common_path` and `find_group_key` edge cases
3. [M] Investigate per-file progress callbacks from openjd-snapshots
4. [S] Document that `snapshot_assets` is blocking and should not be called from async without `spawn_blocking`
5. [L] Separate path validation from grouping logic in `prepare_paths_for_upload`
