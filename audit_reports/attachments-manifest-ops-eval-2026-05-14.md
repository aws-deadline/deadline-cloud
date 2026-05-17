# attachments/manifest_ops Evaluation Report

**Date:** 2026-05-14
**Module:** `attachments/manifest_ops`
**Files reviewed:** `src/attachments/manifest_ops.rs`, `tests/attachments/manifest_ops.rs`

## Summary
High-level manifest lifecycle operations (19KB): glob, snapshot, diff, merge, upload, download. Orchestrates lower-level modules. Clean API surface. Grade: **B+**.

## Findings

### Critical (must fix)
- None identified.

### Important (should fix)

1. **`resolve_glob_config` parses JSON from a file path OR inline string**: The function tries to read the path as a file first, then falls back to parsing as JSON. If a user passes a valid filename that doesn't exist, they get a confusing JSON parse error instead of "file not found." Consider checking file existence explicitly.

2. **Several functions take many parameters (6-8)**: `manifest_download`, `manifest_upload`, `manifest_diff` all have long parameter lists. Options structs would improve ergonomics.

### Minor (nice to have)

3. **`ManifestDiffResult` uses `Vec<String>` for all categories**: Could use a single `Vec<(String, FileStatus)>` to avoid splitting, but the current API matches what callers need for display.

## Test Coverage Assessment

| Function/Area | Happy path | Error cases | Edge cases |
|---|---|---|---|
| `resolve_glob_config` | ✅ | ✅ | ✅ |
| `manifest_snapshot` | ✅ | ❌ | ❌ |
| `manifest_diff` | ✅ | ❌ | ❌ |
| `manifest_merge` | ✅ | ❌ | ❌ |
| `manifest_download` | ✅ | ❌ | ❌ |
| `manifest_upload` | ✅ | ❌ | ❌ |

## Recommended Changes

1. [S] Improve error message in `resolve_glob_config` when file doesn't exist — **DONE (Batch F, 2026-05-16)**
2. [M] Consider options structs for functions with 6+ parameters
3. [S] Add error case tests for manifest operations — **DONE (Batch G2, 2026-05-16)**
