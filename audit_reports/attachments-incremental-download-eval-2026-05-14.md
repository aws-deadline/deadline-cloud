# attachments/incremental_download Evaluation Report

**Date:** 2026-05-14
**Module:** `attachments/incremental_download`
**Files reviewed:** `src/attachments/incremental_download.rs`

## Summary
Checkpoint persistence and manifest processing for `queue sync-output`. Clean serde-based state management with atomic file writes. The manifest processing functions are straightforward. Grade: **A-**.

## Findings

### Critical (must fix)
- None identified.

### Important (should fix)

1. **`merge_absolute_path_manifest_list` uses case-insensitive keys unconditionally** (`incremental_download.rs:~310`): `mp.path.to_lowercase()` is used as the dedup key. This is correct for Windows but incorrect for Linux/macOS (case-sensitive filesystems). Two files differing only in case (`Readme.md` vs `README.md`) would collide. Should use case-sensitive keys on non-Windows platforms.

2. **`add_output_manifests_from_s3` modifies `session_action_list` in place**: This is a mutable reference to a `&mut [Value]` which is fine, but the function has complex error paths that could leave the array in a partially-modified state (some actions have `"manifests"` added, others don't). Consider building the result separately and applying atomically.

### Minor (nice to have)

3. **`session_action_id_regex()` compiles regex on every call**: Should use `LazyLock` for the compiled regex.

4. **`IncrementalDownloadState::save_file` uses `tempfile::NamedTempFile`**: Good — atomic writes prevent corruption. Well done.

5. **`make_manifest_paths_absolute` has `output_unmapped_paths` as an out-parameter**: This is a C-style pattern. Consider returning a struct `(Snapshot, Vec<String>)` instead. However, since the function modifies the manifest in place, the current API is acceptable.

## Test Coverage Assessment

| Function/Area | Happy path | Error cases | Edge cases |
|---|---|---|---|
| `IncrementalDownloadState` serde | ✅ (inline) | ✅ | ✅ |
| `IncrementalDownloadJob` | ✅ (inline) | ✅ | ✅ |
| `save_file` / `from_file` | ✅ (inline) | ✅ | ❌ |
| `add_output_manifests_from_s3` | ❌ | ❌ | ❌ |
| `make_manifest_paths_absolute` | ❌ | ❌ | ❌ |
| `merge_absolute_path_manifest_list` | ❌ | ❌ | ❌ |

The checkpoint state has good inline tests. The manifest processing functions lack direct tests.

## Recommended Changes

1. [S] Use `LazyLock` for `session_action_id_regex()`
2. [M] Make path dedup case-sensitivity platform-aware in `merge_absolute_path_manifest_list`
3. [M] Add unit tests for `add_output_manifests_from_s3` and `merge_absolute_path_manifest_list`
4. [S] Add test for `make_manifest_paths_absolute` with path mapping
