# attachments/download Evaluation Report

**Date:** 2026-05-14
**Module:** `attachments/download`
**Files reviewed:** `src/attachments/download.rs`, `tests/attachments/download.rs`

## Summary
Download engine (47KB) for job attachments from S3 CAS. Well-architected with clear separation between manifest retrieval, filtering, merging, and actual download. The `OutputDownloader` and `InputDownloader` structs provide clean APIs. The fnmatch implementation is custom and could have edge cases. Grade: **B+**.

## Findings

### Critical (must fix)
- None identified.

### Important (should fix)

1. **Custom `fnmatch` implementation compiles a regex per call** (`download.rs:~240-280`): Every call to `matches_any_filter` compiles a new regex for each filter pattern. In a download with 10,000 files and 5 filters, this is 50,000 regex compilations. Should cache compiled patterns or use a glob library.

2. **`select_latest_manifests_per_task` compiles regex on every call** (`download.rs:~650`): `regex::Regex::new(r"step-.*/.*/.*output.*")` is compiled each time the function is called. Use `LazyLock` or pass a pre-compiled regex.

3. **`download_files_from_manifests` takes 7 parameters**: This is at the threshold. Consider an options struct:
   ```rust
   pub struct DownloadOptions {
       pub cas_prefix: Option<String>,
       pub conflict_resolution: FileConflictResolution,
       pub on_downloading_files: Option<Box<dyn Fn(u64, u64) -> bool + Send>>,
   }
   ```

4. **`list_manifest_keys_from_s3` returns `Err` on empty first page but `Ok(vec![])` is unreachable**: The function returns an error if `contents.is_empty()` on the first iteration AND `all_keys.is_empty()`. But the caller `get_output_manifests_by_asset_root` uses `let Ok(manifest_keys) = ... else { return Ok(HashMap::new()) }` — swallowing the error. This means "no manifests found" is silently treated as success, which is correct behavior but the error path is misleading.

### Minor (nice to have)

5. **`normalize_path` duplicated from `bundle/submission`**: Same function exists in both modules. Extract to shared utility.

6. **`fnmatch` doesn't handle `**` (globstar)**: The current implementation treats `*` as matching everything including `/`. This matches Python's `fnmatch.fnmatch` behavior but differs from shell globbing. Document this explicitly.

7. **`rebuild_manifests` clones all manifests**: For large manifest lists, this could be expensive. Consider using `Cow` or references where possible.

8. **`OutputDownloader` and `InputDownloader` are nearly identical**: They share the same fields and methods (`rebuild`, `get_paths_by_root`, `set_root_path`, `apply_include_filters`). Consider a generic `ManifestDownloader<T>` or a shared trait.

## Test Coverage Assessment

| Function/Area | Happy path | Error cases | Edge cases |
|---|---|---|---|
| `ensure_paths_within_directory` | ✅ | ✅ | ✅ |
| `merge_asset_manifests` | ✅ | ✅ | ✅ |
| `matches_any_filter` | ✅ | ❌ | ✅ |
| `filter_manifests` | ✅ | ❌ | ❌ |
| `rebuild_manifests` | ✅ | ❌ | ❌ |
| `download_files_from_manifests` | ✅ (integration) | ❌ | ❌ |
| `get_output_manifests_by_asset_root` | ✅ (integration) | ❌ | ❌ |
| `select_latest_manifests_per_task` | ❌ | ❌ | ❌ |
| `OutputDownloader` | ❌ | ❌ | ❌ |
| `InputDownloader` | ❌ | ❌ | ❌ |

Good coverage of the pure filtering/merging functions. The S3 interaction paths are tested via integration tests with a stub server.

## Recommended Changes

1. [M] Cache compiled regex patterns in `matches_any_filter` (use a `Vec<Regex>` built once per filter set)
2. [S] Use `LazyLock` for the step pattern regex in `select_latest_manifests_per_task`
3. [S] Extract shared `normalize_path` to `crate::util`
4. [M] Add unit tests for `select_latest_manifests_per_task` with various S3 key patterns
5. [L] Unify `OutputDownloader`/`InputDownloader` into a generic downloader struct
