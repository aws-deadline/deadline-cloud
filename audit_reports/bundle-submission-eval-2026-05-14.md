# bundle/submission Evaluation Report

**Date:** 2026-05-14
**Module:** `bundle/submission`
**Files reviewed:** `src/bundle/submission.rs`, `tests/bundle/submission.rs`

## Summary
Large orchestration module (55KB, ~1200 lines) handling end-to-end job submission. The code is functional and correct but suffers from a monolithic function (`create_job_from_job_bundle` at ~500 lines), duplicated telemetry emission patterns, and some API ergonomic issues. Grade: **B**.

## Findings

### Critical (must fix)
- None identified. All tests pass and the logic is correct.

### Important (should fix)

1. ✅ **NOT A BUG (verified 2026-05-15)** — **Duplicate telemetry emission**: Both `hashing_summary` and `upload_summary` events are intentional. Python emits both too (`record_hashing_summary` + `record_upload_summary`). They are different event types consumed by different telemetry dashboard panels. In Rust, both use the same stats because hashing+upload are combined into one operation, but both events must exist for backward compatibility.

2. **`auto_accept` logic inverted for unknown paths** (`submission.rs:~640`): When `auto_accept` is true and files are outside known paths, the code *cancels* submission. This is counterintuitive — `auto_accept` typically means "proceed without prompting." The Python client has the same behavior, so this is a parity issue, but it's a confusing API contract that should be documented.

3. **`SubmitJobParams` has too many fields (18)**: This struct is the sole public API for submission. Consider a builder pattern or grouping related fields (e.g., `AttachmentOptions { file_system, force_s3_check, debug_snapshot_dir }`, `TelemetryOptions { telemetry, hashing_progress_callback, upload_progress_callback }`).

4. **`#[allow(clippy::too_many_lines)]` on `create_job_from_job_bundle`**: The function is ~500 lines with 11 sequential phases. Each phase (load template, get queue, merge parameters, handle attachments, etc.) could be extracted into a helper, improving readability and testability of individual phases.

### Minor (nice to have)

5. ✅ **RESOLVED (2026-05-15, Batch A)** — **`normalize_path` duplicated**: Extracted to shared `crate::util::normalize_path`. `submission.rs` now delegates to it.

6. ✅ **RESOLVED (2026-05-15, Batch A)** — **`op_err` helper defined twice**: Extracted to shared `crate::util::op_err`. All modules now import from there.

7. **`parse_frame_range` uses regex for simple integer parsing**: The regex is fine but could be replaced with a simpler split-based parser for clarity. Low priority since it works correctly.

8. **Missing `#[must_use]` on `AssetReferences::union`**: Already present — good. But `split_parameter_args` returns a tuple that callers could accidentally ignore.

## Test Coverage Assessment

| Function/Area | Happy path | Error cases | Edge cases |
|---|---|---|---|
| `split_parameter_args` | ✅ | ✅ | ✅ |
| `parse_frame_range` | ✅ | ✅ | ✅ |
| `AssetReferences` | ✅ | ✅ | ✅ |
| `create_job_from_job_bundle` | ❌ (no integration test) | ❌ | ❌ |
| `expand_input_directories` | ❌ | ❌ | ❌ |
| `filter_redundant_known_paths` | ❌ | ❌ | ❌ |
| `save_debug_snapshot` | ❌ | ❌ | ❌ |

The unit tests cover the pure functions well, but the orchestration function itself has no Level 1 or Level 2 test coverage in this module's test file. It's likely tested via CLI Level 2 tests in `deadline-cli`.

## Recommended Changes

1. [S] Remove duplicate telemetry emission (lines ~800-830)
2. [S] Extract `normalize_path` to a shared `crate::util` module
3. [M] Break `create_job_from_job_bundle` into phase helper functions
4. [M] Group `SubmitJobParams` fields into sub-structs
5. [L] Add Level 1 integration tests for `create_job_from_job_bundle` using a stub server
