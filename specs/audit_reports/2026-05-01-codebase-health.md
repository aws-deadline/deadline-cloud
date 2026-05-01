# Audit: Codebase Health — Dead Code, Redundancy, Organization, Test Quality

**Date:** 2026-05-01
**Scope:** All Rust crates (48,938 lines across 7 crates, 1,257 tests)
**Status:** Complete

## Summary

| Category | Critical | High | Medium | Low | Total |
|----------|----------|------|--------|-----|-------|
| Dead code | 0 | 1 | 5 | 8 | 14 |
| Redundancy | 0 | 3 | 5 | 1 | 9 |
| Organization | 0 | 2 | 2 | 3 | 7 |
| Function signatures | 0 | 3 | 2 | 0 | 5 |
| Test quality | 0 | 0 | 4 | 2 | 6 |
| **Total** | **0** | **9** | **18** | **14** | **41** |

## Compiler Baseline

- `cargo build`: 0 warnings (clean)
- `cargo clippy --all-targets`: ~50 warnings (collapsible ifs, too many
  arguments, unused imports in tests, redundant closures)
- `#[allow(dead_code)]` suppressions: 1 (CliError enum in config.rs —
  justified)

---

## HIGH-SEVERITY FINDINGS

### HEALTH-001: `expand_tilde` duplicated in job.rs and queue.rs

- **Category:** Redundancy
- **Files:** `deadline-cli/src/commands/job.rs:2222`,
  `deadline-cli/src/commands/queue.rs:1237`
- **Issue:** Two different implementations. queue.rs version is more
  robust (handles `~` alone, supports Windows `USERPROFILE`). job.rs
  version is simpler (only handles `~` prefix).
- **Fix:** Move queue.rs version to `common.rs`, delete job.rs version.

### HEALTH-002: `parse_conflict_resolution` copy-pasted 4 times

- **Category:** Redundancy
- **Files:** `job.rs:35`, `attachment.rs:56`, `queue.rs:542`, `mcp.rs:583`
- **Issue:** SKIP/OVERWRITE/CREATE_COPY → `FileConflictResolution` mapping
  duplicated in 4 files with minor variations.
- **Fix:** Add `impl FromStr for FileConflictResolution` in
  `deadline-job-attachments/src/models.rs`.

### HEALTH-003: S3 error handling duplicated between upload.rs and download.rs

- **Category:** Redundancy
- **Files:** `upload.rs:370-430` (`s3_upload_error`),
  `download.rs:72-130` (`s3_download_error`)
- **Issue:** Near-identical 60-line functions matching HTTP status codes
  and producing guidance strings. Only difference is action verb and
  permission name.
- **Fix:** Extract shared `fn s3_operation_error(status, action, bucket,
  key, permission)` into `s3.rs`.

### HEALTH-004: job.rs at 2279 lines needs splitting

- **Category:** Organization
- **Files:** `deadline-cli/src/commands/job.rs`
- **Issue:** 31 functions spanning 6 feature areas (list/get/search,
  cancel/requeue, wait, logs, download, trace-schedule).
- **Fix:** Split into `job/` module directory: `mod.rs` (dispatch),
  `get.rs`, `actions.rs`, `wait.rs`, `logs.rs`, `download.rs`,
  `trace.rs`.

### HEALTH-005: upload.rs at 1960 lines needs splitting

- **Category:** Organization
- **Files:** `deadline-job-attachments/src/upload.rs`
- **Issue:** 4 distinct sections: path preparation, hashing, S3 upload
  context, orchestration, plus 700 lines of tests.
- **Fix:** Split into `upload/` module: `path_preparation.rs`,
  `hashing.rs`, `s3_context.rs`, `orchestration.rs`.

### HEALTH-006: `SubmitterInfo` module is entirely dead code

- **Category:** Dead code
- **Files:** `deadline-api/src/submitter_info.rs` (137 lines)
- **Issue:** `SubmitterInfo`, `YamlValue`, and all methods have zero
  callers outside the file. Module exported from `lib.rs` but never
  imported by any crate.
- **Fix:** Remove module or add `#[allow(dead_code)]` with TODO linking
  to the work item that will use it.

### HEALTH-007: `get_session_logs()` takes 9 parameters

- **Category:** Function signatures
- **Files:** `deadline-api/src/log_retrieval.rs:~130`
- **Issue:** 9 params: farm_id, queue_id, session_id, job_id, limit,
  start_time, end_time, next_token, config.
- **Fix:** Introduce `SessionLogQuery` struct.

### HEALTH-008: `get_worker_logs()` takes 8 parameters

- **Category:** Function signatures
- **Files:** `deadline-api/src/log_retrieval.rs:~190`
- **Issue:** Same pattern as HEALTH-007.
- **Fix:** Introduce `WorkerLogQuery` struct.

### HEALTH-009: `create_job()` uses stringly-typed `serde_json::Map`

- **Category:** Function signatures
- **Files:** `deadline-api/src/api.rs:~310`
- **Issue:** Takes raw JSON map. Callers must know exact key names
  ("farmId", "queueId", etc.). Typos silently produce wrong behavior.
- **Fix:** Define `CreateJobRequest` struct with typed fields.

---

## MEDIUM-SEVERITY FINDINGS

### HEALTH-010: `format_duration` / `format_timedelta` duplicated 3 ways

- **Files:** `job.rs:1051`, `queue.rs:1196`, `common.rs:310`,
  `job.rs:1812`
- **Issue:** 3 different duration formatting functions with naming
  collisions.
- **Fix:** Consolidate into `common.rs` with distinct names.

### HEALTH-011: Config setup boilerplate repeated across 10 command modules

- **Files:** Every command module has its own setup function/block.
- **Issue:** Identical `read_config()` + `apply_cli_options_to_config()`
  pattern with minor variations.
- **Fix:** Standardize on the `setup_config` pattern from job.rs.

### HEALTH-012: MCP list/get duplicates CLI fetch logic

- **Files:** `mcp.rs:155-380`
- **Issue:** 7 list functions independently call SDK, paginate, format
  JSON. Same API calls exist in CLI commands.
- **Fix:** Extract shared fetch functions returning typed structs.

### HEALTH-013: `validate_job_parameter` duplicated across crates

- **Files:** `deadline-job-bundle/src/parameters.rs:30`,
  `deadline-api/src/queue_parameters.rs:99`
- **Issue:** Two separate validation functions with similar but not
  identical logic.
- **Fix:** Consolidate into one function in `deadline-api`.

### HEALTH-014: Duplicate `human_readable_file_size()` across crates

- **Files:** `deadline-job-attachments/src/progress_tracker.rs:8`,
  `deadline-api/src/path_utils.rs:8`
- **Issue:** Identical function in two crates.
- **Fix:** Remove from job-attachments, use deadline-api version.

### HEALTH-015: Duplicate `fmt_size()` within path_utils.rs

- **Files:** `deadline-api/src/path_utils.rs:8` and `:277`
- **Issue:** `human_readable_file_size()` (public) and `fmt_size()`
  (private) implement nearly identical logic.
- **Fix:** Delete `fmt_size()`, call `human_readable_file_size()`.

### HEALTH-016: Duplicate AWS config file path resolution in auth.rs

- **Files:** `deadline-api/src/auth.rs:44-46`, `:58-60`, `:72-74`
- **Issue:** Same 4-line pattern repeated 3 times.
- **Fix:** Extract `fn aws_config_file_path() -> String`.

### HEALTH-017: Duplicate SDK error formatting (CloudWatch vs Deadline)

- **Files:** `log_retrieval.rs:~100` (`cw_sdk_err`),
  `client.rs:14` (`format_sdk_error`)
- **Issue:** Near-identical error extraction logic.
- **Fix:** Make `format_sdk_error` generic enough for CloudWatch.

### HEALTH-018: Dead `DeadlineError::NonValidInput` variant

- **Files:** `deadline-api/src/errors.rs:20`
- **Issue:** Defined but never constructed anywhere.
- **Fix:** Remove until needed.

### HEALTH-019: Dead `with_telemetry_latency_async()`

- **Files:** `deadline-api/src/telemetry.rs:~380`
- **Issue:** Public function with zero callers.
- **Fix:** Remove or make `pub(crate)`.

### HEALTH-020: Dead `summarize_paths_by_nested_directory()` — no-op wrapper

- **Files:** `deadline-api/src/path_utils.rs:~250`
- **Issue:** Calls `summarize_paths_by_sequence()` and returns unchanged.
  Zero external callers.
- **Fix:** Remove stub.

### HEALTH-021: `HashCacheEntry::new()` validation bypassed

- **Files:** `deadline-job-attachments/src/caches.rs:100-115`
- **Issue:** Validates `range_end > range_start`, but all production code
  constructs via struct literal, bypassing validation.
- **Fix:** Either make fields private (force `new()`) or remove `new()`.

### HEALTH-022: 10+ functions with too many parameters in job-attachments

- **Files:** `upload.rs:1037` (8), `download.rs:331` (10),
  `download.rs:550` (9), `api.rs:197` (9), `manifest_ops.rs:440` (11)
- **Issue:** Clippy `too_many_arguments` warnings.
- **Fix:** Create `S3Context`, `DownloadContext`, `UploadAssetsParams`,
  `ManifestDownloadParams` structs. `s3_client` + `account_id` appear
  together in 8+ signatures — prime candidate for a shared struct.

### HEALTH-023: Test helper duplication across 3 test files

- **Files:** `tests/suite/upload_s3.rs:60`, `tests/suite/download.rs:50`,
  `tests/suite/api.rs:40`
- **Issue:** Three different manifest-creation helpers and S3 client
  builders doing the same thing.
- **Fix:** Extract to shared `tests/suite/common.rs`.

### HEALTH-024: Response struct `From` impls untested at Level 1

- **Files:** `deadline-api/src/responses.rs:~400-963`
- **Issue:** Tests verify serde serialization but not the `From<Output>`
  conversion logic (datetime formatting, field mapping).
- **Fix:** Add tests that construct SDK output types and convert via
  `From`.

### HEALTH-025: Missing test coverage for critical paths

- **Files:** `download.rs:170` (CreateCopy collision), `upload.rs:460`
  (multipart abort), `manifest_ops.rs:440` (manifest_download)
- **Issue:** No unit tests for these code paths.
- **Fix:** Add targeted unit tests.

---

## LOW-SEVERITY FINDINGS

### HEALTH-026 through HEALTH-041

Dead `pub` visibility on 8+ internal functions (models.rs helpers,
parameters.rs validators, submission.rs normalizers, IniConfig methods,
telemetry helpers). Regex compiled on every call in 2 places. Empty
`vfs.rs` module. Trivially redundant error trait test. Tests that could
be parameterized with `#[test_case]`. `is_auto_accept` duplicated in
2 places. hooks.rs tests could be external. Unused `_farm_id` and
`_queue` parameters in `add_output_manifests_from_s3`.

---

## Top 10 Actions by Impact

| # | Action | Findings | Lines saved | Risk |
|---|--------|----------|-------------|------|
| 1 | Split `job.rs` into `job/` module | HEALTH-004 | 0 (reorg) | Low |
| 2 | Split `upload.rs` into `upload/` module | HEALTH-005 | 0 (reorg) | Low |
| 3 | Extract `parse_conflict_resolution` to `FromStr` | HEALTH-002 | ~80 | Low |
| 4 | Extract shared S3 error handler | HEALTH-003 | ~60 | Low |
| 5 | Create parameter structs for 10+ functions | HEALTH-007,008,009,022 | 0 (clarity) | Low |
| 6 | Delete dead `SubmitterInfo` module | HEALTH-006 | ~137 | Low |
| 7 | Consolidate `expand_tilde` | HEALTH-001 | ~20 | Low |
| 8 | Consolidate duration formatting | HEALTH-010 | ~30 | Low |
| 9 | Delete duplicate `fmt_size` / `human_readable_file_size` | HEALTH-014,015 | ~40 | Low |
| 10 | Extract shared test helpers | HEALTH-023 | ~100 | Low |

## Cross-Crate Boundary Assessment

| Boundary | Status |
|----------|--------|
| `deadline-cli` → `deadline-api` | ✅ Clean — uses session, client, auth, telemetry APIs |
| `deadline-cli` → `deadline-job-bundle` | ✅ Clean — uses `create_job_from_job_bundle` + `SubmitJobParams` |
| `deadline-cli` → `deadline-job-attachments` | ⚠️ **Over-coupled** — `queue sync-output` uses 15+ internal types directly. Should use a higher-level orchestration API. |
| `deadline-job-bundle` → `deadline-api` | ✅ Clean |
| `deadline-job-bundle` → `deadline-job-attachments` | ✅ Clean |
| `deadline-python-bindings` → all | ✅ Clean — thin FFI layer |
