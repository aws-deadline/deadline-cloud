# Audit: Behavioral Parity (post-April-10 changes)

**Date:** 2026-04-15
**Scope:** Code added/changed since the 2026-04-10 audit: `bundle submit` implementation,
credential scoping wiring, crate consolidation, and verification of previous fixes.
**Status:** Complete

## Summary

| Priority | Count | Fixed | Remaining |
|----------|-------|-------|-----------|
| Critical | 0     | 0     | 0         |
| High     | 2     | 0     | 2         |
| Medium   | 4     | 0     | 4         |
| Low      | 5     | 0     | 5         |

All 7 fixes from the April 10 audit verified as correctly applied (including F-1).
Crate consolidation verified clean — no regressions.

---

## Previous Fix Verification

All fixes from the 2026-04-10 audit are confirmed applied:

| ID | Issue | Status |
|----|-------|--------|
| C-3 | YAML 1.1 boolean quoting | ✅ Verified — regex post-processing in `cli_object_repr()` |
| A-1 | Setting descriptions truncated | ✅ Verified — full descriptions in `settings.rs`, wrapped at 77 chars |
| B-1 | `get_credentials_source` for non-existent profiles | ✅ Verified — `aws_profile_exists()` returns `NotValid` |
| C-1 | Missing required option exit code | ✅ Verified — `MissingRequired` → exit code 2 |
| F-1 | `job logs` "service error" | ✅ Verified — `format_sdk_error()` and `cw_sdk_err()` extract error codes |
| A-2 | Error message quoting | ✅ Verified — single quotes, confirmed by snapshots |
| A-3 | Error message wording | ✅ Verified — unified code path for all unknown settings |
| A-4 | JSON output spacing | ✅ Verified — `json_with_spaces()` matches Python's `json.dumps()` |

---

## Crate Consolidation Verification

✅ All public APIs from dissolved `deadline-models` and `deadline-common` properly
re-homed in `deadline-api` and `deadline-job-attachments`. Zero stale imports.
Zero missing re-exports. All downstream `Cargo.toml` files correct. No regressions.

One minor spec-vs-code discrepancy: `sanitize_path_for_filename` listed in
`specs/api/architecture.md` but not implemented in code (planned, not yet ported).

---

## New Findings

### AUDIT-019: Files-outside-known-paths warning not implemented in bundle submit

- **Category:** Bug
- **Priority:** High
- **Command/Function:** `create_job_from_job_bundle()` in `submission.rs`, step 9
- **Python behavior:** Checks if input files are outside known asset paths, warns the
  user, and prompts for confirmation (unless `auto_accept` is true).
- **Rust behavior:** Known paths are computed via `filter_redundant_known_paths()` but
  assigned to `_known_paths` (unused). No comparison against input files. No warning.
  No confirmation prompt.
- **Impact:** Users submitting jobs with files from unexpected locations get no warning.
  The known-paths feature exists to prevent accidental upload of sensitive files.
- **Resolution:** Wire the known-paths check: compare input files against known paths,
  warn via `print_callback`, prompt via `continue_callback` if `!auto_accept`.

### AUDIT-020: Job history not saved during CLI submission

- **Category:** Behavioral gap
- **Priority:** High
- **Command/Function:** `create_job_from_job_bundle()` in `submission.rs`
- **Python behavior:** CLI path does NOT save job history (only GUI does — observation #15).
- **Rust behavior:** `history.rs` has `create_job_history_bundle_dir()` implemented and
  tested, but never called from the submission pipeline. Dead code.
- **Spec behavior:** `submission-pipeline.md` step 14 says "Save job history snapshot."
- **Impact:** Spec-vs-code mismatch. The function exists but isn't wired. This matches
  Python CLI behavior but contradicts the spec.
- **Resolution:** Either wire `create_job_history_bundle_dir()` into the pipeline (making
  Rust better than Python) or update the spec to mark this as "not yet wired."

### AUDIT-021: `--job-attachments-file-system` lacks value validation

- **Category:** Bug
- **Priority:** Medium
- **Command/Function:** `BundleAction::Submit` clap definition in `bundle.rs`
- **Python behavior:** Validates value is `COPIED` or `VIRTUAL`.
- **Rust behavior:** No `value_parser` constraint. Invalid values silently become `COPIED`
  via `if ja_file_system == "VIRTUAL" { "VIRTUAL" } else { "COPIED" }`.
- **Impact:** Typos like `--job-attachments-file-system VIRTUL` silently default to
  `COPIED` instead of erroring.
- **Resolution:** Add `value_parser = ["COPIED", "VIRTUAL"]` to the clap definition.

### AUDIT-022: `continue_callback` not wired for SIGINT during polling

- **Category:** Bug
- **Priority:** Medium
- **Command/Function:** `bundle.rs` → `SubmitJobParams.continue_callback`
- **Rust behavior:** CLI sets `continue_callback: None`, which becomes `Box::new(|| true)`.
  Progress bars check `should_continue()` for SIGINT, but the CreateJob polling loop
  uses the always-true callback.
- **Impact:** SIGINT during the CreateJob polling phase (after upload completes) won't
  cancel — the loop runs until timeout or completion.
- **Resolution:** Wire `continue_callback` to `should_continue()`.

### AUDIT-023: Spec incorrectly claims both CLIs support `file://` parameters

- **Category:** Spec drift
- **Priority:** Medium
- **Command/Function:** `specs/cli/bundle.md` differences table
- **Python behavior:** Observation #18 confirms `--parameter` only handles `Name=Value`.
  The `file://` parser is wired to `--submitter-info`, not `--parameter`.
- **Rust behavior:** Only `Name=Value` — matches Python.
- **Impact:** The spec's differences table (updated in this session) now incorrectly says
  both support `file://` and inline JSON for parameters.
- **Resolution:** Revert the bundle.md parameter format row to say `Name=Value` only for
  both Python and Rust.

### AUDIT-024: `CREATE_FAILED` output doesn't include job ID

- **Category:** Nice-to-have
- **Priority:** Medium
- **Command/Function:** `create_job_from_job_bundle()` in `submission.rs`
- **Rust behavior:** On `CREATE_FAILED`, returns `Err(op_err(status_message))` — the job
  ID is lost. On success, both message and job ID are printed.
- **Impact:** Users can't easily look up a failed job for debugging.
- **Resolution:** Include job ID in error: `"Job {job_id} creation failed: {status_message}"`.

### AUDIT-025: Submitter name default differs from Python

- **Category:** Behavioral gap (accepted)
- **Priority:** Low
- **Command/Function:** `bundle.rs` submitter_name default
- **Python behavior:** Default is `"deadline-cloud-cli"`.
- **Rust behavior:** Default is `"CLI"`. Documented in spec.
- **Resolution:** Accepted difference — already documented.

### AUDIT-026: No test for invalid parameter name regex

- **Category:** Missing coverage
- **Priority:** Low
- **Command/Function:** `parse_parameters()` in `bundle.rs`
- **Rust behavior:** Regex validation `[A-Za-z_][A-Za-z0-9_]*` is implemented but only
  invalid format (missing `=`) is tested. No test for `123bad=value`.
- **Resolution:** Add a snapshot test with an invalid parameter name.

### AUDIT-027: Priority hardcoded to 50 before being overwritten

- **Category:** Dead code
- **Priority:** Low
- **Command/Function:** `create_job_from_job_bundle()` in `submission.rs`
- **Rust behavior:** `create_job_args.insert("priority", json!(50))` is immediately
  overwritten by the conditional insert from `params.priority` (which always has `Some(50)`).
- **Resolution:** Remove the initial insert.

### AUDIT-028: Fleet-scoped config missing endpoint URL propagation

- **Category:** Latent bug
- **Priority:** Low
- **Command/Function:** `get_fleet_scoped_config()` in `log_retrieval.rs`
- **Rust behavior:** Builds `SdkConfig` from `AssumeFleetRoleForRead` credentials but
  doesn't propagate endpoint URL overrides from base config. Not triggered yet because
  worker logs aren't exposed via CLI.
- **Resolution:** Copy endpoint URL propagation pattern from `get_queue_user_config()`.

### AUDIT-029: Missing test for CloudWatch AccessDeniedException formatting

- **Category:** Missing coverage
- **Priority:** Low
- **Command/Function:** `cw_sdk_err()` in `log_retrieval.rs`
- **Rust behavior:** Error formatting looks correct but has no regression test.
- **Resolution:** Add a Level 1 or Level 2 test for CloudWatch 403 error formatting.

---

## Ranked Summary

| # | ID | Priority | Category | Issue |
|---|-----|----------|----------|-------|
| 1 | AUDIT-019 | High | Bug | Known-paths warning not implemented in bundle submit |
| 2 | AUDIT-020 | High | Gap/Spec drift | Job history not saved (dead code, spec says it should) |
| 3 | AUDIT-021 | Medium | Bug | `--job-attachments-file-system` no value validation |
| 4 | AUDIT-022 | Medium | Bug | `continue_callback` not wired for SIGINT during polling |
| 5 | AUDIT-023 | Medium | Spec drift | bundle.md incorrectly claims `file://` parameter support |
| 6 | AUDIT-024 | Medium | Nice-to-have | `CREATE_FAILED` output missing job ID |
| 7 | AUDIT-025 | Low | Accepted | Submitter name default differs ("CLI" vs "deadline-cloud-cli") |
| 8 | AUDIT-026 | Low | Coverage | No test for invalid parameter name regex |
| 9 | AUDIT-027 | Low | Dead code | Priority hardcoded before overwrite |
| 10 | AUDIT-028 | Low | Latent bug | Fleet-scoped config missing endpoint URL propagation |
| 11 | AUDIT-029 | Low | Coverage | Missing CloudWatch AccessDeniedException test |

### Accepted Differences

- Submitter name default (AUDIT-025) — documented, intentional
- `sanitize_path_for_filename` not yet ported — planned, no downstream usage
