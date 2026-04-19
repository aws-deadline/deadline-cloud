# Audit: Behavioral Parity (post-April-10 changes)

**Date:** 2026-04-15
**Scope:** Code added/changed since the 2026-04-10 audit: `bundle submit` implementation,
credential scoping wiring, crate consolidation, and verification of previous fixes.
**Status:** Complete

## Summary

| Priority | Count | Fixed | Remaining |
|----------|-------|-------|-----------|
| Critical | 0     | 0     | 0         |
| High     | 1     | 1     | 0         |
| Medium   | 2     | 2     | 0         |
| Low      | 2     | 2     | 0         |

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

---

## Findings (parity gaps only)

### AUDIT-019: Files-outside-known-paths warning not implemented in bundle submit

- **Category:** Bug
- **Priority:** High
- **Command/Function:** `create_job_from_job_bundle()` in `submission.rs`, step 9
- **Python behavior:** Checks if input files are outside known asset paths, warns the
  user, and prompts for confirmation (unless `auto_accept` is true).
- **Rust behavior:** Known paths were computed but unused. No warning. No prompt.
- **Resolution:** Fixed. Wired known-paths check with TDD.

### AUDIT-021: `--job-attachments-file-system` lacks value validation

- **Category:** Bug
- **Priority:** Medium
- **Command/Function:** `BundleAction::Submit` clap definition in `bundle.rs`
- **Python behavior:** Uses `click.Choice([e.value for e in JobAttachmentsFileSystem])`
  — rejects invalid values at the CLI layer.
- **Rust behavior:** No `value_parser` constraint. Invalid values silently become `COPIED`.
- **Resolution:** Fixed — added `value_parser = ["COPIED", "VIRTUAL"]`.

### AUDIT-022: `continue_callback` not wired for SIGINT during polling

- **Category:** Bug
- **Priority:** Medium
- **Command/Function:** `bundle.rs` → `SubmitJobParams.continue_callback`
- **Python behavior:** Wires `_check_create_job_wait_canceled` which checks
  `sigint_handler.continue_operation`. SIGINT during polling cancels the wait.
- **Rust behavior:** CLI sets `continue_callback: None`, which becomes always-true.
  SIGINT during CreateJob polling is ignored.
- **Resolution:** Fixed — wired `continue_callback` to `should_continue()`.

### AUDIT-026: No test for invalid parameter name regex

- **Category:** Missing coverage
- **Priority:** Low
- **Command/Function:** `parse_parameters()` in `bundle.rs`
- **Rust behavior:** Regex validation exists but only invalid format (missing `=`) is
  tested. No test for invalid name like `123bad=value`.
- **Resolution:** Fixed — snapshot test added. Verified: removing regex check causes test failure.

### AUDIT-029: Missing regression test for CloudWatch error formatting

- **Category:** Regression guard
- **Priority:** Low
- **Command/Function:** `cw_sdk_err()` in `log_retrieval.rs`
- **Rust behavior:** Error formatting extracts error codes correctly (F-1 fix), but
  has no regression test. If refactored, could silently regress to generic "service error".
- **Resolution:** Fixed — Level 2 test added. Verified: breaking `cw_sdk_err()` produces
  `"service error"` instead of `"AccessDeniedException"`, caught by test.

---

## Not parity gaps (removed from action items)

| ID | Original finding | Why removed |
|----|-----------------|-------------|
| AUDIT-020 | Job history not saved in CLI | Python CLI doesn't save history either — GUI only. Matches Python. Spec updated. |
| AUDIT-024 | CREATE_FAILED missing job ID | Python also omits job ID in this error. Our fix is a Rust improvement, not a parity fix. Already applied — keeping it. |
| AUDIT-025 | Submitter name "CLI" vs "deadline-cloud-cli" | Python also defaults to `"CLI"`. No difference exists. |
| AUDIT-027 | Priority hardcoded before overwrite | Python uses the same pattern (`"priority": 50` then overwrite). Not dead code — it's the default. |
| AUDIT-028 | Fleet-scoped config missing endpoint URL | Worker logs not exposed via CLI. Not a parity gap for current scope. |
| AUDIT-029 | Missing CloudWatch AccessDeniedException test | Test coverage gap, not a behavioral parity issue. Moved to Low as regression guard for F-1 fix. |

---

## Ranked Summary

| # | ID | Priority | Category | Status |
|---|-----|----------|----------|--------|
| 1 | AUDIT-019 | High | Bug | ✅ Fixed |
| 2 | AUDIT-021 | Medium | Bug | ✅ Fixed |
| 3 | AUDIT-022 | Medium | Bug | ✅ Fixed |
| 4 | AUDIT-026 | Low | Coverage | ✅ Fixed |
| 5 | AUDIT-029 | Low | Regression guard | ✅ Fixed |
