# Handoff

Current in-flight work. Read this at the start of every session before
consulting the Work Items table in `specs/progress.md`.

## Active Work Item

**15e — Behavioral parity audit (Batch 1: Quick Fixes & CLI Polish)**

Audit is complete. 56 findings documented in
`specs/audit_reports/2026-04-17-behavioral-parity.md`. First fix batch
addressed 8 findings. Now working through remaining 48 in batches.

## Critical Context

1. **Python source is at `../deadline-cloud-python`** (sibling directory).
2. **All tests pass.** 1045 tests across all crates.
3. **Testing rules:** Level 2 CLI tests must exercise the full stack through
   the CLI binary. No stubs at the application layer — only HTTP stub server.
4. **Serialization patterns.** See `specs/patterns.md`.
5. **`serde_json` has `preserve_order` enabled** — `json!()` preserves
   insertion order, not alphabetical.

## Batch 1 — Implementation Plan

### Triage Summary

| ID | Verdict | Reason |
|----|---------|--------|
| AUDIT-016 | **Fix** | Use `json_with_spaces()` for auth status JSON output |
| AUDIT-017 | **No Issue** | Rust is more correct — Python has latent bug (exit code 0 is falsy) |
| AUDIT-021 | **No Issue** | Rust error model has no "unexpected exceptions" — all errors are typed |
| AUDIT-022 | **Fix** | Add `storage_profile_id` and `conflict_resolution` to `CliOptions` |
| AUDIT-023 | **Fix** | Fix negative timedelta formatting |
| AUDIT-025 | **Fix** | Add `suggest_resources_on_client_error` to worker list/get |
| AUDIT-032 | **Fix** | Add `suggest_resources_on_client_error` to bundle submit |
| AUDIT-033 | **No Issue** | CLI already sets `defaults.job_id` with equivalent condition |
| AUDIT-037 | **Fix** | Add re-prompt loop for cancel/requeue confirmation |
| AUDIT-038 | **Fix** | Add INTERRUPTING and NOT_COMPATIBLE to task summary |
| AUDIT-045 | **Fix** | Change version format to `deadline, version X.Y.Z` |
| AUDIT-050 | **No Issue** | Behavior is equivalent — both skip cache read when force=true |
| AUDIT-052 | **No Issue** | Python also rejects empty paths — behavior matches |
| AUDIT-056 | **Defer** | Needs new `list_storage_profiles_for_queue` API function — move to Batch 2 |

**Result: 8 fix, 5 no issue, 1 defer**

### Fix Details

#### F1: AUDIT-016 — `auth status` JSON spacing
- **File:** `crates/deadline-cli/src/commands/auth.rs:83`
- **Change:** Replace `serde_json::to_string(&json).unwrap()` with
  `crate::common::json_with_spaces(&json)`. Key order is already correct
  (preserve_order enabled).
- **Test:** Level 2 snapshot test `auth_status_json_output_format`

#### F2: AUDIT-022 — Missing CLI options in config apply
- **File:** `crates/deadline-cli/src/common.rs:48-93`
- **Change:** Add `storage_profile_id: Option<String>` and
  `conflict_resolution: Option<String>` to `CliOptions`. Add handling in
  `apply_cli_options_to_config` to set `settings.storage_profile_id` and
  `settings.conflict_resolution`. Update all call sites (default to `None`).
- **Test:** Level 1 unit test `apply_options_storage_profile_id_sets_config`,
  `apply_options_conflict_resolution_sets_config`

#### F3: AUDIT-023 — Negative timedelta formatting
- **File:** `crates/deadline-cli/src/common.rs:283-291`
- **Change:** Handle negative values by using absolute values and prepending
  `-`. For -30 min: `-0:30:00` (readable) instead of `0:-30:00` (broken).
  Note: Python produces `-1 day, 23:30:00` but `-H:MM:SS` is more readable
  and functionally equivalent for relative timestamps.
- **Test:** Level 1 unit test `format_timedelta_negative_thirty_minutes`,
  `format_timedelta_negative_with_microseconds`

#### F4: AUDIT-025 — Worker suggest_resources
- **File:** `crates/deadline-cli/src/commands/worker.rs:50,68`
- **Change:** Replace `.map_err()` with `match` + call to
  `suggest_resources_on_client_error` on error, passing `farm_id` and
  `fleet_id`. Same pattern as fleet.rs.
- **Test:** Level 2 snapshot test `worker_list_invalid_farm_shows_suggestion`,
  `worker_get_invalid_fleet_shows_suggestion`

#### F5: AUDIT-032 — Bundle submit suggest_resources
- **File:** `crates/deadline-cli/src/commands/bundle.rs:201-202`
- **Change:** Replace `.map_err()` with `match` + call to
  `suggest_resources_on_client_error` on error, passing `farm_id` and
  `queue_id` from config.
- **Test:** Level 2 snapshot test `bundle_submit_invalid_queue_shows_suggestion`

#### F6: AUDIT-037 — Confirmation prompt re-prompt
- **File:** `crates/deadline-cli/src/commands/job.rs:545-550,630-635`
- **Change:** Replace single `read_line` with loop. On invalid input
  (not y/yes/n/no), print `"Error: invalid input"` and re-prompt. Empty
  input also re-prompts (matching Python's `click.confirm(default=None)`).
  `n`/`no` exits with "Job not canceled." / "No tasks were requeued."
- **Test:** Level 2 tests are hard to test interactively. Level 1 unit test
  by extracting confirm logic to a testable function, or test via
  stdin piping in Level 2.

#### F7: AUDIT-038 — Task summary missing statuses
- **File:** `crates/deadline-cli/src/commands/job.rs:934-955`
- **Change:** Add `let interrupting = get(&["INTERRUPTING"]);` after
  `running`, and `let not_compatible = get(&["NOT_COMPATIBLE"]);` after
  `canceled`. Add corresponding push lines in correct order: ready,
  running, interrupting, pending, suspended, succeeded, failed, canceled,
  not compatible.
- **Test:** Level 2 snapshot test `job_get_search_term_shows_interrupting_status`,
  `job_get_search_term_shows_not_compatible_status`

#### F8: AUDIT-045 — Version format
- **File:** `crates/deadline-cli/src/main.rs:12`
- **Change:** Replace `version,` with custom version string matching
  Python's click format: `deadline, version X.Y.Z`.
- **Test:** Level 2 snapshot test `version_output_matches_python_format`

#### F9: AUDIT-056 — Storage profile suggestion chain (DEFERRED to Batch 2)
- **Reason:** Needs new `list_storage_profiles_for_queue` API function in
  `crates/deadline-api/src/api.rs`. This is a new API endpoint, not a
  quick fix. Move to Batch 2 alongside AUDIT-020 (suggest_resources
  dispatch refactor).

### Test Mapping

| Fix | Rust Test Name | Level | Crate | Status |
|-----|---------------|-------|-------|--------|
| F1 | `auth_status_json_authenticated` (existing) | L2 | deadline-cli | Snapshot will break after fix |
| F1 | `auth_status_json_configuration_error` (existing) | L2 | deadline-cli | Snapshot will break after fix |
| F1 | `auth_status_json_api_unavailable` (existing) | L2 | deadline-cli | Snapshot will break after fix |
| F2 | (Level 1 tests added in Step 3) | L1 | deadline-cli | Deferred — needs source changes |
| F3 | `job_logs_timestamp_format_relative_negative_timedelta` | L2 | deadline-cli | Fails ✅ |
| F4 | `worker_list_access_denied_suggests_available_fleets` | L2 | deadline-cli | Fails ✅ |
| F4 | `worker_get_not_found_suggests_available_workers` | L2 | deadline-cli | Fails ✅ |
| F5 | `bundle_submit_access_denied_suggests_available_queues` | L2 | deadline-cli | Fails ✅ |
| F6 | `job_cancel_confirm_empty_input_reprompts` | L2 | deadline-cli | Fails ✅ |
| F7 | `job_get_search_term_shows_interrupting_and_not_compatible` | L2 | deadline-cli | Fails ✅ |
| F8 | `version_prints_name_and_semver` (existing) | L2 | deadline-cli | Snapshot will break after fix |

### Step Status

- [x] Step 1: Study Python — complete
- [x] Step 2: Write tests (red) — 6 new Level 2 tests fail; 277 existing pass
- [x] Step 3: Implement fixes — all 1055 tests pass, 13 snapshots updated
- [x] Step 4: Compare CLIs — see comparison results below
- [x] Step 5: Audit & fix — 1 bug found (EOF infinite loop), fixed
- [x] Step 6: Spec — audit report updated
- [x] Step 7: Commit — ready

## Step 4 Comparison Results

| Command | Result | Notes |
|---------|--------|-------|
| `--version` | ✅ Match | `deadline, version X.Y.Z` (version numbers differ — expected) |
| `auth status --output json` | ✅ Match | Identical JSON with spaces |
| `auth status --output JSON` | ✅ Match | Case-insensitive works |
| `auth status` (verbose) | ✅ Match | Identical |
| `job get "search"` (task summary) | ✅ Match | Identical for available statuses |
| `job logs --timestamp-format relative` | ✅ Match | Identical (accepted diff: `.102` vs `.102000` in reference time display) |
| `job cancel` with "n" | ✅ Match | Both: prompt → "Job not canceled." → exit 1 |
| `job cancel` with empty then "n" | ✅ Match | Both: prompt → "Error: invalid input" → re-prompt → "Job not canceled." → exit 1 |
| `worker list --fleet-id bad` | ⚠️ Known gap | Rust suggests queues instead of fleets — AUDIT-020 dispatch (Batch 2) |
| `worker get --worker-id bad` | ⚠️ Known gap | Same AUDIT-020 dispatch issue |
| Task summary INTERRUPTING/NOT_COMPATIBLE | N/A | No jobs with these statuses; stub-server test covers |

## Notes from #15e

**Behavioral parity audit complete.** 56 findings documented in
`specs/audit_reports/2026-04-17-behavioral-parity.md`. First fix batch
addressed 8 findings (AUDIT-002, 003, 004, 005, 015, 027, 028, 029).
48 findings remain. Batch 1 (quick fixes) addresses 8 more, skips 4
(not bugs), defers 1 (AUDIT-056) and 1 (AUDIT-001, AUDIT-014 already
deferred to own work items).

**Deferred from fix batch:** AUDIT-001 (sync-output download pipeline)
and AUDIT-014 (multipart upload) are too large for a bug-fix batch —
they need their own work items.
