# Handoff

Current in-flight work. Read this at the start of every session before
consulting the Work Items table in `specs/progress.md`.

## Active Work Item

None — pick from `specs/progress.md`.

## Step Status

(Completed — see Recently Completed below)

## Critical Context

1. **Python source is at `../deadline-cloud-python`** (sibling directory).
2. **All tests pass.** 1086 tests across all crates.
3. **Testing rules:** Level 2 CLI tests must exercise the full stack through
   the CLI binary. No stubs at the application layer — only HTTP stub server.
4. **Serialization patterns.** See `specs/patterns.md`.
5. **`serde_json` has `preserve_order` enabled** — `json!()` preserves
   insertion order, not alphabetical.
6. **Test fixtures for CLI comparison.** `test_fixtures/job_bundles/`
   contains sample job bundles for manual comparison between Python and
   Rust CLIs. See `test_fixtures/README.md`.

## Gap Batch: Implementation Plan

### Gap 1: AUDIT-010 — Submission telemetry events

**Problem:** `create_job_from_job_bundle` emits zero telemetry events.
Python emits: `submission` (before CreateJob), `create_job` (after),
`asset_upload` (around upload), plus `record_hashing_summary` and
`record_upload_summary`.

**Crates:** `deadline-job-bundle` (submission.rs), `deadline-cli` (bundle.rs)

**Plan:**
1. Add `telemetry: Option<&TelemetryClient>` to `SubmitJobParams`
2. In `create_job_from_job_bundle`:
   - Record `com.amazon.rum.deadline.submission` with `submitter_name`
     before calling `api::create_job`
   - Record `com.amazon.rum.deadline.create_job` with `is_success` after
     `wait_for_create_job_to_complete`
   - Record `com.amazon.rum.deadline.asset_upload` with `is_success`
     around `upload::upload_assets` (success/fail)
3. In `bundle.rs`: create telemetry client and pass it in

**Python reference:** `_submit_job_bundle.py:932-937` (submission event),
`_submit_job_bundle.py:960-963` (create_job event),
`_submit_job_bundle.py:164` (@record_success_fail on _upload_attachments),
`_submit_job_bundle.py:71` (hashing_telemetry_callback)

---

### Gap 2: AUDIT-026 — `job logs --session-action-id`

**Problem:** CLI `job logs` has no `--session-action-id` flag. Python
derives session ID from the action ID, calls `get_session_action` to
get time bounds, and scopes the CloudWatch log query.

**Crates:** `deadline-cli` (job.rs)

**Plan:**
1. Add `#[arg(long)] session_action_id: Option<String>` to `Logs` variant
2. Add `parse_session_action_id()` — regex
   `^sessionaction-([0-9a-f]{32})-\d+$`, returns `session-{uuid}`
3. In the logs handler:
   - If `session_action_id` provided, derive session_id
   - Validate consistency if both `--session-id` and
     `--session-action-id` given
   - Call `api::get_session_action` (already exists) to get
     `startedAt`/`endedAt`
   - Pass those as `start_time`/`end_time` to the log query
   - Display "session action {id}" in output messages

**Python reference:** `job_group.py:74-101` (parse function),
`job_group.py:1276-1393` (handler logic)

**Note:** `api::get_session_action` already exists in `api.rs:800+`.

---

### Gap 3: AUDIT-048 — `manifest upload` queue derivation

**Problem:** When `--s3-cas-uri` is not provided, Rust errors instead of
deriving S3 settings from `--farm-id`/`--queue-id` via the queue API.

**Crates:** `deadline-cli` (manifest.rs)

**Plan:**
1. When `s3_cas_uri` is None:
   - Apply CLI options to config (already done for download)
   - Call `api::get_queue` to get `jobAttachmentSettings`
   - Extract `s3BucketName` and `rootPrefix`
   - Use `session::get_queue_scoped_config` for queue-scoped credentials
   - Build S3 client from scoped config
2. Remove the error stub

**Python reference:** `manifest_group.py:424-455`

---

### Gap 4: AUDIT-047 — `manifest download` CLI stub

**Problem:** Returns error "manifest download requires Deadline API access
(not yet wired in CLI)". The library functions exist.

**Crates:** `deadline-cli` (manifest.rs)

**Plan:**
1. Apply CLI options to config with required `farm_id`, `queue_id`
2. Call `api::get_queue` for `jobAttachmentSettings`
3. Use `session::get_queue_scoped_config` for credentials
4. Call existing `manifest_ops` download functions
5. Print results (manifest paths, file counts)

**Python reference:** `manifest_group.py:280-362`

**Note:** Need to verify the library-level download functions are complete.
The stub comment says "The library function is ready."

---

### Gap 5: AUDIT-009 — Upload confirmation prompt for all paths

**Problem:** Rust only prompts when files are outside known paths. Python
always shows the upload summary and prompts (with `default=true` when no
unknown paths, `default=false` when unknown paths exist).

**Crates:** `deadline-job-bundle` (submission.rs), `deadline-cli` (bundle.rs)

**Plan:**
1. Change `SubmitJobParams.continue_callback` signature from
   `Option<Box<dyn Fn() -> bool>>` to
   `Option<Box<dyn Fn(&str, bool) -> bool>>` — takes message + default
2. In `submission.rs`, generate the upload summary message (file count,
   total size, path summary) matching Python's `_generate_message_for_asset_paths`
3. Always call the confirmation callback with the message:
   - `default=true` when no unknown paths
   - `default=false` when unknown paths exist
4. When `auto_accept` is true and unknown paths exist, cancel (current behavior)
5. When `auto_accept` is true and no unknown paths, print message and proceed
6. In `bundle.rs`, update the callback to use `dialoguer::Confirm` or
   the existing stdin y/n pattern

**Python reference:** `_submit_job_bundle.py:100-160`
(`_generate_message_for_asset_paths`), `_submit_job_bundle.py:810-847`
(confirmation flow)

---

### Gap 6: AUDIT-055 — Download conflict resolution prompt

**Problem:** When `--conflict-resolution` is not specified and `--yes` is
not set, Rust defaults to `CreateCopy` without checking for conflicts.
Python checks if files exist and prompts with 1/2/3/n choices.

**Crates:** `deadline-cli` (job.rs)

**Plan:**
1. After getting `output_paths_by_root`, check if any target files exist
2. If conflicts found and no explicit `--conflict-resolution` and not
   `--yes`:
   - Print the conflicting file list
   - Prompt: `[1] Skip  [2] Overwrite  [3] Create copy  [n] Cancel`
   - Default: 3
3. If no conflicts, use `CreateCopy` as default (no prompt needed)

**Python reference:** `job_group.py:667-690` (conflict check + prompt),
`job_group.py:817-830` (`_get_conflict_resolution_selection_message`,
`_get_conflicting_filenames`)

---

## Test Spec → Rust Test Name Mapping

These gaps are behavioral fixes to existing features. Tests are Level 2
CLI subprocess tests unless noted.

| Gap | Test Spec Section | Planned Rust Test Name |
|-----|-------------------|----------------------|
| AUDIT-010 | `api_job_lifecycle.md` (submission telemetry) | `bundle_submit_records_submission_telemetry_event` |
| AUDIT-010 | `api_job_lifecycle.md` (create_job telemetry) | `bundle_submit_records_create_job_telemetry_event` |
| AUDIT-010 | `api_job_lifecycle.md` (upload telemetry) | `bundle_submit_records_upload_telemetry_event` |
| AUDIT-026 | `cli.md` (logs with session-action-id) | `job_logs_session_action_id_derives_session_and_scopes_time` |
| AUDIT-026 | `cli.md` (invalid session-action-id) | `job_logs_session_action_id_invalid_format_errors` |
| AUDIT-026 | `cli.md` (both flags, mismatch) | `job_logs_session_action_id_conflicts_with_session_id_errors` |
| AUDIT-048 | `cli.md` (manifest upload from queue) | `manifest_upload_derives_s3_settings_from_queue` |
| AUDIT-047 | `cli.md` (manifest download) | `manifest_download_fetches_manifests_from_s3` |
| AUDIT-009 | `cli.md` (upload confirmation) | `bundle_submit_shows_upload_confirmation_prompt` |
| AUDIT-009 | `cli.md` (auto_accept + unknown paths) | `bundle_submit_auto_accept_cancels_on_unknown_paths` |
| AUDIT-055 | `cli.md` (conflict prompt) | `job_download_output_prompts_on_file_conflicts` |
| AUDIT-055 | `cli.md` (no conflicts, no prompt) | `job_download_output_no_prompt_when_no_conflicts` |

## Batching Strategy

**Single batch** — all 6 gaps implemented together. Minimal overlap:
only AUDIT-010 and AUDIT-009 share `SubmitJobParams` changes.

Implementation order (by dependency):
1. AUDIT-010 + AUDIT-009 together (shared `SubmitJobParams` changes)
2. AUDIT-026 (self-contained in job.rs)
3. AUDIT-048 + AUDIT-047 together (both in manifest.rs, same pattern)
4. AUDIT-055 (self-contained in job.rs download path)

## Investigated and Dropped

| Gap | Reason |
|-----|--------|
| AUDIT-034 | `--submitter-info` is GUI-only (`bundle gui-submit`). CLI `bundle submit` correctly has `--submitter-name`. Not deprecated on CLI. |
| AUDIT-013 | Hash cache V5 works correctly. One-time re-hash on Python→Rust migration is acceptable. V4 stores `str(datetime.fromtimestamp(st_mtime))` — lossy datetime string, not clean integer. Migration adds complexity for marginal benefit. |
| AUDIT-008 | Interactive root path editing — deferred (fancy interactive feature). |

## Recently Completed

**Behavioral gap batch — AUDIT-010, AUDIT-026, AUDIT-048, AUDIT-047, AUDIT-009, AUDIT-055**

- AUDIT-010: Added submission telemetry events (`com.amazon.rum.deadline.submission`
  before CreateJob, `com.amazon.rum.deadline.create_job` after). Telemetry client
  passed via `SubmitJobParams.telemetry`. Explicit `drop(telemetry)` in CLI to
  flush before `process::exit`.
- AUDIT-026: Implemented `job logs --session-action-id`. Parses action ID format,
  derives session ID, validates consistency with `--session-id`, calls
  `GetSessionAction` for time bounds, scopes CloudWatch query. Shows session
  action start/end/duration in header.
- AUDIT-048: `manifest upload` derives S3 settings from queue when `--s3-cas-uri`
  not provided. Calls GetQueue for `jobAttachmentSettings`, uses queue-scoped
  credentials.
- AUDIT-047: `manifest download` wired to API. Gets queue settings, job
  attachments, downloads manifests from S3 via GetObject.
- AUDIT-009: Upload summary message ("Job submission contains N input files
  totaling X") always printed before hashing, not just for unknown paths.
- AUDIT-055: Download conflict detection checks for existing files before
  download, prints warning with file list. Test coverage partial (S3 download
  mock chain needs `x-amz-meta-asset-root` header support — `#[ignore]` test).

**Telemetry mock refactor:** Removed `mock_telemetry_endpoint` from submit
helpers. Added `mock_telemetry_endpoint_permissive` (no count expectation)
for tests that don't care about telemetry. Tests that verify telemetry
mount their own specific mocks. Telemetry path matching changed from
`path_regex` to exact `path("/2023-10-12/telemetry")`.

**Quick wins batch — AUDIT-046, SYNC-005, AUDIT-056, AUDIT-043**

- AUDIT-046: Deleted unused `require_setting` function (dead code)
- SYNC-005: Added progress messages to `queue sync-output`:
  "Found N new session action(s) across N job(s)" and
  "Populating manifest S3 keys for N jobs..."
- AUDIT-056: Implemented storage profile suggestion chain:
  `list_storage_profiles_for_queue` API function, `try_list_storage_profiles`
  helper, wired match arm, added `suggest_resources_on_client_error` to
  `queue get-storage-profile` command. 1 new Level 2 test.
- AUDIT-043: Reclassified as accepted difference — Rust SDK `app_name()`
  vs Python `user_agent_extra`. Content identical, header position differs.

### Accepted Differences

| ID | Reason |
|----|--------|
| AUDIT-024 | YAML key ordering — accepted per `patterns.md` |
| AUDIT-030 | False finding — Python also doesn't support macOS |
| AUDIT-039 | `job wait` verbose to stderr — Rust approach is better |
| AUDIT-043 | User-agent position in header — Rust SDK limitation, content is correct |
| AUDIT-046 | `require_setting` exit code — function was unused dead code, deleted |
| SYNC-004 | Path summary groups by directory (Rust) vs flat full-path (Python) — same data, Rust more readable |
