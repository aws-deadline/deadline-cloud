# Handoff

Current in-flight work. Read this at the start of every session before
consulting the Work Items table in `specs/progress.md`.

Active work item: Python parity audit (2026-05-11)

---

## Python Parity Audit — 2026-05-11

**Source:** `deadline-cloud-python` commits `78b10da..509b7bd` (15 commits)

### Gaps Identified

#### GAP-1 — Login loop session refresh (Bug fix, Small)

**Python PR:** `3dfd14b` — "fix: restore deadline auth login after STS-to-ListFarms switch"

**Problem:** In the DCM login polling loop, the Rust code calls
`check_authentication_status(config)` which uses the cached `SdkConfig`.
When DCM writes new profile keys (`user_id`, `identity_store_id`,
`monitor_id`) to `~/.aws/config`, the cached config never picks them up,
so the auth probe keeps failing and login never resolves to AUTHENTICATED.

**Python fix:** Calls `_session.get_boto3_session(force_refresh=True)` before
each `check_authentication_status` in the login loop.

**Rust fix:** Add `session::invalidate_session_cache_async().await` before
`check_authentication_status(config).await` in `auth.rs::login_inner` loop.

**Files:** `crates/deadline-api/src/auth.rs`
**Effort:** ~5 lines

---

#### GAP-2 — `deadline job download-input` command (New feature, Large)

**Python PR:** `242bb81` — "feat(cli): Add download-input command with --include filtering"

**Description:** New CLI command to download a job's input attachments.
Supports `--include` glob filtering, `--match-paths-by JOB|LOCAL`,
`--ignore-storage-profiles`, `--conflict-resolution`, `--yes`, `--output json`.

**Implementation scope:**
1. `deadline-job-attachments` crate:
   - `InputDownloader` struct (reads manifests from job's `attachments` field)
   - `filter_manifests(manifests_by_root, patterns)` function (glob matching)
   - `apply_include_filters(&mut self, patterns)` method on both downloaders
   - `normalize_filters(patterns)` utility function
2. `deadline-cli` crate:
   - New `DownloadInput` variant in `JobAction` enum
   - `download_input_impl` function (mirrors `download_output_impl` structure)
   - Shared helpers: `_prompt_for_os_mismatch_roots`, `_prompt_to_confirm_roots`

**Depends on:** GAP-3 (shared filtering infrastructure)

---

#### GAP-3 — `--include` and `--match-paths-by` on `download-output` (New feature, Medium)

**Python PR:** `b601164` — "feat(cli): Add --include-path selective filtering to download-output"

**Description:** New `-i/--include` (repeatable) and `--match-paths-by JOB|LOCAL`
options on `deadline job download-output`. Allows selective download of specific
files matching glob patterns.

**Implementation scope:**
1. `deadline-job-attachments` crate:
   - `filter_manifests(manifests_by_root, patterns)` — filters manifest paths by glob
   - `normalize_filters(patterns)` — normalizes `\` → `/`, strips `./`, collapses `//`
   - `OutputDownloader::apply_include_filters(&mut self, patterns)` method
   - `OutputDownloader::get_paths_by_root()` (rename of `get_output_paths_by_root`)
2. `deadline-cli` crate:
   - Add `--include` (Vec<String>) and `--match-paths-by` (enum) to `DownloadOutput`
   - Pass `include_filters` to `OutputDownloader::new()` when `match_paths_by == JOB`
   - Call `apply_include_filters` after root editing when `match_paths_by == LOCAL`

**Files:**
- `crates/deadline-job-attachments/src/download.rs`
- `crates/deadline-cli/src/commands/job.rs`

---

### Items Already Aligned (No Action Needed)

| Python Change | Status |
|---------------|--------|
| `_session.py`: STS → ListFarms auth probe | ✅ Rust already uses ListFarms |
| `_session.py`: Remove `sts_regional_endpoints` | ✅ N/A for Rust SDK |
| `_list_apis.py`: `_apply_principal_id_filter` refactor | ✅ Code cleanup only |
| `job_group.py`: job logs ISO timestamp parsing | ✅ Rust already handles this |

---

### Recommended Implementation Order

1. **GAP-1** (bug fix, immediate) — 1 line change, high impact for DCM users
2. **GAP-3** (feature, medium) — builds the filtering infrastructure
3. **GAP-2** (feature, large) — reuses GAP-3 infrastructure + adds InputDownloader

### Status

- [x] Audit complete
- [x] GAP-1 implemented (Steps 1-5 complete, pending manual retest with fresh SSO)
- [x] GAP-3 implemented — Steps 1-7 complete
- [x] GAP-2 implemented — Steps 1-7 complete

---

### GAP-3 Implementation Plan — `--include` and `--match-paths-by` on `download-output`

#### Summary

Add `-i/--include` (repeatable glob patterns) and `--match-paths-by JOB|LOCAL`
options to `deadline job download-output`. Filters select which files to download
using fnmatch-style glob matching against the full path (root + relative).

#### Key Behavioral Contract (from Python)

1. **`_normalize_filters(patterns)`** — normalizes `\` → `/`, strips `./`, collapses `//`
2. **`_matches_any_filter(file_path, filters)`** — fnmatch matching with:
   - `*` matches across `/` separators (fnmatch behavior)
   - Filter ending with `/` → appends `*` (directory match)
   - Relative filter (not starting with `/` or `*` or drive letter) → prepends `*/`
3. **`--match-paths-by LOCAL` (default)** — filters applied AFTER root editing
   (against workstation paths). Calls `apply_include_filters()` post-root-edit.
4. **`--match-paths-by JOB`** — filters applied at construction time (against
   original job submission paths). Passed as `include_filters` to `OutputDownloader::new()`.
5. **Multiple `--include` values are OR'd** — file matches if ANY filter matches.
6. **If filters result in empty paths** → print "no output files available" message.

#### Crate Changes

**`deadline-job-attachments` (`crates/deadline-job-attachments/src/download.rs`):**

1. Add `apply_include_filters(&mut self, patterns: &[String])` to `OutputDownloader`
   - Stores filter group, calls `_rebuild()` (same pattern as Python)
   - Requires refactoring `OutputDownloader` to store `_initial_outputs_by_root`
     and `_include_filter_groups` (currently it only stores `outputs_by_root`)
2. Add `_rebuild(&mut self)` private method — recomputes `outputs_by_root` from
   initial state by applying root mappings then filter groups sequentially
3. Add `include_filters` parameter to `OutputDownloader::new()` — applies initial
   filter group at construction (for `--match-paths-by JOB`)
4. Add module-level functions:
   - `pub fn normalize_filters(patterns: &[String]) -> Vec<String>`
   - `pub fn matches_any_filter(file_path: &str, filters: &[String]) -> bool`
   - `fn full_path(root: &str, relative: &str) -> String` (private)
   - `pub fn filter_manifests(manifests_by_root: &HashMap<String, Vec<AssetManifest>>, filters: &[String]) -> HashMap<String, Vec<AssetManifest>>`

**`deadline-cli` (`crates/deadline-cli/src/commands/job.rs`):**

5. Add `--include` (`Vec<String>`, short `-i`) and `--match-paths-by` (enum `JOB|LOCAL`,
   default `LOCAL`) to `DownloadOutput` variant
6. Update `download_output_impl` signature to accept `include_patterns: Option<Vec<String>>`
   and `match_paths_by: MatchPathsBy`
7. Pass `include_filters` to `OutputDownloader::new()` when `match_paths_by == JOB`
8. After root editing loop, if `match_paths_by == LOCAL` and patterns exist:
   call `downloader.apply_include_filters(patterns)`, re-check for empty paths

#### Test Plan

| Test Case | Rust Test Name | Level |
|-----------|---------------|-------|
| Basic --include glob filters matching files | `download_output_include_glob_filters_matching_files` | L2 (CLI) |
| --include with exact file path | `download_output_include_exact_file_path` | L2 |
| Multiple --include values OR'd | `download_output_include_multiple_patterns_ored` | L2 |
| --include with no matches → "no output" msg | `download_output_include_no_match_shows_no_output` | L2 |
| --include matches full workstation path | `download_output_include_matches_full_local_path` | L2 |
| --match-paths-by JOB filters at construction | `download_output_match_paths_by_job` | L2 |
| Relative path filter auto-prepends `*/` | `download_output_include_relative_path_prepends_star` | L2 |
| normalize_filters unit tests | `normalize_filters_*` | L1 (unit) |
| matches_any_filter unit tests | `matches_any_filter_*` | L1 (unit) |

---

### GAP-2 Implementation Plan — `deadline job download-input` command

#### Summary

New CLI command to download a job's input attachments from S3. Reads the
`attachments.manifests[].inputManifestPath` from the job, downloads those
manifests from S3, and downloads the referenced files. Supports the same
`--include` / `--match-paths-by` filtering as download-output.

#### Key Behavioral Contract (from Python)

1. **No `--step-id` or `--task-id`** — inputs are job-level only
2. **Reads `job.attachments`** — parses `ManifestProperties` from the job's
   `attachments.manifests[]` array (each has `rootPath`, `rootPathFormat`,
   `inputManifestPath`, `inputManifestHash`)
3. **Downloads input manifests from S3** — key is `{rootPrefix}/Manifests/{inputManifestPath}`
4. **Same interactive flow** as download-output: OS mismatch prompt, root editing,
   conflict resolution, progress bar
5. **"No input attachments found"** if job has no `attachments` field
6. **"No input files available"** if manifests are empty
7. **"No input files match the provided filters"** if --include filters everything out

#### Crate Changes

**`deadline-job-attachments` (`crates/deadline-job-attachments/src/download.rs`):**

1. Add `InputDownloader` struct — similar to `OutputDownloader` but:
   - Constructor takes `Attachments` (parsed from job) instead of step/task IDs
   - Downloads input manifests from S3 using `inputManifestPath` from each
     `ManifestProperties` entry
   - Same `get_paths_by_root()`, `set_root_path()`, `apply_include_filters()`,
     `download()` interface
2. Extract shared trait or base behavior between `OutputDownloader` and
   `InputDownloader` (both need `_rebuild`, filter groups, root mappings).
   Per patterns.md principle #5 ("don't over-abstract"), use a shared private
   helper struct `FilterableManifests` rather than a trait.

**`deadline-cli` (`crates/deadline-cli/src/commands/job.rs`):**

3. Add `DownloadInput` variant to `JobAction` enum with options:
   `--profile`, `--farm-id`, `--queue-id`, `--job-id`, `-i/--include`,
   `--match-paths-by`, `--ignore-storage-profiles`, `--conflict-resolution`,
   `--yes`, `--output`
4. Add `download_input_impl` function — mirrors `download_output_impl` structure:
   - GetJob → parse attachments → build InputDownloader
   - Same OS mismatch prompt, root editing, filter application, conflict
     resolution, progress bar, summary output
5. Rename `get_output_paths_by_root()` → `get_paths_by_root()` on both
   downloaders (Python renamed this for consistency)

#### Test Plan

| Test Case | Rust Test Name | Level |
|-----------|---------------|-------|
| Basic download-input downloads all files | `download_input_downloads_all_input_files` | L2 |
| --include glob filters input files | `download_input_include_glob_filters` | L2 |
| --include no match → "no input files match" | `download_input_include_no_match` | L2 |
| No attachments → "no input attachments" | `download_input_no_attachments_message` | L2 |
| --match-paths-by JOB on input | `download_input_match_paths_by_job` | L2 |
| InputDownloader unit tests | `input_downloader_*` | L1 |

---

### GAP-2 Detailed Implementation Plan (Step 1 Study Complete)

#### Python Source Files Studied

- `src/deadline/client/cli/_groups/job_group.py` — `_download_job_input()` (L1135-1280), `_build_attachments()` (L1124-1134), `job_download_input` click command (L1282-1384)
- `src/deadline/client/cli/_groups/_job_download_helpers.py` — `_normalize_filters()`, `_prompt_for_os_mismatch_roots()`, `_prompt_to_confirm_roots()`, `_execute_download_with_progress()`, `MatchPathsBy` enum
- `deadline/job_attachments/download.py` — `InputDownloader` (L1560-1580), `_BaseFilterableDownloader` (L1353-1500), `get_job_input_paths_by_asset_root()` (L315-345)
- `deadline/job_attachments/models.py` — `ManifestProperties`, `Attachments`
- `test/cli_e2e/test_job_download_input.py` — 4 e2e tests

#### Key Observations

1. **`InputDownloader` is nearly identical to `OutputDownloader`** — both inherit from `_BaseFilterableDownloader`. The only difference is the data source: `InputDownloader` calls `get_job_input_paths_by_asset_root(s3_settings, attachments, session)` while `OutputDownloader` calls `get_job_output_paths_by_asset_root(...)`.

2. **`get_job_input_paths_by_asset_root`** iterates `attachments.manifests`, downloads each manifest from S3 at key `{rootPrefix}/Manifests/{inputManifestPath}`, and groups results by `rootPath`.

3. **The Rust `manifest_ops.rs` already has this logic** (L470-500) for the `manifest download` command — it downloads input manifests from S3 using the same key construction. We can extract this into a reusable function.

4. **The CLI flow for `download-input` mirrors `download-output`** exactly:
   - GetJob → parse attachments → build downloader
   - OS mismatch prompt → root editing → filter application
   - Conflict resolution → progress bar → summary

5. **No `--step-id` or `--task-id`** — inputs are job-level only (simpler than download-output).

6. **Messages differ slightly:**
   - "No input attachments found for this job." (no `attachments` field)
   - "No input files available for download." (manifests empty)
   - "No input files match the provided filters." (filters eliminate all)
   - "Downloading input for Job 'NAME'" (start message)
   - "Downloading Inputs" (progress bar label)
   - Telemetry metric: `download_job_input`

#### Implementation Steps

**Step 2a: `InputDownloader` in `deadline-job-attachments/src/download.rs`**

Add `get_input_manifests_by_asset_root()` — extracted from `manifest_ops.rs` logic:
```
pub async fn get_input_manifests_by_asset_root(
    s3_settings: &JobAttachmentS3Settings,
    attachments: &Attachments,
    s3_client: &S3Client,
    account_id: &str,
) -> Result<HashMap<String, Vec<AssetManifest>>, JobAttachmentsError>
```

Add `InputDownloader` struct — same fields as `OutputDownloader` minus step/task/session_action_id:
```rust
pub struct InputDownloader {
    s3_settings: JobAttachmentS3Settings,
    initial_inputs_by_root: HashMap<String, Vec<AssetManifest>>,
    include_filter_groups: Vec<Vec<String>>,
    root_mappings: HashMap<String, String>,
    inputs_by_root: HashMap<String, Vec<AssetManifest>>,
    s3_client: S3Client,
    account_id: String,
}
```

Methods: `new()`, `rebuild()`, `get_paths_by_root()`, `set_root_path()`, `apply_include_filters()`, `download()`.

The `rebuild()`, `set_root_path()`, `apply_include_filters()` logic is identical to `OutputDownloader`. Extract a private helper function `rebuild_manifests(initial, root_mappings, filter_groups) -> HashMap<String, Vec<AssetManifest>>` to share between both downloaders (per patterns.md #5: shared helper, not a trait).

**Step 2b: `DownloadInput` CLI variant in `deadline-cli/src/commands/job.rs`**

Add `DownloadInput` variant to `JobAction` enum with options:
- `--profile`, `--farm-id`, `--queue-id`, `--job-id`
- `-i/--include` (Vec<String>)
- `--match-paths-by` (default "LOCAL")
- `--ignore-storage-profiles` (flag)
- `--conflict-resolution`
- `--yes`
- `--output` (verbose|json)

Add `run_download_input()` → `download_input_impl()` following the same pattern as `run_download_output()`.

**Step 2c: `download_input_impl` function**

Flow:
1. GetJob → parse `job.attachments()` into `Attachments` struct
2. If no attachments → print "No input attachments found" → return
3. GetQueue → extract `jobAttachmentSettings`
4. Build S3 client with queue-scoped credentials
5. Create `InputDownloader::new(s3_settings, attachments, s3_client, account_id, job_filters)`
6. Check `get_paths_by_root()` — if empty → "No input files available"
7. OS mismatch prompt (same code as download-output)
8. Root editing loop (same code as download-output)
9. Apply LOCAL filters if applicable — if empty → "No input files match the provided filters"
10. Path summary
11. Conflict resolution
12. Download with progress bar ("Downloading Inputs")
13. Print summary
14. Telemetry: `download_job_input`

**Step 2d: Refactor shared code**

The OS mismatch prompt, root editing loop, path summary, and conflict resolution logic is duplicated between `download_output_impl` and `download_input_impl`. Extract shared helpers:
- `prompt_os_mismatch_roots(downloader, paths, format_mapping, is_json)` — trait-free, takes closures for `set_root_path` and `get_paths_by_root`
- OR: just duplicate the code (it's ~60 lines) since the two functions have slightly different messages. Per patterns.md #5, don't over-abstract.

Decision: **Duplicate with minor message changes.** The shared helpers in Python (`_prompt_for_os_mismatch_roots`, `_prompt_to_confirm_roots`) work because Python has duck typing. In Rust, making these generic over `OutputDownloader`/`InputDownloader` would require a trait, which patterns.md discourages unless it solves a real problem. The duplication is ~80 lines and the messages differ.

#### Cross-Reference: Test Spec → Rust Test Names

| Test Case (from Python e2e) | Rust Test Name | Level |
|------------------------------|---------------|-------|
| Basic download-input downloads all files | `job_download_input_downloads_all_input_files` | L2 |
| --include glob filters input files | `job_download_input_include_glob_filters` | L2 |
| --include no match → "no input files match" | `job_download_input_include_no_match` | L2 |
| No attachments → "no input attachments" | `job_download_input_no_attachments_message` | L2 |
| No input manifests (empty paths) → "no input files available" | `job_download_input_no_files_available` | L2 |
| --match-paths-by JOB filters at construction | `job_download_input_match_paths_by_job` | L2 |
| --output json error → JSON error line | `job_download_input_json_mode_error` | L2 |
| --output json no attachments → JSON summary | `job_download_input_json_mode_no_attachments` | L2 |
| Missing required args (farm/queue/job) | `job_download_input_missing_*_exits_with_error` | L2 |
| Telemetry success event | `download_input_success_emits_telemetry_event` | L2 |
| Telemetry failure event | `download_input_failure_emits_telemetry_event` | L2 |
| `get_input_manifests_by_asset_root` unit | `get_input_manifests_by_asset_root_*` | L1 |

#### Files Changed

| File | Change |
|------|--------|
| `crates/deadline-job-attachments/src/download.rs` | Add `get_input_manifests_by_asset_root()`, `InputDownloader`, extract `rebuild_manifests()` helper |
| `crates/deadline-cli/src/commands/job.rs` | Add `DownloadInput` variant, `run_download_input()`, `download_input_impl()` |
| `crates/deadline-cli/tests/cli/job_download.rs` | Add ~11 L2 tests for download-input |
| `crates/deadline-cli/tests/cli/telemetry_parity.rs` | Add 2 telemetry tests |

#### Estimated Effort

- Step 2a (InputDownloader): ~80 lines new code + ~20 lines refactored from OutputDownloader
- Step 2b (CLI variant): ~30 lines
- Step 2c (download_input_impl): ~180 lines (mostly mirroring download_output_impl with different messages)
- Step 2d (shared refactor): ~20 lines (extract `rebuild_manifests` helper)
- Tests: ~300 lines
- Total: ~600 lines

### Batching Strategy

**Batch 1 (GAP-3):** Filtering infrastructure + download-output integration
- `normalize_filters`, `matches_any_filter`, `filter_manifests` functions
- Refactor `OutputDownloader` to support `_rebuild` pattern with filter groups
- Add `--include` and `--match-paths-by` CLI args
- All GAP-3 tests

**Batch 2 (GAP-2):** InputDownloader + download-input command
- `InputDownloader` struct (reuses filtering infrastructure from Batch 1)
- `download_input_impl` CLI function
- All GAP-2 tests

This ordering ensures GAP-3 builds the shared infrastructure that GAP-2 reuses.

### GAP-1 Implementation Details

**Files changed:**
- `crates/deadline-api/src/auth.rs` — added `session::invalidate_session_cache_async().await` in login loop
- `crates/deadline-cli/tests/cli/auth.rs` — added `auth_login_dcm_picks_up_credentials_written_mid_login` test

**Test results:** Automated test passes (2.6s with fix, failed at 10s without fix).
All 1,342 tests pass. fmt clean. clippy clean.

**Manual test TODO (2026-05-12):** Re-test `./target/debug/deadline auth login` after
SSO session cache expires (overnight). The first login attempt on 2026-05-11 hit exit
code 1 ("was not able to log into") which may have been the bug manifesting with a
stale binary, OR may have been DCM exiting before a slow browser SSO flow completed.
Need a fresh login (no cached SSO) to confirm the fix works end-to-end with real DCM.

---

---

## #16f — DCC Submitter Dependency Switchover

**Goal:** All DCC submitters (except Houdini) work with zero code changes
when switching from `deadline-cloud-python` to `deadline-cloud-rs`.

### Batch A1 — Import shim ✅ Done

Pure Python re-exports so DCC submitter import paths resolve.

- `gui/deadline/client/api/__init__.py` — re-exports from `_native` + `_compat`
- `gui/deadline/job_attachments/` — `FileConflictResolution`, `ProgressReportMetadata`, `ProgressStatus`
- Committed: `7f1d84a`

### Batch A2 — TelemetryClient + ProgressReportMetadata ✅ Done

Rust changes to make DCC submitters work at **runtime** (not just import time).
Without these, all 6 Pattern A DCCs crash on `telemetry_client.update_common_details(...)`.

**Rust (`crates/deadline-python-bindings/src/telemetry.rs`):**
1. ✅ Added `update_common_details(dict)` method to PyO3 `TelemetryClient`
2. ✅ Added `from_gui: bool = False` kwarg to existing `record_event`
3. ✅ Added `record_error(event_details, exception_type, from_gui=False)` method

**Rust (`crates/deadline-python-bindings/src/submission.rs`):**
4. ✅ Changed progress callback dict keys to camelCase to match Python contract:
   - `transfer_rate` → `transferRate`
   - `progress_message` → `progressMessage`
   - `processed_files` → `processedFiles`

**Python (`gui/deadline/client/_compat.py`):**
5. ✅ Updated `ProgressReportMetadata.from_dict()` to read camelCase keys

**Unblocks:** Blender, Maya, Nuke, Cinema 4D, VRED, 3ds Max (zero submitter changes)

### Batch A3 — API module wrappers for Unreal ✅ Done

Python wrappers in `gui/deadline/client/api/__init__.py` and Rust login
callback support. Unblocks Unreal (zero submitter changes).

Committed items:
- Resource wrappers: `list_farms`, `list_queues`, `list_storage_profiles_for_queue`
- Auth wrappers: `get_credentials_source`, `check_authentication_status`,
  `check_deadline_api_available`, `login` (with callbacks), `logout`
- `get_boto3_client` stub (returns None)
- `create_job_from_job_bundle` flat keyword-arg wrapper with ProgressReportMetadata conversion
- Rust `login()` now accepts `on_pending_authorization` and `on_cancellation_check`
- camelCase property aliases on `ProgressReportMetadata` for Unreal compat

### Batch B — Houdini submitter rewrite (separate repo)

Houdini is pinned to `deadline == 0.49.*` and uses APIs that cannot
exist in Rust (boto3 sessions, `S3AssetManager`, deprecated dialog method).

1. Replace `S3AssetManager` + `JobAttachmentS3Settings` with `create_job_from_job_bundle`
2. Replace `api.get_boto3_client` / `api.get_queue_user_boto3_session` (eliminated by above)
3. Replace `SubmitJobProgressDialog.start_submission(...)` with `start_job_submission(...)`
4. Change private `api._queue_parameters` import to public `api.get_queue_parameter_definitions`

### Batch C — Dependency switch (blocked on #24)

Update `pyproject.toml` in all 9 DCC repos to depend on the new package.
Cannot happen until #24 (production distribution) publishes the package.

---

## Remaining work from #30 (low priority)

- **Reduce `serde_json::Value` usage** — 108 references in CLI code.
  Address incrementally when touching those files.
- **Audit `collect()` then iterate** — 18 sites. Quick fixes when
  touching those files.

## Dependency upgrades needed

- **rusqlite** 0.32 → 0.39 (major, breaking changes likely)
- **pyo3** 0.24 → 0.28 (major, breaking API changes)
- **rustls-webpki** advisories pinned by transitive hyper-rustls 0.24
  (awaiting AWS SDK upstream fix). See `deny.toml`.

---

## Completed items

- **#16f Batch A1** — DCC submitter import shim layer. Committed `7f1d84a`.
- **#30 Rust tooling setup** — rustfmt, release profile (LTO+strip,
  36→29MB), cargo-deny, cargo-outdated, cargo-bloat, cargo-udeps.
- **Strict clippy lint resolution** — 2064 → 0 warnings.
- **CLI feature parity audit (AUDIT-108, AUDIT-109)** — Removed
  Rust-only subcommands and `--json` flag.
- **Codebase health audit** — Report:
  `audit_reports/2026-05-01-codebase-health.md`
