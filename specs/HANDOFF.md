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
- [ ] GAP-3 implemented
- [ ] GAP-2 implemented

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
