# Handoff

Current in-flight work. Read this at the start of every session before
consulting the Work Items table in `specs/progress.md`.

Active work item: None.

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

## Queued small items

- **#21e** — `deadlinew` windowless launcher (~7 lines)
- **#21f** — Windows config path normalization (~50 lines)
- **#21g** — Telemetry parity: success/fail events (~50 lines)

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
