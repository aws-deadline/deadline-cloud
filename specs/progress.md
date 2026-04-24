# Progress

AWS Deadline Cloud Rust CLI — work item tracking.

## Getting started

Read `workflow.md` and follow it. The Session Start section contains the
full reading checklist and gates all planning and implementation work.

## Reference Material

Historical migration documents are in `specs/archive/`.

## Progress

### Risk Spikes (must pass before bulk implementation)

All passed. See `migration_strategy.md` for details.

| Spike | Status | Proves |
|-------|--------|--------|
| GUI FFI round-trip (Python ↔ Rust ↔ Qt) | ✅ Passed | Core architecture works: ctypes loading, C ABI calls, callbacks, thread safety |
| GUI FFI inside DCC (Blender) | ✅ Passed | Shared library loads in real DCC Python environment without conflicts |
| S3 transfer performance | ✅ Passed | Rust S3 throughput ≥ Python boto3 transfer manager (see `specs/job-attachments/architecture.md`) |
| Job attachment hashing | ✅ Passed | Parallel xxh128 hashing is faster than Python, hashes match byte-for-byte |

### Work Items

Each row is a self-contained unit of work. See `workflow.md` for how to
pick and execute work items.

| # | Work Item | Status | Test Spec Files | Depends On |
|---|-----------|--------|-----------------|------------|
| 0a | Error types, submitter info | ✅ Done | `common.md` | — |
| 0b | Path utilities | ✅ Done | `common.md` | — |
| 0c | TelemetryClient (common) | ✅ Done | `api_job_lifecycle.md` | — |
| 0d | Config read/write | ✅ Done | `config.md`, `cli.md` | — |
| 0e | Test server infrastructure | ✅ Done | — | — |
| 0f | CLI root & common utilities | ✅ Done | `cli.md` | 0d |
| 0g | Session creation & auth status | ✅ Done | `session.md`, `api_resource_management.md`, `cli.md` | 0d |
| 0h | Queue/job credentials & diagnostics | ✅ Done | `api_resource_management.md`, `api_job_lifecycle.md`, `cli.md` | 0g |
| 0i | GUI FFI spike | ✅ Done | — | 0g |
| 1 | Session caching & user-agent | ✅ Done | `session.md` | 0g |
| 2 | Login/logout | ✅ Done | `api_resource_management.md`, `cli.md` | 1 |
| 3 | Queue user credentials | ✅ Done | `session.md` | 1 |
| 4 | Queue parameters | ✅ Done | `api_resource_management.md`, `cli.md` | 1 |
| 5 | Telemetry API integration | ✅ Done | `api_job_lifecycle.md`, `cli.md` | 1 |
| 6 | Job monitoring & logs | ✅ Done | `api_job_lifecycle.md`, `cli.md` | 1 |
| 7 | Job bundle | ✅ Done | `job_bundle.md`, `cli.md` | 1, 4 |
| 8 | Job attachments: core | ✅ Done | `job_attachments_data_transfer.md`, `job_attachments_orchestration.md` | 1 |
| 9 | Job attachments: transfer | ⚠️ Gaps | `job_attachments_data_transfer.md`, `cli.md` | 3, 8 |
| 10 | Job attachments: orchestration | ✅ Done | `job_attachments_orchestration.md`, `job_attachments_data_transfer.md`, `cli.md` | 9 |
| 11 | Submit job bundle | ⚠️ Gaps | `api_job_lifecycle.md`, `cli.md` | 7, 9 |
| 12 | Job cancel | ✅ Done | `cli.md` | 6 |
| 12b | Job search command | ✅ Done | `cli.md` | 6 |
| 13 | Job download & sync-output | ✅ Done | `job_attachments_orchestration.md`, `job_attachments_data_transfer.md`, `cli.md` | 9 |
| 14 | Handle web URL | ✅ Done | `cli.md` | 13 |
| 15 | Job requeue-tasks | ✅ Done | `cli.md` | 6 |
| 15b | Job get search & estimated time | ✅ Done | `cli.md` | 6 |
| 15c | Job logs auto-selection messages | ✅ Done | `cli.md` | 6 |
| 15d | Level 2 test coverage audit | ✅ Done | — | 11 |
| 15e | Behavioral parity audit | ✅ Done | — | 9 |
| 15f | Wire queue/fleet assume role for all existing CLI commands | ✅ Done | `credential_scoping.md` | 15e-F1 |
| 16 | GUI FFI remaining | In progress | — | 1-14 |
| 16a | FFI: config, resource listing, auth functions | ✅ Done | — | 0i |
| 16b | FFI: submission with callbacks, telemetry | ✅ Done | — | 16a, 11 |
| 16c | Python FFI wrapper (`gui/_ffi.py`) | ✅ Done | — | 16b |
| 16d | Port Python Qt code into `gui/` package | ✅ Done | — | 16c |
| 16e | Python packaging (`gui/pyproject.toml`) | Not started | — | 16d |
| 16f | DCC submitter dependency switchover | Not started | — | 16e |
| 17 | MCP server | ✅ Done | `mcp.md` | 1-14 |
| 18 | Submission hooks | ✅ Done | `submission_hooks.md` | 11 |
| 19 | Update checker | ✅ Done | `new_features.md` | 0g |
| 20 | Batch get API helper | Not started | `new_features.md` | — |
| 21 | Python bug-fix parity sweep | Not started | `new_features.md` | — |
| 22 | Fuzz testing | Not started | — | — |

**Status key:** ✅ Done · ⚠️ Gaps · In progress · Not started · Deferred

**Dependency status:** All core feature dependencies are resolved. Only
2 items remain as ⚠️ Gaps: #9 (parallel/multipart S3 transfer) and #11
(`bundle submit --output json`). #16 (GUI FFI) and #17 (MCP server)
are in progress. New features #19-21 are not started.

**Next action item:** Pick next work item from the table.

**In-progress details:** See `HANDOFF.md` for current state of any
"In progress" work items.

**Deferred items:** #17 (MCP) ships as part of the CLI deliverable
after the core CLI commands are complete.

**Scope changes:**
- **#10**: Originally included asset sync, path mapping, VFS, OS permissions,
  and incremental downloads. Reduced to upload-side orchestration and
  utilities already implemented in #8-9. Download-side orchestration
  (path mapping, incremental downloads, manifest download) moved to #13.
  Worker-agent-only features (asset sync, VFS, OS permissions) deferred.
- **#11**: Dependency on #10 removed. The upload library functions it needs
  were completed in #9. Now depends only on #7 and #9.
- **#13**: Expanded to include download-side orchestration from old #10:
  path mapping from storage profiles, incremental downloads, manifest
  download CLI, plus the original `job download-output` and
  `queue sync-output` CLI commands. All three batches complete:
  Batch A (`job download-output` ✅), Batch B (path mapping ✅),
  Batch C (`queue sync-output` + incremental downloads ✅).
- **#17**: Batch 1 complete (server skeleton, 13 pass-through tools,
  `deadline mcp-server` subcommand, 13 Level 2 tests). The `deadline-mcp`
  crate was removed — MCP server lives in `deadline-cli` at
  `commands/mcp.rs`. Batches 2-3 remain: `submit_job`,
  `download_job_output`, `get_session_and_worker_logs`.

**New work items (discovered 2026-04-20 from Python repo sync):**
- **#18 — Submission hooks**: Pre/post-submission hook framework. Python
  PR #986. Hooks are external scripts run during `bundle submit`:
  pre-submission hooks can modify the CreateJob payload (JSON in/out),
  post-submission hooks run after job creation (failures only warn).
  Two sources: `hooks.yaml` in the job bundle, and `DEADLINE_HOOKS_DIR`
  environment variable. Gated by `settings.allow_bundle_hooks` and
  `settings.allow_environment_hooks` config settings. Includes
  confirmation prompt, timeout handling, and payload validation.
  Touches: `deadline-job-bundle` (new `hooks` module), `submission.rs`
  (integration), `deadline-config/settings.rs` (2 new settings).
  Depends on #11 (submission flow must be complete first).
- **#19 — Update checker**: Check if newer submitter version is available.
  Python PRs #1070, #1087. Fetches manifest from
  `downloads.deadlinecloud.amazonaws.com/submitters/manifest.json`,
  compares installed version, returns `UpdateCheckResult`. GUI dialog
  for DCC submitters. Config setting:
  `settings.submitter_update_notification`. Depends on #0g (session).
- **#20 — Batch get API helper**: Generic helper for `BatchGetTask`,
  `BatchGetStep` APIs. Python PR #1117. Handles chunking (100 items/call),
  partial success, transient error retry with exponential backoff.
  Used by `trace-schedule` and potentially other bulk operations.
  No dependencies.
- **#21 — Python bug-fix parity sweep**: Verify Rust handles fixes from
  recent Python PRs: #1098 (external tools corrupting known paths),
  #1005 (STS/S3 endpoint URL overrides), #1013 (handle-web-url PATH
  lookup on Linux), #1032 (hidden parameters with empty string defaults),
  #1008 (missing newline with no attachments). Quick audit — some may
  already be correct in Rust.

**Audit gaps:** See `audit_reports/2026-04-17-behavioral-parity.md` for
the full audit report with all findings, resolutions, and remaining open
items. 5 findings remain open (4 performance, 1 deferred experimental).

**GUI FFI migration plan (#16a-16f):**

`deadline-cloud-rs` is a complete replacement for `deadline-cloud-python`.
The Python Qt GUI code must ship from this repo, backed by the Rust shared
library. The migration has 6 sub-items:

- **#16a — FFI: config, resource listing, auth** (✅ Done): 10 `extern "C"`
  functions in `deadline-gui-ffi`: config read/get/set, list farms/queues/
  storage profiles/queue parameters, check API available, login, logout.
  25 tests passing.
- **#16b — FFI: submission + telemetry** (Not started): The complex
  `deadline_create_job_from_job_bundle` function with 5 callback types
  (print, hashing progress, upload progress, confirmation, cancellation).
  Plus `deadline_init_telemetry` and `deadline_record_telemetry_event`.
- **#16c — Python FFI wrapper** (Not started): Create `gui/_ffi.py` — a
  thin ctypes wrapper (~200 lines) that loads the `.dylib`/`.so`/`.dll`,
  declares function signatures, handles JSON serialization/deserialization,
  and exposes Pythonic methods. All ctypes boilerplate lives here.
- **#16d — Port Python Qt code** (Not started): Copy the `deadline.client.ui`
  module from `deadline-cloud-python` into `gui/deadline/client/ui/`. Pure
  presentation files (~15) copy unchanged. Business logic callers (~6 files)
  get rewired to use `_ffi.py` instead of `import deadline.client.api`.
  Also port pure Python data classes and utilities (~10 files) that DCC
  submitters import: `AssetReferences`, `JobParameter`, `SubmitterInfo`,
  `deadline_yaml_dump`, `DeadlineOperationError`, `path_utils`, etc.
- **#16e — Python packaging** (Not started): Create `gui/pyproject.toml`
  so the Python Qt code is installable as a package. The package must
  bundle or locate the Rust shared library. Installer/conda recipe
  includes both the Rust CLI binary and the Python+Rust GUI package.
- **#16f — DCC submitter switchover** (Not started): Update each DCC
  submitter repo (Blender, Maya, Nuke, Houdini, Cinema 4D, VRED, 3ds Max,
  Unreal Engine) to depend on the new Python package from `deadline-cloud-rs`
  instead of `deadline-cloud-python`. DCC submitter code itself barely
  changes — same import paths, same `SubmitJobToDeadlineDialog` API.

Python files by category (from `deadline-cloud-python/src/deadline/client/ui/`):

| Category | Files | Changes needed |
|----------|-------|----------------|
| Pure presentation (widgets) | ~15 | None — copy as-is |
| Qt threading infrastructure | 3 (`_async_runner.py`, `_async_task.py`, `_thread_pool.py`) | None |
| Business logic callers | 6 (`_deadline_controller.py`, `deadline_authentication_status.py`, `_job_submission_worker.py`, `deadline_config_dialog.py`, `cli_job_submitter.py`, `job_bundle_submitter.py`) | Rewire to FFI |
| Data classes / utilities | ~10 (`AssetReferences`, `JobParameter`, `SubmitterInfo`, `_yaml.py`, `exceptions.py`, `path_utils.py`, etc.) | None — copy as-is |
| New | 1 (`_ffi.py`) | Write from scratch |

**Missing config settings (discovered 2026-04-20):**
- `settings.allow_bundle_hooks` (default `false`) — needed for #18
- `settings.allow_environment_hooks` (default `false`) — needed for #18
- `settings.submitter_update_notification` (default `true`) — needed for #19

**Technical debt:**
- **#15d**: Audit all Level 1 tests in library crates to identify which
  can be converted to or supplemented with Level 2 CLI subprocess tests.
  Per TESTING.md rule 1: "If the CLI can exercise it, test it through
  the CLI."
- **Realistic test IDs**: Replace hardcoded pseudo-IDs in tests (e.g.
  `"farm-1"`, `"queue-1"`) with pseudorandomly generated IDs that follow
  the actual Deadline Cloud ID format. Add a test helper that generates
  realistic IDs per resource type. Applies across all crates.
- **Test consolidation**: Audit test suite for redundant or overlapping
  tests. Identify Level 1 tests fully subsumed by Level 2 snapshots
  (per testing.md removal rule). Consolidate tests that exercise the
  same code path with minor variations into parameterized test cases.
- **GUI Python code smell audit**: Review ported `gui/` Python code for
  patterns that no longer make sense now that Rust handles business logic.
  Known examples: `config_file.py` re-implements profile-scoped config
  resolution in Python (should use temp-file + FFI path instead),
  `_get_ffi()` singleton duplicated across 7 files (centralize), widgets
  passing `ConfigParser` objects for preview (replace with temp config
  file written to disk and passed as `config_path` to FFI). Goal: keep
  Python side as thin presentation-only, push all logic to Rust.
