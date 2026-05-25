# Handoff

Current in-flight work. Read this at the start of every session before
consulting the Work Items table in `specs/progress.md`.

Active work item: **#35 Phase 2+3 — Submit Dialog (Rust QML) + PyO3 show_submit_dialog()**

---

## #35 Phase 2+3 — Submit Dialog + DCC Integration

**Baseline:** 1,421 tests passing (all green)

### What Phase 2 Delivers

A complete Rust QML submit dialog that replaces the Python
`SubmitJobToDeadlineDialog`. When the user runs `deadline bundle gui-submit`,
the CLI calls `deadline_gui::show_submit_dialog(params)` directly — no
Python subprocess, no PySide6.

### What Phase 3 Delivers

A PyO3 function `show_submit_dialog(params_dict)` in
`deadline-python-bindings` that DCC plugins call to open the same Rust
QML dialog inside their existing QApplication.

### Current State (Phase 1 complete)

Already implemented in `crates/deadline-gui/`:
- `ConfigModel` + `ConfigDialog.qml` — full config dialog
- `AuthModel` — credential source, auth status, login/logout, file watcher
- `ResourceModel` — farm/queue/storage profile cascading lists
- `logic.rs` — pure business logic (26 L1 tests)
- `logic/auth.rs`, `logic/resources.rs`, `logic/watcher.rs`
- `build.rs` — cxx-qt-build with QML module registration
- CLI `config gui` wired to call Rust GUI directly

### Python Submit Dialog Components (to port)

The Python submit dialog (`SubmitJobToDeadlineDialog`) has these tabs/areas:

1. **Shared Job Settings tab** — Job properties (name, description, priority,
   initial state, max failed tasks, max retries, max worker count) +
   Deadline Cloud settings (farm/queue display) + Queue parameters (OpenJD
   dynamic form from queue environments)

2. **Job-specific settings tab** — For `JobBundle` submitter: shows
   `input_job_bundle_dir` and browse button. For `CLI` submitter: shows
   bash script editor and array parameter config. DCC submitters provide
   their own widget type.

3. **Job attachments tab** — Input files list, input directories list,
   output directories list. Add/remove buttons. "Require paths exist"
   checkbox.

4. **Host requirements tab** (optional) — OS requirements, hardware
   requirements (CPU, memory, GPU), custom requirements (amounts/attributes).

5. **Auth status bar** — Same as config dialog (login/logout/profile switch)

6. **Button bar** — Submit, Export Bundle, Settings, Help, (Load Bundle if browse mode)

7. **Progress dialog** — Modal dialog with hashing progress bar, upload
   progress bar, log text area, cancel button. Shows after Submit is clicked.

### Buttons/Actions in Submit View

| Button | Action |
|--------|--------|
| Submit | Validates → creates job history bundle → calls `create_job_from_job_bundle` in background thread → shows progress dialog |
| Export Bundle | Same as Submit but purpose=EXPORT, saves bundle to disk, opens folder, no API call |
| Settings | Opens config dialog (same one from Phase 1) |
| Help | Shows submitter info dialog (name, version, package info) |
| Load Bundle | (only if `browse=true`) File dialog to pick a new bundle dir, refreshes all tabs |
| Login | SSO login flow |
| Logout | Clears credentials |
| Switch Profile | Opens config dialog focused on profile selector |

### Implementation Plan

#### Rust Models Needed

| Model | Properties | Invokables |
|-------|-----------|------------|
| `SubmitModel` | name, description, priority, initial_status, max_failed_tasks, max_retries, max_worker_count, use_max_worker_count, farm_display, queue_display, job_bundle_dir, submitter_name, can_submit, status_message | submit(), export_bundle(), load_bundle(path), set_job_bundle_dir(path) |
| `ParameterListModel` | parameters (semicolon-encoded), loading_state, error_message | refresh_queue_parameters(), set_parameter_value(name, value) |
| `AttachmentModel` | input_files, input_dirs, output_dirs, require_paths_exist | add_input_file(path), remove_input_file(idx), add_input_dir(path), remove_input_dir(idx), add_output_dir(path), remove_output_dir(idx) |
| `HostRequirementsModel` | os_family, cpu_arch, min_cpu, max_cpu, min_memory, max_memory, min_gpu, max_gpu, min_gpu_memory, max_gpu_memory, custom_amounts, custom_attributes | add_custom_amount(), remove_custom_amount(idx), add_custom_attribute(), remove_custom_attribute(idx) |
| `ProgressModel` | status_text, hashing_progress, hashing_message, upload_progress, upload_message, log_text, is_complete, is_canceled | cancel() |

#### QML Files Needed

| File | Purpose |
|------|---------|
| `SubmitDialog.qml` | Main window with TabView (4 tabs) + auth bar + button bar |
| `ProgressDialog.qml` | Modal progress window |
| `components/JobPropertiesForm.qml` | Name, desc, priority, status, max fields |
| `components/QueueParameterForm.qml` | Dynamic parameter inputs |
| `components/AttachmentList.qml` | File/dir lists with add/remove |
| `components/HostRequirementsForm.qml` | OS, hardware, custom requirements |
| `components/AuthStatusBar.qml` | Reuse from config dialog |

#### Logic Modules Needed

| Module | Purpose |
|--------|---------|
| `logic/submit.rs` | Bundle preparation, parameter merging, on_create_job_bundle_callback equivalent |
| `logic/parameters.rs` | Queue parameter fetching, OpenJD parameter validation |
| `logic/attachments.rs` | AssetReferences manipulation (add/remove/merge) |
| `logic/host_requirements.rs` | Serialize host requirements to JSON for template injection |

#### Phase 3 Addition

| File | Change |
|------|--------|
| `deadline-python-bindings/src/gui.rs` | New module: `show_submit_dialog(params_dict)` |
| `deadline-python-bindings/src/lib.rs` | Register `gui::show_submit_dialog` |
| `deadline-gui/src/lib.rs` | Add `pub fn show_submit_dialog(params: SubmitDialogParams)` |

### Batching Strategy

**Batch 2a — SubmitModel + SubmitDialog skeleton + Progress**
- `SubmitModel` (job properties, submit/export actions)
- `ProgressModel` (progress bars, log, cancel)
- `SubmitDialog.qml` (tabs, button bar, basic layout)
- `ProgressDialog.qml`
- `logic/submit.rs` (bundle prep, submission orchestration)
- Wire `deadline bundle gui-submit` to call Rust GUI directly
- Tests: L1 for logic/submit.rs, L2 for CLI gui-submit

**Batch 2b — Queue Parameters + Attachments**
- `ParameterListModel` (fetch queue params, dynamic form state)
- `AttachmentModel` (file/dir lists, add/remove)
- `logic/parameters.rs` (queue parameter fetching + validation)
- `logic/attachments.rs` (AssetReferences manipulation)
- `QueueParameterForm.qml` + `AttachmentList.qml`
- Tests: L1 for parameter/attachment logic

**Batch 2c — Host Requirements + Polish**
- `HostRequirementsModel`
- `HostRequirementsForm.qml`
- `logic/host_requirements.rs`
- Help dialog, Load Bundle action, parameter validation warnings
- Tests: L1 for host requirements serialization

**Batch 3 — PyO3 Integration**
- `deadline-python-bindings/src/gui.rs` — `show_submit_dialog()`
- `deadline-gui/src/lib.rs` — public `show_submit_dialog(params)` entry point
- Test: Python test calling `show_submit_dialog` with auto_close

### Key Design Decisions

1. **Reuse AuthModel and ResourceModel** from Phase 1 — they already handle
   farm/queue display, login/logout, and profile switching.

2. **Queue parameters as semicolon-encoded strings** — same pattern as
   ResourceModel. QML splits on `;` for display. Avoids needing QAbstractListModel
   (complex with cxx-qt).

3. **Submission runs in std::thread** (not tokio task) — same pattern as
   auth/resource refresh. The thread creates its own tokio Runtime, calls
   `create_job_from_job_bundle`, and queues progress updates back to Qt
   via `qt_thread.queue()`.

4. **on_create_job_bundle_callback equivalent** — In Python, DCC submitters
   provide a callback that writes the job bundle. In Rust, the `SubmitModel`
   handles the standard JobBundle case directly (copy template, apply params,
   write asset_references). DCC submitters (Phase 3) pass pre-built bundle
   dirs via PyO3.

5. **No CliJobSubmitter** — The Python `CliJobSubmitter` (bash script editor)
   is a dev tool, not shipped to customers. Skip it for Phase 2.

### Cross-Reference: Python → Rust

| Python Component | Rust Equivalent |
|-----------------|-----------------|
| `SubmitJobToDeadlineDialog` | `SubmitModel` + `SubmitDialog.qml` |
| `SharedJobSettingsWidget` | `SubmitModel` properties (name, desc, priority, etc.) |
| `SharedJobPropertiesWidget` | `components/JobPropertiesForm.qml` |
| `DeadlineCloudSettingsWidget` | Reuse `ResourceModel` (farm/queue display) |
| `OpenJDParametersWidget` | `ParameterListModel` + `QueueParameterForm.qml` |
| `JobBundleSettingsWidget` | Part of `SubmitModel` (job_bundle_dir property) |
| `JobAttachmentsWidget` | `AttachmentModel` + `AttachmentList.qml` |
| `HostRequirementsWidget` | `HostRequirementsModel` + `HostRequirementsForm.qml` |
| `DeadlineAuthenticationStatusWidget` | Reuse `AuthModel` + `AuthStatusBar.qml` |
| `SubmitJobProgressDialog` | `ProgressModel` + `ProgressDialog.qml` |
| `JobSubmissionWorker` (QThread) | `std::thread` in `SubmitModel::submit()` |
| `_gui_entry.py::run_gui_submit` | `deadline_gui::show_submit_dialog()` |
| `job_bundle_submitter.py::on_create_job_bundle_callback` | `logic/submit.rs::prepare_job_bundle()` |

### Status: Batch 2a complete. Next: Batch 2b (Queue Parameters + Attachments UI).

---

## Completed items

- **#35 Batch 2a — Job History + Output JSON + Progress Polish (2026-05-25)** —
  Export bundle button, job history bundle creation before submit, `--output json`
  with correct format (SUBMITTED/CANCELED/jobId/jobHistoryBundleDirectory),
  progress dialog dark mode + status labels + cancel handling, tilde expansion
  in `get_setting` for path settings, Cancel button on main dialog.
  1,477→1,483 tests. 25/35 xa11y tests passing (16/16 submit tests).

- **#35 Batch 2a — Submit Action + Progress Dialog (2026-05-24)** — Wired
  Submit button to `create_job_from_job_bundle` via background thread,
  progress dialog with hashing/upload bars, cancel support, result return.
  TabBar accessibility fix, xa11y test infrastructure fixes (localhost URL,
  dialog/window role selectors). 1,421→1,477 tests. 22/35 xa11y tests passing.

- **#35 Batch 1b+1c — ResourceModel + AuthModel (2026-05-22)** — Async
  farm/queue/storage profile loading with cascading refresh and pre-selection
  from config. Auth status bar with login/logout, file watcher on ~/.aws/
  and ~/.deadline/. QML uses snake_case names (cxx-qt v0.8 convention).
  1,380→1,421 tests. Remaining TODOs: Known Asset Paths UI, spinboxes
  for max_retries/max_failed, file watcher shutdown.

- **#35 Batch 1a+1d — Config Dialog + CLI wiring (2026-05-21)** — Full config
  dialog with all settings, logic module with 26 L1 tests, CLI calls Rust GUI
  directly. 1,355→1,380 tests. L2 xa11y test infrastructure ported from
  deadline-cloud-python (35 tests, acceptance criteria for visual correctness).

- **#34 — Library/CLI boundary refactor (2026-05-20)** — Removed `&IniConfig`
  from ALL library operation signatures: submission, S3, log retrieval, auth
  (`login`/`logout`/`get_credentials_source`/`check_authentication_status`),
  and update checker. Library is now fully usable without config files.
  1,355 Rust tests, 373 Python tests pass.

- **[L] Deferred refactors (2026-05-18)** — Unified `OutputDownloader`/
  `InputDownloader` into `ManifestDownloader`, separated validation from
  grouping in `prepare_paths_for_upload`, consolidated duplicate
  `format_sdk_error` functions, added 5 Level 1 tests for
  `create_job_from_job_bundle`. 1,350→1,355 tests.

- **Per-file upload progress (2026-05-16)** — Wired openjd-snapshots
  `on_progress` callback for per-file progress during upload.
  +2 tests (1,348→1,350).

- **Audit findings — Batches A-J (2026-05-15–2026-05-16)** — 45 findings,
  33 resolved, 5 accepted, 7 deferred (all low-priority). +58 tests
  (1,297→1,355).

- **#31 — Crate Restructure (2026-05-14)** — All 12 steps done.

- **Python parity audit (2026-05-11)** — GAP-1 (login session refresh),
  GAP-3 (--include/--match-paths-by on download-output), GAP-2
  (download-input command). All implemented and verified.

---

## Pending Manual Verification

**GAP-1 login retest (2026-05-12):** Re-test `./target/debug/deadline auth login`
with a fresh SSO session (no cached credentials).

---

## #16f — DCC Submitter Dependency Switchover

**Status:** Batches A1-A3 ✅ Done. Batch B deferred (Houdini, separate repo).
Batch C blocked on #24 (production distribution).
