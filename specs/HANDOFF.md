# Handoff

Current in-flight work. Read this at the start of every session before
consulting the Work Items table in `specs/progress.md`.

Active work item: **#35 — Rust-native GUI (cxx-qt)**

**Baseline:** 1,355 tests passing (2026-05-20)

---

## Status: Spike PASSED ✅ — Ready for Implementation

### What's proven
- cxx-qt v0.8.1 builds on macOS arm64 with Qt 6.11.1 (homebrew)
- QObject model with properties, invokables, Threading compiles
- QML dialog renders on screen (tested visually)
- deadline-lib integration works (config read)
- Build command: `QMAKE=/opt/homebrew/opt/qt/bin/qmake cargo build -p deadline-gui`

### What's in place
- `crates/deadline-gui/` — skeleton crate (Cargo.toml, build.rs, lib.rs, config_model.rs, ConfigDialog.qml)
- `specs/deadline-gui/architecture.md` — crate structure, public API, async pattern
- `specs/deadline-gui/ui-contract.md` — complete behavioral contract (all dialogs, buttons, flows)
- `specs/gui-rewrite.md` — overall migration plan, dependency graph, phases
- `../cxx-qt/` — reference repo with book docs and examples

### Implementation Batches

| Batch | Scope | Deliverable |
|-------|-------|-------------|
| **1** | Config Dialog | Full config dialog matching ui-contract §1. Wire `deadline config gui`. |
| **2** | Submit Dialog | Full submit + progress dialog matching §2-§6. Wire `deadline bundle gui-submit`. |
| **3** | DCC Entry Point | PyO3 `show_submit_dialog()`. Test from Python with existing QApp. |
| **4** | Cleanup | Delete `gui/`, simplify `pyproject.toml` + `Makefile`, update `specs/architecture.md`. |

### Next Session Action Items

1. **Start Batch 1** — Implement full ConfigModel (all settings from ui-contract §1)
2. Read `../cxx-qt/book/src/` for reference on:
   - `concepts/types.md` — available Qt types in Rust
   - `bridge/extern_rustqt.md` — properties, signals, invokables syntax
   - `bridge/traits.md` — Threading, Initialize, Constructor
   - `examples/demo_threading/` — async pattern with qt_thread
   - `examples/qml_features/` — QML integration patterns
3. Expand `ConfigDialog.qml` to match ui-contract §1 (all sections, cascading)
4. Expand `config_model.rs` with all settings, async farm/queue loading
5. Add `auth_model.rs` and `resource_model.rs`
6. Wire `deadline-cli/src/commands/config.rs` to call `deadline_gui::show_config_dialog()`

### Key Technical Decisions Made

- **Library:** cxx-qt v0.8.1 (KDAB), MIT/Apache-2.0
- **Qt version:** System Qt 6 via `QMAKE` env var (no bundled Qt)
- **UI language:** QML/Qt Quick (declarative, not Qt Widgets)
- **Async:** std::thread + tokio runtime + `CxxQtThread::queue()` for Qt thread safety
- **No Python in GUI:** Pure Rust + QML. Python only for DCC plugin shim.
- **Makefile/pyproject.toml:** Keep until Batch 4. Still needed for `_native.abi3.so` (DCC plugins).

### Build Prerequisites

```bash
# macOS
brew install qt
export QMAKE=/opt/homebrew/opt/qt/bin/qmake

# Then build
cargo build -p deadline-gui
```

### Data Flow: Config Dialog

```
User opens dialog
  → Reads INI config, populates all form fields
  → DeadlineAuthenticationStatus watches ~/.aws and ~/.deadline
  → Farm/queue/storage profile combo boxes refresh via controller (async)
  → User changes settings → tracked in `changes` dict
  → Apply: writes all changes to INI config file
  → Cascading: profile change → clear farm/queue → refresh lists
```

### Rust Functions Already Exposed via PyO3 (`deadline._native`)

- `login()`, `logout()`
- `get_credentials_source()`, `check_auth_status()`, `check_api_available()`
- `list_farms()`, `get_farm()`, `list_queues()`, `get_queue()`
- `list_storage_profiles_for_queue()`, `get_queue_parameter_definitions()`
- `create_job_from_job_bundle()` (with callbacks: print, hashing, upload, confirm, continue)
- `get_setting()`, `set_setting()`, `read_config()`
- `TelemetryClient` class

### Technology Assessment: qtbridge-rust (v0.1.5)

**Source:** https://github.com/qt/qtbridge-rust (cloned locally at
`../qtbridge-rust`)

**Platform support:**
- Linux x86_64 ✅
- Windows x64 ✅
- **macOS arm64 ✅ (experimental)**

**Key capabilities confirmed from source:**
- `QApp::new().register::<T>().load_qml(bytes).run()` — app lifecycle
- `#[qobject_impl]` / `#[qobject]` — expose Rust structs to QML
- `qproperty!()` — bind struct fields to QML properties with notify signals
- `#[qslot]` — QML-invokable methods
- `#[qsignal]` — Rust→QML notifications
- `QListModel` / `QTableModel` — data models for ListView/TableView
- `Singleton` option — global instances accessible by type name in QML
- **`QmlMethodInvoker`** — `Send` handle for invoking slots from any thread
  (key for tokio integration)
- `invoke_method!()` macro — schedule slot calls on Qt main thread from
  background threads

**Tokio integration pattern (from `apps/host_monitor`):**
```rust
// Store a tokio runtime in the struct
runtime: tokio::runtime::Runtime,

#[qslot]
fn make_request(&self) {
    let invoker = self.get_qml_method_invoker(); // Send handle
    self.runtime.spawn(async move {
        let result = do_async_work().await;
        invoke_method!(invoker, "updateStatus", result);
    });
}

#[qslot]
fn update_status(&mut self, result: bool) {
    self.status = result;
    self.status_changed(); // notify QML
}
```

**Supported types in signals/slots/properties:**
- Scalars: i8, u8, i16, u16, i32, u32, i64, u64, isize, usize, f32, f64
- Strings: String, &str
- Collections: Vec<T> where T is scalar or String

**Limitations identified:**
- No `HashMap` support in properties/signals (need Vec of pairs or custom model)
- Objects held in `Rc<RefCell<_>>` — panics if QML borrows while already borrowed
- No direct QWidget embedding API (Qt Quick only, not Qt Widgets)
- Requires `qmake` on PATH and Qt 6 installation
- License: LGPL-3.0-only OR Qt-Commercial (compatible with our Apache-2.0
  via dynamic linking, same as PySide6)

**Critical gap for DCC embedding:**
The README explicitly states: "If your project requires mixing Rust and C++
code, using Qt Widgets, or accessing Qt modules that only provide a C++ API,
consider using CXX-Qt instead."

This means **qtbridge does NOT support Qt Widgets** — it's Qt Quick/QML only.
DCC widget embedding (criterion #4 from gui-rewrite.md) requires embedding a
Python QWidget into a Qt Quick window. This is possible via Qt's
`QWidget::createWindowContainer()` or `QQuickWidget`, but would need custom
C++ glue code beyond what qtbridge provides natively.

### Implementation Plan

**Phase 1 (Spike): Config Dialog**

The config dialog is simpler (form fields, dropdowns, checkboxes) and doesn't
need DCC widget embedding. Perfect for proving the architecture.

```
crates/deadline-gui/
├── Cargo.toml
├── src/
│   ├── lib.rs              # pub fn show_config_dialog(), show_submit_dialog()
│   ├── app.rs              # QApp creation helper
│   ├── config_model.rs     # ConfigDialog backend (Singleton)
│   ├── auth_model.rs       # AuthStatus backend (Singleton)
│   ├── resource_model.rs   # Farm/Queue/StorageProfile list models
│   └── submit_model.rs     # SubmitDialog backend (Phase 2)
└── qml/
    ├── ConfigDialog.qml    # Main config window
    ├── SubmitDialog.qml    # Phase 2
    └── components/
        ├── AuthStatusBar.qml
        ├── FarmSelector.qml
        └── ...
```

**Mapping Python → Rust+QML:**

| Python component | Rust model | QML component |
|-----------------|------------|---------------|
| `DeadlineAuthenticationStatus` | `AuthModel` (Singleton) | `AuthStatusBar.qml` |
| `DeadlineUIController` | `ResourceModel` (Singleton) | Drives combo boxes |
| `DeadlineWorkstationConfigWidget` | `ConfigModel` (Singleton) | `ConfigDialog.qml` |
| `AsyncTaskRunner` | tokio runtime + `QmlMethodInvoker` | N/A (invisible) |
| `DeadlineConfigDialog` | `show_config_dialog()` entry point | `ConfigDialog.qml` |
| `SubmitJobToDeadlineDialog` | `SubmitModel` | `SubmitDialog.qml` |
| `JobSubmissionWorker` | tokio task + `QmlMethodInvoker` | `ProgressDialog.qml` |
| `SharedJobSettingsWidget` | Properties on `SubmitModel` | Tab in `SubmitDialog.qml` |
| `OpenJDParametersWidget` | `QListModel` of parameters | `ParameterForm.qml` |
| `HostRequirementsWidget` | Nested model | `HostRequirements.qml` |
| `JobAttachmentsWidget` | `QListModel` of paths | `Attachments.qml` |
| Translation (`tr()`) | Qt's built-in `qsTr()` | QML native i18n |

**Async pattern for all API calls:**
```rust
#[qslot]
fn refresh_farms(&self) {
    self.set_farms_loading(true);
    let invoker = self.get_qml_method_invoker();
    let profile = self.profile.clone();
    self.runtime.spawn(async move {
        match deadline_lib::api::resources::list_farms(profile.as_deref()).await {
            Ok(farms) => invoke_method!(invoker, "onFarmsLoaded", farms_to_json(farms)),
            Err(e) => invoke_method!(invoker, "onFarmsError", e.to_string()),
        }
    });
}
```

### Spike Success Criteria Assessment

| Criterion | Feasibility | Notes |
|-----------|-------------|-------|
| 1. Build on macOS arm64, Linux, Windows | ✅ High | macOS experimental but listed as supported |
| 2. Minimal dialog with form fields | ✅ High | QML native capability, well-demonstrated |
| 3. Tokio integration | ✅ High | `host_monitor` example proves the pattern |
| 4. DCC widget embedding | ⚠️ Medium | Needs custom C++ glue (QWidget in Qt Quick) |
| 5. Qt version ABI compat | ✅ High | Links against system Qt 6, same as DCC |

### Questions Requiring Human Decision

1. **Qt 6 installation:** Need to install Qt 6 on this machine to build the
   spike. `brew install qt@6` or download from qt.io?
2. **DCC embedding approach:** qtbridge is Qt Quick only. For embedding Python
   QWidgets, we'd need a small C++ bridge file. Accept this complexity, or
   defer DCC embedding to Phase 2?
3. **Spike scope:** Start with config dialog (simpler, no DCC embedding needed)
   and prove criteria 1-3, then tackle submit dialog + DCC embedding?

---

## Spike Build Attempt (2026-05-20)

### qtbridge-rust: FAILED on macOS arm64

Tried Qt 6.8.3, 6.10.2, 6.11.1. All failed:
- Qt 6.11: `QList` not trivially relocatable (CXX static_assert)
- Qt 6.10/6.8 (pre-built): `__yield` implicit declaration (SDK mismatch)
- Qt 6.8: `QMetaMethod::nameView()` missing (requires Qt 6.9+)

Root cause: qtbridge's CXX bridge declares `QList` as relocatable but on
macOS arm64 it isn't. Pre-built Qt binaries have SDK mismatches with our
Clang. **qtbridge is not viable for macOS development today.**

### cxx-qt (KDAB): SUCCESS on macOS arm64 ✅

```
$ export PATH="/opt/homebrew/opt/qt/bin:$PATH"
$ export QMAKE=/opt/homebrew/opt/qt/bin/qmake
$ cargo build -p qml-minimal-no-cmake
    Finished `dev` profile in 2m 26s

$ file target/debug/qml-minimal-no-cmake
Mach-O 64-bit executable arm64
```

- Qt 6.11.1 (homebrew) — works with system Qt
- Key: set `QMAKE` env var to system Qt, don't use `qt_minimal` feature
- Binary: 7MB debug, native arm64
- Repo cloned at `../cxx-qt/`

### Technology Decision: **cxx-qt**

cxx-qt is the chosen library for the GUI rewrite. Reasons:
- Builds on macOS arm64 ✅ (proven)
- Supports Linux, Windows, macOS, WebAssembly
- MIT/Apache-2.0 license (perfect for us)
- v0.8.1, actively maintained by KDAB
- Uses QML/Qt Quick (same declarative approach)
- Has threading support (`impl cxx_qt::Threading`)
- Works with Cargo (no CMake required)
- Supports Qt 5.15 and all Qt 6 versions

### Next Steps (Session 2)

1. Implement full ConfigDialog (all settings, cascading farm/queue refresh)
2. Implement SubmitDialog + ProgressDialog
3. Wire `deadline config gui` and `deadline bundle gui-submit` CLI commands
4. Add PyO3 entry point for DCC plugins
5. Delete `gui/` Python directory

---

**Status: Spike PASSED ✅ — cxx-qt builds, renders GUI, integrates with deadline-lib. Ready for full implementation.**

---

## Completed items (older)

- **#34 — Library/CLI boundary refactor (2026-05-20)** — Removed `&IniConfig`
  from ALL library operation signatures: submission, S3, log retrieval, auth
  (`login`/`logout`/`get_credentials_source`/`check_authentication_status`),
  and update checker. Library is now fully usable without config files.
  1,355 Rust tests, 373 Python tests pass.
- **#34 Batches A+B — Session + Telemetry refactor (2026-05-20, bd763ef)** —
  Session functions take `profile: Option<&str>`, telemetry functions take
  `opt_out: bool, identifier: Option<&str>` instead of `&IniConfig`.
  Fixed 35 pre-existing Python test failures (stale `_get_ffi` mocks).
  1,355 Rust tests pass, 371/373 Python tests pass.

- **[L] Deferred refactors (2026-05-18)** — Unified `OutputDownloader`/
  `InputDownloader` into `ManifestDownloader`, separated validation from
  grouping in `prepare_paths_for_upload`, consolidated duplicate
  `format_sdk_error` functions, added 5 Level 1 tests for
  `create_job_from_job_bundle`. 1,350→1,355 tests.

- **Per-file upload progress (2026-05-16)** — Wired openjd-snapshots
  `on_progress` callback for per-file progress during upload. Progress
  bar now fills incrementally instead of jumping per-group. Added
  `signal_file_done` to ProgressTracker, `ProgressFn` → `Send + Sync`.
  +2 tests (1,348→1,350). Bumped flaky perf test budget.
- **Audit findings — Batches G+I+J (2026-05-16)** — Test coverage gaps
  (+17 tests), session mutex minimization (lock no longer held across
  network I/O), submission function extraction (`load_and_confirm_hooks`),
  HookManager constructor cleanup. 1,324→1,341 tests.
- **Audit findings — Batch F quick wins (2026-05-16)** — 13 findings
  addressed: HashSet perf fix in diff (O(N²)→O(N)), platform-aware
  normcase in incremental_download, clear error for missing glob config
  file, 10 doc comments. +2 tests (1,322→1,324). Added #33 (error type
  parity audit) to progress.md.
- **Audit findings — Idiomatic Rust cleanup (2026-05-15)** — Batches A-C+E:
  shared `util.rs` (op_err + normalize_path), thread leak fix in hooks,
  FilterSet regex caching (1000x speedup), HookFailed error variant.
  +25 tests (1,297→1,322). Batch D (session mutex) deferred.
- **#31 — Crate Restructure (2026-05-14)** — All 12 steps done.
  See `audit_reports/archive/2026-05-14-cli-behavioral-audit.md`.
- **Python parity audit (2026-05-11)** — GAP-1 (login session refresh),
  GAP-3 (--include/--match-paths-by on download-output), GAP-2
  (download-input command). All implemented and verified.

---

## Pending Manual Verification

**GAP-1 login retest (2026-05-12):** Re-test `./target/debug/deadline auth login`
with a fresh SSO session (no cached credentials). On 2026-05-11 the first attempt
hit "was not able to log into" which may have been the bug manifesting with a stale
binary. On 2026-05-12 login succeeded, but need one more clean test to confirm.

---

## #16f — DCC Submitter Dependency Switchover

**Status:** Batches A1-A3 ✅ Done. Batch B deferred (Houdini, separate repo).
Batch C blocked on #24 (production distribution).
