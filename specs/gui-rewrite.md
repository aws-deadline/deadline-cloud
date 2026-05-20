# GUI Rewrite: Rust-Native Qt via qtbridge-rust

## Goal

Eliminate all Python from `deadline-cloud-rs`. The GUI becomes Rust + QML
via [qtbridge-rust](https://github.com/qt/qtbridge-rust). DCC submitter
plugins (Maya, Blender, etc.) continue working unchanged via PyO3 bindings.

## Target Architecture

```
deadline-cloud-rs/
├── crates/
│   ├── deadline-lib/              # Business logic (unchanged)
│   ├── deadline-gui/              # NEW: Rust Qt backend + QML UI
│   ├── deadline-cli/              # CLI binary (GUI commands call deadline-gui directly)
│   └── deadline-python-bindings/  # PyO3 (business logic + GUI class exports)
├── qml/                           # QML UI files (shared tabs, dialogs)
└── (no gui/ directory)            # Python GUI code eliminated
```

### Crate dependency graph

```
deadline-lib          (no GUI, no Qt dependency)
     ↑
deadline-gui          (qtbridge-rust, QML rendering, dialog logic)
     ↑          ↑
deadline-cli    deadline-python-bindings
(binary)        (cdylib for DCC plugins)
```

## How It Works

### CLI path (`deadline bundle gui-submit`)

```
User runs: deadline bundle gui-submit --job-bundle-dir /path/to/bundle

1. CLI parses args, reads config
2. CLI calls deadline_gui::show_submit_dialog(params)
3. Rust creates QApp, loads QML, shows dialog
4. User interacts with Qt Quick UI
5. On submit: calls deadline_lib::create_job_from_job_bundle()
6. Returns job ID to CLI, CLI prints result
```

No subprocess. No Python. Single process.

### DCC plugin path (Maya, Blender, etc.)

```
DCC plugin (Python, running inside Maya's interpreter):

from deadline._native import SubmitJobToDeadlineDialog

dialog = SubmitJobToDeadlineDialog(
    job_setup_widget_type=SceneSettingsWidget,  # DCC-specific Python QWidget
    initial_job_settings=render_settings,
    ...
)
result = dialog.exec()
```

The PyO3 `SubmitJobToDeadlineDialog` class:
1. Instantiates the Python `job_setup_widget_type()` to get a QWidget
2. Extracts the raw QWidget* pointer via `shiboken6.getCppPointer(widget)`
3. Creates the Rust Qt dialog (via deadline-gui)
4. Embeds the DCC widget pointer into the dialog's tab layout
5. Runs the dialog event loop
6. Returns the result

**DCC submitter code does not change.** Same import, same API.

### Key constraint: Qt version compatibility

DCC applications ship their own Qt (Maya 2025 = Qt 6.5, Blender = Qt 6.x via PySide6).
The Rust shared library links against Qt 6 at build time. For in-process DCC usage,
the Qt versions must be ABI-compatible (same major, close minor). This is the primary
risk to validate in the spike.

## Spike (#35): What to Prove

### Success criteria

1. **Build qtbridge-rust** on macOS arm64, Linux x86_64, Windows x64
2. **Minimal dialog** — Create a QML dialog with form fields, show it from Rust
3. **Tokio integration** — Run async submission (deadline-lib) alongside Qt event loop
4. **DCC embedding** — Embed a Python QWidget (via raw pointer) into a Rust-owned QDialog
5. **Qt version** — Confirm ABI compatibility with DCC-shipped Qt 6.x

### Spike deliverables

- `crates/deadline-gui/` with a working config dialog (form fields, apply/cancel)
- `deadline bundle gui-submit` launches the Rust dialog instead of Python subprocess
- One DCC test: Maya plugin creates `SubmitJobToDeadlineDialog` with a custom widget

### Spike non-goals

- Full feature parity with current Python GUI
- All DCC submitters tested
- Production packaging/distribution

## Migration Plan (post-spike)

### Phase 1: Config dialog
- Port `DeadlineConfigDialog` to QML
- Simplest dialog (form fields, dropdowns, apply/cancel)
- Validates the full pipeline

### Phase 2: Submit dialog
- Port `SubmitJobToDeadlineDialog` to QML
- Shared tabs (job settings, attachments, host requirements, timeouts)
- Progress dialog with async submission
- DCC widget embedding

### Phase 3: Eliminate Python
- Remove `gui/` directory entirely
- Remove PySide6/qtpy dependencies
- `deadline-python-bindings` exports GUI classes directly
- Update installer pipeline (#26)

### Phase 4: DCC submitter switchover
- Update DCC repos to import from `deadline._native` instead of `deadline.client.ui`
- Or: keep a namespace shim package that re-exports (zero code, just `__init__.py`)

## What This Eliminates

- `gui/` directory (~400KB of Python)
- PySide6/qtpy/PyYAML Python dependencies
- Python runtime in the installer (`_internal/` directory)
- `maturin develop` step for GUI development
- Python test suite for GUI (`pytest gui/tests/`)
- The Rust→Python subprocess spawn for GUI commands

## What This Keeps

- `deadline-python-bindings` crate (PyO3 shared library for DCC plugins)
- Python as the DCC plugin language (Maya/Blender/etc. are Python hosts)
- Qt as the UI framework (same toolkit, different binding)

## Risks

| Risk | Mitigation |
|------|-----------|
| qtbridge-rust is pre-release (no crates.io) | Pin to git commit; it's official Qt org |
| macOS arm64 "experimental" | Spike validates this first |
| Qt version mismatch with DCCs | Build against Qt 6.5 (Maya's version); test ABI compat |
| QWidget embedding from Python pointer | Proven pattern (shiboken6 + raw pointer); spike validates |
| DCC event loop ownership | DCC owns QApplication; Rust dialog runs modal within it |
| Accessibility (screen readers) | Qt Quick has accessibility support; verify in Phase 2 |

## Impact on Existing Work Items

| Item | Status | Impact |
|------|--------|--------|
| #16c PyO3 bindings | ✅ Done | Stays — bindings expand to include GUI classes |
| #16d Port Python Qt code | ✅ Done | Will be undone — replaced by Rust QML |
| #16d2 GUI CLI commands | ✅ Done | Will be undone — CLI calls Rust GUI directly |
| #16d3 Widget rendering fixes | ✅ Done | Will be undone — QML replaces widgets |
| #16e Python packaging | ✅ Done | Will be undone — no Python package |
| #16f DCC switchover | Blocked | Superseded by Phase 4 above |
| #21c GUI boundary contract | Not started | Eliminated — no boundary |
| #24 Production distribution | Not started | Simplified — no Python runtime to bundle |
| #25 backwards-compat shim | Not started | Reduced — only namespace routing |
| #26 Installer pipeline | Not started | Simplified — Rust binary + Qt libs only |
