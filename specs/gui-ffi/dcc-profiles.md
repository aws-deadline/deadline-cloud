# DCC Submitter Integration Profiles

How each DCC submitter integrates with Deadline Cloud. This document
informs the GUI FFI migration (#16a-16f) — what each DCC depends on
determines what must ship in the `gui/` Python package.

## Integration Patterns

There are three distinct patterns across the 9 DCC submitters:

### Pattern A: Shared Qt Dialog (7 DCCs)

Blender, Maya, Nuke, Cinema 4D, VRED, 3ds Max, Houdini (partially).

These DCCs import `deadline.client.ui` and show the shared
`SubmitJobToDeadlineDialog` inside the DCC's own Qt event loop. Each
provides a custom `SceneSettingsWidget` for DCC-specific settings.

**Depends on:** `deadline.client.ui`, `deadline.client.api`,
`deadline.client.job_bundle`, `deadline.client.config`,
`deadline.client.exceptions`, `deadline.job_attachments` (data classes).

### Pattern B: Native UI + Python Library (1 DCC)

Unreal Engine.

Uses Unreal's native Slate/UMG C++ widgets — no Qt at all. Calls
`deadline.client.api.create_job_from_job_bundle()` from Python, but
runs it in a subprocess of Unreal's own Python interpreter.

**Depends on:** `deadline.client.api`, `deadline.client.job_bundle`,
`deadline.client.exceptions`. Does NOT depend on `deadline.client.ui`.

### Pattern C: CLI Binary (1 DCC)

After Effects.

Pure ExtendScript (JSX) — no Python, no Qt. Shells out to the
`deadline` CLI binary: `deadline bundle gui-submit <bundle> --output json`.

**Depends on:** The `deadline` CLI binary only. Does NOT import any
Python modules.

---

## Per-DCC Profiles

### Blender

| Aspect | Detail |
|--------|--------|
| Language | Python (Blender addon) |
| Launch | Render menu → "Submit to AWS Deadline Cloud" / Shift+F |
| Entry point | `addons/deadline_cloud_blender_submitter/__init__.py` |
| Qt source | Creates own `QApplication` if none exists (via `qtpy`) |
| Dialog | `SubmitJobToDeadlineDialog` — **modal** (`exec_()`) |
| Custom widgets | `SceneSettingsWidget`, `FileSearchLineEdit` |
| Uses CLI binary | No |
| Uses Python lib | Yes — `api`, `job_bundle`, `ui.dialogs`, `exceptions` |
| Subprocess calls | `pip install deadline[gui]` (one-time setup) |
| Special notes | Only DCC that creates its own QApplication. Blender's Python doesn't ship Qt, so the addon installs PySide via pip. |

### Maya

| Aspect | Detail |
|--------|--------|
| Language | Python (Maya plugin + MEL command) |
| Launch | AWSDeadline shelf button → `DeadlineCloudSubmitter` MEL command |
| Entry point | `DeadlineCloudForMaya.py` plugin → `mel_commands.py` |
| Qt source | Uses Maya's existing Qt (via `qtpy`) |
| Dialog | `SubmitJobToDeadlineDialog` — **non-modal** (`show()`) |
| Custom widgets | `SceneSettingsWidget`, `FileSearchLineEdit` |
| Uses CLI binary | No |
| Uses Python lib | Yes — `api`, `job_bundle`, `ui.dialogs`, `ui.block_signals`, `exceptions` |
| Subprocess calls | None |
| Special notes | Caches dialog instance, recreates on scene change. Gets Maya main window via `QApplication.instance().topLevelWidgets()`. |

### Nuke

| Aspect | Detail |
|--------|--------|
| Language | Python (Nuke menu.py) |
| Launch | AWS Deadline menu → "Submit to Deadline Cloud" |
| Entry point | `src/menu.py` → `deadline_submitter_for_nuke.py` |
| Qt source | Uses Nuke's existing Qt (**PySide2 directly**, not qtpy) |
| Dialog | `SubmitJobToDeadlineDialog` — **non-modal** (`show()`) |
| Custom widgets | `SceneSettingsWidget` (no FileSearchLineEdit) |
| Uses CLI binary | No |
| Uses Python lib | Yes — `api`, `job_bundle`, `ui.dialogs`, `ui.gui_error_handler`, `exceptions` |
| Subprocess calls | None |
| Special notes | Only DCC using PySide2 directly instead of qtpy. Caches dialog globally, calls `refresh()` on reopen. |

### Houdini

| Aspect | Detail |
|--------|--------|
| Language | Python (Houdini Digital Asset / HDA) |
| Launch | ROP node buttons (Submit, Settings, Login, Save Bundle) |
| Entry point | `submitter.py` (HDA parameter callbacks) |
| Qt source | Uses Houdini's Qt (via `hou.qt.mainWindow()`) |
| Dialog | `SubmitJobProgressDialog` only — **NOT** `SubmitJobToDeadlineDialog` |
| Custom widgets | **None** — uses Houdini's native HDA parameter UI for scene settings |
| Uses CLI binary | No |
| Uses Python lib | Yes — `api`, `job_bundle`, `config`, `ui.dialogs.SubmitJobProgressDialog`, `ui.dialogs.DeadlineConfigDialog`, `ui.dialogs.DeadlineLoginDialog`, `job_attachments.upload.S3AssetManager` |
| Subprocess calls | None |
| Special notes | Unique among all DCCs: does NOT use `SubmitJobToDeadlineDialog`. Builds job template/parameters/assets itself using Houdini's native UI, then calls `SubmitJobProgressDialog` directly for the upload+submit step. Also directly uses `S3AssetManager` and `api.get_queue_user_boto3_session`. |

### Cinema 4D

| Aspect | Detail |
|--------|--------|
| Language | Python (C4D plugin command via `.pyp`) |
| Launch | Plugin menu → "AWS Deadline Cloud Submitter" |
| Entry point | `DeadlineCloud.pyp` → `cinema4d_render_submitter.py` |
| Qt source | Gets existing `QApplication` or creates one; applies C4D stylesheet |
| Dialog | `SubmitJobToDeadlineDialog` — **modal** (`exec_()`) |
| Custom widgets | `SceneSettingsWidget`, `FileSearchLineEdit` |
| Uses CLI binary | No |
| Uses Python lib | Yes — `job_bundle`, `ui.dialogs`, `exceptions` |
| Subprocess calls | `pip install deadline[gui]` (one-time setup) |
| Special notes | May create its own QApplication if C4D doesn't have one. Applies custom C4D stylesheet to the dialog. |

### VRED

| Aspect | Detail |
|--------|--------|
| Language | Python (VRED plugin script) |
| Launch | Deadline Cloud menu → "Submit to Deadline Cloud" |
| Entry point | `DeadlineCloudForVRED.py` → `vred_submitter.py` |
| Qt source | Uses VRED's PySide6 (direct, not qtpy) |
| Dialog | `SubmitJobToDeadlineDialog` — **non-modal** (`show()`) |
| Custom widgets | `SceneSettingsWidget` (extensive), `FileSearchLineEdit`, `CustomGroupBox`, `AutoSizedButton`, `AutoSizedComboBox`, `AutoSizingMessageBox` |
| Uses CLI binary | No |
| Uses Python lib | Yes — `api`, `job_bundle`, `ui.dialogs`, `exceptions` |
| Subprocess calls | None |
| Special notes | Most extensive custom UI of all DCCs. Uses PySide6 directly (not qtpy). Gets VRED main window via `vrMainWindow`. |

### 3ds Max

| Aspect | Detail |
|--------|--------|
| Language | Python + MaxScript |
| Launch | AWS Deadline menu → "Submit to Deadline Cloud" (MaxScript macro → Python) |
| Entry point | `run_ui.py` → `max_render_submitter.py` |
| Qt source | Uses 3ds Max's Qt (via `qtmax.GetQMaxMainWindow()`) |
| Dialog | **Subclass** of `SubmitJobToDeadlineDialog` (`SubmitMaxJobToDeadlineDialog`) — non-modal |
| Custom widgets | `SceneSettingsWidget`, `RenderElementsWidget`, `SubmitMaxJobToDeadlineDialog`, `ElideMiddleDelegate` |
| Uses CLI binary | No |
| Uses Python lib | Yes — `api`, `job_bundle`, `ui.dialogs`, `ui.block_signals`, `config`, `exceptions` |
| Subprocess calls | None |
| Special notes | Only DCC that **subclasses** `SubmitJobToDeadlineDialog` (adds custom close/submit behavior). |

### Unreal Engine

| Aspect | Detail |
|--------|--------|
| Language | C++ plugin + Python |
| Launch | Movie Render Queue pipeline / Data Asset (no menu item) |
| Entry point | C++ plugin → `submitter.py` |
| Qt source | **No Qt** — uses Unreal Slate/UMG (C++ native UI) |
| Dialog | `unreal.ScopedSlowTask` (Unreal progress), `unreal.EditorDialog` (messages) |
| Custom widgets | None (all Unreal-native C++) |
| Uses CLI binary | No |
| Uses Python lib | Yes — `api.create_job_from_job_bundle`, `api.get_deadline_cloud_library_telemetry_client`, `job_bundle.submission.AssetReferences`, `job_bundle.deadline_yaml_dump` |
| Subprocess calls | Runs `job_submit_wrapper.py` via Unreal's Python interpreter |
| Special notes | Only DCC with no Qt dependency. Runs submission in a subprocess to avoid blocking the Unreal Editor. Does NOT import `deadline.client.ui` at all. |

### After Effects

| Aspect | Detail |
|--------|--------|
| Language | ExtendScript (JSX) — no Python |
| Launch | AE panel: "Submit to AWS Deadline Cloud" |
| Entry point | `OpenAESubmitter.jsx` |
| Qt source | **No Qt** — uses AE's native ScriptUI |
| Dialog | ScriptUI `Window("palette")` for the panel; `deadline bundle gui-submit` for submission |
| Custom widgets | All ScriptUI (AE-native) |
| Uses CLI binary | **Yes** — `deadline bundle gui-submit <bundle> --output json --install-gui` |
| Uses Python lib | No |
| Subprocess calls | `deadline bundle gui-submit` (primary submission), `python3 get_user_fonts.py` (font detection) |
| Special notes | Only DCC that uses the CLI binary. Only DCC with no Python at all. The CLI's `gui-submit` command launches the full Qt submission dialog as a separate process. |

---

## What Each DCC Needs from `deadline-cloud-rs`

| DCC | Needs Rust CLI binary | Needs Rust shared lib | Needs Python `gui/` package | Needs `deadline.client.ui` |
|-----|----------------------|----------------------|---------------------------|--------------------------|
| Blender | No | Yes (via gui/) | Yes | Yes |
| Maya | No | Yes (via gui/) | Yes | Yes |
| Nuke | No | Yes (via gui/) | Yes | Yes |
| Houdini | No | Yes (via gui/) | Yes | Yes (partial — 3 dialogs) |
| Cinema 4D | No | Yes (via gui/) | Yes | Yes |
| VRED | No | Yes (via gui/) | Yes | Yes |
| 3ds Max | No | Yes (via gui/) | Yes | Yes |
| Unreal Engine | No | Yes (via gui/) | Yes (api + job_bundle only) | **No** |
| After Effects | **Yes** | No | No | No |

### Key takeaways for the migration:

1. **After Effects is the simplest** — it only needs the Rust CLI binary.
   `deadline bundle gui-submit` must work. No Python, no FFI.

2. **Unreal Engine needs the Python package but NOT the Qt widgets.** It
   imports `deadline.client.api` and `deadline.client.job_bundle` but
   never touches `deadline.client.ui`. The `gui/` package must export
   these non-UI modules backed by FFI.

3. **Houdini needs a submitter-side change.** It currently bypasses the
   shared `SubmitJobToDeadlineDialog` and manually orchestrates uploads
   via `S3AssetManager` + `get_queue_user_boto3_session` +
   `SubmitJobProgressDialog.start_submission`. These are deep library
   APIs that would require significant FFI surface to expose.

   **Recommended approach:** Modify the Houdini submitter to call
   `create_job_from_job_bundle(job_bundle_dir=...)` instead of manually
   orchestrating uploads. This function handles the full upload+submit
   flow internally and supports progress callbacks — which is what the
   FFI's `deadline_create_job_from_job_bundle` (Batch D) will expose.
   The Houdini HDA parameter UI stays unchanged (native Houdini, not Qt).
   Houdini can still show `SubmitJobProgressDialog` for progress by
   wiring the FFI's progress callbacks to Qt signals, same as every
   other DCC does through the shared dialog.

   This eliminates the need to expose `S3AssetManager`,
   `get_queue_user_boto3_session`, and `SubmitJobProgressDialog.start_submission`
   through FFI — reducing the FFI surface and aligning Houdini with the
   same submission path as the other 6 Qt-based DCCs.

   Note: Houdini already has full Qt support (ships PySide2/6). The
   reason it doesn't use `SubmitJobToDeadlineDialog` is that its scene
   settings live on the HDA node (native Houdini UI), not in a QWidget.
   This is a design choice, not a Qt limitation.

4. **3ds Max subclasses `SubmitJobToDeadlineDialog`** — the shared dialog
   must remain subclassable in the `gui/` package.

5. **Qt binding varies**: qtpy (Blender, Maya, Cinema 4D), PySide2 (Nuke),
   PySide6 (VRED), none (Unreal, After Effects). The `gui/` package
   should use `qtpy` for maximum compatibility.

6. **7 of 9 DCCs** follow the same pattern: import shared dialog, provide
   custom `SceneSettingsWidget`, show dialog inside DCC's Qt. The FFI
   migration primarily needs to make this pattern work.
