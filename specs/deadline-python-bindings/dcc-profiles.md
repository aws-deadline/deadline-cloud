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
