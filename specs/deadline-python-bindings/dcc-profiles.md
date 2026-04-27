# DCC Submitter Integration Profiles

How each DCC submitter integrates with Deadline Cloud, and what's needed
to switch them from `deadline-cloud-python` to `deadline-cloud-rs`.

## Integration Patterns

There are three distinct patterns across the 9 DCC submitters:

### Pattern A: Shared Qt Dialog (7 DCCs)

Blender, Maya, Nuke, Cinema 4D, VRED, 3ds Max, Houdini (partially).

These DCCs import `deadline.client.ui` and show the shared
`SubmitJobToDeadlineDialog` inside the DCC's own Qt event loop. Each
provides a custom `SceneSettingsWidget` for DCC-specific settings.

Qt binding varies: qtpy (Blender, Maya, Cinema 4D), PySide2 (Nuke),
PySide6 (VRED). The `gui/` package uses `qtpy` for compatibility.

3ds Max subclasses `SubmitJobToDeadlineDialog` — the shared dialog
must remain subclassable.

### Pattern B: Native UI + Python Library (1 DCC)

Unreal Engine.

Uses Unreal's native Slate/UMG C++ widgets — no Qt. Calls
`deadline.client.api.create_job_from_job_bundle()` from Python in a
subprocess of Unreal's own Python interpreter. Does NOT depend on
`deadline.client.ui`.

### Pattern C: CLI Binary (1 DCC)

After Effects.

Pure ExtendScript (JSX) — no Python, no Qt. Shells out to the
`deadline` CLI binary. Already works with the Rust binary.

### Houdini: Pattern A with deep library usage

Houdini uses the shared Qt dialogs for config and login, but bypasses
`SubmitJobToDeadlineDialog` for submission. It manually orchestrates
uploads via `S3AssetManager` + `JobAttachmentS3Settings` and imports
the private `api._queue_parameters` module. These are deep library APIs
not exposed via `_native`.

**Recommended approach:** Modify the Houdini submitter to call
`create_job_from_job_bundle` instead of manual orchestration. This
handles the full upload+submit flow with progress callbacks, aligning
Houdini with the same submission path as the other 6 Qt-based DCCs.
The Houdini HDA parameter UI stays unchanged (native Houdini, not Qt).

## Import Inventory and Gap Analysis (verified 2026-04-27)

Imports verified by grepping all 9 DCC submitter repos.

### What `gui/` already provides

These import paths are fully functional — backed by `_native` (PyO3) or
pure Python code in `gui/`:

- `deadline.client.ui.*` — all Qt widgets, dialogs, utilities
- `deadline.client.exceptions` — `DeadlineOperationError`, `UserInitiatedCancel`, etc.
- `deadline.client.job_bundle.*` — `AssetReferences`, `deadline_yaml_dump`, `JobParameter`, `parameters`, `submission`, `loader`, `saver`
- `deadline.client.config` — `get_setting`, `set_setting`, `read_config`, `str2bool`, `config_file`

### What `gui/` is missing (shim work, no submitter changes)

`gui/deadline/client/api/__init__.py` is currently a minimal stub. These
need to be re-exported from `_native`:

| Import | Used by |
|--------|---------|
| `deadline.client.api.get_deadline_cloud_library_telemetry_client` | Blender, Maya, Nuke, Houdini, Unreal, VRED |
| `deadline.client.api.TelemetryClient` | Blender, Maya, Nuke, Houdini, Unreal |
| `deadline.client.api.create_job_from_job_bundle` | Unreal |
| `deadline.client.api.AwsCredentialsSource` | Unreal |
| `deadline.client.api.AwsAuthenticationStatus` | Unreal |
| `deadline.client.api.precache_clients` | Unreal |
| `deadline.client.job_bundle.create_job_history_bundle_dir` | Houdini, Unreal |

### What requires submitter-side changes

| Import | Used by | Notes |
|--------|---------|-------|
| `deadline.job_attachments.upload.S3AssetManager` | Houdini | Deep API — recommend `create_job_from_job_bundle` instead |
| `deadline.job_attachments.models.JobAttachmentS3Settings` | Houdini | Used alongside `S3AssetManager` |
| `deadline.client.api._queue_parameters.get_queue_parameter_definitions` | Houdini | Private module import — should use public API |
| `deadline.job_attachments.models.FileConflictResolution` | Unreal | Enum — can be re-exported as pure Python class |
| `deadline.job_attachments.progress_tracker.ProgressReportMetadata` | Unreal | Data class — can be re-exported as pure Python class |
| `deadline.job_attachments.progress_tracker.ProgressStatus` | Unreal | Data class — can be re-exported as pure Python class |

### Switchover impact per DCC

| DCC | Submitter changes needed? | Blocking gaps |
|-----|--------------------------|---------------|
| After Effects | No | CLI binary only — already works |
| 3ds Max | No | None — all imports provided |
| Cinema 4D | No | None — all imports provided |
| Blender | No | `api` shim needs `TelemetryClient` + telemetry helper |
| Maya | No | Same as Blender |
| Nuke | No | Same as Blender |
| VRED | No | Same as Blender |
| Unreal | **Maybe** | Most gaps are shimmable; `precache_clients` needs design decision |
| Houdini | **Yes** | `S3AssetManager`, `JobAttachmentS3Settings`, private `_queue_parameters` import |

### Switchover action items

1. **Complete `gui/deadline/client/api/__init__.py`** — re-export
   `TelemetryClient`, `get_deadline_cloud_library_telemetry_client`,
   `create_job_from_job_bundle`, `AwsCredentialsSource`,
   `AwsAuthenticationStatus` from `_native`. Unblocks 6 DCCs.
2. **Add `job_attachments` data class re-exports** — `FileConflictResolution`,
   `ProgressReportMetadata`, `ProgressStatus` as pure Python classes in
   `gui/deadline/job_attachments/`. Unblocks Unreal.
3. **Decide on `precache_clients`** — Unreal calls this. Either implement
   as a no-op (Rust handles caching internally) or expose via `_native`.
4. **Houdini submitter change** — modify to use `create_job_from_job_bundle`
   instead of `S3AssetManager` + manual orchestration.
