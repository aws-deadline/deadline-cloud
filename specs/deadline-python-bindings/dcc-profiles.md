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
`deadline.client.api` functions directly from Python in Unreal's own
Python interpreter. Does NOT depend on `deadline.client.ui`.

### Pattern C: CLI Binary (1 DCC)

After Effects.

Pure ExtendScript (JSX) — no Python, no Qt. Shells out to the
`deadline` CLI binary. Already works with the Rust binary.

### Houdini: Pattern A with deep library usage

Houdini uses the shared Qt dialogs for config and login, but bypasses
`SubmitJobToDeadlineDialog` for submission. It manually orchestrates
uploads via `S3AssetManager` + `JobAttachmentS3Settings` and calls
`api.get_boto3_client` / `api.get_queue_user_boto3_session` for
credential management. These are deep boto3-based APIs that cannot
exist in the Rust package.

Additionally, Houdini is pinned to `deadline == 0.49.*` and calls
`SubmitJobProgressDialog.start_submission(...)` which no longer exists
in the current Python package (renamed to `start_job_submission` with
a different signature). Houdini is already incompatible with the latest
`deadline-cloud-python`.

**Recommended approach:** Modify the Houdini submitter to call
`create_job_from_job_bundle` instead of manual orchestration. This
handles the full upload+submit flow with progress callbacks, aligning
Houdini with the same submission path as the other 6 Qt-based DCCs.

## Runtime Compatibility Analysis (verified 2026-05-04)

Verified by grepping all 9 DCC submitter repos AND testing actual
runtime behavior (method calls, attribute access, return types).

### What `gui/` already provides (fully functional)

- `deadline.client.ui.*` — all Qt widgets, dialogs, utilities
- `deadline.client.exceptions` — `DeadlineOperationError`, `UserInitiatedCancel`, etc.
- `deadline.client.job_bundle.*` — `AssetReferences`, `deadline_yaml_dump`, `JobParameter`, `parameters`, `submission`, `loader`, `saver`, `create_job_history_bundle_dir`
- `deadline.client.config` — `get_setting`, `set_setting`, `read_config`, `str2bool`, `config_file` (with `write_config`)
- `deadline.client.api.create_job_from_job_bundle` — full submission with callbacks
- `deadline.client.api.AwsCredentialsSource`, `AwsAuthenticationStatus` — enums
- `deadline.client.api.precache_clients` — no-op (Rust handles caching)
- `deadline.client.api.get_queue_parameter_definitions` — via `_native`
- `deadline.job_attachments.models.FileConflictResolution` — enum
- `deadline.job_attachments.progress_tracker.ProgressReportMetadata` — dataclass
- `deadline.job_attachments.progress_tracker.ProgressStatus` — enum

### Remaining gaps: TelemetryClient methods (Batch A2)

The PyO3 `TelemetryClient` only exposes `record_event(event_type, details)`
and `close()`. DCC submitters call additional methods at runtime:

| Missing method | Signature | Used by | Fix |
|---|---|---|---|
| `update_common_details(dict)` | `(self, details: dict)` | ALL adaptors (Blender, Maya, Nuke, Houdini, Unreal) | Add to PyO3 class — store dict, merge into events |
| `record_error(...)` | `(self, event_details: dict, exception_type: str, from_gui: bool = False)` | Unreal | Add to PyO3 class — thin wrapper around `record_event` |
| `record_event` `from_gui` kwarg | `(self, event_type, event_details, *, from_gui=False)` | Unreal, Houdini | Add kwarg to existing PyO3 method |

**Impact:** Without these, ALL 6 Pattern A DCCs crash at runtime when
calling `telemetry_client.update_common_details(...)` even though the
import succeeds.

### Remaining gaps: `api` module functions (Batch A3 — Unreal only)

Unreal's `settings.py` calls `api.*` functions directly (not through
the Qt dialog). These need thin Python wrappers around `_native`:

| Missing function | Used by | Resolution |
|---|---|---|
| `api.list_farms()` | Unreal | Wrap `_native.list_farms()`, return `{"farms": [...]}` |
| `api.list_queues(farmId=...)` | Unreal | Wrap `_native.list_queues(farm_id)` |
| `api.list_storage_profiles_for_queue(...)` | Unreal | Wrap `_native.list_storage_profiles_for_queue(...)` |
| `api.get_credentials_source(config=...)` | Unreal | Wrap `_native.get_credentials_source()`, return enum |
| `api.check_authentication_status(config=...)` | Unreal | Wrap `_native.check_auth_status()` |
| `api.check_deadline_api_available(config=...)` | Unreal | Wrap `_native.check_api_available()` |
| `api.login(...)` | Unreal | Wrap `_native.login()` |
| `api.logout()` | Unreal | Wrap `_native.logout()` |
| `api.get_boto3_client("deadline")` | Unreal | Return dummy object (only passed to `precache_clients` which is a no-op) |

### Unsupported (requires submitter rewrite — Houdini only)

| Function | Why it can't be shimmed |
|---|---|
| `api.get_boto3_client("deadline")` (for real use) | Returns a boto3 client. Rust doesn't use boto3. |
| `api.get_queue_user_boto3_session(...)` | Returns a boto3 session with queue-scoped credentials. |
| `S3AssetManager` | Complex stateful class for manual upload orchestration. |
| `JobAttachmentS3Settings` | Only used with `S3AssetManager`. |
| `SubmitJobProgressDialog.start_submission(...)` | Deprecated method (removed in deadline 0.50+). |

### Switchover readiness per DCC

| DCC | Works today? | Remaining work | Submitter changes? |
|-----|---|---|---|
| After Effects | ✅ Yes | None | No |
| 3ds Max | ❌ Runtime crash | Batch A2 (TelemetryClient methods) | No |
| Cinema 4D | ❌ Runtime crash | Batch A2 | No |
| Blender | ❌ Runtime crash | Batch A2 | No |
| Maya | ❌ Runtime crash | Batch A2 | No |
| Nuke | ❌ Runtime crash | Batch A2 | No |
| VRED | ❌ Runtime crash | Batch A2 | No |
| Unreal | ❌ Runtime crash | Batch A2 + A3 | Minor (remove `get_boto3_client` usage) |
| Houdini | ❌ Incompatible | Submitter rewrite | **Yes** (major) |

### Switchover action plan

**Batch A2 — TelemetryClient methods (this repo, Rust changes):**
1. Add `update_common_details(dict)` to PyO3 `TelemetryClient`
2. Add `from_gui: bool = False` kwarg to `record_event`
3. Add `record_error(event_details, exception_type, from_gui=False)` method
4. Unblocks: Blender, Maya, Nuke, Cinema 4D, VRED, 3ds Max (zero submitter changes)

**Batch A3 — api module resource/auth wrappers (this repo, Python only):**
1. Add `list_farms`, `list_queues`, `list_storage_profiles_for_queue` wrappers
2. Add `get_credentials_source`, `check_authentication_status`, `check_deadline_api_available`
3. Add `login`, `logout` wrappers
4. Add `get_boto3_client` stub (returns dummy, only used for `precache_clients`)
5. Unblocks: Unreal (with minor submitter cleanup PR to remove `get_boto3_client` real usage)

**Batch B — Houdini submitter rewrite (separate repo):**
1. Replace `S3AssetManager` + `JobAttachmentS3Settings` with `create_job_from_job_bundle`
2. Replace `api.get_boto3_client` / `api.get_queue_user_boto3_session` (eliminated by above)
3. Replace `start_submission(...)` with `start_job_submission(...)` (already needed for latest Python)
4. Change private `api._queue_parameters` import to public `api.get_queue_parameter_definitions`

**Batch C — Dependency switch (all DCC repos, blocked on #24):**
1. Update `pyproject.toml` to depend on new package from `deadline-cloud-rs`
2. Verify each DCC's test suite passes
