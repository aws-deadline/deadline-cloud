# Handoff

Current in-flight work. Read this at the start of every session before
consulting the Work Items table in `specs/progress.md`.

## Active Work Item

None — pick next from `specs/progress.md`.

## Recently Completed — #16d Batches 2-4

Port Python Qt code into `gui/` (Batches 2-4).

Results:
- Rust: 1,169 passed, 0 failed (47 FFI tests)
- Python (non-Qt): 64 passed, 0 failed
- Self-audit: zero banned imports (api, boto3, botocore, job_attachments)
- CLI comparison: `farm get` and `queue get` semantically identical
- FFI live test: `get_farm` and `get_queue` verified against real AWS

Changes:
- 2 new Rust FFI functions: `deadline_get_farm`, `deadline_get_queue`
- 2 new Python `_ffi.py` methods: `get_farm`, `get_queue`
- 3 new `_compat.py` additions: `FileConflictResolution`, `JobAttachmentsFileSystem`, `str2bool`
- 55 Python ui/ files copied and rewired (10 files had api imports replaced)
- `api/__init__.py` shim with `session_context = {}`
- 15 ported test files in `gui/tests/ui/`
- `test_ui_port.py` with 9 test sections

Audit findings:
- FINDING-1 (fixed): Added 5 Rust tests for get_farm/get_queue
- FINDING-2 (fixed): Added null-arg tests for get_farm/get_queue
- FINDING-3 (deferred): Duplicated `_get_ffi()` singleton — mechanical cleanup

## Implementation Plan — Unified Batches 2-4

### Goal

Copy all Python `ui/` files into `gui/deadline/client/ui/`, rewire the
9 files that import `from ... import api` to use `_ffi.py` instead, and
verify with tests. This is a single unified batch — the old Batch 2/3/4
split is collapsed because the rewiring is mechanical and the files are
interdependent.

### Batch 1 (completed)

Foundation modules already in `gui/deadline/client/`:
- `_ffi.py`, `_compat.py`, `exceptions.py`, `__init__.py`
- `config/` — `config_file.py` (FFI shim), `__init__.py`
- `dataclasses/` — `submitter_info.py`, `__init__.py`
- `job_bundle/` — 8 files (parameters, loader, saver, _yaml, etc.)

### File Classification

#### Pure-copy files (25 files + resources + translations)

These have NO `from ... import api` and need zero changes. Copy as-is
from `deadline-cloud-python/src/deadline/client/ui/`:

| # | File | Notes |
|---|------|-------|
| 1 | `__init__.py` | Exports `block_signals`, `gui_error_handler`, etc. |
| 2 | `_utils.py` | `tr()`, `block_signals`, `gui_context_for_cli`. Uses `config_file` (already shimmed). |
| 3 | `cli_job_submitter.py` | Top-level submitter. No api imports. |
| 4 | `job_bundle_submitter.py` | Top-level submitter. Imports `session_context` — needs rewire. |
| 5 | `dataclasses/__init__.py` | `JobBundleSettings`, `CliJobSettings`, `HostRequirements`, etc. |
| 6 | `dataclasses/timeouts.py` | `TimeoutEntry`, `TimeoutTableEntries` |
| 7 | `dataclasses/_environment_info.py` | `_EnvironmentInfo.collect()` |
| 8 | `controllers/__init__.py` | Re-exports `AsyncTaskRunner`, `DeadlineUIController`, etc. |
| 9 | `controllers/_async_runner.py` | `AsyncTaskRunner` — manages background tasks |
| 10 | `controllers/_async_task.py` | `AsyncTask` QRunnable + `WorkerSignals` |
| 11 | `controllers/_thread_pool.py` | `DeadlineThreadPool` singleton |
| 12 | `widgets/__init__.py` | Re-exports all widget classes |
| 13 | `widgets/host_requirements_tab.py` | Pure Qt widget (imports `NonValidInputError` from exceptions) |
| 14 | `widgets/openjd_parameters_widget.py` | Pure Qt widget (imports from job_bundle) |
| 15 | `widgets/job_attachments_tab.py` | Pure Qt widget (imports `AssetReferences`) |
| 16 | `widgets/job_timeouts_widget.py` | Pure Qt widget |
| 17 | `widgets/spinbox_widgets.py` | Pure Qt widget |
| 18 | `widgets/path_widgets.py` | Pure Qt widget |
| 19 | `widgets/cli_job_settings_tab.py` | Pure Qt widget |
| 20 | `widgets/job_bundle_settings_tab.py` | Uses `config_file` (already shimmed) |
| 21 | `dialogs/__init__.py` | Re-exports dialog classes |
| 22 | `dialogs/_types.py` | `JobBundlePurpose` enum |
| 23 | `dialogs/_help_dialog.py` | Uses `SubmitterInfo` (already in gui/) |
| 24 | `dialogs/update_available_dialog.py` | Uses `config_file` (already shimmed) |
| 25 | `translations/` | README.md + `locales/` (12 JSON files) — copy entire directory |
| 26 | `resources/` | SVGs + 3 example bundles — copy entire directory |

#### Files that need rewiring (9 files)

These import `from ... import api` and call `api.*` functions. Each
must be changed to use `_ffi.py` or `_compat.py` instead.

| # | File | api.* calls used | Rewiring strategy |
|---|------|------------------|-------------------|
| 1 | `deadline_authentication_status.py` | `get_credentials_source`, `check_authentication_status`, `check_deadline_api_available`, `get_boto3_session(force_refresh)`, `AwsCredentialsSource`, `AwsAuthenticationStatus` | Replace `api.*` with `_ffi.*` calls. Enums from `_compat.py`. `get_boto3_session(force_refresh)` → no-op or remove (Rust handles session refresh internally). |
| 2 | `controllers/_deadline_controller.py` | `list_farms`, `list_queues`, `list_storage_profiles_for_queue`, `get_queue_parameter_definitions` | Replace with `_ffi.*` calls. All 4 are already in FFI. |
| 3 | `widgets/deadline_authentication_status_widget.py` | `AwsCredentialsSource`, `AwsAuthenticationStatus` (enum comparisons only) | Replace enum imports with `_compat.py`. No function calls. |
| 4 | `widgets/shared_job_settings_tab.py` | `get_boto3_client('deadline')` for `get_farm`, `get_queue`, `list_storage_profiles_for_queue` | These 3 display widgets (`DeadlineFarmDisplay`, `DeadlineQueueDisplay`, `DeadlineStorageProfileNameDisplay`) call boto3 directly. Need new FFI functions OR rewrite to use existing FFI list functions. See "FFI gaps" below. |
| 5 | `dialogs/deadline_login_dialog.py` | `login`, `AwsCredentialsSource` | Replace `api.login` with `_ffi.login`. Enum from `_compat.py`. |
| 6 | `dialogs/deadline_config_dialog.py` | `logout`, `get_deadline_cloud_library_telemetry_client().set_opt_out`, `boto3.Session()` for profile listing, `FileConflictResolution`, `JobAttachmentsFileSystem`, `get_setting_default`, `str2bool` | Most complex file. See detailed plan below. |
| 7 | `dialogs/submit_job_to_deadline_dialog.py` | `logout`, `get_deadline_cloud_library_telemetry_client().record_error`, `session_context` | Replace `api.logout` → `_ffi.logout`. Telemetry → `_ffi.record_telemetry_event`. `session_context` → local dict or remove. |
| 8 | `dialogs/_job_submission_worker.py` | `create_job_from_job_bundle`, `ProgressReportMetadata` | Replace `_api.create_job_from_job_bundle` → `_ffi.create_job_from_job_bundle`. `ProgressReportMetadata` from `_compat.py`. |
| 9 | `dev_application.py` | `api.logout()` | Replace with `_ffi.logout()`. |

Also needs rewiring (missed in original classification):
- `job_bundle_submitter.py` imports `from ..api._session import session_context`

### FFI Gaps — Functions needed but not yet exposed

| Gap | Used by | Resolution |
|-----|---------|------------|
| `get_boto3_session(force_refresh=True)` | `deadline_authentication_status.py` | Remove call. Rust manages session refresh internally. When file watcher fires, just call `refresh_status()` directly. |
| `get_boto3_client('deadline')` → `get_farm`, `get_queue`, `list_storage_profiles_for_queue` | `shared_job_settings_tab.py` display widgets | Add `deadline_get_farm` and `deadline_get_queue` to FFI (Rust already has `api::get_farm` and `api::get_queue`). Storage profile display can reuse existing `list_storage_profiles_for_queue` FFI. |
| `session_context` (mutable dict) | `submit_job_to_deadline_dialog.py`, `job_bundle_submitter.py` | Create a local Python-side dict. The FFI `create_job_from_job_bundle` already accepts `submitter_name` in params. |
| `get_deadline_cloud_library_telemetry_client()` | `submit_job_to_deadline_dialog.py` (`.record_error`), `deadline_config_dialog.py` (`.set_opt_out`) | Use `_ffi.record_telemetry_event` for error recording. For `set_opt_out`, add to FFI or use `_ffi.set_setting("settings.telemetry_opt_out", ...)`. |
| `boto3.Session()` for AWS profile listing | `deadline_config_dialog.py` | Use `_ffi.read_config()` to get current profile, then list profiles from `~/.aws/config` directly (pure Python INI parsing). No boto3 needed. |
| `FileConflictResolution`, `JobAttachmentsFileSystem` | `deadline_config_dialog.py` | Define as simple enums in `_compat.py`. Values: `COPIED`/`VIRTUAL` and `SKIP`/`OVERWRITE`/`CREATE_COPY`. |
| `get_setting_default` | `deadline_config_dialog.py` | Already noted as Batch 1 limitation. Use `_ffi.get_setting` as fallback (returns current value). |
| `str2bool` | `deadline_config_dialog.py` | Pure Python utility — copy from Python source or implement inline. |
| `AwsCredentialsSource` (for `on_pending_authorization` callback) | `deadline_login_dialog.py` | Already in `_compat.py`. |

### Detailed plan for `deadline_config_dialog.py` (hardest file)

This file has the most external dependencies:
1. `import boto3` → Replace with pure Python `~/.aws/config` parsing for profile list
2. `from botocore.exceptions import ProfileNotFound` → Replace with generic `Exception` catch
3. `from deadline.job_attachments.models import FileConflictResolution, JobAttachmentsFileSystem` → Add to `_compat.py`
4. `api.logout()` → `_ffi.logout()`
5. `api.get_deadline_cloud_library_telemetry_client().set_opt_out()` → `_ffi.set_setting("settings.telemetry_opt_out", ...)`
6. `get_setting_default()` → `_ffi.get_setting()` (returns current value, acceptable)
7. `str2bool()` → inline implementation

### New FFI functions needed (Rust side)

Two new `extern "C"` functions in `deadline-gui-ffi`:

1. `deadline_get_farm(farm_id, config_path) -> JSON` — calls `api::get_farm`
2. `deadline_get_queue(farm_id, queue_id, config_path) -> JSON` — calls `api::get_queue`

These are simple wrappers following the existing pattern in `ffi.rs`.

### New `_compat.py` additions

```python
class FileConflictResolution(str, Enum):
    CREATE_COPY = "CREATE_COPY"
    SKIP = "SKIP"
    OVERWRITE = "OVERWRITE"

class JobAttachmentsFileSystem(str, Enum):
    COPIED = "COPIED"
    VIRTUAL = "VIRTUAL"
```

### Execution order

1. **Copy pure files** — all 25 pure-copy files + translations + resources
2. **Add `_compat.py` enums** — `FileConflictResolution`, `JobAttachmentsFileSystem`
3. **Add 2 new FFI functions** (Rust) — `deadline_get_farm`, `deadline_get_queue`
4. **Add 2 new `_ffi.py` methods** — `get_farm`, `get_queue`
5. **Rewire 9+1 files** — replace `api.*` imports with `_ffi.*` / `_compat.*`
6. **Add `str2bool` utility** — in `_compat.py` or `config/__init__.py`
7. **Add `session_context`** — local dict in `gui/deadline/client/api/__init__.py` shim
8. **Copy and adapt Python tests** — 21 test files from Python repo
9. **Run tests** — verify all pass
10. **Self-audit** — verify every `from ... import api` is eliminated

### Import rewiring cheat sheet

Old Python import → New gui/ import:

```
from ... import api                    → from ..._ffi import DeadlineFFI (or singleton)
from ...api._session import ...        → from ..._compat import ...
from ...config import config_file      → (already shimmed, no change)
from ...config import get_setting      → from ..config import get_setting
from ...config import set_setting      → from ..config import set_setting
from ...config import get_setting_default → from ..config import get_setting (fallback)
from ...config import str2bool         → from .._compat import str2bool
from ...exceptions import ...          → from ..exceptions import ...
from ...job_bundle import ...          → from ..job_bundle import ...
from ...dataclasses import ...         → from ..dataclasses import ...
from ....job_attachments.progress_tracker import ProgressReportMetadata
                                       → from ..._compat import ProgressReportMetadata
from deadline.job_attachments.models import FileConflictResolution, JobAttachmentsFileSystem
                                       → from ..._compat import FileConflictResolution, JobAttachmentsFileSystem
import boto3                           → (remove, use pure Python config parsing)
from botocore.exceptions import ...    → (remove)
```

### Test plan

Copy the 21 Python test files from `deadline-cloud-python/test/unit/deadline_client/ui/`
into `gui/tests/ui/`. Tests that mock `api.*` will need their mocks updated
to target `_ffi.*` instead. Tests that use `deadline.job_attachments` mocks
will need similar updates.

Key test files:
- `dialogs/test_deadline_config_dialog.py` — heaviest mocking
- `dialogs/test_submit_job_to_deadline_dialog.py`
- `dialogs/test_job_submission_worker.py`
- `controllers/test_deadline_controller.py`
- `controllers/test_async_runner.py`, `test_async_task.py`, `test_thread_pool.py`
- `widgets/test_shared_job_settings_tab.py`
- `widgets/test_host_requirements_tab.py`
- `dataclasses/test_timeouts.py`
- `test_translations.py`
- `test_host_requirements_dataclass_serialize.py`

### Risk assessment

- **Low risk**: Pure-copy files (25) — no changes needed
- **Low risk**: Enum additions to `_compat.py` — trivial
- **Medium risk**: 2 new FFI functions — follows existing pattern
- **Medium risk**: Rewiring 9 files — mechanical but must verify every call site
- **High risk**: `deadline_config_dialog.py` — most complex, removes boto3 dependency
- **Medium risk**: Test adaptation — mock targets change

### Audit checklist (post-implementation)

- [ ] `grep -r "from.*import api" gui/deadline/client/ui/` returns zero results
- [ ] `grep -r "import boto3" gui/deadline/client/ui/` returns zero results
- [ ] `grep -r "from botocore" gui/deadline/client/ui/` returns zero results
- [ ] `grep -r "deadline.job_attachments" gui/deadline/client/ui/` returns zero results
- [ ] All Python tests pass
- [ ] All Rust tests pass (`cargo test`)
- [ ] No new `api.*` calls introduced

## Recently Completed — #16d Batch 1

Foundation modules ported into `gui/deadline/client/`:
- `exceptions.py` — copied from Python
- `dataclasses/` — `SubmitterInfo` copied from Python
- `job_bundle/` — 8 files copied from Python
- `config/config_file.py` — new FFI shim routing through Rust
- `_compat.py` — new enum types + `ProgressReportMetadata`

33 Python tests + 28 FFI tests pass. 1164 Rust tests pass.

### Audit findings (Batch 1)

1. (Bug, fixed) `AwsAuthenticationStatus` had `NOT_AUTHENTICATED` instead
   of `NEEDS_LOGIN` — Python uses `NEEDS_LOGIN`, widget checks for it.
2. (Limitation, documented) `get_setting_default` returns current value,
   not hardcoded default. FFI doesn't expose this endpoint. Deferred to
   Batch 3 when config dialog needs it.
3. (By design) `config` parameter on `get_setting`/`set_setting` is
   ignored — always reads from disk. Batch 3 will add temp-file support
   for config dialog preview.
