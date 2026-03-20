# Decoupling Client from Job Attachments

## Problem

The `deadline` package contains two main components: the Client (`deadline.client`) and Job Attachments (`deadline.job_attachments`). The Client is the higher-level package — it provides the CLI, GUI, configuration management, session handling, and telemetry. Job Attachments is the lower-level library responsible for hashing files, uploading/downloading assets to S3, and managing manifests.

The correct dependency direction is for the Client to depend on Job Attachments, not the other way around. However, over time, Job Attachments accumulated upward imports into the Client:

- Job Attachments imported `_ProgressBarCallbackManager` from `client.cli._common` to display a progress bar during file hashing — a UI concern that doesn't belong in a library.
- The `_hash_attachments` function orchestrates `S3AssetManager.hash_assets_and_create_manifest()` with callbacks. Despite only calling Job Attachments code, it was defined in the Client, and Job Attachments imported it back.
- Several Job Attachments modules directly read the Client's `~/.deadline/config` file via `config_file.get_cache_directory()` and `config_file.get_setting(...)` to get cache directories, S3 connection pool sizes, and upload threshold multipliers.
- `NonValidInputError` from `client.exceptions` — an exception type defined in the Client package but raised by Job Attachments functions.
- Path summarization utilities (`human_readable_file_size`, `summarize_path_list`, etc.) lived in a shared `common.path_utils` module with ambiguous ownership, even though they were only used in the context of Job Attachments operations.

This coupling meant Job Attachments could not be used, tested, or released independently. Any change to Client internals (config key names, exception hierarchies, CLI utilities) could break Job Attachments. It also blocked the longer-term goal of splitting Job Attachments into its own repository.

## Solution

Enforce a strict one-way dependency: `client` → `job_attachments`, never the reverse. Every function in Job Attachments that needs a boto3 session, a config value, or a progress callback receives it as a parameter. After these changes, `job_attachments` has zero imports from `deadline.client`.

Because all dependencies are now injected, Job Attachments can be used without the Client entirely:

```python
from deadline.job_attachments.api.manifest import _manifest_snapshot

# No client config, no progress bar, no telemetry — just hash files
result = _manifest_snapshot(
    root="/path/to/files",
    destination="/path/to/output",
    name="my-manifest",
)
```

Or with a custom session and progress callback:

```python
import boto3
from deadline.job_attachments.api.manifest import _manifest_snapshot

session = boto3.Session(profile_name="my-profile")
result = _manifest_snapshot(
    root="/path/to/files",
    destination="/path/to/output",
    name="my-manifest",
    hashing_progress_callback=my_custom_progress_handler,
    hash_cache_dir="/tmp/my-cache",
)
```

## Design Approach

The decoupling uses two patterns, applied consistently across all seven PRs:

### UI and telemetry concerns → Dependency Inversion

Job Attachments needed to report progress and record telemetry, but the concrete implementations (a CLI progress bar, a telemetry client) belong to the Client. Rather than importing those implementations, Job Attachments now defines callback interfaces (`Optional[Callable]`) and the Client provides the implementation at call time.

The callback types use classes already defined within Job Attachments:

| Callback | Signature | Purpose |
|---|---|---|
| Progress reporting | `Optional[Callable[[ProgressReportMetadata], bool]]` | UI progress updates. Returns `True` to continue, `False` to cancel. |
| Telemetry recording | `Optional[Callable[[SummaryStatistics], None]]` | Records hashing metrics. |
| Print/logging | `Callable[[Any], None]` | Text output (CLI echo, logger, etc.). |

All callbacks default to `None` or a no-op lambda, so non-CLI callers (programmatic use, worker agent) get silent operation with no extra work.

### Config values and sessions → Parameter Injection

Job Attachments needed config values (cache directories, pool sizes) and AWS sessions, but reading config files and creating sessions are Client responsibilities. Those values are now passed in as explicit function parameters by the caller.

Config values fall into two categories:

- **Required parameters** (`s3_max_pool_connections`, `small_file_threshold_multiplier` on `S3AssetUploader`) — intentionally required because silent defaults would cause subtle performance differences that are hard to debug. Missing parameters are caught immediately by linters and type checkers.
- **Optional parameters** (`cache_dir`, `s3_check_cache_dir`, `hash_cache_dir`) — default to `None`, allowing callers to omit them when the default behavior is acceptable.

Sessions (`boto3.Session`, `botocore.client.BaseClient`) and S3 settings (`JobAttachmentS3Settings`) are always passed explicitly. Job Attachments never creates or assumes AWS sessions.

## Changes (7 PRs)

### [PR #1028](https://github.com/aws-deadline/deadline-cloud/pull/1028) — Remove `_ProgressBarCallbackManager` import from Job Attachments

`_create_manifest_for_single_root` had three upward dependencies into the Client: the progress bar manager, `_hash_attachments`, and `config_file.get_cache_directory()`. Replaced all three with optional parameters:

```python
def _create_manifest_for_single_root(
    *,
    files: List[str],
    root: str,
    print_function_callback: Callable[[Any], None] = lambda msg: None,
    hashing_progress_callback: Optional[Callable[[ProgressReportMetadata], bool]] = None,
    telemetry_callback: Optional[Callable[[SummaryStatistics], None]] = None,
    hash_cache_dir: Optional[str] = None,
) -> Optional[BaseAssetManifest]:
```

These parameters thread through `_manifest_snapshot` → `_create_manifest_for_single_root` → `_hash_attachments`, with each layer passing them down unchanged.

**Files changed:** `job_attachments/asset_manifests/_create_manifest.py`, `job_attachments/api/manifest.py`, `client/cli/_groups/manifest_group.py`

### [PR #1035](https://github.com/aws-deadline/deadline-cloud/pull/1035) — Remove client error types from Job Attachments

Moved `NonValidInputError` from `deadline.client.exceptions` into `deadline.job_attachments.exceptions`, where it now inherits from `JobAttachmentsError`. Several Job Attachments functions (`_attachment_download`, `_attachment_upload`, `_process_path_mapping`, glob/diff utilities) were raising this Client-owned exception. Updated the Client's `attachment_group.py` to import and catch the Job Attachments-owned exception.

Technically a breaking change for external code catching `NonValidInputError` by its fully-qualified Client path, but the affected functions are underscored Beta API, so this is acceptable under the project's API conventions.

**Files changed:** `job_attachments/exceptions.py`, `job_attachments/_diff.py`, `job_attachments/_glob.py`, `job_attachments/api/_utils.py`, `job_attachments/api/attachment.py`, `client/cli/_groups/attachment_group.py`

### [PR #1036](https://github.com/aws-deadline/deadline-cloud/pull/1036) — Move S3 client and config logic out of manifest download

`_manifest_download` internally created its own Deadline API client, assumed queue role credentials, and resolved S3 settings by importing from `client.api._session` and reading the Client's config.

Pushed all session and config setup to the caller (`manifest_group.py`), which now passes three explicit parameters:

```python
def _manifest_download(
    *,
    deadline_client: botocore.client.BaseClient,  # for GetJob, ListStepDependencies calls
    queue_role_session: boto3.Session,             # for S3 manifest downloads
    queue_s3_settings: JobAttachmentS3Settings,    # bucket name and root prefix
    ...
)
```

**Files changed:** `job_attachments/api/manifest.py`, `client/cli/_groups/manifest_group.py`, `client/api/_session.py`

### [PR #1037](https://github.com/aws-deadline/deadline-cloud/pull/1037) — Move `_hash_attachments` into Job Attachments

`_hash_attachments` orchestrates the hashing workflow but lived in `client/api/_job_attachment.py` despite only calling Job Attachments code. Job Attachments' `_create_manifest_for_single_root` imported it back, creating a circular dependency.

Moved to `job_attachments/api/_hashing.py`. The telemetry callback is the key decoupling point — previously it directly called the Client's telemetry client, now it accepts an optional callback:

```python
# In client/api/_submit_job_bundle.py — Client provides the implementation
def hashing_telemetry_callback(hashing_summary: SummaryStatistics):
    api.get_deadline_cloud_library_telemetry_client().record_hashing_summary(hashing_summary)
```

**Files changed:** `client/api/_job_attachment.py` (function removed), `job_attachments/api/_hashing.py` (new), `job_attachments/asset_manifests/_create_manifest.py`, `client/api/_submit_job_bundle.py`, `client/cli/_groups/manifest_group.py`

### [PR #1038](https://github.com/aws-deadline/deadline-cloud/pull/1038) — Move path summarization into Job Attachments

Moved ~535 lines of path summarization utilities from `deadline.common.path_utils` (a shared module with ambiguous ownership) into `job_attachments/_path_summarization.py` where they logically belong.

Reduced the original `common/path_utils.py` to a deprecation shim that re-exports from the new location:

```python
from ..job_attachments._path_summarization import (
    human_readable_file_size, summarize_paths_by_nested_directory,
    summarize_paths_by_sequence, summarize_path_list, PathSummary,
)

import warnings
warnings.warn(
    "The deadline.common module is deprecated. Please use deadline.job_attachments.api instead.",
    DeprecationWarning, stacklevel=2,
)
```

All internal Client callers were updated to import directly from `job_attachments`.

**Files changed:** `common/path_utils.py` (gutted to shim), `job_attachments/_path_summarization.py` (new), `client/api/_submit_job_bundle.py`, `client/cli/_groups/job_group.py`, `client/cli/_incremental_download.py`, `job_attachments/api/__init__.py`

### [PR #1040](https://github.com/aws-deadline/deadline-cloud/pull/1040) — Remove direct config access in Job Attachments

Removed six direct imports of `config_file.get_cache_directory()` and `config_file.get_setting(...)` from `deadline.client.config` across Job Attachments:

| File | Config Call |
|---|---|
| `_diff.py` | `get_cache_directory()` |
| `upload.py` | `get_setting("settings.small_file_threshold_multiplier")` |
| `upload.py` | `get_setting("settings.s3_max_pool_connections")` |
| `api/attachment.py` | `get_cache_directory()` |
| `api/manifest.py` | `get_cache_directory()` |
| `_aws/aws_clients.py` | `get_setting("settings.s3_max_pool_connections")` |

Replaced all with explicit parameters. `S3AssetUploader.__init__` now requires `s3_max_pool_connections` and `small_file_threshold_multiplier` as keyword arguments. This is an intentional breaking change — `S3AssetUploader` is a public symbol used by both the Client and Worker Agent.

**Files changed:** `job_attachments/upload.py`, `job_attachments/_aws/aws_clients.py`, `job_attachments/_diff.py`, `job_attachments/api/attachment.py`, `job_attachments/api/manifest.py`, `job_attachments/download.py`, `job_attachments/asset_sync.py`, `client/api/_session.py`, `client/api/_submit_job_bundle.py`, `client/cli/_groups/attachment_group.py`, `client/cli/_groups/manifest_group.py`

### [PR #1062](https://github.com/aws-deadline/deadline-cloud/pull/1062) — Remove VFS dependencies on the Client

`VFSProcessManager` in `vfs.py` imported `get_deadline_cloud_library_telemetry_client` from `deadline.client.api` to call `record_vfs_mounting()` after each mount — the last remaining reverse dependency from Job Attachments into the Client.

Replaced the direct telemetry call with an `on_mount_complete: Optional[Callable[[bool], None]]` callback parameter, threaded through the full VFS mount chain:

```python
AssetSync.sync_inputs(
    ...,
    on_vfs_mount_complete=some_telemetry_callback,   # caller provides callback
)
  → _launch_vfs(on_mount_complete=on_vfs_mount_complete)
    → mount_vfs_from_manifests(on_mount_complete=on_mount_complete)
      → VFSProcessManager(on_mount_complete=on_mount_complete)
        → vfs_manager.start()
          → is_mounted = wait_for_mount(...)
          → if self._on_mount_complete is not None:
                self._on_mount_complete(is_mounted)
```

All parameters default to `None`, so no existing callers need changes. If no callback is provided, the mount event is silently ignored. This is not a breaking change — all affected interfaces are underscored.

With this PR, Job Attachments has zero imports from `deadline.client`.

**Files changed:** `job_attachments/vfs.py`, `job_attachments/download.py`, `job_attachments/asset_sync.py`

## Call Stacks

These are the primary user-facing flows that exercise the decoupled interface. Each shows how the Client prepares sessions, config values, and callbacks, then passes them into Job Attachments functions.

### 1. `deadline bundle submit` (Job Submission)

```
client.cli.bundle_group.cli_bundle_submit()
  └─ client.api._submit_job_bundle.create_job_from_job_bundle()
       │
       │  # Client reads config and creates the asset manager
       ├─ s3_max_pool_connections = config_file.get_setting("settings.s3_max_pool_connections")
       ├─ small_file_threshold_multiplier = config_file.get_setting("settings.small_file_threshold_multiplier")
       ├─ S3AssetManager(session, s3_max_pool_connections, small_file_threshold_multiplier)
       │
       │  # Client calls into JA for hashing, passing callbacks and config
       ├─ job_attachments.api._hashing._hash_attachments(
       │    asset_manager=asset_manager,
       │    hashing_progress_callback=on_preparing_to_submit,
       │    telemetry_callback=hashing_telemetry_callback,    ← client-defined, records to telemetry
       │    hash_cache_dir=config_file.get_cache_directory()
       │  )
       │    └─ S3AssetManager.hash_assets_and_create_manifest()
       │
       │  # Client calls into JA for upload
       └─ _upload_attachments(
            asset_manager=asset_manager,
            s3_check_cache_dir=config_file.get_cache_directory()
          )
            └─ S3AssetManager.upload_assets()
```

### 2. `deadline manifest snapshot` (Local Hashing)

```
client.cli.manifest_group.manifest_snapshot()
  │
  │  # Client creates UI and reads config
  ├─ progress_manager = _ProgressBarCallbackManager(length=100, label="Hashing Attachments")
  ├─ hash_cache_dir = config_file.get_cache_directory()
  │
  │  # Client calls into JA, passing callback and config
  └─ job_attachments.api.manifest._manifest_snapshot(
       hashing_progress_callback=progress_manager.callback,
       telemetry_callback=hashing_telemetry_callback,
       hash_cache_dir=hash_cache_dir
     )
       └─ _create_manifest_for_single_root(...)
            └─ job_attachments.api._hashing._hash_attachments(...)
                 └─ S3AssetManager.hash_assets_and_create_manifest()
```

### 3. `deadline manifest download` (S3 Download)

```
client.cli.manifest_group.manifest_download()
  │
  │  # Client handles all session and config setup
  ├─ boto3_session = api.get_boto3_session(config=config)
  ├─ deadline_client = boto3_session.client("deadline", config=get_default_client_config())
  ├─ queue = deadline_client.get_queue(farmId=farm_id, queueId=queue_id)
  ├─ queue_s3_settings = JobAttachmentS3Settings(**queue["jobAttachmentSettings"])
  ├─ queue_role_session = _get_queue_user_boto3_session(deadline_client, boto3_session, ...)
  │
  │  # Client passes everything JA needs as parameters
  └─ job_attachments.api.manifest._manifest_download(
       deadline_client=deadline_client,
       queue_role_session=queue_role_session,
       queue_s3_settings=queue_s3_settings,
       farm_id=farm_id, queue_id=queue_id, job_id=job_id
     )
       ├─ deadline_client.get_job(...)
       ├─ get_manifest_from_s3(session=queue_role_session)
       └─ get_output_manifests_by_asset_root(session=queue_role_session)
```

### 4. `deadline manifest upload` (S3 Upload)

```
client.cli.manifest_group.manifest_upload()
  │
  ├─ session = api.get_boto3_session(config=config)
  ├─ (resolves bucket_name and cas_path from queue or --s3-cas-uri)
  ├─ session = api.get_queue_user_boto3_session(...)   ← if using farm/queue
  │
  └─ job_attachments.api.manifest._manifest_upload(boto_session=session)
       └─ S3AssetUploader(session=session, s3_max_pool_connections=50, small_file_threshold_multiplier=20)
            └─ upload_bytes_to_s3(...)
```

### 5. `deadline attachment download` / `deadline attachment upload`

```
client.cli.attachment_group.attachment_download()
  ├─ boto3_session = api.get_queue_user_boto3_session(...)
  └─ job_attachments.api.attachment._attachment_download(boto3_session=session)
       └─ download_files_from_manifests(session=boto3_session)

client.cli.attachment_group.attachment_upload()
  │
  ├─ boto3_session = api.get_queue_user_boto3_session(...)
  ├─ s3_max_pool_connections = config_file.get_setting("settings.s3_max_pool_connections")
  ├─ small_file_threshold_multiplier = config_file.get_setting("settings.small_file_threshold_multiplier")
  │
  └─ job_attachments.api.attachment._attachment_upload(
       boto3_session=session,
       s3_check_cache_dir=config_file.get_cache_directory(),
       s3_max_pool_connections=s3_max_pool_connections,
       small_file_threshold_multiplier=small_file_threshold_multiplier
     )
       └─ S3AssetUploader(session, s3_max_pool_connections, small_file_threshold_multiplier)
            └─ upload_assets(s3_check_cache_dir=...)
```

### 6. `deadline job download-output` / Incremental Downloads

```
client.cli.job_group.job_download_output()
  ├─ queue_role_session = api.get_queue_user_boto3_session(...)
  └─ job_attachments.download.OutputDownloader(session=queue_role_session)
       └─ download_job_output()

client.cli.queue_group → _incremental_output_download()
  └─ job_attachments incremental download APIs (session injected)
  └─ job_attachments._path_summarization.summarize_path_list()
```

### 7. Worker Agent — VFS Mount with Telemetry

```
worker_agent → AssetSync.sync_inputs(
    ...,
    on_vfs_mount_complete=telemetry_callback,       ← caller provides callback
)
  └─ _launch_vfs(on_mount_complete=on_vfs_mount_complete)
       └─ mount_vfs_from_manifests(on_mount_complete=on_mount_complete)
            └─ VFSProcessManager(on_mount_complete=on_mount_complete)
                 └─ start(session_dir=session_dir)
                      ├─ is_mounted = wait_for_mount(...)
                      └─ if self._on_mount_complete:
                             self._on_mount_complete(is_mounted)
```

## Contract Quick Reference

### What the Client provides to Job Attachments

| What | Type | How it's provided |
|---|---|---|
| AWS credentials for S3 | `boto3.Session` | Parameter on every function that makes S3 calls |
| Deadline API client | `botocore.client.BaseClient` | Parameter on functions that call Deadline APIs |
| Queue S3 settings | `JobAttachmentS3Settings` | Parameter (bucket name, root prefix) |
| Hash cache directory | `Optional[str]` | Parameter, defaults to `None` |
| S3 check cache directory | `Optional[str]` | Parameter, defaults to `None` |
| S3 connection pool size | `int` | Required parameter on `S3AssetUploader` |
| Upload threshold multiplier | `int` | Required parameter on `S3AssetUploader` |
| Progress reporting | `Optional[Callable[[ProgressReportMetadata], bool]]` | Callback, defaults to `None` (silent) |
| Telemetry recording | `Optional[Callable[[SummaryStatistics], None]]` | Callback, defaults to `None` (no telemetry) |
| VFS mount completion | `Optional[Callable[[bool], None]]` | Callback, defaults to `None` (silent). Receives `True` if mount succeeded. |
| Print/logging output | `Callable[[Any], None]` | Callback, defaults to no-op lambda |

### What Job Attachments owns

- All S3 operations (upload, download, manifest management) using the provided session
- File hashing and manifest creation
- Asset diffing and comparison
- Path summarization and display formatting
- All exception types for attachment operations
- Progress tracking data types and reporting interfaces
- Data models for S3 settings, manifests, filesystem locations, and storage profiles

### What Job Attachments never does

- Import from `deadline.client.*` — zero imports exist (verified by grep)
- Read from `~/.deadline/config` — all config values are injected as parameters
- Create UI components — progress bars, click loggers, etc. are provided by the Client as callbacks
- Access telemetry clients — telemetry recording is done via an optional callback
- Create or assume AWS sessions — all sessions are created by the Client and passed in

### Key decoupling-related exports

These are the symbols whose signatures changed as part of the decoupling work. For the full export surface, see the source code.

| Symbol | Module | Change |
|---|---|---|
| `_hash_attachments` | `job_attachments.api._hashing` | Moved from `client.api._job_attachment`; added `telemetry_callback` |
| `_manifest_snapshot` | `job_attachments.api.manifest` | Added `hashing_progress_callback`, `telemetry_callback`, `hash_cache_dir` |
| `_manifest_download` | `job_attachments.api.manifest` | Now accepts `deadline_client`, `queue_role_session`, `queue_s3_settings` instead of creating its own |
| `_create_manifest_for_single_root` | `job_attachments.asset_manifests._create_manifest` | Added `hashing_progress_callback`, `telemetry_callback`, `hash_cache_dir` |
| `S3AssetUploader` | `job_attachments.upload` | `__init__` now requires `s3_max_pool_connections` and `small_file_threshold_multiplier` |
| `S3AssetManager` | `job_attachments.upload` | `__init__` accepts `s3_max_pool_connections` (default 50) and `small_file_threshold_multiplier` (default 20) |
| `NonValidInputError` | `job_attachments.exceptions` | Moved from `client.exceptions`; now inherits from `JobAttachmentsError` |
| `summarize_path_list`, `human_readable_file_size`, `PathSummary` | `job_attachments.api` (re-exported) | Moved from `common.path_utils`; old location is a deprecation shim |
| `VFSProcessManager` | `job_attachments.vfs` | Added `on_mount_complete: Optional[Callable[[bool], None]]`; removed `get_deadline_cloud_library_telemetry_client` import |
| `mount_vfs_from_manifests` | `job_attachments.download` | Added `on_mount_complete: Optional[Callable[[bool], None]]` |
| `AssetSync.sync_inputs` | `job_attachments.asset_sync` | Added `on_vfs_mount_complete: Optional[Callable[[bool], None]]` |

## Migration Guide

### For `deadline-cloud` Client code (internal)

No action needed — all Client callers were updated in the same PR set.

### For `deadline-cloud-worker-agent`

The Worker Agent imports and uses `S3AssetUploader` from the `deadline-cloud` package (`deadline.job_attachments.upload`). As part of [PR #1040](https://github.com/aws-deadline/deadline-cloud/pull/1040), the `S3AssetUploader.__init__` signature in `deadline-cloud` was changed to require two additional keyword arguments — `s3_max_pool_connections` and `small_file_threshold_multiplier` — which were previously read internally from the Client's config. Since the Worker Agent calls this constructor directly, it must be updated to pass these values explicitly:

```python
# Before (deadline-cloud handled config lookup internally)
uploader = S3AssetUploader(session=my_session)

# After (caller must provide the values that deadline-cloud no longer reads from config)
uploader = S3AssetUploader(
    session=my_session,
    s3_max_pool_connections=50,           # or read from your own config
    small_file_threshold_multiplier=20,   # or read from your own config
)
```

### For external consumers importing from `deadline.common.path_utils`

The import still works but emits a `DeprecationWarning`. Update imports to use the new location:

```python
# Before
from deadline.common.path_utils import summarize_path_list, human_readable_file_size

# After
from deadline.job_attachments.api import summarize_path_list, human_readable_file_size
```

<!-- TODO: Should we set a removal timeline for the deprecation shim? e.g., "Will be removed in version X.Y" -->

### For external code catching `NonValidInputError`

If you catch `NonValidInputError` by its fully-qualified path from `deadline.client.exceptions` when calling a Job Attachments function, update to catch from `deadline.job_attachments.exceptions`. The affected functions (`_attachment_download`, `_attachment_upload`) are underscored Beta API.

```python
# Before
from deadline.client.exceptions import NonValidInputError

try:
    _attachment_upload(...)
except NonValidInputError:
    ...

# After
from deadline.job_attachments.exceptions import NonValidInputError

try:
    _attachment_upload(...)
except NonValidInputError:
    ...
```

## Testing

Verification was performed at three levels — unit, integration, and downstream consumer — to confirm the decoupling changes do not introduce regressions.

### Unit Tests

Updated mocks and assertions across ~30 test files to reflect the new parameter signatures and import paths. The changes fall into these categories:

- **Callback threading** — verify that `hashing_progress_callback`, `telemetry_callback`, and `hash_cache_dir` are correctly passed through the full call chain (`_manifest_snapshot` → `_create_manifest_for_single_root` → `_hash_attachments` → `S3AssetManager.hash_assets_and_create_manifest()`).
- **Config injection** — verify that `S3AssetUploader` and `S3AssetManager` receive `s3_max_pool_connections` and `small_file_threshold_multiplier` as constructor arguments rather than reading from config. Tests confirm that invalid values (zero, negative) raise `AssetSyncError`.
- **Session injection** — verify that `_manifest_download` uses the injected `deadline_client` for API calls and `queue_role_session` for S3 operations, rather than creating its own.
- **Exception types** — verify that `NonValidInputError` is raised from `job_attachments.exceptions` and can be caught by the Client.
- **Deprecation shim** — verify that importing from `deadline.common.path_utils` still works but emits a `DeprecationWarning`.

```bash
# Single Python version
hatch run test

# All supported Python versions
hatch run all:test
```

### Integration Tests

Ran against a live AWS Deadline Cloud environment (us-west-2) to verify end-to-end behavior across all CLI flows:

- Job submission with file attachments (`deadline bundle submit`)
- Manifest snapshot, diff, download, and upload (`deadline manifest *`)
- Attachment download and upload (`deadline attachment *`)

This confirms that the decoupled interfaces work correctly with real AWS credentials, real S3 operations, and real Deadline Cloud API calls. The integration tests exercise the full call stacks documented above, from CLI entry point through to S3.

```bash
hatch run integ:test
```

### Downstream Consumer Tests — deadline-cloud-worker-agent

The Worker Agent ([deadline-cloud-worker-agent](https://github.com/aws-deadline/deadline-cloud-worker-agent)) is the primary downstream consumer of the `deadline` package. Running the worker agent's own tests against a wheel built from this branch catches issues that our own tests may not — specifically, import path changes, removed or renamed public symbols, and signature mismatches that only surface when a real consumer tries to use the package. This is the most direct way to validate that the decoupling doesn't break the contract between `deadline-cloud` and its dependents.

Two test tiers were run:

- **Worker Agent unit tests** — validates that the agent's code can import and call into our package without errors. Catches import path changes, removed or renamed symbols, and signature mismatches.
- **Worker Agent E2E tests** — deploys real worker agents on EC2 instances against the live Deadline Cloud service with our wheel installed. Jobs are submitted with input attachments, executed on workers, and output is downloaded. E2E tests were run on both Linux and Windows to verify cross-platform path handling.

```bash
# Build wheel from the feat/client-decouple branch
hatch build

# In the worker agent repo, install our wheel and run unit tests
pip install /path/to/deadline-cloud/dist/deadline-*.whl
hatch run test

# Deploy E2E testing infrastructure
./scripts/deploy_e2e_testing_infrastructure.sh

# Gather environment variables for each OS
./scripts/get_e2e_test_ids_from_cfn.sh --os Linux > .e2e_linux_infra.sh
./scripts/get_e2e_test_ids_from_cfn.sh --os Windows > .e2e_windows_infra.sh

# Run E2E tests (Linux)
source .e2e_linux_infra.sh
hatch run integ:test

# Run E2E tests (Windows)
source .e2e_windows_infra.sh
hatch run integ:test
```