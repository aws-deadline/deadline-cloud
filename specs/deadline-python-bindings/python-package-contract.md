# Python Package Contract

Documents the Python-side contract of the `deadline` package
(`gui/deadline/`). This is the consumer-facing API that DCC submitters
and the Qt GUI import — the other side of the PyO3 boundary documented
in `architecture.md`.

## Package Structure

```
gui/deadline/
├── _native.abi3.so              # PyO3 module (built by maturin)
├── client/
│   ├── api/__init__.py          # DCC-facing shim → routes to _native
│   ├── _compat.py               # Types: enums, ProgressReportMetadata
│   ├── config/                  # Config shim (get_setting, set_setting)
│   ├── exceptions.py            # DeadlineOperationError subclasses
│   └── ui/                      # Qt widgets and dialogs
└── job_attachments/
    ├── models.py                # FileConflictResolution enum
    └── progress_tracker.py      # Re-exports ProgressReportMetadata, ProgressStatus
```

## `deadline.client.api` Exports

The primary entry point for DCC submitters. Routes to `_native` or
`_compat` types.

| Export | Source | Purpose |
|--------|--------|---------|
| `TelemetryClient` | `_native` | Telemetry recording |
| `get_deadline_cloud_library_telemetry_client()` | shim | Returns a `TelemetryClient` instance |
| `create_job_from_job_bundle(job_bundle_dir, ...)` | shim → `_native` | Full submission (flat kwargs) |
| `get_queue_parameter_definitions(...)` | `_native` | Queue parameter definitions |
| `list_farms(config=None, **kwargs)` | shim → `_native` | List farms |
| `list_queues(config=None, **kwargs)` | shim → `_native` | List queues (accepts `farmId=`) |
| `list_storage_profiles_for_queue(config=None, **kwargs)` | shim → `_native` | List storage profiles (accepts `farmId=`, `queueId=`) |
| `get_credentials_source(config=None)` | shim → `_native` | Returns `AwsCredentialsSource` enum |
| `check_authentication_status(config=None)` | shim → `_native` | Returns `AwsAuthenticationStatus` enum |
| `check_deadline_api_available(config=None)` | shim → `_native` | Returns bool |
| `login(on_pending_authorization=None, on_cancellation_check=None, config=None)` | shim → `_native` | DCM login with callbacks |
| `logout(config=None)` | shim → `_native` | DCM logout |
| `get_boto3_client(service_name, config=None)` | shim | Returns `None` (stub) |
| `AwsCredentialsSource` | `_compat` | Enum: HOST_PROVIDED, DEADLINE_CLOUD_MONITOR_LOGIN, NOT_VALID |
| `AwsAuthenticationStatus` | `_compat` | Enum: AUTHENTICATED, CONFIGURATION_ERROR, NEEDS_LOGIN |
| `precache_clients(...)` | shim | No-op (Rust handles caching) |

## TelemetryClient API

Exposed via `deadline._native.TelemetryClient` and re-exported through
`deadline.client.api`.

```python
class TelemetryClient:
    def __init__(self, config_path: Optional[str] = None): ...
    def record_event(self, event_type: str, details: dict, *, from_gui: bool = False): ...
    def record_error(self, event_details: dict, exception_type: str, from_gui: bool = False): ...
    def update_common_details(self, details: dict): ...
    def close(self): ...
```

| Method | Behavior |
|--------|----------|
| `record_event` | Enqueues event with `usage_mode` set to "GUI" or "CLI" based on `from_gui` |
| `record_error` | Inserts `exception_type` key into details, then records as `com.amazon.rum.deadline.error` |
| `update_common_details` | Merges dict into common details included in all future events |
| `close` | Flushes pending events and drops the inner client |

**DCC adaptor pattern:**
```python
client = get_deadline_cloud_library_telemetry_client()
client.update_common_details({"deadline-cloud-for-blender-submitter-version": "1.0.0"})
client.record_event("com.amazon.rum.deadline.adaptor.runtime.start", {})
client.record_error({"exit_code": 1}, "RuntimeError")
```

## Progress Callback Dict Contract

Rust progress callbacks (`on_hashing_progress`, `on_upload_progress`)
pass a dict to the Python callable with these keys:

| Key | Type | Description |
|-----|------|-------------|
| `progress` | `float` | Percentage (0.0–100.0) |
| `transferRate` | `float` | Bytes/second |
| `progressMessage` | `str` | Human-readable status message |
| `processedFiles` | `int` | Number of files completed |

**Keys are camelCase** — matching the original Python
`ProgressReportMetadata` dataclass field names from
`deadline-cloud-python`.

## ProgressReportMetadata

Defined in `gui/deadline/client/_compat.py`, re-exported from
`deadline.job_attachments.progress_tracker`.

```python
@dataclass
class ProgressReportMetadata:
    status: str = ""
    progress: float = 0.0
    transfer_rate: float = 0.0      # snake_case Python attribute
    progress_message: str = ""      # snake_case Python attribute
    processed_files: int = 0        # snake_case Python attribute

    # camelCase property aliases (read-only) for DCC submitter compat
    @property
    def transferRate(self): ...
    @property
    def progressMessage(self): ...
    @property
    def processedFiles(self): ...

    @classmethod
    def from_dict(cls, data: dict) -> "ProgressReportMetadata":
        # Reads camelCase keys from Rust callback dict
        # Maps to snake_case dataclass attributes
```

**Key naming convention:**
- Rust callback dict → camelCase keys (`transferRate`, `progressMessage`, `processedFiles`)
- Python dataclass attributes → snake_case (`transfer_rate`, `progress_message`, `processed_files`)
- camelCase property aliases → read-only, for DCC submitters that access original field names
- `from_dict` bridges the Rust dict to the dataclass

## Known Gaps (deferred)

| Gap | Impact | Tracked |
|-----|--------|---------|
| `status` field not included in progress callback dict | None — GUI doesn't read it | Fix when touching `submission.rs` |
| `from_gui` in `create_job_from_job_bundle` params is ignored | Telemetry tagged CLI instead of GUI | #21g |
