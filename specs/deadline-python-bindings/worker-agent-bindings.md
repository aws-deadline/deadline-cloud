# Worker Agent Bindings

Expose job attachment operations through `deadline._native` so the Python
worker agent (`deadline-cloud-worker-agent`) can consume our Rust
implementation without code changes.

**Status:** Planned. Ready to start (#31 prerequisites complete).

**Goal:** `pip install deadline` (our maturin-built package) provides
everything the worker agent imports from `deadline.job_attachments.*`.
Zero changes to the worker agent repo.

---

## Worker Agent Import Surface

Collected from `deadline-cloud-worker-agent/src/` production code:

### Models & Types

| Import path | Type | PyO3 strategy |
|-------------|------|---------------|
| `models.ManifestProperties` | dataclass | `#[pyclass]` or dict via pythonize |
| `models.PathMappingRule` | dataclass | dict |
| `models.PathFormat` | enum (POSIX/WINDOWS) | `#[pyclass]` enum |
| `models.JobAttachmentS3Settings` | dataclass + `from_s3_root_uri()` classmethod | `#[pyclass]` with `#[classmethod]` |
| `models.JobAttachmentsFileSystem` | enum | `#[pyclass]` enum |
| `models.ManifestSnapshot` | dataclass (return) | dict |
| `models.ManifestMerge` | dataclass (return) | dict |
| `models.UploadManifestInfo` | dataclass (return) | dict |
| `asset_manifests.BaseAssetManifest` | base class | `#[pyclass]` (flat, no inheritance) |
| `asset_manifests.v2023_03_03.AssetManifest` | concrete class + `get_default_hash_alg()` | same pyclass as above |
| `progress_tracker.SummaryStatistics` | dataclass | dict |
| `progress_tracker.DownloadSummaryStatistics` | dataclass | dict |
| `progress_tracker.ProgressReportMetadata` | dataclass (callback arg) | dict |
| `progress_tracker.ProgressTracker` | mutable stateful object | `#[pyclass]` |
| `progress_tracker.ProgressStatus` | enum | `#[pyclass]` enum |

### Functions

| Import path | Signature pattern | PyO3 strategy |
|-------------|-------------------|---------------|
| `asset_manifests.hash_data` | `(bytes, alg) → str` | `#[pyfunction]` direct |
| `asset_manifests.decode.decode_manifest` | `str → BaseAssetManifest` | `#[pyfunction]` direct |
| `download.download_files_from_manifests` | kwargs + `on_downloading_files` callback | See "Callback functions" below |
| `upload.S3AssetUploader` | class with `upload_assets` method | `#[pyclass]` + `#[pymethods]` |
| `api.manifest._manifest_snapshot` | function + `print_function_callback` | `#[pyfunction]`, ignore print callback |
| `api.manifest._manifest_merge` | function + `print_function_callback` | `#[pyfunction]`, ignore print callback |
| `api.human_readable_file_size` | `int → str` | `#[pyfunction]` trivial |
| `os_file_permission.*` | file permission utilities | `#[pyfunction]` |
| `_utils._get_unique_dest_dir_name` | path utility | `#[pyfunction]` |
| `asset_sync.AssetSync` | orchestrator class | `#[pyclass]` |

### Config & Telemetry (already exposed)

| Import path | Status |
|-------------|--------|
| `client.config.config_file.get_cache_directory` | Need to add |
| `client.api.TelemetryClient` | ✅ Already exposed |
| `client.version` | Need to add (string constant) |
| `job_attachments.version` | Need to add (string constant) |

---

## Callback Functions — Design

The worker agent passes Python callables to `download_files_from_manifests`
for progress reporting and cancellation. Two approaches:

### Approach A: Accept Python callable in Rust (recommended)

```rust
#[pyfunction]
fn download_files_from_manifests(
    py: Python<'_>,
    s3_bucket: &str,
    manifests_by_root: HashMap<String, PyObject>,
    cas_prefix: Option<&str>,
    on_downloading_files: Option<PyObject>,  // Python callable
) -> PyResult<PyObject> {
    // Rust does the download, periodically calls back:
    let should_continue = on_downloading_files
        .call1(py, (progress_dict,))?
        .extract::<bool>(py)?;
}
```

PyO3 supports this pattern. The Rust download engine runs on a tokio
runtime; progress callbacks release the GIL briefly to call Python.

### Approach B: Python wrapper (fallback)

If GIL contention or callback frequency causes issues:

```python
# deadline/job_attachments/download.py
from deadline._native import _download_files_core

def download_files_from_manifests(..., on_downloading_files=None):
    # Rust does the download, returns progress via channel/queue
    result = _download_files_core(s3_bucket, manifests_by_root, cas_prefix)
    # Python handles callback loop
    ...
```

**Decision:** Start with Approach A. Fall back to B only if benchmarking
shows GIL contention under the worker agent's 1-second progress interval.

---

## Python Module Structure

The worker agent imports from `deadline.job_attachments.*`. We need to
provide these module paths. Since maturin installs our package as
`deadline/`, we add Python shim modules:

```
deadline/
├── _native.abi3.so              # PyO3 module (existing)
├── client/                      # existing (GUI uses this)
│   ├── config/
│   │   └── config_file.py       # wraps _native.get_setting etc.
│   └── api.py                   # wraps _native (TelemetryClient etc.)
└── job_attachments/             # NEW — worker agent compatibility layer
    ├── __init__.py              # version string, re-exports
    ├── models.py                # re-exports from _native
    ├── asset_manifests/
    │   ├── __init__.py          # BaseAssetManifest, hash_data
    │   ├── decode.py            # decode_manifest
    │   └── v2023_03_03/
    │       └── asset_manifest.py  # AssetManifest class
    ├── download.py              # download_files_from_manifests
    ├── upload.py                # S3AssetUploader
    ├── progress_tracker.py      # ProgressTracker, ProgressStatus, etc.
    ├── asset_sync.py            # AssetSync orchestrator
    ├── api/
    │   ├── __init__.py          # human_readable_file_size
    │   └── manifest.py          # _manifest_snapshot, _manifest_merge
    ├── os_file_permission.py    # file permission utilities
    └── _utils.py                # _get_unique_dest_dir_name
```

Each `.py` file is either:
- A direct re-export from `deadline._native` (for simple types/functions)
- A thin wrapper that adapts the Rust API to match the old Python signature

---

## Private API Handling

The worker agent imports private APIs (underscore-prefixed):
- `_manifest_snapshot`
- `_manifest_merge`
- `_get_unique_dest_dir_name`

**Strategy:** Implement these as proper public functions in Rust, expose
via PyO3, then provide the underscore-named re-exports in the Python shim
for backward compatibility. This lets us eventually deprecate the
underscore names.

---

## Phased Delivery

### Phase 1: Models & Pure Functions
- All model types as pyclasses or dicts
- `hash_data`, `decode_manifest`, `human_readable_file_size`
- Version strings
- Python module structure with re-exports

### Phase 2: Download Path
- `download_files_from_manifests` with callback support
- `JobAttachmentS3Settings.from_s3_root_uri()`
- `DownloadSummaryStatistics` return type

### Phase 3: Upload Path
- `S3AssetUploader` pyclass
- `_manifest_snapshot`, `_manifest_merge`
- `ProgressTracker` pyclass
- `UploadManifestInfo` return type

### Phase 4: Orchestration
- `AssetSync` class
- `os_file_permission` utilities
- `config_file.get_cache_directory()`

---

## Prerequisites

All prerequisites are met:

1. ~~**#31 Step 9 (CLI/Library Boundary)**~~ ✅ Done — library functions
   take clean typed inputs/outputs, no callbacks or config-from-disk.
2. ~~**#31 Step 10 (Crate Merge)**~~ ✅ Done — consolidated into `deadline-lib`.

The library API now looks like:
```rust
pub fn download_files(
    manifests_by_root: &HashMap<String, AssetManifest>,
    s3_settings: &S3Settings,
    conflict: FileConflictResolution,
    on_progress: Option<Box<dyn Fn(&DownloadStats) -> bool + Send>>,
) -> Result<DownloadSummary, AttachmentError>
```

The PyO3 adapter then just converts Python dict → Rust types, calls the
function, converts the callback, and returns the result as a Python dict.

---

## Validation

The worker agent has unit tests that mock `deadline.job_attachments.*`.
Final validation:
1. Install our `deadline` package into the worker agent's test venv
2. Run `pytest test/unit/` — all tests that don't mock internals should pass
3. Run `pytest test/e2e/` against a real Deadline farm

---

## Open Questions

1. **`BaseAssetManifest` inheritance** — The worker agent type-hints
   against this base class. PyO3 doesn't support Python inheritance well.
   Options: (a) single `AssetManifest` pyclass that satisfies both, (b)
   protocol/duck-typing (worker agent only accesses `.paths` attribute).

2. **`ProgressTracker` mutability** — Upload path passes a tracker that
   the function mutates. Options: (a) `#[pyclass]` with interior
   mutability, (b) return stats from upload function instead.

3. **boto3 session** — `download_files_from_manifests` currently takes a
   `boto3.session.Session`. Our Rust implementation uses AWS SDK directly.
   The Python shim ignores this parameter (Rust manages its own credentials).
