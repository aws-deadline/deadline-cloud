# deadline-python-bindings Crate Specifications

PyO3 extension module exposing Deadline Cloud business logic to Python.
Python imports this as `deadline._native`.

Consumers: `gui/` Python widgets, DCC submitter plugins (Blender, Maya,
Houdini, etc. — indirectly via the `gui/` package).

## Document Index

| Document | Description |
|----------|-------------|
| [architecture.md](architecture.md) | Module layout, PyO3 boundary design, callback patterns |
| [python-package-contract.md](python-package-contract.md) | Python-side API: exports, TelemetryClient, progress callback keys, type conventions |
| [dcc-profiles.md](dcc-profiles.md) | Per-DCC integration profiles |
| [worker-agent-bindings.md](worker-agent-bindings.md) | Plan to expose attachment operations for the Python worker agent |

## Exported Functions

| Function | Category | Wraps |
|----------|----------|-------|
| `get_setting` | Config | `config_file::get_setting` |
| `set_setting` | Config | `config_file::set_setting` + write |
| `read_config` | Config | `config_file::read_config` |
| `get_credentials_source` | Auth | `auth::get_credentials_source` |
| `check_auth_status` | Auth | credentials + auth + API check |
| `check_auth_status_with_progress` | Auth | same + Python callable callback |
| `check_api_available` | Auth | `auth::check_deadline_api_available` |
| `login` | Auth | `auth::login` |
| `logout` | Auth | `auth::logout` |
| `list_farms` | Resources | `api::list_farms` |
| `get_farm` | Resources | `api::get_farm` |
| `list_queues` | Resources | `api::list_queues` |
| `get_queue` | Resources | `api::get_queue` |
| `list_storage_profiles_for_queue` | Resources | `api::list_storage_profiles_for_queue` |
| `get_queue_parameter_definitions` | Resources | `queue_parameters::get_queue_parameter_definitions` |
| `create_job_from_job_bundle` | Submission | `submission::create_job_from_job_bundle` |
| `TelemetryClient` | Telemetry | `telemetry::create_telemetry` (pyclass) |
| `DeadlineOperationError` | Exception | Python exception type |

## Key Design Choices

- **PyO3 with `abi3-py39`** — single `.abi3.so` for Python 3.9+
- **`pythonize` crate** — converts `serde_json::Value` → Python dicts
  directly, no JSON string round-trip
- **Native Python callables** for callbacks — no C function pointers,
  no `user_data` void pointers, no CFUNCTYPE declarations
- **`DeadlineOperationError`** — native Python exception raised directly
  from Rust, same name as the original Python exception
- **`TelemetryClient` pyclass** — Python-owned with automatic cleanup
  on drop (no manual `free_telemetry` call needed)
- **Per-call tokio runtime** — each async function creates a fresh
  `Runtime::new()` and blocks on it. Safe because calls happen on
  Qt worker threads. Cost (~1ms) negligible vs network I/O.

## Build & Development

```bash
# Install into a venv for development
python3 -m venv .venv && source .venv/bin/activate
pip install maturin pytest
maturin develop

# Run tests
pytest pytests/bindings/test_native.py -v
```

## Migration from C ABI (`deadline-gui-ffi`)

The previous architecture used a C ABI shared library
(`libdeadline_gui_ffi.{dylib,so,dll}`) with a Python ctypes wrapper
(`gui/deadline/client/_ffi.py`). This has been replaced by the PyO3
module. Historical docs were removed during the documentation reorganization.

What was removed:
- `crates/deadline-gui-ffi/` — C ABI crate with `extern "C"` functions
- `gui/deadline/client/_ffi.py` — 300 lines of ctypes boilerplate
- JSON serialization on every call (both directions)
- Manual `deadline_free_string` memory management
- 5 CFUNCTYPE callback type declarations
