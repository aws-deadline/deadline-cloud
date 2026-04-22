# deadline-gui-ffi Architecture

## Crate Position in the Workspace

```
gui/_ffi.py (Python) ──ctypes.CDLL──► deadline-gui-ffi    ← this crate
                                          │
                                          ├── deadline-config
                                          ├── deadline-api
                                          ├── deadline-job-bundle
                                          └── deadline-job-attachments
```

DCC plugins don't load this library directly. They import
`deadline.client.ui` from the `gui/` Python package, which internally
uses `_ffi.py` to call this library.

## Module Layout

```
src/
└── lib.rs    # All extern "C" functions, helpers (read_c_str, read_config_at,
              #   make_runtime, json_to_ptr, error_to_ptr), JSON serialization,
              #   CString memory management
```

## Exported Functions

### Memory management

- **`deadline_free_string(ptr)`** — Free a string returned by any `deadline_*` function. Null-safe.

### Spike (auth status)

- **`deadline_get_credentials_source(config_path)`** → `{"credentials_source": "..."}`
- **`deadline_check_auth_status(config_path)`** → `{"credentials_source", "auth_status", "api_available"}`
- **`deadline_check_auth_status_with_progress(config_path, on_progress, user_data)`** → same + callbacks

### Batch A: Config

- **`deadline_read_config(config_path)`** → `{"config": "..."}` (INI text)
- **`deadline_get_setting(setting_name, config_path)`** → `{"value": "..."}` or `{"error": "..."}`
- **`deadline_set_setting(setting_name, value, config_path)`** → `{"success": true}` or `{"error": "..."}`

### Batch B: Resource listing

- **`deadline_list_farms(config_path)`** → `{"farms": [...]}` or `{"error": "..."}`
- **`deadline_list_queues(farm_id, config_path)`** → `{"queues": [...]}` or `{"error": "..."}`
- **`deadline_list_storage_profiles_for_queue(farm_id, queue_id, config_path)`** → `{"storageProfiles": [...]}`
- **`deadline_get_queue_parameter_definitions(farm_id, queue_id, config_path)`** → `{"parameters": [...]}`

### Batch C: Auth actions

- **`deadline_check_api_available(config_path)`** → `{"api_available": bool}`
- **`deadline_login(config_path)`** → `{"success": "..."}` or `{"error": "..."}`
- **`deadline_logout(config_path)`** → `{"success": "..."}` or `{"error": "..."}`

### Batch D: Submission (not yet implemented)

- **`deadline_create_job_from_job_bundle(...)`** — Complex: 5 callback types for progress, confirmation, cancellation.

### Batch E: Telemetry (not yet implemented)

- **`deadline_init_telemetry(...)`**
- **`deadline_record_telemetry_event(...)`**

### Error convention

All functions return JSON strings. Errors are `{"error": "message"}` —
never null pointers. The `_ffi.py` wrapper checks for the `error` key
and raises `DeadlineOperationError`.

### Null argument convention

All `config_path` parameters accept null, meaning "use the default config
file path" (`~/.deadline/config` or `DEADLINE_CONFIG_FILE_PATH` env var).
Required parameters (like `farm_id`) return `{"error": "..."}` when null.

## FFI Boundary Design

**JSON strings cross the boundary for all complex data.** Avoids complex
C structs and makes the interface version-tolerant. Adding a new field to
a response doesn't break existing callers.

**Rust owns all allocated strings.** Every string returned by a `deadline_*`
function must be freed by calling `deadline_free_string`. The `_ffi.py`
wrapper handles this automatically.

**Callbacks use C function pointers with opaque `user_data`.** Standard C
closure pattern. Python wraps Qt signal emitters into `@ctypes.CFUNCTYPE`
callbacks.

**Per-call tokio runtime.** Each async FFI function creates a fresh
`Runtime::new()` and blocks on it. Safe because FFI calls happen on a
dedicated worker thread. Runtime dropped when call returns. Cost (~1ms)
is negligible compared to network I/O.

**No global state.** Each call is self-contained — reads config from disk,
does work, returns result. Avoids initialization order issues across
different DCC host processes.

## Python Wrapper Layer (`gui/_ffi.py`)

The `_ffi.py` module is the only Python file that touches ctypes. All
other Python code calls it via normal Python methods:

```python
class DeadlineFFI:
    """Thin ctypes wrapper around libdeadline_gui_ffi."""

    def __init__(self):
        self._lib = ctypes.CDLL(self._find_library())
        self._declare_signatures()

    def list_farms(self, config_path=None):
        result = self._lib.deadline_list_farms(
            config_path.encode() if config_path else None
        )
        try:
            data = json.loads(ctypes.string_at(result))
        finally:
            self._lib.deadline_free_string(result)
        if "error" in data:
            raise DeadlineOperationError(data["error"])
        return data

    # ... same pattern for all functions
```

The Python Qt controllers (`_deadline_controller.py`, etc.) call
`self._ffi.list_farms()` instead of `api.list_farms()`. The change is
mechanical — same data shapes, same error handling, just a different
call target.

## Threading Model

Qt GUI applications require that only the main thread touches widgets.
All Rust FFI calls happen on a Python `QThread` worker thread:

```
MAIN THREAD (Qt event loop)          WORKER THREAD
════════════════════════════          ═════════════

QDialog                              QThread
  status label  ◄──── Qt signal ──── 1. Call _ffi.check_auth_status()
  updates widget      (queued)       2. _ffi.py calls ctypes → Rust
                                     3. Rust creates tokio runtime
                                     4. Rust does async work (STS, API)
                                     5. Rust returns JSON string
                                     6. _ffi.py parses JSON, frees string
                                     7. Python emits Qt signal with result
```

Rust code runs entirely on the worker thread. It never touches Qt widgets
or interacts with the main thread directly.

## Callback Flow (Progress Reporting)

Callbacks are synchronous — Rust calls the C function pointer, Python runs
it on the same worker thread, Python emits a Qt signal (thread-safe), and
returns immediately.

```
Rust calls on_progress("Checking credentials...", user_data)
  → Python callback runs on worker thread
  → Python emits self.progress_signal.emit(message)
  → Qt delivers signal to main thread via queued connection
  → Main thread updates status label
  → Python callback returns
  → Rust continues
```

## Memory Ownership

```
Rust allocates  →  Python reads  →  Python calls free

CString::into_raw()    ctypes.string_at()    deadline_free_string()
returns *mut c_char    copies to Python str   Rust deallocates via CString::from_raw()
```

The `_ffi.py` wrapper uses try/finally to ensure `deadline_free_string`
is always called, even if JSON parsing fails.

## Packaging

The shared library ships inside the installer's `_internal/` directory:

```
DeadlineClient/
├── deadline                           # Rust CLI binary
└── _internal/
    ├── libdeadline_gui_ffi.dylib      # This crate's output
    └── deadline/client/_ffi.py        # Locates and loads the .dylib
```

`_ffi.py` finds the library relative to its own `__file__` path. No
hardcoded paths, no environment variables needed.

## Risks and Failure Modes

| Risk | Manifestation |
|------|---------------|
| ctypes can't load .dylib | `OSError: dlopen failed` |
| Symbol name mangling | `AttributeError: function not found` |
| Tokio runtime conflicts with Qt | Deadlock or hang |
| Callback stack corruption | `SIGSEGV` |
| Memory leak from unfree'd strings | Gradual memory growth |
| Library not found at runtime | `_ffi.py` raises clear error with expected path |
