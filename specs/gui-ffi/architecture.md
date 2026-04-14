# deadline-gui-ffi Architecture

## Crate Position in the Workspace

```
gui/ (Python) ──ctypes.CDLL──► deadline-gui-ffi    ← this crate
DCC plugins ──ctypes.CDLL──► deadline-gui-ffi
```

Depends on: `deadline-config`, `deadline-client`, `deadline-job-bundle`,
`deadline-job-attachments`, `deadline-common`, `deadline-models`.

## Module Layout

```
src/
└── lib.rs    # All extern "C" functions, tokio runtime management,
              #   JSON serialization, CString memory management
```

Single file — the crate is small (spike surface only).

## Exported Functions

### `deadline_free_string(ptr: *mut c_char)`
Free a string previously returned by a `deadline_*` function. Null-safe (no-op on null).

### `deadline_get_credentials_source(config_json: *const c_char) -> *mut c_char`
Returns JSON: `{"credentials_source": "HOST_PROVIDED"}` (or `DEADLINE_CLOUD_MONITOR_LOGIN`,
`NOT_VALID`). Synchronous — no tokio runtime needed.

### `deadline_check_auth_status(config_json: *const c_char) -> *mut c_char`
Returns JSON with `credentials_source`, `auth_status`, `api_available` fields.
Creates an internal tokio runtime and blocks. On runtime creation failure,
returns `{"error": "..."}`.

### `deadline_check_auth_status_with_progress(config_json, on_progress, user_data) -> *mut c_char`
Same as `deadline_check_auth_status` but calls `on_progress` callback with
status messages during the operation:
1. "Checking credentials source..."
2. "Checking authentication status..."
3. "Checking API availability..."
4. "Done"

`on_progress` is `Option<extern "C" fn(*const c_char, *mut c_void)>`. Null
callback is safe (messages are skipped).

### Error Returns
All functions return JSON. Errors are returned as `{"error": "message"}` —
never as null pointers. The Python wrapper checks for the `error` key.

## FFI Boundary Design

**JSON strings cross the boundary for complex data.** Simple values use C
types directly. Structured data serialized as JSON strings. Avoids complex
C structs and makes the interface version-tolerant.

**Rust owns all allocated strings.** Every string returned by a `deadline_*`
function must be freed by calling `deadline_free_string`. Python wrapper
handles this automatically.

**Callbacks use C function pointers with opaque `user_data`.** Standard C
closure pattern. Python wraps Qt signal emitters into `@ctypes.CFUNCTYPE`
callbacks. Callback return values control cancellation (`true` = continue,
`false` = cancel).

**Per-call tokio runtime.** Each FFI function that needs async creates a
fresh `Runtime::new()` and blocks on it. Safe because FFI calls happen on
a dedicated worker thread. Runtime dropped when call returns.

**No global state.** Each call is self-contained — reads config, does work,
returns result. Avoids initialization order issues across different host
processes.

## Key Design Decisions

**Per-call runtime instead of persistent.** A long-lived runtime would
require careful lifecycle management across the FFI boundary. Per-call is
simpler and sufficient — FFI calls are infrequent (user-initiated) and
runtime creation cost (~1ms) is negligible compared to network I/O.

**No panics across FFI boundary.** All public `extern "C"` functions must
catch panics and convert to error returns. Unwinding across FFI is
undefined behavior.

## Threading Model

Qt GUI applications require that only the main thread touches widgets.
All Rust FFI calls happen on a Python `QThread` worker thread:

```
MAIN THREAD (Qt event loop)          WORKER THREAD
════════════════════════════          ═════════════

QDialog                              QThread
  status label  ◄──── Qt signal ──── 1. Call Rust FFI: lib.check_auth()
  updates widget      (queued)       2. Rust creates tokio runtime
                                     3. Rust does async work (STS, API)
                                     4. Rust returns JSON string
                                     5. Python emits Qt signal with result
```

Rust code runs entirely on the worker thread. It never touches Qt widgets
or interacts with the main thread directly. The Python callback emits a
Qt signal, and Qt's queued connection delivers it to the main thread safely.

## Callback Flow (Progress Reporting)

Callbacks are synchronous — Rust calls the C function pointer, Python runs
it on the same worker thread, Python emits a Qt signal (thread-safe), and
returns immediately. No threads are created by the callback mechanism.

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

Rust never frees a string Python is still using. Python never frees
directly — always calls back into Rust. Prevents memory corruption from
mismatched allocators.

## DCC Plugin Flow

DCC plugins (Maya, Blender, etc.) follow the same pattern without the
CLI binary. The plugin is ~150 lines of Python that:
1. Queries the DCC for scene data (render layers, cameras, etc.)
2. Loads the Rust shared library via `ctypes.CDLL`
3. Shows the submission dialog (Python Qt widgets from `gui/`)
4. Calls Rust for every operation (list farms, submit job, etc.)

## Risks and Failure Modes

| Risk | Manifestation |
|------|---------------|
| ctypes can't load .dylib | `OSError: dlopen failed` |
| Symbol name mangling | `AttributeError: function not found` |
| Tokio runtime conflicts with Qt | Deadlock or hang |
| Callback stack corruption | `SIGSEGV` |
| Memory leak from unfree'd strings | Gradual memory growth |
