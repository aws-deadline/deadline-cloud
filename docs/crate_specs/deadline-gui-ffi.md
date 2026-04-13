# deadline-gui-ffi

Shared library crate with C ABI. Exposes Deadline Cloud business logic to
external callers — the Python GUI widgets, DCC submitter plugins, and the
Unreal Engine plugin.

## Status: Risk spike passed

The GUI FFI round-trip spike validated the core architecture on macOS:
- ctypes loads the `.dylib` and calls `extern "C"` functions
- JSON string passing works across the C ABI boundary
- Tokio async runtime runs on a Python worker thread without deadlock
- C function pointer callbacks from Rust to Python work
- Callbacks from a worker thread (simulating QThread) work

See `tests/python/gui_ffi_test.py` for the Python integration tests and
`src/lib.rs` for the Rust FFI functions.

## Architecture

See `../design_docs/rust-rewrite/gui_ffi_architecture.md` for detailed
diagrams of how the FFI layer connects Python, Rust, and Qt.

### How it works (summary)

```
┌─────────────────────────────────────────────────────────────────┐
│  Python Process (Qt main thread)                                │
│                                                                 │
│  ┌──────────────┐    Qt signal     ┌──────────────────────┐    │
│  │  QDialog     │◄────────────────│  QThread (worker)     │    │
│  │  (main       │  (queued conn)  │                       │    │
│  │   thread)    │                 │  ctypes.CDLL(...)     │    │
│  │              │                 │    ↓                   │    │
│  │  Updates     │                 │  lib.deadline_fn()    │    │
│  │  widgets     │                 │    ↓                   │    │
│  └──────────────┘                 │  ┌─────────────────┐  │    │
│                                   │  │ Rust C ABI fn   │  │    │
│                                   │  │ (extern "C")    │  │    │
│                                   │  │                 │  │    │
│                                   │  │ → config read   │  │    │
│                                   │  │ → AWS API call  │  │    │
│                                   │  │ → callback(msg) │──┤    │
│                                   │  │ → return JSON   │  │    │
│                                   │  └─────────────────┘  │    │
│                                   └───────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
```

Key points:
- All Rust FFI calls happen on the Python worker thread, never the Qt
  main thread (which would freeze the UI).
- Callbacks from Rust to Python are synchronous on the worker thread.
  The Python callback emits a Qt signal with `QueuedConnection` to
  safely deliver updates to the main thread.
- JSON strings cross the FFI boundary for complex data. Simple values
  use C types directly.
- Rust owns all allocated strings. Python must call `deadline_free_string`
  to release them.

## C ABI Surface

The shared library exports `extern "C"` functions callable from any language
that supports C FFI (Python via ctypes/cffi, C++, etc.).

Planned functions (refined during Phase 3 implementation):

| Function | Purpose |
|----------|---------|
| `get_setting(name) -> string` | Read a config setting |
| `set_setting(name, value)` | Write a config setting |
| `get_config_file_path() -> string` | Return config file location |
| `login()` | Initiate Deadline Cloud Monitor login |
| `logout()` | Log out |
| `get_auth_status() -> json` | Return credential source, auth status, API availability |
| `list_farms(config_json) -> json` | List farms for GUI dropdowns |
| `list_queues(farm_id) -> json` | List queues for GUI dropdowns |
| `list_storage_profiles(farm_id, queue_id) -> json` | List storage profiles |
| `get_queue_parameters(farm_id, queue_id) -> json` | Get queue parameter definitions |
| `submit_job(scene_data_json, callbacks) -> json` | Full submission: config, upload, API call |

Complex data crosses the FFI boundary as JSON strings. Callbacks (progress
reporting, confirmation prompts) cross as C function pointers.

## Memory Management

Rust allocates strings with `CString::into_raw()`. The caller must free
them by calling `deadline_free_string()`:

```c
const char* result = deadline_get_credentials_source(NULL);
// ... use result ...
deadline_free_string((char*)result);
```

This is the standard C FFI ownership pattern. Python's ctypes handles
this via a wrapper that calls `deadline_free_string` automatically.

## Error Handling

Rust errors are converted to C-compatible error codes + message strings at
the FFI boundary. The Python GUI reads the error code and displays the
message in a `QMessageBox`.

## Callback Design

The submission flow requires Rust to call back into Python for progress
updates, confirmation prompts, and cancellation checks. This uses standard
C function pointers with an opaque `user_data` pointer for context.

### Callback Types

```c
// Progress callback — called during hashing and upload phases.
// metadata_json contains progress percentage, file counts, byte counts.
// Return value: true to continue, false to cancel.
typedef bool (*deadline_progress_callback_t)(
    const char* metadata_json,
    void* user_data
);

// Confirmation callback — called when user input is needed.
// message is the prompt text, default_response is the suggested answer.
// Return value: true to proceed, false to cancel.
typedef bool (*deadline_confirmation_callback_t)(
    const char* message,
    bool default_response,
    void* user_data
);

// Print callback — called when the submission has a message to display.
typedef void (*deadline_print_callback_t)(
    const char* message,
    void* user_data
);
```

### Usage in submit_job

```c
int deadline_submit_job(
    const char* bundle_dir,
    const char* parameters_json,
    const char* config_json,
    deadline_progress_callback_t on_hashing_progress,
    deadline_progress_callback_t on_upload_progress,
    deadline_confirmation_callback_t on_confirmation,
    deadline_print_callback_t on_print,
    void* user_data,
    char** out_job_id,
    char** out_error_message
);
```

The `void* user_data` is an opaque pointer that Rust passes back to every
callback invocation without inspecting it. The Python side uses this to
carry a reference to the Qt worker thread's signal emitters:

```python
# Python side — wraps Qt signals into C function pointers
@ctypes.CFUNCTYPE(ctypes.c_bool, ctypes.c_char_p, ctypes.c_void_p)
def on_hashing_progress(metadata_json, user_data):
    worker.hashing_progress.emit(json.loads(metadata_json))
    return not worker.is_canceled
```

This is a standard, well-understood pattern used by every major FFI-heavy
project. The `user_data` idiom is the C equivalent of a closure — it
captures context without requiring the callback to be a method on an object.

### Thread Safety

The Rust submission function runs on the Python worker thread (a `QThread`).
Callbacks are invoked synchronously from that same thread. The Python
callback implementations emit Qt signals with `QueuedConnection` to safely
deliver updates to the main Qt thread. No cross-thread Rust/Python
interaction occurs — all FFI calls happen on the worker thread.

## Risk Spike: GUI FFI Round-Trip

### Goal

Prove the core FFI architecture works before investing in full Phase 3
implementation. This is the highest-risk technical bet in the migration —
if it fails, the GUI strategy must be revised.

### Sub-tasks

**Sub-task 1: Basic C ABI call (sync, no callback)**
- Expose `deadline_get_credentials_source()` as `extern "C"` returning
  a JSON C string
- Python loads `.dylib` via `ctypes.CDLL` and calls it
- Proves: ctypes loading, C ABI string passing, no symbol conflicts

**Sub-task 2: Async call from Python worker thread**
- Expose `deadline_check_auth_status()` — creates internal tokio runtime,
  blocks on async auth check
- Python calls from a `QThread`, emits Qt signal with result to main thread
- Proves: Rust async runtime inside Python worker thread, Qt event loop
  stays responsive

**Sub-task 3: Callback from Rust to Python**
- Expose `deadline_check_auth_status_with_progress()` — takes a C function
  pointer callback, calls it during operation
- Python provides `@ctypes.CFUNCTYPE` callback that emits Qt signal
- Proves: C function pointer callbacks work, no crash, no deadlock

### Pass criteria (from migration_strategy.md)

1. Loads on macOS (development platform)
2. Callback doesn't crash
3. Qt event loop stays responsive

### Spike FFI surface

```c
// Sub-task 1
const char* deadline_get_credentials_source(const char* config_json);
void deadline_free_string(char* ptr);

// Sub-task 2
const char* deadline_check_auth_status(const char* config_json);

// Sub-task 3
typedef void (*deadline_status_callback_t)(const char* message, void* user_data);
const char* deadline_check_auth_status_with_progress(
    const char* config_json,
    deadline_status_callback_t on_progress,
    void* user_data
);
```

## Consumers

- `gui/` Python widgets — loads via `ctypes.CDLL("libdeadline_gui_ffi.so")`
- DCC submitter plugins — same mechanism
- Unreal Engine plugin — links via C ABI
- `deadline-cli` — for GUI commands, spawns Python which loads this library

## Dependencies

| Crate | Purpose |
|-------|---------|
| `deadline-config` | Config operations |
| `deadline-client` | AWS API calls |
| `deadline-job-bundle` | Bundle loading, parameters |
| `deadline-job-attachments` | Attachment upload/download |
| `deadline-models` | Shared types |
| `deadline-common` | Utilities |

Note: During the spike, only `deadline-config` and `deadline-client` are
needed. The full dependency set is for Phase 3.
