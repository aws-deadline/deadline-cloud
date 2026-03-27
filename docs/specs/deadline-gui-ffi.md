# deadline-gui-ffi

Shared library crate with C ABI. Exposes Deadline Cloud business logic to
external callers — the Python GUI widgets, DCC submitter plugins, and the
Unreal Engine plugin.

## Status: Not started (Phase 3)

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
