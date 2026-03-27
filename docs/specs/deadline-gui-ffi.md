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
