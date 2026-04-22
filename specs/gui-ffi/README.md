# deadline-gui-ffi Crate Specifications

Shared library crate with C ABI. Exposes Deadline Cloud business logic to
Python GUI widgets and DCC submitter plugins via ctypes.

Consumers: `gui/` Python widgets (via `_ffi.py`), DCC submitter plugins
(Blender, Maya, Houdini, etc. — indirectly via the `gui/` package).

## Document Index

| Document | Description |
|----------|-------------|
| [architecture.md](architecture.md) | Module layout, FFI boundary design, memory ownership, threading, callbacks |
| [dcc-profiles.md](dcc-profiles.md) | Per-DCC integration profiles: what each submitter imports, how it shows GUI, what it needs from the Rust migration |

## Status

### Implemented (Batches A-C)

| Function | Batch | Wraps |
|----------|-------|-------|
| `deadline_free_string` | Spike | Memory management |
| `deadline_get_credentials_source` | Spike | `auth::get_credentials_source` |
| `deadline_check_auth_status` | Spike | `auth::check_authentication_status` |
| `deadline_check_auth_status_with_progress` | Spike | Same + callbacks |
| `deadline_read_config` | A | `config_file::read_config_from` |
| `deadline_get_setting` | A | `config_file::get_setting_with_config` |
| `deadline_set_setting` | A | `config_file::set_setting_in_config` + write |
| `deadline_list_farms` | B | `api::list_farms` |
| `deadline_list_queues` | B | `api::list_queues` |
| `deadline_list_storage_profiles_for_queue` | B | `api::list_storage_profiles_for_queue` |
| `deadline_get_queue_parameter_definitions` | B | `queue_parameters::get_queue_parameter_definitions` |
| `deadline_check_api_available` | C | `auth::check_deadline_api_available` |
| `deadline_login` | C | `auth::login` |
| `deadline_logout` | C | `auth::logout` |
| `deadline_create_job_from_job_bundle` | D | `submission::create_job_from_job_bundle` |
| `deadline_init_telemetry` | E | `telemetry::create_telemetry` |
| `deadline_record_telemetry_event` | E | `TelemetryClient::record_event` |
| `deadline_free_telemetry` | E | `drop(Box<TelemetryClient>)` |

### Not yet implemented

| Function | Batch | Wraps | Complexity |
|----------|-------|-------|------------|
| — | — | — | All batches implemented |

## Migration Phases

This crate is one piece of the full GUI migration. See `specs/progress.md`
items #16a-16f for the complete plan. The phases are:

1. **#16a** ✅ FFI functions for config, resource listing, auth
2. **#16b** — FFI functions for submission (with callbacks) and telemetry
3. **#16c** — Python `_ffi.py` wrapper (ctypes boilerplate, in `gui/`)
4. **#16d** — Port Python Qt code into `gui/` (rewire ~6 files to use FFI)
5. **#16e** — Python packaging for the `gui/` directory
6. **#16f** — DCC submitter dependency switchover

## Gotchas & Constraints

- The shared library must not panic across the FFI boundary. All public
  `extern "C"` functions must catch panics and convert to error returns.
  Unwinding across FFI is undefined behavior.

- DCC plugins load this library into their own Python process. The library
  must not conflict with other shared libraries the DCC loads — no static
  mutable globals without synchronization.

- The library name varies by platform: `libdeadline_gui_ffi.dylib` (macOS),
  `libdeadline_gui_ffi.so` (Linux), `deadline_gui_ffi.dll` (Windows).

- `user_data` pointers are opaque to Rust. Never dereference them — just
  pass them back to the callback.

- The shared library ships inside `_internal/` alongside the Python Qt
  code. The `_ffi.py` wrapper locates it relative to its own path.
