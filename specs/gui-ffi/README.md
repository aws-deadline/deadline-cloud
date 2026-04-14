# deadline-gui-ffi Crate Specifications

Shared library crate with C ABI. Exposes Deadline Cloud business logic to
Python GUI widgets, DCC submitter plugins, and the Unreal Engine plugin.

Consumers: `gui/` Python widgets, DCC submitter plugins (Blender, Maya,
Houdini, etc.), Unreal Engine plugin.

## Document Index

| Document | Description |
|----------|-------------|
| [architecture.md](architecture.md) | Module layout, FFI boundary design, memory ownership, design decisions |

## Status

Implemented: `deadline_free_string`, `deadline_get_credentials_source`,
`deadline_check_auth_status`, `deadline_check_auth_status_with_progress`.

Gaps:
- Full submission flow with progress and confirmation callbacks
- Config read/write functions
- Resource listing functions (farms, queues, storage profiles)
- Login/logout functions
- Deferred until core CLI commands are complete

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
