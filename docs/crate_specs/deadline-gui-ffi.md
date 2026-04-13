# deadline-gui-ffi

Shared library crate with C ABI. Exposes Deadline Cloud business logic to
Python GUI widgets, DCC submitter plugins, and the Unreal Engine plugin.

## Role in the System

The FFI boundary between Rust business logic and external callers. Every
GUI operation (config read/write, auth checks, farm/queue listing, job
submission) crosses this boundary as a C function call. The shared library
is loaded once per process via `ctypes.CDLL` (Python) or direct C linkage
(Unreal).

Consumers: `gui/` Python widgets, DCC submitter plugins (Blender, Maya,
Houdini, etc.), Unreal Engine plugin.

## Key Concepts

**All FFI calls happen on a worker thread, never the UI thread.** The
Python GUI runs FFI calls on a `QThread`. Rust functions that need async
(API calls) create an internal tokio runtime and block on it. The Qt main
thread stays responsive because it never enters Rust code directly.

**JSON strings cross the boundary for complex data.** Simple values use C
types directly. Structured data (auth status, farm lists, submission
parameters) is serialized as JSON strings. This avoids defining complex C
structs and makes the interface version-tolerant — new fields can be added
without breaking the ABI.

**Rust owns all allocated strings.** Every string returned by a
`deadline_*` function must be freed by calling `deadline_free_string`.
The Python wrapper handles this automatically. Forgetting to free is a
memory leak, not a crash — but it accumulates over long-lived GUI
sessions.

**Callbacks use C function pointers with opaque `user_data`.** This is
the standard C closure pattern. The Python side wraps Qt signal emitters
into `@ctypes.CFUNCTYPE` callbacks. Rust invokes the callback
synchronously on the worker thread; the Python callback emits a Qt signal
with `QueuedConnection` to safely deliver updates to the main thread.

## Behavior & Contracts

**Memory ownership:** Rust allocates via `CString::into_raw()`. Caller
frees via `deadline_free_string()`. Passing null to free is safe (no-op).
Passing a pointer not from a `deadline_*` function is undefined behavior.

**Error reporting:** Errors are returned as JSON strings with an `error`
field. The Python GUI reads this and displays it in a `QMessageBox`. No
exceptions cross the FFI boundary — Rust panics are caught at the
boundary (unwinding across FFI is undefined behavior).

**Callback return values control cancellation.** Progress callbacks return
`bool` — `true` to continue, `false` to cancel. The Rust side checks
this after each progress report and aborts the operation if `false`.

**Tokio runtime is per-call.** Each FFI function that needs async creates
a fresh `Runtime::new()` and blocks on it. This is safe because FFI calls
happen on a dedicated worker thread. The runtime is dropped when the call
returns — no persistent async state between calls.

## Design Decisions

**Per-call runtime instead of a persistent one.** A long-lived runtime
would require careful lifecycle management across the FFI boundary
(initialization, shutdown, error recovery). Per-call is simpler and
sufficient — FFI calls are infrequent (user-initiated) and the runtime
creation cost (~1ms) is negligible compared to network I/O.

**JSON over structured C types.** Defining C structs for every response
type would create a brittle ABI that breaks when fields are added. JSON
strings are self-describing and forward-compatible. The serialization cost
is negligible for the data sizes involved (farm lists, auth status).

**No global state in the shared library.** Each call is self-contained —
reads config, does work, returns result. This avoids initialization order
issues when the library is loaded by different host processes (Python,
Blender, Maya, Unreal) with different lifecycle expectations.

## Gotchas & Constraints

- The shared library must not panic across the FFI boundary. All public
  `extern "C"` functions must catch panics (via `catch_unwind` or
  equivalent) and convert them to error returns.

- DCC plugins load this library into their own Python process. The library
  must not conflict with other shared libraries the DCC loads. In
  practice, this means avoiding global state that could collide (no
  static mutable globals without synchronization).

- The library name varies by platform: `libdeadline_gui_ffi.dylib`
  (macOS), `libdeadline_gui_ffi.so` (Linux), `deadline_gui_ffi.dll`
  (Windows). Python's `ctypes.CDLL` handles this, but the path must be
  correct.

- `user_data` pointers are opaque to Rust. Never dereference them — just
  pass them back to the callback. The Python side manages the lifetime of
  whatever `user_data` points to.

## Status & Gaps

Implemented: `deadline_free_string`, `deadline_get_credentials_source`,
`deadline_check_auth_status`, `deadline_check_auth_status_with_progress`
(the risk spike surface).

Gaps:
- Full submission flow (`deadline_submit_job` with progress and
  confirmation callbacks)
- Config read/write functions
- Resource listing functions (farms, queues, storage profiles)
- Login/logout functions
- All of these are deferred until core CLI commands are complete (work
  items #16+)
