# GUI FFI Architecture (Archived)

> Historical spike document. Current spec: [specs/gui-ffi/architecture.md](../gui-ffi/architecture.md).

How the Rust shared library (`deadline-gui-ffi`) connects Python GUI
widgets to Rust business logic. This document covers the interaction
model, threading, memory ownership, and callback flow.

## The Big Picture

Today, the Python GUI does everything in Python — reads config, calls
AWS APIs, uploads attachments, all via `import deadline.client`. After
migration, the Python GUI becomes a thin presentation layer that calls
Rust for every operation:

```
┌─────────────────────────────────────────────────────────────────────┐
│                        BEFORE (all Python)                          │
│                                                                     │
│  Python GUI widgets ──import──► deadline.client (Python library)     │
│                                    │                                │
│                                    ├── config (Python)              │
│                                    ├── API calls (boto3)            │
│                                    ├── job bundle (Python)          │
│                                    └── attachments (Python)         │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│                        AFTER (Python + Rust)                        │
│                                                                     │
│  Python GUI widgets ──ctypes──► deadline-gui-ffi.dylib (Rust)       │
│  (~15 files, pure                  │                                │
│   layout, no logic)                ├── deadline-config (Rust)       │
│                                    ├── deadline-api (Rust/SDK)   │
│                                    ├── deadline-job-bundle (Rust)   │
│                                    └── deadline-job-attachments     │
│                                         (Rust)                      │
└─────────────────────────────────────────────────────────────────────┘
```

The Python GUI files contain zero business logic. Every button click,
dropdown load, and form submission calls into Rust.

## What is FFI?

FFI = Foreign Function Interface. It's how code written in one language
calls code written in another. The universal lingua franca is the C ABI
(Application Binary Interface) — every language can call C functions.

Rust can export functions with `extern "C"` that look exactly like C
functions to the outside world. Python can call C functions via its
built-in `ctypes` module. This is the bridge:

```
Python                          Rust
──────                          ────
ctypes.CDLL("lib.dylib")  ──►  #[no_mangle]
lib.my_function(args)      ──►  pub extern "C" fn my_function(args)
```

No special bindings generator needed. No PyO3. No SWIG. Just standard
C calling conventions that both languages understand natively.

## Threading Model

This is the most important part to understand. Qt GUI applications have
a strict rule: **only the main thread can touch widgets**. If you call
an AWS API on the main thread, the UI freezes until it returns. So all
I/O must happen on a background thread.

Here's how the threads interact:

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Python Process                              │
│                                                                     │
│  MAIN THREAD (Qt event loop)          WORKER THREAD                 │
│  ════════════════════════════          ═════════════                 │
│                                                                     │
│  ┌──────────────────────┐             ┌──────────────────────┐     │
│  │  QDialog             │             │  QThread / QRunnable  │     │
│  │  ┌────────────────┐  │             │                      │     │
│  │  │ "Checking..."  │  │  ◄─signal── │  1. Call Rust FFI:   │     │
│  │  │ status label   │  │  (queued)   │     lib.check_auth() │     │
│  │  ├────────────────┤  │             │         │            │     │
│  │  │ ● Authenticated│  │             │         ▼            │     │
│  │  │ ○ API Available│  │             │  ┌──────────────┐    │     │
│  │  └────────────────┘  │             │  │ RUST CODE    │    │     │
│  │                      │             │  │              │    │     │
│  │  Receives signal,    │             │  │ STS call     │    │     │
│  │  updates widgets     │             │  │ ListFarms    │    │     │
│  │                      │             │  │              │    │     │
│  └──────────────────────┘             │  │ Returns JSON │    │     │
│                                       │  └──────────────┘    │     │
│                                       │         │            │     │
│                                       │  2. Emit Qt signal   │     │
│                                       │     with result      │     │
│                                       └──────────────────────┘     │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

The critical insight: **Rust code runs entirely on the worker thread**.
It never touches Qt widgets. It never interacts with the main thread
directly. The Python callback on the worker thread emits a Qt signal,
and Qt's event loop delivers it to the main thread safely.

## Data Flow: Auth Status Check

This is the specific flow the spike validates. Here's every step:

```
Step 1: User opens config dialog
        Main thread creates QDialog, starts worker thread

Step 2: Worker thread calls Rust
        ┌─────────────────────────────────────────────────┐
        │  Python (worker thread)                         │
        │                                                 │
        │  lib = ctypes.CDLL("libdeadline_gui_ffi.dylib") │
        │  result = lib.deadline_check_auth_status(None)  │
        │                          │                      │
        └──────────────────────────┼──────────────────────┘
                                   │
                    ═══════════════╪═══════════════
                     C ABI BOUNDARY (extern "C")
                    ═══════════════╪═══════════════
                                   │
        ┌──────────────────────────┼──────────────────────┐
        │  Rust (same worker thread)                      │
        │                          ▼                      │
        │  1. Create tokio runtime (thread-local)         │
        │  2. runtime.block_on(async {                    │
        │       let source = get_credentials_source();    │
        │       let status = check_auth_status().await;   │
        │       let api_ok = check_api_available().await; │
        │       json!({"source": source,                  │
        │              "status": status,                  │
        │              "api_available": api_ok})           │
        │     })                                          │
        │  3. Return JSON as C string                     │
        └─────────────────────────────────────────────────┘

Step 3: Python receives result, emits signal
        ┌─────────────────────────────────────────────────┐
        │  Python (worker thread)                         │
        │                                                 │
        │  json_str = ctypes.string_at(result)            │
        │  data = json.loads(json_str)                    │
        │  lib.deadline_free_string(result)               │
        │                                                 │
        │  self.auth_status_changed.emit(data)  ──────────┤
        │                                        signal   │
        └─────────────────────────────────────────────────┘
                                                    │
                                          Qt queued │
                                          connection│
                                                    ▼
        ┌─────────────────────────────────────────────────┐
        │  Python (main thread)                           │
        │                                                 │
        │  def on_auth_status_changed(self, data):        │
        │      self.status_label.setText(data["status"])   │
        │      self.api_indicator.setChecked(              │
        │          data["api_available"])                  │
        └─────────────────────────────────────────────────┘
```

## Data Flow: Callback (Progress Reporting)

When Rust needs to report progress back to Python during a long
operation (like file upload), it uses C function pointers:

```
        ┌─────────────────────────────────────────────────┐
        │  Python (worker thread)                         │
        │                                                 │
        │  # Define callback as C function pointer        │
        │  @ctypes.CFUNCTYPE(None, c_char_p, c_void_p)   │
        │  def on_progress(message, user_data):           │
        │      self.progress_signal.emit(message)         │
        │                                                 │
        │  # Pass callback to Rust                        │
        │  lib.deadline_check_auth_status_with_progress(  │
        │      config, on_progress, None)                 │
        │              │                                  │
        └──────────────┼──────────────────────────────────┘
                       │
        ═══════════════╪═══════════════
         C ABI BOUNDARY
        ═══════════════╪═══════════════
                       │
        ┌──────────────┼──────────────────────────────────┐
        │  Rust         ▼                                 │
        │                                                 │
        │  // Rust calls the Python callback              │
        │  on_progress("Checking credentials...", data);  │
        │       │                                         │
        │       │  ◄── This calls back into Python!       │
        │       │      Same worker thread. Synchronous.   │
        │       │      Python emits Qt signal inside.     │
        │       │      Returns immediately.               │
        │       ▼                                         │
        │  on_progress("Checking API access...", data);   │
        │       │                                         │
        │       ▼                                         │
        │  // Done, return final result                   │
        │  return json_result;                            │
        └─────────────────────────────────────────────────┘
```

The callback is synchronous — Rust calls it, Python runs it, Python
returns, Rust continues. No threads are created. No async. The Python
callback just emits a Qt signal (which is thread-safe) and returns.

## Memory Ownership

Strings crossing the FFI boundary follow a simple rule:

```
Rust allocates  ──►  Python reads  ──►  Python calls free

  CString::into_raw()    ctypes.string_at()    deadline_free_string()
  returns *const c_char  copies to Python str   Rust deallocates
```

Rust never frees a string that Python is still using. Python never
frees a string directly — it always calls back into Rust to free it.
This prevents memory corruption from mismatched allocators.

## Platform Differences

The shared library has a different filename on each platform:

| Platform | Filename | Loaded by |
|----------|----------|-----------|
| macOS | `libdeadline_gui_ffi.dylib` | `ctypes.CDLL("libdeadline_gui_ffi.dylib")` |
| Linux | `libdeadline_gui_ffi.so` | `ctypes.CDLL("libdeadline_gui_ffi.so")` |
| Windows | `deadline_gui_ffi.dll` | `ctypes.CDLL("deadline_gui_ffi.dll")` |

The spike validates macOS. Linux and Windows are validated in CI.

## Where This Fits in the Crate Graph

```
deadline-cli (binary)
│
├── [for headless commands] ──► deadline-api, deadline-config, etc.
│
└── [for GUI commands] ──► spawns Python process
                                    │
                                    ▼
                           Python loads gui/ widgets
                           Python loads deadline-gui-ffi.dylib via ctypes
                                    │
                                    ▼
                           deadline-gui-ffi (shared library)
                           ├── deadline-config
                           ├── deadline-api
                           ├── deadline-job-bundle
                           ├── deadline-job-attachments
                           ├── deadline-models
                           └── deadline-common
```

For headless CLI commands (`deadline farm list`), the Rust binary calls
library crates directly — no Python, no FFI.

For GUI commands (`deadline config gui`), the Rust binary spawns a
Python process. Python loads the Qt widgets and the Rust shared library.
All business logic still runs in Rust, just via the FFI bridge instead
of direct function calls.

## DCC Plugin Flow

DCC plugins (Maya, Blender, etc.) follow the same pattern but without
the CLI binary:

```
┌─────────────────────────────────────────────────────────────────┐
│  DCC Application (e.g., Maya)                                   │
│                                                                 │
│  Maya's Python ──► Plugin script (~150 lines)                   │
│                       │                                         │
│                       ├── maya.cmds queries (scene data)        │
│                       │                                         │
│                       ├── ctypes.CDLL("libdeadline_gui_ffi")    │
│                       │       │                                 │
│                       │       ├── lib.list_farms()              │
│                       │       ├── lib.list_queues()             │
│                       │       └── lib.submit_job(scene_data)    │
│                       │                                         │
│                       └── gui/ widgets (Qt dialogs)             │
└─────────────────────────────────────────────────────────────────┘
```

The DCC plugin is ~150 lines of Python that:
1. Queries the DCC for scene data (render layers, cameras, etc.)
2. Loads the Rust shared library
3. Shows the submission dialog (Python Qt widgets)
4. Calls Rust for every operation (list farms, submit job, etc.)

## Risk: What Could Go Wrong

These are the specific failure modes the spike is designed to detect:

| Risk | How it would manifest | Spike sub-task that catches it |
|------|----------------------|-------------------------------|
| ctypes can't load the .dylib | `OSError: dlopen failed` | Sub-task 1 |
| Symbol name mangling | `AttributeError: function not found` | Sub-task 1 |
| String encoding mismatch | Garbled JSON or crash | Sub-task 1 |
| Tokio runtime conflicts with Qt event loop | Deadlock or hang | Sub-task 2 |
| Async runtime on worker thread panics | `SIGABRT` | Sub-task 2 |
| Qt freezes during Rust call | UI unresponsive | Sub-task 2 |
| Callback crashes (stack corruption) | `SIGSEGV` | Sub-task 3 |
| Callback deadlocks with Qt signals | Hang | Sub-task 3 |
| Memory leak from unfree'd strings | Gradual memory growth | Sub-task 1 (free_string test) |
