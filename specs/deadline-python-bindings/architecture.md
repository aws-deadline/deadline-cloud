# deadline-python-bindings Architecture

## Crate Position in the Workspace

```
gui/ (Python) ── import deadline._native ──► deadline-python-bindings
                                                 │
                                                 ├── deadline-lib
                                                 │   ├── config
                                                 │   ├── api
                                                 │   ├── bundle
```

DCC plugins don't load this module directly. They import
`deadline.client.ui` from the `gui/` Python package, which internally
imports `deadline._native`.

## Module Layout

```
src/
├── lib.rs          # #[pymodule] registration, shared helpers
├── config.rs       # get_setting, set_setting, read_config
├── auth.rs         # credentials, auth status, login/logout
├── resources.rs    # list/get farms, queues, storage profiles, parameters
├── submission.rs   # create_job_from_job_bundle with callbacks
└── telemetry.rs    # TelemetryClient pyclass
```

## PyO3 Boundary Design

**Native Python types cross the boundary.** PyO3 converts Rust types to
Python objects directly — no JSON serialization, no manual memory
management.

**Typed SDK → extract needed fields → pythonize to Python dict.** FFI
functions call the SDK directly, extract only the fields the GUI needs,
and return them as Python dicts via `pythonize`:

```rust
// List: SDK paginator → extract fields → pythonize
let pages = collect_paginated(dl.list_farms().into_paginator().send()).await?;
let farms: Vec<Value> = pages.iter().flat_map(|p| p.farms())
    .map(|f| json!({"farmId": f.farm_id(), "displayName": f.display_name()}))
    .collect();
pythonize(py, &json!({"farms": farms}))

// Get: SDK output → response struct → serialize → pythonize
let output = dl.get_queue().farm_id(f).queue_id(q).send().await?;
let resp = QueueResponse::from(output);
pythonize(py, &serde_json::to_value(&resp)?)
```

The GUI only consumes the specific fields it renders (e.g. `farmId`,
`displayName` for a dropdown). The FFI layer extracts exactly those
fields from typed SDK output — no full response dump, no raw JSON
passthrough.

**Python callables for callbacks.** Submission progress and confirmation
callbacks accept `PyObject` (any Python callable). PyO3 calls them via
`callback.call1(py, (args,))`. No C function pointers or `user_data`.

**`DeadlineOperationError` exception.** Created via
`pyo3::create_exception!`. Rust functions return `PyResult<T>` and
convert errors with `.map_err(|e| DeadlineOperationError::new_err(...))`.

**`TelemetryClient` pyclass.** Wraps the Rust `TelemetryClient` with
Python ownership. `Drop` flushes pending events. Explicit `close()`
method for eager cleanup.

## Threading Model

Qt GUI applications require that only the main thread touches widgets.
All Rust calls happen on a Python `QThread` worker thread:

```
MAIN THREAD (Qt event loop)          WORKER THREAD
════════════════════════════          ═════════════

QDialog                              QThread
  status label  ◄──── Qt signal ──── 1. Call deadline._native.check_auth_status()
  updates widget      (queued)       2. PyO3 converts args, calls Rust
                                     3. Rust creates tokio runtime
                                     4. Rust does async work (STS, API)
                                     5. pythonize converts result → Python dict
                                     6. Python emits Qt signal with result
```

## Callback Flow (Submission)

```python
from deadline._native import create_job_from_job_bundle

result = create_job_from_job_bundle(
    params,
    on_print=lambda msg: status_signal.emit(msg),
    on_hashing_progress=lambda meta: progress_signal.emit(meta),
)
```

Rust calls the Python callable directly via `PyObject::call1()`.
The callable emits a Qt signal (thread-safe), which the main thread
receives via queued connection.

## Packaging

Built by maturin into a wheel. The `.abi3.so` is placed inside the
`deadline/` namespace package alongside the Python GUI code:

```
deadline/
├── _native.abi3.so     # This crate's output
├── client/
│   ├── ui/             # Python Qt widgets
│   ├── config/         # Python config wrapper
│   └── ...
```

`maturin develop` places the `.abi3.so` directly in `gui/deadline/`
for editable development. `maturin build` packages it into a wheel.
