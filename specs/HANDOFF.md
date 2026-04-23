# Handoff

Current in-flight work. Read this at the start of every session before
consulting the Work Items table in `specs/progress.md`.

## Active Work Item

None. Pick next from `specs/progress.md`.

## Step Status — #16c (completed)

**Steps 1-7: ✅ All complete**

Audit findings (Step 5):
1. (Bug, fixed) `_encode('')` returned None — empty strings now encode correctly
2. (Improvement, fixed) `_callbacks.clear()` after each callback-using call
3. (Improvement, fixed) Removed unused imports

28 Python tests pass in 0.5s against stub server. 1165 Rust tests pass.

## Step Status — #16b (previous)

**Step 1 (Study Python): ✅ Complete (gate passed)**

Studied:
- Python `_submit_job_bundle.py`: `create_job_from_job_bundle` signature
  (20+ parameters, 5 callback types, hooks integration, telemetry events)
- Python `_telemetry.py`: `TelemetryClient`, `record_event`,
  `get_deadline_cloud_library_telemetry_client`, `record_hashing_summary`,
  `record_upload_summary`
- Python `_job_submission_worker.py`: QThread pattern — 5 callbacks wired
  to Qt signals, `_confirmation_event` threading.Event for blocking
  confirmation, cancellation flag
- Rust `submission.rs`: `SubmitJobParams` struct, `create_job_from_job_bundle`
  — already has `print_callback`, `hashing_progress_callback`,
  `upload_progress_callback`, `continue_callback`. Missing:
  `interactive_confirmation_callback(message, default) -> bool`
- Rust `telemetry.rs`: `TelemetryClient` with `record_event`,
  `create_telemetry`, `create_telemetry_with_metadata`. Background
  thread + mpsc. Already fully functional.
- Rust `gui-ffi/lib.rs`: 14 existing `extern "C"` functions, established
  patterns (JSON strings, `read_c_str`, `make_runtime`, `json_to_ptr`,
  `error_to_ptr`, `StatusCallback` type)
- `specs/gui-ffi/architecture.md`: callback flow, threading model,
  memory ownership
- `specs/gui-ffi/dcc-profiles.md`: all 9 DCCs, Unreal needs
  `create_job_from_job_bundle` + telemetry without Qt

### Key findings

1. **Rust submission is missing `interactive_confirmation_callback`.**
   Python has `interactive_confirmation_callback(message: str, default: bool) -> bool`
   used for: (a) unknown asset path warnings, (b) hooks confirmation,
   (c) GUI auto_accept override. Rust's `continue_callback: Fn() -> bool`
   is used for both unknown-path confirmation AND create_job polling —
   it can't pass a message or default. Need to either:
   - (a) Add `interactive_confirmation_callback` to `SubmitJobParams` in
     `deadline-job-bundle`, or
   - (b) Handle it entirely in the FFI layer by wrapping the Rust
     `continue_callback` with the C callback.

   **Decision: Option (a)** — add to `SubmitJobParams`. The Rust library
   should support the same callback contract as Python for non-FFI callers
   too (MCP server, future Rust GUI).

2. **Telemetry FFI is straightforward.** Rust `TelemetryClient` already
   exists with `create_telemetry()` and `record_event()`. The FFI just
   needs to:
   - `deadline_init_telemetry(config_path)` → create + store a handle
   - `deadline_record_telemetry_event(handle, event_type, event_details_json)`
   - Handle: opaque pointer to a `Box<TelemetryClient>`

3. **5 C callback types needed for submission FFI:**
   - `PrintCallback: fn(message: *const c_char, user_data: *mut c_void)`
   - `ProgressCallback: fn(metadata_json: *const c_char, user_data: *mut c_void) -> bool`
     (shared by hashing + upload — same signature, return false to cancel)
   - `ConfirmationCallback: fn(message: *const c_char, default_response: bool, user_data: *mut c_void) -> bool`
   - `ContinueCallback: fn(user_data: *mut c_void) -> bool`
     (for create_job polling — no arguments, just "should I keep waiting?")

4. **`from_gui` flag.** Python's `create_job_from_job_bundle` has
   `from_gui: bool = False` which affects telemetry `usage_mode` and
   auto_accept behavior. Rust's `SubmitJobParams` doesn't have this.
   The FFI should pass `from_gui=true` since it's always called from GUI.

5. **Hooks integration.** Python's submission loads hooks from
   `DEADLINE_HOOKS_DIR` and bundle `hooks.yaml`. Rust's
   `create_job_from_job_bundle` doesn't implement hooks yet (#18 is
   Not started). The FFI can defer hooks — DCC submitters don't use
   hooks today. The `interactive_confirmation_callback` is still needed
   for the asset path warning flow.

6. **Progress metadata serialization.** The `ProgressReportMetadata`
   struct needs to cross the FFI as JSON. Fields: `status`, `progress`,
   `transfer_rate`, `progress_message`, `processed_files`.

**Step 2 (Write Tests): ✅ Complete**

Wrote 16 new tests (8 submission, 8 telemetry). Audited for redundancy
(removed 1 duplicate) and missing coverage (added 2 edge cases).
All fail to compile before implementation — confirmed Red phase.

**Step 3 (Implement): ✅ Complete**

Implemented 4 new `extern "C"` FFI functions + 1 library change:
- `deadline_create_job_from_job_bundle` (Batch D) — 5 C callback types
- `deadline_init_telemetry`, `deadline_record_telemetry_event`,
  `deadline_free_telemetry` (Batch E) — opaque handle pattern
- Added `interactive_confirmation_callback` to `SubmitJobParams`

Results: 1165 tests pass, 0 failures. 43 gui-ffi tests (16 new).

**Step 4 (Compare CLIs): ✅ Complete**

CLI-vs-CLI diffs (3/3 MATCH): `auth status`, `auth status --output json`,
`config get defaults.farm_id`. All identical output.

`bundle submit` comparison: both CLIs fail identically (no auth). The
`interactive_confirmation_callback` change only affects the unknown-path
warning flow during active submission — not reachable without auth.

FFI functions (Batch D + E) have no CLI equivalent — they're called via
ctypes from Python, not through the CLI binary. CLI comparison is N/A
for these.

**Step 5 (Audit & Fix): ✅ Complete**

Findings:
1. (Improvement, deferred) No `catch_unwind` on FFI functions — pre-existing
   pattern across all FFI functions, not introduced by #16b.
2. (Improvement, deferred) Rust submission confirmation flow is simpler than
   Python's (missing `from_gui` nuance, `_generate_message_for_asset_paths`
   detailed message) — pre-existing library difference, not introduced by #16b.
No bugs found. No code changes needed.

**Step 6 (Write spec): ✅ Complete**

Updated specs:
- `specs/gui-ffi/architecture.md` — Batch D/E function signatures, callback
  types, params_json fields
- `specs/gui-ffi/README.md` — status table updated, Batch D/E implemented

**Step 7 (Commit): ✅ Ready**

### Implementation Plan

#### Phase 1: Add `interactive_confirmation_callback` to Rust library

**Crate:** `deadline-job-bundle/src/submission.rs`

Add to `SubmitJobParams`:
```rust
pub interactive_confirmation_callback: Option<Box<dyn Fn(&str, bool) -> bool + Send>>,
```

Wire it into the unknown-path warning flow (replacing the current
`continue_callback` usage at line 399) and keep `continue_callback`
for the `wait_for_create_job_to_complete` polling only.

Update the CLI caller in `deadline-cli` to pass a confirmation callback
that uses the existing interactive prompt.

#### Phase 2: Batch D — `deadline_create_job_from_job_bundle` FFI

**Crate:** `deadline-gui-ffi/src/lib.rs`

New FFI function with C callback types:

```c
// C signature (what Python ctypes sees):
char* deadline_create_job_from_job_bundle(
    const char* params_json,        // JSON with all scalar params
    PrintCallback print_cb,
    ProgressCallback hashing_cb,
    ProgressCallback upload_cb,
    ConfirmationCallback confirm_cb,
    ContinueCallback continue_cb,
    void* user_data                 // opaque, passed to all callbacks
);
```

The `params_json` contains: `job_bundle_dir`, `job_parameters`,
`name`, `priority`, `max_failed_tasks_count`, `max_retries_per_task`,
`max_worker_count`, `target_task_run_status`, `job_attachments_file_system`,
`require_paths_exist`, `submitter_name`, `known_asset_paths`,
`debug_snapshot_dir`, `config_path`, `force_s3_check`.

Implementation:
1. Parse `params_json` into fields
2. Read config from `config_path` (or default)
3. Create tokio runtime
4. Wrap C callbacks into Rust closures
5. Build `SubmitJobParams` with the closures
6. Call `create_job_from_job_bundle(params).await`
7. Return `{"job_id": "..."}` or `{"error": "..."}`

#### Phase 3: Batch E — Telemetry FFI

**Crate:** `deadline-gui-ffi/src/lib.rs`

Two new FFI functions:

```c
// Returns opaque handle (pointer to TelemetryClient)
void* deadline_init_telemetry(const char* config_path);

// Record an event. handle is from deadline_init_telemetry.
char* deadline_record_telemetry_event(
    void* handle,
    const char* event_type,
    const char* event_details_json
);

// Free the telemetry handle
void deadline_free_telemetry(void* handle);
```

Implementation:
1. `deadline_init_telemetry`: call `create_telemetry(config)`, box it,
   return as `*mut c_void`
2. `deadline_record_telemetry_event`: cast handle back to
   `&TelemetryClient`, parse JSON details, call `record_event`
3. `deadline_free_telemetry`: drop the `Box<TelemetryClient>`

### Crates/Modules Changed

| Crate | File | Changes |
|-------|------|---------|
| `deadline-job-bundle` | `submission.rs` | Add `interactive_confirmation_callback` to `SubmitJobParams`, wire into unknown-path flow |
| `deadline-cli` | `commands/bundle.rs` | Pass confirmation callback to `SubmitJobParams` |
| `deadline-gui-ffi` | `lib.rs` | Add `deadline_create_job_from_job_bundle`, `deadline_init_telemetry`, `deadline_record_telemetry_event`, `deadline_free_telemetry`, 4 callback type aliases |
| `specs/gui-ffi` | `architecture.md` | Update with Batch D/E function signatures, callback types |
| `specs/gui-ffi` | `README.md` | Update status section |

### Cross-Reference: Test Spec Cases → Planned Rust Tests

| Batch | Test Case | Planned Rust Test Name |
|-------|-----------|----------------------|
| D | Submission with all callbacks | `create_job_calls_print_callback` |
| D | Null callbacks safe | `create_job_null_callbacks_returns_json` |
| D | Null params_json returns error | `create_job_null_params_returns_error` |
| D | Invalid JSON returns error | `create_job_invalid_json_returns_error` |
| D | Missing required fields returns error | `create_job_missing_bundle_dir_returns_error` |
| D | Confirmation callback receives message | `create_job_confirmation_callback_receives_message` |
| D | Progress callback receives metadata JSON | `create_job_progress_callback_receives_json` |
| D | Cancellation via progress callback | `create_job_cancel_via_progress_callback` |
| E | Init telemetry returns handle | `init_telemetry_returns_non_null_handle` |
| E | Init telemetry null config uses default | `init_telemetry_null_config_returns_handle` |
| E | Record event with valid handle | `record_event_valid_handle_returns_success` |
| E | Record event null handle returns error | `record_event_null_handle_returns_error` |
| E | Free telemetry null safe | `free_telemetry_null_does_not_crash` |
| E | Free telemetry valid handle | `free_telemetry_valid_handle_does_not_crash` |

### Batching Strategy

Two batches, ordered by dependency:

1. **Batch D (Submission)** — 1 FFI function + library change.
   Highest complexity: 5 callback types, JSON params, async runtime.
   Sub-steps:
   a. Add `interactive_confirmation_callback` to `SubmitJobParams`
   b. Update CLI caller
   c. Implement FFI function
   d. Write tests

2. **Batch E (Telemetry)** — 3 FFI functions. Low complexity: thin
   wrappers around existing `TelemetryClient`. Opaque handle pattern.

### Design Decisions

- **Phase 1 bundled with FFI.** The `interactive_confirmation_callback`
  addition to `SubmitJobParams` ships in the same commit as the FFI
  implementation. It's a GUI-essential callback (unknown-path warning
  dialog) and doesn't make sense as a standalone change.

### Risks

| Risk | Mitigation |
|------|------------|
| Confirmation callback blocks worker thread waiting for main thread response | This is by design — Python's `_confirmation_event.wait()` does the same. The FFI callback is synchronous. |
| `SubmitJobParams` change breaks CLI caller | Small change — add one optional field, update one call site |
| Telemetry handle lifetime across FFI | Use `Box::into_raw` / `Box::from_raw` pattern. Python wrapper must call `deadline_free_telemetry`. |
| Panic in callback crosses FFI boundary | Wrap all FFI functions in `std::panic::catch_unwind` |

---

## Previous Work Item: #16a (Batches A-C) — ✅ Done

**Step 1 (Study Python): ✅ Complete**

Studied:
- Archived and current GUI FFI architecture docs
- GUI FFI spec README and test spec index
- Full `deadline-gui-ffi` crate source (lib.rs, Cargo.toml, Python tests)
- Python `deadline.client.ui` module (controllers, dialogs, widgets)
- Python `deadline.client.api` public surface
- All DCC submitter repos (Blender, Maya, Nuke, Houdini, Cinema 4D, VRED,
  3ds Max, Unreal Engine, After Effects)
- `specs/architecture.md` crate graph
- `specs/workflow.md` development loop

**Step 2 (Write Tests): ✅ Complete**

Wrote 20 new tests for Batches A, B, and C (10 new FFI functions).
Refactored 5 existing spike tests to use shared `call_ffi_json` helper.
Total: 25 tests. Batch D (Submission) and Batch E (Telemetry) tests
deferred to their own implementation cycles.

**Step 3 (Implement): ✅ Complete**

Implemented 10 new `extern "C"` FFI functions across 3 batches:
- Batch A (Config): `deadline_read_config`, `deadline_get_setting`, `deadline_set_setting`
- Batch B (Resources): `deadline_list_farms`, `deadline_list_queues`,
  `deadline_list_storage_profiles_for_queue`, `deadline_get_queue_parameter_definitions`
- Batch C (Auth): `deadline_check_api_available`, `deadline_login`, `deadline_logout`

Also added shared helpers: `read_c_str`, `read_config_at`, `make_runtime`,
`json_to_ptr`, `success_to_ptr`. Refactored existing spike functions to use
shared helpers.

Results: 25/25 gui-ffi tests pass. Full workspace builds. Full test suite
passes (1147 tests, 0 failures, 0 ignored). One pre-existing flaky test
(`job_wait_timeout_exits_1`) occasionally fails under parallel load — passes
in isolation. Being investigated separately.

**Step 4 (Compare CLIs): ✅ Complete**

CLI-vs-CLI diffs (6/6 MATCH): `auth status`, `farm list`, `queue list`,
`config get` for farm_id, queue_id, aws_profile_name. All identical output.

FFI-vs-CLI comparison (14/14 passed): Python script calls Rust FFI via
ctypes and compares results against Rust CLI output. Covers auth (4),
config (3), list farms (2), list queues (2), queue parameters (2),
logout (1). No differences found.

Comparison script: `crates/deadline-gui-ffi/tests/python/ffi_comparison.py`

**Step 5 (Audit & Fix): ✅ Complete**

Findings:
1. (Improvement, deferred) Spike functions ignore config_path — pre-existing,
   not introduced by this work item.
2. (Bug, fixed) `set_setting_null_path_uses_default` test polluted the real
   `~/.deadline/config` with `farm-nullpath`. Fixed: test now uses
   `DEADLINE_CONFIG_FILE_PATH` env var to redirect to a temp file, and
   verifies the round-trip properly.
3. (Improvement, deferred) `read_c_str` silently returns None for invalid
   UTF-8 — low risk, DCC submitters always pass valid UTF-8.

**Step 6 (Write spec): ✅ Complete**

Updated specs:
- `specs/gui-ffi/architecture.md` — full rewrite with all 14 exported
  functions, Python wrapper layer, packaging, threading model
- `specs/gui-ffi/README.md` — function inventory, migration phases
- `specs/architecture.md` — repo structure, gui/ package, distribution
  before/after comparison
- `specs/progress.md` — #16 split into #16a-16f, GUI migration plan

**Step 7 (Commit): ✅ Ready**

Test inventory:

| Batch | Test Name | Tests |
|-------|-----------|-------|
| A: Config | `read_config_returns_valid_json` | null path |
| A: Config | `read_config_with_path_reads_file` | explicit path |
| A: Config | `read_config_nonexistent_path_returns_empty` | missing file |
| A: Config | `get_setting_known_key_returns_value` | happy path |
| A: Config | `get_setting_unknown_key_returns_default` | default fallback |
| A: Config | `get_setting_invalid_name_returns_error` | error path |
| A: Config | `set_setting_persists_value` | round-trip |
| A: Config | `set_setting_invalid_name_returns_error` | error path |
| A: Config | `set_setting_null_path_uses_default` | null safety |
| B: Resources | `list_farms_returns_json_with_farms_array` | structure check |
| B: Resources | `list_queues_returns_json_with_queues_array` | happy path |
| B: Resources | `list_queues_null_farm_id_returns_error` | null safety |
| B: Resources | `list_storage_profiles_returns_json` | structure check |
| B: Resources | `list_storage_profiles_null_ids_returns_error` | null safety |
| B: Resources | `get_queue_parameters_returns_json` | structure check |
| B: Resources | `get_queue_parameters_null_ids_returns_error` | null safety |
| C: Auth | `check_api_available_returns_bool_json` | structure check |
| C: Auth | `login_null_config_returns_json` | null safety |
| C: Auth | `logout_null_config_returns_json` | null safety |
| C: Auth | `logout_returns_success_field` | structure check |

Files modified:
- `crates/deadline-gui-ffi/src/lib.rs` — 22 new tests, refactored 5 existing
- `crates/deadline-gui-ffi/Cargo.toml` — added `tempfile` dev-dependency

## High-Level Implementation Plan

### Architecture Summary

The FFI bridge replaces the Python *library* layer, not the CLI. DCC
submitters currently `import deadline.client.api` for business logic.
After migration, the Python Qt widgets call Rust via ctypes instead.

```
BEFORE: Python Qt widgets → import deadline.client.api (Python)
AFTER:  Python Qt widgets → ctypes → deadline-gui-ffi.so (Rust)
```

Qt widgets stay in Python. DCC submitter code (~150-300 lines per DCC)
stays in Python. Only the business logic moves to Rust.

### FFI API Surface (derived from Python GUI code analysis)

Every DCC submitter and every Python Qt widget ultimately calls these
Python functions. Each needs a corresponding `extern "C"` FFI function.

#### Already implemented (spike)

| FFI Function | Wraps | Caller |
|-------------|-------|--------|
| `deadline_free_string` | Memory management | All callers |
| `deadline_get_credentials_source` | `api.get_credentials_source()` | `DeadlineAuthenticationStatus` |
| `deadline_check_auth_status` | `api.check_authentication_status()` | `DeadlineAuthenticationStatus` |
| `deadline_check_auth_status_with_progress` | Same + callbacks | `DeadlineAuthenticationStatus` |

#### Batch A — Config (sync, no tokio)

| FFI Function | Wraps | Caller |
|-------------|-------|--------|
| `deadline_read_config` | `config_file.read_config()` | `DeadlineAuthenticationStatus`, `DeadlineConfigDialog` |
| `deadline_get_setting` | `config_file.get_setting(name, config)` | All widgets, `SharedJobSettingsWidget` |
| `deadline_set_setting` | `config_file.set_setting(name, value, config)` | `DeadlineConfigDialog`, `SharedJobSettingsWidget` |

#### Batch B — Resource listing (async, tokio)

| FFI Function | Wraps | Caller |
|-------------|-------|--------|
| `deadline_list_farms` | `api.list_farms(config)` | `DeadlineUIController` |
| `deadline_list_queues` | `api.list_queues(config, farmId)` | `DeadlineUIController` |
| `deadline_list_storage_profiles_for_queue` | `api.list_storage_profiles_for_queue(config, farmId, queueId)` | `DeadlineUIController` |
| `deadline_get_queue_parameter_definitions` | `api.get_queue_parameter_definitions(config, farmId, queueId)` | `DeadlineUIController` |

#### Batch C — Auth actions (async, tokio)

| FFI Function | Wraps | Caller |
|-------------|-------|--------|
| `deadline_check_api_available` | `api.check_deadline_api_available(config)` | `DeadlineAuthenticationStatus` |
| `deadline_login` | `api.login(config)` | `DeadlineLoginDialog` |
| `deadline_logout` | `api.logout(config)` | `DeadlineLoginDialog` |

#### Batch D — Submission (async, complex callbacks)

| FFI Function | Wraps | Caller |
|-------------|-------|--------|
| `deadline_create_job_from_job_bundle` | `api.create_job_from_job_bundle(...)` | `JobSubmissionWorker` |

Callbacks needed:
- `print_function_callback(message)` — status messages
- `hashing_progress_callback(metadata) -> bool` — hash progress, return false to cancel
- `upload_progress_callback(metadata) -> bool` — upload progress, return false to cancel
- `interactive_confirmation_callback(message, default) -> bool` — user confirmation
- `create_job_result_callback() -> bool` — cancellation check

#### Batch E — Telemetry (sync + async)

| FFI Function | Wraps | Caller |
|-------------|-------|--------|
| `deadline_init_telemetry` | `api.get_deadline_cloud_library_telemetry_client()` | All DCC submitters |
| `deadline_record_telemetry_event` | `telemetry_client.record_event(...)` | Submission flow |

### Crates/Modules Changed

| Crate | Changes |
|-------|---------|
| `deadline-gui-ffi/src/lib.rs` | Add all new `extern "C"` functions. Split into modules if >500 lines. |
| `deadline-gui-ffi/Cargo.toml` | No changes needed — already depends on all required crates. |
| `specs/gui-ffi/architecture.md` | Update with full API surface, callback types, batch descriptions. |
| `specs/gui-ffi/README.md` | Update status section. |
| `specs/test_specs/index.md` | Add GUI FFI test spec section (currently TBD). |

### Batching Strategy

5 batches, ordered by dependency and complexity:

1. **Batch A (Config)** — 3 functions, sync, simplest. Unblocks `DeadlineConfigDialog`.
2. **Batch B (Resource listing)** — 4 functions, async. Unblocks `DeadlineUIController`.
3. **Batch C (Auth actions)** — 3 functions, async. Unblocks `DeadlineLoginDialog`.
4. **Batch D (Submission)** — 1 function, complex callbacks. Unblocks `JobSubmissionWorker`.
5. **Batch E (Telemetry)** — 2 functions. Unblocks DCC submitter telemetry.

Each batch: write tests → implement → test with Python integration script → commit.

### Test Strategy

Two levels of testing:

1. **Rust unit tests** (in `lib.rs` `#[cfg(test)]`): Call each `extern "C"` function
   directly from Rust, verify JSON output structure, verify null safety, verify
   callback invocation counts.

2. **Python integration tests** (in `tests/python/`): Load the `.dylib` via ctypes,
   call each function, verify JSON round-trip, verify callbacks work from Python
   threads. Extends the existing `gui_ffi_test.py` pattern.

### Cross-Reference: Test Spec Cases → Planned Rust Tests

Test spec section for GUI FFI is currently "TBD / Not started (Phase 3)".
Test cases will be written as part of Step 2 for each batch. Mapping:

| Batch | Test Spec Section (to create) | Planned Rust Test Names |
|-------|-------------------------------|------------------------|
| A | Config FFI | `read_config_returns_valid_json`, `get_setting_known_key_returns_value`, `get_setting_unknown_key_returns_null`, `set_setting_persists_value` |
| B | Resource Listing FFI | `list_farms_null_config_returns_json_array`, `list_queues_with_farm_id_returns_json_array`, `list_storage_profiles_returns_json_array`, `get_queue_parameters_returns_json_array` |
| C | Auth Actions FFI | `check_api_available_returns_bool_json`, `login_null_config_returns_json`, `logout_null_config_returns_json` |
| D | Submission FFI | `create_job_calls_all_callbacks`, `create_job_cancellation_via_callback`, `create_job_null_callbacks_safe`, `create_job_returns_job_id_json` |
| E | Telemetry FFI | `init_telemetry_returns_handle`, `record_event_with_valid_handle` |

### Key Design Decisions

1. **JSON strings cross the boundary for all complex data.** Same pattern as
   the spike. No C structs for complex types.

2. **Per-call tokio runtime.** Same pattern as the spike. Each async FFI
   function creates `Runtime::new()` and blocks. Safe because FFI calls
   happen on dedicated worker threads.

3. **Config passed as JSON string.** Python serializes its `ConfigParser`
   to JSON, passes to Rust. Rust deserializes to `IniConfig`. This avoids
   sharing mutable state across the FFI boundary.

4. **Callbacks use C function pointers with opaque `user_data`.** Same
   pattern as the spike. Python wraps Qt signal emitters into
   `@ctypes.CFUNCTYPE` callbacks.

5. **No global state in Rust.** Each call is self-contained. Avoids
   initialization order issues across different DCC host processes.

## Critical Context

1. **Python source is at `../deadline-cloud-python`** (sibling directory).
2. **All tests pass.** 1127 tests across all crates, 0 ignored.
3. **Testing rules:** Level 2 CLI tests must exercise the full stack through
   the CLI binary. No stubs at the application layer — only HTTP stub server.
4. **Serialization patterns.** See `specs/patterns.md`.
5. **`serde_json` has `preserve_order` enabled** — `json!()` preserves
   insertion order, not alphabetical.
6. **Test fixtures for CLI comparison.** `test_fixtures/job_bundles/`
   contains sample job bundles for manual comparison between Python and
   Rust CLIs. See `test_fixtures/README.md`.
7. **S3 mock encoding:** `encode_s3_path()` in `deadline-test-server`
   handles percent-encoding of colons in S3 key paths. Callers of
   `mock_s3_get_object` and `mock_s3_get_object_with_metadata` should
   pass raw keys — encoding is the mock helper's responsibility.
8. **DCC submitters do NOT call the CLI.** They import the Python library.
   The FFI replaces the Python library layer, not the CLI.
9. **Qt stays in Python.** Only business logic moves to Rust. The Qt
   widgets, threading model (QThread + signals), and DCC submitter
   Python code all remain unchanged.

## Investigated and Dropped

| Gap | Reason |
|-----|--------|
| AUDIT-034 | `--submitter-info` is GUI-only (`bundle gui-submit`). CLI `bundle submit` correctly has `--submitter-name`. Not deprecated on CLI. |
| AUDIT-051 | Download path collision — worker-agent scope, not CLI. Deferred. |

## Recently Completed

**S3 download mock chain fix (2026-04-21)**

Fixed test infrastructure bug: S3 SDK percent-encodes colons (`:` → `%3A`)
in HTTP paths with `force_path_style(true)`, but wiremock `path()` matcher
compared against raw encoded URLs. Added `encode_s3_path()` helper to
`deadline-test-server` that handles encoding centrally. Removed manual
`.replace(':', "%3A")` from 4 call sites in `queue_sync_output.rs`.
Unblocked 7 `#[ignore]` tests in `job_download.rs`. Result: 1127 tests,
0 ignored.

**Gap sweep: audit findings F1-F8 (2026-04-21)**

Fixed 7 audit findings across 6 files:
- **F1** (AUDIT-042): Windows stdin for login — `Stdio::piped()` on Windows
- **F2** (AUDIT-054): `--redirect-output` cross-platform — `SetStdHandle` on Windows
- **F3** (AUDIT-053): Windows long path UNC — `get_long_path_compatible_path()`
- **F5**: Telemetry hashing/upload summary events wired into submission flow
- **F6**: Telemetry error event on submission failure
- **F7** (AUDIT-008): Interactive root path editing in `job download-output`
- **F8** (AUDIT-031): `--save-debug-snapshot` — full implementation

**AUDIT-013: Hash cache V4 compatibility** (prior session)

- Rust now uses Python's `hashesV4` table instead of `hashesV5`. Both CLIs
  share one hash cache — zero re-hashing when switching between tools.

### Accepted Differences

| ID | Reason |
|----|--------|
| AUDIT-024 | YAML key ordering — accepted per `patterns.md` |
| AUDIT-030 | False finding — Python also doesn't support macOS |
| AUDIT-039 | `job wait` verbose to stderr — Rust approach is better |
| AUDIT-043 | User-agent position in header — Rust SDK limitation, content is correct |
| AUDIT-046 | `require_setting` exit code — function was unused dead code, deleted |
| SYNC-004 | Path summary groups by directory (Rust) vs flat full-path (Python) — same data, Rust more readable |
