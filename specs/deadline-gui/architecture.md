# deadline-gui Crate

Rust + QML GUI for AWS Deadline Cloud, built with cxx-qt v0.8.

## What This Crate Does

Provides functions that the CLI and DCC plugins call:

```rust
// CLI calls these (creates QApplication, blocks until dialog closes):
deadline_gui::show_config_dialog();
deadline_gui::show_submit_dialog(&params) -> String;  // JSON result
```

## How It Fits

```
deadline-cli ──→ deadline-gui ──→ deadline-lib ──→ AWS
                      │
deadline-python-bindings ──┘ (DCC plugins, Phase 3)
```

- **deadline-lib** does all business logic (API calls, config, submission)
- **deadline-gui** does all UI (QML rendering, user interaction, async coordination)
- **deadline-cli** does arg parsing and calls one or the other

## Architecture: Logic + Model + View

```
┌─────────────────────────────────────────────────────┐
│  QML (View)                                         │
│  ConfigDialog.qml — declarative layout, bindings    │
└──────────────────────┬──────────────────────────────┘
                       │ property bindings + invokable calls
┌──────────────────────▼──────────────────────────────┐
│  QObject Models                                     │
│  config_model.rs — settings form state              │
│  resource_model.rs — farm/queue/storage lists       │
│  auth_model.rs — auth status + file watcher         │
└──────────────────────┬──────────────────────────────┘
                       │ function calls
┌──────────────────────▼──────────────────────────────┐
│  Logic Module (logic/)                              │
│  Pure Rust, no Qt dependency. Testable with         │
│  cargo test. Calls deadline-lib for config/API.     │
└──────────────────────┬──────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────┐
│  deadline-lib (config, API, auth)                   │
└─────────────────────────────────────────────────────┘
```

## Current Layout

```
crates/deadline-gui/
├── Cargo.toml
├── build.rs                  # Registers QML module + bridge files
├── src/
│   ├── lib.rs                # Public API: show_config_dialog(), show_submit_dialog()
│   ├── config_model.rs       # QObject: settings form, load/apply/dirty
│   ├── resource_model.rs     # QObject: async farm/queue/storage profile lists
│   ├── auth_model.rs         # QObject: auth status, login/logout, file watcher
│   ├── submit_model.rs       # QObject: job submission, progress, cancel
│   ├── progress_model.rs     # QObject: standalone progress state
│   ├── logic.rs              # Module root
│   ├── logic/tests.rs        # L1 tests for config logic
│   ├── logic/auth.rs         # Auth state derivation (+ tests)
│   ├── logic/resources.rs    # Resource fetch + selection (+ tests)
│   ├── logic/watcher.rs      # File watcher path logic (+ tests)
│   ├── logic/submit.rs       # Bundle prep, validation, config extraction (+ tests)
│   ├── logic/attachments.rs  # AssetReferences CRUD (+ tests)
│   ├── logic/parameters.rs   # Queue parameter parsing (+ tests)
│   ├── logic/host_requirements.rs  # Host requirements serialization (+ tests)
│   └── bin/gui_test_harness.rs     # Test binary for xa11y tests
└── qml/
    ├── ConfigDialog.qml      # Config dialog
    ├── SubmitDialog.qml      # Submit dialog with inline progress
    └── ProgressDialog.qml    # Standalone progress dialog (reusable)
```

## cxx-qt Naming Convention

**Critical:** cxx-qt v0.8 exposes Rust snake_case names directly to QML.
No automatic camelCase conversion.

- Property `#[qproperty(bool, farms_loading)]` → QML: `resourceModel.farms_loading`
- Method `fn refresh_farms(...)` → QML: `resourceModel.refresh_farms()`
- Signal for property change → QML: `onFarms_loadingChanged`

## Logic Module Contract

### Config (logic.rs)

| Function | Purpose |
|----------|---------|
| `load_config_state(path)` | Read all dialog settings from INI |
| `apply_config_changes(path, changes)` | Write changed settings to disk |
| `compute_dirty_fields(baseline, current)` | Dirty tracking for Apply button |
| `parse_aws_profiles(aws_dir)` | Profile dropdown from ~/.aws/ |
| `extract_farms/queues/storage_profiles(pages)` | Parse API JSON → sorted ResourceEntry list |

### Auth (logic/auth.rs)

| Function | Purpose |
|----------|---------|
| `derive_auth_state(creds, auth, api)` | Map 3 async results → display state enum |
| `auth_status_text(state, profile)` | Human-readable status message |
| `should_show_login/logout/more_info(state)` | Button visibility predicates |

### Resources (logic/resources.rs)

| Function | Purpose |
|----------|---------|
| `fetch_farms/queues/storage_profiles(profile, ...)` | Async API call → Vec<ResourceEntry> |
| `resolve_selected_index(entries, configured_id)` | Find configured ID in list, fallback to raw ID |
| `selected_id_at(entries, index)` | Get ID at index for cascading |

### Watcher (logic/watcher.rs)

| Function | Purpose |
|----------|---------|
| `watch_paths()` | Returns ~/.aws/ and ~/.deadline/ |
| `should_trigger_refresh(path, watch_paths)` | Filter relevant file changes |

### Submit (logic/submit.rs)

| Function | Purpose |
|----------|---------|
| `prepare_job_bundle(output_dir, settings, queue_params, assets, host_req)` | Copy template, merge params, write assets, copy hooks |
| `export_bundle_to_history(settings, queue_params, assets, host_req, submitter, history_dir)` | Create job history dir + prepare bundle there |
| `format_gui_submit_result(output_mode, job_id, history_dir)` | Format result for stdout (JSON or verbose) |
| `validate_submit_readiness(farm_id, queue_id, api_available)` | Returns issues list (empty = ready) |
| `read_submit_config_fields()` | Extract submission config from INI (testable) |
| `resolve_target_task_run_status(initial_status)` | Map "SUSPENDED" → Some, else None |

### Attachments (logic/attachments.rs)

| Function | Purpose |
|----------|---------|
| `AssetReferences::add_input_file/dir`, `remove_*`, `merge` | CRUD for file/dir lists |
| `AssetReferences::from_json/to_json` | Serialize/deserialize |

### Parameters (logic/parameters.rs)

| Function | Purpose |
|----------|---------|
| `parse_queue_parameters(environments)` | Extract params from queue env YAML |
| `merge_parameters(queue_params, job_params)` | Job overrides queue |
| `validate_parameters(params, known_names)` | Detect unrecognized params |

### Host Requirements (logic/host_requirements.rs)

| Function | Purpose |
|----------|---------|
| `serialize_host_requirements(os, hardware, custom)` | Build JSON for template injection |

## Async Pattern

All API calls happen off the Qt thread:

```
QML calls #[qinvokable]
  → Rust spawns std::thread with tokio::Runtime
  → tokio runs deadline-lib async function
  → Result sent back via qt_thread.queue(|obj| obj.set_property(...))
  → QML reacts to property change
```

## Resource Loading: Cascading Refresh

```
Profile change → clear all → fetch farms
  → farms loaded → resolve_selected_index(configured_farm_id) → fetch queues
    → queues loaded → resolve_selected_index(configured_queue_id) → fetch storage profiles
      → storage profiles loaded → resolve_selected_index(configured_sp_id)
```

On user selection change (e.g. pick new farm):
- Clears dependent lists (queues, storage profiles)
- Updates configured ID
- Triggers fetch for next level

## Auth Status: State Machine

```
All None → Refreshing (⟳ icon)
Authenticated + api=true → AuthenticatedReady (✓ icon, logout if DCM)
Authenticated + api=false → AuthenticatedNoApi (⚠ icon, more info)
NeedsLogin → NeedsLogin (⚠ icon, login button)
ConfigurationError → ConfigurationError (⚠ icon, more info)
Other → UnexpectedError (⚠ icon, more info)
```

File watcher (`notify` crate) monitors ~/.aws/ and ~/.deadline/ and
triggers `refresh_status()` on changes.

## Build

```bash
QMAKE=/opt/homebrew/opt/qt/bin/qmake cargo build -p deadline-gui
```

Dependencies: `cxx-qt 0.8`, `cxx-qt-lib`, `cxx-qt-build`, `deadline-lib`,
`serde_json`, `tokio`, `dirs`, `notify`.

## Testing

- **L1:** `cargo test -p deadline-gui` — 128 tests (config + auth + resources + watcher + submit + attachments + parameters + host requirements logic)
- **L2:** `pytest test/ui/` — 35 accessibility tests via xa11y against the real binary (25 passing, 10 deferred — all config gui combo box accessibility)

## Remaining TODOs

- Queue parameters dynamic form
- Attachments UI (add/remove)
- Host requirements UI
- Settings button (open config from submit)
- Known Asset Paths UI (Add/Edit/Remove list widget)
- File watcher shutdown on dialog close
- Phase 3: PyO3 `show_submit_dialog()` for DCC plugins
