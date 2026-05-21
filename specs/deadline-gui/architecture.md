# deadline-gui Crate

Rust + QML GUI for AWS Deadline Cloud, built with cxx-qt.

## What This Crate Does

Provides functions that the CLI and DCC plugins call:

```rust
// CLI calls this (creates QApplication, blocks until dialog closes):
deadline_gui::show_config_dialog();

// Future (Batch 2):
deadline_gui::show_submit_dialog(params) -> Option<SubmitResult>;
```

## How It Fits

```
deadline-cli ──→ deadline-gui ──→ deadline-lib ──→ AWS
                      │
deadline-python-bindings ──┘ (DCC plugins, future)
```

- **deadline-lib** does all business logic (API calls, config, submission)
- **deadline-gui** does all UI (QML rendering, user interaction, async coordination)
- **deadline-cli** does arg parsing and calls one or the other
- **deadline-python-bindings** will expose `show_submit_dialog()` to Python for DCCs (Batch 3)

## Architecture: Logic + Model + View

```
┌─────────────────────────────────────────────────────┐
│  QML (View)                                         │
│  ConfigDialog.qml — declarative layout, bindings    │
└──────────────────────┬──────────────────────────────┘
                       │ property bindings + invokable calls
┌──────────────────────▼──────────────────────────────┐
│  QObject Model (config_model.rs)                    │
│  Thin wrapper: exposes properties to QML,           │
│  delegates to logic module                          │
└──────────────────────┬──────────────────────────────┘
                       │ function calls
┌──────────────────────▼──────────────────────────────┐
│  Logic Module (logic.rs)                            │
│  Pure Rust, no Qt dependency. Testable with         │
│  cargo test. Calls deadline-lib for config I/O.     │
└──────────────────────┬──────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────┐
│  deadline-lib (config, API, auth)                   │
└─────────────────────────────────────────────────────┘
```

**Key design decision:** Business logic lives in `logic.rs` as plain
functions taking `&Path` / `&[Value]` arguments. QObject methods are
thin wrappers that call these functions and set Qt properties. This
enables L1 testing without Qt and keeps the cxx-qt bridge minimal.

## Current Layout (Batch 1)

```
crates/deadline-gui/
├── Cargo.toml
├── build.rs              # Registers QML module + bridge files with cxx-qt-build
├── src/
│   ├── lib.rs            # Public API: show_config_dialog()
│   ├── logic.rs          # Pure logic: load/apply/dirty/parse (testable without Qt)
│   ├── logic/tests.rs    # L1 tests for logic module
│   └── config_model.rs   # QObject: properties + invokables, delegates to logic.rs
└── qml/
    └── ConfigDialog.qml  # Full config dialog (4 groups, all settings)
```

## Planned Layout (Batches 2-4)

```
crates/deadline-gui/src/
├── lib.rs
├── logic.rs              # Shared logic (config, resource parsing, auth)
├── config_model.rs       # Config dialog QObject
├── resource_model.rs     # Farm/queue/storage profile async loading
├── auth_model.rs         # Auth status + file watching
├── submit_model.rs       # Submit dialog QObject
├── progress_model.rs     # Submission progress
└── ...
```

## Logic Module Contract

The logic module (`logic.rs`) provides these pure functions:

| Function | Input | Output | Purpose |
|----------|-------|--------|---------|
| `load_config_state(path)` | Config file path | `ConfigState` struct | Read all dialog settings |
| `apply_config_changes(path, changes)` | Path + changed settings | Updated `ConfigState` | Write changes to disk |
| `compute_dirty_fields(baseline, current)` | Two states | List of changed setting keys | Dirty tracking |
| `parse_aws_profiles(aws_dir)` | `~/.aws/` path | Sorted profile names | Profile dropdown |
| `extract_farms(pages)` | JSON API pages | Sorted `ResourceEntry` list | Farm dropdown |
| `extract_queues(pages)` | JSON API pages | Sorted `ResourceEntry` list | Queue dropdown |
| `extract_storage_profiles(pages, os)` | JSON + OS filter | Sorted list with `<none>` | Storage profile dropdown |

All functions are synchronous. Async API calls happen in the QObject
layer (spawning threads with tokio) and pass results to these functions
for parsing.

## Async Pattern

All API calls happen off the Qt thread:

```
User clicks button
  → QML calls #[qinvokable] on Rust model
  → Rust spawns std::thread with tokio
  → tokio runs deadline-lib async function
  → Result sent back via qt_thread.queue(|obj| obj.set_result(...))
  → QML reacts to property change
```

This uses cxx-qt's `impl cxx_qt::Threading` trait which provides
`self.qt_thread()` — a `Send` handle for safely updating the QObject
from any thread.

## Build

```bash
# Requires Qt 6 + qmake on system
QMAKE=/opt/homebrew/opt/qt/bin/qmake cargo build -p deadline-gui
```

Dependencies: `cxx-qt 0.8`, `cxx-qt-lib`, `cxx-qt-build`, `deadline-lib`, `serde_json`, `tokio`.

## Testing

- **L1:** `cargo test -p deadline-gui` — tests logic.rs without Qt
- **L2:** `pytest test/ui/` — accessibility tests via xa11y against the real binary
- See `specs/testing.md` § GUI Testing for full details
