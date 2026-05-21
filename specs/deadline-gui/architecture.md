# deadline-gui Crate

Rust + QML GUI for AWS Deadline Cloud, built with cxx-qt.

## What This Crate Does

Provides two functions that the CLI and DCC plugins call:

```rust
// CLI calls these:
deadline_gui::show_config_dialog();
deadline_gui::show_submit_dialog(params) -> Option<SubmitResult>;

// DCC plugins call this (reuses existing QApplication):
deadline_gui::show_submit_dialog_in_app(params) -> Option<SubmitResult>;
```

Each function creates a Qt window, runs the event loop, and returns when
the user closes the dialog or completes an action.

## How It Fits

```
deadline-cli ──→ deadline-gui ──→ deadline-lib ──→ AWS
                      │
deadline-python-bindings ──┘ (DCC plugins)
```

- **deadline-lib** does all business logic (API calls, config, submission)
- **deadline-gui** does all UI (QML rendering, user interaction, async coordination)
- **deadline-cli** does arg parsing and calls one or the other
- **deadline-python-bindings** exposes `show_submit_dialog()` to Python for DCCs

## Internal Layout

```
crates/deadline-gui/
├── Cargo.toml
├── build.rs              # Registers QML module + bridge files with cxx-qt-build
├── src/
│   ├── lib.rs            # Public API (show_config_dialog, show_submit_dialog)
│   ├── config_model.rs   # Config dialog backend
│   ├── auth_model.rs     # Auth state (credentials, login status)
│   ├── resource_model.rs # Farm/queue/storage profile lists
│   ├── submit_model.rs   # Submit dialog backend
│   ├── progress_model.rs # Submission progress (hashing, upload)
│   ├── parameter_model.rs # Dynamic parameter form data
│   ├── attachment_model.rs # File lists
│   └── login_model.rs    # Login flow
└── qml/
    ├── ConfigDialog.qml
    ├── SubmitDialog.qml
    ├── ProgressDialog.qml
    ├── LoginDialog.qml
    └── components/       # Reusable pieces (AuthStatusBar, selectors, etc.)
```

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

Dependencies: `cxx-qt 0.8`, `cxx-qt-lib`, `cxx-qt-build`, `deadline-lib`, `tokio`.
