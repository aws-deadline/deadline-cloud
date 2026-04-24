# Architecture

## Repository Structure

This repo is a complete replacement for `deadline-cloud-python`. It ships:

1. **Rust CLI binary** (`deadline`) — headless commands
2. **Rust shared library** (`libdeadline_gui_ffi`) — C ABI for GUI callers
3. **Python Qt GUI** (`gui/`) — presentation layer, backed by the Rust library
4. **Python compatibility package** — re-exports `deadline.client.api`,
   `deadline.client.ui`, etc. so DCC submitters work with unchanged imports

```
deadline-cloud-rs/
├── crates/                          # Rust workspace
│   ├── deadline-cli/                # Binary — headless CLI commands
│   ├── deadline-gui-ffi/            # Shared library — C ABI for Python
│   ├── deadline-config/             # Config read/write
│   ├── deadline-api/                # AWS API calls, auth, telemetry
│   ├── deadline-job-bundle/         # Job bundle parsing, submission
│   ├── deadline-job-attachments/    # S3 transfer, hashing, manifests
│   └── deadline-test-server/        # Test infrastructure
├── gui/                             # Python Qt GUI package (planned)
│   └── deadline/
│       ├── client/
│       │   ├── api/                 # Python wrappers → FFI calls
│       │   ├── ui/                  # Qt widgets, dialogs, controllers
│       │   ├── config/              # Python wrapper → FFI config calls
│       │   ├── job_bundle/          # Data classes, YAML helpers
│       │   ├── dataclasses/         # SubmitterInfo, etc.
│       │   └── exceptions.py        # Python exception types
│       ├── job_attachments/
│       │   └── progress_tracker.py  # ProgressReportMetadata (callbacks)
│       └── common/
│           └── path_utils.py
├── specs/                           # Design docs
└── test_fixtures/                   # Manual comparison bundles
```

## Crate Dependency Graph

```
deadline-cli (binary)
├── deadline-config
├── deadline-api
│   └── deadline-config
├── deadline-job-bundle
│   ├── deadline-api
│   ├── deadline-job-attachments
│   └── deadline-config
├── deadline-job-attachments
│   └── deadline-config
└── rmcp (MCP server, built into CLI)

deadline-gui-ffi (shared library, C ABI)
├── deadline-config
├── deadline-api
├── deadline-job-bundle
└── deadline-job-attachments

gui/ (Python package) ──ctypes──► deadline-gui-ffi.{dylib,so,dll}
├── Qt widgets (pure presentation, no Rust dependency)
├── Controllers/dialogs (call Rust via _ffi.py)
├── _ffi.py (ctypes wrapper — loads shared library)
└── Data classes & utilities (pure Python, no Rust dependency)

deadline-test-server (dev-dependency of deadline-cli)
├── wiremock
├── tempfile
└── assert_cmd
```

## Crate Responsibilities

| Crate | Role |
|-------|------|
| `deadline-cli` | Binary. Clap argument parsing, subcommand dispatch, output formatting, MCP server (`mcp-server` subcommand via rmcp SDK). No business logic beyond presentation. For GUI commands, spawns a Python process that loads the GUI widgets + `deadline-gui-ffi`. |
| `deadline-gui-ffi` | Shared library with C ABI. Exposes config, auth, API listing, submission, and telemetry to external callers (Python GUI, DCC plugins, Unreal). |
| `gui/` | Python package. Qt widgets (presentation), controllers (call FFI), data classes (pure Python). Shipped alongside the Rust artifacts. DCC submitters import from this package. |
| `deadline-config` | INI config file read/write, hierarchical setting resolution, str2bool. No AWS dependencies. |
| `deadline-api` | AWS API calls (Deadline Cloud service), session/credential management, auth, telemetry, error types (`DeadlineError`), submitter info, path utilities, job monitoring types. Owns the SDK/HTTP interaction. |
| `deadline-job-bundle` | Job bundle parsing, parameter validation, and submission orchestration. Owns the full lifecycle: load bundle → validate → merge parameters → upload attachments → CreateJob → poll for completion. |
| `deadline-job-attachments` | Asset manifest handling, S3 upload/download, hash cache, content-addressed storage. Owns its error types (`JobAttachmentsError`), `PathFormat`, and file conflict resolution. Independent S3/STS clients. |
| `deadline-test-server` | Test-only. Wiremock-based fake AWS server and `TestHarness` for CLI subprocess tests. |

## Data Flows

### CLI Headless Command (e.g., `deadline bundle submit`)

```
deadline-cli
  → Clap parses args
  → Config loaded once via deadline-config
  → CLI flags applied as in-memory overrides
  → Business logic via deadline-job-bundle, deadline-job-attachments, deadline-api
  → Output formatted and printed
```

All Rust, one process, direct function calls.

### CLI GUI Command (e.g., `deadline config gui`)

```
deadline-cli
  → Spawns Python process
  → Python loads gui/ package
  → gui/ loads deadline-gui-ffi.{dylib,so,dll} via _ffi.py (ctypes)
  → Python shows QDialog
  → User interacts with dialog
  → Every button click / dropdown load calls _ffi → Rust shared library
  → Rust reads config, calls APIs, saves config
  → Python displays results
```

For detailed diagrams of the FFI threading model, callback flow, and
memory ownership, see `specs/gui-ffi/architecture.md`.

### DCC Plugin (e.g., Maya)

```
Maya Python plugin
  → Scene introspection via maya.cmds (Python, ~100-200 lines)
  → Builds scene_data dict
  → import deadline.client.ui (from gui/ package)
  → gui/ loads deadline-gui-ffi via _ffi.py (one-time)
  → Shows SubmitJobToDeadlineDialog (Python Qt)
  → User clicks Submit
  → Dialog calls _ffi.create_job_from_job_bundle() → Rust does everything
  → Returns job_id
```

DCC submitters use the same import paths as before (`from deadline.client.api
import ...`, `from deadline.client.ui.dialogs import ...`). The `gui/` package
re-exports these, backed by Rust via FFI.

### After Effects

```
ExtendScript (JSX)
  → system.callSystem("deadline bundle gui-submit ...")
  → deadline-cli binary handles everything
```

## Distribution

Today's Python `deadline` binary is a PyInstaller-frozen executable. The
`DeadlineClient/deadline` file is a Mach-O binary that embeds the Python
runtime, and `DeadlineClient/_internal/` contains the full Python package
tree (boto3, PySide6, the entire `deadline.*` library, etc.).

The Rust `deadline` binary is a real native executable. For headless CLI
commands, it needs nothing else. For GUI commands (`deadline config gui`,
`deadline bundle gui-submit`), it spawns a Python process that loads the
Qt widgets from a bundled `_internal/` directory.

### What ships today (Python)

```
DeadlineClient/
├── deadline                    # PyInstaller-frozen Python (Mach-O wrapper)
└── _internal/
    ├── Python.framework/       # Embedded Python runtime
    ├── PySide6/                # Qt bindings (~large)
    ├── boto3/                  # AWS SDK for Python
    ├── botocore/               # AWS SDK internals
    ├── deadline/
    │   ├── client/api/         # Python business logic (ALL of it)
    │   ├── client/cli/         # Python CLI (click-based)
    │   ├── client/config/      # Python config
    │   ├── client/ui/          # Python Qt widgets
    │   ├── client/job_bundle/  # Python job bundle
    │   └── job_attachments/    # Python S3 transfer
    └── [~25 more Python packages]
```

### What ships after Rust migration

```
DeadlineClient/
├── deadline                    # Native Rust binary (all CLI logic built-in)
└── _internal/
    ├── Python.framework/       # Embedded Python runtime (GUI only)
    ├── PySide6/                # Qt bindings (GUI only)
    ├── libdeadline_gui_ffi.dylib  # Rust shared library (C ABI)
    └── deadline/
        ├── client/
        │   ├── _ffi.py         # ctypes wrapper → Rust shared library
        │   ├── api/            # Thin re-exports → FFI calls (NOT boto3)
        │   ├── ui/             # Qt widgets (presentation only)
        │   ├── config/         # Thin wrapper → FFI config calls
        │   ├── job_bundle/     # Data classes, YAML helpers (pure Python)
        │   ├── dataclasses/    # SubmitterInfo, etc. (pure Python)
        │   └── exceptions.py   # Python exception types
        ├── job_attachments/
        │   └── progress_tracker.py  # Callback data class
        └── common/
            └── path_utils.py
```

**What's gone:** boto3, botocore, the entire Python business logic layer
(`_submit_job_bundle.py`, `_session.py`, `_loginout.py`, `upload.py`,
`download.py`, `_list_apis.py`, etc.), click, and ~20 other Python
packages. All replaced by the Rust binary and shared library.

**What remains:** Python runtime (for Qt), PySide6 (Qt bindings), and
the thin Python Qt presentation layer that calls Rust via `_ffi.py`.

DCC submitters (Blender, Maya, etc.) don't use the `deadline` binary at
all — they load the `_internal/deadline/` Python package directly into
their embedded Python. The import paths stay the same
(`from deadline.client.ui.dialogs import SubmitJobToDeadlineDialog`),
but the business logic behind those imports now goes through FFI to Rust.

## Shared Conventions

- **Error handling:** All crates use `thiserror` for error types. The CLI
  catches errors at the top level and prints them. The FFI layer converts
  Rust errors to JSON error objects (`{"error": "message"}`).
- **No mocking:** Tests use real temp directories and wiremock HTTP servers.
  See [`testing.md`](testing.md).
- **API responses:** All API functions in `deadline-api` use the
  `ResponseBodyCapture` interceptor to return raw `serde_json::Value`.
  The CLI layer never sees SDK types. See `specs/patterns.md` § "AWS SDK for
  Rust Usage".
- **Config threading:** Functions that need config take `&IniConfig` (reads)
  or `&mut IniConfig` (writes). Convenience wrappers that hit disk exist but
  are not the primary API.
- **Credential scoping:** Non-Deadline AWS clients (CloudWatch Logs, S3)
  that access queue-scoped or fleet-scoped resources must use scoped
  credentials when the user is logged in via DCM. See
  [`patterns.md`](patterns.md) § "Credential Scoping for Non-Deadline
  AWS Services".
