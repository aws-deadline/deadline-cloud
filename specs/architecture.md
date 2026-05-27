# Architecture

## Repository Structure

This repo is a complete replacement for `deadline-cloud-python`. It ships:

1. **Rust CLI binary** (`deadline`) — headless commands, GUI dispatch via Python subprocess
2. **PyO3 extension module** (`deadline._native`) — Python bindings for DCC plugins
3. **Python Qt GUI** (`gui/`) — config and submit dialogs (PySide6)

```
deadline-cloud-rs/
├── crates/                          # Rust workspace
│   ├── deadline-lib/                # Library — config, API, bundles, attachments
│   │   └── src/
│   │       ├── config/              # INI config read/write
│   │       ├── api/                 # AWS API calls, auth, telemetry
│   │       ├── bundle/              # Job bundle parsing, submission
│   │       └── attachments/         # S3 transfer, hashing, manifests
│   ├── deadline-cli/                # Binary — headless CLI commands + GUI dispatch
│   ├── deadline-python-bindings/    # PyO3 extension module (deadline._native)
│   └── deadline-test-server/        # Test infrastructure (wiremock)
├── gui/                             # Python Qt GUI package (PySide6)
│   └── deadline/
│       ├── _native.abi3.so          # PyO3 module (built by maturin)
│       └── client/                  # UI widgets, dialogs, DCC submitter compat layer
├── pytests/                         # Python-based test suites
│   ├── _common/                     # Shared mock backend
│   ├── ui_accessibility/            # L2 xa11y tests (GUI subprocess + accessibility tree)
│   └── bindings/                    # PyO3 binding tests
├── pyproject.toml                   # maturin build config + test deps
├── specs/                           # Design docs
└── test_fixtures/                   # Manual comparison bundles
```

## Crate Dependency Graph

```
deadline-cli (binary)
├── deadline-lib
│   ├── config       — INI config, settings, hierarchical resolution
│   ├── api          — AWS SDK client, auth, session, telemetry
│   │   └── config
│   ├── bundle       — Job bundle parsing, submission orchestration
│   │   ├── api
│   │   ├── attachments
│   │   └── config
│   └── attachments  — S3 transfer, manifests, hash/check caches
│       └── config
├── openjd-snapshots — hashing, manifests, upload/download engine
└── rmcp             — MCP server (built into CLI)

deadline-python-bindings (PyO3 extension module, abi3-py39)
├── deadline-lib
├── pyo3
└── pythonize

deadline-test-server (dev-dependency of deadline-cli)
├── wiremock
├── tempfile
└── assert_cmd
```

## Crate Responsibilities

| Crate | Role |
|-------|------|
| `deadline-cli` | Binary. Clap argument parsing, subcommand dispatch, output formatting, MCP server (`mcp-server` subcommand via rmcp SDK). GUI commands spawn a Python subprocess for the Qt dialog. |
| `deadline-python-bindings` | PyO3 extension module (`deadline._native`). Exposes config, auth, API listing, submission, and telemetry as native Python functions. Used by DCC plugins and the Python Qt GUI. Uses `abi3-py39` for compatibility with Python 3.9+. |
| `deadline-lib` | Unified library crate containing all business logic as modules (see below). |
| `deadline-lib::config` | INI config file read/write, hierarchical setting resolution, str2bool. No AWS dependencies. |
| `deadline-lib::api` | AWS API calls (Deadline Cloud service), session/credential management, auth, telemetry, error types (`DeadlineError`), job monitoring types. Owns the SDK/HTTP interaction. |
| `deadline-lib::bundle` | Job bundle parsing, parameter validation, and submission orchestration. Owns the full lifecycle: load bundle → validate → merge parameters → upload attachments → CreateJob → poll for completion. |
| `deadline-lib::attachments` | Asset manifest handling, S3 upload/download via openjd-snapshots, hash cache, content-addressed storage. Owns its error types (`JobAttachmentsError`), `PathFormat`, and file conflict resolution. Independent S3/STS clients. |
| `deadline-test-server` | Test-only. Wiremock-based fake AWS server and `TestHarness` for CLI subprocess tests. |

## Python ↔ Rust Interface

The `deadline-python-bindings` crate uses PyO3 to expose Rust functions
directly as Python functions. Python calls them natively — no JSON
serialization, no ctypes, no manual memory management.

```python
# Python code calls Rust directly:
from deadline._native import list_farms, get_setting, DeadlineOperationError

farms = list_farms()                    # returns Python dict
value = get_setting("defaults.farm_id") # returns Python str
```

PyO3 handles type conversion automatically:
- `serde_json::Value` → Python dict/list/str/int/bool via `pythonize`
- Python callables → Rust closures (for progress/confirmation callbacks)
- Rust errors → `DeadlineOperationError` Python exception
- `#[pyclass] TelemetryClient` → Python object with automatic cleanup on drop

The module is built with `abi3-py39`, producing a single `.abi3.so` that
works on Python 3.9 through 3.14+. This is the same mechanism PySide6
and other compiled Python extensions use.

## Data Flows

### CLI Headless Command (e.g., `deadline bundle submit`)

```
deadline-cli
  → Clap parses args
  → Config loaded once via deadline-lib::config
  → CLI flags applied as in-memory overrides
  → Business logic via deadline-lib::{bundle, attachments, api}
  → Output formatted and printed
```

All Rust, one process, direct function calls.

### CLI GUI Command (e.g., `deadline config gui`, `deadline bundle gui-submit`)

```
deadline-cli
  → Clap parses args, validates (submitter-info, bundle dir)
  → Finds Python interpreter (venv or system)
  → Spawns: python -m deadline.client.ui._gui_entry gui-submit --params-json '{...}'
  → Python subprocess:
    → Checks PySide6 is installed
    → Creates QApplication
    → Shows SubmitJobToDeadlineDialog (Python Qt)
    → User interacts, submits
    → Dialog calls deadline._native.create_job_from_job_bundle() → Rust
    → Prints result JSON to stdout
  → CLI reads stdout, prints result
```

### DCC Plugin (e.g., Maya)

```
Maya Python plugin
  → Scene introspection via maya.cmds (Python, ~100-200 lines)
  → Builds scene_data dict
  → import deadline.client.ui (from gui/ package)
  → gui/ imports deadline._native (one-time, via dlopen)
  → Shows SubmitJobToDeadlineDialog (Python Qt via PySide6)
  → User clicks Submit
  → Dialog calls deadline._native.create_job_from_job_bundle() → Rust
  → Returns job_id
```

### After Effects

```
ExtendScript (JSX)
  → system.callSystem("deadline bundle gui-submit ...")
  → deadline-cli binary handles everything
```

## Packaging (maturin + PyO3)

The root `pyproject.toml` uses maturin as the build backend. Currently
used for:

1. Building the PyO3 extension module (`deadline._native.abi3.so`)
2. Managing Python test dependencies (`pip install -e ".[test]"`)

The `gui/` Python package provides the Qt GUI for both the CLI
(`bundle gui-submit`, `config gui`) and DCC plugins (Maya, Blender, etc.).
It calls `deadline._native` for all business logic.

### What ships in the wheel

```
deadline-0.1.0-cp39-abi3-macosx_11_0_arm64.whl
├── deadline/
│   ├── _native.abi3.so              # PyO3 extension module
│   ├── client/
│   │   ├── api/                     # Python wrappers → _native calls
│   │   ├── ui/                      # Qt widgets (presentation only)
│   │   ├── config/                  # Python wrapper → _native config
│   │   ├── job_bundle/              # Data classes, YAML helpers
│   │   ├── dataclasses/             # SubmitterInfo, etc.
│   │   └── exceptions.py
│   ├── job_attachments/
│   │   └── progress_tracker.py
│   └── common/
│       └── path_utils.py
└── deadline-0.1.0.dist-info/
```

**What's gone vs Python:** boto3, botocore, click, xxhash, psutil, the
entire Python business logic layer, and ~20 other Python packages. All
replaced by the Rust code compiled into the PyO3 module.

## Shared Conventions

- **Error handling:** All crates use `thiserror` for error types. The CLI
  catches errors at the top level and prints them. The PyO3 layer converts
  Rust errors to `DeadlineOperationError` Python exceptions.
- **No mocking:** Tests use real temp directories and wiremock HTTP servers.
  See [`testing.md`](testing.md).
- **API responses:** `deadline-lib::api` owns session management, credential
  scoping, telemetry (via a client-level interceptor), and error mapping
  for all Deadline Cloud API calls. Consumer crates import SDK types
  directly from `aws-sdk-deadline` and use the SDK's fluent builders at
  callsite. All API calls return typed SDK output. Display paths use
  serializable response structs built from typed output via `From<Output>`
  impls. See `specs/patterns.md` § "AWS SDK for Rust Usage".
- **Config threading:** Config utilities (`get_setting`, `set_setting`)
  take `&IniConfig` (reads) or `&mut IniConfig` (writes). Session-layer
  operations take explicit params (`profile: Option<&str>`, `farm_id: &str`,
  etc.) — callers extract values from config before calling. See
  `specs/patterns.md` § "Library/CLI separation of concerns".
- **Credential scoping:** Non-Deadline AWS clients (CloudWatch Logs, S3)
  that access queue-scoped or fleet-scoped resources must use scoped
  credentials when the user is logged in via DCM. See
  [`patterns.md`](patterns.md) § "Credential Scoping for Non-Deadline
  AWS Services".
