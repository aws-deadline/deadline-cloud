# Architecture

## Repository Structure

This repo is a complete replacement for `deadline-cloud-python`. It ships:

1. **Rust CLI binary** (`deadline`) — headless commands
2. **PyO3 extension module** (`deadline._native`) — Python bindings for GUI callers
3. **Python Qt GUI** (`gui/`) — presentation layer, backed by the Rust library
4. **Python compatibility package** — re-exports `deadline.client.api`,
   `deadline.client.ui`, etc. so DCC submitters work with unchanged imports

```
deadline-cloud-rs/
├── crates/                          # Rust workspace
│   ├── deadline-cli/                # Binary — headless CLI commands
│   ├── deadline-python-bindings/    # PyO3 extension module (deadline._native)
│   ├── deadline-config/             # Config read/write
│   ├── deadline-api/                # AWS API calls, auth, telemetry
│   ├── deadline-job-bundle/         # Job bundle parsing, submission
│   ├── deadline-job-attachments/    # S3 transfer, hashing, manifests
│   └── deadline-test-server/        # Test infrastructure
├── gui/                             # Python Qt GUI package
│   └── deadline/
│       ├── _native.abi3.so          # PyO3 module (built by maturin)
│       ├── client/
│       │   ├── api/                 # Python wrappers → _native calls
│       │   ├── ui/                  # Qt widgets, dialogs, controllers
│       │   ├── config/              # Python wrapper → _native config calls
│       │   ├── job_bundle/          # Data classes, YAML helpers
│       │   ├── dataclasses/         # SubmitterInfo, etc.
│       │   └── exceptions.py        # Python exception types
│       ├── job_attachments/
│       │   └── progress_tracker.py  # ProgressReportMetadata (callbacks)
│       └── common/
│           └── path_utils.py
├── pyproject.toml                   # maturin build config for PyPI wheel
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

deadline-python-bindings (PyO3 extension module, abi3-py39)
├── deadline-config
├── deadline-api
├── deadline-job-bundle
├── deadline-job-attachments
├── pyo3
└── pythonize

gui/ (Python package) ── import deadline._native ──► PyO3 module
├── Qt widgets (pure presentation, no Rust dependency)
├── Controllers/dialogs (call Rust via deadline._native)
└── Data classes & utilities (pure Python, no Rust dependency)

deadline-test-server (dev-dependency of deadline-cli)
├── wiremock
├── tempfile
└── assert_cmd
```

## Crate Responsibilities

| Crate | Role |
|-------|------|
| `deadline-cli` | Binary. Clap argument parsing, subcommand dispatch, output formatting, MCP server (`mcp-server` subcommand via rmcp SDK). No business logic beyond presentation. |
| `deadline-python-bindings` | PyO3 extension module (`deadline._native`). Exposes config, auth, API listing, submission, and telemetry as native Python functions. Uses `abi3-py39` for compatibility with Python 3.9+. |
| `gui/` | Python package. Qt widgets (presentation), controllers (call `deadline._native`), data classes (pure Python). Shipped alongside the Rust artifacts. DCC submitters import from this package. |
| `deadline-config` | INI config file read/write, hierarchical setting resolution, str2bool. No AWS dependencies. |
| `deadline-api` | AWS API calls (Deadline Cloud service), session/credential management, auth, telemetry, error types (`DeadlineError`), submitter info, path utilities, job monitoring types. Owns the SDK/HTTP interaction. |
| `deadline-job-bundle` | Job bundle parsing, parameter validation, and submission orchestration. Owns the full lifecycle: load bundle → validate → merge parameters → upload attachments → CreateJob → poll for completion. |
| `deadline-job-attachments` | Asset manifest handling, S3 upload/download, hash cache, content-addressed storage. Owns its error types (`JobAttachmentsError`), `PathFormat`, and file conflict resolution. Independent S3/STS clients. |
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
  → gui/ imports deadline._native (PyO3 module)
  → Python shows QDialog
  → User interacts with dialog
  → Every button click / dropdown load calls deadline._native → Rust
  → Rust reads config, calls APIs, saves config
  → Python displays results
```

### DCC Plugin (e.g., Maya)

```
Maya Python plugin
  → Scene introspection via maya.cmds (Python, ~100-200 lines)
  → Builds scene_data dict
  → import deadline.client.ui (from gui/ package)
  → gui/ imports deadline._native (one-time, via dlopen)
  → Shows SubmitJobToDeadlineDialog (Python Qt)
  → User clicks Submit
  → Dialog calls deadline._native.create_job_from_job_bundle() → Rust
  → Returns job_id
```

DCC submitters use the same import paths as before (`from deadline.client.api
import ...`, `from deadline.client.ui.dialogs import ...`). The `gui/` package
re-exports these, backed by Rust via the PyO3 module.

### After Effects

```
ExtendScript (JSX)
  → system.callSystem("deadline bundle gui-submit ...")
  → deadline-cli binary handles everything
```

## Packaging (maturin + PyO3)

The root `pyproject.toml` uses maturin as the build backend. A single
`pip install deadline` command installs:

- The Rust CLI binary (`deadline`) on PATH
- The PyO3 extension module (`deadline._native.abi3.so`)
- The Python Qt GUI code (from `gui/`)

```
pip install deadline          # CLI + PyO3 module (no GUI deps)
pip install "deadline[gui]"   # Above + PySide6 + QtPy
```

Platform-specific wheels are built per OS/arch (linux-x64, macos-arm64,
windows-x64). The `abi3-py39` flag means one wheel per platform covers
all Python versions 3.9+.

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
- **API responses:** All API functions in `deadline-api` use the
  `ResponseBodyCapture` interceptor to return raw `serde_json::Value`.
  The CLI layer never sees SDK types. The PyO3 layer converts `Value` to
  Python dicts via `pythonize`. See `specs/patterns.md` § "AWS SDK for
  Rust Usage".
- **Config threading:** Functions that need config take `&IniConfig` (reads)
  or `&mut IniConfig` (writes). Convenience wrappers that hit disk exist but
  are not the primary API.
- **Credential scoping:** Non-Deadline AWS clients (CloudWatch Logs, S3)
  that access queue-scoped or fleet-scoped resources must use scoped
  credentials when the user is logged in via DCM. See
  [`patterns.md`](patterns.md) § "Credential Scoping for Non-Deadline
  AWS Services".
