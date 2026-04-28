# deadline-cloud-rs

Rust implementation of the [AWS Deadline Cloud](https://aws.amazon.com/deadline-cloud/)
client software: CLI, GUI FFI layer, and shared library crates.

## Prerequisites

- [Rust](https://rustup.rs/) (stable, edition 2024)
- `cargo-insta` for snapshot test review: `cargo install cargo-insta`
- Python 3.9+ (for GUI commands and Python bindings development)

## Build

```bash
# Full build + install into venv (recommended)
make

# Or individual steps:
cargo build                    # full workspace (Rust only)
cargo build -p deadline-cli    # just the CLI binary
make wheel                     # build distributable .whl
```

After `make`, the `deadline` CLI is on PATH (in the active venv) and
`deadline._native` is importable from Python.

The CLI binary is also at `target/debug/deadline`.

## Test

```bash
make test                      # Rust + Python tests
cargo test                     # Rust tests only
cargo test -p deadline-cli     # CLI subprocess tests
cargo test -p deadline-config  # single crate
cargo insta review             # review new/changed output snapshots
```

See [specs/testing.md](specs/testing.md) for the test philosophy (no mocking, Level 1
vs Level 2, snapshot workflow).

## Python GUI Development

The CLI binary handles all business logic in Rust. GUI commands
(`bundle gui-submit`, `config gui`) spawn a Python subprocess that runs
Qt dialogs. The Python code lives in `gui/` and calls back into Rust
via the `deadline._native` PyO3 module.

### Setup

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install maturin PySide6-essentials qtpy pyyaml pytest-qt
make                           # builds everything + installs into venv
```

### Running GUI commands

```bash
# After maturin develop, the Rust CLI finds Python on PATH:
./target/debug/deadline bundle gui-submit /path/to/job/bundle
./target/debug/deadline config gui

# Or point to a specific Python:
DEADLINE_PYTHON=.venv/bin/python ./target/debug/deadline config gui
```

### Testing Python code

```bash
pytest gui/tests/ -v                    # all Python tests (needs PySide6)
pytest gui/tests/test_native.py -v      # PyO3 bindings tests
pytest gui/tests/test_gui_entry.py -v   # GUI entry point tests (needs PySide6)
```

### How it works

```
deadline (Rust binary)
  ├── All CLI commands (clap parsing, API calls, job attachments, etc.)
  └── GUI commands only:
        ├── Validate args in Rust
        ├── Find Python: DEADLINE_PYTHON env → _internal/Python → PATH
        └── Spawn: python -m deadline.client.ui._gui_entry <command> --params-json '{...}'
              ├── Creates QApplication
              ├── Opens Qt dialog (uses deadline._native for business logic)
              └── Prints result to stdout
```

## Crates

| Crate | What it does |
|-------|-------------|
| `deadline-cli` | CLI binary — argument parsing, subcommand dispatch, output formatting |
| `deadline-python-bindings` | PyO3 extension module (`deadline._native`) for Python GUI and DCC plugins |
| `deadline-config` | INI config file read/write, setting resolution |
| `deadline-api` | AWS Deadline Cloud API, session, auth, telemetry |
| `deadline-job-bundle` | Job bundle parsing, template loading, parameter validation |
| `deadline-job-attachments` | Asset manifests, S3 upload/download, hash cache |
| `deadline-test-server` | Test-only wiremock stub server |

See [specs/architecture.md](specs/architecture.md) for the crate dependency graph and
data flows.

## Python GUI Code

| Path | Contents |
|------|----------|
| `gui/deadline/client/ui/` | Qt widgets, dialogs, controllers (PySide6/qtpy) |
| `gui/deadline/client/ui/_gui_entry.py` | Entry point spawned by Rust CLI for GUI commands |
| `gui/deadline/client/config/` | Config shim routing through `deadline._native` |
| `gui/deadline/client/dataclasses/` | `SubmitterInfo` and other shared types |
| `gui/deadline/client/job_bundle/` | Job bundle YAML/parameter handling for GUI |
| `gui/tests/` | Python tests (pytest + pytest-qt) |
| `pyproject.toml` | maturin build config for the `deadline` Python package |

## Documentation

| Path | Contents |
|------|----------|
| [specs/](specs/) | All design specifications |
| [specs/architecture.md](specs/architecture.md) | Crate dependency graph, data flows |
| [specs/testing.md](specs/testing.md) | Test philosophy, no-mocking policy |
| [specs/patterns.md](specs/patterns.md) | AWS SDK usage patterns, coding conventions |
| [specs/deadline-cli/](specs/deadline-cli/) | Per-command CLI feature documentation |
| [specs/deadline-cli/reference.md](specs/deadline-cli/reference.md) | Complete CLI command reference |
| [specs/deadline-python-bindings/](specs/deadline-python-bindings/) | PyO3 module architecture, DCC profiles |
| [specs/progress.md](specs/progress.md) | Work item tracking |
| [specs/HANDOFF.md](specs/HANDOFF.md) | Current/next work item for session continuity |

## License

Apache-2.0
