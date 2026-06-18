# deadline-cloud-rs

Rust implementation of the [AWS Deadline Cloud](https://aws.amazon.com/deadline-cloud/)
client software: CLI, GUI FFI layer, and shared library crates.

## Prerequisites

- [Rust](https://rustup.rs/) (stable, edition 2024)
- Python 3.9+ (for GUI commands and Python bindings development)
- Dev tools: `make setup` (installs Cargo tools + Python packages)

See [DEVELOPMENT.md](DEVELOPMENT.md) for the full setup guide.

## Build

```bash
make build                     # Rust binary + Python extension
make build-rust                # Rust binary only (no venv needed)
make build-python              # PyO3 extension into venv (maturin develop)
cargo build -p deadline-cli    # just the CLI binary
```

After `make build`, the `deadline` CLI binary is at `target/debug/deadline`
and `deadline._native` is importable from Python.

## Test

```bash
make test                      # all tests (Rust + xa11y UI + PyO3 bindings)
make test-rust                 # Rust tests only (no venv needed)
make test-ui                   # xa11y GUI accessibility tests
make test-bindings             # PyO3 binding tests
cargo test -p deadline-lib     # single crate
cargo insta review             # review new/changed output snapshots
```

See [specs/testing.md](specs/testing.md) for the test philosophy (no mocking, Level 1
vs Level 2, snapshot workflow). See [DEVELOPMENT.md](DEVELOPMENT.md) §
"UI accessibility tests" for xa11y setup.

## CI

GitHub Actions runs on every PR and push to mainline:

| Job | What |
|-----|------|
| Rustfmt | `cargo fmt --check` |
| cargo-deny | License/advisory/ban checks |
| Build & Test | Clippy + `cargo test` on **ubuntu, macOS, Windows** |
| Documentation | `cargo doc` with `-D warnings` |

A **nightly conformance workflow** replays `deadline-cloud-python`'s
`test/cli_e2e/` suite against the Rust binary to catch parity drift
(`.github/workflows/conformance.yml`).

See [specs/HANDOFF.md](specs/HANDOFF.md) § "GitHub CI/CD Architecture"
for full details.

## Python GUI Development

The CLI binary handles all business logic in Rust. GUI commands
(`bundle gui-submit`, `config gui`) spawn a Python subprocess that runs
Qt dialogs. The Python code lives in `gui/` and calls back into Rust
via the `deadline._native` PyO3 module.

### Setup

```bash
python3 -m venv .venv && source .venv/bin/activate
make setup-python              # install Python deps (once)
make build                     # builds Rust binary + installs extension into venv
```

### Running GUI commands

```bash
# After make build, the Rust CLI finds Python on PATH:
./target/debug/deadline bundle gui-submit /path/to/job/bundle
./target/debug/deadline config gui

# Or point to a specific Python:
DEADLINE_PYTHON=.venv/bin/python ./target/debug/deadline config gui
```

### Testing Python code

```bash
make test-bindings             # PyO3 binding tests (builds extension first)
make test-ui                   # xa11y GUI tests (builds binary first)
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
| `deadline-lib` | Unified library: config, API, job bundles, job attachments |
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
| `pytests/bindings/` | PyO3 binding tests |
| `pytests/ui_accessibility/` | xa11y GUI accessibility tests |
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
