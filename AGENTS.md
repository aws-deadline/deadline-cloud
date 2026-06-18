# AGENTS.md

This is the Rust implementation of the AWS Deadline Cloud client software:
CLI, Python bindings, and shared library crates.

## Documentation

Start with `README.md` for the repo overview, build instructions, and
Python GUI development setup.

Key top-level docs:
- `specs/architecture.md` — crate dependency graph, data flows, shared conventions
- `specs/testing.md` — test philosophy, levels, no-mocking policy, snapshot workflow
- `specs/patterns.md` — AWS SDK usage patterns, coding conventions

Deeper docs:
- `specs/` — per-crate subdirectories with architecture and topic-scoped specs
- `specs/deadline-cli/` — per-command CLI feature documentation
- `specs/deadline-python-bindings/` — PyO3 module architecture, DCC integration profiles
- `specs/rust-port-workflow.md` — development loop: study Python → write tests → implement → write spec → audit → fix → commit
- `test_fixtures/` — job bundles for manual CLI comparison testing (see `test_fixtures/README.md`)

## Architecture: Rust + Python

All business logic is in Rust. Python is only used for the Qt GUI:

```
deadline (Rust binary)
├── CLI commands, API calls, job attachments, config — all Rust
├── deadline._native (PyO3) — Rust library exposed to Python
└── GUI commands (bundle gui-submit, config gui):
      Rust validates args → finds Python → spawns subprocess
      → gui/_gui_entry.py → QApplication → Qt dialog → result
```

Key directories:
- `crates/` — Rust crates (CLI, lib, PyO3 bindings, test-server)
- `gui/` — Python GUI code (Qt widgets, dialogs, config shim, entry point)
- `pyproject.toml` — maturin config for building the `deadline` Python package

## Keeping docs in sync with code

- If you change a crate's public API or behavior, update `specs/{crate}/`.
- If you change a dependency direction or add a new crate, update `specs/architecture.md`.
- If you change a feature's design, update the relevant spec in `specs/`.

## Code style

- Comments explain *what* and *why*, not Rust language concepts.
- Test names: `{command_or_function}_{scenario}_{expected_outcome}`

## Build and test

```bash
cargo build                              # full workspace
cargo test                               # full test suite
cargo test -p deadline-lib              # library crate
cargo test -p deadline-cli               # CLI subprocess tests (Level 2)
cargo insta review                       # review new/changed CLI output snapshots
```

### Full test suite (Rust + Python)

`make test` runs Rust tests, xa11y UI tests, and PyO3 binding tests.
The Python tests require a virtual environment:

```bash
python3 -m venv .venv && source .venv/bin/activate
make setup-python                        # install Python deps (once)
make test                                # Rust + Python tests
```

If you only need Rust tests, `cargo test` works without a venv.

### CI

GitHub Actions (`.github/workflows/ci.yml`) runs fmt, cargo-deny, clippy,
and `cargo test` on ubuntu/macOS/Windows for every PR. A nightly conformance
workflow (`conformance.yml`) replays `deadline-cloud-python`'s `cli_e2e/` tests
against the Rust binary.

Key CI notes:
- `rust-toolchain.toml` pins the Rust version — CI uses this exact version.
- Some tests are `#[cfg(unix)]` (spawn `sh`); Windows twins are TODO.
- If you add a new `#[cfg(unix)]` gate, add a TODO comment and update `specs/progress.md`.
- Snapshot tests run on all 3 OSes — use cross-OS filters in `insta::Settings`.

### Python GUI development

The Python Qt GUI (`gui/`) provides the submit and config dialogs.
GUI commands (`bundle gui-submit`, `config gui`) spawn a Python
subprocess that runs the PySide6 dialog.

To run GUI accessibility tests:
```bash
make test-ui                             # xa11y tests against Python Qt GUI
```

GUI commands require Python 3.9+ and PySide6 installed.
