# AGENTS.md

This is the Rust implementation of the AWS Deadline Cloud client software:
CLI, GUI FFI layer, and shared library crates.

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

### Python GUI development

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install maturin PySide6-essentials qtpy pyyaml pytest-qt
maturin develop                          # build PyO3 module + install gui/ package
pytest gui/tests/ -v                     # run Python tests
```

GUI commands need Python with PySide6. The Rust CLI finds Python via:
1. `DEADLINE_PYTHON` env var
2. `_internal/Python` relative to binary (installer layout)
3. `python3` / `python` on PATH
