# AGENTS.md

This is the Rust implementation of the AWS Deadline Cloud client software:
CLI, GUI FFI layer, and shared library crates. The Python GUI
widget files also live in this repo under `gui/`.

## Documentation

Start with `README.md` for the repo overview and documentation layout.

Key top-level docs:
- `ARCHITECTURE.md` — crate dependency graph, data flows, shared conventions
- `TESTING.md` — test philosophy, levels, no-mocking policy, snapshot workflow
- `PATTERNS.md` — AWS SDK usage patterns, coding conventions

Deeper docs:
- `docs/crate_specs/` — one file per crate, describes current behavior and design
- `docs/design_docs/` — topic-scoped TDDs, feature designs, decision records

## Keeping docs in sync with code

- If you change a crate's public API or behavior, update `docs/crate_specs/<crate>.md`.
- If you change a dependency direction or add a new crate, update `ARCHITECTURE.md`.
- If you change a feature's design, update the relevant doc in `docs/design_docs/`.

## Code style

- Comments explain *what* and *why*, not Rust language concepts.
- Test names: `{command_or_function}_{scenario}_{expected_outcome}`

## Build and test

```bash
cargo build                              # full workspace
cargo test                               # full test suite
cargo test -p deadline-config            # single crate
cargo test -p deadline-cli               # CLI subprocess tests (Level 2)
cargo test -p deadline-gui-ffi           # GUI FFI tests
cargo insta review                       # review new/changed CLI output snapshots
```
