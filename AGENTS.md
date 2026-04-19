# AGENTS.md

This is the Rust implementation of the AWS Deadline Cloud client software:
CLI, GUI FFI layer, and shared library crates.

## Documentation

Start with `README.md` for the repo overview and documentation layout.

Key top-level docs:
- `specs/architecture.md` — crate dependency graph, data flows, shared conventions
- `specs/testing.md` — test philosophy, levels, no-mocking policy, snapshot workflow
- `specs/patterns.md` — AWS SDK usage patterns, coding conventions

Deeper docs:
- `specs/` — per-crate subdirectories with architecture and topic-scoped specs
- `specs/cli/` — per-command CLI feature documentation
- `specs/workflow.md` — development loop: study Python → write tests → implement → write spec → audit → fix → commit
- `test_fixtures/` — job bundles for manual CLI comparison testing (see `test_fixtures/README.md`)

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
cargo test -p deadline-config            # single crate
cargo test -p deadline-cli               # CLI subprocess tests (Level 2)
cargo test -p deadline-gui-ffi           # GUI FFI tests
cargo test -p deadline-mcp               # MCP server tests
cargo insta review                       # review new/changed CLI output snapshots
```
