# AGENTS.md

This is the Rust implementation of the AWS Deadline Cloud client software:
CLI, GUI FFI layer, and shared library crates. The Python GUI
widget files also live in this repo under `gui/`.

## Before you start

Read `docs/README.md` for the documentation reading order and layout.

## Keeping docs in sync with code

- **`docs/specs/`** — One file per crate. Describes what the code currently does.
  If you change a crate's public API or behavior, update its spec before committing.
- **`docs/ARCHITECTURE.md`** — Describes how crates relate to each other.
  If you change a dependency direction or add a new crate, update this file.
- **`docs/designs/`** — Topic-scoped documents (TDDs, feature designs, decision
  records). These cover features that may span multiple crates. If you change
  a feature's design, update the relevant design doc.

## Code style

- Comments explain *what* and *why*, not Rust language concepts.
- Test names: `{command_or_function}_{scenario}_{expected_outcome}`
- No mocking. Use real temp directories, real HTTP servers (wiremock).

## Build and test

```bash
cargo build                              # full workspace
cargo test                               # full test suite
cargo test -p deadline-config            # single crate
cargo test -p deadline-cli               # CLI subprocess tests (Level 2)
cargo test -p deadline-gui-ffi           # GUI FFI tests
cargo insta review                       # review new/changed CLI output snapshots
```
