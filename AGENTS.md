# AGENTS.md

This is the Rust implementation of the AWS Deadline Cloud CLI.

## Before you start

1. Read `docs/ARCHITECTURE.md` — crate dependency graph and shared conventions.
2. Read `docs/specs/<crate>.md` for whichever crate you're working on.
3. Read `docs/TESTING.md` for how tests are structured (Level 1 vs Level 3, no mocking).

## Keeping docs in sync with code

- **`docs/specs/`** — One file per crate. Describes what the code currently does.
  If you change a crate's public API or behavior, update its spec before committing.
- **`docs/ARCHITECTURE.md`** — Describes how crates relate to each other.
  If you change a dependency direction or add a new crate, update this file.
- **`docs/designs/`** — Topic-scoped documents (TDDs, feature designs, decision
  records). These cover features that may span multiple crates. Named
  `YYYY-MM-topic.md`. If you change a feature's design, update the relevant
  design doc.

## Code style

- Comments explain *what* and *why*, not Rust language concepts.
- Test names: `{command_or_function}_{scenario}_{expected_outcome}`
- Every test gets a `// §N case M` traceability comment linking to the test spec.
- No mocking. Use real temp directories, real HTTP servers (wiremock).

## Build and test

```bash
cargo build                    # full workspace
cargo test                     # full test suite
cargo test -p deadline-config  # single crate
cargo test -p deadline-cli     # CLI subprocess tests (Level 3)
```
