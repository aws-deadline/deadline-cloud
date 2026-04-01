# Documentation

## Reading order

1. [`designs/rust-rewrite/migration_strategy.md`](designs/rust-rewrite/migration_strategy.md) — goals, what we're migrating, architecture, and phased rollout.
2. [`ARCHITECTURE.md`](ARCHITECTURE.md) — crate dependency graph and shared conventions.
3. [`specs/<crate>.md`](specs/) for whichever crate you're working on.
4. [`TESTING.md`](TESTING.md) — test philosophy (Level 1 vs Level 2, no mocking).

## Layout

| Path | What it contains |
|------|-----------------|
| [`ARCHITECTURE.md`](ARCHITECTURE.md) | Crate dependency graph, data flows, shared conventions |
| [`TESTING.md`](TESTING.md) | Test philosophy, no-mocking policy, insta snapshots |
| [`specs/`](specs/) | One file per crate — describes what the code currently does |
| [`designs/`](designs/) | Technical design documents, feature designs, and decision records (one subdirectory per topic) |
