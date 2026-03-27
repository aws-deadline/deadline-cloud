# Rust Rewrite

Migrating all AWS Deadline Cloud client software from Python to Rust.
The Python source lives at `../deadline-cloud-python`.

## Why Rust?

Speed, type safety, and development velocity. See `migration_strategy.md` for
the full rationale, downstream dependency analysis, and phased rollout plan.

## Getting started

1. Read `migration_strategy.md` — goals, architecture, what we're migrating,
   and how we ship iteratively.
2. Read `workflow.md` — the development cycle for porting each feature.
3. Read `../../../AGENTS.md` — repo-wide conventions, build commands, doc-sync rules.
4. Read `../../TESTING.md` — testing philosophy, TDD, test levels, no mocking.
5. Check the **Progress** table below to find what's in progress or next.
6. Read `../../specs/<crate>.md` for the crate you'll be working on.
7. Read the relevant `test_specs/` section for behavioral test cases.

## Reference Material

- `migration_strategy.md` — Goals, GUI strategy, DCC plugin constraints,
  downstream dependency analysis, phased rollout, decision record
- `workflow.md` — Development workflow: study Python → update spec → red →
  green → refactor → update docs
- `data_flow.md` — Persistent data formats (INI config, job bundle layout,
  manifest JSON, cache schemas, checkpoint format)
- `observations.md` — Behavioral notes and ambiguities discovered during
  analysis
- `test_specs/` — Behavioral test specifications (52 sections, ~1,300 cases).
  Used as the implementation checklist — not maintained after tests are written.

## Progress

### Implemented

| Crate | Sections | What's done |
|-------|----------|-------------|
| `deadline-models` | §51, §52 | Error types, submitter info |
| `deadline-common` | §36 | Path utilities |
| `deadline-config` | §1 (partial) | INI read/write, hierarchical settings, str2bool, get/set/clear |
| `deadline-test-server` | — | TestHarness, wiremock stub server scaffolding |
| `deadline-cli` | §37 (cases 1-2), §38 (cases 1-8) | `--version`, `--help`, config subcommands (show, get, set, clear), Level 2 tests |

### In Progress

| Crate | Sections | What's next |
|-------|----------|-------------|
| `deadline-cli` | §37 (cases 3-58) | Root group utilities (log level, redirect, markdown strip, SIGINT, etc.) |

### Not Started

| Crate | Phase | Sections | Scope |
|-------|-------|----------|-------|
| `deadline-config` | 1 | §1 (remaining), §2 | Profile resolution, get_best_profile_for_farm |
| `deadline-cli` | 1 | §37-49 | All remaining CLI commands |
| `deadline-client` | 1 | §3-14, §34 | Session, auth, API resource mgmt, job lifecycle, telemetry |
| `deadline-job-bundle` | 1 | §15-18 | Bundle loading, parameters, history |
| `deadline-job-attachments` | 1 | §19-35 | Models, hashing, upload, download, caches, manifests, VFS, path mapping |
| `deadline-common` | 1 | §50 | MCP server (deferred) |
| `deadline-worker-agent` | 2 | — | Session mgmt, attachment sync, progress reporting |
| `deadline-gui-ffi` | 3 | — | C ABI shared library for GUI + DCC plugins |
| `gui/` (Python) | 3 | — | Refactored QWidgets layout calling Rust FFI |
| `deadline-mcp` | 5 | §49, §50 | MCP server binary (rmcp SDK) |
