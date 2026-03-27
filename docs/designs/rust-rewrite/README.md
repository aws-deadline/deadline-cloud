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
3. Read `../../AGENTS.md` — repo-wide conventions, build commands, doc-sync rules.
4. Read `../../TESTING.md` — testing philosophy, TDD, test levels, no mocking.
5. Check the **Progress** tables below to find what's in progress or next.
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

---

## Overall Progress

| Phase | Goal | Status |
|-------|------|--------|
| 1 | Rust CLI binary | 🔧 In progress |
| 2 | Rust worker agent | ⬜ Not started (crate scaffolded) |
| 3 | GUI FFI + Python widget refactor | ⬜ Not started (crate scaffolded) |
| 4 | Migrate DCC submitter plugins (7 repos) | ⬜ Not started |
| 5 | Rust MCP server | ⬜ Not started (crate scaffolded) |
| 6 | Migrate Unreal to full Rust | ⬜ Not started |
| 7 | Delete `deadline-cloud-python` | ⬜ Blocked on phases 1-6 |

---

## Phase 1: Rust CLI Binary

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

| Crate | Sections | Scope |
|-------|----------|-------|
| `deadline-config` | §1 (remaining), §2 | Profile resolution, get_best_profile_for_farm |
| `deadline-client` | §3-14, §34 | Session, auth, API resource mgmt, job lifecycle, telemetry |
| `deadline-job-bundle` | §15-18 | Bundle loading, parameters, history |
| `deadline-job-attachments` | §19-35 | Models, hashing, upload, download, caches, manifests, VFS, path mapping |
| `deadline-cli` | §39-49 | Auth, farm, fleet, queue, worker, job, bundle, attachment, manifest, handle-web-url commands |

---

## Phase 2: Rust Worker Agent

| Item | Status |
|------|--------|
| `deadline-worker-agent` crate scaffolded | ✅ Done |
| Write test specs for worker agent behavior | ⬜ Not started |
| Session management, polling, action execution | ⬜ Not started |
| Attachment sync (download/upload) | ⬜ Not started |
| Progress reporting | ⬜ Not started |
| Integration tests against existing suite | ⬜ Not started |
| Canary fleet deployment | ⬜ Not started |

**Prerequisite:** Phase 1 library crates complete.

---

## Phase 3: GUI FFI + Python Widget Refactor

| Item | Status |
|------|--------|
| `deadline-gui-ffi` crate scaffolded | ✅ Done |
| FFI callback design documented | ✅ Done (see `../../specs/deadline-gui-ffi.md`) |
| Implement C ABI functions (config, auth, listing, submission) | ⬜ Not started |
| Refactor Python GUI — remove business logic, replace with ctypes | ⬜ Not started |
| Move Python GUI files into monorepo `gui/` directory | ⬜ Not started |
| Wire CLI GUI commands to spawn Python process | ⬜ Not started |
| Test on Linux, macOS, Windows | ⬜ Not started |

**Prerequisite:** Phase 1 library crates complete.

---

## Phase 4: Migrate DCC Submitter Plugins

| Plugin | Status | Complexity |
|--------|--------|------------|
| Blender | ⬜ Not started | Low (prove the pattern) |
| Nuke | ⬜ Not started | Low |
| Cinema 4D | ⬜ Not started | Low |
| VRED | ⬜ Not started | Low |
| Maya | ⬜ Not started | Medium |
| 3ds Max | ⬜ Not started | Medium |
| Houdini | ⬜ Not started | High (custom submission flow) |

**Prerequisite:** Phase 3 complete (GUI FFI working).

---

## Phase 5: Rust MCP Server

| Item | Status |
|------|--------|
| `deadline-mcp` crate scaffolded | ✅ Done |
| Implement 16 MCP tools using `rmcp` SDK | ⬜ Not started |
| Wire into `deadline-cli` as `deadline mcp-server` subcommand | ⬜ Not started |
| Telemetry per tool invocation | ⬜ Not started |

**Prerequisite:** Phase 1 library crates complete (`deadline-client` for API calls).

---

## Phase 6: Migrate Unreal to Full Rust

| Item | Status |
|------|--------|
| Rewrite Unreal submitter using `deadline-gui-ffi` C ABI | ⬜ Not started |

**Prerequisite:** Phase 3 complete.

---

## Phase 7: Delete `deadline-cloud-python`

| Item | Status |
|------|--------|
| Verify no downstream repo imports `deadline.client` or `deadline.job_attachments` | ⬜ Not started |
| Delete the repo | ⬜ Not started |

**Prerequisite:** All phases above complete.
