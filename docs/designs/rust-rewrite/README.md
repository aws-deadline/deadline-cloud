# Rust Rewrite

Migrating all AWS Deadline Cloud client software from Python to Rust.
The Python source lives at `../deadline-cloud-python`.

## Why Rust?

Speed, type safety, and development velocity. See `migration_strategy.md` for
the full rationale, downstream dependency analysis, and phased rollout plan.

## Getting started

Read `workflow.md` and follow it. Step 0 contains the full reading checklist
and gates all planning and implementation work.

## Reference Material

- `migration_strategy.md` — Goals, GUI strategy, DCC plugin constraints,
  downstream dependency analysis, phased rollout, decision record
- `workflow.md` — Development workflow: study Python → update spec → red →
  green → verify against Python CLI → refactor → update docs.
  Also documents AWS SDK for Rust usage patterns.
- `gui_ffi_architecture.md` — Detailed diagrams of the FFI layer: threading
  model, callback flow, memory ownership, data flow for auth status and
  progress reporting, risk table for the spike.
- `data_flow.md` — Persistent data formats (INI config, job bundle layout,
  manifest JSON, cache schemas, checkpoint format)
- `observations.md` — Behavioral notes and ambiguities discovered during
  analysis
- `test_specs/` — Behavioral test specifications (52 sections, ~1,300 cases).
  Used as the implementation checklist — not maintained after tests are written.

## Progress

### Risk Spikes (must pass before bulk implementation)

All passed. See `migration_strategy.md` § "Fail-Fast Strategy" for details.

| Spike | Status | Proves |
|-------|--------|--------|
| GUI FFI round-trip (Python ↔ Rust ↔ Qt) | ✅ Passed | Core architecture works: ctypes loading, C ABI calls, callbacks, thread safety |
| GUI FFI inside DCC (Blender) | ✅ Passed | Shared library loads in real DCC Python environment without conflicts |
| S3 transfer performance | ✅ Passed | Rust S3 throughput ≥ Python boto3 transfer manager (see `docs/specs/deadline-job-attachments.md` § "S3 Transfer Spike") |
| Job attachment hashing | ✅ Passed | Parallel xxh128 hashing is faster than Python, hashes match byte-for-byte |

### Work Items

Each row is a self-contained unit of work. See `workflow.md` for how to
pick and execute work items.

| # | Work Item | Status | Library §§ | CLI §§ | Test Spec Files | Depends On |
|---|-----------|--------|-----------|--------|-----------------|------------|
| 0a | Error types, submitter info | ✅ Done | §51, §52 | — | `common.md` | — |
| 0b | Path utilities | ✅ Done | §36 | — | `common.md` | — |
| 0c | TelemetryClient (common) | ✅ Done | §14 (partial) | — | `api_job_lifecycle.md` | — |
| 0d | Config read/write | ✅ Done | §1, §2 | §38 cases 1-8 | `config.md`, `cli.md` | — |
| 0e | Test server infrastructure | ✅ Done | — | — | — | — |
| 0f | CLI root & common utilities | ✅ Done | — | §37 | `cli.md` | 0d |
| 0g | Session creation & auth status | ✅ Done | §4, §7 | §39 cases 4-7, §40-44 list/get | `session.md`, `api_resource_management.md`, `cli.md` | 0d |
| 0h | Queue/job credentials & diagnostics | ✅ Done | §9, §10, §13 | §42 cases 8-10, §44 diagnostics | `api_resource_management.md`, `api_job_lifecycle.md`, `cli.md` | 0g |
| 0i | GUI FFI spike | ✅ Done | — | — | — | 0g |
| 1 | Session caching & user-agent | ✅ Done | §3 (28 cases) | — | `session.md` | 0g |
| 2 | Login/logout | ✅ Done | §6 (18 cases) | §39 cases 1-3 | `api_resource_management.md`, `cli.md` | 1 |
| 3 | Queue user credentials | ✅ Done | §5 (20 cases) | — | `session.md` | 1 |
| 4 | Queue parameters | ✅ Done | §8 (10 cases) | §42 cases 6-7 | `api_resource_management.md`, `cli.md` | 1 |
| 5 | Telemetry API integration | ✅ Done | §14 (24 cases) | §42 cases 11-13 | `api_job_lifecycle.md`, `cli.md` | 1 |
| 6 | Job monitoring & logs | ✅ Done | §12 (35 cases) | §44 cases 17-24 | `api_job_lifecycle.md`, `cli.md` | 1 |
| 7 | Job bundle | ✅ Done | §15-18 (174 cases) | §45 case 14 | `job_bundle.md`, `cli.md` | 1, 4 |
| 8 | Job attachments: core | ✅ Done | §19-20, §24-25, §33 (123 cases) | — | `job_attachments_data_transfer.md`, `job_attachments_orchestration.md` | 1 |
| 9 | Job attachments: transfer | In progress | §21-22, §30-31, §34 (226 cases) | §46 (15 cases) | `job_attachments_data_transfer.md`, `cli.md` | 3, 8 |
| 10 | Job attachments: orchestration | Not started | §23, §26-29, §32, §35 (325 cases) | §47 (26 cases) | `job_attachments_orchestration.md`, `job_attachments_data_transfer.md`, `cli.md` | 9 |
| 11 | Submit job bundle | Not started | §11 (42 cases) | §45 cases 1-13 | `api_job_lifecycle.md`, `cli.md` | 7, 9 |
| 12 | Job cancel | ✅ Done | — | §44 cases 25-26 | `cli.md` | 6 |
| 12b | Job search command | ✅ Done | — | §44 cases 6-9 | `cli.md` | 6 |
| 13 | Job download & sync-output | Not started | — | §44 cases 10-16, §42 cases 14-26 | `cli.md` | 10 |
| 14 | Handle web URL | Not started | — | §48 (14 cases) | `cli.md` | 13 |
| 15 | Job requeue-tasks | ✅ Done | — | §44 cases 29-42 | `cli.md` | 6 |
| 15b | Job get search & estimated time | ✅ Done | — | §44 cases 1-5 | `cli.md` | 6 |
| 15c | Job logs auto-selection messages | ✅ Done | — | §44 cases 17-21 | `cli.md` | 6 |
| 15d | Level 2 test coverage audit | Not started | — | — | — | 11 |
| 16 | GUI FFI remaining | Deferred | TBD | — | — | 1-14 |
| 17 | MCP server | Deferred | §49, §50 | — | `cli.md`, `mcp.md` | 1-14 |
| 18 | Worker agent | Deferred | §53+ | — | — | 1-17 |

**Status key:** ✅ Done · In progress · Not started · Deferred (blocked on CLI completion)

**In-progress details:** See `HANDOFF.md` for current state of any
"In progress" work items.

**Deferred items:** #16-17 (GUI FFI, MCP) ship as part of the CLI deliverable
after the core CLI commands are complete. #18 (worker agent) is deferred until
the entire CLI — including GUI FFI and MCP — is done. The CLI exercises all
the same library crates, so completing it first means battle-tested foundations
for the worker agent.

**Deferred features within completed items:**
- **#6**: `deadline job trace-schedule` (§44 cases 27-28) is EXPERIMENTAL
  in Python and deferred. `--session-action-id` for `job logs` is also
  deferred. `--timezone` deprecated flag not implemented.

**Technical debt:**
- **#15d**: Audit all Level 1 tests in library crates (`deadline-job-bundle`,
  `deadline-job-attachments`, etc.) to identify which can be converted to or
  supplemented with Level 2 CLI subprocess tests. Per TESTING.md rule 1: "If
  the CLI can exercise it, test it through the CLI." Once `bundle submit` (#11)
  and other CLI commands exist, many library behaviors become CLI-reachable.
  Add `assert_cmd_snapshot!` tests for those paths. Keep Level 1 tests only
  for behaviors too low-level to assert through CLI output.
- **Realistic test IDs**: Replace hardcoded pseudo-IDs in tests (e.g.
  `"farm-1"`, `"queue-1"`, `"job-1"`, `"step-1"`, `"task-1"`,
  `"session-1"`, `"sessionaction-1"`, `"fleet-1"`) with pseudorandomly
  generated IDs that follow the actual Deadline Cloud ID format (e.g.
  `farm-0123456789abcdef0123456789abcdef`,
  `queue-0123456789abcdef0123456789abcdef`). Add a test helper that
  generates realistic IDs per resource type. Applies across all crates.
- **Crate rename discussion**: Consider renaming `deadline-client` to
  something more specific (e.g. `deadline-api` or `deadline-service`).
  Currently it holds Deadline Cloud API calls, session/credential management,
  auth, job monitoring, and log retrieval. The name "client" is ambiguous —
  it could mean "the client library" (the whole workspace) or "the Deadline
  service client" (what it actually is). With `deadline-job-attachments`
  building its own S3/STS clients independently, the distinction matters.
  Discuss before the worker agent work (#18) when both crates are consumed
  together.

