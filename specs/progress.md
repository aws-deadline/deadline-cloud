# Progress

AWS Deadline Cloud Rust CLI — work item tracking.

## Getting started

Read `workflow.md` and follow it. The Session Start section contains the
full reading checklist and gates all planning and implementation work.

## Reference Material

Historical migration documents are in `specs/archive/`.

## Progress

### Risk Spikes (must pass before bulk implementation)

All passed. See `migration_strategy.md` for details.

| Spike | Status | Proves |
|-------|--------|--------|
| GUI FFI round-trip (Python ↔ Rust ↔ Qt) | ✅ Passed | Core architecture works: ctypes loading, C ABI calls, callbacks, thread safety |
| GUI FFI inside DCC (Blender) | ✅ Passed | Shared library loads in real DCC Python environment without conflicts |
| S3 transfer performance | ✅ Passed | Rust S3 throughput ≥ Python boto3 transfer manager (see `specs/job-attachments/architecture.md`) |
| Job attachment hashing | ✅ Passed | Parallel xxh128 hashing is faster than Python, hashes match byte-for-byte |

### Work Items

Each row is a self-contained unit of work. See `workflow.md` for how to
pick and execute work items.

| # | Work Item | Status | Test Spec Files | Depends On |
|---|-----------|--------|-----------------|------------|
| 0a | Error types, submitter info | ✅ Done | `common.md` | — |
| 0b | Path utilities | ✅ Done | `common.md` | — |
| 0c | TelemetryClient (common) | ✅ Done | `api_job_lifecycle.md` | — |
| 0d | Config read/write | ✅ Done | `config.md`, `cli.md` | — |
| 0e | Test server infrastructure | ✅ Done | — | — |
| 0f | CLI root & common utilities | ✅ Done | `cli.md` | 0d |
| 0g | Session creation & auth status | ✅ Done | `session.md`, `api_resource_management.md`, `cli.md` | 0d |
| 0h | Queue/job credentials & diagnostics | ✅ Done | `api_resource_management.md`, `api_job_lifecycle.md`, `cli.md` | 0g |
| 0i | GUI FFI spike | ✅ Done | — | 0g |
| 1 | Session caching & user-agent | ✅ Done | `session.md` | 0g |
| 2 | Login/logout | ✅ Done | `api_resource_management.md`, `cli.md` | 1 |
| 3 | Queue user credentials | ✅ Done | `session.md` | 1 |
| 4 | Queue parameters | ✅ Done | `api_resource_management.md`, `cli.md` | 1 |
| 5 | Telemetry API integration | ✅ Done | `api_job_lifecycle.md`, `cli.md` | 1 |
| 6 | Job monitoring & logs | ✅ Done | `api_job_lifecycle.md`, `cli.md` | 1 |
| 7 | Job bundle | ✅ Done | `job_bundle.md`, `cli.md` | 1, 4 |
| 8 | Job attachments: core | ✅ Done | `job_attachments_data_transfer.md`, `job_attachments_orchestration.md` | 1 |
| 9 | Job attachments: transfer | ✅ Done | `job_attachments_data_transfer.md`, `cli.md` | 3, 8 |
| 10 | Job attachments: orchestration | ✅ Done | `job_attachments_orchestration.md`, `job_attachments_data_transfer.md`, `cli.md` | 9 |
| 11 | Submit job bundle | ✅ Done | `api_job_lifecycle.md`, `cli.md` | 7, 9 |
| 12 | Job cancel | ✅ Done | `cli.md` | 6 |
| 12b | Job search command | ✅ Done | `cli.md` | 6 |
| 13 | Job download & sync-output | ✅ Done | `job_attachments_orchestration.md`, `job_attachments_data_transfer.md`, `cli.md` | 9 |
| 14 | Handle web URL | ✅ Done | `cli.md` | 13 |
| 15 | Job requeue-tasks | ✅ Done | `cli.md` | 6 |
| 15b | Job get search & estimated time | ✅ Done | `cli.md` | 6 |
| 15c | Job logs auto-selection messages | ✅ Done | `cli.md` | 6 |
| 15d | Level 2 test coverage audit | ✅ Done | — | 11 |
| 15e | Behavioral parity audit | ✅ Done | — | 9 |
| 15f | Wire queue/fleet assume role for all existing CLI commands | ✅ Done | `credential_scoping.md` | 15e-F1 |
| 16 | GUI FFI remaining | In progress | — | 1-14 |
| 16a | FFI: config, resource listing, auth functions | ✅ Done | — | 0i |
| 16b | FFI: submission with callbacks, telemetry | ✅ Done | — | 16a, 11 |
| 16c | Python FFI wrapper (`gui/_ffi.py`) | ✅ Done | — | 16b |
| 16d | Port Python Qt code into `gui/` package | ✅ Done | — | 16c |
| 16e | Python packaging (`gui/pyproject.toml`) | Not started | — | 16d |
| 16f | DCC submitter dependency switchover | Not started | — | 16e |
| 17 | MCP server | ✅ Done | `mcp.md` | 1-14 |
| 18 | Submission hooks | ✅ Done | `submission_hooks.md` | 11 |
| 19 | Update checker | ✅ Done | `new_features.md` | 0g |
| 20 | Batch get API helper | ✅ Done | `new_features.md` | — |
| 21 | Python bug-fix parity sweep | ✅ Done | `new_features.md` | — |
| 22 | Fuzz testing | Not started | — | — |
| 23 | Failure case handling analysis | Not started | — | — |

**Status key:** ✅ Done · ⚠️ Gaps · In progress · Not started · Deferred

**Dependency status:** All core feature dependencies are resolved.
No items remain as ⚠️ Gaps. #16 (GUI FFI) is in progress.

**Next action item:** Pick next work item from the table.

**In-progress details:** See `HANDOFF.md` for current state of any
"In progress" work items.

**Audit status:** See `audit_reports/2026-04-17-behavioral-parity.md`.
All findings resolved (0 remaining).

**GUI FFI migration plan (#16a-16f):**

`deadline-cloud-rs` is a complete replacement for `deadline-cloud-python`.
The Python Qt GUI code must ship from this repo, backed by the Rust shared
library. Remaining sub-items:

- **#16e — Python packaging** (Not started): Create `gui/pyproject.toml`
  so the Python Qt code is installable as a package. The package must
  bundle or locate the Rust shared library.
- **#16f — DCC submitter switchover** (Not started): Update each DCC
  submitter repo to depend on the new Python package from
  `deadline-cloud-rs` instead of `deadline-cloud-python`.

**Technical debt:**
- **Spec docs audit**: Review `specs/` docs to ensure they reflect
  current implementation.
- **#15d**: Audit Level 1 tests for conversion to Level 2.
- **Realistic test IDs**: Replace hardcoded pseudo-IDs with realistic
  Deadline Cloud ID format.
- **Test consolidation**: Audit for redundant/overlapping tests.
- **#23 — Failure case handling analysis**: Systematic audit of error
  handling across all crates.
- **GUI Python code smell audit**: Review ported `gui/` Python code for
  patterns that no longer make sense now that Rust handles business logic.
