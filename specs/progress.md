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
| 0f | CLI root & common utilities | ⚠️ Gaps | `cli.md` | 0d |
| 0g | Session creation & auth status | ✅ Done | `session.md`, `api_resource_management.md`, `cli.md` | 0d |
| 0h | Queue/job credentials & diagnostics | ✅ Done | `api_resource_management.md`, `api_job_lifecycle.md`, `cli.md` | 0g |
| 0i | GUI FFI spike | ✅ Done | — | 0g |
| 1 | Session caching & user-agent | ⚠️ Gaps | `session.md` | 0g |
| 2 | Login/logout | ⚠️ Gaps | `api_resource_management.md`, `cli.md` | 1 |
| 3 | Queue user credentials | ✅ Done | `session.md` | 1 |
| 4 | Queue parameters | ✅ Done | `api_resource_management.md`, `cli.md` | 1 |
| 5 | Telemetry API integration | ⚠️ Gaps | `api_job_lifecycle.md`, `cli.md` | 1 |
| 6 | Job monitoring & logs | ⚠️ Gaps | `api_job_lifecycle.md`, `cli.md` | 1 |
| 7 | Job bundle | ✅ Done | `job_bundle.md`, `cli.md` | 1, 4 |
| 8 | Job attachments: core | ⚠️ Gaps | `job_attachments_data_transfer.md`, `job_attachments_orchestration.md` | 1 |
| 9 | Job attachments: transfer | ⚠️ Gaps | `job_attachments_data_transfer.md`, `cli.md` | 3, 8 |
| 10 | Job attachments: orchestration | ✅ Done | `job_attachments_orchestration.md`, `job_attachments_data_transfer.md`, `cli.md` | 9 |
| 11 | Submit job bundle | ⚠️ Gaps | `api_job_lifecycle.md`, `cli.md` | 7, 9 |
| 12 | Job cancel | ✅ Done | `cli.md` | 6 |
| 12b | Job search command | ✅ Done | `cli.md` | 6 |
| 13 | Job download & sync-output | ⚠️ Gaps | `job_attachments_orchestration.md`, `job_attachments_data_transfer.md`, `cli.md` | 9 |
| 14 | Handle web URL | ✅ Done | `cli.md` | 13 |
| 15 | Job requeue-tasks | ✅ Done | `cli.md` | 6 |
| 15b | Job get search & estimated time | ✅ Done | `cli.md` | 6 |
| 15c | Job logs auto-selection messages | ✅ Done | `cli.md` | 6 |
| 15d | Level 2 test coverage audit | ✅ Done | — | 11 |
| 15e | Behavioral parity audit | ✅ Done | — | 9 |
| 15f | Wire queue/fleet assume role for all existing CLI commands | ✅ Done | `credential_scoping.md` | 15e-F1 |
| 16 | GUI FFI remaining | Deferred | — | 1-14 |
| 17 | MCP server | ✅ Done | `cli.md`, `mcp.md` | 1-14 |

**Status key:** ✅ Done · ⚠️ Gaps · In progress · Not started · Deferred

**In-progress details:** See `HANDOFF.md` for current state of any
"In progress" work items.

**Deferred items:** #16-17 (GUI FFI, MCP) ship as part of the CLI deliverable
after the core CLI commands are complete.

**Scope changes:**
- **#10**: Originally included asset sync, path mapping, VFS, OS permissions,
  and incremental downloads. Reduced to upload-side orchestration and
  utilities already implemented in #8-9. Download-side orchestration
  (path mapping, incremental downloads, manifest download) moved to #13.
  Worker-agent-only features (asset sync, VFS, OS permissions) deferred.
- **#11**: Dependency on #10 removed. The upload library functions it needs
  were completed in #9. Now depends only on #7 and #9.
- **#13**: Expanded to include download-side orchestration from old #10:
  path mapping from storage profiles, incremental downloads, manifest
  download CLI, plus the original `job download-output` and
  `queue sync-output` CLI commands. Split into three batches:
  Batch A (`job download-output` ✅), Batch B (path mapping),
  Batch C (`queue sync-output` + incremental downloads).

**Audit gaps in completed items** (from `audit_reports/2026-04-17-behavioral-parity.md`):
- **#0f**: AUDIT-054 — `--redirect-output` is Unix-only, needs Windows support
- **#1**: AUDIT-043 — User-agent string uses `app_name()` instead of `user_agent_extra`
- **#2**: AUDIT-042 — Windows stdin handling for login subprocess
- **#4**: ~~AUDIT-006~~ ✅ `fleet get --queue-id` mode implemented
- **#5**: AUDIT-010 — No telemetry events during submission flow
- **#6**: AUDIT-026 — `job logs --session-action-id` deferred.
  AUDIT-041 — `job trace-schedule` deferred (EXPERIMENTAL in Python).
  `--timezone` deprecated flag not implemented.
- **#8**: AUDIT-013 — Hash cache V5 schema incompatible with Python V4.
  AUDIT-053 — Windows long path (UNC) handling missing.
- **#9**: AUDIT-011 — Upload is sequential (no parallelism).
  AUDIT-012 — Download is sequential (no parallelism).
  AUDIT-014 — No multipart upload (5GB PutObject limit).
  AUDIT-048 — `manifest upload` missing queue derivation.
  AUDIT-049 — No multipart download for large files.
  AUDIT-056 — `suggest_resources` storage profile chain is a stub.
- **#11**: AUDIT-009 — Upload confirmation prompt only for unknown paths.
  AUDIT-031 — `--save-debug-snapshot` not implemented.
  AUDIT-034 — `--submitter-info` not implemented.
- **#13**: AUDIT-008 — `job download-output` no interactive root path editing.
  AUDIT-047 — `manifest download` CLI is a stub.
  AUDIT-055 — Download conflict resolution prompt missing.
  SYNC-005 — `sync-output` missing intermediate progress messages
  ("Retrieving session actions...", "Populating manifest S3 keys...",
  "Downloading N asset manifests...").
- **#14**: ~~AUDIT-030~~ ✅ False finding — Python also doesn't support macOS
- **#15**: ~~AUDIT-040~~ ✅ All gaps fixed

**Dropped findings (not bugs, accepted differences):**
- AUDIT-024 — YAML key ordering differs (accepted per `patterns.md`)
- AUDIT-030 — `handle-web-url` macOS support — false finding (Python also doesn't support macOS)
- AUDIT-039 — `job wait` verbose to stderr (Rust approach is better)
- AUDIT-046 — `require_setting` exit code (function is unused dead code)

**Technical debt:**
- **#15d**: Audit all Level 1 tests in library crates to identify which
  can be converted to or supplemented with Level 2 CLI subprocess tests.
  Per TESTING.md rule 1: "If the CLI can exercise it, test it through
  the CLI."
- **Realistic test IDs**: Replace hardcoded pseudo-IDs in tests (e.g.
  `"farm-1"`, `"queue-1"`) with pseudorandomly generated IDs that follow
  the actual Deadline Cloud ID format. Add a test helper that generates
  realistic IDs per resource type. Applies across all crates.
- **Test consolidation**: Audit test suite for redundant or overlapping
  tests. Identify Level 1 tests fully subsumed by Level 2 snapshots
  (per testing.md removal rule). Consolidate tests that exercise the
  same code path with minor variations into parameterized test cases.
