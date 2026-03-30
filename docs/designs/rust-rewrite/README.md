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
| `deadline-config` | §1, §2 | INI read/write, hierarchical settings, str2bool, get/set/clear, get_best_profile_for_farm |
| `deadline-test-server` | — | TestHarness, wiremock stub server, STS/Deadline REST mocks (with pagination) |
| `deadline-cli` | §37 (cases 1-2), §38 (cases 1-8) | `--version`, `--help`, config subcommands (show, get, set, clear), Level 2 tests |
| `deadline-cli` | §37 (cases 3-47, 54-58) | `--log-level`, `--redirect-output`, markdown stripping, error handling, `apply_cli_options_to_config`, `cli_object_repr`, `parse_file_parameter`, `parse_multi_format_parameters`, `TimestampFormat`, SIGINT handler, `ProgressBarManager` |
| `deadline-client` | §4 (cases 1-3, 8, 10-11), §7 (cases 1-2, 4-6, 8-17, 22) | Session creation, profile resolution, `check_authentication_status`, `check_deadline_api_available`, DCM credential source detection, `list_farms`/`list_queues`/`list_fleets`/`list_jobs`/`search_jobs` (all via ResponseBodyCapture), `get_farm`/`get_queue`/`get_fleet`/`get_job`/`get_worker` (ResponseBodyCapture), `search_workers` (ResponseBodyCapture) |
| `deadline-cli` | §39 (cases 4-7), §40-§44 (list/get cases) | `deadline auth status`, `deadline farm list/get`, `deadline fleet list/get`, `deadline queue list/get`, `deadline job list/get` (job list uses `search_jobs` with count header, field selection, `estimatedTimeRemaining`) |
| `deadline-cli` | §37 (cases 48-53), §43 (cases 1-7) | `suggest_resources_on_client_error`, `deadline worker list/get`, SDK error formatting with error codes |
| `deadline-cli` | — | All CLI test files converted to `insta-cmd` snapshots: `cli_auth`, `cli_config`, `cli_root`, `cli_suggest`, `cli_dcm`, `cli_common` |

### In Progress

| Crate | Sections | What's next |
|-------|----------|-------------|
| `deadline-client` | §3-10 | Session caching/user-agent (§3), queue user creds (§5), login/logout (§6), queue params (§8), queue creds (§9), storage profile (§10) |

### Risk Spikes (must pass before bulk implementation)

See `migration_strategy.md` § "Fail-Fast Strategy" for full details.

| Spike | Status | Proves |
|-------|--------|--------|
| GUI FFI round-trip (Python ↔ Rust ↔ Qt) | Not started | Core architecture works: ctypes loading, C ABI calls, callbacks, thread safety |
| GUI FFI inside DCC (Blender) | Not started | Shared library loads in real DCC Python environment without conflicts |
| S3 transfer performance | Not started | Rust S3 throughput ≥ Python boto3 transfer manager |
| Job attachment hashing | Not started | Parallel xxh128 hashing is faster than Python, hashes match byte-for-byte |

### Not Started

| Crate | Phase | Sections | Scope |
|-------|-------|----------|-------|
| `deadline-cli` | 1 | §37-49 | All remaining CLI commands |
| `deadline-client` | 1 | §3-14, §34 | Session, auth, API resource mgmt, job lifecycle, telemetry |
| `deadline-job-bundle` | 1 | §15-18 | Bundle loading, parameters, history |
| `deadline-job-attachments` | 1 | §19-35 | Models, hashing, upload, download, caches, manifests, VFS, path mapping |
| `deadline-common` | 1 | §50 | MCP server (deferred) |
| `deadline-worker-agent` | 2 | — | Session mgmt, attachment sync, progress reporting |
| `deadline-gui-ffi` | 3 | — | C ABI shared library for GUI + DCC plugins |
| `gui/` (Python) | 3 | — | Refactored QWidgets layout calling Rust FFI |
| `deadline-mcp` | 5 | §49, §50 | MCP server binary (rmcp SDK) |
