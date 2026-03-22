# Rust Rewrite

Tracking the implementation of the AWS Deadline Cloud CLI in Rust.

## Reference Material

- `data_flow.md` — Persistent data formats (INI config, job bundle layout,
  manifest JSON, cache schemas, checkpoint format)
- `observations.md` — Behavioral notes and ambiguities discovered during analysis
- `test_specs/` — Behavioral test specifications (52 sections, ~1,300 cases).
  Used as the implementation checklist — not maintained after tests are written.

## Progress

### Implemented

| Crate | Sections | What's done |
|-------|----------|-------------|
| `deadline-models` | §51, §52 | Error types, submitter info |
| `deadline-common` | §36 | Path utilities |
| `deadline-config` | §1 (partial) | INI read/write, hierarchical settings, str2bool, get/set/clear |
| `deadline-test-server` | — | TestHarness, wiremock fake server scaffolding |

### In Progress

| Crate | Sections | What's next |
|-------|----------|-------------|
| `deadline-cli` | §37 (cases 1-2), §38 (cases 1-8) | CLI skeleton with clap, config subcommands, Level 3 tests |

### Not Started

| Crate | Sections | Scope |
|-------|----------|-------|
| `deadline-config` | §1 (remaining), §2 | Profile resolution, get_best_profile_for_farm |
| `deadline-cli` | §37 (cases 3-58) | Root group utilities (log level, redirect, markdown strip, SIGINT, etc.) |
| `deadline-cli` | §39 | Auth commands |
| `deadline-cli` | §40-43 | Farm, fleet, queue, worker commands |
| `deadline-cli` | §44 | Job commands |
| `deadline-cli` | §45 | Bundle submit |
| `deadline-cli` | §46-47 | Attachment and manifest commands |
| `deadline-cli` | §48 | handle-web-url |
| `deadline-cli` | §49 | MCP server (deferred — depends on MCP library) |
| `deadline-client` | §3-5 | Session management, auth, credentials |
| `deadline-client` | §6-10 | API resource management |
| `deadline-client` | §11-14 | Job lifecycle, telemetry |
| `deadline-job-bundle` | §15-18 | Bundle loading, parameters, history |
| `deadline-job-attachments` | §19-35 | Models, hashing, upload, download, caches, manifests, VFS, path mapping |
| `deadline-common` | §50 | MCP server (deferred) |
