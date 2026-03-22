# Architecture

## Crate Dependency Graph

```
deadline-cli
├── deadline-config
├── deadline-client
│   ├── deadline-config
│   └── deadline-models
├── deadline-job-bundle
│   └── deadline-models
├── deadline-job-attachments
│   └── deadline-models
├── deadline-common
└── deadline-models

deadline-test-server  (dev-dependency of deadline-cli only)
├── wiremock
├── tempfile
└── assert_cmd
```

## Crate Responsibilities

| Crate | Role |
|-------|------|
| `deadline-cli` | Binary. Clap argument parsing, subcommand dispatch, output formatting. No business logic. |
| `deadline-config` | INI config file read/write, hierarchical setting resolution, str2bool. No AWS dependencies. |
| `deadline-client` | AWS API calls (Deadline Cloud service). Owns the boto/SDK interaction. |
| `deadline-models` | Shared data types and error types. No I/O, no logic beyond construction and display. |
| `deadline-common` | Utility functions shared across crates (path utils, formatting). |
| `deadline-job-bundle` | Job bundle directory parsing, template loading, parameter resolution. |
| `deadline-job-attachments` | Asset manifest handling, S3 upload/download, hash cache, content-addressed storage. |
| `deadline-test-server` | Test-only. Wiremock-based fake AWS server and `TestHarness` for CLI subprocess tests. |

## Data Flow: CLI Command Execution

Every CLI command follows the same pattern:

1. **Clap parses args** in `deadline-cli`
2. **Config loaded once** via `read_config()` or `read_config_from(path)`
3. **CLI flags applied as in-memory overrides** via `set_setting_in_config()` — this mutates the `IniConfig` without writing to disk
4. **The `IniConfig` is threaded through** all downstream calls via `get_setting_with_config()`
5. **API calls** go through `deadline-client`, which uses the config for credentials/endpoint resolution
6. **Output formatted** and printed by `deadline-cli`

The config is never re-read from disk mid-command. Writes to disk only happen for explicit mutations (`deadline config set`, `deadline config clear`, saving telemetry identifier, updating job_id after submission).

## Shared Conventions

- **Error handling:** All crates use `thiserror` for error types. The CLI catches errors at the top level and prints them.
- **No mocking:** Tests use real temp directories and wiremock HTTP servers. See `TESTING.md`.
- **Config threading:** Functions that need config take `&IniConfig` (reads) or `&mut IniConfig` (writes). Convenience wrappers that hit disk exist but are not the primary API.
