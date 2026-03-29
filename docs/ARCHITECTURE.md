# Architecture

## Crate Dependency Graph

```
deadline-cli (binary)
├── deadline-config
├── deadline-client
│   ├── deadline-config
│   └── deadline-models
├── deadline-job-bundle
│   └── deadline-models
├── deadline-job-attachments
│   └── deadline-models
├── deadline-mcp
├── deadline-common
└── deadline-models

deadline-worker-agent (binary)
├── deadline-config
├── deadline-client
├── deadline-job-attachments
├── deadline-common
└── deadline-models

deadline-gui-ffi (shared library, C ABI)
├── deadline-config
├── deadline-client
├── deadline-job-bundle
├── deadline-job-attachments
├── deadline-common
└── deadline-models

deadline-mcp (library, used by deadline-cli)
├── rmcp
├── deadline-config
├── deadline-client
├── deadline-job-bundle
├── deadline-job-attachments
├── deadline-common
└── deadline-models

deadline-test-server (dev-dependency of deadline-cli, deadline-worker-agent)
├── wiremock
├── tempfile
└── assert_cmd

gui/ (Python, not a Cargo crate)
└── loads deadline-gui-ffi shared library via ctypes
```

## Crate Responsibilities

| Crate | Role |
|-------|------|
| `deadline-cli` | Binary. Clap argument parsing, subcommand dispatch, output formatting. No business logic. For GUI commands, spawns a Python process that loads the GUI widgets + `deadline-gui-ffi`. |
| `deadline-worker-agent` | Binary. Runs on worker machines. Session management, job attachment sync (download/upload), progress reporting. No GUI. |
| `deadline-gui-ffi` | Shared library with C ABI. Exposes config, auth, API listing, submission, and telemetry to external callers (Python GUI, DCC plugins, Unreal). |
| `deadline-config` | INI config file read/write, hierarchical setting resolution, str2bool. No AWS dependencies. |
| `deadline-client` | AWS API calls (Deadline Cloud service). Owns the SDK/HTTP interaction. |
| `deadline-models` | Shared data types and error types. No I/O, no logic beyond construction and display. |
| `deadline-common` | Utility functions shared across crates (path utils, formatting). |
| `deadline-job-bundle` | Job bundle directory parsing, template loading, parameter resolution. |
| `deadline-job-attachments` | Asset manifest handling, S3 upload/download, hash cache, content-addressed storage. |
| `deadline-test-server` | Test-only. Wiremock-based fake AWS server and `TestHarness` for CLI subprocess tests. |
| `deadline-mcp` | Library. MCP server logic invoked by `deadline-cli` via `deadline mcp-server`. Uses rmcp SDK. |
| `gui/` (Python) | Qt QWidgets layout code (~15 files). Pure presentation — no business logic. Calls `deadline-gui-ffi` for every operation. |

## Data Flows

### CLI Headless Command (e.g., `deadline bundle submit`)

```
deadline-cli
  → Clap parses args
  → Config loaded once via deadline-config
  → CLI flags applied as in-memory overrides
  → Business logic via deadline-job-bundle, deadline-job-attachments, deadline-client
  → Output formatted and printed
```

All Rust, one process, direct function calls.

### CLI GUI Command (e.g., `deadline config gui`)

```
deadline-cli
  → Spawns Python process
  → Python loads gui/ widgets + deadline-gui-ffi.so via ctypes
  → Python shows QDialog
  → User interacts with dialog
  → Every button click / dropdown load calls deadline-gui-ffi → Rust
  → Rust reads config, calls APIs, saves config
  → Python displays results
```

### Worker Agent

```
deadline-worker-agent
  → Polls for work via deadline-client
  → Downloads attachments via deadline-job-attachments
  → Runs session actions
  → Uploads output via deadline-job-attachments
  → Reports progress via deadline-client
```

All Rust, one process, no Python, no GUI.

### DCC Plugin (e.g., Maya)

```
Maya Python plugin
  → Scene introspection via maya.cmds (Python, ~100-200 lines)
  → Builds scene_data dict
  → Loads deadline-gui-ffi.so via ctypes (one-time)
  → Calls ffi.list_farms(), ffi.list_queues() to populate GUI dropdowns
  → Shows Python QWidgets submit dialog (from gui/ package)
  → User clicks Submit
  → Calls ffi.submit_job(scene_data) → Rust does everything
  → Returns job_id
```

### After Effects

```
ExtendScript (JSX)
  → system.callSystem("deadline bundle gui-submit ...")
  → deadline-cli binary handles everything
```

## Shared Conventions

- **Error handling:** All crates use `thiserror` for error types. The CLI
  catches errors at the top level and prints them. The FFI layer converts
  Rust errors to C-compatible error codes + message strings.
- **No mocking:** Tests use real temp directories and wiremock HTTP servers.
  See `TESTING.md`.
- **API responses:** All API functions in `deadline-client` use the
  `ResponseBodyCapture` interceptor to return raw `serde_json::Value`.
  The CLI layer never sees SDK types. See `workflow.md` § "AWS SDK for
  Rust Usage".
- **Config threading:** Functions that need config take `&IniConfig` (reads)
  or `&mut IniConfig` (writes). Convenience wrappers that hit disk exist but
  are not the primary API.
