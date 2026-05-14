# deadline-cli Crate Specifications

Binary crate. Clap-based argument parsing, subcommand dispatch, output
formatting. Contains no business logic — delegates everything to library
crates. The user-facing entry point for the `deadline` binary.

Consumers: end users via the `deadline` binary.

Dependencies: `deadline-lib` (config, api, bundle, attachments),
`deadline-test-server` (dev).

## Design: Thin Dispatch Layer

The CLI crate is intentionally thin. Every subcommand follows the same
pattern: parse clap args → load config → call a library function →
format and print the result. Business logic lives in the library crates,
not here.

GUI commands (`bundle gui-submit`, `config gui`) are the exception —
they validate args in Rust, then spawn a Python subprocess that runs
Qt dialogs. Python calls back into Rust via the `deadline._native` PyO3
module for all business logic. See [bundle.md](bundle.md) for the GUI
subprocess architecture.

## Document Index

| Document | Description |
|----------|-------------|
| [architecture.md](architecture.md) | Module layout, entry point flow, global options, design decisions |
| [config-and-auth.md](config-and-auth.md) | `deadline config` and `deadline auth` commands |
| [resource-commands.md](resource-commands.md) | `deadline farm`, `deadline fleet`, `deadline worker` commands |
| [queue.md](queue.md) | `deadline queue` commands: list, get, export-credentials |
| [job.md](job.md) | `deadline job` commands: list, get, search, wait, logs, cancel, requeue-tasks, trace-schedule |
| [bundle.md](bundle.md) | `deadline bundle` commands: submit, gui-submit |
| [attachments-and-manifests.md](attachments-and-manifests.md) | `deadline attachment` and `deadline manifest` commands (BETA) |
| [handle-web-url.md](handle-web-url.md) | `deadline handle-web-url`: protocol handler, install/uninstall |
| [output-and-errors.md](output-and-errors.md) | YAML/JSON output formatting, error-path resource suggestions |
| [mcp.md](mcp.md) | MCP server: tool surface, design decisions, Python differences |
| [reference.md](reference.md) | Complete CLI command reference (user-facing) |

## Status

Implemented: config, auth (login/logout/status), farm list/get, queue
list/get/export-credentials/get-storage-profile/paramdefs/sync-output,
fleet list/get, worker list/get, job
list/get/search/wait/logs/cancel/requeue-tasks/download-output/trace-schedule,
attachment download/upload, manifest snapshot/diff/download/upload, bundle
submit/gui-submit, handle-web-url.

Gaps:
- None — all CLI features implemented
