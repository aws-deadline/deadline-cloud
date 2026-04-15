# deadline-cli Crate Specifications

Binary crate. Clap-based argument parsing, subcommand dispatch, output
formatting. Contains no business logic — delegates everything to library
crates. The user-facing entry point for the `deadline` binary.

Consumers: end users via the `deadline` binary.

Dependencies: `deadline-config`, `deadline-api`, `deadline-job-bundle`,
`deadline-job-attachments`, `deadline-mcp`, `deadline-test-server` (dev).

## Document Index

| Document | Description |
|----------|-------------|
| [architecture.md](architecture.md) | Module layout, entry point flow, global options, design decisions |
| [config.md](config.md) | `deadline config` commands: show, get, set, clear, gui |
| [auth.md](auth.md) | `deadline auth` commands: login, logout, status |
| [farm.md](farm.md) | `deadline farm` commands: list, get |
| [queue.md](queue.md) | `deadline queue` commands: list, get, export-credentials |
| [fleet.md](fleet.md) | `deadline fleet` commands: list, get |
| [worker.md](worker.md) | `deadline worker` commands: list, get |
| [job.md](job.md) | `deadline job` commands: list, get, search, wait, logs, cancel, requeue-tasks |
| [bundle.md](bundle.md) | `deadline bundle` commands: submit, gui-submit |
| [attachment.md](attachment.md) | `deadline attachment` commands: download, upload (BETA) |
| [manifest.md](manifest.md) | `deadline manifest` commands: snapshot, diff, download, upload (BETA) |
| [output-formatting.md](output-formatting.md) | YAML/JSON output, cli_object_repr, boolean quoting |
| [resource-suggestions.md](resource-suggestions.md) | Error-path resource suggestion behavior |
| [reference.md](reference.md) | Complete CLI command reference (user-facing) |

## Status

Implemented: config, auth (login/logout/status), farm list/get, queue
list/get/export-credentials, fleet list/get, worker list/get, job
list/get/search/wait/logs/cancel/requeue-tasks, attachment
download/upload, manifest snapshot/diff/download/upload, bundle
submit/gui-submit.

Gaps:
- `bundle submit --json` output format
- `bundle submit --save-debug-snapshot` mode
- `job download-output` / `job sync-output`
- `handle-web-url`
- `job trace-schedule` — experimental, deferred
