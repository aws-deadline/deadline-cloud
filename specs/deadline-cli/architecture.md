# deadline-cli Architecture

## Crate Position

```
deadline-cli (binary)
├── deadline-config
├── deadline-api
├── deadline-job-bundle
├── deadline-job-attachments
└── rmcp (MCP server, built into CLI)
```

Thin orchestration layer. Parses user arguments, delegates to library crates
for all domain logic, formats output. The boundary rule: if it involves AWS
calls, config resolution, or domain logic, it belongs in a library crate.

## Module Layout

The binary has a single entry point (`main.rs`) that parses clap args,
resolves logging, and dispatches to command modules. Shared utilities
live in `common.rs` — config override application, output formatting,
SIGINT handling, progress bars, and parameter parsing.

Each command group has its own module under `commands/`. Every command
follows the same pattern: parse args → load config → apply CLI overrides
→ call library function → format and print result.

```
src/
├── main.rs             # Entry point, clap parsing, log level resolution, error handler
├── common.rs           # Shared utilities: config overrides, output formatting, SIGINT, progress bars
└── commands/
    ├── config.rs       # config show/get/set/clear/gui
    ├── auth.rs         # auth login/logout/status
    ├── farm.rs         # farm list/get
    ├── queue.rs        # queue list/get/export-credentials/storage-profile/paramdefs/sync-output
    ├── fleet.rs        # fleet list/get
    ├── worker.rs       # worker list/get
    ├── job.rs          # job list/get/search/wait/logs/cancel/requeue-tasks/download-output
    ├── bundle.rs       # bundle submit/gui-submit
    ├── attachment.rs   # attachment download/upload
    ├── manifest.rs     # manifest snapshot/diff/download/upload
    ├── handle_web_url.rs # handle-web-url dispatch, install/uninstall
    ├── gui.rs          # Python subprocess launcher for GUI commands
    ├── mcp.rs          # MCP server (deadline mcp-server)
    └── helpers.rs      # Error-path resource suggestions
```

## Entry Point Flow

```
main()
  │
  ├── Cli::parse() (clap)
  │
  ├── Output redirection (if --redirect-output)
  │   └── dup2 stdout+stderr to file
  │
  ├── Resolve log level: --log-level > config settings.log_level > WARNING
  │
  ├── init_logging() → env_logger to stderr
  │
  ├── Set CLI command name in session context for user-agent tracking
  │
  ├── Dispatch: match command → commands::{module}::run(action)
  │
  └── Error handler:
      ├── CliError::ExitCode { code, message } → print message, exit(code)
      └── CliError::Operation / Config → print to stdout, exit(1)
```

## Error Model

The CLI has a simple error model with three variants:

- **Operation errors** — known failures from library calls (API errors,
  validation failures). Printed to stdout, exit code 1.
- **Config errors** — invalid settings, missing config file. Printed to
  stdout, exit code 1.
- **Semantic exit codes** — some commands use specific exit codes to
  signal outcomes (e.g., `job wait` uses 2-5 for different terminal
  states). These carry a code and message.

Missing required settings (like `--farm-id` when no default is
configured) produce a usage error with exit code 2.

Error messages print to stdout (matching Python CLI behavior), not
stderr. Log output goes to stderr via env_logger.

## Config Loading Pattern

Every command follows the same pattern:

1. Read config from disk once
2. Apply CLI flags (`--profile`, `--farm-id`, `--queue-id`, `--job-id`,
   `--yes`) as in-memory overrides, validating that required settings
   are non-empty
3. Thread `&config` through all library calls

## SIGINT Handling

A global `AtomicBool` flag is set to `false` by a signal handler when
the user presses Ctrl+C. Long-running operations and progress bar
callbacks check this flag and trigger cancellation in library code.

## Output Formatting

**YAML (default):** API responses are serialized to YAML with two
post-processing steps: multi-line strings get block literal style
(`|-`), and YAML 1.1 boolean literals (`yes`, `no`, `on`, `off`) are
single-quoted to prevent misinterpretation by YAML 1.1 parsers like
PyYAML.

**JSON:** `--output json` produces compact JSON with spaces after `:`
and `,`, matching Python's `json.dumps()` default.

**Markdown stripping:** Help text containing markdown is converted to
plain text for terminal display — inline links become `text (url)`,
bold/italic markers are removed.

## Key Design Decisions

**Errors print to stdout, not stderr.** Matches the Python CLI. Log
output goes to stderr, but command errors and results go to stdout.

**Output redirection via dup2 happens before logging init.** If
redirection fails, the error goes to the original stderr.

**No business logic in the CLI crate.** Every command delegates to
library crates. The CLI only handles argument parsing, config loading,
and output formatting.
