# deadline-cli Architecture

## Crate Position in the Workspace

```
deadline-cli (binary)
├── deadline-config
├── deadline-client
├── deadline-job-bundle
├── deadline-job-attachments
├── deadline-mcp
├── deadline-common
└── deadline-models
```

Thin orchestration layer. Parses user arguments, delegates to library crates
for all domain logic, formats output. The boundary rule: if it involves AWS
calls, config resolution, or domain logic, it belongs in a library crate.

## Module Layout

```
src/
├── main.rs             # Cli struct (clap Parser), Commands enum, entry point,
│                       #   log level resolution, output redirection, error handler
├── common.rs           # CliOptions, config override application, output formatting,
│                       #   SIGINT handling, progress bars, parameter parsing,
│                       #   markdown stripping, timestamp formatting
└── commands/
    ├── mod.rs          # Re-exports all command modules
    ├── config.rs       # CliError enum, ConfigAction: Show/Get/Set/Clear
    ├── auth.rs         # AuthAction: Login/Logout/Status
    ├── farm.rs         # FarmAction: List/Get
    ├── queue.rs        # QueueAction: List/Get/ExportCredentials/GetStorageProfile/Paramdefs
    ├── fleet.rs        # FleetAction: List/Get
    ├── worker.rs       # WorkerAction: List/Get
    ├── job.rs          # JobAction: List/Get/GetSession/ListSessions/ListSteps/
    │                   #   ListTasks/Wait/Logs/Cancel/RequeueTasks/Search
    ├── bundle.rs       # BundleAction: Submit
    ├── attachment.rs   # AttachmentAction: Download/Upload
    ├── manifest.rs     # ManifestAction: Snapshot/Diff/Download/Upload
    └── helpers.rs      # suggest_resources_on_client_error
```

## Entry Point Flow

```
main()
  │
  ├── Cli::parse() (clap)
  │
  ├── Output redirection (if --redirect-output)
  │   └── dup2 stdout+stderr to file, then forget(file) to keep fd open
  │
  ├── Resolve log level: --log-level > config settings.log_level > WARNING
  │   └── Invalid config values → stderr warning, fall back to WARNING
  │
  ├── init_logging() → env_logger to stderr, no timestamps, no target
  │
  ├── Set CLI command name in session context for user-agent tracking
  │   └── command_name() maps Commands enum → "deadline.{group}.{action}"
  │
  ├── Dispatch: match command → commands::{module}::run(action)
  │
  └── Error handler:
      ├── CliError::ExitCode { code, message } → print message, exit(code)
      └── CliError::Operation / Config → print to stdout, exit(1)
```

## Error Model

`CliError` in `config.rs` has three variants:
- `Operation(String)` — known operation error, exit 1
- `Config(ConfigError)` — config file error, exit 1
- `ExitCode { code, message }` — semantic exit code (e.g., `job wait` uses 2-5)

`CliConfigError` bridges config validation to `CliError`:
- `Operation(msg)` → `CliError::Operation`
- `MissingRequired(msg)` → `CliError::ExitCode { code: 2 }` (usage error)

Error messages print to stdout (matching Python CLI behavior), not stderr.

## Config Loading Pattern

Every command follows the same pattern:

1. `config_file::read_config()` — read from disk once
2. `apply_cli_options_to_config(&mut config, &opts, required)` — apply
   `--profile`, `--farm-id`, `--queue-id`, `--job-id`, `--yes` as in-memory
   overrides. Validate that `required` settings are non-empty.
3. Thread `&config` through all library calls

Missing required settings produce: `"Missing '--farm-id' or default Farm ID configuration"` → exit 2.

## SIGINT Handling

`CONTINUE_OPERATION: AtomicBool` — global flag, initially `true`.

`install_sigint_handler()` registers a C signal handler via `libc::signal`
that sets the flag to `false`. Long-running operations check `should_continue()`.
Progress bar callbacks also check and return `false` to trigger cancellation
in library code.

## Output Formatting

**YAML (default):** `cli_object_repr` serializes `serde_json::Value` via serde_yaml.
Two post-processing steps:
1. Multi-line strings without trailing `\n` get one appended → forces YAML `|-` block style
2. YAML 1.1 boolean literals (`yes`, `no`, `true`, `false`, `on`, `off`, `y`, `n`)
   appearing as mapping values or sequence items are single-quoted → prevents
   misinterpretation by YAML 1.1 parsers (PyYAML)

**JSON:** `--output json` uses `json_with_spaces()` which produces compact JSON
with spaces after `:` and `,` (matching Python's `json.dumps()` default).

**Markdown stripping:** Help text can contain markdown. `strip_markdown_for_terminal()`
converts inline links to `text (url)`, removes bold/italic markers, strips
reference link definitions, and collapses excess blank lines.

## Multi-Format Parameter Parsing

`parse_multi_format_parameters()` in `common.rs` handles `-p`/`--parameter`:

1. `file://path` → read file as JSON (.json) or YAML (otherwise), must be a dict
2. Inline JSON → parse as object, must be a dict
3. `Key=Value` → split on first `=`, value is always a string

Later values override earlier ones for the same key. This matches the Python CLI.

## Progress Bars

`ProgressBarManager` wraps `indicatif::ProgressBar`:
- Created lazily on first callback (avoids empty bars for fast operations)
- `callback(progress) -> bool` — updates position, returns `should_continue()`
- Auto-closes at 100% or on SIGINT

## User-Agent Tracking

`command_name()` maps the parsed `Commands` enum to a dot-separated path
(e.g., `"deadline.farm.list"`, `"deadline.queue.export-credentials"`). This is
set on the session context before dispatch, so all API calls include it in the
User-Agent header.

## Key Design Decisions

**Errors print to stdout, not stderr.** Matches the Python CLI. The log output
goes to stderr (via env_logger), but command errors and results go to stdout.

**Output redirection via dup2 happens before logging init.** If redirection
fails, the error goes to the original stderr (before redirection). Intentional —
you can't report a redirection failure to the redirected output.

**`command_name()` uses enum matching, not argv parsing.** Earlier approach
parsed `std::env::args()` but that included positional arg values in the
command path. Enum matching produces only the command path.

**No `config gui` in Commands enum.** The `config gui` command spawns a Python
process — it's handled inside `ConfigAction` dispatch, not as a separate
top-level command.
