# deadline-cli

Binary crate. Clap-based argument parsing, subcommand dispatch, output
formatting. Contains no business logic — delegates to library crates.

## Root Command

The `deadline` binary is the single entry point. It accepts global options
before any subcommand:

- `--version` — prints `deadline <version>` and exits 0
- `-h` / `--help` — prints help text listing subcommands and global options, exits 0
- `--log-level <LEVEL>` — sets logging verbosity (ERROR, WARNING, INFO, DEBUG).
  If omitted, reads `settings.log_level` from config. If the config value is
  invalid, falls back to WARNING and prints a warning to stderr.
- `--redirect-output <PATH>` — redirects stdout and stderr to the given file
  via `dup2`. Combined with `--redirect-mode replace`, opens in write mode.
- `--redirect-mode <MODE>` — `append` (default) or `replace`. Only meaningful
  with `--redirect-output`.

The help text includes a short description and a "common workflows" section
showing typical command sequences. Markdown syntax in help text is stripped
for terminal display (links become `text (url)`, bold markers removed).

Both `-h` and `--help` are accepted (clap provides both by default).

## Error Handling

Known errors (`CliError::Config`, `CliError::Operation`) print the error
message to stdout and exit 1, matching the Python CLI's `click.echo(str(e))`
behavior. The `CliError` enum in `commands/config.rs` distinguishes error
types via `thiserror` derives with `From` conversions for `ConfigError`.

## Common Utilities (`src/common.rs`)

### `strip_markdown_for_terminal(text) -> String`

Transforms markdown syntax into plain text for terminal display using
compiled `regex::Regex` patterns stored in `LazyLock` statics:
- `[text](url)` → `text (url)`
- `[text][ref]` with reference definition → `text` (definition line removed)
- `**bold**` / `__bold__` → `bold`
- `*italic*` → `italic` (requires preceding non-`*` character to avoid
  matching list markers; uses named capture groups since the `regex` crate
  does not support lookbehind)
- No-markdown text returned unchanged.

### `CliOptions` and `apply_cli_options_to_config`

`CliOptions` is a typed struct replacing Python's `**kwargs` pattern:

```rust
pub struct CliOptions {
    pub profile: Option<String>,
    pub farm_id: Option<String>,
    pub queue_id: Option<String>,
    pub job_id: Option<String>,
    pub yes: bool,
}
```

`apply_cli_options_to_config` applies these to an `IniConfig` via
`config_file::set_setting_in_config`, then validates required options.
Missing required options return an error like
`"Missing '--farm-id' or default Farm ID configuration"`.

### `cli_object_repr(obj) -> String`

Formats a `serde_json::Value` as YAML via `serde_yaml::to_string`.
Multi-line strings that don't end with `\n` get one appended so
serde_yaml uses `|`-style block scalars.

### `parse_file_parameter(path) -> HashMap`

Parses a file as JSON (if `.json` extension) or YAML (otherwise) into
`HashMap<String, serde_json::Value>`. Errors if file missing, is a
directory, has invalid content, or contains a non-object top-level value.

### `parse_multi_format_parameters(params) -> HashMap`

Parses a `&[String]` in mixed formats:
1. `file://path` — delegates to `parse_file_parameter`
2. Inline JSON — detected via `serde_json::from_str` attempt (no regex)
3. `key=value` — split on first `=`
4. Otherwise → error

Later values override earlier for same key.

### `TimestampFormat`

Enum with variants instead of Python's struct-with-field:

```rust
pub enum TimestampFormat {
    Utc,
    Local,
    Relative { reference: DateTime<FixedOffset> },
}
```

The reference time is only stored for `Relative`. Timezone-missing errors
from Python (cases 38-39) are prevented at compile time — `DateTime<FixedOffset>`
always has a timezone.

### SIGINT Handling

Static `AtomicBool` instead of Python's singleton class:

```rust
static CONTINUE_OPERATION: AtomicBool = AtomicBool::new(true);
```

`install_sigint_handler()` registers a `libc::signal` handler.
`should_continue()` reads the flag. No instantiation needed.

### `ProgressBarManager`

Uses `Option<ProgressBar>` (from `indicatif`) instead of Python's explicit
state enum. `None` = not created, `Some` = active, `.take()` = closed.
`callback(progress)` returns `should_continue()`.

### `suggest_resources_on_client_error`

Free function in `commands/helpers.rs`. Called on API error paths to suggest
alternative resources when a command fails with `AccessDeniedException`,
`ResourceNotFoundException`, or `ValidationException`.

Takes the error message string, an operation name hint (e.g. `"GetQueue"`),
and optional resource IDs (`farm_id`, `queue_id`, `fleet_id`, `worker_id`).
Returns a `String` to append to the error message (empty if no suggestions).

Behavior:
- Only fires for the three error codes above; other errors return `""`.
- Uses a chain-of-fetchers: tries the most specific resource list first,
  falls back to broader ones (e.g. queues → farms).
- Workers use `search_workers` (not `list_workers`), with `totalResults`.
- Shows at most 10 items; appends `"... and N more"` if more exist.
- If all list calls also fail, returns a hint about missing List permissions.

## Subcommands

Each subcommand group lives in `src/commands/<group>.rs`.

### `deadline config`

Manages the Deadline Cloud configuration file. All subcommands operate on the
config file at `DEADLINE_CONFIG_FILE_PATH` (or `~/.deadline/config` by default).

- `deadline config show` — verbose mode (default) prints the config file path,
  then for each setting: `name: value (default)` (the suffix only when value
  equals the default), followed by the description indented with 3 spaces.
  With `--output json`, prints a JSON object containing
  `settings.config_file_path` and all setting name/value pairs.
  `OutputFormat` uses `#[derive(clap::ValueEnum)]`.
- `deadline config get <setting>` — prints the current value of a single setting.
  If not explicitly set, prints the default.
- `deadline config set <setting> <value>` — persists a value to the config file.
- `deadline config clear <setting>` — reverts a setting to its default by writing
  the default value back to the config file (does not remove the key).
- `deadline config gui` — spawns a Python process that loads the GUI widget
  package and `deadline-gui-ffi` shared library, then shows the config dialog.

### `deadline bundle gui-submit`

Spawns a Python process that loads the GUI widget package and
`deadline-gui-ffi` shared library, then shows the job submission dialog.

### `deadline job`

- `deadline job list [--farm-id] [--queue-id] [--page-size 5] [--item-offset 0]`
  Uses `SearchJobs` API (not `ListJobs`), sorted by `CREATED_AT` descending.
  Prints a count/offset header line (`"Displaying N of T Jobs starting at O"`),
  then a blank line, then a YAML list with these fields per job (in order):
  `name` (falls back to `displayName` if `name` absent), `jobId`,
  `taskRunStatus`, `startedAt`, `endedAt`, `createdBy`, `createdAt`,
  `estimatedTimeRemaining`. The `estimatedTimeRemaining` field is computed
  client-side from `taskRunStatusCounts` and `startedAt`; shows `"N/A"` if
  not computable. DateTime fields are formatted as `YYYY-MM-DD HH:MM:SS+00:00`.
- `deadline job get [--farm-id] [--queue-id] [--job-id]`
  Calls `GetJob`, prints full response as YAML (minus `ResponseMetadata`).

Both commands use `suggest_resources_on_client_error` on API failure.

### `deadline worker`

- `deadline worker list --fleet-id <id> [--farm-id] [--page-size 5] [--item-offset 0]`
  Uses `SearchWorkers` API (not `ListWorkers`). Prints a count/offset header
  line (`"Displaying N of T workers starting at O"`), then YAML list with
  `workerId`, `status`, `createdAt` fields per worker.
- `deadline worker get --fleet-id <id> --worker-id <id> [--farm-id]`
  Calls `GetWorker`, prints full response as YAML (minus `ResponseMetadata`).

Both commands use `suggest_resources_on_client_error` on API failure.

## Dependencies

| Crate | Purpose |
|-------|---------|
| `clap` | Argument parsing |
| `serde_json` | JSON output for `--output json` |
| `serde_yaml` | YAML output for `cli_object_repr` |
| `regex` | Markdown stripping |
| `log` / `env_logger` | Logging |
| `chrono` | Timestamp formatting |
| `libc` | Output redirection (`dup2`), SIGINT handler |
| `indicatif` | Progress bars |
| `thiserror` | Error type derives |
| `textwrap` | Description wrapping in `config show` |
| `deadline-config` | Config file operations |
| `deadline-client` | AWS API calls |
| `deadline-models` | Shared types |
| `deadline-common` | Utilities |
| `deadline-job-bundle` | Bundle submission |
| `deadline-job-attachments` | Attachment handling |
| `deadline-mcp` | MCP server (for `deadline mcp-server` subcommand) |
