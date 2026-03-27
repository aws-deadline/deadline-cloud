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
  invalid, falls back to WARNING.

- `--redirect-output <PATH>` — redirects stdout and stderr to the given file
  in append mode. Combined with `--redirect-mode replace`, opens in write mode.
- `--redirect-mode <MODE>` — `append` (default) or `replace`. Only meaningful
  with `--redirect-output`.

The help text includes a short description and a "common workflows" section
showing typical command sequences. Markdown syntax in help text is stripped
for terminal display (links become `text (url)`, bold markers removed).

Both `-h` and `--help` are accepted (matching the Python CLI's behavior).

## Common Utilities (`src/common.rs`)

### `strip_markdown_for_terminal(text) -> String`

Transforms markdown syntax into plain text for terminal display:
- `[text](url)` → `text (url)`
- `[text][ref]` with reference definition → `text` (definition line removed)
- `**bold**` → `bold`
- `*italic*` → `italic` (but not list markers at line start)
- No-markdown text returned unchanged.

### Error Handling

Commands that fail with a known `DeadlineOperationError` print the error
message to stderr and exit 1. Unexpected errors print
"The AWS Deadline Cloud CLI encountered the following exception" followed
by the error chain, then exit 1.

### `apply_cli_options_to_config`

Applies `--profile`, `--farm-id`, `--queue-id`, `--job-id`, `--yes` flags
to an in-memory config. If a required option (e.g. `farm_id`) is in
`required_options` but not set in config or args, returns a usage error.

### `cli_object_repr(obj) -> String`

Formats an API response as YAML output. Multi-line strings that don't end
with `\n` get one appended so YAML uses `|`-style block scalars.

### `parse_file_parameter(path) -> Map`

Parses a file as JSON (if `.json` extension) or YAML (otherwise). Errors
if file missing, is a directory, has invalid content, or contains a
non-dict top-level value.

### `parse_multi_format_parameters(params) -> Map`

Parses a list of strings in mixed formats: `key=value`, inline JSON
(`{"k":"v"}`), or `file://path`. Later values override earlier for same key.

### `TimestampFormatter`

Formats timestamps in UTC (ISO 8601), LOCAL (local timezone ISO 8601), or
RELATIVE (time delta from a reference start time). Both the reference time
and the timestamp to format must have timezones.

### `SigIntHandler`

Singleton that installs a SIGINT handler. `continue_operation` starts as
`true` and is set to `false` on SIGINT.

### `ProgressBarCallbackManager`

Manages a progress bar lifecycle: created on first callback, updated on
subsequent calls, closed at 100% or on SIGINT. Returns
`continue_operation` from each callback.

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

## Dependencies

| Crate | Purpose |
|-------|---------|
| `clap` | Argument parsing |
| `serde_json` | JSON output for `--output json` |
| `deadline-config` | Config file operations |
| `deadline-client` | AWS API calls |
| `deadline-models` | Shared types |
| `deadline-common` | Utilities |
| `deadline-job-bundle` | Bundle submission |
| `deadline-job-attachments` | Attachment handling |
| `deadline-mcp` | MCP server (for `deadline mcp-server` subcommand) |
