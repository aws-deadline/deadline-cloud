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
behavior. Usage errors (missing required options like `--farm-id` when not
configured) print the error message to stdout and exit 2, matching Python
Click's `UsageError` exit code. The `CliError` enum in `commands/config.rs`
distinguishes error types via `thiserror` derives with `From` conversions
for `ConfigError`.

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
serde_yaml uses `|`-style block scalars. String values matching YAML 1.1
boolean literals (`y`, `Y`, `yes`, `Yes`, `YES`, `n`, `N`, `no`, `No`,
`NO`, `true`, `True`, `TRUE`, `false`, `False`, `FALSE`, `on`, `On`,
`ON`, `off`, `Off`, `OFF`) are single-quoted in the output to prevent
data corruption when parsed by YAML 1.1 consumers (e.g. PyYAML).

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
  `settings.config_file_path` and all setting name/value pairs, with
  spaces after colons and commas to match Python's `json.dumps()` default
  format (e.g. `{"key": "value", "key2": "value2"}`).
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
- `deadline job get [SEARCH_TERM] [--farm-id] [--queue-id] [--job-id]`
  Calls `GetJob`, prints full response as YAML (minus `ResponseMetadata`),
  then prints `estimatedTimeRemaining: <value>` on a separate line after
  the YAML. The estimate is computed client-side from `taskRunStatusCounts`
  and `startedAt`; shows `"N/A"` if not computable (no `startedAt`, no
  completed tasks, or no remaining tasks).
  Optional positional `SEARCH_TERM`: if it matches `job-[0-9a-f]{32}`,
  treated as `--job-id`. Otherwise, searches for jobs matching the term.
  Single match → shows full details. Multiple matches → shows summary
  list with name (truncated to 80 chars), jobId, taskRunStatus, created
  timestamp, and task summary. Prints "To get details, run: deadline job
  get --job-id <job-id>". No matches → prints "No jobs found matching
  ...".
- `deadline job search [--farm-id] [--queue-id] [--filter-expressions JSON] [--sort-expressions JSON] [--page-size 5] [--item-offset 0]`
  Calls `SearchJobs` with user-provided filter and sort expressions.
  `--filter-expressions` accepts inline JSON or `file://path.json`.
  `--sort-expressions` accepts inline JSON or `file://path.json`.
  Output format matches `job list`.
- `deadline job wait [--farm-id] [--queue-id] [--job-id] [--max-poll-interval 120] [--timeout 0] [--output verbose|json]`
  Polls `GetJob` until terminal state (SUCCEEDED, FAILED, CANCELED,
  SUSPENDED, NOT_COMPATIBLE). Verbose mode prints status updates to stderr
  via `\r` overwrite; JSON mode prints a single JSON object to stdout.
  On non-SUCCEEDED, collects failed tasks from steps/tasks. Exit codes:
  0=SUCCEEDED, 1=timeout, 2=FAILED, 3=CANCELED, 4=SUSPENDED/ARCHIVED,
  5=NOT_COMPATIBLE. Uses `CliError::ExitCode` for non-zero exits.

`job list`, `job get`, and `job wait` all use
`suggest_resources_on_client_error` on API failure.

- `deadline job logs [--farm-id] [--queue-id] [--job-id] [--session-id] [--session-action-id] [--limit 100] [--start-time] [--end-time] [--next-token] [--output verbose|json] [--timestamp-format utc|local|relative]`
  Retrieves CloudWatch session logs. Always calls `GetJob` first for the
  job name. If `--session-id` is omitted, auto-selects from the job's
  sessions (ongoing preferred, then most recently ended). Prints a
  message indicating how the session was selected (non-JSON only):
  - Single session: `"Using the only available session: {session_id}"`
  - Multiple sessions: `"Using the latest session: {session_id}"`
  If `--session-action-id` is provided, derives session ID from it (strict
  format: `sessionaction-{32hex}-{digits}`), fetches the action for time
  bounds, and intersects with user-provided time range. Verbose output
  prints `[timestamp] message` per event. JSON output includes jobId,
  jobName, events array, count, nextToken, logGroup, logStream.
  `--timestamp-format relative` shows delta from session/action start.
  Deprecated `--timezone` option maps to `--timestamp-format` with a
  stderr warning; errors if both are provided.
- `deadline job cancel [--farm-id] [--queue-id] [--job-id] [--mark-as CANCELED] [--yes]`
  Calls `GetJob` for a summary, prints it as YAML (fields: `name`,
  `jobId`, `taskRunStatus`, `taskRunStatusCounts` with zero counts
  removed, `startedAt`, `endedAt`, `createdBy`, `createdAt`), then asks
  for confirmation. Confirmation message varies: "Are you sure you want
  to cancel this job?" when `--mark-as` is CANCELED (default), otherwise
  "Are you sure you want to cancel this job and mark its taskRunStatus
  as {mark_as}?". Confirmation skipped if `--yes` or
  `settings.auto_accept` is true. On decline: prints "Job not canceled."
  and exits 1. On confirm: prints "Canceling job..." (or "Canceling job
  and marking as {mark_as}..." when not CANCELED), then calls
  `UpdateJob` with `targetTaskRunStatus`. `--mark-as` accepts
  CANCELED, SUSPENDED, FAILED, SUCCEEDED (case-insensitive).
  Uses `suggest_resources_on_client_error` on `GetJob` failure.
- `deadline job requeue-tasks [--farm-id] [--queue-id] [--job-id] [--run-status ...] [--yes]`
  Calls `GetJob` for summary, prints `"Job: {name} ({jobId})"`, then
  `taskRunStatusCounts` as YAML (zero counts removed, keys uppercased),
  then `"Requeuing all tasks with run status among: {sorted statuses}"`.
  Computes estimated task count from `taskRunStatusCounts`. If zero:
  prints "No tasks to requeue." and exits 0. Otherwise asks for
  confirmation (skipped if `--yes` or `settings.auto_accept`). With
  auto-accept: prints `"Estimated {N} total tasks ({breakdown}) to
  requeue."`. Without: prints `"This action will requeue an estimated
  {N} total tasks ({breakdown})"` then `"Are you sure you want to
  requeue these tasks?"`. On decline: prints "No tasks were requeued."
  and exits 1. On confirm: iterates `ListSteps` → `ListTasks` per step.
  For each step: prints step name and ID, estimated count, then each
  matching task as `"    {runStatus} {param=value,...} ({taskId})"`.
  Task parameters use union type extraction: `{"Frame": {"int": "1"}}`
  → `Frame=1`. Calls `UpdateTask` with `targetRunStatus=PENDING` for
  each matching task. Prints total requeued count at end. `--run-status`
  is repeatable; defaults to FAILED, CANCELED, SUSPENDED. Accepts
  SUSPENDED, CANCELED, FAILED, SUCCEEDED, NOT_COMPATIBLE
  (case-insensitive). Uses `suggest_resources_on_client_error` on
  `GetJob` failure.
- `deadline job trace-schedule [--farm-id] [--queue-id] [--job-id] [-v] [--trace-format chrome] [--trace-file path]`
  EXPERIMENTAL. Fetches all sessions, session actions per session,
  caches steps/tasks by ID, computes timing statistics (session count,
  action count, durations). Optional Chrome trace format output via
  `--trace-format chrome --trace-file path`. Errors if `--trace-file`
  without `--trace-format`.

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

## `deadline attachment` (batch 9e-3)

BETA subcommand group for uploading and downloading job attachment data
files using manifest files.

### `deadline attachment download`

Downloads files from S3 CAS based on manifest files.

Options:
- `-m, --manifests <PATH>...` (required, multiple) — manifest file paths
- `--s3-root-uri <URI>` — S3 root URI (bucket + prefix)
- `--path-mapping-rules <PATH>` — path mapping rules JSON file
- `--farm-id <ID>` — override farm ID
- `--queue-id <ID>` — override queue ID
- `--profile <NAME>` — AWS profile for S3 access
- `--conflict-resolution <MODE>` — SKIP, OVERWRITE, or CREATE_COPY
- `--json` — print summary as JSON

Credential resolution:
- If `--profile` provided: use profile credentials + `--s3-root-uri`
- If no `--profile`: read farm/queue from config, get queue attachment
  settings for S3 URI, assume queue role for S3 credentials
- Error if queue has no attachment settings or no S3 root URI

Conflict resolution precedence: `--conflict-resolution` flag > config
`settings.conflict_resolution` > default `CREATE_COPY`.

Output: prints `DownloadSummaryStatistics` (processed/skipped counts,
transfer rate). With `--json`, prints the summary as JSON.

### `deadline attachment upload`

Uploads files to S3 CAS based on manifest files.

Options:
- `-m, --manifests <PATH>...` (required, multiple) — manifest file paths
- `-r, --root-dirs <PATH>...` (multiple) — root directories
- `--path-mapping-rules <PATH>` — path mapping rules JSON file
- `--s3-root-uri <URI>` — S3 root URI
- `--upload-manifest-path <PREFIX>` — S3 prefix for manifest upload
- `--farm-id <ID>`, `--queue-id <ID>`, `--profile <NAME>`
- `--json` — JSON output

Requires exactly one of `--root-dirs` or `--path-mapping-rules`.
Same credential resolution as download.

## `deadline manifest` (batch 9e-3)

BETA subcommand group for creating, comparing, downloading, and
uploading job attachment manifests.

### `deadline manifest snapshot`

Creates a manifest of files in a directory.

Options:
- `--root <DIR>` (required) — directory to snapshot
- `-d, --destination <DIR>` — where to write manifest (default: root)
- `-n, --name <NAME>` — manifest name
- `-i, --include <GLOB>...` — include patterns
- `-e, --exclude <GLOB>...` — exclude patterns
- `-ie, --include-exclude-config <JSON>` — glob config
- `--diff <PATH>` — diff against existing manifest
- `--force-rehash` — hash-based diff instead of mtime
- `--json` — JSON output

Error exits 1 if root or destination doesn't exist.
When no `--destination`, defaults to root and prints a message.

### `deadline manifest diff`

Computes file differences against a manifest.

Options:
- `--manifest <PATH>` (required) — manifest to diff against
- `--root <DIR>` — directory to compare
- `-i, --include`, `-e, --exclude`, `-ie, --include-exclude-config`
- `--force-rehash`
- `--json` — JSON output with new/modified/deleted lists

Error exits 1 if manifest or root doesn't exist.

### `deadline manifest download <DOWNLOAD_DIR>`

Downloads job manifests from S3.

Arguments:
- `<DOWNLOAD_DIR>` — where to write manifests

Options:
- `--job-id <ID>` (required)
- `--step-id <ID>` — include step dependencies
- `--farm-id <ID>`, `--queue-id <ID>`, `--profile <NAME>`
- `--asset-type <TYPE>` — INPUT, OUTPUT, or ALL (default: ALL)
- `--json` — JSON output

Error exits 1 if download directory doesn't exist.

### `deadline manifest upload <MANIFEST_FILE>`

Uploads a manifest to S3 CAS.

Arguments:
- `<MANIFEST_FILE>` — manifest file to upload

Options:
- `--s3-cas-uri <URI>` — S3 CAS URI
- `--s3-manifest-prefix <PREFIX>` — S3 key prefix
- `--farm-id <ID>`, `--queue-id <ID>`, `--profile <NAME>`
- `--json` — JSON output

Error exits 1 if manifest file doesn't exist.
