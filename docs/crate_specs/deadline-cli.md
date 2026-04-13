# deadline-cli

Binary crate. Clap-based argument parsing, subcommand dispatch, output
formatting. Contains no business logic — delegates everything to library
crates.

## Role in the System

The user-facing entry point. Parses arguments, loads config once, applies
CLI flag overrides, calls into library crates, and formats output. The
boundary rule is strict: if it involves AWS calls, config resolution, or
domain logic, it belongs in a library crate. The CLI only handles
presentation and user interaction (confirmation prompts, progress bars,
output formatting).

Consumers: end users via the `deadline` binary.

## Key Concepts

**Config is loaded once and threaded through.** The CLI reads the config
file at startup and passes the `IniConfig` struct to all library calls.
CLI flags (`--farm-id`, `--queue-id`, etc.) are applied as in-memory
overrides via `set_setting_in_config` — they don't write to disk.

**Output is YAML by default, JSON with `--output json`.** Most commands
format API responses through `cli_object_repr` which serializes
`serde_json::Value` to YAML. The YAML output preserves insertion order
and uses block literal style for multi-line strings. String values that
look like YAML 1.1 booleans (`yes`, `no`, `true`, `false`, `on`, `off`,
etc.) are single-quoted to prevent data corruption by YAML 1.1 parsers.

**Error exit codes are semantic.** Exit 0 = success, exit 1 = operation
error (API failure, declined confirmation), exit 2 = usage error (missing
required option). `job wait` has additional codes: 2=FAILED, 3=CANCELED,
4=SUSPENDED/ARCHIVED, 5=NOT_COMPATIBLE.

**Credential scoping is the CLI's responsibility.** Library functions
accept pre-built AWS clients. When a command accesses non-Deadline
services (S3, CloudWatch) without `--profile`, the CLI layer must: read
farm/queue from config → resolve attachment settings → assume queue role
→ build scoped client → pass to library function. This keeps library
functions session-agnostic and testable.

**SIGINT handling uses a static `AtomicBool`.** `install_sigint_handler()`
registers a signal handler that flips the flag. Long-running operations
check `should_continue()` periodically. Progress bar callbacks also check
this flag and return `false` to trigger cancellation in library code.

## Behavior & Contracts

**Global options** (before any subcommand):
- `--log-level` — ERROR/WARNING/INFO/DEBUG. Falls back to config, then
  WARNING. Invalid config values produce a stderr warning.
- `--redirect-output` + `--redirect-mode` — redirects stdout/stderr to a
  file via `dup2`. Used by DCC plugins that need to capture CLI output.

**Confirmation prompts:** Commands that mutate state (cancel, requeue)
ask for confirmation unless `--yes` or `settings.auto_accept` is set.
Declined confirmation prints a message and exits 1.

**Resource suggestions on error:** When an API call fails with
AccessDenied, ResourceNotFound, or ValidationException, the CLI attempts
to list alternative resources and suggests them. This helps users who
mistyped an ID or lack permissions to a specific resource.

**Progress bars:** Use `indicatif`. Created lazily, updated via callback
from library code. The callback returns `should_continue()` so
cancellation propagates through the progress reporting path.

**Markdown stripping in help text:** Help strings can contain markdown
(links, bold). These are stripped for terminal display — links become
`text (url)`, bold markers are removed.

**Multi-format parameter parsing:** The `--parameter` flag accepts
`file://path`, inline JSON, or `key=value` syntax. Later values override
earlier ones for the same key.

## Design Decisions

**`CliOptions` struct instead of kwargs.** CLI flags that map to config
overrides are collected into a typed struct. This makes the interface
between CLI parsing and config application explicit and compile-time
checked.

**`TimestampFormat` as an enum with data.** The `Relative` variant carries
a reference time. This eliminates the class of bugs where relative
formatting is requested but no reference time is available — the compiler
enforces it.

**Subcommand modules are flat.** Each file in `commands/` handles one
subcommand group (job, farm, queue, etc.). No nested module trees — the
CLI isn't complex enough to warrant it.

**`suggest_resources_on_client_error` is a free function, not middleware.**
It's called explicitly on error paths rather than wrapping all API calls.
This gives each command control over when and whether to suggest
alternatives (some error paths don't benefit from suggestions).

## Gotchas & Constraints

- The `deadline` binary must be the only binary in the workspace (for
  `cargo_bin("deadline")` in tests to work). Don't add other `[[bin]]`
  targets.

- YAML output uses `serde_yaml` which follows YAML 1.2 by default, but
  many consumers (including the Deadline service) use YAML 1.1 parsers.
  The boolean-quoting logic exists specifically to prevent `yes`/`no`
  values from being misinterpreted as booleans by those parsers.

- `dup2`-based output redirection happens early in `main()`. If it fails,
  the error goes to the original stderr (before redirection). This is
  intentional — you can't report a redirection failure to the redirected
  output.

- `job wait` exit codes are part of the public contract — scripts depend
  on them. Don't change the numeric values.

- The `attachment` and `manifest` subcommand groups are marked BETA.
  Their interface may change in future releases.

## Status & Gaps

Implemented: config, auth (login/logout/status), farm list/get, queue
list/get/export-credentials, fleet list/get, worker list/get, job
list/get/search/wait/logs/cancel/requeue-tasks, attachment
download/upload, manifest snapshot/diff/download/upload, bundle
gui-submit, config gui.

Gaps:
- `bundle submit` (headless job submission) — blocked on work item #11
- `job download-output` / `job sync-output` — blocked on work item #13
- `handle-web-url` — blocked on work item #14
- `job trace-schedule` — experimental, deferred
