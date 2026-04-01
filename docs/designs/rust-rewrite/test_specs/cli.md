# CLI Commands

> Part of [AWS Deadline Cloud Client — Behavioral Test Specification](index.md)
>
> This document describes observable behavior through public interfaces.
> Error descriptions use category labels (e.g., "returns error") rather than
> language-specific exception types. The Rust implementation should produce
> equivalent observable outcomes, not necessarily identical internal mechanics.

---

## Section 37: CLI: root group & common utilities

> **Rust crate:** `deadline-cli` · **Module:** `main` + (new) `common`
>
> **Logic under test:** The `deadline` root command group (version, log level,
> output redirection, help text markdown stripping), error handling decorator,
> config option application, YAML output formatting, file/multi-format parameter
> parsing, progress bar management, SIGINT handling, timestamp formatting,
> markdown stripping, and resource suggestion on API errors.

### `deadline` (root command)

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 1 | Happy path | `deadline --version` | Prints program name and version; exits 0 | |
| 2 | Happy path | `deadline -h` or `deadline --help` | Prints help text with markdown stripped for terminal display | |
| 3 | Happy path | `deadline --log-level DEBUG <subcommand>` | Logging level set to DEBUG; debug messages printed | |
| 4 | Config interaction | No `--log-level` provided; config has `settings.log_level=WARNING` | Log level defaults to WARNING from config | |
| 5 | Config interaction | Config has invalid log level (e.g., `TRACE`) | Falls back to default log level; logs a warning | |
| 6 | Happy path | `deadline --redirect-output /tmp/out.log <subcommand>` | stdout and stderr redirected to the file in append mode | |
| 7 | Happy path | `deadline --redirect-output /tmp/out.log --redirect-mode replace <subcommand>` | File opened in write (replace) mode | |
| 8 | Happy path | On Windows without `--redirect-output` | stdout encoding forced to UTF-8 | |
| 9 | Happy path | Help text contains markdown links `[text](url)` | Displayed as `text (url)` in terminal | |
| 10 | Happy path | Help text contains bold `**text**` | Displayed as `text` in terminal | |
| 11 | Happy path | CLI command name recorded in user agent | Session context updated with dotted command path (e.g., `deadline.job.get`) | |

### Error handling decorator

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 12 | Error handling | Command raises a known operation error | Error message printed to stdout; exits 1 | |
| 13 | Error handling | Command raises an unexpected error | "CLI encountered the following exception" printed with stack trace; exits 1 | |
| 14 | Happy path | `PROMPT_WHEN_COMPLETE` is true and command fails | Prompt displayed before exit | |

### `apply_cli_options_to_config(config?, required_options?, **args) -> config`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 15 | Happy path | `--profile`, `--farm-id`, `--queue-id` all provided | Config updated with all three values | |
| 16 | Happy path | No standard options provided | Returns none (no config modification needed) | |
| 17 | Error handling | `farm_id` in required_options but not set in config or args | Returns usage error "Missing '--farm-id' or default Farm ID configuration" | |
| 18 | Error handling | `queue_id` in required_options but not set | Returns usage error for queue ID | |
| 19 | Error handling | `job_id` in required_options but not set | Returns usage error for job ID | |
| 20 | Happy path | `--yes` flag provided | Config setting `auto_accept` set to `"true"` | |

### `cli_object_repr(obj) -> string`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 21 | Happy path | Dict with string values | Formatted as YAML output | |
| 22 | Happy path | Dict with multi-line string not ending in newline | Newline appended so YAML uses `\|`-style block scalar | |

### `parse_file_parameter(file_path) -> dict`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 23 | Happy path | Valid JSON file | Returns parsed dict | |
| 24 | Happy path | Valid YAML file (non-.json extension) | Returns parsed dict | |
| 25 | Error handling | File does not exist | Returns error (file does not exist) | |
| 26 | Error handling | Path is a directory, not a file | Returns error (not a file) | |
| 27 | Error handling | File contains invalid JSON/YAML | Returns error (formatted incorrectly) | |
| 28 | Error handling | File contains a list instead of a dict | Returns error (should contain a dictionary) | |

### `parse_multi_format_parameters(params) -> dict`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 29 | Happy path | `["key1=value1", "key2=value2"]` | Returns `{"key1": "value1", "key2": "value2"}` | |
| 30 | Happy path | `['{"key": "value"}']` (inline JSON) | Returns `{"key": "value"}` | |
| 31 | Happy path | `["file:///path/to/file.json"]` | File read and parsed; contents merged into result | |
| 32 | Happy path | Mixed formats in one list | All merged; later values override earlier for same key | |
| 33 | Error handling | Malformed parameter string (no `=`, not JSON, no `file://`) | Returns error (not formatted correctly) | |
| 34 | Error handling | Inline JSON is not a dict | Returns error (must contain a dictionary) | |

### `TimestampFormatter(format_type, reference_start_time)`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 35 | Happy path | format_type=UTC | Formats timestamp as UTC ISO 8601 string | |
| 36 | Happy path | format_type=LOCAL | Formats timestamp in local timezone ISO 8601 | |
| 37 | Happy path | format_type=RELATIVE | Formats as time delta from reference start time | |
| 38 | Error handling | Reference time has no timezone | Returns error (must have timezone) | |
| 39 | Error handling | Timestamp to format has no timezone | Returns error (must have timezone) | |

### `strip_markdown_for_terminal(text) -> string`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 40 | Happy path | `[text](url)` | Converted to `text (url)` | |
| 41 | Happy path | `[text][ref]` with reference definition | Converted to `text`; definition line removed | |
| 42 | Happy path | `**bold**` | Converted to `bold` | |
| 43 | Happy path | `*italic*` (not at line start) | Converted to `italic` | |
| 44 | Boundary values | Text with no markdown | Returned unchanged | |

### `SigIntHandler`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 45 | Happy path | No signal received | `continue_operation` is true | |
| 46 | Concurrency/cancellation | SIGINT received | `continue_operation` set to false | |
| 47 | Happy path | Multiple instantiations | Same singleton instance returned | |

### `suggest_resources_on_client_error(exc, farm_id?, queue_id?, fleet_id?, worker_id?, config?)`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 48 | Happy path | AccessDeniedException on GetQueue with valid farm_id | Lists available queues in the farm as suggestions | |
| 49 | Happy path | ResourceNotFoundException on GetFarm | Lists available farms as suggestions | |
| 50 | Happy path | AccessDeniedException on GetJob with farm and queue | Lists recent jobs; falls back to queues then farms | |
| 51 | Happy path | Error code is not access/not-found/validation | Returns empty string (no suggestions) | |
| 52 | Error handling | Listing resources also fails (no permissions) | Returns hint about missing List permissions | |
| 53 | Happy path | More than 10 resources available | Shows first 10 with "... and N more" | |

### `ProgressBarCallbackManager(length, label)`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 54 | Happy path | First callback invocation | Progress bar created with given length and label | |
| 55 | Happy path | Subsequent callback with increased progress | Progress bar updated by the delta | |
| 56 | Happy path | Progress reaches 100% | Progress bar closed | |
| 57 | Concurrency/cancellation | SIGINT handler sets continue_operation=false | Callback returns false; bar closed | |
| 58 | Happy path | Callback after bar already closed | Returns continue_operation value without error | |

> ✅ Complete (58 cases)

---

## Section 38: CLI: deadline config

> **Rust crate:** `deadline-cli` · **Module:** (new) `commands/config`
>
> **Logic under test:** The `deadline config` subcommands: `show`, `get`, `set`,
> `clear`, and `gui`.

### `deadline config show`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 1 | Happy path | Default (verbose) output | Prints config file path, then each setting with value, default indicator, and description | |
| 2 | Output formats | `--output json` | Prints all settings as a single JSON object including config file path | |
| 3 | Happy path | Setting has non-default value | No "(default)" suffix shown for that setting | |
| 4 | Happy path | Setting has default value | "(default)" suffix shown | |

### `deadline config get <setting_name>`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 5 | Happy path | Valid setting name | Prints the current value to stdout | |
| 6 | Happy path | Setting not explicitly set | Prints the default value | |

### `deadline config set <setting_name> <value>`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 7 | Happy path | Valid setting name and value | Setting persisted to config file | |

### `deadline config clear <setting_name>`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 8 | Happy path | Setting was previously set | Setting removed; subsequent `get` returns default | |

### `deadline config gui`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 9 | Happy path | GUI dependencies installed | Config dialog opens | CLI spawns Python process with GUI widgets + deadline-gui-ffi |
| 10 | Happy path | `--install-gui` flag | GUI dependencies installed before opening dialog | CLI spawns Python process with GUI widgets + deadline-gui-ffi |

> ✅ Complete (10 cases)

---

## Section 39: CLI: deadline auth

> **Rust crate:** `deadline-cli` · **Module:** (new) `commands/auth`
>
> **Logic under test:** The `deadline auth` subcommands: `login`, `logout`, `status`.

### `deadline auth login`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 1 | Happy path | Profile supports Deadline Cloud monitor | Opens monitor; prints success message with profile name | |
| 2 | Auth/credential states | Profile does not support monitor login | Appropriate error or message | |

### `deadline auth logout`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 3 | Happy path | Logged in via monitor | Logs out; prints success message | |

### `deadline auth status`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 4 | Happy path | Default (verbose) output | Prints profile name, credential source, auth status, and API availability in aligned columns | |
| 5 | Output formats | `--output json` | Prints JSON with keys: profile_name, source, status, api_availability | |
| 6 | Happy path | `--profile` option provided | Status checked for the specified profile | |
| 7 | Auth/credential states | No valid credentials | Status shows appropriate unauthenticated state | |

> ✅ Complete (7 cases)

---

## Section 40: CLI: deadline farm

> **Rust crate:** `deadline-cli` · **Module:** (new) `commands/farm`
>
> **Logic under test:** The `deadline farm` subcommands: `list` and `get`.

### `deadline farm list`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 1 | Happy path | Farms exist | Prints YAML list with farmId and displayName per farm | |
| 2 | Happy path | `--profile` option | Uses specified AWS profile | |
| 3 | Error handling | API call fails | Prints error with resource suggestions; exits 1 | |

### `deadline farm get`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 4 | Happy path | `--farm-id` provided | Prints full farm details as YAML | |
| 5 | Happy path | No `--farm-id` but default configured | Uses default farm ID from config | |
| 6 | Missing/invalid args | No farm ID available | Returns usage error | |
| 7 | Error handling | API call fails | Prints error with resource suggestions; exits 1 | |

> ✅ Complete (7 cases)

---

## Section 41: CLI: deadline fleet

> **Rust crate:** `deadline-cli` · **Module:** (new) `commands/fleet`
>
> **Logic under test:** The `deadline fleet` subcommands: `list` and `get`.

### `deadline fleet list`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 1 | Happy path | `--farm-id` provided | Prints YAML list with fleetId and displayName | |
| 2 | Missing/invalid args | No farm ID available | Returns usage error | |
| 3 | Error handling | API call fails | Prints error with resource suggestions; exits 1 | |

### `deadline fleet get`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 4 | Happy path | `--fleet-id` provided | Prints full fleet details as YAML | |
| 5 | Happy path | `--queue-id` provided instead of `--fleet-id` | Lists all fleets associated with the queue, with association status | |
| 6 | Error handling | Both `--fleet-id` and `--queue-id` provided | Returns error (only one may be provided) | |
| 7 | Missing/invalid args | Neither `--fleet-id` nor `--queue-id` nor default queue | Returns usage error | |
| 8 | Error handling | API call fails | Prints error with resource suggestions; exits 1 | |

> ✅ Complete (8 cases)

---

## Section 42: CLI: deadline queue

> **Rust crate:** `deadline-cli` · **Module:** (new) `commands/queue`
>
> **Logic under test:** The `deadline queue` subcommands: `list`, `get`,
> `paramdefs`, `export-credentials`, and `sync-output`.

### `deadline queue list`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 1 | Happy path | Farm ID available | Prints YAML list with queueId and displayName | |
| 2 | Missing/invalid args | No farm ID | Returns usage error | |
| 3 | Error handling | API call fails | Prints error with suggestions; exits 1 | |

### `deadline queue get`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 4 | Happy path | Farm and queue IDs available | Prints full queue details as YAML | |
| 5 | Error handling | API call fails | Prints error with suggestions; exits 1 | |

### `deadline queue paramdefs`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 6 | Happy path | Farm and queue IDs available | Prints queue parameter definitions as YAML | |
| 7 | Error handling | API call fails | Prints error with suggestions; exits 1 | |

### `deadline queue export-credentials`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 8 | Happy path | `--mode USER` (default) | Assumes queue user role; prints credentials_process JSON with Version, AccessKeyId, SecretAccessKey, SessionToken, Expiration | |
| 9 | Happy path | `--mode READ` | Assumes queue read role; prints credentials_process JSON | |
| 10 | Error handling | AccessDenied from API | Prints insufficient permissions error; exits 1 | |
| 11 | Error handling | UnrecognizedClientException | Prints authentication failure with login guidance; exits 1 | |
| 12 | Happy path | Telemetry recorded on success | Event includes mode, queue_id, output_format, is_success=true, duration_ms | |
| 13 | Error handling | Telemetry recorded on failure | Event includes error_type | |

### `deadline queue sync-output`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 14 | Happy path | First run with `--bootstrap-lookback-minutes 60` | Initializes checkpoint from 60 minutes ago; downloads outputs; saves checkpoint | |
| 15 | Happy path | Subsequent run with existing checkpoint | Resumes from checkpoint timestamp | |
| 16 | Happy path | `--force-bootstrap` with existing checkpoint | Overwrites checkpoint; re-initializes from lookback | |
| 17 | Happy path | `--ignore-storage-profiles` | Downloads to unmapped paths; checkpoint uses none for storage profile | |
| 18 | Error handling | Both `--storage-profile-id` and `--ignore-storage-profiles` | Returns usage error (cannot use both) | |
| 19 | Error handling | No storage profile configured and `--ignore-storage-profiles` not set | Returns error with guidance about storage profiles | |
| 20 | Error handling | Checkpoint storage profile ID does not match current | Returns error (mismatched storage profile) | |
| 21 | Error handling | Queue has no job attachment settings | Returns error (no job attachments configured) | |
| 22 | Error handling | Checkpoint directory not writable | Returns error (not writable) | |
| 23 | Happy path | `--dry-run` flag | Downloads simulated; checkpoint not saved | |
| 24 | Happy path | `--conflict-resolution SKIP` | Passed through to download; existing files skipped | |
| 25 | Happy path | `--json` flag | Output printed as JSON | |
| 26 | Concurrency/cancellation | Process lock prevents concurrent runs | Second invocation blocked by PID file lock | |

> ✅ Complete (26 cases)

---

## Section 43: CLI: deadline worker

> **Rust crate:** `deadline-cli` · **Module:** (new) `commands/worker`
>
> **Logic under test:** The `deadline worker` subcommands: `list` and `get`.

### `deadline worker list`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 1 | Happy path | `--fleet-id` and farm ID available | Prints workers with workerId, status, createdAt; shows count and offset | |
| 2 | Happy path | `--page-size 10 --item-offset 5` | Returns up to 10 workers starting at offset 5 | |
| 3 | Missing/invalid args | `--fleet-id` not provided | Returns usage error (required) | |
| 4 | Error handling | API call fails | Prints error with suggestions; exits 1 | |

### `deadline worker get`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 5 | Happy path | `--fleet-id` and `--worker-id` provided | Prints full worker details as YAML | |
| 6 | Missing/invalid args | `--fleet-id` or `--worker-id` not provided | Returns usage error (required) | |
| 7 | Error handling | API call fails | Prints error with suggestions; exits 1 | |

> ✅ Complete (7 cases)

---

## Section 44: CLI: deadline job

> **Rust crate:** `deadline-cli` · **Module:** (new) `commands/job`
>
> **Logic under test:** The `deadline job` subcommands. This is the largest CLI
> group covering job get, list, search, download-output, logs, wait, trace-schedule,
> and cancel. Due to the size of the source (~75KB), test cases focus on the
> observable CLI interface behavior.

### `deadline job get`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 1 | Happy path | Farm, queue, and job IDs available | Prints full job details as YAML | |
| 2 | Missing/invalid args | No job ID available | Returns usage error | |
| 3 | Error handling | API call fails | Prints error with suggestions; exits 1 | |

### `deadline job list`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 4 | Happy path | Farm and queue IDs available | Prints YAML list of jobs with jobId and name | |
| 5 | Error handling | API call fails | Prints error with suggestions; exits 1 | |

### `deadline job search`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 6 | Happy path | `--filter-expressions` provided as JSON | Searches jobs with filter; prints results | |
| 7 | Happy path | `--filter-expressions file://path.json` | Reads filter from file | |
| 8 | Happy path | `--sort-expressions` provided | Results sorted accordingly | |
| 9 | Pagination/batching | `--page-size` and `--item-offset` | Pagination applied to search | |

### `deadline job download-output`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 10 | Happy path | Job with output attachments | Output files downloaded to local directory; summary printed | |
| 11 | Happy path | `--step-id` and `--task-id` provided | Downloads output for specific step/task | |
| 12 | Happy path | `--output` directory specified | Files downloaded to specified directory | |
| 13 | Happy path | `--conflict-resolution SKIP` | Existing files not overwritten | |
| 14 | Interactive vs scripted | `--yes` flag | Auto-accepts confirmation prompts | |
| 15 | Error handling | Job has no attachments | Prints appropriate message | |
| 16 | Happy path | `--json` flag | Output printed as JSON | |

### `deadline job logs`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 17 | Happy path | Job with log events | Prints log lines to stdout | |
| 18 | Happy path | `--follow` flag | Continuously polls for new log events until interrupted | |
| 19 | Happy path | `--timestamp-format utc` | Timestamps shown in UTC | |
| 20 | Happy path | `--timestamp-format local` | Timestamps shown in local timezone | |
| 21 | Happy path | `--timestamp-format relative` | Timestamps shown as delta from start | |

### `deadline job wait`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 22 | Happy path | Job completes within timeout | Exits 0 with final status | |
| 23 | Error handling | Job fails | Exits with non-zero code | |
| 24 | Concurrency/cancellation | Timeout exceeded | Exits with timeout indication | |

### `deadline job cancel`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 25 | Happy path | Valid job ID | Job cancelled; confirmation printed | |
| 26 | Error handling | API call fails | Prints error; exits 1 | |

### `deadline job trace-schedule`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 27 | Happy path | Job with steps and tasks | Prints schedule trace showing task execution timeline | |
| 28 | Output formats | `--json` flag | Schedule printed as JSON | |

### `deadline job requeue-tasks`

> **Logic under test:** Requeues tasks of a job by setting their target run status
> to `PENDING`. Calls `GetJob` for a summary, `ListSteps` to enumerate steps,
> `ListTasks` per step to find matching tasks, and `UpdateTask` per task to requeue.
> Uses an adaptive retry client config for the `UpdateTask` calls. Default statuses
> are `FAILED`, `CANCELED`, `SUSPENDED`.

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 29 | Happy path | Job with FAILED tasks, default `--run-status` | Prints job summary, step-by-step requeue progress, and total count; calls `UpdateTask` with `targetRunStatus=PENDING` for each matching task | Default statuses: FAILED, CANCELED, SUSPENDED |
| 30 | Happy path | `--run-status FAILED --run-status SUCCEEDED` | Only tasks with those two statuses are requeued | `--run-status` is repeatable |
| 31 | Happy path | Task has parameters | Output shows `param=value (taskId)` format per task | |
| 32 | Happy path | Task has no parameters | Output shows just `taskId` per task | |
| 33 | Happy path | Job has multiple steps | Each step printed with its own requeue summary; tasks listed per step | |
| 34 | Boundary values | No tasks match the requested statuses | Prints "No tasks to requeue." and exits 0 | |
| 35 | Boundary values | Step has no tasks matching status | Prints "Step has no tasks to requeue." for that step; continues to next | |
| 36 | Interactive vs scripted | `--yes` flag | Skips confirmation prompt; prints estimated count | |
| 37 | Interactive vs scripted | No `--yes`, user confirms | Prints "This action will requeue..." prompt; proceeds on confirmation | |
| 38 | Interactive vs scripted | No `--yes`, user declines | Prints "No tasks were requeued."; exits 1 | |
| 39 | Interactive vs scripted | `settings.auto_accept` is `true` in config | Behaves like `--yes`; skips prompt | |
| 40 | Missing/invalid args | No job ID available | Returns usage error | Required via `_apply_cli_options_to_config` |
| 41 | Error handling | `GetJob` API call fails | Prints error with suggestions; exits 1 | |
| 42 | Error handling | `UpdateTask` API call fails | Error propagated; partial requeue may have occurred | Adaptive retry handles transient failures |

> ✅ Complete (42 cases)

---

## Section 45: CLI: deadline bundle

> **Rust crate:** `deadline-cli` · **Module:** (new) `commands/bundle`
>
> **Logic under test:** The `deadline bundle` subcommands: `submit` and `gui-submit`.

### `deadline bundle submit <path>`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 1 | Happy path | Valid job bundle directory | Job submitted; job ID printed | |
| 2 | Happy path | `--parameter key=value` provided | Parameter overrides applied to submission | |
| 3 | Happy path | `--parameter file://params.json` | Parameters loaded from file | |
| 4 | Happy path | `--name "My Job"` | Job name overridden | |
| 5 | Happy path | `--priority 50` | Job priority set | |
| 6 | Happy path | `--max-failed-tasks-count 10` | Failure threshold set | |
| 7 | Happy path | `--max-retries-per-task 3` | Retry limit set | |
| 8 | Interactive vs scripted | `--yes` flag | Skips confirmation prompt | |
| 9 | Happy path | `--json` flag | Output as JSON with jobId | |
| 10 | Error handling | Bundle directory does not exist | Returns error; exits 1 | |
| 11 | Error handling | Bundle missing required template file | Returns error; exits 1 | |
| 12 | Happy path | Bundle with job attachments | Files uploaded before submission; attachment settings included | |
| 13 | Error handling | API call fails | Prints error; exits 1 | |

### `deadline bundle gui-submit`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 14 | Happy path | GUI dependencies installed | Opens submission dialog | CLI spawns Python process with GUI widgets + deadline-gui-ffi |

> ✅ Complete (14 cases)

---

## Section 46: CLI: deadline attachment

> **Rust crate:** `deadline-cli` · **Module:** (new) `commands/attachment`
>
> **Logic under test:** The `deadline attachment` subcommands: `download` and `upload`.

### `deadline attachment download`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 1 | Happy path | `--manifests` and `--s3-root-uri` provided with `--profile` | Downloads files using provided S3 URI and profile credentials | |
| 2 | Happy path | No `--profile`; farm and queue configured | S3 URI derived from queue settings; queue role credentials used | |
| 3 | Error handling | Queue has no attachment settings | Returns error (missing job attachment settings) | |
| 4 | Error handling | No S3 root URI available from any source | Returns error (no valid S3 root path) | |
| 5 | Happy path | `--conflict-resolution SKIP` | Existing files skipped | |
| 6 | Happy path | No `--conflict-resolution`; config has a value | Config value used | |
| 7 | Happy path | `--path-mapping-rules` provided | Path mapping applied to download destinations | |
| 8 | Output formats | `--json` flag | Summary printed as JSON | |
| 9 | Happy path | Download summary printed | Shows processed/skipped files and transfer rate | |

### `deadline attachment upload`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 10 | Happy path | `--manifests` and `--root-dirs` provided | Files uploaded to CAS; upload info printed | |
| 11 | Happy path | `--manifests` and `--path-mapping-rules` provided | Path mapping used for source/destination resolution | |
| 12 | Happy path | `--s3-root-uri` and `--profile` provided | Uses provided S3 URI and profile | |
| 13 | Happy path | No `--profile`; farm and queue configured | S3 URI from queue; queue role credentials used | |
| 14 | Happy path | `--upload-manifest-path` provided | Manifest file uploaded to CAS under given prefix | |
| 15 | Error handling | Queue has no attachment settings | Returns error (missing job attachment settings) | |

> ✅ Complete (15 cases)

---

## Section 47: CLI: deadline manifest

> **Rust crate:** `deadline-cli` · **Module:** (new) `commands/manifest`
>
> **Logic under test:** The `deadline manifest` subcommands: `snapshot`, `diff`,
> `download`, and `upload`.

### `deadline manifest snapshot`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 1 | Happy path | `--root` is a valid directory | Manifest created; path printed | |
| 2 | Happy path | `--destination` provided | Manifest written to specified directory | |
| 3 | Happy path | No `--destination` | Defaults to root directory | |
| 4 | Happy path | `--name` provided | Manifest filename uses provided name | |
| 5 | Happy path | `--diff` with existing manifest | Only changed files included in output manifest | |
| 6 | Happy path | `--force-rehash` with `--diff` | Hash-based comparison instead of timestamp-based | |
| 7 | Happy path | `--include` and `--exclude` patterns | Only matching files included | |
| 8 | Error handling | Root directory does not exist | Returns error; exits 1 | |
| 9 | Error handling | Destination directory does not exist | Returns error; exits 1 | |
| 10 | Output formats | `--json` flag | Snapshot result printed as JSON | |
| 11 | Happy path | On Windows, manifest path exceeds max length | Warning printed about long path | |

### `deadline manifest diff`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 12 | Happy path | `--manifest` and `--root` provided | Prints new, modified, and deleted files | |
| 13 | Happy path | `--force-rehash` | Uses hash comparison | |
| 14 | Output formats | `--json` flag | Diff result printed as JSON with new/modified/deleted lists | |
| 15 | Error handling | Manifest file does not exist | Returns error; exits 1 | |
| 16 | Error handling | Root directory does not exist | Returns error; exits 1 | |

### `deadline manifest download <download_dir>`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 17 | Happy path | `--job-id`, farm, and queue available | Manifests downloaded and merged; written to download_dir | |
| 18 | Happy path | `--step-id` provided | Includes step dependencies and step-specific outputs | |
| 19 | Happy path | `--asset-type INPUT` | Only input manifests downloaded | |
| 20 | Happy path | `--asset-type OUTPUT` | Only output manifests downloaded | |
| 21 | Output formats | `--json` flag | Download response printed as JSON | |
| 22 | Error handling | Download directory does not exist | Returns error; exits 1 | |

### `deadline manifest upload <manifest_file>`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 23 | Happy path | `--s3-cas-uri` and `--profile` provided | Manifest uploaded to S3 CAS | |
| 24 | Happy path | No `--s3-cas-uri`; farm and queue configured | S3 settings derived from queue; queue credentials used | |
| 25 | Happy path | `--s3-manifest-prefix` provided | Manifest uploaded under specified prefix | |
| 26 | Error handling | Manifest file does not exist | Returns error; exits 1 | |

> ✅ Complete (26 cases)

---

## Section 48: CLI: deadline handle-web-url

> **Rust crate:** `deadline-cli` · **Module:** (new) `commands/handle_web_url`
>
> **Logic under test:** The `deadline handle-web-url` command that processes
> `deadline://` protocol URLs from web applications, and installs/uninstalls
> the OS protocol handler.

### `deadline handle-web-url <url>`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 1 | Happy path | `deadline://download-output?farm-id=...&queue-id=...&job-id=...` | Downloads job output using provided IDs | |
| 2 | Happy path | URL includes optional `step-id` and `task-id` | Downloads output for specific step/task | |
| 3 | Happy path | URL includes `profile` parameter | Uses specified AWS profile | |
| 4 | Happy path | URL omits `profile` | Best profile guessed from farm and queue IDs | |
| 5 | Error handling | URL scheme is not `deadline` | Returns error (unsupported scheme) | |
| 6 | Error handling | Command in URL is not `download-output` | Returns error (command not supported) | |
| 7 | Error handling | Required parameters missing from URL | Returns error (missing required parameters) | |
| 8 | Error handling | URL provided with `--install` or `--uninstall` | Returns error (cannot combine) | |

### `deadline handle-web-url --install / --uninstall`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 9 | Happy path | `--install` | Registers `deadline://` protocol handler; prints success | |
| 10 | Happy path | `--uninstall` | Removes `deadline://` protocol handler; prints success | |
| 11 | Happy path | `--install --all-users` | Installs for all users (system-wide) | |
| 12 | Error handling | Both `--install` and `--uninstall` | Returns error (only one may be provided) | |
| 13 | Error handling | No URL, no `--install`, no `--uninstall` | Returns error (at least one required) | |

### `deadline handle-web-url --prompt-when-complete`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 14 | Interactive vs scripted | Flag set | Prompts for keyboard input before exiting | |

> ✅ Complete (14 cases)

---

## Section 49: CLI: deadline mcp-server

> **Rust crate:** `deadline-cli` · **Module:** (new) `commands/mcp_server`
> **⚠️ DEFERRED:** Depends on MCP protocol library availability in Rust.
>
> **Logic under test:** The `deadline mcp-server` command that starts the MCP
> (Model Context Protocol) server.

### `deadline mcp-server`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 1 | Happy path | MCP dependencies installed | Server starts; telemetry event recorded with usage_mode=MCP, startup_method=cli | |
| 2 | Error handling | MCP dependencies not installed | Prints error with install instructions; exits 1 | |
| 3 | Happy path | Telemetry recording fails | Server still starts (telemetry errors suppressed) | |
| 4 | Concurrency/cancellation | Ctrl+C or Ctrl+D | Server shuts down gracefully | |

> ✅ Complete (4 cases)

---
