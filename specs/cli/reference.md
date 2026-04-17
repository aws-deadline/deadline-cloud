# CLI Reference

Complete command reference for the `deadline` CLI. Commands marked ✅ are
implemented in Rust. Commands marked 🔲 are not yet implemented (exist in
the legacy CLI only).

## Global Options

All commands accept these options before the subcommand:

| Flag | Description |
|------|-------------|
| `--version` | Print version and exit |
| `-h, --help` | Print help |
| `--log-level <LEVEL>` | ERROR, WARNING, INFO, DEBUG. Falls back to config `settings.log_level`, then WARNING |
| `--redirect-output <PATH>` | Redirect stdout/stderr to file (used by DCC plugins) |
| `--redirect-mode <MODE>` | `append` (default) or `replace` |

## Common Patterns

**Resource resolution:** Most commands accept `--farm-id`, `--queue-id`,
`--job-id`. If omitted, values are read from `~/.deadline/config` for the
active profile. Missing required IDs produce exit code 2.

**Profile override:** `--profile` overrides the AWS profile for the
command, affecting both credential resolution and which config section is
read.

**Output format:** Commands with `--output` accept `verbose` (YAML,
default) or `json`. Some BETA commands use `--json` as a flag instead.

**Confirmation:** Mutating commands ask for confirmation unless `--yes` or
config `settings.auto_accept` is set. Declined → exit 1.

**Error suggestions:** On AccessDenied, ResourceNotFound, or
ValidationException, the CLI attempts to list alternative resources and
suggests them in the error output.

---

## `deadline config` ✅

Manage the Deadline Cloud workstation configuration file.

### `deadline config show`

| Flag | Description |
|------|-------------|
| `--output <verbose\|json>` | Output format |

- Reads all 18 settings from config
- Verbose: prints file path, then each setting with value, default indicator, and description
- JSON: prints a flat object with `settings.config_file_path` and all setting key/value pairs

### `deadline config get <SETTING>`

Prints the current value of a single setting. Uses default if not explicitly set.

### `deadline config set <SETTING> <VALUE>`

Persists a value to the config file (atomic write).

### `deadline config clear <SETTING>`

Writes the default value back to the config file (does not remove the key).

### `deadline config gui` ✅

Spawns a Python process that loads the GUI widget package and shows the
config dialog. Requires the Python GUI package to be installed.

---

## `deadline auth` ✅

Manage authentication for Deadline Cloud.

### `deadline auth login`

- Checks credential source is DCM (Deadline Cloud Monitor)
- Reads `deadline-cloud-monitor.path` from config
- Spawns the monitor process with `login --profile <name>`
- Polls auth status every 0.5s until authenticated or cancelled
- Prints success message with profile name
- Error if not a DCM profile or monitor binary not found

### `deadline auth logout`

- Spawns monitor with `logout --profile <name>`
- Invalidates session cache (clears cached credentials)
- Prints success message
- Error if not a DCM profile

### `deadline auth status`

| Flag | Description |
|------|-------------|
| `--profile <NAME>` | AWS profile to check |
| `--output <verbose\|json>` | Output format |

- Determines credential source (HOST_PROVIDED, DEADLINE_CLOUD_MONITOR_LOGIN, NOT_VALID)
- Calls STS GetCallerIdentity to check auth status (AUTHENTICATED, NEEDS_LOGIN, CONFIGURATION_ERROR)
- Calls ListFarms (maxResults=1) to check API availability
- Verbose: aligned key-value display. JSON: flat object.

---

## `deadline farm` ✅

### `deadline farm list`

| Flag | Description |
|------|-------------|
| `--profile <NAME>` | AWS profile |

- Calls `ListFarms` API (paginated, all pages)
- Outputs: `farmId`, `displayName` per farm as YAML list

### `deadline farm get`

| Flag | Description |
|------|-------------|
| `--profile <NAME>` | AWS profile |
| `--farm-id <ID>` | Farm to get (or from config) |

- Calls `GetFarm` API
- Outputs full response as YAML
- On error: suggests available farms

---

## `deadline fleet` ✅

### `deadline fleet list`

| Flag | Description |
|------|-------------|
| `--profile <NAME>` | AWS profile |
| `--farm-id <ID>` | Farm (or from config) |

- Calls `ListFleets` API (paginated)
- Outputs: `fleetId`, `displayName` per fleet

### `deadline fleet get`

| Flag | Description |
|------|-------------|
| `--profile <NAME>` | AWS profile |
| `--farm-id <ID>` | Farm |
| `--fleet-id <ID>` | Fleet (required) |

- Calls `GetFleet` API
- Outputs full response as YAML
- On error: suggests available fleets

---

## `deadline queue` ✅

### `deadline queue list`

| Flag | Description |
|------|-------------|
| `--profile <NAME>` | AWS profile |
| `--farm-id <ID>` | Farm |

- Calls `ListQueues` API (paginated)
- Outputs: `queueId`, `displayName` per queue

### `deadline queue get`

| Flag | Description |
|------|-------------|
| `--profile <NAME>` | AWS profile |
| `--farm-id <ID>` | Farm |
| `--queue-id <ID>` | Queue |

- Calls `GetQueue` API
- Outputs full response as YAML
- On error: suggests available queues

### `deadline queue export-credentials`

| Flag | Description |
|------|-------------|
| `--profile <NAME>` | AWS profile |
| `--farm-id <ID>` | Farm |
| `--queue-id <ID>` | Queue |
| `--mode <USER\|READ>` | Credential mode (default: USER) |

- Calls `AssumeQueueRoleForUser` (USER mode) or `AssumeQueueRoleForRead` (READ mode)
- Outputs JSON in AWS `credential_process` format: `Version`, `AccessKeyId`, `SecretAccessKey`, `SessionToken`, `Expiration` (RFC 3339)
- Records telemetry event `com.amazon.rum.deadline.queue_export_credentials` with success/fail, duration, mode, queue_id

### `deadline queue get-storage-profile`

| Flag | Description |
|------|-------------|
| `--profile <NAME>` | AWS profile |
| `--farm-id <ID>` | Farm |
| `--queue-id <ID>` | Queue |
| `--storage-profile-id <ID>` | Storage profile (required) |

- Calls `GetStorageProfileForQueue` API
- Outputs full response as YAML

### `deadline queue paramdefs`

| Flag | Description |
|------|-------------|
| `--profile <NAME>` | AWS profile |
| `--farm-id <ID>` | Farm |
| `--queue-id <ID>` | Queue |

- Lists all queue environments (paginated), sorted by priority
- For each: fetches environment template, extracts `parameterDefinitions`
- Validates, deduplicates by name, auto-detects UI controls
- Outputs merged parameter definitions as YAML list

### `deadline queue sync-output` 🔲

| Flag | Description |
|------|-------------|
| `--profile <NAME>` | AWS profile |
| `--farm-id <ID>` | Farm |
| `--queue-id <ID>` | Queue |
| `--storage-profile-id <ID>` | Storage profile for path mapping |
| `--json` | JSON output |
| `--bootstrap-lookback-minutes <N>` | Minutes to look back on first run (default: 0) |
| `--checkpoint-dir <PATH>` | Directory for download progress checkpoints |
| `--force-bootstrap` | Restart from lookback period, ignoring checkpoint |
| `--ignore-storage-profiles` | Skip path mapping (same-machine only) |
| `--conflict-resolution <MODE>` | SKIP, OVERWRITE (default), CREATE_COPY |
| `--dry-run` | Show what would be downloaded without downloading |

- Incrementally downloads new job attachment outputs for all jobs in a queue
- On first run (or `--force-bootstrap`): downloads outputs completed since `bootstrap-lookback-minutes` ago
- On subsequent runs: resumes from checkpoint file in `checkpoint-dir`
- Requires storage profile for cross-OS path mapping (unless `--ignore-storage-profiles`)
- Resolves queue attachment settings via `GetQueue` API
- Assumes queue role for S3 access (DCM users)
- Downloads via `OutputDownloader` with path remapping
- Writes checkpoint after each successful batch

---

## `deadline worker` ✅

### `deadline worker list`

| Flag | Description |
|------|-------------|
| `--profile <NAME>` | AWS profile |
| `--farm-id <ID>` | Farm |
| `--fleet-id <ID>` | Fleet (required) |
| `--page-size <N>` | Results per page (default: 5) |
| `--item-offset <N>` | Starting offset (default: 0) |

- Calls `SearchWorkers` API
- Prints count/offset header, then YAML list with `workerId`, `status`, `createdAt`

### `deadline worker get`

| Flag | Description |
|------|-------------|
| `--profile <NAME>` | AWS profile |
| `--farm-id <ID>` | Farm |
| `--fleet-id <ID>` | Fleet (required) |
| `--worker-id <ID>` | Worker (required) |

- Calls `GetWorker` API
- Outputs full response as YAML

---

## `deadline job` ✅ (partial)

### `deadline job list` ✅

| Flag | Description |
|------|-------------|
| `--profile <NAME>` | AWS profile |
| `--farm-id <ID>` | Farm |
| `--queue-id <ID>` | Queue |
| `--page-size <N>` | Results per page (default: 5) |
| `--item-offset <N>` | Starting offset (default: 0) |

- Calls `SearchJobs` API sorted by `CREATED_AT` descending
- Prints header: `"Displaying N of T Jobs starting at O"`
- YAML list with: `name`, `jobId`, `taskRunStatus`, `startedAt`, `endedAt`, `createdBy`, `createdAt`, `estimatedTimeRemaining`
- `estimatedTimeRemaining` computed client-side from `taskRunStatusCounts` and `startedAt`

### `deadline job get` ✅

| Flag | Description |
|------|-------------|
| `[SEARCH_TERM]` | Positional: job ID or search string |
| `--profile <NAME>` | AWS profile |
| `--farm-id <ID>` | Farm |
| `--queue-id <ID>` | Queue |
| `--job-id <ID>` | Job |

- If `SEARCH_TERM` matches `job-[0-9a-f]{32}`: treated as `--job-id`
- If `SEARCH_TERM` is other text: searches jobs by name
  - Single match → full details
  - Multiple matches → summary list with "To get details, run: ..."
  - No matches → "No jobs found matching ..."
- Direct get: calls `GetJob`, outputs full response as YAML, plus `estimatedTimeRemaining` line

### `deadline job search` ✅

| Flag | Description |
|------|-------------|
| `--profile <NAME>` | AWS profile |
| `--farm-id <ID>` | Farm |
| `--queue-id <ID>` | Queue |
| `--filter-expressions <JSON>` | Filter (inline JSON or `file://path`) |
| `--sort-expressions <JSON>` | Sort (inline JSON or `file://path`) |
| `--page-size <N>` | Results per page (default: 5) |
| `--item-offset <N>` | Starting offset (default: 0) |

- Calls `SearchJobs` with user-provided filter/sort expressions
- Output format matches `job list`

### `deadline job wait` ✅

| Flag | Description |
|------|-------------|
| `--profile <NAME>` | AWS profile |
| `--farm-id <ID>` | Farm |
| `--queue-id <ID>` | Queue |
| `--job-id <ID>` | Job |
| `--max-poll-interval <SECS>` | Max backoff interval (default: 120) |
| `--timeout <SECS>` | Timeout, 0=none (default: 0) |
| `--output <verbose\|json>` | Output format |

- Polls `GetJob` with exponential backoff (starts 0.5s, doubles, capped at max-poll-interval)
- Terminal states: SUCCEEDED, FAILED, CANCELED, SUSPENDED, NOT_COMPATIBLE
- On non-SUCCEEDED: collects failed tasks from steps/tasks
- Verbose: status updates on stderr via `\r` overwrite
- JSON: single JSON object on stdout at completion
- Exit codes: 0=SUCCEEDED, 1=timeout, 2=FAILED, 3=CANCELED, 4=SUSPENDED/ARCHIVED, 5=NOT_COMPATIBLE

### `deadline job logs` ✅

| Flag | Description |
|------|-------------|
| `--profile <NAME>` | AWS profile |
| `--farm-id <ID>` | Farm |
| `--queue-id <ID>` | Queue |
| `--job-id <ID>` | Job |
| `--session-id <ID>` | Session (auto-selected if omitted) |
| `--limit <N>` | Max log events (default: 100) |
| `--start-time <ISO>` | Filter start |
| `--end-time <ISO>` | Filter end |
| `--next-token <TOKEN>` | Pagination token |
| `--output <verbose\|json>` | Output format |
| `--timestamp-format <FMT>` | `utc`, `local`, or `relative` |

- Calls `GetJob` first for job name
- Session auto-selection if `--session-id` omitted:
  - Paginates all sessions, prefers ongoing (no `endedAt`), then most recently ended
  - Prints selection message: "Using the only available session: ..." or "Using the latest session: ..."
- Fetches CloudWatch Logs from `/aws/deadline/{farm_id}/{queue_id}` log group, `{session_id}` stream
- Credential scoping: DCM users → queue-role credentials for CloudWatch access
- Verbose: `[timestamp] message` per event
- JSON: object with jobId, jobName, events array, count, nextToken, logGroup, logStream
- `relative` timestamp shows delta from session start

### `deadline job cancel` ✅

| Flag | Description |
|------|-------------|
| `--profile <NAME>` | AWS profile |
| `--farm-id <ID>` | Farm |
| `--queue-id <ID>` | Queue |
| `--job-id <ID>` | Job |
| `--mark-as <STATUS>` | CANCELED (default), SUSPENDED, FAILED, SUCCEEDED |
| `--yes` | Skip confirmation |

- Calls `GetJob` for summary, prints as YAML (name, jobId, taskRunStatus, taskRunStatusCounts, startedAt, endedAt, createdBy, createdAt)
- Asks confirmation (varies by mark-as value)
- Calls `UpdateJob` with `targetTaskRunStatus`
- Declined → "Job not canceled." exit 1

### `deadline job requeue-tasks` ✅

| Flag | Description |
|------|-------------|
| `--profile <NAME>` | AWS profile |
| `--farm-id <ID>` | Farm |
| `--queue-id <ID>` | Queue |
| `--job-id <ID>` | Job |
| `--run-status <STATUS>...` | Repeatable. Default: FAILED, CANCELED, SUSPENDED |
| `--yes` | Skip confirmation |

- Calls `GetJob` for summary, prints job name and `taskRunStatusCounts`
- Computes estimated task count from status counts matching `--run-status`
- If zero: "No tasks to requeue." exit 0
- Asks confirmation with estimated count and breakdown
- Iterates `ListSteps` → `ListTasks` per step
- For each matching task: prints status, parameters, task ID; calls `UpdateTask` with `targetRunStatus=PENDING`
- Prints total requeued count

### `deadline job get-session` ✅

| Flag | Description |
|------|-------------|
| `--profile`, `--farm-id`, `--queue-id`, `--job-id` | Standard |
| `--session-id <ID>` | Session (required) |

- Calls `GetSession` API, outputs full response as YAML

### `deadline job list-sessions` ✅

Standard flags. Calls `ListSessions`, outputs sessions array as YAML.

### `deadline job list-steps` ✅

Standard flags. Calls `ListSteps`, outputs steps array as YAML.

### `deadline job list-tasks` ✅

| Flag | Description |
|------|-------------|
| Standard flags | |
| `--step-id <ID>` | Step (required) |

Calls `ListTasks`, outputs tasks array as YAML.

### `deadline job download-output` 🔲

| Flag | Description |
|------|-------------|
| `--profile <NAME>` | AWS profile |
| `--farm-id <ID>` | Farm |
| `--queue-id <ID>` | Queue |
| `--job-id <ID>` | Job |
| `--step-id <ID>` | Step (optional, scopes download) |
| `--task-id <ID>` | Task (optional, requires step-id) |
| `--conflict-resolution <MODE>` | SKIP, OVERWRITE, CREATE_COPY |
| `--yes` | Skip confirmation |
| `--output <verbose\|json>` | Output format |

- Resolves queue attachment settings via `GetQueue` API
- Assumes queue role for S3 access (DCM users)
- Calls `GetJob` to get attachment metadata (manifest properties with root paths)
- Resolves local download paths using storage profile path mapping
- Shows download plan with paths and sizes, asks confirmation
- Downloads files from S3 CAS via `OutputDownloader`
- Reports progress (file counts, transfer rate)
- On Windows: checks long path registry setting, warns if disabled

### `deadline job trace-schedule` ✅ (EXPERIMENTAL)

| Flag | Description |
|------|-------------|
| `--profile`, `--farm-id`, `--queue-id`, `--job-id` | Standard |
| `-v` | Verbose statistics |
| `--trace-format chrome` | Chrome trace format output |
| `--trace-file <PATH>` | Output file for trace |

- Fetches all sessions and session actions
- Computes timing statistics (session count, action count, durations)
- Optional Chrome trace format output for visualization

---

## `deadline bundle` (partial)

### `deadline bundle submit` ✅

| Flag | Description |
|------|-------------|
| `JOB_BUNDLE_DIR` | Positional: path to job bundle directory |
| `-p, --parameter <PARAM>` | Repeatable. `Key=Value`, inline JSON, or `file://path` |
| `--profile <NAME>` | AWS profile |
| `--farm-id <ID>` | Farm |
| `--queue-id <ID>` | Queue |
| `--storage-profile-id <ID>` | Storage profile |
| `--name <NAME>` | Override job name |
| `--priority <N>` | Job priority (default: 50) |
| `--max-failed-tasks-count <N>` | Max failed tasks before job fails |
| `--max-retries-per-task <N>` | Max retries per task |
| `--max-worker-count <N>` | Max workers |
| `--target-task-run-status <READY\|SUSPENDED>` | Initial task status |
| `--job-attachments-file-system <COPIED\|VIRTUAL>` | Attachment access method |
| `--yes` | Skip confirmation |
| `--require-paths-exist` | Error if input files missing |
| `--submitter-name <NAME>` | Submitter application name |
| `--known-asset-path <PATH>` | Repeatable. Suppress warnings for paths outside storage profile |
| `--save-debug-snapshot <PATH>` | Save debug snapshot instead of submitting |
| `--force-s3-check / --no-force-s3-check` | Force S3 existence verification |
| `--submitter-info <INFO>` | Repeatable. `Key=Value`, inline JSON, or `file://path` |

Flow:
1. Validate bundle directory (symlink containment)
2. Load template, validate specificationVersion
3. Read parameter values from bundle, merge with `--parameter` overrides
4. Resolve queue environment parameters via `ListQueueEnvironments` + `GetQueueEnvironment`
5. Merge queue params with job params, validate types
6. Extract asset references from PATH parameters
7. Group input files by asset root (respecting storage profile LOCAL/SHARED)
8. Hash input files (using hash cache), create manifests
9. Upload manifests and files to S3 CAS (with progress reporting)
10. Build `CreateJob` request with attachment metadata
11. Call `CreateJob` API
12. Optionally wait for job completion
13. Record telemetry (success/fail, duration, file counts)

### `deadline bundle gui-submit` ✅

Spawns a Python process that loads the GUI widget package and shows the
job submission dialog. No flags beyond `--profile`.

---

## `deadline attachment` ✅ (BETA)

### `deadline attachment download`

| Flag | Description |
|------|-------------|
| `-m, --manifests <PATH>...` | Manifest files (required, multiple) |
| `--s3-root-uri <URI>` | S3 root URI |
| `--path-mapping-rules <PATH>` | Path mapping rules JSON file |
| `--farm-id <ID>` | Farm |
| `--queue-id <ID>` | Queue |
| `--profile <NAME>` | AWS profile for S3 |
| `--conflict-resolution <MODE>` | SKIP, OVERWRITE, CREATE_COPY |
| `--json` | JSON output |

- Reads and decodes manifest files
- Applies path mapping rules to determine local download destinations
- Resolves S3 credentials:
  - With `--profile`: uses profile credentials + `--s3-root-uri`
  - Without `--profile`: reads farm/queue from config, assumes queue role
- Downloads files from S3 CAS by hash
- Conflict resolution precedence: flag > config `settings.conflict_resolution` > CREATE_COPY
- Reports download statistics (processed/skipped counts, transfer rate)

### `deadline attachment upload`

| Flag | Description |
|------|-------------|
| `-m, --manifests <PATH>...` | Manifest files (required, multiple) |
| `-r, --root-dirs <PATH>...` | Root directories |
| `--path-mapping-rules <PATH>` | Path mapping rules JSON file |
| `--s3-root-uri <URI>` | S3 root URI |
| `--upload-manifest-path <PREFIX>` | S3 prefix for manifest upload |
| `--farm-id <ID>` | Farm |
| `--queue-id <ID>` | Queue |
| `--profile <NAME>` | AWS profile |
| `--json` | JSON output |

- Requires exactly one of `--root-dirs` or `--path-mapping-rules`
- Reads and decodes manifest files
- Matches manifests to roots via hashed source path in filename
- Uploads files to S3 CAS (content-addressed by hash)
- Optionally uploads manifest to S3 at `--upload-manifest-path`
- Same credential resolution as download

---

## `deadline manifest` ✅ (BETA)

### `deadline manifest snapshot`

| Flag | Description |
|------|-------------|
| `--root <DIR>` | Directory to snapshot (required) |
| `-d, --destination <DIR>` | Where to write manifest (default: root) |
| `-n, --name <NAME>` | Manifest name |
| `-i, --include <GLOB>...` | Include patterns |
| `-e, --exclude <GLOB>...` | Exclude patterns |
| `--include-exclude-config <JSON>` | Glob config (file path or inline JSON) |
| `--diff <PATH>` | Diff against existing manifest |
| `--force-rehash` | Hash-based diff instead of mtime |
| `--json` | JSON output |

- Globs files in root using include/exclude config
- Without `--diff`: hashes all files, creates manifest
- With `--diff` and no `--force-rehash`: fast diff (mtime/size), hashes only new/modified
- With `--diff` and `--force-rehash`: hashes all, compares hashes
- Writes manifest to destination: `{name}-{root_hash}-{timestamp}.manifest`
- Returns None (no output) if no files match or diff produces zero changes

### `deadline manifest diff`

| Flag | Description |
|------|-------------|
| `--manifest <PATH>` | Manifest to diff against (required) |
| `--root <DIR>` | Directory to compare (required) |
| `-i, --include`, `-e, --exclude`, `--include-exclude-config` | Glob config |
| `--force-rehash` | Hash-based comparison |
| `--json` | JSON output with new/modified/deleted lists |

- Computes differences between manifest and directory
- Without `--force-rehash`: compares by mtime and size (fast)
- With `--force-rehash`: compares by content hash (thorough)
- Outputs three lists: new, modified, deleted files

### `deadline manifest download <DOWNLOAD_DIR>`

| Flag | Description |
|------|-------------|
| `<DOWNLOAD_DIR>` | Positional: where to write manifests |
| `--job-id <ID>` | Job (required) |
| `--step-id <ID>` | Step (includes step dependencies) |
| `--farm-id <ID>` | Farm |
| `--queue-id <ID>` | Queue |
| `--profile <NAME>` | AWS profile |
| `--asset-type <TYPE>` | INPUT, OUTPUT, or ALL (default: ALL) |
| `--json` | JSON output |

- Downloads job attachment manifests from S3
- Resolves queue attachment settings via Deadline API
- For inputs: extracts `inputManifestPath` from job attachments metadata
- For outputs: lists output manifests from S3 prefix, merges per asset root
- If `--step-id`: also fetches step dependencies and their output manifests
- Writes merged manifests to download directory

### `deadline manifest upload <MANIFEST_FILE>`

| Flag | Description |
|------|-------------|
| `<MANIFEST_FILE>` | Positional: manifest to upload |
| `--s3-cas-uri <URI>` | S3 CAS URI |
| `--s3-manifest-prefix <PREFIX>` | S3 key prefix |
| `--farm-id <ID>` | Farm |
| `--queue-id <ID>` | Queue |
| `--profile <NAME>` | AWS profile |
| `--json` | JSON output |

- Reads manifest file from disk
- Uploads to S3 at `{cas_prefix}/Manifests/{prefix}/{filename}`
- Requires `--s3-cas-uri` or farm/queue for settings resolution

---

## `deadline handle-web-url` ✅

| Flag | Description |
|------|-------------|
| `[URL]` | Positional: `deadline://` protocol URL |
| `--install` | Register as `deadline://` URL handler |
| `--uninstall` | Unregister URL handler |
| `--all-users` | System-wide install/uninstall |
| `--prompt-when-complete` | Wait for keypress at end |

- Handles URLs sent from web applications (e.g., Deadline Cloud console)
- URL format: `deadline://<command>?arg=value&arg=value`
- Currently supports: `deadline://download-output?farm-id=...&queue-id=...&job-id=...`
- Validates URL scheme, parses query parameters, validates resource IDs
- Delegates to `_download_job_output` for the download-output command
- `--install`/`--uninstall` registers/removes the OS protocol handler

---

## `deadline mcp-server` ✅

No flags. Starts a stdio-based MCP server for AI agent integration.

- Communicates via stdin/stdout using JSON-RPC (MCP protocol)
- Exposes Deadline Cloud operations as tools for AI agents
- Tools include: list_farms, list_queues, list_jobs, get_job, search_jobs, get_session_logs, submit_job, download_job_output, check_authentication_status
- Long-lived process — handles multiple tool calls per session

---

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | Operation error (API failure, declined confirmation, general error) |
| 2 | Usage error (missing required option) |

`deadline job wait` additional codes:

| Code | Meaning |
|------|---------|
| 2 | Job FAILED |
| 3 | Job CANCELED |
| 4 | Job SUSPENDED or ARCHIVED |
| 5 | Job NOT_COMPATIBLE |
