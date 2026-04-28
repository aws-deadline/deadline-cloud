# CLI Reference

Complete command reference for the `deadline` CLI. Commands marked ✅ are
implemented in Rust. Commands marked 🔲 are not yet implemented (exist in
the legacy CLI only).

## See Also — Per-Command Spec Files

Several command groups have dedicated spec files with full behavioral
documentation. This reference covers commands that don't have their own
file.

| Command Group | Spec File |
|---------------|-----------|
| `deadline config`, `deadline auth` | [config-and-auth.md](config-and-auth.md) |
| `deadline farm`, `deadline fleet`, `deadline worker` | [resource-commands.md](resource-commands.md) |
| `deadline queue` | [queue.md](queue.md) |
| `deadline job` | [job.md](job.md) |
| `deadline bundle` | [bundle.md](bundle.md) |
| `deadline attachment`, `deadline manifest` | [attachments-and-manifests.md](attachments-and-manifests.md) |
| `deadline handle-web-url` | [handle-web-url.md](handle-web-url.md) |
| `deadline mcp-server` | [mcp.md](mcp.md) |

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
## `deadline mcp-server` ✅

No flags. Starts a stdio-based MCP server for AI agent integration.

- Communicates via stdin/stdout using JSON-RPC (MCP protocol)
- Exposes Deadline Cloud operations as tools for AI agents
- Tools include: list_farms, list_queues, list_jobs, get_job, search_jobs, get_session_logs, submit_job, download_job_output, check_authentication_status
- Long-lived process — handles multiple tool calls per session
