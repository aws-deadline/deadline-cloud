# bundle Commands

## Subcommands

| Subcommand | Status | Description |
|------------|--------|-------------|
| `bundle submit` | ✅ | Submit a job bundle to a Deadline Cloud queue |
| `bundle gui-submit` | 🔲 | Submit via the GUI dialog (not yet implemented) |

## `bundle submit`

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `job_bundle_dir` (positional) | required | Path to the job bundle directory |
| `-p, --parameter` | — | `Name=Value` overrides (repeatable) |
| `--profile` | config | AWS profile |
| `--farm-id` | config | Farm ID |
| `--queue-id` | config | Queue ID |
| `--storage-profile-id` | config | Storage profile ID |
| `--name` | from template | Override job name |
| `--priority` | 50 | Job priority |
| `--max-failed-tasks-count` | — | Max failed tasks before job fails |
| `--max-retries-per-task` | — | Max retries per task |
| `--max-worker-count` | — | Max worker count |
| `--target-task-run-status` | — | READY or SUSPENDED |
| `--job-attachments-file-system` | — | COPIED or VIRTUAL |
| `--yes` | false | Skip confirmation prompts |
| `--require-paths-exist` | false | Error if any input paths are missing |
| `--submitter-name` | "CLI" | Name of the submitting application |
| `--known-asset-path` | — | Paths that should not generate warnings (repeatable) |
| `--force-s3-check` | config | Force S3 existence verification for every file |
| `--no-force-s3-check` | — | Skip S3 existence verification |

### Parameter Validation

Parameter names must match `[A-Za-z_][A-Za-z0-9_]*` (OpenJD identifier pattern).
Format must be `Name=Value`. Invalid format or name → error with specific message.

### Execution Flow

```
bundle submit <dir> -p Frames=1-100
  │
  ├── Parse -p parameters into [{name, value}] JSON array
  ├── Load config, apply --profile/--farm-id/--queue-id/--yes overrides
  ├── Apply --storage-profile-id separately (not in shared CliOptions)
  ├── Resolve force_s3_check: --force-s3-check > --no-force-s3-check > config
  ├── Create progress bars (hashing + upload, via ProgressBarManager)
  │
  ├── Call create_job_from_job_bundle(SubmitJobParams)
  │   ├── Library sends status messages via print_callback → printed to stdout
  │   │   (e.g., "Uploading attachments...", "Creating job...", job ID)
  │   └── On failure: error propagated as CliError::Operation
  │       (hashing failure, upload failure, CreateJob failure, polling timeout)
  │
  └── On success: update defaults.job_id in config file
      └── Only when no CLI overrides were provided for profile/farm/queue/storage
```

### Progress Reporting

Two progress bars via `ProgressBarManager`:
- "Hashing Attachments" — tracks file hashing progress
- "Uploading Attachments" — tracks S3 upload progress

Both support cancellation via SIGINT (progress callback returns `should_continue()`).

### Config Update After Submission

After successful submission, `defaults.job_id` is updated in the config file
so subsequent `job get`, `job logs`, etc. default to the just-submitted job.
This update is skipped when any of `--profile`, `--farm-id`, `--queue-id`,
or `--storage-profile-id` were provided as CLI flags — the assumption is that
explicit overrides indicate a non-default context that shouldn't persist.

### force_s3_check Resolution

Three-way precedence:
1. `--force-s3-check` flag → true
2. `--no-force-s3-check` flag → false
3. Neither → read from `settings.force_s3_check` in config (handled by library)

The two flags use `overrides_with` in clap so the last one wins if both are specified.

## Asset Path Safety

When job attachments are present, the submission pipeline checks whether
input files are within "known asset paths" — the bundle directory, storage
profile locations, PATH-type parameter values, and paths from
`settings.known_asset_paths` (split on `:` on Unix, `;` on Windows —
the path-list separator, matching `os.pathsep` in Python).

If files exist outside known paths:
- **Interactive mode** (`--yes` not set): warns and prompts for confirmation
- **Auto-accept mode** (`--yes` or `settings.auto_accept`): **cancels** the
  submission with message "Job submission canceled (settings.auto_accept
  enabled and there were unknown paths)." This is a safety measure — auto-accept
  should not silently upload files from unexpected locations.

This matches Python's behavior where `auto_accept=True` + unknown paths raises
`DeadlineOperationCanceled`.

## Differences from Python CLI

| Aspect | Python | Rust |
|--------|--------|------|
| Parameter format | `-p Name=Value` only | `-p Name=Value` only |
| GUI submit | `bundle gui-submit` command | Not yet implemented |
| `--json` output | Supported | Not yet implemented |
| `--save-debug-snapshot` | Supported | Not yet implemented |
| Submitter name default | "deadline-cloud-cli" | "CLI" |
