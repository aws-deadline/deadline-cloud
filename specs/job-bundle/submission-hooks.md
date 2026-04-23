# Submission Hooks

Pre/post-submission hook framework. External scripts run during
`bundle submit`: pre-hooks can modify the submission payload,
post-hooks run after job creation (failures only warn).

## Hook Sources

Two independent sources, each gated by its own config setting:

| Source | Config Setting | Default |
|--------|---------------|---------|
| Bundle (`hooks.yaml` in job bundle dir) | `settings.allow_bundle_hooks` | `false` |
| Environment (`DEADLINE_HOOKS_DIR` env var) | `settings.allow_environment_hooks` | `false` |

When both are active, environment hooks run first, then bundle hooks.

## Hook Configuration Format

`hooks.yaml` or `hooks.json` in the bundle or env hooks directory:

```yaml
version: "1.0"
preSubmission:
  - command: python3
    args: [validate.py]
    timeout: 30
    env:
      VALIDATION_LEVEL: strict
postSubmission:
  - command: python3
    args: [notify.py]
```

Fields: `command` (required string), `args` (optional list, default `[]`),
`timeout` (optional positive integer, default `60`), `env` (optional map,
default `{}`).

## Execution Model

Hooks are subprocesses. Each receives:
- **stdin**: `HookMetadata` as JSON (job name, farm/queue IDs, parameters,
  asset references, submission payload)
- **env vars**: `DEADLINE_JOB_NAME`, `DEADLINE_PRIORITY`, `DEADLINE_FARM_ID`,
  `DEADLINE_QUEUE_ID`, `DEADLINE_JOB_BUNDLE_DIR`, plus optional
  `DEADLINE_STORAGE_PROFILE_ID` and `DEADLINE_JOB_ID` (post-hooks only)
- **custom env**: from the hook's `env` field

### Pre-submission hooks
- Run sequentially before attachment hashing/upload
- stdout JSON is validated and merged into the submission payload
- Asset references in hook output are added to the upload set
- Non-zero exit or timeout → submission canceled (error)
- Empty stdout → payload unchanged

### Post-submission hooks
- Run sequentially after successful CreateJob
- Failures only log warnings, never block
- `job_id` available in metadata

## Path Resolution

1. Absolute paths → used as-is
2. Relative paths → resolved from script resolve dir
3. Command names → searched in system PATH

The script resolve dir is normally the bundle directory, but
`.hooks_origin` file (written by GUI for job history bundles) redirects
resolution to the original bundle location.

## Confirmation Prompt

When hooks are present and `auto_accept` is false, a confirmation
message lists all hooks and their commands. Without an interactive
callback, submission is canceled.

## Payload Merging

Hook output is shallow-merged into the submission payload. Special
handling for `attachments.assetReferences`: each nested key
(`inputFilenames`, `inputDirectories`, etc.) is replaced independently.
Other attachment fields are preserved.

## Differences from Python

| Aspect | Python | Rust |
|--------|--------|------|
| Hook execution messages | stdout via `print_function_callback` | stderr via `eprintln` |
| Confirmation bundle path | `script_resolve_dir` (from `.hooks_origin`) | `job_bundle_dir` |

Both are accepted differences. The stderr convention is consistent with
how the Rust CLI handles other status messages. The bundle path difference
only matters for the GUI job history case (deferred to #16d).
