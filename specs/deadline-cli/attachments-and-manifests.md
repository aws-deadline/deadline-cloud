# Attachment and Manifest Commands (BETA)

Low-level commands for working with job attachment data files and manifests.
Most users interact with attachments through `bundle submit` — these commands
are for advanced workflows and debugging.
## manifest Commands

| Subcommand | Status | Description |
|------------|--------|-------------|
| `manifest snapshot` | ✅ | Create a manifest snapshot of files in a directory |
| `manifest diff` | ✅ | Compute file differences against a manifest |
| `manifest download` | ✅ | Download job attachment manifests from S3 |
| `manifest upload` | ✅ | Upload a manifest and its files to S3 CAS |

### `manifest snapshot`

Options:
- `--root` (required) — directory to snapshot
- `-d, --destination` — output directory for manifest file (defaults to root)
- `-n, --name` — manifest filename
- `-i, --include` / `-e, --exclude` — glob patterns (repeatable)
- `--include-exclude-config` — path to glob config file
- `--diff` — path to a previous manifest for incremental diff
- `--force-rehash` — skip hash cache, rehash all files
- `--json` — output snapshot metadata as JSON

Output on success: "Manifest generated at {path}".

### `manifest diff`

Options:
- `--manifest` (required) — path to the baseline manifest file
- `--root` — root directory to compare against
- `-i, --include`, `-e, --exclude`, `--include-exclude-config` — glob patterns
- `--force-rehash`, `--json`

**Human-readable output:**
```
New files:
  + new_file.txt
Modified files:
  M changed_file.txt
Deleted files:
  - removed_file.txt
```

Or "No differences found." if clean.

### `manifest download`

Options: `download_dir` (positional), `--job-id` (required), `--step-id`,
`--farm-id`, `--queue-id`, `--profile`, `--asset-type` (input/output/all,
default all), `--json`.

Downloads manifest files from S3 for a given job. Filters by step and asset
type. Uses queue-scoped credentials.

### `manifest upload`

Options:
- `manifest_file` (positional) — path to the manifest file
- `--s3-cas-uri` — S3 CAS URI (or derived from queue's jobAttachmentSettings)
- `--s3-manifest-prefix` — prefix for the manifest in S3
- `--farm-id`, `--queue-id`, `--profile`

When `--s3-cas-uri` is not provided, derives S3 settings from the queue and
uses queue-scoped credentials.

### Sync vs Async

`snapshot` and `diff` are synchronous (filesystem-only). `download` and
`upload` are async (S3 calls). The `run()` function dispatches accordingly.
