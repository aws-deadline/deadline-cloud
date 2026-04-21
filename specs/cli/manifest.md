# manifest Commands (BETA)

## Subcommands

| Subcommand | Status | Description |
|------------|--------|-------------|
| `manifest snapshot` | ✅ | Create a manifest snapshot of files in a directory |
| `manifest diff` | ✅ | Compute file differences against a manifest |
| `manifest download` | 🔲 | Download job attachment manifests (stub — not yet wired) |
| `manifest upload` | ✅ | Upload a manifest and its files to S3 CAS |

## Sync vs Async

`snapshot` and `diff` are synchronous (filesystem-only operations). `download`
and `upload` are async (S3 calls). The `run()` function dispatches accordingly —
sync commands call `run_sync()`, async commands create a tokio runtime.

## `manifest snapshot`

Options:
- `--root` (required) — directory to snapshot
- `-d, --destination` — output directory for manifest file (defaults to root,
  prints "Manifest creation path defaulted to {root}" when defaulting)
- `-n, --name` — manifest filename
- `-i, --include` — glob include patterns (repeatable)
- `-e, --exclude` — glob exclude patterns (repeatable)
- `--include-exclude-config` — path to glob config file
- `--diff` — path to a previous manifest. When provided, the snapshot computes
  an incremental diff against the previous manifest instead of a full snapshot.
  Only new and modified files are included in the output manifest.
- `--force-rehash` — skip hash cache, rehash all files even if mtime unchanged
- `--json` — output snapshot metadata as JSON (in addition to the manifest file)

Validates that `--root` and `--destination` directories exist (error if not).
Resolves glob config from include/exclude args and optional config file via
`resolve_glob_config()`.

Output on success: "Manifest generated at {path}". With `--json`, also prints
the snapshot metadata as JSON.

## `manifest diff`

Options:
- `--manifest` (required) — path to the baseline manifest file
- `--root` — root directory to compare against
- `-i, --include`, `-e, --exclude`, `--include-exclude-config` — glob patterns
- `--force-rehash`, `--json`

Validates that manifest file and root directory exist (error if not).

**Human-readable output:**
```
Manifest Diff of root directory: /path
New files:
  + new_file.txt
Modified files:
  M changed_file.txt
Deleted files:
  - removed_file.txt
```

Or "No differences found." if clean.

**JSON output:** Pretty-printed JSON with `new`, `modified`, `deleted` arrays.

## `manifest download`

Options: `download_dir` (positional), `--job-id` (required), `--step-id`,
`--farm-id`, `--queue-id`, `--profile`, `--asset-type` (input/output/all,
default all), `--json`.

Requires: farm_id, queue_id.

### Execution Flow

1. GetQueue to retrieve `jobAttachmentSettings` (bucket, prefix)
2. GetJob to check for attachments and manifest entries
3. Get queue-scoped credentials via `get_queue_scoped_config`
4. For each manifest entry in the job's attachments:
   - Filter by `--step-id` if provided
   - Download manifest from S3 via GetObject
   - Write to `download_dir`
5. Print download count

## `manifest upload`

Options:
- `manifest_file` (positional) — path to the manifest file
- `--s3-cas-uri` — S3 CAS URI. If provided, uses it directly.
- `--s3-manifest-prefix` — prefix for the manifest in S3
- `--farm-id`, `--queue-id`, `--profile` — used to derive S3 settings
  from the queue's `jobAttachmentSettings` when `--s3-cas-uri` is not given.
  Uses queue-scoped credentials.
- `--json` — accepted but currently unused (no JSON output implemented)

When `--s3-cas-uri` is not provided, requires `--farm-id` and `--queue-id`
(from flags or config). Calls GetQueue to get `jobAttachmentSettings`,
then uses queue-scoped credentials for the S3 upload.

Validates manifest file exists (error if not).

Output:
```
Uploading Manifest to {bucket} {prefix} Manifests, prefix: {manifest_prefix}
Uploading successful!
```
