# attachment Commands (BETA)

## Subcommands

| Subcommand | Status | Description |
|------------|--------|-------------|
| `attachment download` | ✅ | Download job attachment data files using manifests |
| `attachment upload` | ✅ | Upload job attachment data files using manifests |

## Credential Resolution (`resolve_s3_context`)

Both commands share a credential resolution pattern:

- **With `--profile`:** Use profile credentials directly. Requires `--s3-root-uri`
  to be provided explicitly. If missing → error: "No valid s3 root path available".
- **Without `--profile`:** Derive S3 root URI from the queue's `jobAttachmentSettings`:
  1. Call GetQueue API
  2. Extract `s3BucketName` and `rootPrefix` from `jobAttachmentSettings`
  3. Build URI: `s3://{bucket}/{prefix}`
  4. If queue has no attachment settings → error: "Queue {queue_id} has no attachment settings"
  5. Get queue-scoped credentials via `get_queue_user_config` (unconditional —
     always assumes queue role, matching Python's `get_queue_user_boto3_session`)

This means attachment commands always use queue-scoped credentials when no explicit
profile is provided, regardless of whether the user is logged in via DCM.

## `attachment download`

Options:
- `-m, --manifests` (required, multiple) — manifest file paths
- `--s3-root-uri` — S3 root URI (required with --profile, derived from queue otherwise)
- `--path-mapping-rules` — path mapping rules JSON
- `--farm-id`, `--queue-id`, `--profile`
- `--conflict-resolution` — SKIP, OVERWRITE, or CREATE_COPY
- `--json` — output download stats as JSON instead of human-readable

Conflict resolution defaults to `settings.conflict_resolution` from config.
If config value is not SKIP or OVERWRITE, defaults to CREATE_COPY.

Delegates to `attachment_download()` in `deadline-job-attachments`. On success,
prints download statistics (file count, bytes transferred, elapsed time) either
as human-readable text or JSON depending on `--json`.

## `attachment upload`

Options:
- `-m, --manifests` (required, multiple) — manifest file paths
- `-r, --root-dirs` (multiple) — root directories for the manifests
- `--s3-root-uri` — S3 root URI
- `--upload-manifest-path` — where to write the uploaded manifest
- `--path-mapping-rules` — path mapping rules JSON
- `--farm-id`, `--queue-id`, `--profile`
- `--json` — accepted but currently unused (no JSON output implemented)

Delegates to `attachment_upload()` in `deadline-job-attachments`. No explicit
success message is printed — the library handles output via its progress callbacks.

## Shared Behavior

Both commands:
1. Build an S3 client via `s3::build_s3_client` with the resolved `SdkConfig`
2. Get the account ID via `s3::get_account_id` (STS GetCallerIdentity) for
   `ExpectedBucketOwner` on all S3 calls
3. Are async — `run()` creates a tokio runtime and blocks on the async implementation
