# Incremental Download State

Checkpoint persistence for `queue sync-output`. Tracks per-job download
progress so subsequent runs only download new outputs.

## Data Model

### `IncrementalDownloadJob`

Represents a single job in the checkpoint. Fields:
- `job` — opaque job dict (`serde_json::Value`) as returned by SearchJobs
- `session_ended_timestamp` — largest `endedAt` of a downloaded session (optional)
- `session_completed_indexes` — map from session ID to latest downloaded action index

### `IncrementalDownloadState`

Full checkpoint. Fields:
- `local_storage_profile_id` — host's storage profile, or None for `--ignore-storage-profiles`
- `downloads_started_timestamp` — when the state was bootstrapped
- `downloads_completed_timestamp` — up to which downloads are complete (defaults to started)
- `eventual_consistency_max_seconds` — SearchJobs overlap window (default 120)
- `jobs` — list of `IncrementalDownloadJob`

## Serialization

Uses `#[derive(Serialize, Deserialize)]` with `#[serde(rename_all = "camelCase")]`
per the owned-file-format pattern in `specs/patterns.md`. This is a file format
we control, not an API response, so serde derives are appropriate.

JSON field names match the Python implementation's dict keys (camelCase).
Optional fields are omitted when None/empty via `skip_serializing_if`.
DateTime fields use chrono's RFC 3339 serialization (`Z` suffix for UTC),
which is compatible with Python's `datetime.fromisoformat()`.

## File Persistence

- `save_file` — atomic write via `tempfile::NamedTempFile` + `persist()` (rename)
- `from_file` — `fs::read_to_string` + `serde_json::from_str`
- Parent directories created automatically on save

## Differences from Python

- Python uses manual `from_dict`/`to_dict` with runtime field validation.
  Rust uses serde derives for compile-time safety and automatic error messages.
- Python serializes UTC as `+00:00` suffix; Rust uses `Z`. Both are valid
  ISO 8601 and interoperable.
- Python's `_datetimes_to_str` recursively converts boto3 datetime objects.
  Not needed in Rust since API responses arrive as strings via `ResponseBodyCapture`.

## Manifest S3 Download Pipeline

Functions for the incremental download flow that compose with the existing
`download.rs` engine.

### `add_output_manifests_from_s3`

Matches S3 manifest keys to session actions by:
1. Extracting session action ID from key via regex (`sessionaction-{id}-{index}/`)
2. Hashing each job manifest's `fileSystemLocationName + rootPath` with xxh128
3. Finding which root path hash appears in the S3 key
4. Setting the `outputManifestPath` on the matching session action's manifests array

Skips session actions that already have a `manifests` field. Returns early
if the job has no attachments or no keys to process.

### `make_manifest_paths_absolute`

Converts relative manifest paths to absolute by joining with the root path:
- POSIX source: joins with `/`
- Windows source: joins with `\`

If a `PathMappingRuleApplier` is provided, applies `strict_transform` to
each absolute path. Paths that fail mapping are removed from the manifest
and recorded in `output_unmapped_paths`.

### `merge_absolute_path_manifest_list`

Merges manifests ordered by last-modified timestamp. Sorts by timestamp
first (earlier processed first), then inserts paths into a HashMap keyed
by lowercased path. Later entries overwrite earlier ones, so the most
recent version of each file wins. Case-insensitive dedup matches Python's
`os.path.normcase` behavior.

### Download orchestration

The actual file download reuses `download::download_file` from `download.rs`.
The incremental pipeline passes absolute paths (manifest paths are already
joined with root and mapped), so `download_file` receives them via its
existing interface. No fork of the download engine.
