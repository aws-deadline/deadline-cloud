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
