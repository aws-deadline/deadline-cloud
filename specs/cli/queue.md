# queue Commands

## Subcommands

| Subcommand | Status | Description |
|------------|--------|-------------|
| `queue list` | ✅ | List queues in a farm |
| `queue get` | ✅ | Get details of a specific queue |
| `queue export-credentials` | ✅ | Export queue credentials for AWS CLI use |
| `queue get-storage-profile` | ✅ | Get a storage profile for a queue |
| `queue paramdefs` | ✅ | List queue parameter definitions from queue environments |

All accept `--profile`, `--farm-id`. Most also accept `--queue-id`.

## `queue list`

Requires: farm_id. Calls `ListQueues`. Output shows queueId and displayName
for each queue via `cli_object_repr` (YAML list). On error,
`suggest_resources_on_client_error` lists available farms.

## `queue get`

Requires: farm_id, queue_id. Calls `GetQueue`. Full response via `cli_object_repr`.
On error, `suggest_resources_on_client_error` lists available queues.

## `queue export-credentials`

Requires: farm_id, queue_id. Options: `--mode USER|READ` (default USER).

Calls `AssumeQueueRoleForUser` (USER mode) or `AssumeQueueRoleForRead` (READ mode).
Invalid mode values silently fall back to USER mode (the `_ =>` match arm).

Output is JSON matching the AWS credential_process format:

```json
{
  "Version": 1,
  "AccessKeyId": "...",
  "SecretAccessKey": "...",
  "SessionToken": "...",
  "Expiration": "2024-01-15T10:30:00+00:00"
}
```

The `Expiration` field converts the space-separated datetime from the API response
to ISO 8601 format (replaces first space with `T`).

### Telemetry

Records a `com.amazon.rum.deadline.queue_export_credentials` event on both
success and failure. Event details include: `mode`, `queue_id`, `duration_ms`,
`is_success`, and `error_type` (on failure). The telemetry client is created
fresh for this command (not reused from the session cache).

## `queue get-storage-profile`

Requires: farm_id, queue_id, `--storage-profile-id`. Calls
`GetStorageProfileForQueue`. Full response via `cli_object_repr`.

## `queue paramdefs`

Requires: farm_id, queue_id. Calls `get_queue_parameter_definitions` from
`deadline-api`, which:
1. Lists all queue environments
2. Fetches each environment's full details
3. Sorts by priority
4. Parses YAML templates and extracts parameterDefinitions
5. Deduplicates by name (later environments override earlier)

Output is the merged parameter definitions list via `cli_object_repr`.
On error, `suggest_resources_on_client_error` lists available queues.

## `queue sync-output`

Incrementally downloads job output attachments for all jobs in a queue.
Uses a checkpoint file to track download progress across invocations.

### Job Discovery

Uses `list_jobs_by_filter_expression` (in `deadline-api`) to paginate
through all matching jobs via `createdAt` thresholding. Two queries:

1. **Active jobs**: filter by `TASK_RUN_STATUS` in `[READY, ASSIGNED,
   STARTING, SCHEDULED, RUNNING]`, then filter to jobs with
   `SUCCEEDED > 0` in `taskRunStatusCounts`.
2. **Recently ended jobs**: filter by `ENDED_AT >= checkpoint timestamp`,
   then filter to `SUCCEEDED > 0`.

The pagination algorithm sorts by `CREATED_AT ASC` and uses the last
job's `createdAt` as a `GREATER_THAN_EQUAL_TO` threshold for the next
page. Jobs are deduped by `jobId`. This replaces the previous approach
of a single `search_jobs_with_filters(..., 0, 100)` call that silently
dropped jobs beyond 100.

### Manifest Merge Order

Output manifests are downloaded from S3 with their `LastModified`
timestamps preserved. Manifests for the same asset root are sorted by
`LastModified` (oldest first) before merging, so newer files overwrite
older ones. This matches Python's `_merge_asset_manifests_sorted_asc_by_last_modified`.
