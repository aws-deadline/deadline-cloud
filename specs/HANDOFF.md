# Handoff

Current in-flight work. Read this at the start of every session before
consulting the Work Items table in `specs/progress.md`.

---

## #27 — Typed SDK API layer

**Status:** In progress — D5d next.

### Design intent (FINAL — no exceptions)

**No raw HTTP interception. No `Value` from API responses. No `ResponseBodyCapture`.**

Every API call uses SDK fluent builders (typed input) and returns SDK
output structs (typed output). Period.

- **Business logic callers** chain typed accessors directly:
  `output.job_attachment_settings().unwrap().s3_bucket_name()`

- **No thin wrapper functions.** Callers call the SDK directly via
  `session::deadline_client(config).await.get_queue()...send().await`.
  The only shared infrastructure is `session::deadline_client()` (session
  caching), `client::deadline_error()` / `client::format_sdk_error()`
  (error formatting), `client::collect_paginated()` (pagination), and
  telemetry (interceptor on the client).

- **Display callers** (CLI `get` commands that print the full response)
  extract every field and subfield manually from the SDK output into a
  serializable response struct. Helper functions convert shared nested
  types (e.g. `HostPropertiesResponse` used by both Session and Worker)
  to avoid duplication.

- **No `Value` intermediary.** The response struct fields that hold
  nested types use `Option<Value>` only because the nested SDK types
  lack `Serialize`. The `Value` is built by walking typed accessors —
  never by intercepting raw HTTP.

- **Helper functions exist only for:** pagination, error handling with
  telemetry, and shared nested type conversion (deduplication).

- **If a new SDK field is added**, we add it to the response struct and
  converter. That's fine — explicit is better than magic.

---

### Current state (exact)

#### ✅ Already typed (return SDK output, no raw):

| Function | Returns |
|----------|---------|
| `get_job` | `GetJobOutput` |
| `get_step` | `GetStepOutput` |
| `get_task` | `GetTaskOutput` |
| `get_session` | `GetSessionOutput` |
| `get_worker` | `GetWorkerOutput` |

#### ⚠️ Typed but ALSO have `_with_raw` variant (MUST REMOVE):

| Function | Why raw is used (will be replaced by typed extraction) |
|----------|-------------------------------------------------------|
| `get_job_with_raw` | `attachments`, `parameters`, `taskRunStatusCounts` |
| `get_step_with_raw` | `taskRunStatusCounts`, `dependencyCounts`, `parameterSpace`, `requiredCapabilities` |
| `get_task_with_raw` | `parameters` |
| `get_session_with_raw` | `log`, `hostProperties`, `workerLog` |
| `get_worker_with_raw` | `hostProperties`, `log` |

#### ❌ Still fully raw (return `Value`, MUST CONVERT):

| Function | Callers access |
|----------|---------------|
| `get_queue` | `jobAttachmentSettings.{s3BucketName, rootPrefix}`, `displayName` |
| `list_jobs` | `jobs[]` array iteration |
| `search_jobs` / `search_jobs_with_filters` | `jobs[]`, `totalResults` |
| `search_workers` | `workers[]`, `totalResults` |
| `list_sessions` | `sessions[]` |
| `list_steps` | `steps[]` |
| `list_tasks` | `tasks[]` |
| `batch_get_steps_page` / `batch_get_tasks_page` | `steps[]`, `tasks[]` |
| `assume_queue_role_for_user/read` | `credentials.{accessKeyId,...}` |
| `assume_fleet_role_for_read` | `credentials.{...}` |
| `get_storage_profile_for_queue` | `displayName`, `fileSystemLocations` |
| `list_storage_profiles_for_queue` | `storageProfiles[]` |
| `get_session_action` | `startedAt`, `endedAt` |
| `list_session_actions` | `sessionActions[]` |
| `list_queue_environments` | `environments[]` |
| `get_queue_environment` | `template`, `templateType` |
| `list_queue_fleet_associations` | `queueFleetAssociations[]` |
| `update_job` / `update_task` | Ignores response |
| `create_job` | `jobId` |

#### ❌ Direct `ResponseBodyCapture` usage outside api.rs (MUST REMOVE):

| File | What it does |
|------|-------------|
| `fleet.rs` | `fleet get` — captures raw for `FleetResponse::from_output_and_raw` |
| `queue.rs` | `queue get` — captures raw for `QueueResponse::from_output_and_raw` |
| `python-bindings/resources.rs` | `get_queue` for FFI |
| `session.rs` | Test helper |

#### Response structs needing conversion from `from_output_and_raw` → `From<Output>`:

| Struct | Nested types needing manual extraction |
|--------|----------------------------------------|
| `FarmResponse` | ✅ Already `From<Output>` — no nested types |
| `QueueResponse` | `JobAttachmentSettings`, `JobRunAsUser`, `SchedulingConfiguration` |
| `FleetResponse` | `FleetConfiguration` (deep), host config, capabilities |
| `JobResponse` | `Attachments`, `JobParameter` map, `TaskRunStatus` map |
| `StepResponse` | `TaskRunStatus` map, `DependencyCounts`, `ParameterSpace`, `RequiredCapabilities` |
| `TaskResponse` | `TaskParameterValue` map |
| `SessionResponse` | `LogConfiguration`, `HostPropertiesResponse` |
| `WorkerResponse` | `LogConfiguration`, `HostPropertiesResponse` |

---

### Implementation plan

#### New module: `type_conversions.rs`

Helper functions that convert nested SDK types → `Value` by walking
typed accessors. Only for types used by 2+ response structs (dedup).
Types used by only one struct can be inlined in the `From` impl.

Shared helpers needed:
- `log_configuration_to_value` — used by SessionResponse + WorkerResponse
- `host_properties_to_value` — used by SessionResponse + WorkerResponse
- `ip_addresses_to_value` — used by host_properties_to_value

Everything else can be inlined or extracted based on readability.

#### Changes to `api.rs`

- Delete `capture_send`, `paginated_list`, `capture_err`
- Delete all `_with_raw` variants
- Convert each remaining function to return SDK output type
- For paginated functions: use SDK paginator or manual typed pagination
- `update_job`/`update_task` → return `()`
- `create_job` → return `CreateJobOutput`

#### Caller migration

All `value["field"]` access → typed accessor chains:
- `queue["jobAttachmentSettings"]["s3BucketName"]` → `output.job_attachment_settings().unwrap().s3_bucket_name()`
- `resp["jobs"].as_array()` → `output.jobs()` (from paginator)
- `resp["credentials"]["accessKeyId"]` → `output.credentials().unwrap().access_key_id()`

### Batching strategy

- **D5a:** ✅ Convert all 7 response structs to `From<Output>` with manual
  nested type extraction. Remove `_with_raw` variants. Fix display callers.
- **D5b:** ✅ Delete `get_queue` + credential API wrappers. Callers call SDK directly.
- **D5c:** ✅ Convert list/search APIs to typed paginators. Delete wrappers. Migrate callers.
  - Deleted 7 wrappers: `list_jobs`, `search_jobs`, `search_jobs_with_filters`,
    `search_workers`, `list_sessions`, `list_steps`, `list_tasks`
  - Replaced `list_sessions`/`list_steps`/`list_tasks` with typed paginator versions
  - Refactored `list_jobs_by_filter_expression` to call SDK directly
  - Made `build_filter_expressions`/`build_sort_expressions` public
  - Migrated 15+ callers across job.rs, worker.rs, helpers.rs, mcp.rs,
    queue.rs, job_monitoring.rs, log_retrieval.rs
  - One accepted difference: task parameter ordering now sorted alphabetically
    (HashMap non-determinism → explicit sort for deterministic output)
- **D5d:** Convert remaining (queue environments, fleet associations,
  storage profiles, session actions, update/create). Delete remaining thin
  get_* wrappers (get_job, get_step, get_task, get_worker, get_session).
- **D5e:** Delete `response_capture.rs`, `paginated_list`, `capture_send`. Clean sweep.

### Step status

**D5a** — ✅ Complete (type_conversions.rs + From<Output> for all 7 structs)

**D5b** — ✅ Complete
- Deleted 4 thin wrappers: `get_queue`, `assume_queue_role_for_user`,
  `assume_queue_role_for_read`, `assume_fleet_role_for_read`
- Migrated 8 callers to direct SDK calls with typed output
- Migrated `session.rs` QueueUserCredentialProvider (removed ResponseBodyCapture)
- Migrated `log_retrieval.rs` fleet role (typed AwsCredentials)
- CLI comparison: identical output (pre-existing error format difference only)

---

## Queued small items (from #21b Bucket 3)

### #21d — CLI backward-compat flags (~30 lines)

- `job logs --timezone` — deprecated flag mapping to `--timestamp-format`
- `queue export-credentials --output-format` — accept `credentials_process`
- `manifest snapshot/diff -ie` — short alias for `--include-exclude-config`
- `auth status --output` — validate `verbose`/`json`

### #21e — `deadlinew` windowless launcher (~7 lines)

`#![windows_subsystem = "windows"]` binary target for GUI commands on Windows.

### #21f — Windows config path normalization (~50 lines)

Normalize `\`↔`/` for path-type config settings on Windows.

### #21g — Telemetry parity: success/fail events (~50 lines)

`asset_upload`, `asset_snapshot`, `queue_sync_output`, `download_job_output`
success/fail telemetry events.
