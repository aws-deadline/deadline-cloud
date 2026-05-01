# deadline-api Architecture

## Crate Position

```
deadline-cli ──► deadline-api    ← this crate
deadline-python-bindings ──► deadline-api
```

All Deadline Cloud API calls are constructed and sent through this
crate — it owns the session cache, credential resolution, telemetry
interceptor, and error mapping. Consumer crates (`deadline-cli`,
`deadline-python-bindings`) import SDK types (`FarmSummary`,
`GetJobInput`, etc.) directly from `aws-sdk-deadline` and use the SDK's
fluent builders at callsite. The helpers in `deadline-api::client`
observe the call (pagination, telemetry via interceptor, error
mapping) — they do not re-declare SDK inputs.

Exception: `deadline-job-attachments` owns its own S3 and STS clients
independently (different service, different credential scoping needs).

## Module Layout

The crate is organized around three main concerns: session management,
authentication, and API call helpers. Supporting modules handle job
monitoring, log retrieval, queue parameters, telemetry, and version
checking.

Callers use the SDK fluent builder directly. A `TelemetryInterceptor`
installed on the client at construction time reads the SDK's own
`Metadata` config-bag entry to emit latency events — no hand-maintained
operation-name table. Session and credential state is managed by
`session.rs`. Authentication detection and DCM login/logout live in
`auth.rs`.

```
src/
├── lib.rs              # Re-exports all public modules
├── client.rs           # collect_paginated, apply_dcm_principal,
│                       #   format_sdk_error, deadline_error,
│                       #   WithPrincipalId, pascal_to_snake
├── telemetry_interceptor.rs  # TelemetryInterceptor + pagination grouping
├── api.rs              # list_jobs_by_filter_expression, build_filter/sort_expressions,
│                       #   batch_get_steps_page/batch_get_tasks_page,
│                       #   create_job, wait_for_create_job_to_complete
│                       #   (NO thin wrappers — callers own SDK calls directly)
├── session.rs          # Session caching, credential resolution, queue-scoped configs
├── auth.rs             # DCM detection, login/logout, auth status checks
├── job_monitoring.rs   # Poll job until terminal state, collect failed task details
├── log_retrieval.rs    # CloudWatch Logs, session auto-selection, fleet credentials
├── queue_parameters.rs # Queue environment parameter extraction
├── errors.rs           # DeadlineError enum
├── submitter_info.rs   # Submitter metadata (name, version, DCC info)
├── path_utils.rs       # File size formatting, path summarization
├── telemetry.rs        # Background telemetry client
├── type_conversions.rs # Nested SDK type → Value helpers (shared by response structs)
├── responses.rs        # Serializable response structs (From<Output> impls)
└── update_checker.rs   # Remote version check, never panics
```

## Key Design Decisions

**Client-level telemetry interceptor.** The `TelemetryInterceptor` is
installed on every `DeadlineClient` at construction time. It reads
`cfg.load::<Metadata>()` in `read_after_execution` to get the SDK's own
operation name (e.g. "GetFarm"), converts to snake_case ("get_farm"),
and records latency. Zero plaintext operation names in our code.

**Callers drive the SDK fluent builder.** No wrapper functions that
re-declare `&str` parameters. Callers write
`client.get_farm().farm_id(id).send()` directly. The helpers
(`collect_paginated`, `apply_dcm_principal`) handle pagination and
error mapping only.

**Typed SDK output everywhere.** All API calls return typed SDK output.
Callers access fields via typed accessors (`.farm_id()`, `.name()`).
Display paths use `From<Output>` response structs that manually extract
all fields into serializable types. Nested SDK types that lack
`Serialize` are converted via `type_conversions.rs` helpers.

**Global session cache.** A process-wide cache holds SDK configs keyed
by profile name, and queue credential configs keyed by farm+queue pair.
This avoids re-creating SDK clients on every API call. The cache is
invalidated on logout or when the profile changes.

**Native SDK paginators for typed list.** Every `list_*` operation in
`aws-sdk-deadline` has `.into_paginator()`. `collect_paginated` drains
the paginator into `Vec<PageOutput>` with one telemetry event covering
all pages (matches Python's one-event-per-outer-call model via a
pagination group flag in the ConfigBag).

**DCM detection by config file parsing.** Rather than making an API call
to determine the credential source, the crate reads `~/.aws/config`
directly and checks for the `monitor_id` key. This is fast and works
offline.

**Login/logout spawns a subprocess.** The DCM binary is a separate
process. Login spawns it with `login` args and polls auth status every
0.5 seconds. Logout spawns with `logout` args and invalidates the
session cache.

## Public API Surface

### Making an API call (new pattern)

Callers use the SDK fluent builder directly. Helpers handle pagination
and error mapping:

```rust
// Typed paginated
use aws_sdk_deadline::operation::list_farms::ListFarmsOutput;
let client = session::deadline_client(Some(&config)).await;
let pages: Vec<ListFarmsOutput> = client::collect_paginated(
    client.list_farms().into_paginator().send()
).await?;
```

### Client helpers (`client.rs`)

| Function | Description |
|----------|-------------|
| `collect_paginated` | Drain SDK paginator into Vec, one telemetry event |
| `apply_dcm_principal` | Set principal_id on list builders for DCM users |
| `format_sdk_error` | Extract error code + message from any `SdkError` |
| `deadline_error` | Map `SdkError` → `DeadlineError::OperationError` |
| `pascal_to_snake` | Convert "GetFarm" → "get_farm" for telemetry |

### Domain functions (`job_api.rs`)

| Function | Description |
|----------|-------------|
| `list_jobs_by_filter_expression` | Paginate all jobs matching a filter (createdAt thresholding, dedup) |
| `wait_for_create_job_to_complete` | Poll until CreateJob completes (backoff, timeout, cancellation) |
| `build_filter_expressions` | JSON → SDK typed filter struct converters |
| `build_sort_expressions` | JSON → SDK typed sort struct converters |
| `build_sdk_attachments` | Build attachments from `Map<String, Value>` |

### Auth functions (`auth.rs`)

| Function | Description |
|----------|-------------|
| `get_credentials_source` | Detect credential source (DCM, host, not valid) |
| `check_authentication_status` | Check if credentials are valid |
| `check_deadline_api_available` | Probe API reachability via `list_farms(max_results=1)` |
| `get_user_and_identity_store_id` | Get DCM user ID and identity store ID |
| `get_monitor_id` | Get DCM monitor ID from AWS config |
| `login` | Spawn DCM login subprocess |
| `logout` | Spawn DCM logout subprocess, invalidate cache |

### Getting a session / SDK config

```rust
let client = deadline_api::session::deadline_client(Some(&config)).await;
```

This returns a cached `DeadlineClient` for the active profile with the
`TelemetryInterceptor` installed. Changing profiles invalidates the cache.

### Queue-scoped credentials

For operations that access S3 or CloudWatch on behalf of a queue:

```rust
let scoped_config = deadline_api::session::get_queue_scoped_config(
    farm_id, queue_id, Some(&config)
).await?;
```

This checks whether the user is logged in via DCM. If so, it assumes
the queue role and returns a scoped `SdkConfig`. If not, it returns the
base config.

### Core types

| Type | Purpose |
|------|---------|
| `DeadlineError` | Error enum for all API, config, auth, and attachment failures |
| `TelemetryClient` | Background telemetry — pass to API functions for latency tracking |
| `AwsCredentialsSource` | Enum: `NotValid`, `HostProvided`, `DeadlineCloudMonitorLogin` |
| `AwsAuthenticationStatus` | Enum: `ConfigurationError`, `Authenticated`, `NeedsLogin` |
| `TelemetryInterceptor` | SDK interceptor for automatic latency recording |
