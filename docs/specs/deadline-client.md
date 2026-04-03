# deadline-client

AWS API interaction layer. Owns the SDK/HTTP calls to the Deadline Cloud
service and STS.

## Status: In Progress (§3-10)

Session creation, auth status, session caching, user-agent, farm list/get,
queue user credentials implemented. Remaining: queue parameters.

## Consumers

- `deadline-cli` — CLI commands that call Deadline Cloud APIs
- `deadline-worker-agent` — worker agent polling, session management, progress reporting
- `deadline-gui-ffi` — GUI dropdown population, submission

## Dependencies

| Crate | Purpose |
|-------|---------|
| `deadline-config` | Config for endpoint/credential resolution |
| `deadline-models` | Shared types and errors |
| `deadline-common` | `TelemetryClient`, `with_telemetry_latency`, `with_telemetry_latency_async`, `create_telemetry` for API-layer latency events |

External:
- `aws-sdk-deadline` — Deadline Cloud service SDK
- `aws-sdk-sts` — STS GetCallerIdentity for auth checks
- `aws-config` — SDK credential/region resolution

## Module Layout

```
src/
├── lib.rs       // pub mod session, auth, api
├── session.rs   // SDK config loading, client construction
├── auth.rs      // credential source, auth status, API availability
└── api.rs       // list/get for farms, queues, fleets, jobs
```

## Session (`session.rs`)

`SessionCache` struct owns cached `SdkConfig` and user-agent context.
Replaces Python's module-level `@lru_cache` functions and `session_context`
global dict (Design Principles 1 and 2: structs over globals).

### `SessionContext`

Tracks caller identity for the User-Agent header on all AWS API calls.
Python uses a module-level `session_context` dict; Rust owns it on the
struct.

- `submitter_name: Option<String>` — set by GUI/DCC plugins (e.g. "Blender")
- `submitter_version: Option<String>` — set alongside submitter_name
- `cli_command_name: Option<String>` — set by CLI before each command
- `build_user_agent() -> String` — produces
  `app/deadline-client#<version> submitter/<name>#<ver> cli-command/<cmd>`

Applied via `user_agent_extra` on the service config builder.

### `SessionCache`

Caches one `SdkConfig` for the current profile. Building service clients
from a cached `SdkConfig` is cheap (no credential re-resolution), so
per-service client caching is unnecessary.

- `new() -> Self` — empty cache
- `get_config(config) -> &SdkConfig` — loads once, returns cached
- `deadline_client(config) -> DeadlineClient` — from cached config + user-agent
- `sts_client(config) -> StsClient` — from cached config
- `invalidate()` — clears cached config; next call re-resolves credentials
- `display_profile_name(config) -> String` — returns profile name for display

### Free functions (kept for backward compatibility during migration)

- `resolve_profile_name(config)` — public, used by `auth.rs` for DCM detection

Profile resolution: `"(default)"`, `"default"`, and `""` all map to the
default credential chain (no named profile).

Endpoint override: reads `AWS_ENDPOINT_URL_DEADLINE` (for Deadline) and
`AWS_ENDPOINT_URL_STS` (for STS) and passes them to the service-specific
config builder via `endpoint_url()`.

### Deadline SDK Host Prefix

The Deadline SDK prepends `management.` or `scheduling.` to the endpoint
hostname (Smithy `@endpoint(hostPrefix)`). This is SDK-internal behavior
we don't control. In tests, we set `endpoint_url` to `http://localhost:PORT`
so the SDK connects to `http://management.localhost:PORT`, which resolves
to `127.0.0.1` via the `.localhost` TLD (RFC 6761).

## Auth (`auth.rs`)

`AwsCredentialsSource` enum: `NotValid`, `HostProvided`,
`DeadlineCloudMonitorLogin`. Determined by parsing `~/.aws/config`
(or `AWS_CONFIG_FILE`) for the active profile's `monitor_id` key.
DCM-created profiles have `monitor_id`, `user_id`, and
`identity_store_id` in their `[profile <name>]` section.

`get_user_and_identity_store_id(config)`: returns `(Option<String>,
Option<String>)` — the `user_id` and `identity_store_id` from the
AWS profile if it's a DCM profile, otherwise `(None, None)`.

`AwsAuthenticationStatus` enum: `ConfigurationError`, `Authenticated`,
`NeedsLogin`. Determined by calling STS GetCallerIdentity.

`check_deadline_api_available`: calls ListFarms with `maxResults=1`.
Returns bool.

## API (`api.rs`)

All API functions use the `ResponseBodyCapture` interceptor
(`raw_response.rs`) to capture the raw HTTP response body as
`serde_json::Value`. This is the single, consistent approach for every
API call — `get_*`, `list_*`, and `search_*` alike. It matches
Python/boto3 behavior where responses are raw dicts, and scales
automatically when the API adds new fields. The interceptor
post-processes datetime strings and removes null values.

### Limitation: Extra fields vs boto3

The raw HTTP response includes fields that boto3 strips (e.g. `arn`,
`schedulingMode`). This happens because boto3 filters responses through
its Smithy service model, only keeping fields defined in the operation's
output shape. The Rust SDK output types would provide the same filtering,
but they don't implement `serde::Serialize`
([awslabs/aws-sdk-rust#269](https://github.com/awslabs/aws-sdk-rust/issues/269),
open since Oct 2021), so we can't serialize them back to JSON/YAML.

The extra fields are harmless (more data than Python, never less), and
field ordering differences are cosmetic. See `../designs/rust-rewrite/workflow.md` § "Known
differences from Python/boto3" for the full list.

**Future option: Smithy model filtering.** The Smithy model JSON for
each AWS service is bundled in the SDK crate source. A post-processing
step could load the operation's output shape, build an allowlist of
field names (recursively for nested types), and filter the captured JSON
to match boto3's output exactly. This would be fully automated — new
fields would only appear after the SDK updates its model. See the
"Future improvements" section below.

List functions use manual `nextToken` loops with `ResponseBodyCapture`
on each page (SDK paginators don't support `.customize().interceptor()`).

See `../designs/rust-rewrite/workflow.md` § "AWS SDK for Rust Usage"
for the full rationale and patterns.

Functions:
- `list_farms`, `list_queues`, `list_fleets` — paginated via manual
  `nextToken` loop with `ResponseBodyCapture` per page
- `get_farm`, `get_queue`, `get_fleet`, `get_job` — single resource, all fields
- `search_jobs(farm_id, queue_ids, item_offset, page_size, config)` — calls
  `SearchJobs` (POST), sorted by `CREATED_AT` descending. Returns
  `{"jobs": [...], "totalResults": N}`. Each job contains raw API fields.
  Replaces `list_jobs` (Python CLI uses `SearchJobs`, not `ListJobs`).
- `search_workers(farm_id, fleet_ids, item_offset, page_size, config)` — calls
  `SearchWorkers` (POST). Returns `{"workers": [...], "totalResults": N}`.
- `get_worker(farm_id, fleet_id, worker_id, config)` — single worker, all fields.
- `list_storage_profiles_for_queue(farm_id, queue_id, config)` — paginated,
  returns `{"storageProfiles": [...]}`. Does NOT inject `principalId`.

## Auth (`auth.rs`) — login/logout (§6)

`login(on_pending_authorization, on_cancellation_check, config, telemetry)`:
checks credential source is DCM, reads `deadline-cloud-monitor.path`
from config, spawns the monitor process with
`["login", "--profile", profile_name]`. Calls `on_pending_authorization`
(if provided) with the credential source after spawning. Polls
`check_authentication_status` in a 0.5s loop. Each iteration checks
`on_cancellation_check` (if provided) — if it returns true, kills the
child process and returns an error. Returns success message or error.

`logout(config, telemetry)`: checks credential source is DCM, runs the
monitor with `["logout", "--profile", profile_name]` via
`Command::output()`, returns stdout. Calls
`invalidate_session_cache()` after successful logout to clear cached
credentials. Errors if not DCM profile or monitor not found.

Both functions only support DCM-created profiles (those with
`monitor_id` in the AWS config profile section).

## Auth (`auth.rs`) — additional functions

`get_monitor_id(config)`: returns `Option<String>` — the `monitor_id`
from the AWS profile if it's a DCM profile, otherwise `None`.

## API (`api.rs`) — paginated list helper

All paginated list functions use a shared `paginated_list` helper that
eliminates the duplicated `nextToken` loop. The helper takes a closure
that builds each page request and the JSON key containing the items
array. This replaces the copy-pasted loops in `list_farms`,
`list_queues`, `list_fleets`, `list_jobs`.

## API (`api.rs`) — queue credentials (§9)

`assume_queue_role_for_user(farm_id, queue_id, config)` and
`assume_queue_role_for_read(farm_id, queue_id, config)`: thin wrappers
using `ResponseBodyCapture`. Return the full API response as
`serde_json::Value`.

### DateTime format for credential export

The `export-credentials` CLI command outputs JSON consumed by the AWS
SDK's `credential_process` feature. The `Expiration` field must be an
RFC 3339 timestamp (requires `T` separator between date and time). The
`ResponseBodyCapture` interceptor converts datetimes to Python display
format (space separator: `2024-12-18 01:00:00+00:00`), so the CLI
converts back to RFC 3339 (`2024-12-18T01:00:00+00:00`) for this
specific output path. See
https://docs.aws.amazon.com/sdkref/latest/guide/feature-process-credentials.html

## API (`api.rs`) — storage profile (§10)

`get_storage_profile_for_queue(farm_id, queue_id, storage_profile_id, config)`:
single `ResponseBodyCapture` call. Returns the full API response as
`serde_json::Value`.

## API (`api.rs`) — diagnostics (§13)

`get_session`, `list_sessions`, `list_steps`, `list_tasks`: follow
existing patterns. List functions use the paginated helper.

## Session (`session.rs`) — queue user credentials (§5)

`QueueUserCredentialProvider` implements the AWS SDK's `ProvideCredentials`
trait. It calls `AssumeQueueRoleForUser` via the Deadline SDK and returns
temporary credentials with an expiry time. The SDK automatically calls
`provide_credentials()` when credentials expire, providing the same
auto-refresh behavior as Python's `RefreshableCredentials`.

Error handling in `provide_credentials()` inspects the AWS error code:
- `ThrottlingException` → "Throttled..." with retry guidance
- `InternalServerException` → "An internal server error occurred..."
- Other errors → "Failed to assume Queue role..." with admin contact guidance
- Empty/missing credentials → "Empty credentials received"

If `queue_display_name` is provided, error messages use it; otherwise
they fall back to `queue_id`.

`get_queue_user_config(farm_id?, queue_id?, queue_display_name?,
force_refresh?, config?) -> SdkConfig`: builds an `SdkConfig` with
`QueueUserCredentialProvider` as the credential source. Falls back to
config defaults for `farm_id` and `queue_id`. Inherits region from the
base session. Cached on `SessionCache` keyed by `(farm_id, queue_id)`.
`force_refresh` clears the base session cache (causing a new base config
to be loaded), and since the queue config cache keys against the base
config identity, a new queue config is also created.

`invalidate_session_cache()` clears both the base session cache and all
queue user config entries, matching Python's `invalidate_boto3_session_cache()`.

`precache_clients(deadline_client?, config?, farm_id?, queue_id?,
queue_display_name?)`: creates a deadline client (or uses provided one),
reads farm/queue from settings if not provided, calls `GetQueue` for
display name if not provided, then calls `get_queue_user_config()` to
trigger credential resolution and caching. Returns `(DeadlineClient, SdkConfig)`.
The S3 client return is deferred to work item #8 (`deadline-job-attachments`
AWS client infrastructure) — revisit when implementing that item.

## Not Yet Implemented

- Queue parameters (§8) — blocked on `deadline-job-bundle` §16

### Telemetry — API-layer latency events

Every public API function in `api.rs` and `login`/`logout` in `auth.rs`
accept an optional `telemetry: Option<&TelemetryClient>` parameter. If
`None`, the function creates an ephemeral `TelemetryClient` internally
(initialized from `AWS_ENDPOINT_URL_DEADLINE`). If `Some`, it reuses the
caller's client (for long-lived processes like the worker agent that
want a single background thread).

Each function records a `com.amazon.rum.deadline.latency` event with
`{latency: <nanoseconds>, function_call: "<function_name>"}`, matching
Python's `@record_function_latency_telemetry_event()` decorator.

Telemetry is best-effort fire-and-forget. The `TelemetryClient` silently
swallows all errors (network failures, HTTP errors, full queue). A
telemetry failure never affects the API call result or CLI exit code.

The telemetry helpers live in `deadline-common::telemetry`:
- `with_telemetry_latency_async` — used by async API functions in `api.rs`
- `with_telemetry_latency` — used by sync functions in `auth.rs`
  (`login`/`logout`)
- `create_telemetry` — used by `queue.rs` export-credentials for its
  CLI-level success/fail event (not a latency event)
- `record_latency` — low-level primitive for callers managing timing
  themselves

Functions with latency telemetry:
- `list_farms`, `list_queues`, `list_fleets`, `list_jobs`
- `get_farm`, `get_queue`, `get_fleet`, `get_job`
- `search_jobs`, `search_workers`, `get_worker`
- `get_session`, `list_sessions`, `list_steps`, `list_tasks`
- `get_storage_profile_for_queue`
- `assume_queue_role_for_user`, `assume_queue_role_for_read`
- `login`, `logout`

CLI-level success/fail events (separate from API-layer latency):
- `queue export-credentials` — records
  `com.amazon.rum.deadline.queue_export_credentials` with `is_success`,
  `duration_ms`, `mode`, `queue_id`, `error_type`. This stays in the
  CLI layer (`queue.rs`) because it includes CLI-specific context.

Telemetry for blocked features (`get_queue_parameter_definitions` §8,
`create_job_from_job_bundle` §11, job monitoring §12) will follow the
same pattern when those API functions are implemented — add the
`telemetry` parameter and record latency internally.

## Deferred

- §11 (submit job bundle) — blocked on `deadline-job-bundle` and
  `deadline-job-attachments`
- §12 (job monitoring & logs) — depends on CloudWatch Logs SDK +
  queue/fleet role credential flows from §5/§9
- §34 (AWS client helpers) — part of `deadline-job-attachments`

## Future Improvements

- **Smithy model response filtering** — Post-process `ResponseBodyCapture`
  JSON through the Smithy service model to strip fields that boto3 would
  not include, and reorder fields to match boto3's Smithy-defined ordering.
  This would eliminate the field-order and extra-field differences from
  Python without requiring manual typed extraction. Low priority — the
  extra fields are harmless and the ordering is cosmetic.
