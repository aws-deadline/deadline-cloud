# deadline-client

AWS API interaction layer. Owns the SDK/HTTP calls to the Deadline Cloud
service and STS.

## Status: In Progress (§3-10)

Session creation, auth status, farm list/get implemented. Remaining: DCM
credential source, queue user credentials, login/logout, remaining list
APIs, queue parameters, queue credentials, storage profile.

## Consumers

- `deadline-cli` — CLI commands that call Deadline Cloud APIs
- `deadline-worker-agent` — worker agent polling, session management, progress reporting
- `deadline-gui-ffi` — GUI dropdown population, submission

## Dependencies

| Crate | Purpose |
|-------|---------|
| `deadline-config` | Config for endpoint/credential resolution |
| `deadline-models` | Shared types and errors |

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

Free functions that construct AWS SDK clients. No struct — the SDK clients
are stateless and cheap to construct per-call. Caching may be added later
if profiling shows it's needed (Design Principle 3: earn its keep).

- `deadline_client(config)` — builds `aws_sdk_deadline::Client`
- `sts_client(config)` — builds `aws_sdk_sts::Client`
- `display_profile_name(config)` — returns the profile name for display

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
field ordering differences are cosmetic. See `workflow.md` § "Known
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

See `docs/designs/rust-rewrite/workflow.md` § "AWS SDK for Rust Usage"
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

`login(on_cancellation_check, config)`: checks credential source is DCM,
reads `deadline-cloud-monitor.path` from config, spawns the monitor
process with `["login", "--profile", profile_name]`, polls
`check_authentication_status` in a 0.5s loop until authenticated or
process exits. Returns success message or error.

`logout(config)`: checks credential source is DCM, runs the monitor
with `["logout", "--profile", profile_name]` via `Command::output()`,
returns stdout. Errors if not DCM profile or monitor not found.

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

## Not Yet Implemented

- Session caching and user-agent construction (§3 cases 5-20)
- Queue user credentials — custom credential provider (§5)
- Login/logout (§6)
- Queue parameters (§8)

## Deferred

- §11 (submit job bundle) — blocked on `deadline-job-bundle` and
  `deadline-job-attachments`
- §12 (job monitoring & logs) — depends on CloudWatch Logs SDK +
  queue/fleet role credential flows from §5/§9
- §14 (telemetry) — separate subsystem with background thread, retry
  logic, endpoint prefixing
- §34 (AWS client helpers) — part of `deadline-job-attachments`

## Future Improvements

- **Smithy model response filtering** — Post-process `ResponseBodyCapture`
  JSON through the Smithy service model to strip fields that boto3 would
  not include, and reorder fields to match boto3's Smithy-defined ordering.
  This would eliminate the field-order and extra-field differences from
  Python without requiring manual typed extraction. Low priority — the
  extra fields are harmless and the ordering is cosmetic.
