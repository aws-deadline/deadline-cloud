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

- `list_farms`, `list_queues`, `list_fleets`, `list_jobs` — paginated, concatenates all pages via `nextToken`
- `get_farm`, `get_queue`, `get_fleet`, `get_job` — single resource by ID

All take `farm_id` (and `queue_id`/`fleet_id` where needed) as parameters.
Returns `serde_json::Value` — the CLI formats and prints it directly.

- `search_workers(farm_id, fleet_ids, item_offset, page_size, config)` — calls
  `SearchWorkers` (POST). Returns `{"workers": [...], "totalResults": N}`.
  Used by `deadline worker list` (Python uses SearchWorkers, not ListWorkers).
- `get_worker(farm_id, fleet_id, worker_id, config)` — single worker by ID.
- `list_storage_profiles_for_queue(farm_id, queue_id, config)` — paginated,
  returns `{"storageProfiles": [...]}`. Does NOT inject `principalId`.

## Not Yet Implemented

- Session caching and user-agent construction (§3 cases 5-20)
- Queue user credentials (§5)
- Login/logout (§6)
- Queue parameters (§8)
- Queue credentials — assume role (§9)
- Storage profile (§10)

## Deferred

- §11 (submit job bundle) — blocked on `deadline-job-bundle` and
  `deadline-job-attachments`
- §12 (job monitoring & logs) — depends on CloudWatch Logs SDK +
  queue/fleet role credential flows
- §13 (diagnostics) — thin wrappers, deferred to keep scope focused
- §14 (telemetry) — separate subsystem with background thread, retry
  logic, endpoint prefixing
- §34 (AWS client helpers) — part of `deadline-job-attachments`
