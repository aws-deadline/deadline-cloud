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
`DeadlineCloudMonitorLogin`. Currently only `HostProvided` is returned —
DCM detection (checking `monitor_id` in AWS profile scoped config) is not
yet implemented.

`AwsAuthenticationStatus` enum: `ConfigurationError`, `Authenticated`,
`NeedsLogin`. Determined by calling STS GetCallerIdentity.

`check_deadline_api_available`: calls ListFarms with `maxResults=1`.
Returns bool.

## API (`api.rs`)

- `list_farms`, `list_queues`, `list_fleets`, `list_jobs` — paginated, concatenates all pages via `nextToken`
- `get_farm`, `get_queue`, `get_fleet`, `get_job` — single resource by ID

All take `farm_id` (and `queue_id`/`fleet_id` where needed) as parameters.
Returns `serde_json::Value` — the CLI formats and prints it directly.

## Not Yet Implemented

- Session caching and user-agent construction (§3 cases 5-20)
- DCM credential source detection (§4 cases 3-7, 9)
- Queue user credentials (§5)
- Login/logout (§6)
- Remaining list APIs: queues, jobs, fleets, storage profiles (§7)
- principalId auto-injection for list APIs (§7 cases 5-17)
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
