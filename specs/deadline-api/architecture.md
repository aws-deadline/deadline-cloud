# deadline-api Architecture

## Crate Position

```
deadline-cli ──► deadline-api    ← this crate
deadline-python-bindings ──► deadline-api
```

All AWS API calls go through this crate — no other crate imports AWS SDK
service crates for Deadline, STS, or CloudWatch. Exception:
`deadline-job-attachments` owns its own S3 and STS clients independently.

## Module Layout

The crate is organized around three main concerns: session management,
authentication, and API calls. Supporting modules handle job monitoring,
log retrieval, queue parameters, telemetry, and version checking.

All API calls flow through `api.rs` using the ResponseBodyCapture
interceptor from `raw_response.rs`. Session and credential state is
managed by `session.rs`, which caches SDK configs per profile and
queue-scoped credentials per farm+queue pair. Authentication detection
and DCM login/logout live in `auth.rs`.

```
src/
├── lib.rs              # Re-exports all public modules
├── session.rs          # Session caching, credential resolution, queue-scoped configs
├── auth.rs             # DCM detection, login/logout, auth status checks
├── api.rs              # All Deadline Cloud API calls, pagination helper, error formatting
├── raw_response.rs     # ResponseBodyCapture interceptor
├── job_monitoring.rs   # Poll job until terminal state, collect failed task details
├── log_retrieval.rs    # CloudWatch Logs, session auto-selection, fleet credentials
├── queue_parameters.rs # Queue environment parameter extraction
├── errors.rs           # DeadlineError enum
├── submitter_info.rs   # Submitter metadata (name, version, DCC info)
├── path_utils.rs       # File size formatting, path summarization
├── telemetry.rs        # Background telemetry client
└── update_checker.rs   # Remote version check, never panics
```

## Key Design Decisions

**Raw JSON response capture.** All API calls use the ResponseBodyCapture
interceptor to grab the raw HTTP response as `serde_json::Value`. The
AWS SDK output types don't implement `Serialize`, so this is the only
way to get JSON without manually extracting every field. The interceptor
also converts datetimes to Python display format and strips null values.
New API fields appear automatically without code changes. See
[response-capture.md](response-capture.md) for the full pattern.

**Global session cache.** A process-wide cache holds SDK configs keyed
by profile name, and queue credential configs keyed by farm+queue pair.
This avoids re-creating SDK clients on every API call. The cache is
invalidated on logout or when the profile changes.

**Manual pagination.** SDK paginators don't support the
`.customize().interceptor()` chain needed for response capture, so all
paginated operations use explicit `nextToken` loops. A shared helper
keeps this DRY.

**DCM detection by config file parsing.** Rather than making an API call
to determine the credential source, the crate reads `~/.aws/config`
directly and checks for the `monitor_id` key. This is fast and works
offline.

**Login/logout spawns a subprocess.** The DCM binary is a separate
process. Login spawns it with `login` args and polls auth status every
0.5 seconds. Logout spawns with `logout` args and invalidates the
session cache.

**Telemetry on every API call.** All public API functions optionally
accept a telemetry client and record latency. If none is provided, an
ephemeral client is created internally. Telemetry never affects the API
call result.

## Public API Surface

### Making an API call

All API functions follow the same pattern — pass optional config and
telemetry, get back `serde_json::Value`:

```rust
let farms = deadline_api::list_farms(Some(&config), Some(&telemetry)).await?;
let farm = deadline_api::get_farm("farm-abc", Some(&config), None).await?;
```

The returned `Value` is the raw API response with datetimes converted
and nulls stripped. The CLI formats and prints it directly.

### Getting a session / SDK config

```rust
let sdk_config = deadline_api::get_sdk_config(Some(&config)).await;
```

This returns a cached `SdkConfig` for the active profile. Changing
profiles invalidates the cache.

### Queue-scoped credentials

For operations that access S3 or CloudWatch on behalf of a queue:

```rust
let scoped_config = deadline_api::get_queue_scoped_config(
    farm_id, queue_id, Some(&config)
).await?;
```

This checks whether the user is logged in via DCM. If so, it assumes
the queue role and returns a scoped `SdkConfig`. If not, it returns the
base config. See [credential-scoping.md](credential-scoping.md).

### Core types

| Type | Purpose |
|------|---------|
| `DeadlineError` | Error enum for all API, config, auth, and attachment failures |
| `TelemetryClient` | Background telemetry — pass to API functions for latency tracking |
| `AwsCredentialsSource` | Enum: `NotValid`, `HostProvided`, `DeadlineCloudMonitorLogin` |
| `AwsAuthenticationStatus` | Enum: `ConfigurationError`, `Authenticated`, `NeedsLogin` |
| `ResponseBodyCapture` | SDK interceptor — not used directly by consumers, but good to know about |
