# deadline-client

AWS API interaction layer. Owns all SDK/HTTP calls to the Deadline Cloud
service, STS, and CloudWatch Logs.

## Role in the System

The bridge between business logic and AWS. All AWS API calls go through
this crate — no other crate should import AWS SDK service crates for
Deadline, STS, or CloudWatch. (Exception: `deadline-job-attachments` owns
its own S3 and STS clients because the attachment subsystem has
independent credential and caching lifecycles.)

Consumers: `deadline-cli`, `deadline-gui-ffi`, `deadline-mcp`.

## Key Concepts

**Raw JSON response capture.** All API calls use a `ResponseBodyCapture`
interceptor that grabs the raw HTTP response body as `serde_json::Value`.
This is necessary because the Rust SDK output types don't implement
`Serialize` (upstream issue, open since 2021). The interceptor
post-processes datetime strings and removes null values. This approach
means the CLI always gets the full API response — new fields appear
automatically without code changes.

**Global session cache.** A `LazyLock<Mutex<SessionCache>>` holds the
cached `SdkConfig` and queue credential configs. Building service clients
from a cached `SdkConfig` is cheap (no credential re-resolution), so
per-service client caching is unnecessary. The cache is invalidated on
logout or when `force_refresh` is requested.

**Credential scoping for non-Deadline services.** When a user is logged
in via Deadline Cloud Monitor (DCM), their base credentials only have
Deadline API permissions. Accessing CloudWatch Logs or S3 requires
assuming the queue role (or fleet role for worker logs). The
`get_queue_scoped_config` function handles this: checks if DCM is active,
assumes the queue role if so, returns base credentials for non-DCM users.
If role assumption fails for a DCM user, the error is propagated.
This is a critical behavioral contract — without it, `job logs` and
attachment operations fail silently for DCM users.

**User-Agent enrichment.** Every API call includes a custom User-Agent
string: `app/deadline-client#<version> submitter/<name>#<ver>
cli-command/<cmd>`. The session context tracks submitter identity (set by
GUI/DCC plugins) and CLI command name (set before each command).

## Behavior & Contracts

**Session caching:** One `SdkConfig` cached per profile. Invalidated by
`invalidate_session_cache()` (called on logout). Queue user configs are
cached keyed by `(farm_id, queue_id)` and invalidated alongside the base
session.

**Profile resolution:** `"(default)"`, `"default"`, and `""` all map to
the default credential chain. DCM profiles are detected by the presence
of `monitor_id` in the `[profile <name>]` section of `~/.aws/config`.

**Endpoint override:** Reads `AWS_ENDPOINT_URL_DEADLINE` (for Deadline)
and `AWS_ENDPOINT_URL_STS` (for STS) environment variables. These are
used by tests to point at the stub server.

**Auth status determination:** `AwsCredentialsSource` is determined by
parsing the AWS config file for `monitor_id`. If the profile section
doesn't exist, returns `NotValid` (not `HostProvided`).
`AwsAuthenticationStatus` is determined by calling STS
`GetCallerIdentity`.

**Login/logout:** Only works for DCM profiles. Spawns the Deadline Cloud
Monitor process with `login`/`logout` args. Login polls auth status in a
0.5s loop with cancellation support. Logout invalidates the session cache
after success.

**List operations:** Use manual `nextToken` loops (not SDK paginators)
because paginators don't support `.customize().interceptor()`. A shared
`paginated_list` helper eliminates the duplicated loop logic.

**Queue user credentials:** `QueueUserCredentialProvider` implements the
SDK's `ProvideCredentials` trait, calling `AssumeQueueRoleForUser`. The
SDK automatically refreshes when credentials expire. Error messages
include actionable guidance (throttling → retry, internal error → wait,
other → contact admin).

**Telemetry on every API call:** All public API functions accept an
optional `TelemetryClient` and record latency events. If `None`, an
ephemeral client is created internally. Telemetry never affects the API
call result.

## Design Decisions

**Struct-owned state instead of module-level caches.** `SessionCache`
owns the cached config and queue credentials as struct fields. The global
`LazyLock<Mutex<...>>` provides the singleton access pattern, but the
underlying struct is independently constructable for testing.

**`ResponseBodyCapture` over typed SDK outputs.** The SDK types would
give us field filtering (matching boto3's Smithy model filtering), but
they can't be serialized back to JSON. Raw capture gives us full
responses that automatically include new API fields. The tradeoff is
extra fields and different field ordering compared to boto3 — both are
documented as accepted differences.

**Separate log retrieval module.** `log_retrieval.rs` talks to CloudWatch
Logs, not the Deadline API. It's separated because it has different
credential requirements (queue/fleet-scoped) and a different AWS service
client. `get_session_logs` uses `get_queue_scoped_config` (DCM-gated
queue role). `get_worker_logs` uses `get_fleet_scoped_config` (DCM-gated
fleet role via `AssumeFleetRoleForRead`, one-shot temporary credentials).

**Session auto-selection for logs.** When no session ID is provided,
the system paginates all sessions and picks the best one: ongoing
sessions preferred (no `endedAt`, most recently started), then most
recently ended. The CLI reports how the selection was made so users
understand which session's logs they're seeing.

## Gotchas & Constraints

- The Deadline SDK prepends `management.` or `scheduling.` to the
  endpoint hostname (Smithy host prefix). In tests, this means the stub
  server receives requests at `management.localhost:PORT`. The
  `.localhost` TLD resolves to 127.0.0.1 per RFC 6761.

- `ResponseBodyCapture` datetime conversion replaces `T` with space and
  `Z` with `+00:00`. This matches the display format users expect but
  means the raw JSON values are no longer valid ISO 8601. Code that needs
  RFC 3339 (like `export-credentials`) must convert back.

- The two client caching hierarchies (session cache here vs S3 client
  cache in `deadline-job-attachments`) are intentionally independent.
  Credential changes after login don't propagate to attachment clients
  until the process restarts. This is acceptable because attachment
  operations run within a single submission flow.

- CloudWatch SDK retries `AccessDeniedException` with backoff. Error-path
  tests that return 403 from a mock may cause subprocess hangs if retries
  aren't accounted for.

- `get_queue_scoped_config` propagates the error if queue role assumption
  fails for a DCM user (matching Python, which raises
  `DeadlineOperationError`). This means DCM permission issues surface as
  clear "Failed to get queue credentials" errors rather than confusing
  "access denied" errors from CloudWatch/S3.

## Status & Gaps

Implemented: session management, auth, login/logout, all list/get/search
operations for farms/queues/fleets/jobs/workers/sessions/steps/tasks,
queue credentials, queue parameters, job monitoring, log retrieval
(with DCM credential scoping for queue and fleet roles),
telemetry integration.

Gaps:
- `create_job_from_job_bundle` (job submission) — blocked on work item #11
- `job trace-schedule` support APIs — experimental, deferred
- Smithy model response filtering (would eliminate extra-field differences
  from boto3) — low priority, extra fields are harmless
