# deadline-api Crate Specifications

AWS API interaction layer. Owns all SDK/HTTP calls to the Deadline Cloud
service, STS, and CloudWatch Logs. The bridge between business logic and AWS.

Consumers: `deadline-cli`, `deadline-python-bindings`.

Dependencies: `deadline-config`.

## How API Calls Work

Two patterns coexist during the migration from wrapper functions to
direct SDK usage:

### New pattern (Farm, Queue list, Fleet — Batch B complete)

Callers use the SDK fluent builder directly. A `TelemetryInterceptor`
installed on the client at construction time automatically emits one
latency event per operation (reading the operation name from the SDK's
`Metadata` config-bag entry). For raw JSON output, callers attach a
per-call `ResponseBodyCapture` interceptor via `.customize().interceptor(cap)`.

```rust
// Typed list with raw capture for full-response output
let dl = session::deadline_client(config).await;
let builder = client::apply_dcm_principal(dl.list_farms(), config);
let resp = client::collect_paginated_raw("farms", |token| { ... }).await?;

// Raw get for full-response print
let cap = ResponseBodyCapture::new();
dl.get_farm().farm_id(&farm)
    .customize().interceptor(cap.clone())
    .send().await.map_err(client::deadline_error)?;
let resp = cap.json()?;
```

### Legacy pattern (Job, Step, Task, Worker, Session — api.rs)

Wrapper functions in `api.rs` use `ResponseBodyCapture` for raw JSON
capture. Telemetry is handled by the client-level `TelemetryInterceptor`
(same as the new pattern). These wrappers are thin convenience functions
that may be inlined at callers or moved to `job_api.rs` in a future batch.

## Document Index

| Document | Description |
|----------|-------------|
| [architecture.md](architecture.md) | Module layout, public API surface, design decisions |
| [session-cache.md](session-cache.md) | Global session cache, credential resolution, user-agent enrichment |
| [response-capture.md](response-capture.md) | ResponseBodyCapture interceptor, datetime conversion, known differences from boto3 |
| [credential-scoping.md](credential-scoping.md) | Queue/fleet role assumption for DCM users, scoped SdkConfig construction |
| [log-retrieval.md](log-retrieval.md) | CloudWatch Logs integration, session auto-selection, fleet-scoped credentials |
| [job-monitoring.md](job-monitoring.md) | wait_for_job_completion polling loop, failed task collection, backoff curve |
| [update-checker.md](update-checker.md) | Remote manifest fetch, version comparison, config opt-out |

## Status

Implemented: session management, auth, login/logout, all list/get/search
operations for farms/queues/fleets/jobs/workers/sessions/steps/tasks,
queue credentials, queue parameters, job monitoring, log retrieval,
telemetry integration, CreateJob API call and creation polling,
queue/fleet credential scoping for CloudWatch and S3, batch get
steps/tasks (for trace-schedule), update job/task.

Gaps:
- None — all API functions implemented

## Gotchas & Constraints

- The Deadline SDK prepends `management.` or `scheduling.` to the endpoint
  hostname (Smithy host prefix). In tests, the stub server receives requests
  at `management.localhost:PORT`. The `.localhost` TLD resolves to 127.0.0.1
  per RFC 6761.

- `ResponseBodyCapture` datetime conversion replaces `T` with space and
  `Z` with `+00:00`. This matches the display format users expect but
  means the raw JSON values are no longer valid ISO 8601. Code that needs
  RFC 3339 (like `export-credentials`) must convert back.

- CloudWatch SDK retries `AccessDeniedException` with backoff. Error-path
  tests that return 403 from a mock may cause subprocess hangs if retries
  aren't accounted for.

- `get_queue_scoped_config` propagates the error if queue role assumption
  fails for a DCM user (matching Python). DCM permission issues surface as
  clear "Failed to get queue credentials" errors rather than confusing
  "access denied" errors from CloudWatch/S3.

## Relationship to the Python Library

Mirrors the Python `deadline.client.api` module's public API surface.
Key differences:
- Python uses `boto3.Session` with `@lru_cache`; Rust uses `LazyLock<Mutex<SessionCache>>`
- Python returns typed SDK output objects; Rust returns `serde_json::Value`
  (via `ResponseBodyCapture`)
- Python uses `botocore` paginators; Rust uses manual `nextToken` loops
- Python's `QueueBoto3Session` wraps boto3; Rust's `QueueUserCredentialProvider`
  implements the SDK's `ProvideCredentials` trait directly
