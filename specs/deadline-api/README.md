# deadline-api Crate Specifications

AWS API interaction layer. Owns all SDK/HTTP calls to the Deadline Cloud
service, STS, and CloudWatch Logs. The bridge between business logic and AWS.

Consumers: `deadline-cli`, `deadline-python-bindings`.

Dependencies: `deadline-config`.

## How API Calls Work

### Typed pattern (all API calls)

Callers use SDK fluent builders directly and return typed SDK output.
Callers that need specific fields use typed accessors. Callers that
print the full response use `From<Output>` on response structs — nested
types are converted via `type_conversions.rs` helpers.

```rust
// Typed get — returns SDK output directly
let output = api::get_job(farm_id, queue_id, job_id, config).await?;
let name = output.name();  // typed field access

// Full-dump display — response struct built entirely from typed output
let output = client.get_job().farm_id(f).queue_id(q).job_id(j).send().await?;
let resp = JobResponse::from(output);
println!("{}", cli_object_repr(&serde_json::to_value(&resp)?));

// Typed list — paginator + field extraction (caller owns the call)
let pages = client::collect_paginated(dl.list_farms().into_paginator().send()).await?;
let items: Vec<_> = pages.iter().flat_map(|p| p.farms())
    .map(|f| json!({"farmId": f.farm_id(), "displayName": f.display_name()}))
    .collect();

// Search (single-page, offset-based) — caller calls SDK directly
let output = client.search_jobs().farm_id(farm).queue_ids(queue)
    .item_offset(0).page_size(25).send().await?;
for job in output.jobs() { /* typed JobSearchSummary access */ }
```

### What lives in `api.rs` (only real logic, not wrappers)

Functions in `api.rs` exist only when they contain algorithmic logic
beyond parameter forwarding: `list_jobs_by_filter_expression` (pagination
by createdAt threshold), `create_job` (JSON→typed parameter mapping),
`batch_get_steps_page`/`batch_get_tasks_page` (identifier construction),
`wait_for_create_job_to_complete` (polling loop), and
`build_filter_expressions`/`build_sort_expressions` (SDK type construction).

See `specs/patterns.md` § "What is a thin wrapper?" for the full rule.

## Document Index

| Document | Description |
|----------|-------------|
| [architecture.md](architecture.md) | Module layout, public API surface, design decisions |
| [session-cache.md](session-cache.md) | Global session cache, credential resolution, user-agent enrichment |
| [credential-scoping.md](credential-scoping.md) | Queue/fleet role assumption for DCM users, scoped SdkConfig construction |
| [log-retrieval.md](log-retrieval.md) | CloudWatch Logs integration, session auto-selection, fleet-scoped credentials |
| [job-monitoring.md](job-monitoring.md) | wait_for_job_completion polling loop, failed task collection, backoff curve |
| [update-checker.md](update-checker.md) | Remote manifest fetch, version comparison, config opt-out |

`responses.rs` — Response structs (`FarmResponse`, `QueueResponse`,
`FleetResponse`, `JobResponse`, `StepResponse`, `TaskResponse`,
`SessionResponse`, `WorkerResponse`) and `format_datetime` helper. Each
struct maps 1:1 to a Get API output with `#[serde(rename_all = "camelCase")]`
and `skip_serializing_if` for optional fields. Complex nested SDK types
that lack `Serialize` are converted to `serde_json::Value` via
`type_conversions.rs` helpers that walk typed SDK accessors.

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
- Python returns typed SDK output objects; Rust returns typed SDK output
  for all APIs. Three `api.rs` functions (`list_jobs_by_filter_expression`,
  `batch_get_steps_page`, `batch_get_tasks_page`) return `Value` built
  from typed accessors for display-path convenience.
- Python uses `botocore` paginators; Rust uses SDK native paginators via
  `collect_paginated()`
- Python's `QueueBoto3Session` wraps boto3; Rust's `QueueUserCredentialProvider`
  implements the SDK's `ProvideCredentials` trait directly
