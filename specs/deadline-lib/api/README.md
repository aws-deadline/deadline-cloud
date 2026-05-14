# api Module

AWS API interaction layer. Owns all SDK/HTTP calls to the Deadline Cloud
service, STS, and CloudWatch Logs. The bridge between business logic and AWS.

## What It Does

- Session management and credential caching (global process-wide cache)
- DCM (Deadline Cloud Monitor) login/logout and auth status detection
- Queue/fleet credential scoping for non-Deadline AWS services (S3, CloudWatch)
- Telemetry recording (background client with interceptor-based latency tracking)
- Job monitoring (poll until terminal state, collect failed task details)
- CloudWatch log retrieval with session auto-selection
- Remote version checking (update notifications)
- Paginated list/search operations via SDK native paginators

## Key Pattern

Callers use the SDK fluent builder directly — no wrapper functions that
re-declare parameters. Helpers handle pagination (`collect_paginated`),
error mapping (`deadline_error`), and DCM principal injection
(`apply_dcm_principal`). Telemetry is automatic via a client-level
interceptor installed at construction time.

## Consumers

- `deadline-cli` — all resource commands, auth, job monitoring
- `deadline-python-bindings` — resource listing, auth, submission

## Dependencies

- `deadline-lib::config` — setting resolution

## Document Index

| Document | Description |
|----------|-------------|
| [architecture.md](architecture.md) | Module layout, SDK usage patterns, design decisions, public API |
| [session-cache.md](session-cache.md) | Global session cache, credential resolution, user-agent enrichment |
| [credential-scoping.md](credential-scoping.md) | Queue/fleet role assumption for DCM users |
| [log-retrieval.md](log-retrieval.md) | CloudWatch Logs integration, session auto-selection |
| [job-monitoring.md](job-monitoring.md) | Polling loop, failed task collection, backoff curve |
| [update-checker.md](update-checker.md) | Remote manifest fetch, version comparison |

## Status

Fully implemented. No known gaps.

## Gotchas

- The Deadline SDK prepends `management.` or `scheduling.` to the endpoint
  hostname (Smithy host prefix). Test stubs must handle this.
- CloudWatch SDK retries `AccessDeniedException` with backoff — error-path
  tests may hang if retries aren't accounted for.
- `get_queue_scoped_config` propagates the error if queue role assumption
  fails for a DCM user (clear error message, not confusing "access denied").
