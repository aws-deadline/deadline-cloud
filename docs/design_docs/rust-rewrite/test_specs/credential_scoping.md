# Credential Scoping — Non-Deadline AWS Services

> Part of [AWS Deadline Cloud Client — Behavioral Test Specification](index.md)
>
> This document describes observable behavior through public interfaces.
> Error descriptions use category labels (e.g., "returns error") rather than
> language-specific exception types. The Rust implementation should produce
> equivalent observable outcomes, not necessarily identical internal mechanics.

---

## Section 53: Credential scoping for non-Deadline AWS services

> **Rust crate:** `deadline-client` · **Module:** `session`, `log_retrieval`
> **CLI crate:** `deadline-cli` · **Module:** `commands/job`, `commands/attachment`
>
> **Logic under test:** Operations that access CloudWatch Logs or S3 for
> queue-scoped or fleet-scoped resources must use scoped credentials when
> the user is logged in via Deadline Cloud Monitor (DCM). DCM base
> credentials have Deadline Cloud API permissions but NOT direct
> CloudWatch Logs or S3 permissions for queue/fleet resources. The code
> must detect DCM login (via `monitor_id` in the AWS profile), assume
> the queue role (via `AssumeQueueRoleForUser`) or fleet role (via
> `AssumeFleetRoleForRead`), and build the non-Deadline AWS client with
> the returned temporary credentials. Non-DCM users use base credentials
> directly. If scoped role assumption fails for a DCM user, the code
> falls back to base credentials (matching Python's try/except pattern).

### `get_queue_scoped_config(farm_id, queue_id, config?) -> SdkConfig`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 1 | Happy path | User is NOT logged in via DCM (no `monitor_id` in profile) | Returns base SDK config; `AssumeQueueRoleForUser` is not called | |
| 2 | Happy path | User IS logged in via DCM (`monitor_id`, `user_id`, `identity_store_id` present) | Calls `AssumeQueueRoleForUser`; returns SDK config with queue-scoped credentials | |
| 3 | Error handling | DCM user but `AssumeQueueRoleForUser` returns error (e.g., 403) | Falls back to base SDK config; error is not propagated | Matches Python try/except |

### `get_session_logs` — credential path selection

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 4 | Auth/credential states | DCM user calls `get_session_logs` | `AssumeQueueRoleForUser` is called; CloudWatch `GetLogEvents` succeeds with queue credentials | |
| 5 | Auth/credential states | Non-DCM user calls `get_session_logs` | `AssumeQueueRoleForUser` is NOT called; CloudWatch `GetLogEvents` uses base credentials | |

### `get_worker_logs` — credential path selection

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 6 | Auth/credential states | DCM user calls `get_worker_logs` | `AssumeFleetRoleForRead` is called; CloudWatch `GetLogEvents` uses fleet credentials | |
| 7 | Auth/credential states | Non-DCM user calls `get_worker_logs` | `AssumeFleetRoleForRead` is NOT called; CloudWatch `GetLogEvents` uses base credentials | |
| 8 | Error handling | DCM user but `AssumeFleetRoleForRead` returns error | Falls back to base credentials | |

### CloudWatch SDK error formatting

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 9 | Error handling | CloudWatch returns `AccessDeniedException` (403) | Error message contains `"AccessDeniedException"` and the service message; does NOT contain generic `"service error"` | |
| 10 | Error handling | CloudWatch returns `ResourceNotFoundException` | Returns empty result with `count=0`, not an error | Existing behavior, regression guard |

### CLI `deadline job logs` — credential path end-to-end

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 11 | Auth/credential states | DCM profile configured; run `deadline job logs` | Output shows log events; `AssumeQueueRoleForUser` mock was hit | Level 2 |
| 12 | Auth/credential states | Non-DCM profile; run `deadline job logs` | Logs returned; `AssumeQueueRoleForUser` mock was NOT hit | Level 2 |

### CLI `deadline attachment download` — credential path end-to-end

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 13 | Auth/credential states | DCM profile; no `--profile`; run `deadline attachment download` | `AssumeQueueRoleForUser` mock was hit; S3 download uses queue credentials | Level 2 |
| 14 | Auth/credential states | Non-DCM profile; run `deadline attachment download` | `AssumeQueueRoleForUser` mock was NOT hit; S3 uses base credentials | Level 2 |

### CLI `deadline attachment upload` — credential path end-to-end

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 15 | Auth/credential states | DCM profile; no `--profile`; run `deadline attachment upload` | `AssumeQueueRoleForUser` mock was hit; S3 upload uses queue credentials | Level 2 |
| 16 | Auth/credential states | Non-DCM profile; run `deadline attachment upload` | `AssumeQueueRoleForUser` mock was NOT hit; S3 uses base credentials | Level 2 |

### Future operations (add tests when work items are implemented)

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 17 | Auth/credential states | `bundle submit` with DCM profile and attachments | Uses queue credentials for S3 upload | Work item #11 |
| 18 | Auth/credential states | `job download-output` with DCM profile | Uses queue credentials for S3 download | Work item #13 |
| 19 | Auth/credential states | `queue sync-output` with DCM profile | Uses queue credentials for S3 download | Work item #13 |
| 20 | Auth/credential states | `manifest download` with DCM profile | Uses queue credentials for S3 | Work item #10 |
| 21 | Auth/credential states | `manifest upload` without `--s3-cas-uri` with DCM profile | Uses queue credentials for S3 | Work item #10 |
| 22 | Auth/credential states | MCP `get_session_and_worker_logs` with DCM profile | Uses queue/fleet credentials for CloudWatch | Work item #17 |
