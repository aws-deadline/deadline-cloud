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
> queue-scoped or fleet-scoped resources must use scoped credentials.
>
> **Two distinct credential patterns exist in Python:**
>
> 1. **CloudWatch Logs (DCM-gated):** `get_session_logs` and
>    `get_worker_logs` explicitly check for DCM login via
>    `get_user_and_identity_store_id()`. If DCM, assume queue/fleet
>    role. If not DCM, use base credentials. If role assumption fails,
>    propagate the error (Python raises `DeadlineOperationError`).
>
> 2. **S3 operations (unconditional):** `attachment download/upload`,
>    `bundle submit`, `job download-output`, `queue sync-output`, and
>    `manifest download/upload` call `get_queue_user_boto3_session`
>    unconditionally when no `--profile` is provided. The queue
>    credential provider (`QueueUserCredentialProvider`) is always
>    inserted into the botocore session — it is NOT gated on DCM.
>    For DCM users, `AssumeQueueRoleForUser` succeeds. For non-DCM
>    users, it is still called and may succeed if the user has
>    permission. When `--profile` is provided, queue role assumption
>    is skipped entirely.

### `get_queue_scoped_config(farm_id, queue_id, config?) -> SdkConfig`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 1 | Happy path | User is NOT logged in via DCM (no `monitor_id` in profile) | Returns base SDK config; `AssumeQueueRoleForUser` is not called | |
| 2 | Happy path | User IS logged in via DCM (`monitor_id`, `user_id`, `identity_store_id` present) | Calls `AssumeQueueRoleForUser`; returns SDK config with queue-scoped credentials | |
| 3 | Error handling | DCM user but `AssumeQueueRoleForUser` returns error (e.g., 403) | Error is propagated as `DeadlineError`; does not fall back to base credentials | Matches Python: raises `DeadlineOperationError("Failed to get queue credentials: ...")` |

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
| 8 | Error handling | DCM user but `AssumeFleetRoleForRead` returns error | Error is propagated as `DeadlineError`; does not fall back to base credentials | Matches Python: raises `DeadlineOperationError("Failed to get fleet credentials: ...")` |

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

Python calls `get_queue_user_boto3_session` unconditionally when no
`--profile` is provided. This is NOT DCM-gated — the queue credential
provider is always inserted into the botocore session. For DCM users,
`AssumeQueueRoleForUser` succeeds (DCM credentials have permission).
For non-DCM users, `AssumeQueueRoleForUser` is still called and may
succeed if the user has direct permission.

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 13 | Auth/credential states | No `--profile`; run `deadline attachment download` | `AssumeQueueRoleForUser` is called; S3 download uses queue credentials | Level 2. Python always assumes queue role when no --profile. |
| 14 | Auth/credential states | `--profile` provided; run `deadline attachment download` | `AssumeQueueRoleForUser` is NOT called; S3 uses profile credentials directly | Level 2. --profile bypasses queue role assumption. |

### CLI `deadline attachment upload` — credential path end-to-end

Same pattern as download: `get_queue_user_boto3_session` called
unconditionally when no `--profile`.

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 15 | Auth/credential states | No `--profile`; run `deadline attachment upload` | `AssumeQueueRoleForUser` is called; S3 upload uses queue credentials | Level 2. Python always assumes queue role when no --profile. |
| 16 | Auth/credential states | `--profile` provided; run `deadline attachment upload` | `AssumeQueueRoleForUser` is NOT called; S3 uses profile credentials directly | Level 2. --profile bypasses queue role assumption. |

### Future operations (add tests when work items are implemented)

Python calls `get_queue_user_boto3_session` unconditionally (not
DCM-gated) for all S3 operations. The queue credential provider is
always inserted. Tests should verify `AssumeQueueRoleForUser` is called
regardless of DCM status.

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 17 | Auth/credential states | `bundle submit` with attachments | `AssumeQueueRoleForUser` called; uses queue credentials for S3 upload | Work item #11 |
| 18 | Auth/credential states | `job download-output` | `AssumeQueueRoleForUser` called; uses queue credentials for S3 download | Work item #13 |
| 19 | Auth/credential states | `queue sync-output` | `AssumeQueueRoleForUser` called; uses queue credentials for S3 download | Work item #13 |
| 20 | Auth/credential states | `manifest download` | `AssumeQueueRoleForUser` called; uses queue credentials for S3 | Work item #10 |
| 21 | Auth/credential states | `manifest upload` without `--s3-cas-uri` | `AssumeQueueRoleForUser` called; uses queue credentials for S3 | Work item #10 |
| 22 | Auth/credential states | MCP `get_session_and_worker_logs` with DCM profile | Uses queue/fleet credentials for CloudWatch (DCM-gated, same as CLI logs) | Work item #17 |
