# Credential Scoping

## Problem

When a user is logged in via Deadline Cloud Monitor (DCM), their base
credentials only have Deadline API permissions. Accessing non-Deadline
services (CloudWatch Logs, S3) requires assuming a scoped role.

## Queue-Scoped Credentials

`get_queue_scoped_config(farm_id, queue_id, config)` in `session.rs`:

1. Calls `auth::get_user_and_identity_store_id(config)` — reads `user_id`
   and `identity_store_id` from the AWS profile's `~/.aws/config` section
2. If both are present (DCM user) → calls `get_queue_user_config` which
   builds an `SdkConfig` with `QueueUserCredentialProvider`
3. If either is absent (non-DCM user) → returns the base `SdkConfig`
4. If role assumption fails for a DCM user → error propagated (not silent fallback)

Used by: `job logs` (CloudWatch), attachment commands (S3).

## Fleet-Scoped Credentials

`get_fleet_scoped_config(farm_id, fleet_id, config)` in `log_retrieval.rs`:

1. Same DCM check via `get_user_and_identity_store_id`
2. If DCM → calls `AssumeFleetRoleForRead` API, builds one-shot temporary
   `SdkConfig` from the returned credentials (not cached — fleet credentials
   are short-lived and not reused)
3. If not DCM → returns base `SdkConfig`

Used by: `worker logs` (CloudWatch via fleet role).

## Queue User Config Caching

`get_queue_user_config` caches by `(farm_id, queue_id)` in the global
`SessionCache`. The `QueueUserCredentialProvider` implements the SDK's
`ProvideCredentials` trait — the SDK automatically calls it when credentials
expire, so the cached `SdkConfig` stays valid across multiple API calls.

## Endpoint Propagation

Queue-scoped `SdkConfig` instances need endpoint overrides propagated from
the base config. The manually-built `SdkConfig` doesn't read per-service
env vars (`AWS_ENDPOINT_URL_STS`, `AWS_ENDPOINT_URL_S3`) automatically.
The code propagates the global endpoint URL from the base config, with a
fallback to `AWS_ENDPOINT_URL_STS` if no global endpoint is set. This
ensures queue-scoped clients reach the stub server in tests.

## Critical Behavioral Contract

Without credential scoping, `job logs` and attachment operations fail for
DCM users. The error surfaces as "Failed to get queue credentials" or
"Failed to assume Queue role" rather than a confusing "access denied" from
CloudWatch/S3. This matches the Python behavior where `DeadlineOperationError`
is raised on role assumption failure.
