# Session — AWS Sessions, Auth & Credentials

> Part of [AWS Deadline Cloud Client — Behavioral Test Specification](index.md)
>
> This document describes observable behavior through public interfaces.
> Error descriptions use category labels (e.g., "returns error") rather than
> language-specific exception types. The Rust implementation should produce
> equivalent observable outcomes, not necessarily identical internal mechanics.

---

## Section 3: Session — AWS session/client management

> **Rust crate:** `deadline-api` · **Module:** `session`
>
> **Logic under test:** Creating and caching AWS SDK sessions and service clients based
> on the configured profile. Special handling for `"(default)"`, `"default"`, and `""`
> profile names (all map to the default credential chain). Credential refresh timeout
> patching for Deadline Cloud Monitor credentials. User-agent string construction.

### `get_session(force_refresh?, config?) -> AWS session`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 1 | Happy path | Call with default config where `defaults.aws_profile_name` is `"(default)"` | Returns a session using the default credential chain (no named profile) | `"(default)"`, `"default"`, and `""` all map to no named profile |
| 2 | Happy path | Call with config where profile is set to `"MyProfile"` | Returns a session using the `"MyProfile"` named profile | |
| 3 | Happy path | Profile name is `"default"` (literal string) | Returns a session with no named profile | Treated same as `"(default)"` |
| 4 | Happy path | Profile name is `""` (empty string) | Returns a session with no named profile | Empty string is in the special-case set |
| 5 | Caching | Call twice with same profile | Returns the same cached session object | |
| 6 | Caching | Call with `force_refresh=true` | Clears cache and returns a fresh session | |
| 7 | Config interaction | Pass explicit config override | Uses profile name from the provided config, not from disk | |
| 8 | Auth/credential states | Profile name refers to a nonexistent AWS profile | Returns error (profile not found) | |
| 9 | Happy path | Session uses regional STS endpoint | `sts_regional_endpoints` is set to `"regional"` on the underlying session | Avoids cross-region calls to global endpoint |
| 10 | Happy path | Session credentials support automatic refresh (e.g., Deadline Cloud Monitor) | Refreshable credentials use a 5-minute advisory and 2.5-minute mandatory refresh window | DCM credential optimization |
| 11 | Happy path | Session credentials do not support automatic refresh | No refresh window configuration occurs; session is returned normally | |
| 12 | Error handling | Configuring credential refresh windows fails | Error is silently caught; session is returned normally | |

### `invalidate_session_cache()`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 13 | Happy path | Call after sessions have been cached | All session and client caches are cleared | |
| 14 | Happy path | Call when caches are already empty | No error; caches remain empty | |

### `get_default_client_config(**extra_options) -> client config`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 15 | Happy path | No session context set (all empty) | User agent contains `app/deadline-api#{version}` only | |
| 16 | Happy path | `session_context["submitter-name"]` is set to `"Blender"` | User agent contains `submitter/Blender` | |
| 17 | Happy path | Both `submitter-name` and `submitter-version` are set | User agent contains `submitter/Blender#1.0` | |
| 18 | Happy path | `session_context["cli-command-name"]` is set to `"deadline.bundle.submit"` | User agent contains `cli-command/deadline.bundle.submit` | |
| 19 | Happy path | All context fields set | User agent is `app/deadline-api#{version} submitter/Name#Ver cli-command/Cmd` | Concatenated in order |
| 20 | Happy path | Additional options passed (e.g., `max_retry_attempts=3`) | Forwarded to the client config constructor | |

### `get_client(service_name, config?) -> service client`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 21 | Happy path | Get a `"deadline"` client | Returns a client for the Deadline service with default client config | |
| 22 | Happy path | Get an `"s3"` client | Returns a client for S3 | |
| 23 | Caching | Get the same service client twice | Returns the same cached client object | |
| 24 | Config interaction | Pass explicit config with different profile | Uses the session from that profile | |

### `get_session_client(session, service_name)` (cached)

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 25 | Happy path | Create client for `"deadline"` service | Returns client with default client config applied | |
| 26 | Caching | Same session + service_name called twice | Returns identical cached client | |
| 27 | Happy path | Different service names with same session | Returns different clients (one per service) | |
| 28 | Happy path | Different sessions with same service name | Returns different clients (one per session) | |

---

## Section 4: Session — auth status & credential source

> **Rust crate:** `deadline-api` · **Module:** `session`
>
> **Logic under test:** Determining where credentials come from (Deadline Cloud Monitor
> vs host-provided), checking authentication by calling STS, and probing Deadline API
> availability. The `monitor_id` key in the AWS profile's scoped config distinguishes
> DCM-created profiles from standard ones.

### `get_credentials_source(config?) -> AwsCredentialsSource`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 1 | Happy path | AWS profile has `monitor_id` in scoped config | Returns `DEADLINE_CLOUD_MONITOR_LOGIN` | DCM-created profile |
| 2 | Happy path | AWS profile does NOT have `monitor_id` | Returns `HOST_PROVIDED` | Standard AWS credentials |
| 3 | Auth/credential states | Profile name does not exist (profile not found) | Returns `NOT_VALID` | |

### `get_user_and_identity_store_id(config?) -> (optional string, optional string)`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 4 | Happy path | Profile has `monitor_id`, `user_id`, and `identity_store_id` | Returns `(user_id, identity_store_id)` tuple | |
| 5 | Happy path | Profile does NOT have `monitor_id` | Returns `(none, none)` | |

### `get_monitor_id(config?) -> optional string`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 6 | Happy path | Profile has `monitor_id` set | Returns the monitor_id string | |
| 7 | Happy path | Profile does NOT have `monitor_id` | Returns none | |

### `check_authentication_status(config?) -> AwsAuthenticationStatus`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 8 | Happy path | `sts:GetCallerIdentity` succeeds | Returns `AUTHENTICATED` | |
| 9 | Auth/credential states | `sts:GetCallerIdentity` fails AND credentials source is DCM login | Returns `NEEDS_LOGIN` | |
| 10 | Auth/credential states | `sts:GetCallerIdentity` fails AND credentials source is NOT DCM login | Returns `CONFIGURATION_ERROR` | |
| 11 | Auth/credential states | `sts:GetCallerIdentity` fails AND credentials source is NOT_VALID | Returns `CONFIGURATION_ERROR` | |
| 12 | Happy path | During check, credential-related log noise is suppressed | Log level is restored after the check completes | Prevents noisy credential warnings |
| 13 | Error handling | Any error from GetCallerIdentity (not just specific client errors) | Falls through to credential source check | |

### `check_deadline_api_available(config?) -> bool`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 14 | Happy path | `deadline:ListFarms` succeeds with `maxResults=1` | Returns true | |
| 15 | Auth/credential states | `deadline:ListFarms` raises any error | Returns false | Error is logged |
| 16 | Happy path | User has `user_id` from DCM login | `principalId` is included in the ListFarms call | Filters to user's farms |

---

## Section 5: Session — queue user credentials

> **Rust crate:** `deadline-api` · **Module:** `session`
>
> **Logic under test:** Obtaining temporary credentials via `AssumeQueueRoleForUser`,
> wrapping them in a refreshable credential provider, and creating a new AWS session
> with those credentials. Includes caching, fallback to config defaults, and specific
> error messages for throttling, internal server errors, and access denied.

### `get_queue_user_session(deadline_api, config?, farm_id?, queue_id?, queue_display_name?, force_refresh?) -> AWS session`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 1 | Happy path | Provide all parameters explicitly | Returns a session with queue role credentials | |
| 2 | Happy path | `farm_id` is not provided | Falls back to `get_setting("defaults.farm_id")` | |
| 3 | Happy path | `queue_id` is not provided | Falls back to `get_setting("defaults.queue_id")` | |
| 4 | Happy path | `queue_display_name` is not provided | Passed as none to the credential provider (uses queue_id as fallback display) | |
| 5 | Caching | Call twice with same parameters | Returns the same cached session | |
| 6 | Caching | Call with `force_refresh=true` | Base session cache is cleared; new session is created | |
| 7 | Happy path | Base session profile is not `"default"` | Queue session inherits the profile name | |
| 8 | Happy path | Base session profile is `"default"` | Queue session uses no named profile | |
| 9 | Happy path | Queue session inherits `region_name` from base session | Region is passed through to the new session | |

### Queue user credential provider

> **Logic under test:** A custom credential provider that calls `AssumeQueueRoleForUser`
> and returns refreshable credentials. On error, produces specific user-facing messages
> depending on the AWS error code.

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 10 | Happy path | `load()` calls credential fetch and returns refreshable credentials | Credentials contain access_key, secret_key, token, expiry_time | |
| 11 | Happy path | Credential fetch succeeds | Returns dict with `access_key`, `secret_key`, `token`, `expiry_time` (ISO format) | |
| 12 | Happy path | `queue_display_name` is provided | Error messages use the display name | |
| 13 | Happy path | `queue_display_name` is not provided | Error messages use the queue_id as fallback | |
| 14 | Error handling | `AssumeQueueRoleForUser` returns `ThrottlingException` | Returns error with "Throttled" message and retry guidance | |
| 15 | Error handling | `AssumeQueueRoleForUser` returns `InternalServerException` | Returns error with "internal server error" message | |
| 16 | Error handling | `AssumeQueueRoleForUser` returns other AWS error (e.g., `AccessDeniedException`) | Returns error with "Failed to assume Queue role" and admin contact guidance | |
| 17 | Error handling | `AssumeQueueRoleForUser` returns empty credentials (none) | Returns error with "Empty credentials received" | |
| 18 | Error handling | `AssumeQueueRoleForUser` returns response with no `"credentials"` key | Returns error with "Empty credentials received" | |

### `precache_clients(deadline_api?, config?, farm_id?, queue_id?, queue_display_name?) -> (deadline_api, s3_client)`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 19 | Happy path | All parameters provided | Returns `(deadline_api, s3_client)` tuple; S3 client is pre-warmed | |
| 20 | Happy path | All optional params are none | Creates deadline client from config, reads farm/queue from settings, calls GetQueue for display name | |

---
