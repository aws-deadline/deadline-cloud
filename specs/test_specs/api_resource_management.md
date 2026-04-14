# API — Resource Management

> Part of [AWS Deadline Cloud Client — Behavioral Test Specification](index.md)
>
> This document describes observable behavior through public interfaces.
> Error descriptions use category labels (e.g., "returns error") rather than
> language-specific exception types. The Rust implementation should produce
> equivalent observable outcomes, not necessarily identical internal mechanics.

---

## Section 6: API — login/logout

> **Rust crate:** `deadline-api` · **Module:** `auth`
>
> **Logic under test:** Login launches the Deadline Cloud Monitor (DCM) executable as a
> subprocess and polls `check_authentication_status` in a loop until authenticated.
> Logout calls the DCM `logout` subcommand and invalidates the session cache.
> Both operations are only supported for DCM-created profiles.

### `login(on_pending_authorization, on_cancellation_check, config?) -> string`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 1 | Happy path | Credentials source is DCM login, monitor process starts, auth succeeds | Returns `"Deadline Cloud monitor profile: {profile_name}"` | Polls auth status in a loop |
| 2 | Auth/credential states | Credentials source is HOST_PROVIDED (not DCM) | Returns error: login only supported for DCM profiles | |
| 3 | Auth/credential states | Credentials source is NOT_VALID | Returns error: login only supported for DCM profiles | |
| 4 | Error handling | Deadline Cloud Monitor executable not found at configured path | Returns error containing "Could not find Deadline Cloud monitor" | |
| 5 | Error handling | Monitor process exits before auth succeeds (non-zero return) | Returns error containing "was not able to log into" and stdout content | |
| 6 | Concurrency/cancellation | `on_cancellation_check` callback returns true during polling | Monitor process is killed; operation is canceled | |
| 7 | Happy path | `on_pending_authorization` callback is called with `credentials_source=DEADLINE_CLOUD_MONITOR_LOGIN` | Callback receives the credential source enum value | |
| 8 | Happy path | Auth succeeds after multiple polling iterations (0.5s sleep each) | Returns success string after polling loop completes | |
| 9 | Interactive vs scripted | `on_pending_authorization` is not provided | No callback invoked; login proceeds normally | |
| 10 | Interactive vs scripted | `on_cancellation_check` is not provided | No cancellation check; polling continues until auth or process exit | |
| 11 | Cross-platform | On Windows, subprocess uses piped stdin | Avoids Windows-specific stdin issues | |
| 12 | Cross-platform | On non-Windows, subprocess uses devnull stdin | Standard POSIX behavior | |

### `logout(config?) -> string`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 13 | Happy path | Credentials source is DCM login, logout subprocess succeeds | Returns the stdout output of the logout command; session cache is invalidated | |
| 14 | Auth/credential states | Credentials source is HOST_PROVIDED | Returns error: logout only supported for DCM profiles | |
| 15 | Error handling | Monitor executable not found | Returns error containing "Could not find Deadline Cloud monitor" | |
| 16 | Error handling | Logout subprocess returns non-zero exit code | Returns error containing "unable to log out" and return code | |
| 17 | Happy path | After successful logout, session cache is invalidated | `invalidate_session_cache()` is called | Ensures fresh credentials on next use |
| 18 | Happy path | Logout command is called with `["monitor_path", "logout", "--profile", profile_name]` | Correct arguments passed to subprocess | |

---

## Section 7: API — list (farms/queues/jobs/fleets/storage)

> **Rust crate:** `deadline-api` · **Module:** `api`
>
> **Logic under test:** Paginated list API wrapper that concatenates all pages into a
> single result. For farms, queues, jobs, and fleets, the `principalId` is auto-injected
> when the user is logged in via DCM (has a `user_id`). Storage profile listing does NOT
> auto-inject `principalId`.

### `call_paginated_deadline_list_api(list_api, list_property_name, **kwargs)`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 1 | Happy path | API returns single page (no `nextToken`) | Returns `{list_property_name: [...]}` with all items | |
| 2 | Pagination/batching | API returns multiple pages with `nextToken` | Concatenates all pages into a single list under `list_property_name` | |
| 3 | Pagination/batching | API returns first page with items, second page empty | Returns items from first page only | |
| 4 | Boundary values | API returns empty list on first page | Returns `{list_property_name: []}` | |

### `list_farms(config?, **kwargs)`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 5 | Happy path | User is logged in via DCM (has `user_id`) | `principalId` is automatically added to the API call | Filters to user's farms |
| 6 | Happy path | User is NOT logged in via DCM (no `user_id`) | `principalId` is NOT added; all farms returned | |
| 7 | Happy path | Caller explicitly passes `principalId` in kwargs | The explicit value is used (not overridden) | |
| 8 | Pagination/batching | Farm list spans multiple pages | All farms are concatenated into `{"farms": [...]}` | |

### `list_queues(config?, **kwargs)`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 9 | Happy path | User has `user_id`, no explicit `principalId` | `principalId` auto-added | Same pattern as list_farms |
| 10 | Happy path | No `user_id`, no explicit `principalId` | No `principalId` in call | |
| 11 | Pagination/batching | Multiple pages | All queues concatenated into `{"queues": [...]}` | |

### `list_jobs(config?, **kwargs)`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 12 | Happy path | User has `user_id` | `principalId` auto-added | |
| 13 | Happy path | No `user_id` | No `principalId` in call | |
| 14 | Pagination/batching | Multiple pages | All jobs concatenated into `{"jobs": [...]}` | |

### `list_fleets(config?, **kwargs)`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 15 | Happy path | User has `user_id` | `principalId` auto-added | |
| 16 | Happy path | No `user_id` | No `principalId` in call | |
| 17 | Pagination/batching | Multiple pages | All fleets concatenated into `{"fleets": [...]}` | |

### `list_storage_profiles_for_queue(config?, **kwargs)`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 18 | Happy path | Call with `farmId` and `queueId` | Returns `{"storageProfiles": [...]}` | |
| 19 | Happy path | No `principalId` logic | Does NOT add `principalId` (unlike other list APIs) | This API doesn't filter by user |
| 20 | Pagination/batching | Multiple pages | All storage profiles concatenated | |

### Common behavior across all list APIs

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 21 | Config interaction | Pass explicit config override | Uses that config for session/client creation | |
| 22 | Error handling | Underlying API call raises an AWS service error | Error propagates to caller (no special handling) | |

---

## Section 8: API — queue parameters

> **Rust crate:** `deadline-api` · **Module:** `api`
>
> **Logic under test:** Fetching all queue environment templates, extracting their
> `parameterDefinitions`, deduplicating by name, auto-detecting UI controls, and
> setting `groupLabel` to the environment name when not explicitly provided.
> Environments are sorted by priority. Duplicate parameter names with mismatched
> definitions produce an error.

### `get_queue_parameter_definitions(farmId, queueId, config?) -> list of job parameters`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 1 | Happy path | Queue has one environment with one parameter definition | Returns list with one parameter, including `userInterface.groupLabel` set to environment name | |
| 2 | Happy path | Queue has multiple environments, each with parameters | Returns merged list sorted by environment priority | Environments sorted by `priority` field |
| 3 | Happy path | Parameter already has `userInterface.groupLabel` set | Existing `groupLabel` is preserved (not overwritten with environment name) | |
| 4 | Happy path | Parameter has no `userInterface` field | `userInterface` is created with auto-detected `control` and environment name as `groupLabel` | |
| 5 | Happy path | Queue has no environments | Returns empty list | |
| 6 | Error handling | Same parameter name appears in two environments with different definitions | Returns error listing the mismatched fields | |
| 7 | Happy path | Same parameter name appears in two environments with identical definitions | No error; parameter appears once in result | Deduplication by name |
| 8 | Pagination/batching | `ListQueueEnvironments` returns multiple pages | All environments are fetched and processed | Uses paginated list helper |
| 9 | Happy path | Each environment's full template is fetched via `GetQueueEnvironment` | Template is parsed as YAML to extract `parameterDefinitions` | |
| 10 | Happy path | Environment template has no `parameterDefinitions` field | That environment contributes no parameters | |

---

## Section 9: API — queue credentials (assume role)

> **Rust crate:** `deadline-api` · **Module:** `api`
>
> **Logic under test:** Thin wrappers around `AssumeQueueRoleForUser` and
> `AssumeQueueRoleForRead` Deadline API calls. These return temporary credentials
> and propagate AWS service errors directly.

### `assume_queue_role_for_user(farmId, queueId, config?) -> credentials response`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 1 | Happy path | Valid farm and queue IDs | Returns response with `credentials` containing `accessKeyId`, `secretAccessKey`, `sessionToken`, `expiration` | |
| 2 | Auth/credential states | User lacks permission to assume queue role | Propagates AWS service error | |
| 3 | Error handling | Invalid farm or queue ID | Propagates AWS service error (e.g., resource not found) | |
| 4 | Config interaction | Explicit config override | Uses that config for client creation | |

### `assume_queue_role_for_read(farmId, queueId, config?) -> credentials response`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 5 | Happy path | Valid farm and queue IDs | Returns response with read-only `credentials` | |
| 6 | Auth/credential states | User lacks permission | Propagates AWS service error | |
| 7 | Error handling | Invalid farm or queue ID | Propagates AWS service error | |
| 8 | Config interaction | Explicit config override | Uses that config for client creation | |

---

## Section 10: API — storage profile for queue

> **Rust crate:** `deadline-api` · **Module:** `api`
>
> **Logic under test:** Fetching a storage profile from the Deadline API and mapping
> the response into a `StorageProfile` struct with typed `fileSystemLocations` and
> a case-insensitive `osFamily` enum.

### `get_storage_profile_for_queue(farm_id, queue_id, storage_profile_id, deadline_api?, config?) -> StorageProfile`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 1 | Happy path | Valid IDs, response has file system locations | Returns `StorageProfile` with `storageProfileId`, `displayName`, `osFamily`, and `fileSystemLocations` list | |
| 2 | Happy path | Response has no `fileSystemLocations` key | Returns `StorageProfile` with empty `fileSystemLocations` list | |
| 3 | Happy path | `deadline_api` is not provided | Creates a new Deadline client internally | |
| 4 | Happy path | `deadline_api` is provided | Uses the provided client directly | |
| 5 | Happy path | `osFamily` value is case-insensitive (e.g., `"WINDOWS"` or `"Windows"`) | Correctly parsed into `StorageProfileOperatingSystemFamily` enum | Enum supports case-insensitive lookup |

---
