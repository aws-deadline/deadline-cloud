# New Features — Test Specification

> Part of [AWS Deadline Cloud Client — Behavioral Test Specification](index.md)
>
> Covers work items: #19 (Update checker), #20 (Batch get API), #21 (Bug-fix parity)
> Python source: `deadline.client.api._update_checker`, `deadline.client.cli._groups._batch_get`

---

## Section 59: Update checker (#19)

> **Rust crate:** `deadline-api` · **Module:** `update_checker` (new)
>
> **Logic under test:** Fetching remote version manifest, comparing versions,
> returning structured result. Python source: `_update_checker.py`.

### `get_current_platform()`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 1 | Happy path | macOS | Returns `"macos"` | |
| 2 | Happy path | Linux | Returns `"linux"` | |
| 3 | Happy path | Windows | Returns `"windows"` | |

### `safe_check_for_updates(integration_name, current_version)`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 4 | Happy path | Newer version available in manifest | `update_available=true`, `latest_version` set, `download_url` set | |
| 5 | Happy path | Current version is latest | `update_available=false` | |
| 6 | Happy path | Current version is newer than manifest | `update_available=false` | |
| 7 | Error handling | Network timeout | `status=TIMEOUT_ERROR`, `update_available=false` | |
| 8 | Error handling | Network unreachable | `status=NETWORK_ERROR`, `update_available=false` | |
| 9 | Error handling | Manifest JSON is malformed | `status=PARSE_ERROR`, `update_available=false` | |
| 10 | Error handling | Integration not found in manifest | `status=INTEGRATION_NOT_FOUND`, `update_available=false` | |
| 11 | Error handling | Current version string is invalid | `status=INVALID_VERSION`, `update_available=false` | |
| 12 | Happy path | Manifest has platform-specific entries | Correct platform entry selected | |

---

## Section 60: Batch get API helper (#20)

> **Rust crate:** `deadline-api` · **Module:** `batch_get` (new)
>
> **Logic under test:** Chunking large ID lists into batches of 100,
> collecting successful results, retrying transient per-item errors
> with exponential backoff. Python source: `_batch_get.py`.

### `batch_get(identifiers, operation, ...)`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 13 | Happy path | All items succeed in single batch (≤100) | All results returned | |
| 14 | Happy path | >100 items, chunked into multiple batches | All results returned, correct chunking | |
| 15 | Error handling | Terminal per-item error (ResourceNotFoundException) | Error collected, not retried | |
| 16 | Happy path | Transient per-item error (ThrottlingException), recovers on retry | Item eventually succeeds | |
| 17 | Error handling | Transient error exhausts all retries | Error collected after max retries | |
| 18 | Happy path | Mixed success and terminal errors in one batch | Successes collected, terminal errors reported | |
| 19 | Happy path | Empty input list | Returns empty results | |

---

## Section 61: Python bug-fix parity (#21)

> **Rust crate:** various · **Test level:** Level 2 (CLI subprocess)
>
> **Logic under test:** Verify Rust handles edge cases fixed in recent
> Python PRs. Each case maps to a specific Python fix.

### Known paths corruption (Python PR #1098)

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 20 | Happy path | `settings.known_asset_paths` with OS path-list separator | Paths split correctly, not corrupted | Verify `:` on Unix, `;` on Windows |
| 21 | Happy path | External tool writes mixed-case keys to config | Keys read back correctly (case-insensitive) | |

### STS/S3 endpoint URL overrides (Python PR #1005)

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 22 | Happy path | `AWS_ENDPOINT_URL_STS` env var set | STS client uses override URL | |
| 23 | Happy path | `AWS_ENDPOINT_URL_S3` env var set | S3 client uses override URL | |

### handle-web-url PATH lookup (Python PR #1013)

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 24 | Happy path | `deadline` binary found via PATH on Linux | Install uses PATH-resolved binary | |

### Hidden parameters with empty defaults (Python PR #1032)

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 25 | Happy path | Job parameter with `userInterface.control=HIDDEN` and `default=""` | Parameter accepted, empty string used | |

### Missing newline with no attachments (Python PR #1008)

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 26 | Happy path | `bundle submit` with no job attachments | Output has proper newline formatting | |

---

## Section 62: New config settings

> **Rust crate:** `deadline-config` · **Module:** `settings`
>
> **Logic under test:** New settings added for hooks and update checker.

### Settings definitions

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 27 | Happy path | `settings.allow_bundle_hooks` defaults to `"false"` | `get_setting` returns `"false"` | |
| 28 | Happy path | `settings.allow_environment_hooks` defaults to `"false"` | `get_setting` returns `"false"` | |
| 29 | Happy path | `settings.submitter_update_notification` defaults to `"true"` | `get_setting` returns `"true"` | |
| 30 | Happy path | Set and read back each new setting | Round-trip preserves value | |
