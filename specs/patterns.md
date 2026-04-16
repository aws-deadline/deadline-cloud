# Coding Patterns

Patterns and conventions for writing code in this workspace. Read this
before implementing API calls or touching `deadline-api`.

## AWS SDK for Rust Usage

The Rust SDK (`aws-sdk-deadline`) differs from Python's boto3 in ways
that affect how we build API functions. Know these before writing code.

### SDK types don't implement `Serialize`

The SDK output types (`GetFarmOutput`, `GetQueueOutput`, etc.) and their
nested types (`JobAttachmentSettings`, `FleetConfiguration`, etc.) do
**not** implement `serde::Serialize`. You cannot call
`serde_json::to_value(resp)` to get JSON. This is a known limitation
of the AWS SDK for Rust (open since 2021, see
[awslabs/aws-sdk-rust#269](https://github.com/awslabs/aws-sdk-rust/issues/269)).
Other Rust cloud SDKs (Azure, Google via prost) do provide serde support.

### Standard pattern: `ResponseBodyCapture` for all API calls

**All** API functions in `api.rs` use the `ResponseBodyCapture`
interceptor to capture the raw HTTP response body as
`serde_json::Value`. This is the single, consistent approach for every
API call — `get_*`, `list_*`, and `search_*` alike.

```rust
let capture = ResponseBodyCapture::new();
client.get_farm().farm_id(id)
    .customize().interceptor(capture.clone()).send().await?;
let json = capture.json()?;  // full response as serde_json::Value
```

The interceptor post-processes the JSON to convert datetime strings to
Python format and remove null values. New API fields appear automatically
without code changes.

**Why not typed SDK output structs?** The SDK output types can't be
serialized back to JSON/YAML. Using them requires manually extracting
every field with accessor methods and rebuilding JSON — tedious for
`get_*` commands with 20+ fields and nested structs, and inconsistent
if some functions use typed extraction while others use raw JSON. The
`ResponseBodyCapture` approach matches Python/boto3 behavior (responses
are raw dicts) and scales uniformly across all API shapes.

### Pagination with `ResponseBodyCapture`

The SDK's built-in paginators (`.into_paginator()`) don't support
`.customize().interceptor()`, so paginated `list_*` functions use manual
`nextToken` loops with the interceptor on each page:

```rust
let mut all_items = Vec::new();
let mut next_token: Option<String> = None;
loop {
    let capture = ResponseBodyCapture::new();
    let mut req = client.list_farms();
    if let Some(ref uid) = user_id { req = req.principal_id(uid.as_str()); }
    if let Some(t) = next_token.take() { req = req.next_token(t); }
    req.customize().interceptor(capture.clone()).send().await.map_err(sdk_err)?;
    let page = capture.json()?;
    if let Some(items) = page["farms"].as_array() {
        all_items.extend(items.iter().cloned());
    }
    match page.get("nextToken").and_then(|t| t.as_str()) {
        Some(t) => next_token = Some(t.to_string()),
        None => break,
    }
}
```

This trades the paginator's convenience for consistency — every API
function follows the same `ResponseBodyCapture` pattern, and the CLI
layer only ever sees `serde_json::Value`.

### Search APIs (no pagination token)

`search_*` APIs use `itemOffset`/`pageSize` instead of `nextToken`.
They return a single page, so no loop is needed — just a single
`ResponseBodyCapture` call.

### Known differences from Python/boto3

The `ResponseBodyCapture` approach captures the raw HTTP JSON, which
differs from Python/boto3 in several cosmetic ways. These are accepted
differences, verified by comparing `deadline` (Python) vs
`./target/debug/deadline` (Rust) against the real API using `ada`
credentials.

- **Field order:** Raw API response order may differ from boto3, which
  reorders fields based on its Smithy service model.
- **Extra fields:** The raw response may include fields (e.g. `arn`)
  that boto3 strips based on its service model.

Both field-order and extra-field differences stem from the AWS SDK for
Rust not implementing `serde::Serialize` on output types
([awslabs/aws-sdk-rust#269](https://github.com/awslabs/aws-sdk-rust/issues/269),
open since 2021), which prevents using typed extraction. A future Smithy
model filtering step could eliminate both — see
`specs/api/architecture.md` § "Future Improvements".
- **Float precision:** `serde_json` parses JSON `1.0` as integer `1`
  when there is no fractional part. Python preserves `1.0`. Affects
  fields like `costScaleFactor`. Accepted difference — `serde_json`
  behavior is standard; post-processing would add complexity for
  minimal user impact.

### DateTime format: display vs machine-readable

The `ResponseBodyCapture` interceptor converts all datetime strings to
Python display format (space separator: `2024-12-18 00:37:38+00:00`).
This matches Python's `str(datetime)` and is correct for YAML/display
output.

However, some output paths require RFC 3339 format (T separator:
`2024-12-18T00:37:38+00:00`). Notably, `credential_process` JSON
requires RFC 3339 per the AWS SDK spec
(https://docs.aws.amazon.com/sdkref/latest/guide/feature-process-credentials.html).
Python uses `datetime.isoformat()` for these paths. In Rust, the CLI
must convert the space back to `T` when producing machine-readable
timestamps. Known paths requiring RFC 3339:

- `queue export-credentials` → `Expiration` field
- Queue credential provider `expiry_time` (not yet implemented)
- Job submission datetime fields (not yet implemented)
- **Fractional second precision:** The API returns milliseconds (e.g.
  `22:35:01.624Z`). boto3 parses into Python `datetime(microsecond=624000)`,
  and `str()` always displays 6 digits (`.624000`). We preserve the
  API's original precision (`.624`). The trailing zeros are a Python
  display artifact — the API wire format was verified to send `.624Z`,
  not `.624000Z`.

### DateTime formatting

The `ResponseBodyCapture` interceptor converts ISO 8601 datetime strings
to Python/boto3 format: `2024-12-18T00:37:38Z` → `2024-12-18 00:37:38+00:00`.
Fractional seconds are preserved as-is from the API response.

### Error formatting

The SDK's `SdkError` `Display` impl just says `"service error"` — it
never includes the actual error code or message. Always use a helper
that extracts the error code and message via `ProvideErrorMetadata`:

- `api.rs::format_sdk_error` — generic, works for any AWS SDK error.
  Uses `aws_smithy_types::error::metadata::ProvideErrorMetadata`, the
  common trait all SDK crates re-export.
- `api.rs::sdk_err` — wraps `format_sdk_error` into `DeadlineError`
  for Deadline API calls.
- `log_retrieval.rs::cw_sdk_err` — same pattern for CloudWatch Logs.
- `s3.rs::format_sts_sdk_err` — same pattern for STS.

Output format: `"AccessDeniedException: User is not authorized..."`.
This is needed for `suggest_resources_on_client_error` to detect error
types, and for users to understand what went wrong.

**Never use `format!("{e}")` on an `SdkError`.** It produces `"service
error"` which is useless to the user and breaks error-type detection.

## Credential Scoping for Non-Deadline AWS Services

Operations that access AWS services other than the Deadline Cloud API
(CloudWatch Logs, S3) often require queue-scoped or fleet-scoped
credentials. The base SDK config (from the user's AWS profile) may not
have permission to access these resources — the queue or fleet role
grants that access.

**Rule:** When building a non-Deadline AWS client (CloudWatch Logs, S3)
for a queue-scoped or fleet-scoped resource, check whether the user is
logged in via Deadline Cloud Monitor (DCM). If so, use scoped
credentials. If not, use the base session.

### Pattern: queue-scoped credentials

Used by: `get_session_logs` (CloudWatch Logs), `attachment download/upload`
(S3), `manifest download/upload` (S3), `bundle submit` (S3),
`job download-output` (S3), `queue sync-output` (S3).

```rust
// In session.rs — shared helper
pub async fn get_queue_scoped_config(
    farm_id: &str,
    queue_id: &str,
    config: Option<&IniConfig>,
) -> Result<SdkConfig, DeadlineError>
```

Checks `auth::get_user_and_identity_store_id(config)`. If the user has
a DCM login (both `user_id` and `identity_store_id` are `Some`), calls
`get_queue_user_config` to assume the queue role. Otherwise returns the
base SDK config. If queue role assumption fails for a DCM user, the
error is propagated (matching Python, which raises
`DeadlineOperationError`).

### Pattern: fleet-scoped credentials

Used by: `get_worker_logs` (CloudWatch Logs).

Fleet credentials use `assume_fleet_role_for_read` (a one-shot API call
returning temporary credentials), not a cached credential provider.
Build a temporary `SdkConfig` from the returned credentials for the
CloudWatch Logs client.

### Why not always use scoped credentials?

Non-DCM users (e.g. IAM users with direct permissions, SSO profiles
without DCM) may already have the necessary permissions on their base
credentials. Queue/fleet role assumption would fail for these users.
The DCM check gates the credential flow correctly.

## Serialization: API Responses vs Owned File Formats

Two distinct patterns exist for JSON serialization in this codebase.
Choose based on who controls the schema.

### API responses → `serde_json::Value`

API responses use `ResponseBodyCapture` to capture raw JSON as
`serde_json::Value`. This is necessary because AWS SDK output types
don't implement `serde::Serialize` (see above). The CLI layer formats
and prints `Value` directly. Model types that parse API responses use
manual `from_json(&Value)` methods (e.g. `StorageProfile::from_json`).

**Use for:** anything that comes from or goes to an AWS API.

### Owned file formats → `#[derive(Serialize, Deserialize)]`

For file formats we control (checkpoint files, state persistence,
config structures we write and read back), use serde derives:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MyState {
    pub required_field: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub optional_field: Option<String>,
    #[serde(default)]
    pub defaulted_field: Vec<Item>,
}
```

This gives compile-time field validation, automatic error messages for
missing/malformed fields, and eliminates manual JSON object construction.
The schema is under our control so there's no SDK compatibility concern.

**Use for:** checkpoint files, incremental download state, any
persistent data format owned by this codebase.
