# Coding Patterns

Patterns and conventions for writing code in this workspace. Read this
before implementing API calls or touching `deadline-client`.

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
`docs/crate_specs/deadline-client.md` § "Future Improvements".
- **Float precision:** `serde_json` parses JSON `1.0` as integer `1`
  when there is no fractional part. Python preserves `1.0`. Affects
  fields like `costScaleFactor`. **Action item:** investigate
  `serde_json` float preservation or post-processing.

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
- Queue credential provider `expiry_time` (§5, not yet implemented)
- Job submission datetime fields (§11, not yet implemented)
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

The SDK's `SdkError` `Display` impl just says `"service error"`. Use
the `sdk_err` helper in `api.rs` which extracts the error code and
message: `"AccessDeniedException: User is not authorized..."`. This is
needed for `suggest_resources_on_client_error` to detect error types.
