# ResponseBodyCapture

> **⚠️ DEPRECATED — being removed in D5e. Do not use for new code.**
>
> All new API calls should use typed SDK output directly. See
> `specs/patterns.md` § "AWS SDK for Rust Usage" for the current pattern.
> This document is retained as historical context for the remaining legacy
> wrappers in `api.rs` that still use this interceptor.

## Overview

`raw_response.rs` implements an AWS SDK interceptor that captures the raw
HTTP response body as `serde_json::Value`. This is necessary because the
Rust SDK output types don't implement `Serialize`
([awslabs/aws-sdk-rust#269](https://github.com/awslabs/aws-sdk-rust/issues/269),
open since 2021). Other Rust cloud SDKs (Azure, Google via prost) do
provide serde support.

**Why not typed SDK output structs?** Using them requires manually
extracting every field with accessor methods and rebuilding JSON —
tedious for `get_*` commands with 20+ fields and nested structs, and
inconsistent if some functions use typed extraction while others use raw
JSON. The `ResponseBodyCapture` approach matches Python/boto3 behavior
(responses are raw dicts) and scales uniformly across all API shapes.

## How It Works

The interceptor implements the SDK's `Intercept` trait, hooking into
`read_after_deserialization`. At this point the SDK has buffered the full
HTTP response body. The interceptor reads the body bytes, parses as JSON,
applies post-processing, and stores the result in an `Arc<Mutex<Option<Value>>>`.

Every API call in `api.rs` attaches the interceptor:
```rust
let capture = ResponseBodyCapture::new();
client.get_farm().farm_id(id)
    .customize().interceptor(capture.clone()).send().await?;
let json = capture.json()?;  // full response as serde_json::Value
```

The `Arc<Mutex>` makes it safe to clone the interceptor (required by the SDK)
and read the result after the call completes.

## Post-Processing

The interceptor applies two transformations to the captured JSON:

1. **DateTime conversion** — ISO 8601 strings (`2024-12-18T00:37:38Z`) are
   converted to Python/boto3 display format (`2024-12-18 00:37:38+00:00`).
   `T` replaced with space, `Z` replaced with `+00:00`. Fractional seconds
   are preserved as-is from the API response.

2. **Null removal** — `null` values are stripped from the response. Matches
   boto3 behavior where absent fields don't appear in the response dict.

## Pagination

The SDK's built-in paginators (`.into_paginator()`) don't support
`.customize().interceptor()`, so paginated `list_*` functions use manual
`nextToken` loops with the interceptor on each page:

```rust
let mut all_items = Vec::new();
let mut next_token: Option<String> = None;
loop {
    let capture = ResponseBodyCapture::new();
    let mut req = client.list_farms();
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

A shared `paginated_list` helper in `api.rs` eliminates duplicated loop logic.

`search_*` APIs use `itemOffset`/`pageSize` instead of `nextToken`.
They return a single page, so no loop is needed — just a single
`ResponseBodyCapture` call.

## DateTime: Display vs Machine-Readable

The interceptor converts all datetime strings to Python display format
(space separator: `2024-12-18 00:37:38+00:00`). This is correct for
YAML/display output.

However, some output paths require RFC 3339 format (T separator:
`2024-12-18T00:37:38+00:00`). The CLI must convert the space back to
`T` for these paths. Known paths requiring RFC 3339:

- `queue export-credentials` → `Expiration` field
- Queue credential provider `expiry_time` (not yet implemented)
- Job submission datetime fields (not yet implemented)

## Known Differences from Python/boto3

- **Fractional seconds:** API precision preserved (`.624`), not padded to
  microseconds (`.624000`) as Python `datetime.__str__()` does. The
  trailing zeros are a Python display artifact.
- **Field order:** Raw API order, not Smithy model order.
- **Extra fields:** Raw API may include fields (e.g., `arn`) that boto3
  strips based on its service model.
- **Float precision:** `serde_json` parses JSON `1.0` as integer `1`.

Both field-order and extra-field differences stem from the SDK Serialize
limitation. A future Smithy model filtering step could eliminate both —
see `architecture.md` § "Future Improvements".
