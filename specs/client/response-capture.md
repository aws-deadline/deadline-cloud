# ResponseBodyCapture

## Overview

`raw_response.rs` implements an AWS SDK interceptor that captures the raw
HTTP response body as `serde_json::Value`. This is necessary because the
Rust SDK output types don't implement `Serialize`.

## How It Works

The interceptor implements the SDK's `Intercept` trait, hooking into
`read_after_deserialization`. At this point the SDK has buffered the full
HTTP response body. The interceptor reads the body bytes, parses as JSON,
applies post-processing, and stores the result in an `Arc<Mutex<Option<Value>>>`.

Every API call in `api.rs` attaches the interceptor:
```
client.list_farms()
    .customize()
    .interceptor(capture.clone())
    .send()
    .await?;
let json: Value = capture.json()?;
```

The `Arc<Mutex>` makes it safe to clone the interceptor (required by the SDK)
and read the result after the call completes.

## Post-Processing

The interceptor applies two transformations to the captured JSON:

1. **DateTime conversion** — ISO 8601 strings (`2024-12-18T00:37:38Z`) are
   converted to Python/boto3 display format (`2024-12-18 00:37:38+00:00`).
   `T` replaced with space, `Z` replaced with `+00:00`.

2. **Null removal** — `null` values are stripped from the response. Matches
   boto3 behavior where absent fields don't appear in the response dict.

## Known Differences from Python/boto3

- **Fractional seconds:** API precision preserved (`.624`), not padded to
  microseconds (`.624000`) as Python `datetime.__str__()` does.
- **Field order:** Raw API order, not Smithy model order.
- **Extra fields:** Raw API may include fields (e.g., `arn`) that boto3 strips.
- **Float precision:** `serde_json` parses `1.0` as integer `1`.
