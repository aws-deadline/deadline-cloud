//! Capture raw JSON response body from AWS SDK operations.
//!
//! SDK output types don't implement Serialize, so for all API calls we
//! intercept the raw HTTP response body after deserialization (when the
//! SDK has buffered it) and parse it as serde_json::Value. This matches
//! Python/boto3 behavior where responses are raw dicts.
//!
//! DateTime strings are converted from ISO 8601 (`2024-12-18T00:37:38Z`)
//! to Python/boto3 format (`2024-12-18 00:37:38+00:00`).
//!
//! ## Known differences from Python/boto3
//!
//! - **Fractional second precision:** The API returns milliseconds (e.g.
//!   `.624Z`). boto3 parses this into a Python `datetime` with microseconds
//!   (`624000`), and `datetime.__str__()` always displays 6 fractional
//!   digits (`.624000`). We preserve the API's original precision (`.624`).
//!   The values are identical — the trailing zeros are a Python display
//!   artifact, not additional precision from the API.
//!
//! - **Field order:** Raw API response order may differ from Python/boto3,
//!   which reorders fields based on its Smithy service model.
//!
//! - **Extra fields:** The raw API response may include fields (e.g. `arn`)
//!   that boto3 strips based on its service model.
//!
//! - **Float precision:** `serde_json` parses JSON `1.0` as integer `1`
//!   when there is no fractional part. Python preserves `1.0`. This
//!   affects fields like `costScaleFactor`.

use aws_sdk_deadline::config::interceptors::AfterDeserializationInterceptorContextRef;
use aws_sdk_deadline::config::{ConfigBag, Intercept, RuntimeComponents};
use serde_json::Value;
use std::sync::{Arc, Mutex};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Interceptor that captures the raw HTTP response body bytes.
/// Attach via `.customize().interceptor(capture.clone()).send()`.
/// Read the captured JSON after `.send()` completes via `.json()`.
#[derive(Debug, Clone)]
pub struct ResponseBodyCapture {
    bytes: Arc<Mutex<Vec<u8>>>,
}

impl Default for ResponseBodyCapture {
    fn default() -> Self {
        Self::new()
    }
}

impl ResponseBodyCapture {
    pub fn new() -> Self {
        Self { bytes: Arc::new(Mutex::new(Vec::new())) }
    }

    /// Parse the captured bytes as JSON with datetime strings converted
    /// to match Python/boto3 format and null values removed.
    pub fn json(&self) -> Result<Value, serde_json::Error> {
        let mut val: Value = serde_json::from_slice(&self.bytes.lock().unwrap())?;
        convert_datetimes(&mut val);
        remove_nulls(&mut val);
        Ok(val)
    }
}

impl Intercept for ResponseBodyCapture {
    fn name(&self) -> &'static str { "ResponseBodyCapture" }

    fn read_after_deserialization(
        &self,
        context: &AfterDeserializationInterceptorContextRef<'_>,
        _runtime_components: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        if let Some(bytes) = context.response().body().bytes() {
            *self.bytes.lock().unwrap() = bytes.to_vec();
        }
        Ok(())
    }
}

/// Walk a JSON value and convert ISO 8601 datetime strings to Python format.
/// "2024-12-18T00:37:38Z" → "2024-12-18 00:37:38+00:00"
fn convert_datetimes(val: &mut Value) {
    match val {
        Value::String(s) => {
            if looks_like_datetime(s) {
                *s = s.replace('T', " ").replace('Z', "+00:00");
            }
        }
        Value::Object(map) => {
            for v in map.values_mut() {
                convert_datetimes(v);
            }
        }
        Value::Array(arr) => {
            for v in arr.iter_mut() {
                convert_datetimes(v);
            }
        }
        _ => {}
    }
}

/// Heuristic: does this string look like an ISO 8601 datetime?
/// Matches patterns like "2024-12-18T00:37:38Z" or "2024-12-18T00:37:38.123Z"
fn looks_like_datetime(s: &str) -> bool {
    s.len() >= 20 && s.as_bytes().get(4) == Some(&b'-') && s.as_bytes().get(10) == Some(&b'T') && s.ends_with('Z')
}

/// Remove null values from JSON objects (recursive).
/// Matches Python/boto3 behavior where None values are omitted.
fn remove_nulls(val: &mut Value) {
    match val {
        Value::Object(map) => {
            map.retain(|_, v| !v.is_null());
            for v in map.values_mut() {
                remove_nulls(v);
            }
        }
        Value::Array(arr) => {
            for v in arr.iter_mut() {
                remove_nulls(v);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use test_case::test_case;

    #[test_case("2024-12-18T00:37:38Z", "2024-12-18 00:37:38+00:00" ; "no fractional seconds")]
    #[test_case("2023-01-27T07:37:53.624Z", "2023-01-27 07:37:53.624+00:00" ; "fractional seconds preserved")]
    fn convert_datetime_string(input: &str, expected: &str) {
        let mut val = json!(input);
        convert_datetimes(&mut val);
        assert_eq!(val.as_str().unwrap(), expected);
    }

    // Python/boto3 pads fractional seconds to 6 digits (e.g. .624000) because
    // datetime objects always display microsecond precision. We preserve the
    // API's original precision (.624) since the values are identical.
    // This is an accepted cosmetic difference.

    #[test_case("not-a-date" ; "plain string")]
    #[test_case("2024-12-18" ; "date only")]
    #[test_case("farm-abc123" ; "resource id")]
    fn non_datetime_string_unchanged(input: &str) {
        let mut val = json!(input);
        convert_datetimes(&mut val);
        assert_eq!(val.as_str().unwrap(), input);
    }

    #[test]
    fn convert_datetimes_in_nested_object() {
        let mut val = json!({
            "farmId": "farm-abc",
            "createdAt": "2024-12-18T00:37:38Z",
            "jobs": [{"startedAt": "2023-01-27T07:37:53.624Z"}]
        });
        convert_datetimes(&mut val);
        assert_eq!(val["createdAt"], "2024-12-18 00:37:38+00:00");
        assert_eq!(val["jobs"][0]["startedAt"], "2023-01-27 07:37:53.624+00:00");
    }

    #[test]
    fn remove_nulls_from_object() {
        let mut val = json!({"a": "keep", "b": null, "c": {"d": null, "e": "keep"}});
        remove_nulls(&mut val);
        assert_eq!(val, json!({"a": "keep", "c": {"e": "keep"}}));
    }
}
