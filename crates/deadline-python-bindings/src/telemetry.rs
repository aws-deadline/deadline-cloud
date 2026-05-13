use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::collections::HashMap;

use crate::DeadlineOperationError;

/// Python-owned telemetry client. Automatically flushes on drop.
#[pyclass]
pub struct TelemetryClient {
    inner: Option<deadline_api::telemetry::TelemetryClient>,
}

#[pymethods]
impl TelemetryClient {
    #[new]
    #[pyo3(signature = (config_path=None))]
    // PyO3 #[new] requires PyResult even when construction is infallible.
    #[allow(
        clippy::unnecessary_wraps,
        reason = "PyO3 requires PyResult for __new__"
    )]
    fn new(config_path: Option<&str>) -> PyResult<Self> {
        let config = match config_path {
            Some(p) => deadline_config::config_file::read_config_from(std::path::Path::new(p))
                .unwrap_or_else(|_| deadline_config::ini::IniConfig::new()),
            None => deadline_config::config_file::read_config()
                .unwrap_or_else(|_| deadline_config::ini::IniConfig::new()),
        };
        let client = deadline_api::telemetry::create_telemetry(&config);
        Ok(Self {
            inner: Some(client),
        })
    }

    /// Record a telemetry event.
    #[pyo3(signature = (event_type, details, *, from_gui=false))]
    fn record_event(
        &self,
        event_type: &str,
        details: &Bound<'_, PyDict>,
        from_gui: bool,
    ) -> PyResult<()> {
        let client = self
            .inner
            .as_ref()
            .ok_or_else(|| DeadlineOperationError::new_err("telemetry client is closed"))?;
        let mut map = HashMap::new();
        for (key, value) in details.iter() {
            let k: String = key.extract()?;
            let json_val = python_to_json_value(&value)?;
            map.insert(k, json_val);
        }
        client.record_event(event_type, map, from_gui);
        Ok(())
    }

    /// Record an error telemetry event.
    #[pyo3(signature = (event_details, exception_type, from_gui=false))]
    fn record_error(
        &self,
        event_details: &Bound<'_, PyDict>,
        exception_type: &str,
        from_gui: bool,
    ) -> PyResult<()> {
        let client = self
            .inner
            .as_ref()
            .ok_or_else(|| DeadlineOperationError::new_err("telemetry client is closed"))?;
        let mut map = HashMap::new();
        for (key, value) in event_details.iter() {
            let k: String = key.extract()?;
            let json_val = python_to_json_value(&value)?;
            map.insert(k, json_val);
        }
        map.insert(
            "exception_type".into(),
            serde_json::Value::String(exception_type.to_owned()),
        );
        client.record_event("com.amazon.rum.deadline.error", map, from_gui);
        Ok(())
    }

    /// Update common details merged into every future telemetry event.
    fn update_common_details(&mut self, details: &Bound<'_, PyDict>) -> PyResult<()> {
        let client = self
            .inner
            .as_mut()
            .ok_or_else(|| DeadlineOperationError::new_err("telemetry client is closed"))?;
        let mut map = HashMap::new();
        for (key, value) in details.iter() {
            let k: String = key.extract()?;
            let json_val = python_to_json_value(&value)?;
            map.insert(k, json_val);
        }
        client.update_common_details(map);
        Ok(())
    }

    /// Close the client, flushing any pending events.
    fn close(&mut self) {
        self.inner.take(); // Drop flushes
    }
}

/// Convert a Python object to `serde_json::Value` (simple types only).
fn python_to_json_value(obj: &Bound<'_, PyAny>) -> PyResult<serde_json::Value> {
    if obj.is_none() {
        Ok(serde_json::Value::Null)
    } else if let Ok(b) = obj.extract::<bool>() {
        Ok(serde_json::Value::Bool(b))
    } else if let Ok(i) = obj.extract::<i64>() {
        Ok(serde_json::json!(i))
    } else if let Ok(f) = obj.extract::<f64>() {
        Ok(serde_json::json!(f))
    } else if let Ok(s) = obj.extract::<String>() {
        Ok(serde_json::Value::String(s))
    } else {
        // Fallback: convert via str()
        let s: String = obj.str()?.extract()?;
        Ok(serde_json::Value::String(s))
    }
}
