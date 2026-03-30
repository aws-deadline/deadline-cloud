//! Telemetry client — background event sender for Deadline Cloud.
//!
//! Matches Python's `_telemetry.py`: std::thread + std::sync::mpsc + ureq.
//! No async, no tokio — just a background OS thread doing blocking HTTP.

use deadline_config::config_file;
use deadline_config::ini::IniConfig;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::mpsc::{self, SyncSender};
use std::thread;
use uuid::Uuid;

const MAX_QUEUE_SIZE: usize = 25;
const BASE_TIME: f64 = 0.5;
const MAX_BACKOFF_SECONDS: f64 = 10.0;
const MAX_RETRY_ATTEMPTS: u32 = 4;
const ENDPOINT_PREFIX: &str = "management.";

pub struct TelemetryEvent {
    pub event_type: String,
    pub event_details: HashMap<String, Value>,
}

pub struct TelemetryClient {
    sender: Option<SyncSender<TelemetryEvent>>,
    thread_handle: Option<thread::JoinHandle<()>>,
    initialized: bool,
    opted_out: bool,
    session_id: String,
    telemetry_id: String,
    common_details: HashMap<String, Value>,
    system_metadata: HashMap<String, Value>,
}

impl TelemetryClient {
    pub fn new(
        package_name: &str,
        package_ver: &str,
        config: Option<&IniConfig>,
    ) -> Self {
        let ver = truncate_version(package_ver);
        let telemetry_id = get_or_create_identifier(config);
        let mut common_details = HashMap::new();
        if package_name != "deadline-cloud-library" {
            common_details.insert(
                "deadline-cloud-version".into(),
                Value::String(env!("CARGO_PKG_VERSION").to_string()),
            );
        }

        let mut client = Self {
            sender: None,
            thread_handle: None,
            initialized: false,
            opted_out: false,
            session_id: Uuid::new_v4().to_string(),
            telemetry_id,
            common_details,
            system_metadata: build_system_metadata(package_name, &ver),
        };
        client.opted_out = resolve_opt_out(config);
        client
    }

    /// Start the background sender thread. Call after AWS config is available.
    pub fn initialize(&mut self, endpoint_url: &str, config: Option<&IniConfig>) {
        if self.opted_out {
            return;
        }

        let endpoint = prefix_endpoint(
            &format!("{endpoint_url}/2023-10-12/telemetry"),
            ENDPOINT_PREFIX,
        );

        // Add user_id and monitor_id if available
        if let Some(c) = config {
            if let Ok(uid) = config_file::get_setting_with_config("defaults.user_id", c) {
                if !uid.is_empty() {
                    self.system_metadata.insert("user_id".into(), Value::String(uid));
                }
            }
        }

        let (tx, rx) = mpsc::sync_channel::<TelemetryEvent>(MAX_QUEUE_SIZE);
        let session_id = self.session_id.clone();
        let telemetry_id = self.telemetry_id.clone();
        let system_metadata = self.system_metadata.clone();

        let handle = thread::spawn(move || {
            process_event_queue(rx, &endpoint, &session_id, &telemetry_id, &system_metadata);
        });

        self.sender = Some(tx);
        self.thread_handle = Some(handle);
        self.initialized = true;
    }

    pub fn is_initialized(&self) -> bool {
        self.initialized
    }

    pub fn record_event(
        &self,
        event_type: &str,
        mut event_details: HashMap<String, Value>,
        from_gui: bool,
    ) {
        if !self.initialized || self.opted_out {
            return;
        }
        for (k, v) in &self.common_details {
            event_details.entry(k.clone()).or_insert_with(|| v.clone());
        }
        event_details.insert(
            "usage_mode".into(),
            Value::String(if from_gui { "GUI" } else { "CLI" }.into()),
        );
        if let Some(ref tx) = self.sender {
            let _ = tx.try_send(TelemetryEvent {
                event_type: event_type.to_string(),
                event_details,
            });
        }
    }

    pub fn update_common_details(&mut self, details: HashMap<String, Value>) {
        self.common_details.extend(details);
    }
}

impl Drop for TelemetryClient {
    fn drop(&mut self) {
        // Drop the sender to signal the background thread to exit,
        // then join to ensure all queued events are sent.
        self.sender.take();
        if let Some(handle) = self.thread_handle.take() {
            let _ = handle.join();
        }
    }
}

// ---------------------------------------------------------------------------
// Background thread
// ---------------------------------------------------------------------------

fn process_event_queue(
    rx: mpsc::Receiver<TelemetryEvent>,
    endpoint: &str,
    session_id: &str,
    telemetry_id: &str,
    system_metadata: &HashMap<String, Value>,
) {
    while let Ok(event) = rx.recv() {
        let body = json!({
            "BatchId": Uuid::new_v4().to_string(),
            "RumEvents": [{
                "details": serde_json::to_string(&event.event_details).unwrap_or_default(),
                "id": Uuid::new_v4().to_string(),
                "metadata": serde_json::to_string(system_metadata).unwrap_or_default(),
                "timestamp": std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
                "type": event.event_type,
            }],
            "UserDetails": {
                "sessionId": session_id,
                "userId": telemetry_id,
            },
        });

        if let Err(e) = send_with_retry(endpoint, &body) {
            log::debug!("Telemetry send failed: {e}");
            return; // Stop processing on unrecoverable error (matches Python)
        }
    }
}

fn send_with_retry(endpoint: &str, body: &Value) -> Result<(), String> {
    let body_str = body.to_string();
    for attempt in 0..MAX_RETRY_ATTEMPTS {
        match ureq::post(endpoint)
            .header("Accept", "application-json")
            .header("Content-Type", "application-json")
            .send(body_str.as_bytes())
        {
            Ok(_) => return Ok(()),
            Err(ureq::Error::StatusCode(429)) | Err(ureq::Error::StatusCode(500)) => {
                if attempt + 1 >= MAX_RETRY_ATTEMPTS {
                    return Err("Max retries reached sending telemetry".into());
                }
                let backoff = rand_backoff(attempt + 1);
                thread::sleep(std::time::Duration::from_secs_f64(backoff));
            }
            Err(e) => return Err(e.to_string()),
        }
    }
    Err("Max retries reached".into())
}

fn rand_backoff(attempt: u32) -> f64 {
    let max = MAX_BACKOFF_SECONDS.min(BASE_TIME * 2.0_f64.powi(attempt as i32));
    // Simple pseudo-random: use thread ID + time as seed
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos() as f64;
    (nanos % 1000.0) / 1000.0 * max
}

// ---------------------------------------------------------------------------
// Helpers (public for testing)
// ---------------------------------------------------------------------------

/// Truncate version to first 3 components: "1.2.3.4.5" → "1.2.3"
pub fn truncate_version(ver: &str) -> String {
    ver.splitn(4, '.').take(3).collect::<Vec<_>>().join(".")
}

/// Insert prefix after "https://": "https://example.com" → "https://management.example.com"
pub fn prefix_endpoint(endpoint: &str, prefix: &str) -> String {
    if let Some(rest) = endpoint.strip_prefix("https://") {
        format!("https://{prefix}{rest}")
    } else {
        endpoint.to_string()
    }
}

/// Check opt-out: env var supersedes config.
pub fn resolve_opt_out(config: Option<&IniConfig>) -> bool {
    let env_val = std::env::var("DEADLINE_CLOUD_TELEMETRY_OPT_OUT").unwrap_or_default();
    if !env_val.is_empty() {
        return config_file::str2bool(&env_val).unwrap_or(false);
    }
    let val = match config {
        Some(c) => config_file::get_setting_with_config("telemetry.opt_out", c)
            .unwrap_or_default(),
        None => config_file::get_setting("telemetry.opt_out").unwrap_or_default(),
    };
    config_file::str2bool(&val).unwrap_or(false)
}

/// Validate existing identifier or generate a new UUID4.
pub fn validate_or_generate_identifier(existing: Option<&str>) -> String {
    if let Some(id) = existing {
        if Uuid::parse_str(id).is_ok() {
            return id.to_string();
        }
    }
    Uuid::new_v4().to_string()
}

fn get_or_create_identifier(config: Option<&IniConfig>) -> String {
    let existing = match config {
        Some(c) => config_file::get_setting_with_config("telemetry.identifier", c).ok(),
        None => config_file::get_setting("telemetry.identifier").ok(),
    };
    let id = validate_or_generate_identifier(existing.as_deref());
    if existing.as_deref() != Some(&id) {
        // Save the new identifier — best effort
        let _ = config_file::set_setting("telemetry.identifier", &id);
    }
    id
}

fn build_system_metadata(package_name: &str, package_ver: &str) -> HashMap<String, Value> {
    let mut m = HashMap::new();
    m.insert("service".into(), Value::String(package_name.into()));
    m.insert("version".into(), Value::String(package_ver.into()));
    m.insert("osName".into(), Value::String(
        if cfg!(target_os = "macos") { "macOS" }
        else if cfg!(target_os = "windows") { "Windows" }
        else { "Linux" }
        .into(),
    ));
    m
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_case::test_case;

    // --- Version truncation (pure function) ---

    #[test_case("1.2.3.4.5", "1.2.3" ; "five components truncated to three")]
    #[test_case("1.2.3", "1.2.3" ; "three components unchanged")]
    #[test_case("1.2", "1.2" ; "two components unchanged")]
    #[test_case("1", "1" ; "one component unchanged")]
    fn truncate_version_to_three_components(input: &str, expected: &str) {
        assert_eq!(truncate_version(input), expected);
    }

    // --- Endpoint prefixing (pure function) ---

    #[test_case(
        "https://deadline.us-west-2.amazonaws.com",
        "management.",
        "https://management.deadline.us-west-2.amazonaws.com"
        ; "inserts prefix after https"
    )]
    #[test_case(
        "http://localhost:8080",
        "management.",
        "http://localhost:8080"
        ; "non-https unchanged"
    )]
    fn prefix_endpoint_inserts_after_https(endpoint: &str, prefix: &str, expected: &str) {
        assert_eq!(prefix_endpoint(endpoint, prefix), expected);
    }

    // --- Telemetry identifier (pure function) ---

    #[test]
    fn valid_uuid4_identifier_is_preserved() {
        let id = "550e8400-e29b-41d4-a716-446655440000";
        assert_eq!(validate_or_generate_identifier(Some(id)), id);
    }

    #[test]
    fn invalid_identifier_generates_new_uuid4() {
        let result = validate_or_generate_identifier(Some("not-a-uuid"));
        assert_ne!(result, "not-a-uuid");
        Uuid::parse_str(&result).expect("should be valid UUID");
    }

    #[test]
    fn empty_identifier_generates_new_uuid4() {
        let result = validate_or_generate_identifier(None);
        Uuid::parse_str(&result).expect("should be valid UUID");
    }
}
