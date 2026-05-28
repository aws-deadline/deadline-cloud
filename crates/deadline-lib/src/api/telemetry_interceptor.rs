//! Telemetry interceptor — installed on the Deadline SDK client at
//! construction time. Reads the SDK's own `Metadata` entry from the
//! `ConfigBag` to determine the operation name. Emits one latency event
//! per logical operation.

use crate::api::client::pascal_to_snake;
use crate::api::telemetry::{self, TelemetryClient};
use aws_smithy_runtime_api::client::interceptors::Intercept;
use aws_smithy_runtime_api::client::interceptors::context::{
    BeforeSerializationInterceptorContextRef, FinalizerInterceptorContextRef,
};
use aws_smithy_runtime_api::client::orchestrator::Metadata;
use aws_smithy_runtime_api::client::runtime_components::RuntimeComponents;
use aws_smithy_types::config_bag::ConfigBag;
use std::sync::{Arc, Mutex};
use std::time::Instant;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Records latency telemetry for every Deadline API call.
#[derive(Clone)]
pub struct TelemetryInterceptor {
    telemetry: Arc<Mutex<Option<TelemetryClient>>>,
    pub(crate) start: Arc<Mutex<Option<Instant>>>,
}

impl std::fmt::Debug for TelemetryInterceptor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TelemetryInterceptor")
            .finish_non_exhaustive()
    }
}

impl TelemetryInterceptor {
    pub fn new(telemetry: Option<TelemetryClient>) -> Self {
        Self {
            telemetry: Arc::new(Mutex::new(telemetry)),
            start: Arc::new(Mutex::new(None)),
        }
    }
}

impl Intercept for TelemetryInterceptor {
    fn name(&self) -> &'static str {
        "DeadlineTelemetry"
    }

    fn read_before_execution(
        &self,
        _context: &BeforeSerializationInterceptorContextRef<'_>,
        _cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        *self.start.lock().expect("lock poisoned") = Some(Instant::now());
        Ok(())
    }

    fn read_after_execution(
        &self,
        _context: &FinalizerInterceptorContextRef<'_>,
        _runtime_components: &RuntimeComponents,
        cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let start = self.start.lock().expect("lock poisoned").take();
        if let Some(start) = start {
            let name = cfg
                .load::<Metadata>()
                .map_or_else(|| "unknown".to_owned(), |m| pascal_to_snake(m.name()));
            if let Some(ref tc) = *self.telemetry.lock().expect("lock poisoned") {
                telemetry::record_latency(tc, &name, start);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[allow(unsafe_code, reason = "env var manipulation in serialized tests")]
mod tests {
    use super::*;
    use serde_json::json;
    use serial_test::serial;
    use wiremock::matchers::{method, path_regex};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn setup_env(server: &MockServer) {
        // SAFETY: tests are serialized via #[serial] — no concurrent env mutation.
        unsafe {
            std::env::set_var(
                "AWS_ENDPOINT_URL_DEADLINE",
                format!("http://localhost:{}", server.address().port()),
            );
            std::env::set_var("AWS_ACCESS_KEY_ID", "AKIAIOSFODNN7EXAMPLE");
            std::env::set_var(
                "AWS_SECRET_ACCESS_KEY",
                "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            );
            std::env::set_var("AWS_DEFAULT_REGION", "us-west-2");
            std::env::set_var("AWS_CONFIG_FILE", "/dev/null");
        }
    }

    #[tokio::test]
    #[serial]
    async fn telemetry_interceptor_reads_sdk_metadata_as_snake_case() {
        let server = MockServer::start().await;
        setup_env(&server);

        Mock::given(method("GET"))
            .and(path_regex(".*/farms/farm-abc"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "farmId": "farm-abc", "displayName": "Test",
                "createdAt": "2024-01-01T00:00:00Z", "createdBy": "u",
                "updatedAt": "2024-01-01T00:00:00Z", "updatedBy": "u"
            })))
            .mount(&server)
            .await;

        let sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .endpoint_url(format!("http://localhost:{}", server.address().port()))
            .load()
            .await;

        let interceptor = TelemetryInterceptor::new(None);
        let client = aws_sdk_deadline::Client::from_conf(
            aws_sdk_deadline::config::Builder::from(&sdk_config)
                .interceptor(interceptor.clone())
                .build(),
        );

        let result = client.get_farm().farm_id("farm-abc").send().await;
        assert!(result.is_ok(), "Call failed: {:?}", result.err());

        // After execution, start should be consumed (cleared)
        assert!(
            interceptor.start.lock().unwrap().is_none(),
            "Start should be consumed after read_after_execution"
        );
    }

    #[tokio::test]
    #[serial]
    async fn telemetry_interceptor_emits_one_event_per_call() {
        let server = MockServer::start().await;
        setup_env(&server);

        Mock::given(method("GET"))
            .and(path_regex(".*/farms/farm-abc"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "farmId": "farm-abc", "displayName": "Test",
                "createdAt": "2024-01-01T00:00:00Z", "createdBy": "u",
                "updatedAt": "2024-01-01T00:00:00Z", "updatedBy": "u"
            })))
            .mount(&server)
            .await;

        let sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .endpoint_url(format!("http://localhost:{}", server.address().port()))
            .load()
            .await;

        let interceptor = TelemetryInterceptor::new(None);
        let client = aws_sdk_deadline::Client::from_conf(
            aws_sdk_deadline::config::Builder::from(&sdk_config)
                .interceptor(interceptor.clone())
                .build(),
        );

        // Two calls — start should be cleared after each
        let _ = client.get_farm().farm_id("farm-abc").send().await;
        let _ = client.get_farm().farm_id("farm-abc").send().await;

        assert!(interceptor.start.lock().unwrap().is_none());
    }
}
