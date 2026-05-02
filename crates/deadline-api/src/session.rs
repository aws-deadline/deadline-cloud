use aws_config::SdkConfig;
use aws_credential_types::provider::{self, future, ProvideCredentials, SharedCredentialsProvider};
use aws_credential_types::Credentials;
use aws_sdk_deadline::Client as DeadlineClient;
use aws_sdk_sts::Client as StsClient;
use deadline_config::config_file;
use deadline_config::ini::IniConfig;
use std::collections::HashMap;
use std::sync::LazyLock;
use tokio::sync::Mutex;

use crate::telemetry_interceptor::TelemetryInterceptor;

// ---------------------------------------------------------------------------
// Global session cache
// ---------------------------------------------------------------------------

/// Global session cache — accessible within the crate for testing.
pub(crate) static SESSION: LazyLock<Mutex<SessionCache>> =
    LazyLock::new(|| Mutex::new(SessionCache::new()));

// ---------------------------------------------------------------------------
// SessionContext — user-agent tracking
// ---------------------------------------------------------------------------

/// Caller identity context attached to the User-Agent header on all AWS API calls.
/// Python equivalent: module-level `session_context` dict in `api/_session.py`.
#[derive(Debug, Default)]
pub struct SessionContext {
    pub submitter_name: Option<String>,
    pub submitter_version: Option<String>,
    pub cli_command_name: Option<String>,
}

impl SessionContext {
    /// Build the user-agent extra string.
    /// Format: `app/deadline-client#<version> submitter/<name>#<ver> cli-command/<cmd>`
    pub fn build_user_agent(&self) -> String {
        let version = env!("CARGO_PKG_VERSION");
        let mut ua = format!("app/deadline-client#{version}");
        if let Some(ref name) = self.submitter_name {
            ua.push_str(&format!(" submitter/{name}"));
            if let Some(ref ver) = self.submitter_version {
                ua.push_str(&format!("#{ver}"));
            }
        }
        if let Some(ref cmd) = self.cli_command_name {
            ua.push_str(&format!(" cli-command/{cmd}"));
        }
        ua
    }
}

// ---------------------------------------------------------------------------
// SessionCache — cached SdkConfig + context
// ---------------------------------------------------------------------------

/// Cached AWS SDK config and user-agent context.
/// Replaces Python's `@lru_cache` on `_get_boto3_session_for_profile`.
pub struct SessionCache {
    cached_config: Option<SdkConfig>,
    cached_profile: Option<Option<String>>,
    /// Queue user configs cached by (`farm_id`, `queue_id`).
    /// Python equivalent: `@lru_cache` on `_get_queue_user_boto3_session`.
    cached_queue_configs: HashMap<(String, String), SdkConfig>,
    pub context: SessionContext,
}

impl Default for SessionCache {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionCache {
    pub fn new() -> Self {
        Self {
            cached_config: None,
            cached_profile: None,
            cached_queue_configs: HashMap::new(),
            context: SessionContext::default(),
        }
    }

    /// Clear cached config. Next client construction re-resolves credentials.
    pub fn invalidate(&mut self) {
        self.cached_config = None;
        self.cached_profile = None;
        self.cached_queue_configs.clear();
    }

    /// Whether a config is currently cached.
    pub fn is_cached(&self) -> bool {
        self.cached_config.is_some()
    }

    /// Get or load the SDK config for the current profile.
    async fn get_config(&mut self, config: Option<&IniConfig>) -> &SdkConfig {
        let profile = resolve_profile(config);
        if self.cached_profile.as_ref() != Some(&profile) {
            self.cached_config = None;
        }
        if self.cached_config.is_none() {
            let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest());
            if let Some(ref p) = profile {
                loader = loader.profile_name(p);
            }
            self.cached_config = Some(loader.load().await);
            self.cached_profile = Some(profile);
        }
        self.cached_config.as_ref().unwrap()
    }

    async fn build_deadline_client(&mut self, config: Option<&IniConfig>) -> DeadlineClient {
        let sdk_config = self.get_config(config).await.clone();
        let mut builder = aws_sdk_deadline::config::Builder::from(&sdk_config);
        if let Ok(url) = std::env::var("AWS_ENDPOINT_URL_DEADLINE") {
            builder = builder.endpoint_url(url);
        }
        let ua = self.context.build_user_agent();
        if let Ok(app_name) = aws_sdk_deadline::config::AppName::new(ua) {
            builder = builder.app_name(app_name);
        }
        // Resolve account_id best-effort from STS
        let account_id = crate::telemetry::resolve_account_id(&sdk_config).await;
        let telemetry = crate::telemetry::create_telemetry_with_metadata(
            config, None, None, account_id.as_deref(),
        );
        builder = builder.interceptor(TelemetryInterceptor::new(Some(telemetry)));
        DeadlineClient::from_conf(builder.build())
    }

    async fn build_sts_client(&mut self, config: Option<&IniConfig>) -> StsClient {
        let sdk_config = self.get_config(config).await;
        let mut builder = aws_sdk_sts::config::Builder::from(sdk_config);
        if let Ok(url) = std::env::var("AWS_ENDPOINT_URL_STS") {
            builder = builder.endpoint_url(url);
        }
        let ua = self.context.build_user_agent();
        if let Ok(app_name) = aws_sdk_sts::config::AppName::new(ua) {
            builder = builder.app_name(app_name);
        }
        StsClient::from_conf(builder.build())
    }

    /// Build an `SdkConfig` with queue user credentials for the given farm/queue.
    /// Cached by (`farm_id`, `queue_id`). The credential provider calls
    /// `AssumeQueueRoleForUser` and auto-refreshes when credentials expire.
    pub async fn get_queue_user_config(
        &mut self,
        farm_id: &str,
        queue_id: &str,
        queue_display_name: Option<String>,
        config: Option<&IniConfig>,
    ) -> Result<SdkConfig, crate::errors::DeadlineError> {
        let key = (farm_id.to_owned(), queue_id.to_owned());
        if let Some(cached) = self.cached_queue_configs.get(&key) {
            return Ok(cached.clone());
        }

        let base_config = self.get_config(config).await;
        let region = base_config.region().cloned();

        // Build a deadline client for the credential provider to call AssumeQueueRoleForUser
        let mut dl_builder = aws_sdk_deadline::config::Builder::from(base_config);
        if let Ok(url) = std::env::var("AWS_ENDPOINT_URL_DEADLINE") {
            dl_builder = dl_builder.endpoint_url(url);
        }
        let dl_client = DeadlineClient::from_conf(dl_builder.build());

        let provider = QueueUserCredentialProvider::new(
            dl_client,
            farm_id.to_owned(),
            queue_id.to_owned(),
            queue_display_name,
        );

        let mut builder = SdkConfig::builder()
            .behavior_version(aws_config::BehaviorVersion::latest())
            .credentials_provider(SharedCredentialsProvider::new(provider));
        if let Some(r) = region {
            builder = builder.region(r);
        }
        // Propagate the global endpoint URL override if set. This is
        // needed for tests (stub server) and custom endpoint configs.
        // The AWS SDK reads per-service env vars (AWS_ENDPOINT_URL_STS,
        // AWS_ENDPOINT_URL_S3) when building service clients from an
        // SdkConfig loaded via aws_config::load_defaults(), but NOT
        // from a manually-built SdkConfig. So we set the global
        // endpoint_url which applies to all services built from this
        // config.
        if let Some(url) = base_config.endpoint_url() {
            builder = builder.endpoint_url(url);
        } else if let Ok(url) = std::env::var("AWS_ENDPOINT_URL_STS") {
            // Fallback: if per-service STS endpoint is set but no global
            // endpoint, use it as the global endpoint for this config.
            // This ensures STS and S3 clients built from queue-scoped
            // configs reach the stub server in tests.
            builder = builder.endpoint_url(url);
        }
        let sdk_config = builder.build();
        self.cached_queue_configs.insert(key, sdk_config.clone());
        Ok(sdk_config)
    }
}

// ---------------------------------------------------------------------------
// QueueUserCredentialProvider — calls AssumeQueueRoleForUser
// ---------------------------------------------------------------------------

/// Custom credential provider that calls `AssumeQueueRoleForUser` to obtain
/// temporary credentials scoped to a specific queue. The SDK automatically
/// calls `provide_credentials()` when credentials expire.
///
/// Python equivalent: `QueueUserCredentialProvider` in `api/_session.py`.
#[derive(Debug)]
pub struct QueueUserCredentialProvider {
    client: DeadlineClient,
    farm_id: String,
    queue_id: String,
    queue_display_name_or_id: String,
}

impl QueueUserCredentialProvider {
    pub fn new(
        client: DeadlineClient,
        farm_id: String,
        queue_id: String,
        queue_display_name: Option<String>,
    ) -> Self {
        let queue_display_name_or_id = queue_display_name.unwrap_or_else(|| queue_id.clone());
        Self { client, farm_id, queue_id, queue_display_name_or_id }
    }

    async fn load_credentials(&self) -> provider::Result {
        let result = self.client
            .assume_queue_role_for_user()
            .farm_id(&self.farm_id)
            .queue_id(&self.queue_id)
            .send()
            .await;

        if let Err(ref sdk_err) = result {
            let (code, message) = match sdk_err {
                aws_sdk_deadline::error::SdkError::ServiceError(e) => {
                    let inner = e.err();
                    use aws_sdk_deadline::error::ProvideErrorMetadata;
                    (
                        ProvideErrorMetadata::code(inner).unwrap_or("Unknown").to_owned(),
                        format!("{inner}"),
                    )
                }
                other => ("Unknown".to_owned(), format!("{}", aws_smithy_types::error::display::DisplayErrorContext(other))),
            };

            let display = &self.queue_display_name_or_id;
            let err_msg = match code.as_str() {
                "ThrottlingException" => format!(
                    "Throttled while attempting to assume Queue role for user on Queue '{display}': {message}\n\
                     Please retry the operation later, or contact your administrator to increase the API's rate limit."
                ),
                "InternalServerException" => format!(
                    "An internal server error occurred while attempting to assume Queue role for user on \
                     Queue '{display}': {message}\n"
                ),
                _ => format!(
                    "Failed to assume Queue role for user on Queue '{display}': {message}\nPlease contact your \
                     administrator to ensure a Queue role exists and that you have permissions to access this Queue."
                ),
            };
            return Err(provider::error::CredentialsError::provider_error(err_msg));
        }

        let output = result.unwrap();
        let creds = match output.credentials() {
            Some(c) if !c.access_key_id().is_empty() => c,
            _ => {
                let display = &self.queue_display_name_or_id;
                return Err(provider::error::CredentialsError::provider_error(
                    format!("Failed to get credentials for '{display}': Empty credentials received.")
                ));
            }
        };

        let expiration = {
            let dt = creds.expiration();
            let epoch_secs = dt.secs();
            if epoch_secs > 0 {
                Some(std::time::UNIX_EPOCH + std::time::Duration::from_secs(epoch_secs as u64))
            } else {
                None
            }
        };

        Ok(Credentials::new(
            creds.access_key_id(),
            creds.secret_access_key(),
            Some(creds.session_token().to_owned()),
            expiration,
            "queue-credential-provider",
        ))
    }
}

impl ProvideCredentials for QueueUserCredentialProvider {
    fn provide_credentials<'a>(&'a self) -> future::ProvideCredentials<'a>
    where
        Self: 'a,
    {
        future::ProvideCredentials::new(self.load_credentials())
    }
}

// ---------------------------------------------------------------------------
// Public API — free functions backed by the global cache
// ---------------------------------------------------------------------------

/// Set the CLI command name for user-agent tracking.
/// Called by the CLI before dispatching to a subcommand.
pub fn set_cli_command_name(name: &str) {
    SESSION.blocking_lock().context.cli_command_name = Some(name.to_owned());
}

/// Set submitter info for user-agent tracking.
/// Called by GUI/DCC plugins before making API calls.
pub async fn set_submitter_info(name: &str, version: Option<&str>) {
    let mut cache = SESSION.lock().await;
    cache.context.submitter_name = Some(name.to_owned());
    cache.context.submitter_version = version.map(ToOwned::to_owned);
}

/// Clear the cached SDK config. Next call re-resolves credentials.
/// Python equivalent: `invalidate_boto3_session_cache()`.
/// Use from sync contexts (main, logout). For async contexts use
/// `invalidate_session_cache_async`.
pub fn invalidate_session_cache() {
    // Use block_in_place to safely acquire the async Mutex from a sync context
    // running inside a tokio runtime (e.g., CLI logout). Plain blocking_lock()
    // panics in this situation.
    tokio::task::block_in_place(|| {
        tokio::runtime::Handle::current().block_on(async {
            SESSION.lock().await.invalidate();
        });
    });
}

/// Async version of `invalidate_session_cache` for use inside async functions.
pub async fn invalidate_session_cache_async() {
    SESSION.lock().await.invalidate();
}

/// Build a Deadline Cloud client using the cached SDK config.
pub async fn deadline_client(config: Option<&IniConfig>) -> DeadlineClient {
    SESSION.lock().await.build_deadline_client(config).await
}

/// Get the cached `SdkConfig` (for building non-Deadline AWS clients like `CloudWatch` Logs).
pub async fn get_sdk_config(config: Option<&IniConfig>) -> SdkConfig {
    SESSION.lock().await.get_config(config).await.clone()
}

/// Build an STS client using the cached SDK config.
pub async fn sts_client(config: Option<&IniConfig>) -> StsClient {
    SESSION.lock().await.build_sts_client(config).await
}

/// Get an `SdkConfig` with queue user credentials.
/// Falls back to config defaults for `farm_id` and `queue_id`.
/// Python equivalent: `get_queue_user_boto3_session()`.
pub async fn get_queue_user_config(
    farm_id: Option<&str>,
    queue_id: Option<&str>,
    queue_display_name: Option<String>,
    force_refresh: bool,
    config: Option<&IniConfig>,
) -> Result<SdkConfig, crate::errors::DeadlineError> {
    if force_refresh {
        invalidate_session_cache_async().await;
    }
    let farm = farm_id.map_or_else(|| get_setting("defaults.farm_id", config), String::from);
    let queue = queue_id.map_or_else(|| get_setting("defaults.queue_id", config), String::from);
    SESSION.lock().await.get_queue_user_config(&farm, &queue, queue_display_name, config).await
}

/// Get an `SdkConfig` appropriate for non-Deadline AWS services (`CloudWatch`, S3)
/// that access queue-scoped resources.
///
/// If the user is logged in via DCM (`monitor_id` present in AWS profile),
/// assumes the queue role via `AssumeQueueRoleForUser` and returns an
/// `SdkConfig` with queue-scoped credentials. If not DCM, returns the base
/// `SdkConfig`. If queue role assumption fails for a DCM user, the error is
/// propagated (matching Python, which raises `DeadlineOperationError`).
pub async fn get_queue_scoped_config(
    farm_id: &str,
    queue_id: &str,
    config: Option<&IniConfig>,
) -> Result<SdkConfig, crate::errors::DeadlineError> {
    let (user_id, identity_store_id) = crate::auth::get_user_and_identity_store_id(config);
    if user_id.is_some() && identity_store_id.is_some() {
        // DCM user — assume queue role
        get_queue_user_config(Some(farm_id), Some(queue_id), None, false, config).await
    } else {
        // Non-DCM user — use base credentials
        Ok(get_sdk_config(config).await)
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn get_setting(name: &str, config: Option<&IniConfig>) -> String {
    match config {
        Some(c) => config_file::get_setting(name, c).unwrap_or_default(),
        None => config_file::get_setting_from_disk(name).unwrap_or_default(),
    }
}

fn resolve_profile(config: Option<&IniConfig>) -> Option<String> {
    let name = get_setting("defaults.aws_profile_name", config);
    match name.as_str() {
        "(default)" | "default" | "" => None,
        _ => Some(name),
    }
}

/// Returns the resolved profile name, or None for the default credential chain.
/// Public so auth.rs can use it for DCM detection.
pub fn resolve_profile_name(config: Option<&IniConfig>) -> Option<String> {
    resolve_profile(config)
}

pub fn display_profile_name(config: Option<&IniConfig>) -> String {
    let name = get_setting("defaults.aws_profile_name", config);
    if name.is_empty() { "(default)".to_owned() } else { name }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── User-agent tests ───────────────────────

    #[test]
    fn build_user_agent_no_context_has_app_only() {
        let ctx = SessionContext::default();
        let ua = ctx.build_user_agent();
        let version = env!("CARGO_PKG_VERSION");
        assert_eq!(ua, format!("app/deadline-client#{version}"));
    }

    #[test]
    fn build_user_agent_submitter_name_only() {
        let ctx = SessionContext {
            submitter_name: Some("Blender".into()),
            ..Default::default()
        };
        let ua = ctx.build_user_agent();
        let version = env!("CARGO_PKG_VERSION");
        assert_eq!(ua, format!("app/deadline-client#{version} submitter/Blender"));
    }

    #[test]
    fn build_user_agent_submitter_name_and_version() {
        let ctx = SessionContext {
            submitter_name: Some("Blender".into()),
            submitter_version: Some("4.0".into()),
            ..Default::default()
        };
        let ua = ctx.build_user_agent();
        let version = env!("CARGO_PKG_VERSION");
        assert_eq!(ua, format!("app/deadline-client#{version} submitter/Blender#4.0"));
    }

    #[test]
    fn build_user_agent_cli_command_only() {
        let ctx = SessionContext {
            cli_command_name: Some("deadline.bundle.submit".into()),
            ..Default::default()
        };
        let ua = ctx.build_user_agent();
        let version = env!("CARGO_PKG_VERSION");
        assert_eq!(ua, format!("app/deadline-client#{version} cli-command/deadline.bundle.submit"));
    }

    #[test]
    fn build_user_agent_all_fields() {
        let ctx = SessionContext {
            submitter_name: Some("Blender".into()),
            submitter_version: Some("4.0".into()),
            cli_command_name: Some("deadline.bundle.submit".into()),
        };
        let ua = ctx.build_user_agent();
        let version = env!("CARGO_PKG_VERSION");
        assert_eq!(
            ua,
            format!("app/deadline-client#{version} submitter/Blender#4.0 cli-command/deadline.bundle.submit")
        );
    }

    #[test]
    fn build_user_agent_version_without_name_ignored() {
        let ctx = SessionContext {
            submitter_version: Some("4.0".into()),
            ..Default::default()
        };
        let ua = ctx.build_user_agent();
        let version = env!("CARGO_PKG_VERSION");
        assert_eq!(ua, format!("app/deadline-client#{version}"));
    }

    // ── Caching tests ────────────────────

    // calling twice with same profile returns cached config
    #[tokio::test]
    async fn get_config_twice_returns_cached() {
        let mut cache = SessionCache::new();
        cache.get_config(None).await;
        let ptr1 = std::ptr::from_ref::<SdkConfig>(cache.cached_config.as_ref().unwrap());
        cache.get_config(None).await;
        let ptr2 = std::ptr::from_ref::<SdkConfig>(cache.cached_config.as_ref().unwrap());
        assert_eq!(ptr1, ptr2, "second call should return cached config");
    }

    // invalidate clears cache
    #[tokio::test]
    async fn invalidate_clears_cached_config() {
        let mut cache = SessionCache::new();
        cache.get_config(None).await;
        assert!(cache.cached_config.is_some());
        cache.invalidate();
        assert!(cache.cached_config.is_none());
    }

    // invalidate after caching clears everything
    #[tokio::test]
    async fn invalidate_after_caching_clears_all() {
        let mut cache = SessionCache::new();
        cache.get_config(None).await;
        cache.invalidate();
        assert!(cache.cached_config.is_none());
        assert!(cache.cached_profile.is_none());
    }

    // invalidate on empty cache is a no-op
    #[test]
    fn invalidate_empty_cache_no_error() {
        let mut cache = SessionCache::new();
        cache.invalidate();
        assert!(cache.cached_config.is_none());
    }

    // ── Global accessor tests ───────────────────────────────────

    #[tokio::test]
    async fn global_deadline_client_returns_client() {
        let _client = deadline_client(None).await;
    }

    #[test]
    fn set_cli_command_name_updates_global_context() {
        set_cli_command_name("deadline.farm.list");
        let ua = SESSION.blocking_lock().context.build_user_agent();
        assert!(ua.contains("cli-command/deadline.farm.list"));
    }

    #[tokio::test]
    async fn set_submitter_info_updates_global_context() {
        set_submitter_info("Blender", Some("4.1")).await;
        let cache = SESSION.lock().await;
        assert_eq!(cache.context.submitter_name.as_deref(), Some("Blender"));
        assert_eq!(cache.context.submitter_version.as_deref(), Some("4.1"));
        drop(cache);

        // Calling again without version clears the version field
        set_submitter_info("Custom", None).await;
        let cache = SESSION.lock().await;
        assert_eq!(cache.context.submitter_name.as_deref(), Some("Custom"));
        assert_eq!(cache.context.submitter_version, None);
    }

    // ── Queue user credential provider tests ──────────────

    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// Build a `DeadlineClient` pointed at the given wiremock server.
    fn test_deadline_client(server: &MockServer) -> DeadlineClient {
        let port = server.address().port();
        let config = aws_sdk_deadline::Config::builder()
            .endpoint_url(format!("http://localhost:{port}"))
            .credentials_provider(Credentials::new(
                "AKID", "SECRET", Some("TOKEN".into()), None, "test",
            ))
            .region(aws_sdk_deadline::config::Region::new("us-west-2"))
            .behavior_version_latest()
            .build();
        DeadlineClient::from_conf(config)
    }

    // credential fetch succeeds — returns access_key, secret_key, token, expiry
    #[tokio::test]
    async fn queue_credential_provider_success_returns_credentials() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/2023-10-12/farms/farm-abc/queues/queue-123/user-roles"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "credentials": {
                    "accessKeyId": "ASIAQUEUEUSER",
                    "secretAccessKey": "secretqueue",
                    "sessionToken": "tokenqueue",
                    "expiration": "2099-12-18T01:30:45Z"
                }
            })))
            .mount(&server)
            .await;

        let client = test_deadline_client(&server);
        let provider = QueueUserCredentialProvider::new(
            client, "farm-abc".into(), "queue-123".into(), None,
        );
        let creds = provider.load_credentials().await.expect("should succeed");
        assert_eq!(creds.access_key_id(), "ASIAQUEUEUSER");
        assert_eq!(creds.secret_access_key(), "secretqueue");
        assert_eq!(creds.session_token(), Some("tokenqueue"));
        assert!(creds.expiry().is_some());
    }

    /// Extract the source error message from a `CredentialsError`.
    /// `CredentialsError::ProviderError` wraps our message in `source()`.
    fn credential_error_message(err: &dyn std::error::Error) -> String {
        // Walk the error chain to find our message
        let mut current: Option<&dyn std::error::Error> = Some(err);
        let mut last_msg = err.to_string();
        while let Some(e) = current {
            last_msg = e.to_string();
            current = e.source();
        }
        last_msg
    }

    // queue_display_name is provided — error messages use it
    #[tokio::test]
    async fn queue_credential_provider_error_uses_display_name() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/2023-10-12/farms/farm-abc/queues/queue-123/user-roles"))
            .respond_with(ResponseTemplate::new(403).set_body_json(serde_json::json!({
                "__type": "AccessDeniedException",
                "message": "Not authorized"
            })))
            .mount(&server)
            .await;

        let client = test_deadline_client(&server);
        let provider = QueueUserCredentialProvider::new(
            client, "farm-abc".into(), "queue-123".into(), Some("My Queue".into()),
        );
        let err = provider.load_credentials().await.unwrap_err();
        let msg = credential_error_message(&err);
        assert!(msg.contains("My Queue"), "error should use display name, got: {msg}");
    }

    // queue_display_name is not provided — error messages use queue_id
    #[tokio::test]
    async fn queue_credential_provider_error_falls_back_to_queue_id() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/2023-10-12/farms/farm-abc/queues/queue-123/user-roles"))
            .respond_with(ResponseTemplate::new(403).set_body_json(serde_json::json!({
                "__type": "AccessDeniedException",
                "message": "Not authorized"
            })))
            .mount(&server)
            .await;

        let client = test_deadline_client(&server);
        let provider = QueueUserCredentialProvider::new(
            client, "farm-abc".into(), "queue-123".into(), None,
        );
        let err = provider.load_credentials().await.unwrap_err();
        let msg = credential_error_message(&err);
        assert!(msg.contains("queue-123"), "error should use queue_id, got: {msg}");
    }

    // ThrottlingException — returns error with retry guidance
    #[tokio::test]
    async fn queue_credential_provider_throttling_returns_retry_message() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/2023-10-12/farms/farm-abc/queues/queue-123/user-roles"))
            .respond_with(ResponseTemplate::new(429).set_body_json(serde_json::json!({
                "__type": "ThrottlingException",
                "message": "Rate exceeded"
            })))
            .mount(&server)
            .await;

        let client = test_deadline_client(&server);
        let provider = QueueUserCredentialProvider::new(
            client, "farm-abc".into(), "queue-123".into(), None,
        );
        let err = provider.load_credentials().await.unwrap_err();
        let msg = credential_error_message(&err);
        assert!(msg.contains("Throttled"), "should mention throttling, got: {msg}");
        assert!(msg.contains("retry"), "should mention retry, got: {msg}");
    }

    // InternalServerException — returns error with internal server error message
    #[tokio::test]
    async fn queue_credential_provider_internal_error_returns_message() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/2023-10-12/farms/farm-abc/queues/queue-123/user-roles"))
            .respond_with(ResponseTemplate::new(500).set_body_json(serde_json::json!({
                "__type": "InternalServerException",
                "message": "Something broke"
            })))
            .mount(&server)
            .await;

        let client = test_deadline_client(&server);
        let provider = QueueUserCredentialProvider::new(
            client, "farm-abc".into(), "queue-123".into(), None,
        );
        let err = provider.load_credentials().await.unwrap_err();
        let msg = credential_error_message(&err);
        assert!(msg.contains("internal server error"), "should mention internal error, got: {msg}");
    }

    // other AWS error (AccessDeniedException) — returns admin contact guidance
    #[tokio::test]
    async fn queue_credential_provider_access_denied_returns_admin_guidance() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/2023-10-12/farms/farm-abc/queues/queue-123/user-roles"))
            .respond_with(ResponseTemplate::new(403).set_body_json(serde_json::json!({
                "__type": "AccessDeniedException",
                "message": "User is not authorized"
            })))
            .mount(&server)
            .await;

        let client = test_deadline_client(&server);
        let provider = QueueUserCredentialProvider::new(
            client, "farm-abc".into(), "queue-123".into(), None,
        );
        let err = provider.load_credentials().await.unwrap_err();
        let msg = credential_error_message(&err);
        assert!(msg.contains("Failed to assume Queue role"), "got: {msg}");
        assert!(msg.contains("administrator"), "should mention admin, got: {msg}");
    }

    // empty credentials (None) — returns empty credentials error
    #[tokio::test]
    async fn queue_credential_provider_empty_credentials_returns_error() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/2023-10-12/farms/farm-abc/queues/queue-123/user-roles"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "credentials": null
            })))
            .mount(&server)
            .await;

        let client = test_deadline_client(&server);
        let provider = QueueUserCredentialProvider::new(
            client, "farm-abc".into(), "queue-123".into(), None,
        );
        let err = provider.load_credentials().await.unwrap_err();
        let msg = credential_error_message(&err);
        assert!(msg.contains("Empty credentials received"), "got: {msg}");
    }

    // response with no "credentials" key — returns empty credentials error
    #[tokio::test]
    async fn queue_credential_provider_missing_credentials_key_returns_error() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/2023-10-12/farms/farm-abc/queues/queue-123/user-roles"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .mount(&server)
            .await;

        let client = test_deadline_client(&server);
        let provider = QueueUserCredentialProvider::new(
            client, "farm-abc".into(), "queue-123".into(), None,
        );
        let err = provider.load_credentials().await.unwrap_err();
        let msg = credential_error_message(&err);
        assert!(msg.contains("Empty credentials received"), "got: {msg}");
    }

    // caching — calling get_queue_user_config twice returns cached config
    #[tokio::test]
    async fn get_queue_user_config_caches_by_farm_and_queue() {
        let mut cache = SessionCache::new();
        // First call creates a config
        let cfg1 = cache.get_queue_user_config(
            "farm-abc", "queue-123", None, None,
        ).await;
        assert!(cfg1.is_ok());
        // Second call should return cached (same key)
        assert!(cache.cached_queue_configs.contains_key(&("farm-abc".to_owned(), "queue-123".to_owned())));
    }

    // force_refresh clears base session and queue configs
    #[tokio::test]
    async fn invalidate_clears_queue_config_cache() {
        let mut cache = SessionCache::new();
        let _ = cache.get_queue_user_config(
            "farm-abc", "queue-123", None, None,
        ).await;
        assert!(!cache.cached_queue_configs.is_empty());
        cache.invalidate();
        assert!(cache.cached_queue_configs.is_empty());
    }
}
