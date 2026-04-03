use aws_config::SdkConfig;
use aws_sdk_deadline::Client as DeadlineClient;
use aws_sdk_sts::Client as StsClient;
use deadline_config::config_file;
use deadline_config::ini::IniConfig;
use std::sync::{LazyLock, Mutex};

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
    pub context: SessionContext,
}

impl SessionCache {
    pub fn new() -> Self {
        Self {
            cached_config: None,
            cached_profile: None,
            context: SessionContext::default(),
        }
    }

    /// Clear cached config. Next client construction re-resolves credentials.
    pub fn invalidate(&mut self) {
        self.cached_config = None;
        self.cached_profile = None;
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
        let sdk_config = self.get_config(config).await;
        let mut builder = aws_sdk_deadline::config::Builder::from(sdk_config);
        if let Ok(url) = std::env::var("AWS_ENDPOINT_URL_DEADLINE") {
            builder = builder.endpoint_url(url);
        }
        let ua = self.context.build_user_agent();
        if let Ok(app_name) = aws_sdk_deadline::config::AppName::new(ua) {
            builder = builder.app_name(app_name);
        }
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
}

// ---------------------------------------------------------------------------
// Public API — free functions backed by the global cache
// ---------------------------------------------------------------------------

/// Set the CLI command name for user-agent tracking.
/// Called by the CLI before dispatching to a subcommand.
pub fn set_cli_command_name(name: &str) {
    SESSION.lock().unwrap().context.cli_command_name = Some(name.to_string());
}

/// Set submitter info for user-agent tracking.
/// Called by GUI/DCC plugins before making API calls.
pub fn set_submitter_info(name: &str, version: Option<&str>) {
    let mut cache = SESSION.lock().unwrap();
    cache.context.submitter_name = Some(name.to_string());
    cache.context.submitter_version = version.map(|v| v.to_string());
}

/// Clear the cached SDK config. Next call re-resolves credentials.
/// Python equivalent: `invalidate_boto3_session_cache()`.
pub fn invalidate_session_cache() {
    SESSION.lock().unwrap().invalidate();
}

/// Build a Deadline Cloud client using the cached SDK config.
pub async fn deadline_client(config: Option<&IniConfig>) -> DeadlineClient {
    SESSION.lock().unwrap().build_deadline_client(config).await
}

/// Build an STS client using the cached SDK config.
pub async fn sts_client(config: Option<&IniConfig>) -> StsClient {
    SESSION.lock().unwrap().build_sts_client(config).await
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn get_setting(name: &str, config: Option<&IniConfig>) -> String {
    match config {
        Some(c) => config_file::get_setting_with_config(name, c).unwrap_or_default(),
        None => config_file::get_setting(name).unwrap_or_default(),
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
    if name.is_empty() { "(default)".to_string() } else { name }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── User-agent tests (§3 cases 15-20) ───────────────────────

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

    // ── Caching tests (§3 cases 5-6, 13-14) ────────────────────

    // §3 case 5: calling twice with same profile returns cached config
    #[tokio::test]
    async fn get_config_twice_returns_cached() {
        let mut cache = SessionCache::new();
        cache.get_config(None).await;
        let ptr1 = cache.cached_config.as_ref().unwrap() as *const SdkConfig;
        cache.get_config(None).await;
        let ptr2 = cache.cached_config.as_ref().unwrap() as *const SdkConfig;
        assert_eq!(ptr1, ptr2, "second call should return cached config");
    }

    // §3 case 6: invalidate clears cache
    #[tokio::test]
    async fn invalidate_clears_cached_config() {
        let mut cache = SessionCache::new();
        cache.get_config(None).await;
        assert!(cache.cached_config.is_some());
        cache.invalidate();
        assert!(cache.cached_config.is_none());
    }

    // §3 case 13: invalidate after caching clears everything
    #[tokio::test]
    async fn invalidate_after_caching_clears_all() {
        let mut cache = SessionCache::new();
        cache.get_config(None).await;
        cache.invalidate();
        assert!(cache.cached_config.is_none());
        assert!(cache.cached_profile.is_none());
    }

    // §3 case 14: invalidate on empty cache is a no-op
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
        let ua = SESSION.lock().unwrap().context.build_user_agent();
        assert!(ua.contains("cli-command/deadline.farm.list"));
    }
}
