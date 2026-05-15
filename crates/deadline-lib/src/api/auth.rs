use crate::api::session;
use crate::api::telemetry::{TelemetryClient, with_telemetry_latency};

/// Where the AWS credentials come from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AwsCredentialsSource {
    NotValid,
    HostProvided,
    DeadlineCloudMonitorLogin,
}

impl std::fmt::Display for AwsCredentialsSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotValid => write!(f, "NOT_VALID"),
            Self::HostProvided => write!(f, "HOST_PROVIDED"),
            Self::DeadlineCloudMonitorLogin => write!(f, "DEADLINE_CLOUD_MONITOR_LOGIN"),
        }
    }
}

/// Authentication status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AwsAuthenticationStatus {
    ConfigurationError,
    Authenticated,
    NeedsLogin,
}

impl std::fmt::Display for AwsAuthenticationStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ConfigurationError => write!(f, "CONFIGURATION_ERROR"),
            Self::Authenticated => write!(f, "AUTHENTICATED"),
            Self::NeedsLogin => write!(f, "NEEDS_LOGIN"),
        }
    }
}

/// Resolve the AWS config file path from `AWS_CONFIG_FILE` env var or default `~/.aws/config`.
fn aws_config_file_path() -> String {
    std::env::var("AWS_CONFIG_FILE").ok().unwrap_or_else(|| {
        let home = std::env::var("HOME").unwrap_or_default();
        format!("{home}/.aws/config")
    })
}

/// Read a key from the AWS config profile section.
/// Parses `~/.aws/config` (or `AWS_CONFIG_FILE`) looking for
/// `[profile <name>]` and returning the value of `key` if present.
fn read_aws_profile_key(profile_name: &str, key: &str) -> Option<String> {
    let config_path = aws_config_file_path();

    let content = std::fs::read_to_string(&config_path).ok()?;

    // Find the [profile <name>] section
    let section_header = format!("[profile {profile_name}]");
    read_aws_config_section(&content, &section_header, key)
}

/// Read a key from the `[default]` section of the AWS config file.
fn read_aws_default_profile_key(key: &str) -> Option<String> {
    let config_path = aws_config_file_path();

    let content = std::fs::read_to_string(&config_path).ok()?;
    read_aws_config_section(&content, "[default]", key)
}

/// Shared helper: find a section header in AWS config text and extract a key.
fn read_aws_config_section(content: &str, section_header: &str, key: &str) -> Option<String> {
    let section_start = content.find(section_header)?;
    let after_header = &content[section_start + section_header.len()..];

    for line in after_header.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with('[') {
            break;
        }
        if let Some((k, v)) = trimmed.split_once('=')
            && k.trim() == key
        {
            return Some(v.trim().to_owned());
        }
    }
    None
}

/// Determine where credentials come from.
/// DCM profiles have `monitor_id` in the AWS profile's scoped config.
/// Returns `NotValid` if the specified profile does not exist.
pub fn get_credentials_source(config: &crate::config::ini::IniConfig) -> AwsCredentialsSource {
    let profile_name = session::resolve_profile_name(config);
    match &profile_name {
        Some(name) => {
            if !aws_profile_exists(name) {
                AwsCredentialsSource::NotValid
            } else if read_aws_profile_key(name, "monitor_id").is_some() {
                AwsCredentialsSource::DeadlineCloudMonitorLogin
            } else {
                AwsCredentialsSource::HostProvided
            }
        }
        // Default profile — check [default] section for monitor_id
        None => {
            if read_aws_default_profile_key("monitor_id").is_some() {
                AwsCredentialsSource::DeadlineCloudMonitorLogin
            } else {
                AwsCredentialsSource::HostProvided
            }
        }
    }
}

/// Check whether a named profile section exists in the AWS config file.
fn aws_profile_exists(profile_name: &str) -> bool {
    let config_path = aws_config_file_path();
    let Ok(content) = std::fs::read_to_string(&config_path) else {
        return false;
    };
    let section_header = format!("[profile {profile_name}]");
    content.contains(&section_header)
}

/// If logged in with DCM, returns (`user_id`, `identity_store_id`).
/// Otherwise returns (None, None).
pub fn get_user_and_identity_store_id(
    config: &crate::config::ini::IniConfig,
) -> (Option<String>, Option<String>) {
    let profile_name = session::resolve_profile_name(config);

    let read_key = |key: &str| -> Option<String> {
        match &profile_name {
            Some(name) => read_aws_profile_key(name, key),
            None => read_aws_default_profile_key(key),
        }
    };

    if read_key("monitor_id").is_none() {
        return (None, None);
    }

    let user_id = read_key("user_id");
    let identity_store_id = read_key("identity_store_id");
    (user_id, identity_store_id)
}

/// Returns the `monitor_id` from the AWS profile if it's a DCM profile.
pub fn get_monitor_id(config: &crate::config::ini::IniConfig) -> Option<String> {
    match session::resolve_profile_name(config) {
        Some(name) => read_aws_profile_key(&name, "monitor_id"),
        None => read_aws_default_profile_key("monitor_id"),
    }
}

/// Check authentication by calling `ListFarms` with maxResults=1.
/// This validates both credential validity AND Deadline API reachability
/// in one call, matching Python's behavior. No STS dependency.
pub async fn check_authentication_status(
    config: &crate::config::ini::IniConfig,
) -> AwsAuthenticationStatus {
    let client = session::deadline_client(config).await;
    let mut req = client.list_farms().max_results(1);
    let (user_id, _) = get_user_and_identity_store_id(config);
    if let Some(uid) = user_id {
        req = req.principal_id(uid);
    }
    if req.send().await.is_ok() {
        AwsAuthenticationStatus::Authenticated
    } else {
        let source = get_credentials_source(config);
        match source {
            AwsCredentialsSource::DeadlineCloudMonitorLogin => AwsAuthenticationStatus::NeedsLogin,
            _ => AwsAuthenticationStatus::ConfigurationError,
        }
    }
}

/// Log in via Deadline Cloud Monitor.
/// Only supported for DCM-created profiles (those with `monitor_id`).
pub async fn login(
    on_pending_authorization: Option<&dyn Fn(AwsCredentialsSource)>,
    on_cancellation_check: Option<&dyn Fn() -> bool>,
    config: &crate::config::ini::IniConfig,
    telemetry: Option<&TelemetryClient>,
) -> Result<String, String> {
    let ephemeral;
    let tc = if let Some(t) = telemetry {
        t
    } else {
        ephemeral = crate::api::telemetry::create_telemetry(config);
        &ephemeral
    };
    let start = std::time::Instant::now();
    let result = login_inner(on_pending_authorization, on_cancellation_check, config).await;
    crate::api::telemetry::record_latency(tc, "login", start);
    result
}

async fn login_inner(
    on_pending_authorization: Option<&dyn Fn(AwsCredentialsSource)>,
    on_cancellation_check: Option<&dyn Fn() -> bool>,
    config: &crate::config::ini::IniConfig,
) -> Result<String, String> {
    let source = get_credentials_source(config);
    if source != AwsCredentialsSource::DeadlineCloudMonitorLogin {
        return Err(
            "Logging in is only supported for AWS Profiles created by Deadline Cloud monitor."
                .to_owned(),
        );
    }

    let monitor_path = get_monitor_path(config);
    let profile_name = session::display_profile_name(config);

    let stdin_cfg = if cfg!(windows) {
        std::process::Stdio::piped()
    } else {
        std::process::Stdio::null()
    };

    let mut child = std::process::Command::new(&monitor_path)
        .args(["login", "--profile", &profile_name])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .stdin(stdin_cfg)
        .spawn()
        .map_err(|_| {
            format!(
                "Could not find Deadline Cloud monitor at {monitor_path}. \
                 Please ensure Deadline Cloud monitor is installed correctly \
                 and set up the {profile_name} profile again."
            )
        })?;

    if let Some(cb) = on_pending_authorization {
        cb(AwsCredentialsSource::DeadlineCloudMonitorLogin);
    }

    // Poll authentication status until success or process exit
    loop {
        // Force-refresh the session each iteration so the next probe picks up
        // profile keys DCM writes (credential_process, user_id, identity_store_id)
        // as login completes. Without this, the cached SdkConfig retains stale
        // credentials and the auth probe never resolves to AUTHENTICATED.
        session::invalidate_session_cache_async().await;
        let status = check_authentication_status(config).await;
        if status == AwsAuthenticationStatus::Authenticated {
            return Ok(format!("Deadline Cloud monitor profile: {profile_name}"));
        }
        if let Some(cb) = on_cancellation_check
            && cb()
        {
            let _ = child.kill();
            return Err("Login canceled".to_owned());
        }
        if let Some(_exit) = child.try_wait().ok().flatten() {
            let out = child
                .stdout
                .take()
                .map(|mut s| {
                    let mut buf = String::new();
                    std::io::Read::read_to_string(&mut s, &mut buf).ok();
                    buf
                })
                .unwrap_or_default();
            return Err(format!(
                "Deadline Cloud monitor was not able to log into the {profile_name} profile:\n{out}"
            ));
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
}

/// Log out via Deadline Cloud Monitor.
/// Only supported for DCM-created profiles (those with `monitor_id`).
pub fn logout(
    config: &crate::config::ini::IniConfig,
    telemetry: Option<&TelemetryClient>,
) -> Result<String, String> {
    with_telemetry_latency("logout", config, telemetry, || logout_inner(config))
}

fn logout_inner(config: &crate::config::ini::IniConfig) -> Result<String, String> {
    let source = get_credentials_source(config);
    if source != AwsCredentialsSource::DeadlineCloudMonitorLogin {
        return Err(
            "Logging out is only supported for AWS Profiles created by Deadline Cloud monitor."
                .to_owned(),
        );
    }

    let monitor_path = get_monitor_path(config);
    let profile_name = session::display_profile_name(config);

    let output = std::process::Command::new(&monitor_path)
        .args(["logout", "--profile", &profile_name])
        .output()
        .map_err(|_| {
            format!(
                "Could not find Deadline Cloud monitor at {monitor_path}. \
                 Please ensure Deadline Cloud monitor is installed correctly \
                 and set up the {profile_name} profile again."
            )
        })?;

    if !output.status.success() {
        return Err(format!(
            "Deadline Cloud monitor was unable to log out the profile {profile_name}.\
             Return code {}: {}",
            output.status.code().unwrap_or(-1),
            String::from_utf8_lossy(&output.stdout)
        ));
    }

    session::invalidate_session_cache();
    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

fn get_monitor_path(config: &crate::config::ini::IniConfig) -> String {
    crate::config::config_file::get_setting("deadline-cloud-monitor.path", config)
        .unwrap_or_default()
}

// login/logout are tested at Level 2 in cli_auth.rs (require subprocess isolation).
// Level 1 tests below cover the heavily-reused pure logic functions that are the
// foundation of DCM detection and credential scoping across 7+ call sites.

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;
    use std::io::Write;
    use tempfile::NamedTempFile;

    /// Write content to a temp file and set `AWS_CONFIG_FILE` to point at it.
    /// Returns the temp file (must stay alive for the duration of the test).
    fn with_aws_config(content: &str) -> NamedTempFile {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(content.as_bytes()).unwrap();
        f.flush().unwrap();
        // SAFETY: tests are serialized via #[serial] — no concurrent env mutation.
        unsafe { std::env::set_var("AWS_CONFIG_FILE", f.path()) };
        f
    }

    fn set_nonexistent_config() {
        // SAFETY: tests are serialized via #[serial].
        unsafe { std::env::set_var("AWS_CONFIG_FILE", "/nonexistent/path/aws_config") };
    }

    // ── read_aws_profile_key ──────────────────────────────────

    #[test]
    #[serial]
    fn read_profile_key_happy_path() {
        let _f = with_aws_config("[profile myprof]\nmonitor_id = mon-123\nregion = us-west-2\n");
        assert_eq!(
            read_aws_profile_key("myprof", "monitor_id"),
            Some("mon-123".into())
        );
        assert_eq!(
            read_aws_profile_key("myprof", "region"),
            Some("us-west-2".into())
        );
    }

    #[test]
    #[serial]
    fn read_profile_key_missing_key() {
        let _f = with_aws_config("[profile myprof]\nregion = us-west-2\n");
        assert_eq!(read_aws_profile_key("myprof", "monitor_id"), None);
    }

    #[test]
    #[serial]
    fn read_profile_key_missing_profile() {
        let _f = with_aws_config("[profile other]\nmonitor_id = mon-123\n");
        assert_eq!(read_aws_profile_key("myprof", "monitor_id"), None);
    }

    #[test]
    #[serial]
    fn read_profile_key_stops_at_next_section() {
        let _f = with_aws_config("[profile first]\nkey1 = val1\n[profile second]\nkey2 = val2\n");
        assert_eq!(read_aws_profile_key("first", "key1"), Some("val1".into()));
        assert_eq!(read_aws_profile_key("first", "key2"), None);
        assert_eq!(read_aws_profile_key("second", "key2"), Some("val2".into()));
    }

    #[test]
    #[serial]
    fn read_profile_key_trims_whitespace() {
        let _f = with_aws_config("[profile myprof]\n  monitor_id  =  mon-456  \n");
        assert_eq!(
            read_aws_profile_key("myprof", "monitor_id"),
            Some("mon-456".into())
        );
    }

    #[test]
    #[serial]
    fn read_profile_key_empty_file() {
        let _f = with_aws_config("");
        assert_eq!(read_aws_profile_key("myprof", "monitor_id"), None);
    }

    #[test]
    #[serial]
    fn read_profile_key_no_file() {
        set_nonexistent_config();
        assert_eq!(read_aws_profile_key("myprof", "monitor_id"), None);
    }

    #[test]
    #[serial]
    fn read_profile_key_value_with_equals_sign() {
        let _f = with_aws_config("[profile myprof]\ncredential_process = echo a=b\n");
        assert_eq!(
            read_aws_profile_key("myprof", "credential_process"),
            Some("echo a=b".into())
        );
    }

    // ── aws_profile_exists ────────────────────────────────────

    #[test]
    #[serial]
    fn profile_exists_true() {
        let _f = with_aws_config("[profile myprof]\nregion = us-west-2\n");
        assert!(aws_profile_exists("myprof"));
    }

    #[test]
    #[serial]
    fn profile_exists_false() {
        let _f = with_aws_config("[profile other]\nregion = us-west-2\n");
        assert!(!aws_profile_exists("myprof"));
    }

    #[test]
    #[serial]
    fn profile_exists_no_file() {
        set_nonexistent_config();
        assert!(!aws_profile_exists("myprof"));
    }

    // ── get_credentials_source ────────────────────────────────

    #[test]
    #[serial]
    fn credentials_source_dcm_profile() {
        let _f = with_aws_config("[profile dcm]\nmonitor_id = mon-abc\n");
        let mut ini = crate::config::ini::IniConfig::new();
        ini.set("defaults", "aws_profile_name", "dcm");
        assert_eq!(
            get_credentials_source(&ini),
            AwsCredentialsSource::DeadlineCloudMonitorLogin
        );
    }

    #[test]
    #[serial]
    fn credentials_source_host_provided() {
        let _f = with_aws_config("[profile regular]\nregion = us-west-2\n");
        let mut ini = crate::config::ini::IniConfig::new();
        ini.set("defaults", "aws_profile_name", "regular");
        assert_eq!(
            get_credentials_source(&ini),
            AwsCredentialsSource::HostProvided
        );
    }

    #[test]
    #[serial]
    fn credentials_source_not_valid() {
        let _f = with_aws_config("[profile other]\nregion = us-west-2\n");
        let mut ini = crate::config::ini::IniConfig::new();
        ini.set("defaults", "aws_profile_name", "nonexistent");
        assert_eq!(get_credentials_source(&ini), AwsCredentialsSource::NotValid);
    }

    #[test]
    #[serial]
    fn credentials_source_default_profile() {
        let _f = with_aws_config("");
        let mut ini = crate::config::ini::IniConfig::new();
        ini.set("defaults", "aws_profile_name", "(default)");
        assert_eq!(
            get_credentials_source(&ini),
            AwsCredentialsSource::HostProvided
        );
    }

    // ── get_user_and_identity_store_id ────────────────────────

    #[test]
    #[serial]
    fn user_and_identity_dcm_profile_returns_both() {
        let _f = with_aws_config(
            "[profile dcm]\nmonitor_id = mon-abc\nuser_id = user-123\nidentity_store_id = d-456\n",
        );
        let mut ini = crate::config::ini::IniConfig::new();
        ini.set("defaults", "aws_profile_name", "dcm");
        let (uid, isid) = get_user_and_identity_store_id(&ini);
        assert_eq!(uid.as_deref(), Some("user-123"));
        assert_eq!(isid.as_deref(), Some("d-456"));
    }

    #[test]
    #[serial]
    fn user_and_identity_non_dcm_returns_none() {
        let _f = with_aws_config("[profile regular]\nregion = us-west-2\n");
        let mut ini = crate::config::ini::IniConfig::new();
        ini.set("defaults", "aws_profile_name", "regular");
        let (uid, isid) = get_user_and_identity_store_id(&ini);
        assert!(uid.is_none());
        assert!(isid.is_none());
    }

    #[test]
    #[serial]
    fn user_and_identity_dcm_missing_user_id() {
        let _f =
            with_aws_config("[profile dcm]\nmonitor_id = mon-abc\nidentity_store_id = d-456\n");
        let mut ini = crate::config::ini::IniConfig::new();
        ini.set("defaults", "aws_profile_name", "dcm");
        let (uid, isid) = get_user_and_identity_store_id(&ini);
        assert!(uid.is_none());
        assert_eq!(isid.as_deref(), Some("d-456"));
    }

    #[test]
    #[serial]
    fn user_and_identity_default_profile_returns_none() {
        let _f = with_aws_config("");
        let mut ini = crate::config::ini::IniConfig::new();
        ini.set("defaults", "aws_profile_name", "(default)");
        assert_eq!(get_user_and_identity_store_id(&ini), (None, None));
    }

    // ── get_monitor_id ────────────────────────────────────────

    #[test]
    #[serial]
    fn monitor_id_dcm_profile() {
        let _f = with_aws_config("[profile dcm]\nmonitor_id = mon-xyz\n");
        let mut ini = crate::config::ini::IniConfig::new();
        ini.set("defaults", "aws_profile_name", "dcm");
        assert_eq!(get_monitor_id(&ini), Some("mon-xyz".into()));
    }

    #[test]
    #[serial]
    fn monitor_id_non_dcm_profile() {
        let _f = with_aws_config("[profile regular]\nregion = us-west-2\n");
        let mut ini = crate::config::ini::IniConfig::new();
        ini.set("defaults", "aws_profile_name", "regular");
        assert_eq!(get_monitor_id(&ini), None);
    }
}
