use crate::session;

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

/// Read a key from the AWS config profile section.
/// Parses `~/.aws/config` (or `AWS_CONFIG_FILE`) looking for
/// `[profile <name>]` and returning the value of `key` if present.
fn read_aws_profile_key(profile_name: &str, key: &str) -> Option<String> {
    let config_path = std::env::var("AWS_CONFIG_FILE").ok().unwrap_or_else(|| {
        let home = std::env::var("HOME").unwrap_or_default();
        format!("{home}/.aws/config")
    });

    let content = std::fs::read_to_string(&config_path).ok()?;

    // Find the [profile <name>] section
    let section_header = format!("[profile {profile_name}]");
    let section_start = content.find(&section_header)?;
    let after_header = &content[section_start + section_header.len()..];

    // Read until next section or end of file
    for line in after_header.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with('[') {
            break; // next section
        }
        if let Some((k, v)) = trimmed.split_once('=') {
            if k.trim() == key {
                return Some(v.trim().to_string());
            }
        }
    }
    None
}

/// Determine where credentials come from.
/// DCM profiles have `monitor_id` in the AWS profile's scoped config.
pub fn get_credentials_source(
    config: Option<&deadline_config::ini::IniConfig>,
) -> AwsCredentialsSource {
    let profile_name = session::resolve_profile_name(config);
    match &profile_name {
        Some(name) => {
            if read_aws_profile_key(name, "monitor_id").is_some() {
                AwsCredentialsSource::DeadlineCloudMonitorLogin
            } else {
                AwsCredentialsSource::HostProvided
            }
        }
        // Default profile — check [default] section or [profile default]
        None => AwsCredentialsSource::HostProvided,
    }
}

/// If logged in with DCM, returns (user_id, identity_store_id).
/// Otherwise returns (None, None).
pub fn get_user_and_identity_store_id(
    config: Option<&deadline_config::ini::IniConfig>,
) -> (Option<String>, Option<String>) {
    let profile_name = match session::resolve_profile_name(config) {
        Some(name) => name,
        None => return (None, None),
    };

    if read_aws_profile_key(&profile_name, "monitor_id").is_none() {
        return (None, None);
    }

    let user_id = read_aws_profile_key(&profile_name, "user_id");
    let identity_store_id = read_aws_profile_key(&profile_name, "identity_store_id");
    (user_id, identity_store_id)
}

/// Returns the monitor_id from the AWS profile if it's a DCM profile.
pub fn get_monitor_id(
    config: Option<&deadline_config::ini::IniConfig>,
) -> Option<String> {
    let profile_name = session::resolve_profile_name(config)?;
    read_aws_profile_key(&profile_name, "monitor_id")
}

/// Check authentication by calling STS GetCallerIdentity.
pub async fn check_authentication_status(
    config: Option<&deadline_config::ini::IniConfig>,
) -> AwsAuthenticationStatus {
    let sts = session::sts_client(config).await;
    match sts.get_caller_identity().send().await {
        Ok(_) => AwsAuthenticationStatus::Authenticated,
        Err(_) => {
            let source = get_credentials_source(config);
            match source {
                AwsCredentialsSource::DeadlineCloudMonitorLogin => {
                    AwsAuthenticationStatus::NeedsLogin
                }
                _ => AwsAuthenticationStatus::ConfigurationError,
            }
        }
    }
}

/// Check if Deadline Cloud APIs are accessible by calling ListFarms with maxResults=1.
pub async fn check_deadline_api_available(
    config: Option<&deadline_config::ini::IniConfig>,
) -> bool {
    let client = session::deadline_client(config).await;
    let mut req = client.list_farms().max_results(1);
    let (user_id, _) = get_user_and_identity_store_id(config);
    if let Some(uid) = user_id {
        req = req.principal_id(uid);
    }
    req.send().await.is_ok()
}

/// Log in via Deadline Cloud Monitor.
/// Only supported for DCM-created profiles (those with `monitor_id`).
pub fn login(
    config: Option<&deadline_config::ini::IniConfig>,
) -> Result<String, String> {
    let source = get_credentials_source(config);
    if source != AwsCredentialsSource::DeadlineCloudMonitorLogin {
        return Err(
            "Logging in is only supported for AWS Profiles created by Deadline Cloud monitor."
                .to_string(),
        );
    }

    let monitor_path = match config {
        Some(c) => deadline_config::config_file::get_setting_with_config("deadline-cloud-monitor.path", c).unwrap_or_default(),
        None => deadline_config::config_file::get_setting("deadline-cloud-monitor.path").unwrap_or_default(),
    };
    let profile_name = session::display_profile_name(config);

    let mut child = std::process::Command::new(&monitor_path)
        .args(["login", "--profile", &profile_name])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .stdin(std::process::Stdio::null())
        .spawn()
        .map_err(|_| {
            format!(
                "Could not find Deadline Cloud monitor at {monitor_path}. \
                 Please ensure Deadline Cloud monitor is installed correctly \
                 and set up the {profile_name} profile again."
            )
        })?;

    // Poll authentication status until success or process exit
    let rt = tokio::runtime::Runtime::new().unwrap();
    loop {
        let status = rt.block_on(check_authentication_status(config));
        if status == AwsAuthenticationStatus::Authenticated {
            return Ok(format!("Deadline Cloud monitor profile: {profile_name}"));
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
        std::thread::sleep(std::time::Duration::from_millis(500));
    }
}

/// Log out via Deadline Cloud Monitor.
/// Only supported for DCM-created profiles (those with `monitor_id`).
pub fn logout(
    config: Option<&deadline_config::ini::IniConfig>,
) -> Result<String, String> {
    let source = get_credentials_source(config);
    if source != AwsCredentialsSource::DeadlineCloudMonitorLogin {
        return Err(
            "Logging out is only supported for AWS Profiles created by Deadline Cloud monitor."
                .to_string(),
        );
    }

    let monitor_path = match config {
        Some(c) => deadline_config::config_file::get_setting_with_config("deadline-cloud-monitor.path", c).unwrap_or_default(),
        None => deadline_config::config_file::get_setting("deadline-cloud-monitor.path").unwrap_or_default(),
    };
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

    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}
