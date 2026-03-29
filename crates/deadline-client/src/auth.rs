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

/// Determine where credentials come from.
/// DCM profiles have `monitor_id` in the AWS profile's scoped config.
/// We don't have access to scoped config via the Rust SDK easily,
/// so for now: if the profile exists and works, it's HOST_PROVIDED.
/// DCM detection will be added when we implement login/logout.
pub fn get_credentials_source(
    _config: Option<&deadline_config::ini::IniConfig>,
) -> AwsCredentialsSource {
    // TODO: Check for monitor_id in AWS profile scoped config for DCM detection.
    // For now, all valid profiles are HOST_PROVIDED.
    AwsCredentialsSource::HostProvided
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
    client
        .list_farms()
        .max_results(1)
        .send()
        .await
        .is_ok()
}
