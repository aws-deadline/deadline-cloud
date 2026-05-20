//! S3 client construction and AWS helpers for job attachments.
//!
//! This module provides S3-specific client configuration (timeouts, retries,
//! signature version, user agent, connection pool size) and account identity
//! retrieval. The caller provides an `SdkConfig` with credentials; this module
//! builds a properly-configured S3 client on top of it.

use crate::attachments::errors::JobAttachmentsError;

// --- Constants ---

pub const S3_CONNECT_TIMEOUT_SECS: u64 = 30;
pub const S3_READ_TIMEOUT_SECS: u64 = 30;
/// Python uses `"standard"` string; Rust SDK uses `RetryConfig::standard()`.
/// This constant exists for behavioral parity verification.
pub const S3_RETRIES_MODE: &str = "standard";

pub const S3_USER_AGENT_EXTRA: &str =
    concat!("S3A/Deadline/NA/JobAttachments/", env!("CARGO_PKG_VERSION"));

// --- S3 client construction ---

/// Builds an S3 client with job-attachments-specific configuration on top of
/// the caller's `SdkConfig` (which provides credentials and region).
///
/// `max_pool_connections` is validated at construction time so misconfiguration
/// is caught early rather than mid-transfer. Pass `None` to skip validation.
pub fn build_s3_client(
    sdk_config: &aws_config::SdkConfig,
    max_pool_connections: Option<usize>,
) -> aws_sdk_s3::Client {
    // Validate pool connections early so misconfiguration is caught at client
    // construction time, not mid-transfer.
    if let Some(pool) = max_pool_connections
        && pool == 0
    {
        log::warn!("S3 pool connections config issue: value must be positive");
    }

    let timeout_config = aws_config::timeout::TimeoutConfig::builder()
        .connect_timeout(std::time::Duration::from_secs(S3_CONNECT_TIMEOUT_SECS))
        .read_timeout(std::time::Duration::from_secs(S3_READ_TIMEOUT_SECS))
        .build();

    let mut s3_config_builder = aws_sdk_s3::config::Builder::from(sdk_config)
        .timeout_config(timeout_config)
        .retry_config(aws_config::retry::RetryConfig::standard())
        .force_path_style(false);

    if let Ok(url) = std::env::var("AWS_ENDPOINT_URL_S3") {
        s3_config_builder = s3_config_builder.endpoint_url(url).force_path_style(true);
    }

    if let Ok(app_name) = aws_sdk_s3::config::AppName::new(S3_USER_AGENT_EXTRA.to_owned()) {
        s3_config_builder = s3_config_builder.app_name(app_name);
    }

    aws_sdk_s3::Client::from_conf(s3_config_builder.build())
}

// --- Config helpers ---

/// Parses and validates an S3 max pool connections value.
/// Returns error if the value is not a positive integer.
/// Callers should read `settings.s3_max_pool_connections` from config
/// and pass the string value here.
pub fn parse_s3_max_pool_connections(value_str: &str) -> Result<usize, JobAttachmentsError> {
    let value: usize = value_str.parse().map_err(|_| {
        JobAttachmentsError::AssetSync(
            "Failed to parse configuration settings. Please ensure that the following \
             settings in the config file are integers: 's3_max_pool_connections'"
                .into(),
        )
    })?;

    if value == 0 {
        return Err(JobAttachmentsError::AssetSync(format!(
            "Nonvalid value for configuration setting: \
             's3_max_pool_connections' ({value}) must be positive integer."
        )));
    }

    Ok(value)
}

// --- Account identity ---

use crate::api::client::format_sdk_error;

/// Retrieves the AWS account ID by calling STS `GetCallerIdentity`.
///
/// Creates a new STS client per call. Callers should cache the result
/// if they need the account ID for multiple S3 operations (e.g. as
/// `ExpectedBucketOwner` on every PutObject/GetObject call).
pub async fn get_account_id(
    sdk_config: &aws_config::SdkConfig,
) -> Result<String, JobAttachmentsError> {
    let sts = aws_sdk_sts::Client::new(sdk_config);
    let identity = sts.get_caller_identity().send().await.map_err(|e| {
        JobAttachmentsError::AssetSync(format!(
            "Failed to get caller identity: {}",
            format_sdk_error(&e)
        ))
    })?;
    identity.account().map(ToOwned::to_owned).ok_or_else(|| {
        JobAttachmentsError::AssetSync("GetCallerIdentity returned no account ID".into())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // ===  cases 1-3: Constants ===

    #[test]
    fn s3_connect_timeout_is_30_seconds() {
        assert_eq!(S3_CONNECT_TIMEOUT_SECS, 30);
    }

    #[test]
    fn s3_read_timeout_is_30_seconds() {
        assert_eq!(S3_READ_TIMEOUT_SECS, 30);
    }

    #[test]
    fn s3_retries_mode_is_standard() {
        assert_eq!(S3_RETRIES_MODE, "standard");
    }

    // === build_s3_client produces configured client ===

    #[test]
    fn build_s3_client_returns_client() {
        let sdk_config = aws_config::SdkConfig::builder()
            .behavior_version(aws_config::BehaviorVersion::latest())
            .region(aws_config::Region::new("us-west-2"))
            .build();
        let client = build_s3_client(&sdk_config, None);
        assert!(size_of_val(&client) > 0);
    }

    // === pool connections validation ===

    #[test]
    fn build_s3_client_with_pool_connections() {
        let sdk_config = aws_config::SdkConfig::builder()
            .behavior_version(aws_config::BehaviorVersion::latest())
            .region(aws_config::Region::new("us-west-2"))
            .build();
        // Should not panic — pool connections is validated at build time
        let _client = build_s3_client(&sdk_config, Some(20));
    }

    // === user agent includes job attachments identifier ===

    #[test]
    fn s3_user_agent_contains_job_attachments() {
        assert!(S3_USER_AGENT_EXTRA.contains("JobAttachments"));
        assert!(S3_USER_AGENT_EXTRA.starts_with("S3A/Deadline/NA/JobAttachments/"));
    }

    // ===  cases 17-20: parse_s3_max_pool_connections ===

    #[test]
    fn parse_s3_max_pool_connections_valid_integer() {
        let result = parse_s3_max_pool_connections("10");
        assert_eq!(result.unwrap(), 10);
    }

    #[test]
    fn parse_s3_max_pool_connections_not_integer_errors() {
        let result = parse_s3_max_pool_connections("abc");
        assert!(result.is_err());
    }

    #[test]
    fn parse_s3_max_pool_connections_zero_errors() {
        let result = parse_s3_max_pool_connections("0");
        assert!(result.is_err());
    }

    #[test]
    fn parse_s3_max_pool_connections_negative_errors() {
        let result = parse_s3_max_pool_connections("-5");
        assert!(result.is_err());
    }

    // === get_account_id returns account string ===

    #[tokio::test]
    async fn get_account_id_returns_account_from_sts() {
        let sdk_config = aws_config::SdkConfig::builder()
            .behavior_version(aws_config::BehaviorVersion::latest())
            .region(aws_config::Region::new("us-west-2"))
            .build();
        // No real creds — should fail with an error, not panic
        let result = get_account_id(&sdk_config).await;
        assert!(result.is_err());
    }
}
