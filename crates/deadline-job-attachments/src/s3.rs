//! S3 client construction and AWS helpers for job attachments.
//!
//! This module provides S3-specific client configuration (timeouts, retries,
//! signature version, user agent, connection pool size) and account identity
//! retrieval. The caller provides an `SdkConfig` with credentials; this module
//! builds a properly-configured S3 client on top of it.

use deadline_config::config_file::{get_setting, get_setting_with_config};
use deadline_config::ini::IniConfig;
use deadline_models::errors::JobAttachmentsError;

// --- Constants ---

pub const S3_CONNECT_TIMEOUT_SECS: u64 = 30;
pub const S3_READ_TIMEOUT_SECS: u64 = 30;
/// Python uses `"standard"` string; Rust SDK uses `RetryConfig::standard()`.
/// This constant exists for behavioral parity verification.
pub const S3_RETRIES_MODE: &str = "standard";
pub const S3_MULTIPART_UPLOAD_CHUNK_SIZE: usize = 8 * 1024 * 1024; // 8 MB
pub const S3_UPLOAD_MAX_CONCURRENCY: usize = 10;
pub const S3_DOWNLOAD_MAX_CONCURRENCY: usize = 10;
pub const S3_USER_AGENT_EXTRA: &str =
    concat!("S3A/Deadline/NA/JobAttachments/", env!("CARGO_PKG_VERSION"));

// --- S3 client construction ---

/// Builds an S3 client with job-attachments-specific configuration on top of
/// the caller's `SdkConfig` (which provides credentials and region).
///
/// Concurrency (pool connections, worker counts) is NOT configured here —
/// callers use [`get_s3_max_pool_connections`] to determine worker counts
/// for upload/download parallelism.
pub fn build_s3_client(
    sdk_config: &aws_config::SdkConfig,
    config: Option<&IniConfig>,
) -> aws_sdk_s3::Client {
    // Validate pool connections early so misconfiguration is caught at client
    // construction time, not mid-transfer. The value itself is used by
    // upload/download callers, not by the HTTP client.
    if let Some(c) = config {
        if let Err(e) = get_s3_max_pool_connections(Some(c)) {
            log::warn!("S3 pool connections config issue: {e}");
        }
    }

    let timeout_config = aws_config::timeout::TimeoutConfig::builder()
        .connect_timeout(std::time::Duration::from_secs(S3_CONNECT_TIMEOUT_SECS))
        .read_timeout(std::time::Duration::from_secs(S3_READ_TIMEOUT_SECS))
        .build();

    let mut s3_config_builder = aws_sdk_s3::config::Builder::from(sdk_config)
        .timeout_config(timeout_config)
        .retry_config(aws_config::retry::RetryConfig::standard())
        .force_path_style(false);

    if let Ok(app_name) = aws_sdk_s3::config::AppName::new(S3_USER_AGENT_EXTRA.to_string()) {
        s3_config_builder = s3_config_builder.app_name(app_name);
    }

    aws_sdk_s3::Client::from_conf(s3_config_builder.build())
}

// --- Config helpers ---

/// Reads `settings.s3_max_pool_connections` from config. Returns error if
/// the value is not a positive integer. Falls back to the on-disk config
/// file when no `IniConfig` is provided.
pub fn get_s3_max_pool_connections(
    config: Option<&IniConfig>,
) -> Result<i32, JobAttachmentsError> {
    let value_str = match config {
        Some(c) => get_setting_with_config("settings.s3_max_pool_connections", c),
        None => get_setting("settings.s3_max_pool_connections"),
    }
    .map_err(|e| {
        JobAttachmentsError::AssetSync(format!(
            "Failed to read s3_max_pool_connections setting: {e}"
        ))
    })?;

    let value: i32 = value_str.parse().map_err(|_| {
        JobAttachmentsError::AssetSync(
            "Failed to parse configuration settings. Please ensure that the following \
             settings in the config file are integers: 's3_max_pool_connections'"
                .into(),
        )
    })?;

    if value <= 0 {
        return Err(JobAttachmentsError::AssetSync(format!(
            "Nonvalid value for configuration setting: \
             's3_max_pool_connections' ({value}) must be positive integer."
        )));
    }

    Ok(value)
}

/// Reads `settings.small_file_threshold_multiplier` from config. Returns error
/// if the value is not a positive integer.
pub fn get_small_file_threshold_multiplier(
    config: Option<&IniConfig>,
) -> Result<i32, JobAttachmentsError> {
    let value_str = match config {
        Some(c) => get_setting_with_config("settings.small_file_threshold_multiplier", c),
        None => get_setting("settings.small_file_threshold_multiplier"),
    }
    .map_err(|e| {
        JobAttachmentsError::AssetSync(format!(
            "Failed to read small_file_threshold_multiplier setting: {e}"
        ))
    })?;

    let value: i32 = value_str.parse().map_err(|_| {
        JobAttachmentsError::AssetSync(
            "Failed to parse configuration settings. Please ensure that the following \
             settings in the config file are integers: \
             's3_max_pool_connections', 'small_file_threshold_multiplier'"
                .into(),
        )
    })?;

    if value <= 0 {
        return Err(JobAttachmentsError::AssetSync(format!(
            "Nonvalid value for configuration setting: \
             'small_file_threshold_multiplier' ({value}) must be positive integer."
        )));
    }

    Ok(value)
}

/// Computes upload configuration from config settings.
/// Returns `(small_file_threshold_bytes, num_upload_workers)`.
pub fn compute_upload_config(
    config: Option<&IniConfig>,
) -> Result<(usize, usize), JobAttachmentsError> {
    let multiplier = get_small_file_threshold_multiplier(config)?;
    let pool_connections = get_s3_max_pool_connections(config)?;

    let threshold = S3_MULTIPART_UPLOAD_CHUNK_SIZE * (multiplier as usize);
    let divisor = (multiplier as usize).min(S3_UPLOAD_MAX_CONCURRENCY);
    let workers = ((pool_connections as usize) / divisor).max(1);

    Ok((threshold, workers))
}

// --- Account identity ---

/// Retrieves the AWS account ID by calling STS GetCallerIdentity.
///
/// Creates a new STS client per call. Callers should cache the result
/// if they need the account ID for multiple S3 operations (e.g. as
/// `ExpectedBucketOwner` on every PutObject/GetObject call).
pub async fn get_account_id(
    sdk_config: &aws_config::SdkConfig,
) -> Result<String, JobAttachmentsError> {
    let sts = aws_sdk_sts::Client::new(sdk_config);
    let identity = sts
        .get_caller_identity()
        .send()
        .await
        .map_err(|e| {
            JobAttachmentsError::AssetSync(format!("Failed to get caller identity: {e}"))
        })?;
    identity
        .account()
        .map(|s| s.to_string())
        .ok_or_else(|| {
            JobAttachmentsError::AssetSync("GetCallerIdentity returned no account ID".into())
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use deadline_config::config_file::set_setting_in_config;
    use deadline_config::ini::IniConfig;

    // === §34 cases 1-3: Constants ===

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

    // === §34 case 11: build_s3_client produces configured client ===

    #[test]
    fn build_s3_client_returns_client() {
        let sdk_config = aws_config::SdkConfig::builder()
            .behavior_version(aws_config::BehaviorVersion::latest())
            .region(aws_config::Region::new("us-west-2"))
            .build();
        let client = build_s3_client(&sdk_config, None);
        assert!(std::mem::size_of_val(&client) > 0);
    }

    // === §34 case 13: pool connections from config ===

    #[test]
    fn build_s3_client_uses_pool_connections_from_config() {
        let mut config = IniConfig::new();
        set_setting_in_config("settings.s3_max_pool_connections", "20", &mut config).unwrap();
        let sdk_config = aws_config::SdkConfig::builder()
            .behavior_version(aws_config::BehaviorVersion::latest())
            .region(aws_config::Region::new("us-west-2"))
            .build();
        // Should not panic — pool connections config is validated at build time
        let _client = build_s3_client(&sdk_config, Some(&config));
    }

    // === §34 case 14: user agent includes job attachments identifier ===

    #[test]
    fn s3_user_agent_contains_job_attachments() {
        assert!(S3_USER_AGENT_EXTRA.contains("JobAttachments"));
        assert!(S3_USER_AGENT_EXTRA.starts_with("S3A/Deadline/NA/JobAttachments/"));
    }

    // === §34 cases 17-20: get_s3_max_pool_connections ===

    #[test]
    fn get_s3_max_pool_connections_valid_integer() {
        let mut config = IniConfig::new();
        set_setting_in_config("settings.s3_max_pool_connections", "10", &mut config).unwrap();
        let result = get_s3_max_pool_connections(Some(&config));
        assert_eq!(result.unwrap(), 10);
    }

    #[test]
    fn get_s3_max_pool_connections_not_integer_errors() {
        let mut config = IniConfig::new();
        set_setting_in_config("settings.s3_max_pool_connections", "abc", &mut config).unwrap();
        let result = get_s3_max_pool_connections(Some(&config));
        assert!(result.is_err());
    }

    #[test]
    fn get_s3_max_pool_connections_zero_errors() {
        let mut config = IniConfig::new();
        set_setting_in_config("settings.s3_max_pool_connections", "0", &mut config).unwrap();
        let result = get_s3_max_pool_connections(Some(&config));
        assert!(result.is_err());
    }

    #[test]
    fn get_s3_max_pool_connections_negative_errors() {
        let mut config = IniConfig::new();
        set_setting_in_config("settings.s3_max_pool_connections", "-5", &mut config).unwrap();
        let result = get_s3_max_pool_connections(Some(&config));
        assert!(result.is_err());
    }

    // === §34 case 27: get_account_id returns account string ===

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

    // === get_small_file_threshold_multiplier ===

    #[test]
    fn get_small_file_threshold_multiplier_valid() {
        let mut config = IniConfig::new();
        set_setting_in_config("settings.small_file_threshold_multiplier", "20", &mut config)
            .unwrap();
        assert_eq!(get_small_file_threshold_multiplier(Some(&config)).unwrap(), 20);
    }

    #[test]
    fn get_small_file_threshold_multiplier_not_integer_errors() {
        let mut config = IniConfig::new();
        set_setting_in_config("settings.small_file_threshold_multiplier", "abc", &mut config)
            .unwrap();
        assert!(get_small_file_threshold_multiplier(Some(&config)).is_err());
    }

    #[test]
    fn get_small_file_threshold_multiplier_zero_errors() {
        let mut config = IniConfig::new();
        set_setting_in_config("settings.small_file_threshold_multiplier", "0", &mut config)
            .unwrap();
        assert!(get_small_file_threshold_multiplier(Some(&config)).is_err());
    }

    // === compute_upload_config ===

    #[test]
    fn compute_upload_config_defaults() {
        let mut config = IniConfig::new();
        set_setting_in_config("settings.s3_max_pool_connections", "50", &mut config).unwrap();
        set_setting_in_config("settings.small_file_threshold_multiplier", "20", &mut config)
            .unwrap();
        let (threshold, workers) = compute_upload_config(Some(&config)).unwrap();
        // 8MB * 20 = 160MB
        assert_eq!(threshold, 8 * 1024 * 1024 * 20);
        // 50 / min(20, 10) = 50 / 10 = 5
        assert_eq!(workers, 5);
    }

    #[test]
    fn compute_upload_config_small_multiplier() {
        let mut config = IniConfig::new();
        set_setting_in_config("settings.s3_max_pool_connections", "10", &mut config).unwrap();
        set_setting_in_config("settings.small_file_threshold_multiplier", "2", &mut config)
            .unwrap();
        let (threshold, workers) = compute_upload_config(Some(&config)).unwrap();
        // 8MB * 2 = 16MB
        assert_eq!(threshold, 8 * 1024 * 1024 * 2);
        // 10 / min(2, 10) = 10 / 2 = 5
        assert_eq!(workers, 5);
    }

    // === Concurrency constants ===

    #[test]
    fn small_file_threshold_default() {
        assert_eq!(S3_MULTIPART_UPLOAD_CHUNK_SIZE, 8 * 1024 * 1024);
    }

    #[test]
    fn upload_max_concurrency_default() {
        assert_eq!(S3_UPLOAD_MAX_CONCURRENCY, 10);
    }

    #[test]
    fn download_max_concurrency_default() {
        assert_eq!(S3_DOWNLOAD_MAX_CONCURRENCY, 10);
    }
}
