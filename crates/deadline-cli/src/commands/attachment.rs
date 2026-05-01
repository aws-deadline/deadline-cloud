use clap::Subcommand;
use deadline_config::config_file;
use deadline_config::ini::IniConfig;
use deadline_job_attachments::api::{attachment_download, attachment_upload};
use deadline_job_attachments::models::FileConflictResolution;
use deadline_job_attachments::s3;

use super::config::CliError;

#[derive(Subcommand)]
pub enum AttachmentAction {
    /// BETA - Download job attachment data files using manifest files
    Download {
        #[arg(short = 'm', long, required = true, num_args = 1..)]
        manifests: Vec<String>,
        #[arg(long)]
        s3_root_uri: Option<String>,
        #[arg(long)]
        path_mapping_rules: Option<String>,
        #[arg(long)]
        farm_id: Option<String>,
        #[arg(long)]
        queue_id: Option<String>,
        #[arg(long)]
        profile: Option<String>,
        #[arg(long, value_parser = parse_conflict_resolution)]
        conflict_resolution: Option<FileConflictResolution>,
        #[arg(long)]
        json: bool,
    },
    /// BETA - Upload job attachment data files using manifest files
    Upload {
        #[arg(short = 'm', long, required = true, num_args = 1..)]
        manifests: Vec<String>,
        #[arg(short = 'r', long, num_args = 1..)]
        root_dirs: Vec<String>,
        #[arg(long)]
        path_mapping_rules: Option<String>,
        #[arg(long)]
        s3_root_uri: Option<String>,
        #[arg(long)]
        upload_manifest_path: Option<String>,
        #[arg(long)]
        farm_id: Option<String>,
        #[arg(long)]
        queue_id: Option<String>,
        #[arg(long)]
        profile: Option<String>,
        #[arg(long)]
        json: bool,
    },
}

fn parse_conflict_resolution(s: &str) -> Result<FileConflictResolution, String> {
    match s.to_uppercase().as_str() {
        "SKIP" => Ok(FileConflictResolution::Skip),
        "OVERWRITE" => Ok(FileConflictResolution::Overwrite),
        "CREATE_COPY" => Ok(FileConflictResolution::CreateCopy),
        other => Err(format!(
            "Invalid conflict resolution: {other}. Use SKIP, OVERWRITE, or CREATE_COPY"
        )),
    }
}

/// Resolved S3 credentials and URI for attachment operations.
struct S3Context {
    sdk_config: aws_config::SdkConfig,
    s3_root_uri: String,
}

/// Resolve S3 credentials and root URI based on whether --profile was provided.
///
/// When --profile is provided: use profile credentials directly, require --s3-root-uri.
/// When --profile is absent: call get_queue() to derive S3 URI from queue settings,
/// then call get_queue_user_config() to get queue-scoped credentials (unconditional,
/// matching Python's get_queue_user_boto3_session pattern).
async fn resolve_s3_context(
    profile: &Option<String>,
    s3_root_uri: Option<String>,
    config: &IniConfig,
) -> Result<S3Context, CliError> {
    if profile.is_some() {
        // --profile provided: use profile credentials, require explicit S3 URI
        let uri = s3_root_uri
            .filter(|u| !u.is_empty())
            .ok_or_else(|| CliError::Operation("No valid s3 root path available".into()))?;
        let sdk_config = deadline_api::session::get_sdk_config(Some(config)).await;
        Ok(S3Context { sdk_config, s3_root_uri: uri })
    } else {
        // No --profile: derive S3 URI from queue settings, use queue-scoped credentials
        let farm_id = config_file::get_setting("defaults.farm_id", config)
            .map_err(|e| CliError::Operation(e.to_string()))?;
        let queue_id = config_file::get_setting("defaults.queue_id", config)
            .map_err(|e| CliError::Operation(e.to_string()))?;

        // Derive S3 root URI from queue's jobAttachmentSettings (unless explicitly provided)
        let uri = match s3_root_uri.filter(|u| !u.is_empty()) {
            Some(u) => u,
            None => {
                let queue = deadline_api::session::deadline_client(Some(config)).await
                    .get_queue().farm_id(&farm_id).queue_id(&queue_id)
                    .send().await
                    .map_err(|e| CliError::Operation(deadline_api::client::format_sdk_error(&e)))?;
                match queue.job_attachment_settings() {
                    Some(s) if !s.s3_bucket_name().is_empty() => {
                        let bucket = s.s3_bucket_name();
                        let prefix = s.root_prefix();
                        format!("s3://{bucket}/{prefix}")
                    }
                    _ => {
                        return Err(CliError::Operation(format!(
                            "Queue {queue_id} has no attachment settings"
                        )));
                    }
                }
            }
        };

        // Get queue-scoped credentials (unconditional — matches Python)
        let sdk_config =
            deadline_api::session::get_queue_user_config(Some(&farm_id), Some(&queue_id), None, false, Some(config))
                .await
                .map_err(|e| CliError::Operation(e.to_string()))?;

        Ok(S3Context { sdk_config, s3_root_uri: uri })
    }
}

pub fn run(action: AttachmentAction) -> Result<(), CliError> {
    tokio::runtime::Runtime::new()
        .map_err(|e| CliError::Operation(e.to_string()))?
        .block_on(run_async(action))
}

async fn run_async(action: AttachmentAction) -> Result<(), CliError> {
    match action {
        AttachmentAction::Download {
            manifests, s3_root_uri, path_mapping_rules,
            farm_id, queue_id, profile, conflict_resolution, json,
        } => {
            let mut config = config_file::read_config()
                .map_err(|e| CliError::Operation(e.to_string()))?;
            crate::common::apply_cli_options_to_config(
                &mut config,
                &crate::common::CliOptions {
                    profile: profile.clone(), farm_id, queue_id, job_id: None, yes: false, ..Default::default()
                },
                &[],
            )?;

            let ctx = resolve_s3_context(&profile, s3_root_uri, &config).await?;

            // Resolve conflict resolution
            let resolution = conflict_resolution.unwrap_or_else(|| {
                let setting = config_file::get_setting(
                    "settings.conflict_resolution",
                    &config,
                )
                .unwrap_or_default();
                match setting.to_uppercase().as_str() {
                    "SKIP" => FileConflictResolution::Skip,
                    "OVERWRITE" => FileConflictResolution::Overwrite,
                    _ => FileConflictResolution::CreateCopy,
                }
            });

            let s3_client = s3::build_s3_client(&ctx.sdk_config, Some(&config));
            let account_id = s3::get_account_id(&ctx.sdk_config)
                .await
                .map_err(|e| CliError::Operation(e.to_string()))?;

            let stats = attachment_download(
                &manifests,
                &ctx.s3_root_uri,
                &s3_client,
                &account_id,
                path_mapping_rules.as_deref(),
                None,
                resolution,
            )
            .await
            .map_err(|e| CliError::Operation(e.to_string()))?;

            if json {
                println!("{}", serde_json::to_string(&stats.stats).unwrap_or_default());
            } else {
                println!("{}", stats.stats);
            }
            Ok(())
        }
        AttachmentAction::Upload {
            manifests, root_dirs, path_mapping_rules, s3_root_uri,
            upload_manifest_path, farm_id, queue_id, profile, json: _,
        } => {
            let mut config = config_file::read_config()
                .map_err(|e| CliError::Operation(e.to_string()))?;
            crate::common::apply_cli_options_to_config(
                &mut config,
                &crate::common::CliOptions {
                    profile: profile.clone(), farm_id, queue_id, job_id: None, yes: false, ..Default::default()
                },
                &[],
            )?;

            let ctx = resolve_s3_context(&profile, s3_root_uri, &config).await?;

            let s3_client = s3::build_s3_client(&ctx.sdk_config, Some(&config));
            let account_id = s3::get_account_id(&ctx.sdk_config)
                .await
                .map_err(|e| CliError::Operation(e.to_string()))?;

            let _result = attachment_upload(
                &manifests,
                &ctx.s3_root_uri,
                &s3_client,
                &account_id,
                &root_dirs,
                path_mapping_rules.as_deref(),
                upload_manifest_path.as_deref(),
                None,
                Some(&config),
            )
            .await
            .map_err(|e| CliError::Operation(e.to_string()))?;

            Ok(())
        }
    }
}
