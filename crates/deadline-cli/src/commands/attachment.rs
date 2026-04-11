use clap::Subcommand;
use deadline_config::config_file;
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
        other => Err(format!("Invalid conflict resolution: {other}. Use SKIP, OVERWRITE, or CREATE_COPY")),
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
            let mut config = deadline_config::config_file::read_config()
                .map_err(|e| CliError::Operation(e.to_string()))?;
            crate::common::apply_cli_options_to_config(
                &mut config,
                &crate::common::CliOptions {
                    profile: profile.clone(), farm_id, queue_id, job_id: None, yes: false,
                },
                &[],
            ).map_err(CliError::Operation)?;

            // Resolve S3 root URI
            let uri = match s3_root_uri {
                Some(u) if profile.is_some() => u,
                _ => {
                    // Derive from queue settings
                    let q_id = config_file::get_setting_with_config("defaults.queue_id", &config)
                        .map_err(|e| CliError::Operation(e.to_string()))?;
                    let f_id = config_file::get_setting_with_config("defaults.farm_id", &config)
                        .map_err(|e| CliError::Operation(e.to_string()))?;
                    if q_id.is_empty() || f_id.is_empty() {
                        return Err(CliError::Operation("No valid s3 root path available".into()));
                    }
                    s3_root_uri.unwrap_or_default()
                }
            };

            if uri.is_empty() {
                return Err(CliError::Operation("No valid s3 root path available".into()));
            }

            // Resolve conflict resolution
            let resolution = conflict_resolution.unwrap_or_else(|| {
                let setting = config_file::get_setting_with_config("settings.conflict_resolution", &config)
                    .unwrap_or_default();
                match setting.to_uppercase().as_str() {
                    "SKIP" => FileConflictResolution::Skip,
                    "OVERWRITE" => FileConflictResolution::Overwrite,
                    _ => FileConflictResolution::CreateCopy,
                }
            });

            let sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
                .load().await;
            let s3_client = s3::build_s3_client(&sdk_config, Some(&config));
            let account_id = s3::get_account_id(&sdk_config).await
                .map_err(|e| CliError::Operation(e.to_string()))?;

            let stats = attachment_download(
                &manifests.iter().map(|s| s.as_str().to_string()).collect::<Vec<_>>(),
                &uri,
                &s3_client,
                &account_id,
                path_mapping_rules.as_deref(),
                None,
                resolution,
            ).await.map_err(|e| CliError::Operation(e.to_string()))?;

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
            let mut config = deadline_config::config_file::read_config()
                .map_err(|e| CliError::Operation(e.to_string()))?;
            crate::common::apply_cli_options_to_config(
                &mut config,
                &crate::common::CliOptions {
                    profile: profile.clone(), farm_id, queue_id, job_id: None, yes: false,
                },
                &[],
            ).map_err(CliError::Operation)?;

            let uri = match s3_root_uri {
                Some(u) if profile.is_some() => u,
                _ => {
                    return Err(CliError::Operation("No valid s3 root path available".into()));
                }
            };

            let sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
                .load().await;
            let s3_client = s3::build_s3_client(&sdk_config, Some(&config));
            let account_id = s3::get_account_id(&sdk_config).await
                .map_err(|e| CliError::Operation(e.to_string()))?;

            let _result = attachment_upload(
                &manifests,
                &uri,
                &s3_client,
                &account_id,
                &root_dirs,
                path_mapping_rules.as_deref(),
                upload_manifest_path.as_deref(),
                None,
                Some(&config),
            ).await.map_err(|e| CliError::Operation(e.to_string()))?;

            Ok(())
        }
    }
}
