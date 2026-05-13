use clap::Subcommand;
use deadline_job_attachments::manifest_ops::{
    manifest_diff, manifest_snapshot, manifest_upload, resolve_glob_config,
};

use super::config::CliError;

#[derive(Subcommand)]
pub(crate) enum ManifestAction {
    /// BETA - Create a manifest snapshot of files in a directory
    Snapshot {
        #[arg(long, required = true)]
        root: String,
        #[arg(short = 'd', long)]
        destination: Option<String>,
        #[arg(short = 'n', long)]
        name: Option<String>,
        #[arg(short = 'i', long, num_args = 1..)]
        include: Vec<String>,
        #[arg(short = 'e', long, num_args = 1..)]
        exclude: Vec<String>,
        #[arg(long, visible_alias = "ie")]
        include_exclude_config: Option<String>,
        #[arg(long)]
        diff: Option<String>,
        #[arg(long)]
        force_rehash: bool,
        #[arg(long)]
        json: bool,
    },
    /// BETA - Compute file differences against a manifest
    Diff {
        #[arg(long, required = true)]
        manifest: String,
        #[arg(long)]
        root: Option<String>,
        #[arg(short = 'i', long, num_args = 1..)]
        include: Vec<String>,
        #[arg(short = 'e', long, num_args = 1..)]
        exclude: Vec<String>,
        #[arg(long, visible_alias = "ie")]
        include_exclude_config: Option<String>,
        #[arg(long)]
        force_rehash: bool,
        #[arg(long)]
        json: bool,
    },
    /// BETA - Download job attachment manifests
    Download {
        download_dir: String,
        #[arg(long, required = true)]
        job_id: String,
        #[arg(long)]
        step_id: Option<String>,
        #[arg(long)]
        farm_id: Option<String>,
        #[arg(long)]
        queue_id: Option<String>,
        #[arg(long)]
        profile: Option<String>,
        #[arg(long, default_value = "all")]
        asset_type: String,
        #[arg(long)]
        json: bool,
    },
    /// BETA - Upload a manifest to S3 CAS
    Upload {
        manifest_file: String,
        #[arg(long)]
        s3_cas_uri: Option<String>,
        #[arg(long)]
        s3_manifest_prefix: Option<String>,
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

pub(crate) fn run(action: ManifestAction) -> Result<(), CliError> {
    match action {
        ManifestAction::Snapshot { .. } | ManifestAction::Diff { .. } => run_sync(action),
        _ => tokio::runtime::Runtime::new()
            .map_err(|e| CliError::Operation(e.to_string()))?
            .block_on(run_async(action)),
    }
}

fn run_sync(action: ManifestAction) -> Result<(), CliError> {
    match action {
        ManifestAction::Snapshot {
            root,
            destination,
            name,
            include,
            exclude,
            include_exclude_config,
            diff,
            force_rehash,
            json,
        } => {
            if !std::path::Path::new(&root).is_dir() {
                return Err(CliError::Operation(format!(
                    "Specified root directory {root} does not exist."
                )));
            }

            let dest = if let Some(ref d) = destination {
                if !std::path::Path::new(d).is_dir() {
                    return Err(CliError::Operation(format!(
                        "Specified destination directory {d} does not exist."
                    )));
                }
                d.clone()
            } else {
                if !json {
                    println!("Manifest creation path defaulted to {root} \n");
                }
                root.clone()
            };

            let config = resolve_glob_config(&include, &exclude, include_exclude_config.as_deref())
                .map_err(|e| CliError::Operation(e.to_string()))?;

            let result = manifest_snapshot(
                &root,
                &dest,
                name.as_deref(),
                &config,
                diff.as_deref(),
                force_rehash,
                None,
            )
            .map_err(|e| CliError::Operation(e.to_string()))?;

            if let Some(snap) = result {
                if !json {
                    println!("Manifest generated at {}", snap.manifest);
                }
                if json {
                    println!("{}", serde_json::to_string(&snap).unwrap_or_default());
                }
            }
            Ok(())
        }
        ManifestAction::Diff {
            manifest,
            root,
            include,
            exclude,
            include_exclude_config,
            force_rehash,
            json,
        } => {
            if !std::path::Path::new(&manifest).is_file() {
                return Err(CliError::Operation(format!(
                    "Specified manifest file {manifest} does not exist. "
                )));
            }
            // Derive root from manifest's parent directory when not specified
            let root = root.unwrap_or_else(|| {
                std::path::Path::new(&manifest)
                    .parent()
                    .map_or_else(|| ".".to_owned(), |p| p.to_string_lossy().to_string())
            });
            if !std::path::Path::new(&root).is_dir() {
                return Err(CliError::Operation(format!(
                    "Specified root directory {root} does not exist. "
                )));
            }

            let config = resolve_glob_config(&include, &exclude, include_exclude_config.as_deref())
                .map_err(|e| CliError::Operation(e.to_string()))?;

            let differences = manifest_diff(&manifest, &root, &config, force_rehash, None)
                .map_err(|e| CliError::Operation(e.to_string()))?;

            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&differences).unwrap_or_default()
                );
            } else {
                println!("Manifest Diff of root directory: {root}");
                if !differences.new.is_empty() {
                    println!("New files:");
                    for f in &differences.new {
                        println!("  + {f}");
                    }
                }
                if !differences.modified.is_empty() {
                    println!("Modified files:");
                    for f in &differences.modified {
                        println!("  M {f}");
                    }
                }
                if !differences.deleted.is_empty() {
                    println!("Deleted files:");
                    for f in &differences.deleted {
                        println!("  - {f}");
                    }
                }
                if differences.new.is_empty()
                    && differences.modified.is_empty()
                    && differences.deleted.is_empty()
                {
                    println!("No differences found.");
                }
            }
            Ok(())
        }
        _ => unreachable!(),
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "manifest download/upload are sequential pipelines"
)]
async fn run_async(action: ManifestAction) -> Result<(), CliError> {
    match action {
        ManifestAction::Download {
            download_dir,
            job_id,
            step_id: _,
            farm_id,
            queue_id,
            profile,
            asset_type,
            json: _,
        } => {
            if !std::path::Path::new(&download_dir).is_dir() {
                return Err(CliError::Operation(format!(
                    "Specified destination directory {download_dir} does not exist. "
                )));
            }

            let mut config = deadline_config::config_file::read_config()
                .map_err(|e| CliError::Operation(e.to_string()))?;
            crate::common::apply_cli_options_to_config(
                &mut config,
                &crate::common::CliOptions {
                    profile,
                    farm_id,
                    queue_id,
                    job_id: None,
                    yes: false,
                    ..Default::default()
                },
                &["farm_id", "queue_id"],
            )?;

            let farm = deadline_config::config_file::get_setting("defaults.farm_id", &config)
                .unwrap_or_default();
            let queue = deadline_config::config_file::get_setting("defaults.queue_id", &config)
                .unwrap_or_default();

            let _asset = match asset_type.to_lowercase().as_str() {
                "input" => deadline_job_attachments::manifest_ops::AssetType::Input,
                "output" => deadline_job_attachments::manifest_ops::AssetType::Output,
                _ => deadline_job_attachments::manifest_ops::AssetType::All,
            };

            // Get queue attachment settings
            let queue_resp = deadline_api::session::deadline_client(&config)
                .await
                .get_queue()
                .farm_id(&farm)
                .queue_id(&queue)
                .send()
                .await
                .map_err(|e| {
                    CliError::Operation(format!(
                        "Failed to get queue: {}",
                        deadline_api::client::format_sdk_error(&e)
                    ))
                })?;
            let ja_settings = queue_resp.job_attachment_settings().ok_or_else(|| {
                CliError::Operation(
                    "Queue does not have job attachment settings configured.".into(),
                )
            })?;
            let bucket = ja_settings.s3_bucket_name();
            let prefix = ja_settings.root_prefix();

            // Get job to check for attachments
            let job_output = deadline_api::session::deadline_client(&config)
                .await
                .get_job()
                .farm_id(&farm)
                .queue_id(&queue)
                .job_id(&job_id)
                .send()
                .await
                .map_err(|e| {
                    CliError::Operation(format!(
                        "Failed to get job: {}",
                        deadline_api::client::format_sdk_error(&e)
                    ))
                })?;
            let attachments_sdk = job_output.attachments().ok_or_else(|| {
                CliError::Operation("Job has no attachments — no manifests to download.".into())
            })?;
            let manifests_sdk = attachments_sdk.manifests();

            if manifests_sdk.is_empty() {
                return Err(CliError::Operation(
                    "Job has no manifest entries — no manifests to download.".into(),
                ));
            }

            // Get queue-scoped credentials
            let sdk_config =
                deadline_api::session::get_queue_scoped_config(&farm, &queue, &config)
                    .await
                    .map_err(|e| CliError::Operation(format!("Failed to get credentials: {e}")))?;

            let s3_client =
                deadline_job_attachments::s3::build_s3_client(&sdk_config, &config);
            let account_id = deadline_job_attachments::s3::get_account_id(&sdk_config)
                .await
                .map_err(|e| CliError::Operation(e.to_string()))?;

            // Download manifests for each entry
            let mut downloaded = 0;
            for manifest_entry in manifests_sdk {
                if let Some(manifest_path) = manifest_entry.input_manifest_path() {
                    // Filter by step if specified (ManifestProperties doesn't have stepId — skip filter)
                    let key = format!("{prefix}/Manifests/{manifest_path}");
                    let dest_path = std::path::Path::new(&download_dir).join(
                        std::path::Path::new(manifest_path)
                            .file_name()
                            .unwrap_or_default(),
                    );
                    let result = s3_client
                        .get_object()
                        .bucket(bucket)
                        .key(&key)
                        .expected_bucket_owner(&account_id)
                        .send()
                        .await;
                    match result {
                        Ok(output) => {
                            let bytes = output
                                .body
                                .collect()
                                .await
                                .map_err(|e| {
                                    CliError::Operation(format!("Failed to read S3 body: {e}"))
                                })?
                                .into_bytes();
                            std::fs::write(&dest_path, &bytes).map_err(|e| {
                                CliError::Operation(format!(
                                    "Failed to write {}: {e}",
                                    dest_path.display()
                                ))
                            })?;
                            println!("Downloaded: {}", dest_path.display());
                            downloaded += 1;
                        }
                        Err(e) => {
                            eprintln!("Warning: Failed to download {manifest_path}: {e}");
                        }
                    }
                }
            }
            println!("Downloaded {downloaded} manifest(s) to {download_dir}");
            Ok(())
        }
        ManifestAction::Upload {
            manifest_file,
            s3_cas_uri,
            s3_manifest_prefix,
            farm_id,
            queue_id,
            profile,
            json: _,
        } => {
            if !std::path::Path::new(&manifest_file).is_file() {
                return Err(CliError::Operation(format!(
                    "Specified manifest {manifest_file} does not exist. "
                )));
            }

            let (bucket, cas_prefix, sdk_config, config) = if let Some(ref uri) = s3_cas_uri {
                let settings =
                    deadline_job_attachments::models::JobAttachmentS3Settings::from_s3_root_uri(
                        uri,
                    )
                    .map_err(|e| CliError::Operation(e.to_string()))?;
                let cfg = aws_config::defaults(aws_config::BehaviorVersion::latest())
                    .load()
                    .await;
                let config = deadline_config::config_file::read_config()
                    .unwrap_or_else(|_| deadline_config::ini::IniConfig::new());
                (settings.s3_bucket_name, settings.root_prefix, cfg, config)
            } else {
                // Derive from queue
                let mut config = deadline_config::config_file::read_config()
                    .map_err(|e| CliError::Operation(e.to_string()))?;
                crate::common::apply_cli_options_to_config(
                    &mut config,
                    &crate::common::CliOptions {
                        profile,
                        farm_id,
                        queue_id,
                        job_id: None,
                        yes: false,
                        ..Default::default()
                    },
                    &["farm_id", "queue_id"],
                )?;
                let farm = deadline_config::config_file::get_setting("defaults.farm_id", &config)
                    .unwrap_or_default();
                let queue = deadline_config::config_file::get_setting("defaults.queue_id", &config)
                    .unwrap_or_default();

                let queue_resp = deadline_api::session::deadline_client(&config)
                    .await
                    .get_queue()
                    .farm_id(&farm)
                    .queue_id(&queue)
                    .send()
                    .await
                    .map_err(|e| {
                        CliError::Operation(format!(
                            "Failed to get queue: {}",
                            deadline_api::client::format_sdk_error(&e)
                        ))
                    })?;
                let ja_settings = queue_resp.job_attachment_settings().ok_or_else(|| {
                    CliError::Operation(
                        "Queue does not have job attachment settings not configured.".into(),
                    )
                })?;
                let b = ja_settings.s3_bucket_name().to_owned();
                let p = ja_settings.root_prefix().to_owned();

                let cfg =
                    deadline_api::session::get_queue_scoped_config(&farm, &queue, &config)
                        .await
                        .map_err(|e| {
                            CliError::Operation(format!("Failed to get credentials: {e}"))
                        })?;
                (b, p, cfg, config)
            };

            let s3_client = deadline_job_attachments::s3::build_s3_client(&sdk_config, &config);
            let account_id = deadline_job_attachments::s3::get_account_id(&sdk_config)
                .await
                .map_err(|e| CliError::Operation(e.to_string()))?;

            println!(
                "Uploading Manifest to {bucket} {cas_prefix} Manifests, prefix: {:?}",
                s3_manifest_prefix.as_deref().unwrap_or("None")
            );

            manifest_upload(
                &manifest_file,
                &bucket,
                &cas_prefix,
                &s3_client,
                &account_id,
                s3_manifest_prefix.as_deref(),
                None,
            )
            .await
            .map_err(|e| CliError::Operation(e.to_string()))?;

            println!("Uploading successful!");
            Ok(())
        }
        _ => unreachable!(),
    }
}
