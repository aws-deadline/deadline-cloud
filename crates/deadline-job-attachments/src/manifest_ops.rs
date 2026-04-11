//! Manifest lifecycle operations: glob, snapshot, diff, merge, write.

use std::path::Path;

use deadline_models::errors::JobAttachmentsError;
use serde::Serialize;

use crate::api::read_manifests;
use crate::asset_manifests::{decode_manifest, hash_data, AssetManifest, HashAlgorithm};
use crate::diff::{fast_diff, hash_diff, FileStatus};
use crate::download::merge_asset_manifests;
use crate::models::AssetRootGroup;
use crate::progress_tracker::ProgressReportMetadata;
use crate::upload::{hash_assets_and_create_manifest, prepare_paths_for_upload};

// --- Types ---

/// Glob include/exclude configuration.
#[derive(Debug, Clone)]
pub struct GlobConfig {
    pub include: Vec<String>,
    pub exclude: Vec<String>,
}

impl Default for GlobConfig {
    fn default() -> Self {
        Self {
            include: vec!["**/*".into()],
            exclude: vec![],
        }
    }
}

/// Result of a manifest snapshot operation.
#[derive(Debug, Clone, Serialize)]
pub struct ManifestSnapshot {
    pub root: String,
    pub manifest: String,
}

/// Result of a manifest diff operation.
#[derive(Debug, Clone, Serialize)]
pub struct ManifestDiffResult {
    pub new: Vec<String>,
    pub modified: Vec<String>,
    pub deleted: Vec<String>,
}

/// Result of a manifest merge operation.
#[derive(Debug, Clone, Serialize)]
pub struct ManifestMergeResult {
    pub manifest_root: String,
    pub local_manifest_path: String,
}

// --- Functions ---

/// Resolve glob configuration from CLI arguments.
///
/// If include/exclude are non-empty, they take precedence. Otherwise
/// parse include_exclude_config as a file path or JSON string. Falls
/// back to default (all files).
pub fn resolve_glob_config(
    include: &[String],
    exclude: &[String],
    include_exclude_config: Option<&str>,
) -> Result<GlobConfig, JobAttachmentsError> {
    if !include.is_empty() || !exclude.is_empty() {
        return Ok(GlobConfig {
            include: if include.is_empty() {
                vec!["**/*".into()]
            } else {
                include.to_vec()
            },
            exclude: exclude.to_vec(),
        });
    }

    if let Some(config_input) = include_exclude_config {
        let json_str = match std::fs::read_to_string(config_input) {
            Ok(contents) => contents,
            Err(_) => config_input.to_string(),
        };

        let parsed: serde_json::Value = serde_json::from_str(&json_str).map_err(|_| {
            JobAttachmentsError::AssetSync(format!(
                "Glob input {config_input} cannot be deserialized as JSON"
            ))
        })?;

        let include = parsed
            .get("include")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_else(|| vec!["**/*".into()]);

        let exclude = parsed
            .get("exclude")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        return Ok(GlobConfig { include, exclude });
    }

    Ok(GlobConfig::default())
}

/// Return absolute paths of all files matching the glob config under root.
pub fn glob_files(root: &str, config: &GlobConfig) -> Result<Vec<String>, JobAttachmentsError> {
    let base = std::path::absolute(Path::new(root))
        .map_err(|e| JobAttachmentsError::AssetSync(format!("Invalid root path: {e}")))?;

    let mut matched: std::collections::HashSet<String> = std::collections::HashSet::new();

    for pattern in &config.include {
        let full_pattern = base.join(pattern).to_string_lossy().into_owned();
        for entry in glob::glob(&full_pattern).map_err(|e| {
            JobAttachmentsError::AssetSync(format!("Invalid glob pattern: {e}"))
        })? {
            if let Ok(path) = entry {
                if path.is_file() {
                    let normalized = path
                        .canonicalize()
                        .unwrap_or(path)
                        .to_string_lossy()
                        .into_owned();
                    matched.insert(normalized);
                }
            }
        }
    }

    for pattern in &config.exclude {
        let full_pattern = base.join(pattern).to_string_lossy().into_owned();
        if let Ok(entries) = glob::glob(&full_pattern) {
            for entry in entries.flatten() {
                let normalized = entry
                    .canonicalize()
                    .unwrap_or(entry)
                    .to_string_lossy()
                    .into_owned();
                matched.remove(&normalized);
            }
        }
    }

    Ok(matched.into_iter().collect())
}

/// Write a manifest to disk. Returns the written file path.
pub fn write_manifest(
    root: &str,
    manifest: &AssetManifest,
    destination: &str,
    name: Option<&str>,
) -> Result<String, JobAttachmentsError> {
    let root_hash = hash_data(root.as_bytes(), HashAlgorithm::Xxh128);
    let timestamp = chrono::Local::now().format("%Y-%m-%dT%H-%M-%S").to_string();

    let manifest_name = match name {
        Some(n) => n.to_string(),
        None => {
            let derived = root.replace('/', "_").replace('\\', "_").replace(':', "_");
            if derived.starts_with('_') {
                derived[1..].to_string()
            } else {
                derived
            }
        }
    };

    let filename = format!("{manifest_name}-{root_hash}-{timestamp}.manifest");
    let dest_path = Path::new(destination).join(&filename);

    if let Some(parent) = dest_path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| {
            JobAttachmentsError::AssetSync(format!("Failed to create directory: {e}"))
        })?;
    }

    std::fs::write(&dest_path, manifest.encode()).map_err(|e| {
        JobAttachmentsError::AssetSync(format!("Failed to write manifest: {e}"))
    })?;

    Ok(dest_path.to_string_lossy().into_owned())
}

/// Create a manifest snapshot of files in a directory.
pub fn manifest_snapshot(
    root: &str,
    destination: &str,
    name: Option<&str>,
    config: &GlobConfig,
    diff: Option<&str>,
    force_rehash: bool,
    callback: Option<Box<dyn Fn(ProgressReportMetadata) -> bool + Send>>,
) -> Result<Option<ManifestSnapshot>, JobAttachmentsError> {
    let current_files = glob_files(root, config)?;
    if current_files.is_empty() && diff.is_none() {
        return Ok(None);
    }

    let output_manifest = if let Some(diff_path) = diff {
        let diff_contents = std::fs::read_to_string(diff_path).map_err(|e| {
            JobAttachmentsError::AssetSync(format!("Failed to read diff manifest: {e}"))
        })?;
        let diff_manifest = decode_manifest(&diff_contents)?;

        let changed_paths: Vec<String> = if force_rehash {
            // Hash all files, compare manifests
            let group = build_single_group(root, &current_files);
            let (_, manifests) = hash_assets_and_create_manifest(
                &[group],
                current_files.len() as u64,
                total_bytes(&current_files),
                None,
                callback,
            )?;
            let current_manifest = manifests
                .first()
                .and_then(|m| m.asset_manifest.as_ref());
            match current_manifest {
                None => return Ok(None),
                Some(cm) => {
                    let diffs = hash_diff(&diff_manifest, cm);
                    diffs
                        .into_iter()
                        .filter(|(s, _)| *s == FileStatus::New || *s == FileStatus::Modified)
                        .map(|(_, p)| {
                            Path::new(root)
                                .join(&p.path)
                                .to_string_lossy()
                                .into_owned()
                        })
                        .collect()
                }
            }
        } else {
            let diffs = fast_diff(root, &current_files, &diff_manifest);
            diffs
                .into_iter()
                .filter(|(_, s)| *s != FileStatus::Deleted)
                .map(|(p, _)| {
                    Path::new(root).join(&p).to_string_lossy().into_owned()
                })
                .collect()
        };

        if changed_paths.is_empty() {
            return Ok(None);
        }

        // Hash only the changed files
        let group = build_single_group(root, &changed_paths);
        let (_, manifests) = hash_assets_and_create_manifest(
            &[group],
            changed_paths.len() as u64,
            total_bytes(&changed_paths),
            None,
            None,
        )?;
        manifests
            .into_iter()
            .next()
            .and_then(|m| m.asset_manifest)
    } else {
        // Full snapshot
        let group = build_single_group(root, &current_files);
        let (_, manifests) = hash_assets_and_create_manifest(
            &[group],
            current_files.len() as u64,
            total_bytes(&current_files),
            None,
            callback,
        )?;
        manifests
            .into_iter()
            .next()
            .and_then(|m| m.asset_manifest)
    };

    match output_manifest {
        None => Ok(None),
        Some(manifest) => {
            let path = write_manifest(root, &manifest, destination, name)?;
            Ok(Some(ManifestSnapshot {
                root: root.to_string(),
                manifest: path,
            }))
        }
    }
}

/// Compute file differences between a manifest and a directory.
pub fn manifest_diff(
    manifest_path: &str,
    root: &str,
    config: &GlobConfig,
    force_rehash: bool,
    callback: Option<Box<dyn Fn(ProgressReportMetadata) -> bool + Send>>,
) -> Result<ManifestDiffResult, JobAttachmentsError> {
    let contents = std::fs::read_to_string(manifest_path).map_err(|e| {
        JobAttachmentsError::AssetSync(format!("Failed to read manifest: {e}"))
    })?;
    let reference = decode_manifest(&contents)?;
    let current_files = glob_files(root, config)?;

    let mut result = ManifestDiffResult {
        new: Vec::new(),
        modified: Vec::new(),
        deleted: Vec::new(),
    };

    if force_rehash {
        let group = build_single_group(root, &current_files);
        let (_, manifests) = hash_assets_and_create_manifest(
            &[group],
            current_files.len() as u64,
            total_bytes(&current_files),
            None,
            callback,
        )?;
        if let Some(current) = manifests.first().and_then(|m| m.asset_manifest.as_ref()) {
            for (status, path) in hash_diff(&reference, current) {
                match status {
                    FileStatus::New => result.new.push(path.path),
                    FileStatus::Modified => result.modified.push(path.path),
                    FileStatus::Deleted => result.deleted.push(path.path),
                    FileStatus::Unchanged => {}
                }
            }
        }
    } else {
        for (path, status) in fast_diff(root, &current_files, &reference) {
            match status {
                FileStatus::New => result.new.push(path),
                FileStatus::Modified => result.modified.push(path),
                FileStatus::Deleted => result.deleted.push(path),
                FileStatus::Unchanged => {}
            }
        }
    }

    Ok(result)
}

/// Merge multiple manifest files into one.
pub fn manifest_merge(
    root: &str,
    manifest_files: &[String],
    destination: &str,
    name: Option<&str>,
    _callback: Option<Box<dyn Fn(ProgressReportMetadata) -> bool + Send>>,
) -> Result<Option<ManifestMergeResult>, JobAttachmentsError> {
    let manifest_map = read_manifests(manifest_files)?;
    let manifests: Vec<AssetManifest> = manifest_map.into_values().collect();

    let merged = merge_asset_manifests(&manifests)?;

    match merged {
        None => Ok(None),
        Some(manifest) => {
            let path = write_manifest(root, &manifest, destination, name)?;
            Ok(Some(ManifestMergeResult {
                manifest_root: root.to_string(),
                local_manifest_path: path,
            }))
        }
    }
}

// --- Internal helpers ---

fn build_single_group(root: &str, files: &[String]) -> AssetRootGroup {
    let mut inputs = std::collections::BTreeSet::new();
    for f in files {
        inputs.insert(std::path::PathBuf::from(f));
    }
    AssetRootGroup {
        file_system_location_name: None,
        root_path: root.to_string(),
        inputs,
        outputs: std::collections::BTreeSet::new(),
        references: std::collections::BTreeSet::new(),
    }
}

fn total_bytes(files: &[String]) -> u64 {
    files
        .iter()
        .filter_map(|f| std::fs::metadata(f).ok())
        .map(|m| m.len())
        .sum()
}
