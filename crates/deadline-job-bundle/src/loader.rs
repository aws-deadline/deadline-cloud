use deadline_api::errors::DeadlineError;
use std::fs;
use std::path::Path;

fn op_err(msg: String) -> DeadlineError {
    DeadlineError::OperationError(msg)
}

pub fn validate_directory_symlink_containment(job_bundle_dir: &Path) -> Result<(), DeadlineError> {
    let resolved_root = fs::canonicalize(job_bundle_dir).map_err(|_| {
        op_err(format!(
            "Job bundle path provided is not a directory:\n{}",
            job_bundle_dir.display()
        ))
    })?;
    if !resolved_root.is_dir() {
        return Err(op_err(format!(
            "Job bundle path provided is not a directory:\n{}",
            job_bundle_dir.display()
        )));
    }
    walk_and_check(&resolved_root, &resolved_root)
}

fn walk_and_check(resolved_root: &Path, dir: &Path) -> Result<(), DeadlineError> {
    let entries = fs::read_dir(dir)
        .map_err(|e| op_err(format!("Failed to read directory {}: {e}", dir.display())))?;
    for entry in entries {
        let entry = entry.map_err(|e| op_err(format!("Failed to read directory entry: {e}")))?;
        let path = entry.path();
        let resolved = fs::canonicalize(&path)
            .map_err(|e| op_err(format!("Failed to resolve path {}: {e}", path.display())))?;
        if !resolved.starts_with(resolved_root) {
            return Err(op_err(format!(
                "Job bundle cannot contain a path that resolves outside of the resolved bundle directory:\n{}\n\nPath in bundle:\n{}\nResolves to:\n{}",
                resolved_root.display(),
                path.display(),
                resolved.display()
            )));
        }
        if path.is_dir()
            && !path
                .symlink_metadata()
                .map(|m| m.is_symlink())
                .unwrap_or(false)
        {
            walk_and_check(resolved_root, &path)?;
        }
    }
    Ok(())
}

pub fn read_yaml_or_json(
    job_bundle_dir: &Path,
    filename: &str,
    required: bool,
) -> Result<(String, String), DeadlineError> {
    let base = job_bundle_dir.join(filename);
    let json_path = format!("{}.json", base.display());
    let yaml_path = format!("{}.yaml", base.display());
    let has_json = Path::new(&json_path).is_file();
    let has_yaml = Path::new(&yaml_path).is_file();

    match (has_json, has_yaml) {
        (true, true) => Err(op_err(format!(
            "Job bundle directory has both {filename}.json and {filename}.yaml, only one is permitted:\n{}",
            job_bundle_dir.display()
        ))),
        (true, false) => Ok((
            fs::read_to_string(&json_path)
                .map_err(|e| op_err(format!("Failed to read {json_path}: {e}")))?,
            "JSON".into(),
        )),
        (false, true) => Ok((
            fs::read_to_string(&yaml_path)
                .map_err(|e| op_err(format!("Failed to read {yaml_path}: {e}")))?,
            "YAML".into(),
        )),
        (false, false) if required => Err(op_err(format!(
            "Job bundle directory lacks a {filename}.json or {filename}.yaml:\n{}",
            job_bundle_dir.display()
        ))),
        _ => Ok((String::new(), String::new())),
    }
}

pub fn parse_yaml_or_json_content(
    file_contents: &str,
    file_type: &str,
    bundle_dir: &Path,
    filename: &str,
) -> Result<serde_json::Value, DeadlineError> {
    match file_type {
        "JSON" => serde_json::from_str(file_contents)
            .map_err(|e| op_err(format!("Error loading '{filename}.json':\n{e}"))),
        "YAML" => serde_yaml::from_str(file_contents)
            .map_err(|e| op_err(format!("Error loading '{filename}.yaml':\n{e}"))),
        _ => Err(op_err(format!(
            "Unexpected file type '{file_type}' in job bundle:\n{}",
            bundle_dir.display()
        ))),
    }
}

pub fn read_yaml_or_json_object(
    bundle_dir: &Path,
    filename: &str,
    required: bool,
) -> Result<Option<serde_json::Value>, DeadlineError> {
    let (contents, file_type) = read_yaml_or_json(bundle_dir, filename, required)?;
    if contents.is_empty() && file_type.is_empty() {
        Ok(None)
    } else {
        parse_yaml_or_json_content(&contents, &file_type, bundle_dir, filename).map(Some)
    }
}

pub fn save_yaml_or_json_to_file(
    bundle_dir: &Path,
    filename: &str,
    file_type: &str,
    data: &serde_json::Value,
) -> Result<(), DeadlineError> {
    let (ext, contents) = match file_type {
        "YAML" => ("yaml", deadline_yaml_dump(data)),
        "JSON" => (
            "json",
            serde_json::to_string_pretty(data)
                .map_err(|e| op_err(format!("Failed to serialize JSON: {e}")))?,
        ),
        _ => {
            return Err(op_err(format!(
                "Unexpected file type '{file_type}' in job bundle:\n{}",
                bundle_dir.display()
            )));
        }
    };
    let path = bundle_dir.join(format!("{filename}.{ext}"));
    fs::write(&path, &contents)
        .map_err(|e| op_err(format!("Failed to write {}: {e}", path.display())))
}

pub fn deadline_yaml_dump(data: &serde_json::Value) -> String {
    serde_yaml::to_string(data).unwrap_or_default()
}
