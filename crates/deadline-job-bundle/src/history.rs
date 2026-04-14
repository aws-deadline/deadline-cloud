use deadline_api::errors::DeadlineError;
use std::fs;
use std::path::Path;

fn op_err(msg: String) -> DeadlineError {
    DeadlineError::OperationError(msg)
}

pub fn create_job_history_bundle_dir(
    submitter_name: &str,
    job_name: &str,
    job_history_dir: &str,
) -> Result<String, DeadlineError> {
    let clean_submitter: String = submitter_name
        .chars()
        .filter(|c| c.is_alphanumeric() || *c == ' ' || *c == '-' || *c == '_')
        .collect();
    let clean_job: String = job_name
        .chars()
        .filter(|c| c.is_alphanumeric() || *c == ' ' || *c == '-' || *c == '_')
        .take(128)
        .collect();

    let now = chrono::Local::now();
    let month_tag = now.format("%Y-%m").to_string();
    let date_tag = now.format("%Y-%m-%d").to_string();

    let month_dir = Path::new(job_history_dir).join(&month_tag);
    if !month_dir.is_dir() {
        fs::create_dir_all(&month_dir).map_err(|e| {
            op_err(format!(
                "Failed to create directory {}: {e}",
                month_dir.display()
            ))
        })?;
    }

    let mut number = 1u32;
    let prefix = format!("{date_tag}-");
    if let Ok(entries) = fs::read_dir(&month_dir) {
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.starts_with(&prefix) {
                let after = &name[prefix.len()..];
                if let Some(num_str) = after.split('-').next()
                    && let Ok(n) = num_str.parse::<u32>() {
                        number = number.max(n + 1);
                    }
            }
        }
    }

    let dir_name = format!("{date_tag}-{number:02}-{clean_submitter}-{clean_job}");
    let result = month_dir.join(&dir_name);
    fs::create_dir_all(&result).map_err(|e| {
        op_err(format!(
            "Failed to create directory {}: {e}",
            result.display()
        ))
    })?;

    Ok(result.to_string_lossy().into_owned())
}
