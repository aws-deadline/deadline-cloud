use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::path::PathBuf;

use crate::DeadlineOperationError;

/// Config-derived values needed for submission.
struct SubmissionConfig {
    farm_id: String,
    queue_id: String,
    profile: Option<String>,
    storage_profile_id: Option<String>,
    job_attachments_file_system: String,
    force_s3_check: bool,
    allow_bundle_hooks: bool,
    allow_environment_hooks: bool,
    known_config_paths: Vec<String>,
    s3_max_pool_connections: Option<usize>,
}

fn extract_submission_config(
    config: &deadline_lib::config::ini::IniConfig,
    ja_file_system_override: Option<String>,
    force_s3_check_override: Option<bool>,
) -> SubmissionConfig {
    let get =
        |name| deadline_lib::config::config_file::get_setting(name, config).unwrap_or_default();
    let bool_setting =
        |name| deadline_lib::config::config_file::str2bool(&get(name)).unwrap_or(false);

    SubmissionConfig {
        farm_id: get("defaults.farm_id"),
        queue_id: get("defaults.queue_id"),
        profile: deadline_lib::api::session::resolve_profile_name(config),
        storage_profile_id: {
            let v = get("settings.storage_profile_id");
            if v.is_empty() { None } else { Some(v) }
        },
        job_attachments_file_system: ja_file_system_override
            .unwrap_or_else(|| get("defaults.job_attachments_file_system")),
        force_s3_check: force_s3_check_override
            .unwrap_or_else(|| bool_setting("settings.force_s3_check")),
        allow_bundle_hooks: bool_setting("settings.allow_bundle_hooks"),
        allow_environment_hooks: bool_setting("settings.allow_environment_hooks"),
        known_config_paths: {
            let v = get("settings.known_asset_paths");
            if v.is_empty() {
                Vec::new()
            } else {
                let sep = if cfg!(windows) { ';' } else { ':' };
                v.split(sep).map(String::from).collect()
            }
        },
        s3_max_pool_connections: deadline_lib::config::config_file::get_setting(
            "settings.s3_max_pool_connections",
            config,
        )
        .ok()
        .and_then(|v| deadline_lib::attachments::s3::parse_s3_max_pool_connections(&v).ok()),
    }
}

#[allow(clippy::struct_field_names, reason = "cb suffix clarifies these are callbacks")]
struct PySubmissionHandler {
    print_cb: Option<PyObject>,
    confirm_cb: Option<PyObject>,
    continue_cb: Option<PyObject>,
}

// SAFETY: PyObject is only accessed via Python::with_gil which acquires the GIL,
// ensuring exclusive access to the Python interpreter from any thread.
#[allow(unsafe_code, reason = "PyObject requires manual Send/Sync for cross-thread use with GIL")]
unsafe impl Send for PySubmissionHandler {}
// SAFETY: Same as above — all PyObject access is gated by with_gil.
#[allow(unsafe_code, reason = "PyObject requires manual Send/Sync for cross-thread use with GIL")]
unsafe impl Sync for PySubmissionHandler {}

impl deadline_lib::bundle::SubmissionHandler for PySubmissionHandler {
    fn on_message(&self, msg: &str) {
        if let Some(ref cb) = self.print_cb {
            Python::with_gil(|py| {
                let _ = cb.call1(py, (msg,));
            });
        }
    }
    fn confirm(&self, msg: &str, default: bool) -> bool {
        match &self.confirm_cb {
            Some(cb) => Python::with_gil(|py| {
                cb.call1(py, (msg, default))
                    .map(|r| r.is_truthy(py).unwrap_or(default))
                    .unwrap_or(default)
            }),
            None => default,
        }
    }
    fn should_continue(&self) -> bool {
        match &self.continue_cb {
            Some(cb) => Python::with_gil(|py| {
                cb.call0(py)
                    .map(|r| r.is_truthy(py).unwrap_or(true))
                    .unwrap_or(true)
            }),
            None => true,
        }
    }
    fn on_upload_summary(
        &self,
        stats: &deadline_lib::attachments::progress_tracker::SummaryStatistics,
    ) {
        if let Some(ref cb) = self.print_cb {
            let msg = stats.format_upload_summary();
            Python::with_gil(|py| {
                let _ = cb.call1(py, (msg,));
            });
        }
    }
}

#[pyfunction]
#[pyo3(signature = (params, on_print=None, on_hashing_progress=None, on_upload_progress=None, on_confirm=None, on_continue=None))]
pub fn create_job_from_job_bundle(
    py: Python<'_>,
    params: &Bound<'_, PyDict>,
    on_print: Option<PyObject>,
    on_hashing_progress: Option<PyObject>,
    on_upload_progress: Option<PyObject>,
    on_confirm: Option<PyObject>,
    on_continue: Option<PyObject>,
) -> PyResult<PyObject> {
    // Extract job_bundle_dir (required)
    let job_bundle_dir: String = params
        .get_item("job_bundle_dir")?
        .ok_or_else(|| DeadlineOperationError::new_err("missing required field: job_bundle_dir"))?
        .extract()?;

    // Extract optional fields
    let name: Option<String> = extract_opt(params, "name")?;
    let priority: Option<i32> = extract_opt(params, "priority")?;
    let max_failed_tasks_count: Option<i32> = extract_opt(params, "max_failed_tasks_count")?;
    let max_retries_per_task: Option<i32> = extract_opt(params, "max_retries_per_task")?;
    let max_worker_count: Option<i32> = extract_opt(params, "max_worker_count")?;
    let target_task_run_status: Option<String> = extract_opt(params, "target_task_run_status")?;
    let job_attachments_file_system: Option<String> =
        extract_opt(params, "job_attachments_file_system")?;
    let require_paths_exist: bool = extract_opt(params, "require_paths_exist")?.unwrap_or(false);
    let submitter_name: Option<String> = extract_opt(params, "submitter_name")?;
    let auto_accept: bool = extract_opt(params, "auto_accept")?.unwrap_or(false);
    let force_s3_check: Option<bool> = extract_opt(params, "force_s3_check")?;
    let debug_snapshot_dir: Option<String> = extract_opt(params, "debug_snapshot_dir")?;
    let config_path: Option<String> = extract_opt(params, "config_path")?;

    // Extract job_parameters as serde_json::Value array
    let job_parameters: Vec<serde_json::Value> = match params.get_item("job_parameters")? {
        Some(item) => {
            let json_str: String = py
                .import("json")?
                .call_method1("dumps", (item,))?
                .extract()?;
            serde_json::from_str(&json_str).map_err(|e| {
                DeadlineOperationError::new_err(format!("Invalid job_parameters: {e}"))
            })?
        }
        None => Vec::new(),
    };

    let known_asset_paths: Vec<String> = match params.get_item("known_asset_paths")? {
        Some(item) => item.extract()?,
        None => Vec::new(),
    };

    // Load config
    let config = match config_path.as_deref() {
        Some(p) => deadline_lib::config::config_file::read_config_from(std::path::Path::new(p))
            .unwrap_or_else(|_| deadline_lib::config::ini::IniConfig::new()),
        None => deadline_lib::config::config_file::read_config()
            .unwrap_or_else(|_| deadline_lib::config::ini::IniConfig::new()),
    };

    // Build handler
    let handler = PySubmissionHandler {
        print_cb: on_print,
        confirm_cb: on_confirm,
        continue_cb: on_continue,
    };

    let hashing_cb = on_hashing_progress.map(
        |cb| -> deadline_lib::attachments::progress_tracker::ProgressFn {
            Box::new(move |processed, total| {
                Python::with_gil(|py| {
                    let dict = PyDict::new(py);
                    let pct = if total > 0 {
                        (processed as f64 / total as f64) * 100.0
                    } else {
                        100.0
                    };
                    let _ = dict.set_item("progress", pct);
                    let _ = dict.set_item("transferRate", 0.0);
                    let _ = dict.set_item(
                        "progressMessage",
                        format!(
                            "Processed {} / {}",
                            deadline_lib::attachments::progress_tracker::human_readable_file_size(
                                processed
                            ),
                            deadline_lib::attachments::progress_tracker::human_readable_file_size(
                                total
                            ),
                        ),
                    );
                    let _ = dict.set_item("processedFiles", 0u64);
                    let _ = dict.set_item("processedBytes", processed);
                    let _ = dict.set_item("totalBytes", total);
                    cb.call1(py, (dict,))
                        .map(|r| r.is_truthy(py).unwrap_or(true))
                        .unwrap_or(true)
                })
            })
        },
    );

    let upload_cb = on_upload_progress.map(
        |cb| -> deadline_lib::attachments::progress_tracker::ProgressFn {
            Box::new(move |processed, total| {
                Python::with_gil(|py| {
                    let dict = PyDict::new(py);
                    let pct = if total > 0 {
                        (processed as f64 / total as f64) * 100.0
                    } else {
                        100.0
                    };
                    let _ = dict.set_item("progress", pct);
                    let _ = dict.set_item("transferRate", 0.0);
                    let _ = dict.set_item(
                        "progressMessage",
                        format!(
                            "Uploaded {} / {}",
                            deadline_lib::attachments::progress_tracker::human_readable_file_size(
                                processed
                            ),
                            deadline_lib::attachments::progress_tracker::human_readable_file_size(
                                total
                            ),
                        ),
                    );
                    let _ = dict.set_item("processedFiles", 0u64);
                    let _ = dict.set_item("processedBytes", processed);
                    let _ = dict.set_item("totalBytes", total);
                    cb.call1(py, (dict,))
                        .map(|r| r.is_truthy(py).unwrap_or(true))
                        .unwrap_or(true)
                })
            })
        },
    );

    // Extract config-derived values
    let cfg = extract_submission_config(&config, job_attachments_file_system, force_s3_check);

    let submit_params = deadline_lib::bundle::SubmitJobParams {
        job_bundle_dir: PathBuf::from(job_bundle_dir),
        job_parameters,
        name,
        priority,
        max_failed_tasks_count,
        max_retries_per_task,
        max_worker_count,
        target_task_run_status,
        require_paths_exist,
        submitter_name,
        known_asset_paths: known_asset_paths.into_iter().map(PathBuf::from).collect(),
        auto_accept,
        debug_snapshot_dir: debug_snapshot_dir.map(PathBuf::from),
        handler: &handler,
        hashing_progress_callback: hashing_cb,
        upload_progress_callback: upload_cb,
        telemetry: None,
        farm_id: cfg.farm_id,
        queue_id: cfg.queue_id,
        profile: cfg.profile,
        storage_profile_id: cfg.storage_profile_id,
        job_attachments_file_system: cfg.job_attachments_file_system,
        force_s3_check: cfg.force_s3_check,
        allow_bundle_hooks: cfg.allow_bundle_hooks,
        allow_environment_hooks: cfg.allow_environment_hooks,
        known_config_paths: cfg.known_config_paths,
        s3_max_pool_connections: cfg.s3_max_pool_connections,
    };

    let rt = crate::make_runtime()?;
    let job_id = rt
        .block_on(deadline_lib::bundle::create_job_from_job_bundle(
            submit_params,
        ))
        .map_err(|e| DeadlineOperationError::new_err(e.to_string()))?;

    let dict = PyDict::new(py);
    dict.set_item("job_id", job_id)?;
    Ok(dict.into_any().unbind())
}

/// Extract an optional typed value from a `PyDict`.
fn extract_opt<'py, T: FromPyObject<'py>>(
    dict: &Bound<'py, PyDict>,
    key: &str,
) -> PyResult<Option<T>> {
    match dict.get_item(key)? {
        Some(item) if !item.is_none() => Ok(Some(item.extract()?)),
        _ => Ok(None),
    }
}
