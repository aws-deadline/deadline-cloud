use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::path::PathBuf;

use crate::DeadlineOperationError;

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
    struct PySubmissionHandler {
        on_print: Option<PyObject>,
        on_confirm: Option<PyObject>,
        on_continue: Option<PyObject>,
    }

    // SAFETY: PyObjects are Send when accessed only via Python::with_gil
    unsafe impl Send for PySubmissionHandler {}
    unsafe impl Sync for PySubmissionHandler {}

    impl deadline_lib::bundle::SubmissionHandler for PySubmissionHandler {
        fn on_message(&self, msg: &str) {
            if let Some(ref cb) = self.on_print {
                Python::with_gil(|py| {
                    let _ = cb.call1(py, (msg,));
                });
            }
        }
        fn confirm(&self, msg: &str, default: bool) -> bool {
            match &self.on_confirm {
                Some(cb) => Python::with_gil(|py| {
                    cb.call1(py, (msg, default))
                        .map(|r| r.is_truthy(py).unwrap_or(default))
                        .unwrap_or(default)
                }),
                None => default,
            }
        }
        fn should_continue(&self) -> bool {
            match &self.on_continue {
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
            if let Some(ref cb) = self.on_print {
                let msg = stats.format_upload_summary();
                Python::with_gil(|py| {
                    let _ = cb.call1(py, (msg,));
                });
            }
        }
    }

    let handler = PySubmissionHandler {
        on_print,
        on_confirm,
        on_continue,
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

    let submit_params = deadline_lib::bundle::SubmitJobParams {
        job_bundle_dir: PathBuf::from(job_bundle_dir),
        job_parameters,
        name,
        priority,
        max_failed_tasks_count,
        max_retries_per_task,
        max_worker_count,
        target_task_run_status,
        job_attachments_file_system,
        require_paths_exist,
        submitter_name,
        known_asset_paths: known_asset_paths.into_iter().map(PathBuf::from).collect(),
        auto_accept,
        force_s3_check,
        debug_snapshot_dir: debug_snapshot_dir.map(PathBuf::from),
        config: &config,
        handler: &handler,
        hashing_progress_callback: hashing_cb,
        upload_progress_callback: upload_cb,
        telemetry: None,
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
