//! Submit dialog QObject model.
//!
//! Exposes job submission state to QML. Thin wrapper that delegates
//! to `logic/submit.rs` for bundle preparation and to `deadline-lib`
//! for the actual submission.

use core::pin::Pin;
use cxx_qt::CxxQtType;
use cxx_qt::Threading;
use cxx_qt_lib::QString;

use crate::logic;

/// Rust backing struct for the SubmitModel QObject.
#[derive(Default)]
pub struct SubmitModelRust {
    // Job properties
    name: QString,
    description: QString,
    priority: i32,
    initial_status: QString,
    max_failed_tasks_count: i32,
    max_retries_per_task: i32,
    max_worker_count: i32,
    use_max_worker_count: bool,

    // Farm/queue display
    farm_display: QString,
    queue_display: QString,

    // Bundle
    job_bundle_dir: QString,
    submitter_name: QString,

    // UI state
    can_submit: bool,
    status_message: QString,
    aws_profile: QString,
    is_submitting: bool,

    // Progress state (shown in ProgressDialog)
    hashing_progress: i32,
    hashing_message: QString,
    upload_progress: i32,
    upload_message: QString,
    log_text: QString,
    submission_complete: bool,
    submission_error: QString,
    job_id_result: QString,

    // Internal
    farm_id: String,
    queue_id: String,
    profile: String,
    api_available: bool,
    storage_profile_id: String,
    canceled: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

#[cxx_qt::bridge]
pub mod qobject {
    unsafe extern "C++" {
        include!("cxx-qt-lib/qstring.h");
        type QString = cxx_qt_lib::QString;
    }

    impl cxx_qt::Threading for SubmitModel {}

    extern "RustQt" {
        #[qobject]
        #[qml_element]
        #[qproperty(QString, name)]
        #[qproperty(QString, description)]
        #[qproperty(i32, priority)]
        #[qproperty(QString, initial_status)]
        #[qproperty(i32, max_failed_tasks_count)]
        #[qproperty(i32, max_retries_per_task)]
        #[qproperty(i32, max_worker_count)]
        #[qproperty(bool, use_max_worker_count)]
        #[qproperty(QString, farm_display)]
        #[qproperty(QString, queue_display)]
        #[qproperty(QString, job_bundle_dir)]
        #[qproperty(QString, submitter_name)]
        #[qproperty(bool, can_submit)]
        #[qproperty(QString, status_message)]
        #[qproperty(QString, aws_profile)]
        #[qproperty(bool, is_submitting)]
        #[qproperty(i32, hashing_progress)]
        #[qproperty(QString, hashing_message)]
        #[qproperty(i32, upload_progress)]
        #[qproperty(QString, upload_message)]
        #[qproperty(QString, log_text)]
        #[qproperty(bool, submission_complete)]
        #[qproperty(QString, submission_error)]
        #[qproperty(QString, job_id_result)]
        type SubmitModel = super::SubmitModelRust;

        /// Initialize the model with parameters from the CLI/caller.
        #[qinvokable]
        fn initialize(self: Pin<&mut Self>, params_json: QString);

        /// Refresh submit button state.
        #[qinvokable]
        fn refresh_submit_state(self: Pin<&mut Self>);

        /// Called when auth/resource state changes.
        #[qinvokable]
        fn set_api_available(self: Pin<&mut Self>, available: bool);

        /// Set farm/queue IDs (from config or resource model).
        #[qinvokable]
        fn set_farm_queue(self: Pin<&mut Self>, farm_id: QString, queue_id: QString);

        /// Start job submission in a background thread.
        #[qinvokable]
        fn submit(self: Pin<&mut Self>);

        /// Cancel an in-progress submission.
        #[qinvokable]
        fn cancel_submission(self: Pin<&mut Self>);

        /// Append a message to the log.
        #[qinvokable]
        fn append_log(self: Pin<&mut Self>, message: QString);
    }
}

impl qobject::SubmitModel {
    pub fn initialize(mut self: Pin<&mut Self>, params_json: QString) {
        let json_str = if params_json.to_string().is_empty() {
            crate::get_submit_params_json()
        } else {
            params_json.to_string()
        };
        let params: serde_json::Value =
            serde_json::from_str(&json_str).unwrap_or(serde_json::json!({}));

        let bundle_dir = params
            .get("job_bundle_dir")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        self.as_mut().set_job_bundle_dir(QString::from(bundle_dir));

        // Load template name
        let template_name = if !bundle_dir.is_empty() {
            let bundle_path = std::path::Path::new(bundle_dir);
            load_template_name(bundle_path).unwrap_or_else(|| "Job bundle submission".to_string())
        } else {
            "Job bundle submission".to_string()
        };
        self.as_mut().set_name(QString::from(&template_name));

        // Set defaults
        self.as_mut().set_priority(50);
        self.as_mut().set_initial_status(QString::from("READY"));
        self.as_mut().set_max_failed_tasks_count(20);
        self.as_mut().set_max_retries_per_task(5);
        self.as_mut().set_max_worker_count(-1);
        self.as_mut().set_use_max_worker_count(false);

        let submitter = params
            .get("submitter_info")
            .and_then(|si| si.get("submitter_name"))
            .and_then(|v| v.as_str())
            .unwrap_or("JobBundle");
        self.as_mut().set_submitter_name(QString::from(submitter));

        // Load config for farm/queue
        let config = deadline_lib::config::config_file::read_config().unwrap_or_default();
        let profile = deadline_lib::api::session::resolve_profile_name(&config);
        let farm_id = deadline_lib::config::config_file::get_setting("defaults.farm_id", &config)
            .unwrap_or_default();
        let queue_id = deadline_lib::config::config_file::get_setting("defaults.queue_id", &config)
            .unwrap_or_default();
        let storage_profile_id =
            deadline_lib::config::config_file::get_setting("settings.storage_profile_id", &config)
                .unwrap_or_default();

        let profile_str = profile.unwrap_or_default();
        self.as_mut().rust_mut().profile = profile_str.clone();
        self.as_mut().set_aws_profile(QString::from(&profile_str));
        self.as_mut().rust_mut().farm_id = farm_id.clone();
        self.as_mut().rust_mut().queue_id = queue_id.clone();
        self.as_mut().rust_mut().storage_profile_id = storage_profile_id;
        self.as_mut().set_farm_display(QString::from(&farm_id));
        self.as_mut().set_queue_display(QString::from(&queue_id));

        self.as_mut().refresh_submit_state();
    }

    pub fn refresh_submit_state(mut self: Pin<&mut Self>) {
        let farm_id = self.as_ref().rust().farm_id.clone();
        let queue_id = self.as_ref().rust().queue_id.clone();
        let api_available = self.as_ref().rust().api_available;

        let issues = logic::submit::validate_submit_readiness(&farm_id, &queue_id, api_available);
        let can = issues.is_empty();
        self.as_mut().set_can_submit(can);

        if can {
            self.as_mut().set_status_message(QString::from(""));
        } else {
            self.as_mut()
                .set_status_message(QString::from(&issues.join("\n")));
        }
    }

    pub fn set_api_available(mut self: Pin<&mut Self>, available: bool) {
        self.as_mut().rust_mut().api_available = available;
        self.as_mut().refresh_submit_state();
    }

    pub fn set_farm_queue(mut self: Pin<&mut Self>, farm_id: QString, queue_id: QString) {
        let fid = farm_id.to_string();
        let qid = queue_id.to_string();
        self.as_mut().rust_mut().farm_id = fid.clone();
        self.as_mut().rust_mut().queue_id = qid.clone();
        self.as_mut().set_farm_display(QString::from(&fid));
        self.as_mut().set_queue_display(QString::from(&qid));
        self.as_mut().refresh_submit_state();
    }

    pub fn submit(mut self: Pin<&mut Self>) {
        use std::path::PathBuf;
        use std::sync::Arc;
        use std::sync::atomic::Ordering;

        // Guard against double-submit
        if *self.as_ref().is_submitting() {
            return;
        }

        // Reset progress state
        self.as_mut().set_is_submitting(true);
        self.as_mut().set_hashing_progress(0);
        self.as_mut().set_hashing_message(QString::from(""));
        self.as_mut().set_upload_progress(0);
        self.as_mut().set_upload_message(QString::from(""));
        self.as_mut().set_log_text(QString::from(""));
        self.as_mut().set_submission_complete(false);
        self.as_mut().set_submission_error(QString::from(""));
        self.as_mut().set_job_id_result(QString::from(""));

        let canceled = Arc::new(std::sync::atomic::AtomicBool::new(false));
        self.as_mut().rust_mut().canceled = canceled.clone();

        // Gather params from model state
        let bundle_dir = self.as_ref().job_bundle_dir().to_string();
        let farm_id = self.as_ref().rust().farm_id.clone();
        let queue_id = self.as_ref().rust().queue_id.clone();
        let profile = self.as_ref().rust().profile.clone();
        let storage_profile_id = self.as_ref().rust().storage_profile_id.clone();
        let name = self.as_ref().name().to_string();
        let priority = *self.as_ref().priority();
        let max_failed = *self.as_ref().max_failed_tasks_count();
        let max_retries = *self.as_ref().max_retries_per_task();
        let max_workers = if *self.as_ref().use_max_worker_count() {
            Some(*self.as_ref().max_worker_count())
        } else {
            None
        };
        let initial_status = self.as_ref().initial_status().to_string();
        let submitter_name = self.as_ref().submitter_name().to_string();

        // Read additional config values
        let cfg = logic::submit::read_submit_config_fields();

        // Get known_asset_paths and job_parameters from the params JSON
        let params_json_str = crate::get_submit_params_json();
        let params_json: serde_json::Value =
            serde_json::from_str(&params_json_str).unwrap_or(serde_json::json!({}));
        let known_asset_paths: Vec<PathBuf> = params_json
            .get("known_asset_paths")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(PathBuf::from))
                    .collect()
            })
            .unwrap_or_default();
        let job_parameters: Vec<serde_json::Value> = params_json
            .get("job_parameters")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        let target_task_run_status = logic::submit::resolve_target_task_run_status(&initial_status);

        let qt_thread = self.qt_thread();
        let canceled_for_handler = canceled.clone();
        let qt_thread_for_hash = self.qt_thread();
        let qt_thread_for_upload = self.qt_thread();

        std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();

            struct GuiHandler {
                qt_thread: cxx_qt::CxxQtThread<qobject::SubmitModel>,
                canceled: Arc<std::sync::atomic::AtomicBool>,
            }
            impl deadline_lib::bundle::SubmissionHandler for GuiHandler {
                fn on_message(&self, msg: &str) {
                    let msg = msg.to_string();
                    let _ = self.qt_thread.queue(move |mut obj| {
                        let current = obj.as_ref().log_text().to_string();
                        let new_text = if current.is_empty() {
                            msg
                        } else {
                            format!("{current}\n{msg}")
                        };
                        obj.as_mut().set_log_text(QString::from(&new_text));
                    });
                }
                fn confirm(&self, _msg: &str, _default: bool) -> bool {
                    // GUI auto-accepts (same as auto_accept=true)
                    true
                }
                fn should_continue(&self) -> bool {
                    !self.canceled.load(Ordering::Relaxed)
                }
            }

            let handler = GuiHandler {
                qt_thread: qt_thread.clone(),
                canceled: canceled_for_handler,
            };

            let hashing_cb: deadline_lib::attachments::progress_tracker::ProgressFn =
                Box::new(move |processed, total| {
                    let pct = if total > 0 {
                        (processed * 100 / total) as i32
                    } else {
                        100
                    };
                    let _ = qt_thread_for_hash.queue(move |mut obj| {
                        obj.as_mut().set_hashing_progress(pct);
                        obj.as_mut().set_hashing_message(QString::from(&format!(
                            "Hashing: {processed}/{total} bytes"
                        )));
                    });
                    !canceled.load(Ordering::Relaxed)
                });

            let canceled_for_upload = handler.canceled.clone();
            let upload_cb: deadline_lib::attachments::progress_tracker::ProgressFn =
                Box::new(move |processed, total| {
                    let pct = if total > 0 {
                        (processed * 100 / total) as i32
                    } else {
                        100
                    };
                    let _ = qt_thread_for_upload.queue(move |mut obj| {
                        obj.as_mut().set_upload_progress(pct);
                        obj.as_mut().set_upload_message(QString::from(&format!(
                            "Uploading: {processed}/{total} bytes"
                        )));
                    });
                    !canceled_for_upload.load(Ordering::Relaxed)
                });

            let submit_params = deadline_lib::bundle::SubmitJobParams {
                job_bundle_dir: PathBuf::from(&bundle_dir),
                job_parameters,
                name: Some(name),
                priority: Some(priority),
                max_failed_tasks_count: Some(max_failed),
                max_retries_per_task: Some(max_retries),
                max_worker_count: max_workers,
                target_task_run_status,
                require_paths_exist: true,
                submitter_name: Some(submitter_name),
                known_asset_paths,
                auto_accept: true, // GUI doesn't prompt
                debug_snapshot_dir: None,
                handler: &handler,
                hashing_progress_callback: Some(hashing_cb),
                upload_progress_callback: Some(upload_cb),
                telemetry: None,
                farm_id,
                queue_id,
                profile: if profile.is_empty() {
                    None
                } else {
                    Some(profile)
                },
                storage_profile_id: if storage_profile_id.is_empty() {
                    None
                } else {
                    Some(storage_profile_id)
                },
                job_attachments_file_system: cfg.job_attachments_file_system,
                force_s3_check: cfg.force_s3_check,
                allow_bundle_hooks: cfg.allow_bundle_hooks,
                allow_environment_hooks: cfg.allow_environment_hooks,
                known_config_paths: cfg.known_config_paths,
                s3_max_pool_connections: cfg.s3_max_pool_connections,
            };

            let result = rt.block_on(deadline_lib::bundle::create_job_from_job_bundle(
                submit_params,
            ));

            qt_thread
                .queue(move |mut obj| {
                    obj.as_mut().set_is_submitting(false);
                    obj.as_mut().set_submission_complete(true);
                    match result {
                        Ok(Some(job_id)) => {
                            obj.as_mut().set_job_id_result(QString::from(&job_id));
                            // Store result globally for show_submit_dialog return value
                            crate::set_submit_result(
                                &serde_json::json!({
                                    "status": "SUCCESS",
                                    "job_id": job_id,
                                })
                                .to_string(),
                            );
                        }
                        Ok(None) => {
                            obj.as_mut().set_submission_error(QString::from(
                                "Submission completed but no job ID returned.",
                            ));
                            crate::set_submit_result(
                                &serde_json::json!({
                                    "status": "ERROR",
                                    "message": "No job ID returned",
                                })
                                .to_string(),
                            );
                        }
                        Err(e) => {
                            let msg = e.to_string();
                            obj.as_mut().set_submission_error(QString::from(&msg));
                            crate::set_submit_result(
                                &serde_json::json!({
                                    "status": "ERROR",
                                    "message": msg,
                                })
                                .to_string(),
                            );
                        }
                    }
                })
                .unwrap();
        });
    }

    pub fn cancel_submission(mut self: Pin<&mut Self>) {
        self.as_mut()
            .rust_mut()
            .canceled
            .store(true, std::sync::atomic::Ordering::Relaxed);
        self.as_mut()
            .set_status_message(QString::from("Canceling submission..."));
    }

    pub fn append_log(mut self: Pin<&mut Self>, message: QString) {
        let current = self.as_ref().log_text().to_string();
        let msg = message.to_string();
        let new_text = if current.is_empty() {
            msg
        } else {
            format!("{current}\n{msg}")
        };
        self.as_mut().set_log_text(QString::from(&new_text));
    }
}

pub(crate) fn load_template_name(bundle_dir: &std::path::Path) -> Option<String> {
    for filename in &["template.json", "template.yaml"] {
        let path = bundle_dir.join(filename);
        if let Ok(content) = std::fs::read_to_string(&path) {
            let value: serde_json::Value = if filename.ends_with(".yaml") {
                serde_yaml::from_str(&content).ok()?
            } else {
                serde_json::from_str(&content).ok()?
            };
            return value.get("name").and_then(|n| n.as_str()).map(String::from);
        }
    }
    None
}
