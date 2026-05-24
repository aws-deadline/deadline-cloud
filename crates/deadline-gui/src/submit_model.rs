//! Submit dialog QObject model.
//!
//! Exposes job submission state to QML. Thin wrapper that delegates
//! to `logic/submit.rs` for bundle preparation and to `deadline-lib`
//! for the actual submission.

use core::pin::Pin;
use cxx_qt::CxxQtType;
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

    // Internal
    farm_id: String,
    queue_id: String,
    profile: String,
    api_available: bool,
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
        self.as_mut()
            .set_job_bundle_dir(QString::from(bundle_dir));

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
        self.as_mut()
            .set_initial_status(QString::from("READY"));
        self.as_mut().set_max_failed_tasks_count(20);
        self.as_mut().set_max_retries_per_task(5);
        self.as_mut().set_max_worker_count(-1);
        self.as_mut().set_use_max_worker_count(false);

        let submitter = params
            .get("submitter_info")
            .and_then(|si| si.get("submitter_name"))
            .and_then(|v| v.as_str())
            .unwrap_or("JobBundle");
        self.as_mut()
            .set_submitter_name(QString::from(submitter));

        // Load config for farm/queue
        let config = deadline_lib::config::config_file::read_config().unwrap_or_default();
        let profile = deadline_lib::api::session::resolve_profile_name(&config);
        let farm_id = deadline_lib::config::config_file::get_setting("defaults.farm_id", &config)
            .unwrap_or_default();
        let queue_id =
            deadline_lib::config::config_file::get_setting("defaults.queue_id", &config)
                .unwrap_or_default();

        let profile_str = profile.unwrap_or_default();
        self.as_mut().rust_mut().profile = profile_str.clone();
        self.as_mut()
            .set_aws_profile(QString::from(&profile_str));
        self.as_mut().rust_mut().farm_id = farm_id.clone();
        self.as_mut().rust_mut().queue_id = queue_id.clone();
        self.as_mut()
            .set_farm_display(QString::from(&farm_id));
        self.as_mut()
            .set_queue_display(QString::from(&queue_id));

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
}

fn load_template_name(bundle_dir: &std::path::Path) -> Option<String> {
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
