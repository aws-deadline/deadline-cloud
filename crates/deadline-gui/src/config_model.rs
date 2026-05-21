//! Config dialog QObject model.
//!
//! Exposes workstation configuration state to QML. Thin wrapper that
//! delegates to `logic.rs` for all business logic.

use core::pin::Pin;
use cxx_qt::CxxQtType;
use cxx_qt_lib::QString;
use std::collections::HashMap;

use crate::logic;

/// Rust backing struct for the ConfigModel QObject.
#[derive(Default)]
pub struct ConfigModelRust {
    // Global settings
    aws_profile: QString,
    aws_profile_names: QString, // semicolon-separated list for QML

    // Profile settings
    job_history_dir: QString,
    farm_id: QString,

    // Farm settings
    queue_id: QString,
    storage_profile_id: QString,
    job_attachments_filesystem: QString,

    // General settings
    auto_accept: bool,
    telemetry_opt_out: bool,
    force_s3_check: bool,
    submitter_update_notification: bool,
    conflict_resolution: QString,
    log_level: QString,
    locale: QString,
    known_asset_paths: QString, // pathsep-separated

    // UI state
    has_changes: bool,
    status_message: QString,

    // Internal (not exposed as properties)
    config_path: String,
    baseline: Option<logic::ConfigState>,
}

#[cxx_qt::bridge]
pub mod qobject {
    unsafe extern "C++" {
        include!("cxx-qt-lib/qstring.h");
        type QString = cxx_qt_lib::QString;
    }

    impl cxx_qt::Threading for ConfigModel {}

    extern "RustQt" {
        #[qobject]
        #[qml_element]
        #[qproperty(QString, aws_profile)]
        #[qproperty(QString, aws_profile_names)]
        #[qproperty(QString, job_history_dir)]
        #[qproperty(QString, farm_id)]
        #[qproperty(QString, queue_id)]
        #[qproperty(QString, storage_profile_id)]
        #[qproperty(QString, job_attachments_filesystem)]
        #[qproperty(bool, auto_accept)]
        #[qproperty(bool, telemetry_opt_out)]
        #[qproperty(bool, force_s3_check)]
        #[qproperty(bool, submitter_update_notification)]
        #[qproperty(QString, conflict_resolution)]
        #[qproperty(QString, log_level)]
        #[qproperty(QString, locale)]
        #[qproperty(QString, known_asset_paths)]
        #[qproperty(bool, has_changes)]
        #[qproperty(QString, status_message)]
        type ConfigModel = super::ConfigModelRust;

        /// Load settings from the config file
        #[qinvokable]
        fn load_settings(self: Pin<&mut Self>);

        /// Apply changed settings to disk
        #[qinvokable]
        fn apply_settings(self: Pin<&mut Self>);

        /// Revert to baseline (cancel)
        #[qinvokable]
        fn revert_settings(self: Pin<&mut Self>);

        /// Called from QML when any setting changes — recomputes dirty state
        #[qinvokable]
        fn notify_changed(self: Pin<&mut Self>);
    }
}

impl qobject::ConfigModel {
    pub fn load_settings(mut self: Pin<&mut Self>) {
        // Determine config path
        let config_path = std::env::var("DEADLINE_CONFIG_FILE_PATH")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|_| deadline_lib::config::config_file::get_config_file_path());

        self.as_mut().rust_mut().config_path = config_path.to_string_lossy().to_string();

        let state = logic::load_config_state(&config_path);

        // Load AWS profile names
        let home = std::env::var("HOME").unwrap_or_default();
        let aws_dir = std::path::Path::new(&home).join(".aws");
        let profiles = logic::parse_aws_profiles(&aws_dir);
        let profiles_str = profiles.join(";");

        // Store baseline
        self.as_mut().rust_mut().baseline = Some(state.clone());

        // Set all properties
        self.as_mut()
            .set_aws_profile(QString::from(&state.aws_profile));
        self.as_mut()
            .set_aws_profile_names(QString::from(&profiles_str));
        self.as_mut()
            .set_job_history_dir(QString::from(&state.job_history_dir));
        self.as_mut().set_farm_id(QString::from(&state.farm_id));
        self.as_mut().set_queue_id(QString::from(&state.queue_id));
        self.as_mut()
            .set_storage_profile_id(QString::from(&state.storage_profile_id));
        self.as_mut()
            .set_job_attachments_filesystem(QString::from(&state.job_attachments_filesystem));
        self.as_mut().set_auto_accept(state.auto_accept);
        self.as_mut().set_telemetry_opt_out(state.telemetry_opt_out);
        self.as_mut().set_force_s3_check(state.force_s3_check);
        self.as_mut()
            .set_submitter_update_notification(state.submitter_update_notification);
        self.as_mut()
            .set_conflict_resolution(QString::from(&state.conflict_resolution));
        self.as_mut().set_log_level(QString::from(&state.log_level));
        self.as_mut().set_locale(QString::from(&state.locale));

        let sep = if cfg!(windows) { ";" } else { ":" };
        let paths_str = state.known_asset_paths.join(sep);
        self.as_mut()
            .set_known_asset_paths(QString::from(&paths_str));

        self.as_mut().set_has_changes(false);
        self.as_mut()
            .set_status_message(QString::from("Settings loaded"));
    }

    pub fn apply_settings(mut self: Pin<&mut Self>) {
        let config_path = std::path::PathBuf::from(&self.as_ref().rust().config_path);

        // Build changes map from current properties vs baseline
        let changes = self.as_ref().build_changes();

        if changes.is_empty() {
            return;
        }

        match logic::apply_config_changes(&config_path, &changes) {
            Ok(new_state) => {
                self.as_mut().rust_mut().baseline = Some(new_state);
                self.as_mut().set_has_changes(false);
                self.as_mut()
                    .set_status_message(QString::from("Settings applied"));
            }
            Err(e) => {
                self.as_mut()
                    .set_status_message(QString::from(&format!("Error: {e}")));
            }
        }
    }

    pub fn revert_settings(mut self: Pin<&mut Self>) {
        // Reload from disk
        self.as_mut().load_settings();
    }

    pub fn notify_changed(mut self: Pin<&mut Self>) {
        let current = self.as_ref().current_state();
        let has_changes = if let Some(baseline) = &self.as_ref().rust().baseline {
            !logic::compute_dirty_fields(baseline, &current).is_empty()
        } else {
            false
        };
        self.as_mut().set_has_changes(has_changes);
    }

    fn build_changes(&self) -> HashMap<String, String> {
        let mut changes = HashMap::new();
        if let Some(baseline) = &self.rust().baseline {
            let current = self.current_state();
            let dirty = logic::compute_dirty_fields(baseline, &current);
            for key in dirty {
                let value = match key.as_str() {
                    "defaults.aws_profile_name" => current.aws_profile.clone(),
                    "settings.job_history_dir" => current.job_history_dir.clone(),
                    "defaults.farm_id" => current.farm_id.clone(),
                    "defaults.queue_id" => current.queue_id.clone(),
                    "settings.storage_profile_id" => current.storage_profile_id.clone(),
                    "defaults.job_attachments_file_system" => {
                        current.job_attachments_filesystem.clone()
                    }
                    "settings.auto_accept" => current.auto_accept.to_string(),
                    "telemetry.opt_out" => current.telemetry_opt_out.to_string(),
                    "settings.force_s3_check" => current.force_s3_check.to_string(),
                    "settings.submitter_update_notification" => {
                        current.submitter_update_notification.to_string()
                    }
                    "settings.conflict_resolution" => current.conflict_resolution.clone(),
                    "settings.log_level" => current.log_level.clone(),
                    "settings.locale" => current.locale.clone(),
                    "settings.known_asset_paths" => current
                        .known_asset_paths
                        .join(if cfg!(windows) { ";" } else { ":" }),
                    _ => continue,
                };
                changes.insert(key, value);
            }
        }
        changes
    }

    fn current_state(&self) -> logic::ConfigState {
        let paths_str = self.known_asset_paths().to_string();
        let sep = if cfg!(windows) { ';' } else { ':' };
        let known_asset_paths = if paths_str.is_empty() {
            Vec::new()
        } else {
            paths_str.split(sep).map(|s| s.to_string()).collect()
        };

        logic::ConfigState {
            aws_profile: self.aws_profile().to_string(),
            job_history_dir: self.job_history_dir().to_string(),
            farm_id: self.farm_id().to_string(),
            queue_id: self.queue_id().to_string(),
            storage_profile_id: self.storage_profile_id().to_string(),
            job_attachments_filesystem: self.job_attachments_filesystem().to_string(),
            auto_accept: *self.auto_accept(),
            telemetry_opt_out: *self.telemetry_opt_out(),
            force_s3_check: *self.force_s3_check(),
            submitter_update_notification: *self.submitter_update_notification(),
            conflict_resolution: self.conflict_resolution().to_string(),
            log_level: self.log_level().to_string(),
            locale: self.locale().to_string(),
            known_asset_paths,
        }
    }
}
