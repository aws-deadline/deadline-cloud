//! Config dialog QObject model.
//!
//! Exposes workstation configuration state to QML.

use core::pin::Pin;
use cxx_qt_lib::QString;

/// Rust backing struct for the ConfigModel QObject.
#[derive(Default)]
pub struct ConfigModelRust {
    aws_profile: QString,
    farm_id: QString,
    queue_id: QString,
    status_message: QString,
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
        #[qproperty(QString, farm_id)]
        #[qproperty(QString, queue_id)]
        #[qproperty(QString, status_message)]
        type ConfigModel = super::ConfigModelRust;

        /// Load settings from the config file
        #[qinvokable]
        fn load_settings(self: Pin<&mut Self>);

        /// Apply and save settings
        #[qinvokable]
        fn apply_settings(self: Pin<&mut Self>);
    }
}

impl qobject::ConfigModel {
    /// Load settings from deadline-lib config
    pub fn load_settings(mut self: Pin<&mut Self>) {
        match deadline_lib::config::config_file::read_config() {
            Ok(config) => {
                let profile = deadline_lib::config::config_file::get_setting(
                    "defaults.aws_profile_name",
                    &config,
                )
                .unwrap_or_default();
                let farm = deadline_lib::config::config_file::get_setting(
                    "defaults.farm_id",
                    &config,
                )
                .unwrap_or_default();
                let queue = deadline_lib::config::config_file::get_setting(
                    "defaults.queue_id",
                    &config,
                )
                .unwrap_or_default();

                self.as_mut().set_aws_profile(QString::from(&profile));
                self.as_mut().set_farm_id(QString::from(&farm));
                self.as_mut().set_queue_id(QString::from(&queue));
                self.as_mut()
                    .set_status_message(QString::from("Settings loaded"));
            }
            Err(e) => {
                self.as_mut()
                    .set_status_message(QString::from(&format!("Error: {e}")));
            }
        }
    }

    /// Apply settings back to config file
    pub fn apply_settings(self: Pin<&mut Self>) {
        // For the spike, just log that apply was called
        println!(
            "Apply settings: profile={}, farm={}, queue={}",
            self.aws_profile(),
            self.farm_id(),
            self.queue_id()
        );
    }
}
