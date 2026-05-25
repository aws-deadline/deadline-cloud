//! Attachment QObject model.
//!
//! Exposes auto-detected and user-added file/directory lists to QML
//! as semicolon-separated string properties.

use core::pin::Pin;
use cxx_qt::CxxQtType;
use cxx_qt_lib::QString;

use crate::logic::attachments::{AssetReferences, AttachmentState};

#[derive(Default)]
pub struct AttachmentModelRust {
    input_files: QString,
    input_dirs: QString,
    output_dirs: QString,
    auto_input_files: QString,
    auto_input_dirs: QString,
    auto_output_dirs: QString,
    require_paths_exist: bool,
    state: AttachmentState,
}

#[cxx_qt::bridge]
pub mod qobject {
    unsafe extern "C++" {
        include!("cxx-qt-lib/qstring.h");
        type QString = cxx_qt_lib::QString;
    }

    extern "RustQt" {
        #[qobject]
        #[qml_element]
        #[qproperty(QString, input_files)]
        #[qproperty(QString, input_dirs)]
        #[qproperty(QString, output_dirs)]
        #[qproperty(QString, auto_input_files)]
        #[qproperty(QString, auto_input_dirs)]
        #[qproperty(QString, auto_output_dirs)]
        #[qproperty(bool, require_paths_exist)]
        type AttachmentModel = super::AttachmentModelRust;

        /// Load auto-detected attachments from a bundle directory's asset_references file.
        #[qinvokable]
        fn initialize(self: Pin<&mut Self>, bundle_dir: QString);

        #[qinvokable]
        fn add_input_file(self: Pin<&mut Self>, path: QString);

        #[qinvokable]
        fn remove_input_file(self: Pin<&mut Self>, index: i32);

        #[qinvokable]
        fn add_input_dir(self: Pin<&mut Self>, path: QString);

        #[qinvokable]
        fn remove_input_dir(self: Pin<&mut Self>, index: i32);

        #[qinvokable]
        fn add_output_dir(self: Pin<&mut Self>, path: QString);

        #[qinvokable]
        fn remove_output_dir(self: Pin<&mut Self>, index: i32);
    }
}

impl qobject::AttachmentModel {
    pub fn initialize(mut self: Pin<&mut Self>, bundle_dir: QString) {
        let dir = bundle_dir.to_string();
        let auto = load_asset_references_from_bundle(&dir);
        self.as_mut().rust_mut().state = AttachmentState {
            auto_detected: auto,
            ..Default::default()
        };
        self.sync_properties();
    }

    pub fn add_input_file(mut self: Pin<&mut Self>, path: QString) {
        self.as_mut()
            .rust_mut()
            .state
            .add_input_file(&path.to_string());
        self.sync_properties();
    }

    pub fn remove_input_file(mut self: Pin<&mut Self>, index: i32) {
        if index >= 0 {
            self.as_mut()
                .rust_mut()
                .state
                .remove_input_file(index as usize);
            self.sync_properties();
        }
    }

    pub fn add_input_dir(mut self: Pin<&mut Self>, path: QString) {
        self.as_mut()
            .rust_mut()
            .state
            .add_input_dir(&path.to_string());
        self.sync_properties();
    }

    pub fn remove_input_dir(mut self: Pin<&mut Self>, index: i32) {
        if index >= 0 {
            self.as_mut()
                .rust_mut()
                .state
                .remove_input_dir(index as usize);
            self.sync_properties();
        }
    }

    pub fn add_output_dir(mut self: Pin<&mut Self>, path: QString) {
        self.as_mut()
            .rust_mut()
            .state
            .add_output_dir(&path.to_string());
        self.sync_properties();
    }

    pub fn remove_output_dir(mut self: Pin<&mut Self>, index: i32) {
        if index >= 0 {
            self.as_mut()
                .rust_mut()
                .state
                .remove_output_dir(index as usize);
            self.sync_properties();
        }
    }

    fn sync_properties(mut self: Pin<&mut Self>) {
        let state = self.as_ref().rust().state.clone();
        let auto_files = state.auto_detected.input_file_paths.join(";");
        let auto_dirs = state.auto_detected.input_directory_paths.join(";");
        let auto_out = state.auto_detected.output_directory_paths.join(";");
        let user_files = state.user_added.input_file_paths.join(";");
        let user_dirs = state.user_added.input_directory_paths.join(";");
        let user_out = state.user_added.output_directory_paths.join(";");

        self.as_mut()
            .set_auto_input_files(QString::from(&auto_files));
        self.as_mut().set_auto_input_dirs(QString::from(&auto_dirs));
        self.as_mut().set_auto_output_dirs(QString::from(&auto_out));
        self.as_mut().set_input_files(QString::from(&user_files));
        self.as_mut().set_input_dirs(QString::from(&user_dirs));
        self.as_mut().set_output_dirs(QString::from(&user_out));
    }
}

fn load_asset_references_from_bundle(bundle_dir: &str) -> AssetReferences {
    if bundle_dir.is_empty() {
        return AssetReferences::default();
    }
    let dir = std::path::Path::new(bundle_dir);
    for filename in &["asset_references.json", "asset_references.yaml"] {
        let path = dir.join(filename);
        if let Ok(content) = std::fs::read_to_string(&path) {
            let json: serde_json::Value = if filename.ends_with(".yaml") {
                serde_yaml::from_str(&content).unwrap_or_default()
            } else {
                serde_json::from_str(&content).unwrap_or_default()
            };
            if let Ok(refs) = AssetReferences::from_bundle_json(&json) {
                return refs;
            }
        }
    }
    AssetReferences::default()
}
