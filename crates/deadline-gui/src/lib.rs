//! AWS Deadline Cloud GUI — Rust + QML via cxx-qt.
//!
//! This crate provides the GUI dialogs (config, submit, login, progress)
//! as Rust QObject models backed by QML views.

pub mod attachment_model;
pub mod auth_model;
pub mod config_model;
pub mod logic;
pub mod parameter_model;
pub mod progress_model;
pub mod resource_model;
pub mod submit_model;

use cxx_qt_lib::{QGuiApplication, QQmlApplicationEngine, QUrl};

/// Show the workstation configuration dialog.
/// Blocks until the dialog is closed.
pub fn show_config_dialog() {
    let mut app = QGuiApplication::new();
    let mut engine = QQmlApplicationEngine::new();

    if let Some(engine) = engine.as_mut() {
        engine.load(&QUrl::from(
            "qrc:/qt/qml/com/amazon/deadline/gui/qml/ConfigDialog.qml",
        ));
    }

    if let Some(app) = app.as_mut() {
        app.exec();
    }
}

/// Parameters for the submit dialog.
#[derive(Debug, Clone, Default)]
pub struct SubmitDialogParams {
    pub job_bundle_dir: String,
    pub browse: bool,
    pub output: String,
    pub known_asset_paths: Vec<String>,
    pub submitter_info: Option<serde_json::Value>,
    pub job_parameters: Vec<serde_json::Value>,
    pub name: Option<String>,
}

/// Global storage for submit params (set before QML loads, read by SubmitModel).
static SUBMIT_PARAMS: std::sync::Mutex<Option<String>> = std::sync::Mutex::new(None);

/// Global storage for submit result (set by SubmitModel on completion, read by show_submit_dialog).
static SUBMIT_RESULT: std::sync::Mutex<Option<String>> = std::sync::Mutex::new(None);

/// Get the params JSON stored for the current submit dialog invocation.
pub fn get_submit_params_json() -> String {
    SUBMIT_PARAMS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clone()
        .unwrap_or_else(|| "{}".to_string())
}

/// Store the submission result (called from SubmitModel background thread).
pub fn set_submit_result(result: &str) {
    *SUBMIT_RESULT.lock().unwrap_or_else(|e| e.into_inner()) = Some(result.to_string());
}

/// Show the job submission dialog.
/// Blocks until the dialog is closed. Returns the result as a JSON string.
pub fn show_submit_dialog(params: &SubmitDialogParams) -> String {
    let params_json = serde_json::json!({
        "job_bundle_dir": params.job_bundle_dir,
        "browse": params.browse,
        "output": params.output,
        "known_asset_paths": params.known_asset_paths,
        "submitter_info": params.submitter_info,
        "job_parameters": params.job_parameters,
        "name": params.name,
    });

    *SUBMIT_PARAMS.lock().unwrap_or_else(|e| e.into_inner()) = Some(params_json.to_string());
    *SUBMIT_RESULT.lock().unwrap_or_else(|e| e.into_inner()) = None;

    let mut app = QGuiApplication::new();
    let mut engine = QQmlApplicationEngine::new();

    if let Some(engine) = engine.as_mut() {
        engine.load(&QUrl::from(
            "qrc:/qt/qml/com/amazon/deadline/gui/qml/SubmitDialog.qml",
        ));
    }

    if let Some(app) = app.as_mut() {
        app.exec();
    }

    // Return the result set by the submission thread, or CANCELED if dialog was closed
    SUBMIT_RESULT
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clone()
        .unwrap_or_else(|| serde_json::json!({"status": "CANCELED"}).to_string())
}
