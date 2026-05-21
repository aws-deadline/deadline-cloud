//! AWS Deadline Cloud GUI — Rust + QML via cxx-qt.
//!
//! This crate provides the GUI dialogs (config, submit, login, progress)
//! as Rust QObject models backed by QML views.

pub mod config_model;
pub mod logic;

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
