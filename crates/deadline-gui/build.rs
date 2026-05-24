use cxx_qt_build::{CxxQtBuilder, QmlModule};

fn main() {
    CxxQtBuilder::new_qml_module(
        QmlModule::new("com.amazon.deadline.gui")
            .qml_file("qml/ConfigDialog.qml")
            .qml_file("qml/SubmitDialog.qml"),
    )
    .qt_module("Network")
    .files([
        "src/config_model.rs",
        "src/resource_model.rs",
        "src/auth_model.rs",
        "src/submit_model.rs",
        "src/progress_model.rs",
    ])
    .build();
}
