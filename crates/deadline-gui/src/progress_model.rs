//! Progress dialog QObject model.
//!
//! Tracks submission progress (hashing, upload) and exposes state to QML.

use core::pin::Pin;
use cxx_qt_lib::QString;

/// Rust backing struct for the ProgressModel QObject.
#[derive(Default)]
pub struct ProgressModelRust {
    status_text: QString,
    hashing_progress: i32,
    hashing_message: QString,
    upload_progress: i32,
    upload_message: QString,
    log_text: QString,
    is_complete: bool,
    is_canceled: bool,
    job_id: QString,
}

#[cxx_qt::bridge]
pub mod qobject {
    unsafe extern "C++" {
        include!("cxx-qt-lib/qstring.h");
        type QString = cxx_qt_lib::QString;
    }

    impl cxx_qt::Threading for ProgressModel {}

    extern "RustQt" {
        #[qobject]
        #[qml_element]
        #[qproperty(QString, status_text)]
        #[qproperty(i32, hashing_progress)]
        #[qproperty(QString, hashing_message)]
        #[qproperty(i32, upload_progress)]
        #[qproperty(QString, upload_message)]
        #[qproperty(QString, log_text)]
        #[qproperty(bool, is_complete)]
        #[qproperty(bool, is_canceled)]
        #[qproperty(QString, job_id)]
        type ProgressModel = super::ProgressModelRust;

        /// Request cancellation.
        #[qinvokable]
        fn cancel(self: Pin<&mut Self>);

        /// Append a message to the log.
        #[qinvokable]
        fn append_log(self: Pin<&mut Self>, message: QString);
    }
}

impl qobject::ProgressModel {
    pub fn cancel(mut self: Pin<&mut Self>) {
        self.as_mut().set_is_canceled(true);
        self.as_mut()
            .set_status_text(cxx_qt_lib::QString::from("Canceling submission..."));
    }

    pub fn append_log(mut self: Pin<&mut Self>, message: QString) {
        let current = self.as_ref().log_text().to_string();
        let new_text = if current.is_empty() {
            message.to_string()
        } else {
            format!("{current}\n{}", message.to_string())
        };
        self.as_mut()
            .set_log_text(cxx_qt_lib::QString::from(&new_text));
    }
}
