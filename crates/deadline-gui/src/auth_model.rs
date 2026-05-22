//! Auth status QObject model.
//!
//! Exposes authentication state to QML. Thin wrapper that delegates
//! to `logic::auth` for state derivation. Watches ~/.aws/ and ~/.deadline/
//! for credential changes and auto-refreshes.

use core::pin::Pin;
use cxx_qt::CxxQtType;
use cxx_qt::Threading;
use cxx_qt_lib::QString;

use crate::logic::auth;
use crate::logic::watcher;
use deadline_lib::api::auth as lib_auth;

/// Rust backing struct for the AuthModel QObject.
#[derive(Default)]
pub struct AuthModelRust {
    status_text: QString,
    show_login: bool,
    show_logout: bool,
    show_more_info: bool,
    api_available: bool,
    is_refreshing: bool,

    // Internal
    profile: String,
    creds_source: Option<lib_auth::AwsCredentialsSource>,
    auth_status: Option<lib_auth::AwsAuthenticationStatus>,
    api_available_inner: Option<bool>,
}

#[cxx_qt::bridge]
pub mod qobject {
    unsafe extern "C++" {
        include!("cxx-qt-lib/qstring.h");
        type QString = cxx_qt_lib::QString;
    }

    impl cxx_qt::Threading for AuthModel {}

    extern "RustQt" {
        #[qobject]
        #[qml_element]
        #[qproperty(QString, status_text)]
        #[qproperty(bool, show_login)]
        #[qproperty(bool, show_logout)]
        #[qproperty(bool, show_more_info)]
        #[qproperty(bool, api_available)]
        #[qproperty(bool, is_refreshing)]
        type AuthModel = super::AuthModelRust;

        /// Set the profile and refresh auth status.
        #[qinvokable]
        fn set_profile(self: Pin<&mut Self>, profile: QString);

        /// Refresh all auth checks asynchronously.
        #[qinvokable]
        fn refresh_status(self: Pin<&mut Self>);

        /// Start watching ~/.aws/ and ~/.deadline/ for changes.
        #[qinvokable]
        fn start_watching(self: Pin<&mut Self>);

        /// Trigger login flow.
        #[qinvokable]
        fn login(self: Pin<&mut Self>);

        /// Trigger logout.
        #[qinvokable]
        fn logout(self: Pin<&mut Self>);
    }
}

impl qobject::AuthModel {
    pub fn set_profile(mut self: Pin<&mut Self>, profile: QString) {
        let p = profile.to_string();
        let p = if p == "(default)" || p.is_empty() {
            String::new()
        } else {
            p
        };
        self.as_mut().rust_mut().profile = p;
        self.as_mut().refresh_status();
    }

    pub fn start_watching(self: Pin<&mut Self>) {
        use notify::{Event, RecursiveMode, Watcher};

        let paths = watcher::watch_paths();
        if paths.is_empty() {
            return;
        }

        let qt_thread = self.qt_thread();
        let watch_paths = paths.clone();

        std::thread::spawn(move || {
            let (tx, rx) = std::sync::mpsc::channel::<notify::Result<Event>>();
            let mut file_watcher = match notify::recommended_watcher(tx) {
                Ok(w) => w,
                Err(_) => return,
            };

            for path in &watch_paths {
                if path.exists() {
                    let _ = file_watcher.watch(path, RecursiveMode::NonRecursive);
                }
            }

            // Block this thread, forwarding events to Qt
            while let Ok(Ok(event)) = rx.recv() {
                let dominated = event
                    .paths
                    .iter()
                    .any(|p| watcher::should_trigger_refresh(p, &watch_paths));
                if dominated {
                    let _ = qt_thread.queue(move |mut obj| {
                        obj.as_mut().refresh_status();
                    });
                }
            }
        });
    }

    pub fn refresh_status(mut self: Pin<&mut Self>) {
        // Clear state — show refreshing
        self.as_mut().rust_mut().creds_source = None;
        self.as_mut().rust_mut().auth_status = None;
        self.as_mut().rust_mut().api_available_inner = None;
        self.as_mut().set_is_refreshing(true);
        self.as_mut().update_ui();

        let qt_thread = self.qt_thread();
        let profile = self.as_ref().rust().profile.clone();

        std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            let p = if profile.is_empty() {
                None
            } else {
                Some(profile.as_str())
            };

            let creds = lib_auth::get_credentials_source(p);
            let auth = rt.block_on(lib_auth::check_authentication_status(p));
            let api = auth == lib_auth::AwsAuthenticationStatus::Authenticated;

            qt_thread
                .queue(move |mut obj| {
                    obj.as_mut().rust_mut().creds_source = Some(creds);
                    obj.as_mut().rust_mut().auth_status = Some(auth);
                    obj.as_mut().rust_mut().api_available_inner = Some(api);
                    obj.as_mut().set_is_refreshing(false);
                    obj.as_mut().update_ui();
                })
                .unwrap();
        });
    }

    pub fn login(self: Pin<&mut Self>) {
        let qt_thread = self.qt_thread();
        let profile = self.as_ref().rust().profile.clone();

        std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            let p = if profile.is_empty() {
                None
            } else {
                Some(profile.as_str())
            };
            let config = deadline_lib::config::config_file::read_config().unwrap_or_default();
            let monitor_path = deadline_lib::config::config_file::get_setting(
                "deadline-cloud-monitor.path",
                &config,
            )
            .unwrap_or_default();
            let (opt_out, ident) = deadline_lib::api::telemetry::resolve_telemetry_params(&config);
            let telemetry = deadline_lib::api::telemetry::create_telemetry(opt_out, Some(&ident));
            let _ = rt.block_on(lib_auth::login(None, None, p, &monitor_path, &telemetry));

            qt_thread
                .queue(move |mut obj| {
                    obj.as_mut().refresh_status();
                })
                .unwrap();
        });
    }

    pub fn logout(self: Pin<&mut Self>) {
        let qt_thread = self.qt_thread();
        let profile = self.as_ref().rust().profile.clone();

        std::thread::spawn(move || {
            let p = if profile.is_empty() {
                None
            } else {
                Some(profile.as_str())
            };
            let config = deadline_lib::config::config_file::read_config().unwrap_or_default();
            let monitor_path = deadline_lib::config::config_file::get_setting(
                "deadline-cloud-monitor.path",
                &config,
            )
            .unwrap_or_default();
            let (opt_out, ident) = deadline_lib::api::telemetry::resolve_telemetry_params(&config);
            let telemetry = deadline_lib::api::telemetry::create_telemetry(opt_out, Some(&ident));
            let _ = lib_auth::logout(p, &monitor_path, &telemetry);

            qt_thread
                .queue(move |mut obj| {
                    obj.as_mut().refresh_status();
                })
                .unwrap();
        });
    }

    fn update_ui(mut self: Pin<&mut Self>) {
        let creds = self.as_ref().rust().creds_source;
        let auth = self.as_ref().rust().auth_status;
        let api = self.as_ref().rust().api_available_inner;
        let profile = self.as_ref().rust().profile.clone();
        let display_profile = if profile.is_empty() {
            "(default)".to_string()
        } else {
            profile
        };

        let state = auth::derive_auth_state(creds, auth, api);
        let text = auth::auth_status_text(&state, &display_profile);

        self.as_mut().set_status_text(QString::from(&text));
        self.as_mut()
            .set_show_login(auth::should_show_login(&state));
        self.as_mut()
            .set_show_logout(auth::should_show_logout(creds));
        self.as_mut()
            .set_show_more_info(auth::should_show_more_info(&state));
        self.as_mut().set_api_available(api.unwrap_or(false));
    }
}
