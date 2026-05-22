//! Resource list QObject model.
//!
//! Exposes farm/queue/storage profile lists to QML. Thin wrapper that
//! delegates to `logic::resources` for async fetching.

use core::pin::Pin;
use cxx_qt::CxxQtType;
use cxx_qt::Threading;
use cxx_qt_lib::QString;

use crate::logic::resources;

/// Rust backing struct for the ResourceModel QObject.
#[derive(Default)]
pub struct ResourceModelRust {
    // Farm list (semicolon-separated names and ids for QML)
    farm_names: QString,
    farm_ids: QString,
    farms_loading: bool,
    selected_farm_index: i32,

    // Queue list
    queue_names: QString,
    queue_ids: QString,
    queues_loading: bool,
    selected_queue_index: i32,

    // Storage profile list
    storage_profile_names: QString,
    storage_profile_ids: QString,
    storage_profiles_loading: bool,
    selected_storage_profile_index: i32,

    // Internal
    profile: String,
    configured_farm_id: String,
    configured_queue_id: String,
    configured_storage_profile_id: String,
}

#[cxx_qt::bridge]
pub mod qobject {
    unsafe extern "C++" {
        include!("cxx-qt-lib/qstring.h");
        type QString = cxx_qt_lib::QString;
    }

    impl cxx_qt::Threading for ResourceModel {}

    extern "RustQt" {
        #[qobject]
        #[qml_element]
        #[qproperty(QString, farm_names)]
        #[qproperty(QString, farm_ids)]
        #[qproperty(bool, farms_loading)]
        #[qproperty(i32, selected_farm_index)]
        #[qproperty(QString, queue_names)]
        #[qproperty(QString, queue_ids)]
        #[qproperty(bool, queues_loading)]
        #[qproperty(i32, selected_queue_index)]
        #[qproperty(QString, storage_profile_names)]
        #[qproperty(QString, storage_profile_ids)]
        #[qproperty(bool, storage_profiles_loading)]
        #[qproperty(i32, selected_storage_profile_index)]
        type ResourceModel = super::ResourceModelRust;

        /// Set the AWS profile and refresh all lists.
        #[qinvokable]
        fn set_profile(self: Pin<&mut Self>, profile: QString);

        /// Set configured IDs from config for pre-selection after load.
        #[qinvokable]
        fn set_configured_ids(
            self: Pin<&mut Self>,
            farm_id: QString,
            queue_id: QString,
            storage_profile_id: QString,
        );

        /// Refresh farms list from API.
        #[qinvokable]
        fn refresh_farms(self: Pin<&mut Self>);

        /// Called from QML when user selects a farm — refreshes queues.
        #[qinvokable]
        fn select_farm(self: Pin<&mut Self>, index: i32);

        /// Called from QML when user selects a queue — refreshes storage profiles.
        #[qinvokable]
        fn select_queue(self: Pin<&mut Self>, index: i32);

        /// Refresh queues list from API.
        #[qinvokable]
        fn refresh_queues(self: Pin<&mut Self>);

        /// Refresh storage profiles list from API.
        #[qinvokable]
        fn refresh_storage_profiles(self: Pin<&mut Self>);
    }
}

impl qobject::ResourceModel {
    pub fn set_profile(mut self: Pin<&mut Self>, profile: QString) {
        self.as_mut().rust_mut().profile = profile.to_string();
        // Clear all lists
        self.as_mut().set_farm_names(QString::from(""));
        self.as_mut().set_farm_ids(QString::from(""));
        self.as_mut().set_queue_names(QString::from(""));
        self.as_mut().set_queue_ids(QString::from(""));
        self.as_mut().set_storage_profile_names(QString::from(""));
        self.as_mut().set_storage_profile_ids(QString::from(""));
        self.as_mut().refresh_farms();
    }

    pub fn set_configured_ids(
        mut self: Pin<&mut Self>,
        farm_id: QString,
        queue_id: QString,
        storage_profile_id: QString,
    ) {
        self.as_mut().rust_mut().configured_farm_id = farm_id.to_string();
        self.as_mut().rust_mut().configured_queue_id = queue_id.to_string();
        self.as_mut().rust_mut().configured_storage_profile_id = storage_profile_id.to_string();
    }

    pub fn refresh_farms(mut self: Pin<&mut Self>) {
        self.as_mut().set_farms_loading(true);
        let qt_thread = self.qt_thread();
        let profile = self.as_ref().rust().profile.clone();

        std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            let p = if profile.is_empty() || profile == "(default)" {
                None
            } else {
                Some(profile.as_str())
            };
            let entries = rt.block_on(resources::fetch_farms(p));

            qt_thread
                .queue(move |mut obj| {
                    let configured_id = obj.as_ref().rust().configured_farm_id.clone();
                    let mut entries = entries;
                    let idx = resources::resolve_selected_index(&mut entries, &configured_id);

                    let names: Vec<&str> =
                        entries.iter().map(|e| e.display_name.as_str()).collect();
                    let ids: Vec<&str> = entries.iter().map(|e| e.id.as_str()).collect();
                    obj.as_mut().set_farm_names(QString::from(&names.join(";")));
                    obj.as_mut().set_farm_ids(QString::from(&ids.join(";")));
                    obj.as_mut().set_selected_farm_index(idx as i32);
                    obj.as_mut().set_farms_loading(false);

                    // Cascade: fetch queues for selected farm
                    let farm_id = resources::selected_id_at(&entries, idx);
                    if !farm_id.is_empty() {
                        obj.as_mut().refresh_queues();
                    }
                })
                .unwrap();
        });
    }

    pub fn select_farm(mut self: Pin<&mut Self>, index: i32) {
        self.as_mut().set_selected_farm_index(index);
        // Update configured_farm_id to the newly selected one
        let farm_ids_str = self.as_ref().farm_ids().to_string();
        let farm_id = farm_ids_str.split(';').nth(index as usize).unwrap_or("");
        self.as_mut().rust_mut().configured_farm_id = farm_id.to_string();
        // Clear configured queue since farm changed
        self.as_mut().rust_mut().configured_queue_id = String::new();
        self.as_mut().rust_mut().configured_storage_profile_id = String::new();
        // Clear dependent lists
        self.as_mut().set_queue_names(QString::from(""));
        self.as_mut().set_queue_ids(QString::from(""));
        self.as_mut().set_storage_profile_names(QString::from(""));
        self.as_mut().set_storage_profile_ids(QString::from(""));
        self.as_mut().refresh_queues();
    }

    pub fn refresh_queues(mut self: Pin<&mut Self>) {
        let farm_ids_str = self.as_ref().farm_ids().to_string();
        let idx = *self.as_ref().selected_farm_index() as usize;
        let farm_id = farm_ids_str.split(';').nth(idx).unwrap_or("").to_string();

        if farm_id.is_empty() {
            return;
        }

        self.as_mut().set_queues_loading(true);
        let qt_thread = self.qt_thread();
        let profile = self.as_ref().rust().profile.clone();

        std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            let p = if profile.is_empty() || profile == "(default)" {
                None
            } else {
                Some(profile.as_str())
            };
            let entries = rt.block_on(resources::fetch_queues(p, &farm_id));

            qt_thread
                .queue(move |mut obj| {
                    let configured_id = obj.as_ref().rust().configured_queue_id.clone();
                    let mut entries = entries;
                    let idx = resources::resolve_selected_index(&mut entries, &configured_id);

                    let names: Vec<&str> =
                        entries.iter().map(|e| e.display_name.as_str()).collect();
                    let ids: Vec<&str> = entries.iter().map(|e| e.id.as_str()).collect();
                    obj.as_mut()
                        .set_queue_names(QString::from(&names.join(";")));
                    obj.as_mut().set_queue_ids(QString::from(&ids.join(";")));
                    obj.as_mut().set_selected_queue_index(idx as i32);
                    obj.as_mut().set_queues_loading(false);

                    // Cascade: fetch storage profiles for selected queue
                    let queue_id = resources::selected_id_at(&entries, idx);
                    if !queue_id.is_empty() {
                        obj.as_mut().refresh_storage_profiles();
                    }
                })
                .unwrap();
        });
    }

    pub fn select_queue(mut self: Pin<&mut Self>, index: i32) {
        self.as_mut().set_selected_queue_index(index);
        // Update configured_queue_id
        let queue_ids_str = self.as_ref().queue_ids().to_string();
        let queue_id = queue_ids_str.split(';').nth(index as usize).unwrap_or("");
        self.as_mut().rust_mut().configured_queue_id = queue_id.to_string();
        self.as_mut().rust_mut().configured_storage_profile_id = String::new();
        // Clear storage profiles
        self.as_mut().set_storage_profile_names(QString::from(""));
        self.as_mut().set_storage_profile_ids(QString::from(""));
        self.as_mut().refresh_storage_profiles();
    }

    pub fn refresh_storage_profiles(mut self: Pin<&mut Self>) {
        let farm_ids_str = self.as_ref().farm_ids().to_string();
        let farm_idx = *self.as_ref().selected_farm_index() as usize;
        let farm_id = farm_ids_str
            .split(';')
            .nth(farm_idx)
            .unwrap_or("")
            .to_string();

        let queue_ids_str = self.as_ref().queue_ids().to_string();
        let queue_idx = *self.as_ref().selected_queue_index() as usize;
        let queue_id = queue_ids_str
            .split(';')
            .nth(queue_idx)
            .unwrap_or("")
            .to_string();

        if farm_id.is_empty() || queue_id.is_empty() {
            return;
        }

        self.as_mut().set_storage_profiles_loading(true);
        let qt_thread = self.qt_thread();
        let profile = self.as_ref().rust().profile.clone();

        std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            let p = if profile.is_empty() || profile == "(default)" {
                None
            } else {
                Some(profile.as_str())
            };
            let entries = rt.block_on(resources::fetch_storage_profiles(p, &farm_id, &queue_id));

            qt_thread
                .queue(move |mut obj| {
                    let configured_id = obj.as_ref().rust().configured_storage_profile_id.clone();
                    let mut entries = entries;
                    let idx = resources::resolve_selected_index(&mut entries, &configured_id);

                    let names: Vec<&str> =
                        entries.iter().map(|e| e.display_name.as_str()).collect();
                    let ids: Vec<&str> = entries.iter().map(|e| e.id.as_str()).collect();
                    obj.as_mut()
                        .set_storage_profile_names(QString::from(&names.join(";")));
                    obj.as_mut()
                        .set_storage_profile_ids(QString::from(&ids.join(";")));
                    obj.as_mut().set_selected_storage_profile_index(idx as i32);
                    obj.as_mut().set_storage_profiles_loading(false);
                })
                .unwrap();
        });
    }
}
