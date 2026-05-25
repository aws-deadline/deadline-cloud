//! Queue parameter QObject model.
//!
//! Fetches queue environment parameters from the API in a background
//! thread, parses them, and exposes the result as a JSON string property
//! for QML to render dynamically.

use core::pin::Pin;
use cxx_qt::Threading;
use cxx_qt_lib::QString;

use crate::logic::parameters;

#[derive(Default)]
pub struct ParameterListModelRust {
    parameters_json: QString,
    loading_state: QString,
    is_loading: bool,
}

#[cxx_qt::bridge]
pub mod qobject {
    unsafe extern "C++" {
        include!("cxx-qt-lib/qstring.h");
        type QString = cxx_qt_lib::QString;
    }

    impl cxx_qt::Threading for ParameterListModel {}

    extern "RustQt" {
        #[qobject]
        #[qml_element]
        #[qproperty(QString, parameters_json)]
        #[qproperty(QString, loading_state)]
        #[qproperty(bool, is_loading)]
        type ParameterListModel = super::ParameterListModelRust;

        /// Fetch queue parameters for the given farm/queue in a background thread.
        #[qinvokable]
        fn refresh(self: Pin<&mut Self>, farm_id: QString, queue_id: QString, profile: QString);

        /// Update a parameter value by name.
        #[qinvokable]
        fn set_parameter_value(self: Pin<&mut Self>, name: QString, value: QString);
    }
}

impl qobject::ParameterListModel {
    pub fn refresh(
        mut self: Pin<&mut Self>,
        farm_id: QString,
        queue_id: QString,
        profile: QString,
    ) {
        let fid = farm_id.to_string();
        let qid = queue_id.to_string();
        let prof = profile.to_string();

        if fid.is_empty() || qid.is_empty() {
            self.as_mut().set_parameters_json(QString::from("[]"));
            self.as_mut().set_loading_state(QString::from(""));
            self.as_mut().set_is_loading(false);
            return;
        }

        self.as_mut().set_is_loading(true);
        self.as_mut()
            .set_loading_state(QString::from("Loading Queue Environments..."));

        let qt_thread = self.qt_thread();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            let result = rt.block_on(fetch_queue_parameters(&fid, &qid, &prof));

            let _ = qt_thread.queue(move |mut obj| {
                obj.as_mut().set_is_loading(false);
                match result {
                    Ok(json) => {
                        obj.as_mut().set_loading_state(QString::from(""));
                        obj.as_mut().set_parameters_json(QString::from(&json));
                    }
                    Err(e) => {
                        obj.as_mut().set_loading_state(QString::from(&e));
                        obj.as_mut().set_parameters_json(QString::from("[]"));
                    }
                }
            });
        });
    }

    pub fn set_parameter_value(mut self: Pin<&mut Self>, name: QString, value: QString) {
        let name_str = name.to_string();
        let value_str = value.to_string();
        let current = self.as_ref().parameters_json().to_string();

        let mut params: Vec<serde_json::Value> = serde_json::from_str(&current).unwrap_or_default();
        for param in params.iter_mut() {
            if param.get("name").and_then(|n| n.as_str()) == Some(&name_str) {
                param["value"] = serde_json::json!(value_str);
                break;
            }
        }

        let json = serde_json::to_string(&params).unwrap_or_else(|_| "[]".to_string());
        self.as_mut().set_parameters_json(QString::from(&json));
    }
}

async fn fetch_queue_parameters(
    farm_id: &str,
    queue_id: &str,
    profile: &str,
) -> Result<String, String> {
    let client = deadline_lib::api::session::deadline_client(if profile.is_empty() {
        None
    } else {
        Some(profile)
    })
    .await;

    // List queue environments
    let mut environments = Vec::new();
    let mut next_token: Option<String> = None;
    loop {
        let mut req = client
            .list_queue_environments()
            .farm_id(farm_id)
            .queue_id(queue_id);
        if let Some(token) = &next_token {
            req = req.next_token(token);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| deadline_lib::api::client::format_sdk_error(&e))?;
        for env in resp.environments() {
            let env_id = env.queue_environment_id();
            // Fetch full environment to get template
            let full = client
                .get_queue_environment()
                .farm_id(farm_id)
                .queue_id(queue_id)
                .queue_environment_id(env_id)
                .send()
                .await
                .map_err(|e| deadline_lib::api::client::format_sdk_error(&e))?;
            environments.push(serde_json::json!({
                "queueEnvironmentId": env_id,
                "name": full.name(),
                "priority": full.priority(),
                "template": full.template(),
            }));
        }
        next_token = resp.next_token().map(String::from);
        if next_token.is_none() {
            break;
        }
    }

    // Sort by priority
    parameters::sort_environments_by_priority(&mut environments);

    // Parse parameters from templates, assign group labels per environment
    let mut all_params = Vec::new();
    for env in &environments {
        let env_name = env
            .get("name")
            .and_then(|n| n.as_str())
            .unwrap_or("Unknown");
        let mut env_params =
            parameters::parse_queue_environment_parameters(std::slice::from_ref(env));
        parameters::assign_group_labels(&mut env_params, env_name);
        all_params.extend(env_params);
    }

    // Deduplicate with conflict detection
    let deduped = parameters::detect_parameter_conflicts(&all_params)?;

    // Add resolved control type to each parameter for QML rendering
    let enriched: Vec<serde_json::Value> = deduped
        .into_iter()
        .map(|mut p| {
            let control = parameters::get_ui_control(&p);
            p.as_object_mut()
                .unwrap()
                .insert("_resolvedControl".to_string(), serde_json::json!(control));
            p
        })
        .collect();

    serde_json::to_string(&enriched).map_err(|e| e.to_string())
}
