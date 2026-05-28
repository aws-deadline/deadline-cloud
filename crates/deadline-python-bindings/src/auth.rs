use pyo3::prelude::*;

use crate::DeadlineOperationError;

#[pyfunction]
#[pyo3(signature = (config_path=None))]
// PyO3 requires PyResult return type for #[pyfunction] even when the
// function is infallible — Python always expects an exception-capable call.
#[allow(
    clippy::unnecessary_wraps,
    reason = "PyO3 requires PyResult return type"
)]
pub fn get_credentials_source(config_path: Option<&str>) -> PyResult<String> {
    let config = crate::load_config(config_path)
        .unwrap_or_else(|_| deadline_lib::config::ini::IniConfig::new());
    let profile = crate::extract_profile(&config);
    let source = deadline_lib::api::auth::get_credentials_source(profile.as_deref());
    Ok(source.to_string())
}

#[pyfunction]
#[pyo3(signature = (config_path=None))]
pub fn check_auth_status(py: Python<'_>, config_path: Option<&str>) -> PyResult<PyObject> {
    let config = crate::load_config(config_path)
        .unwrap_or_else(|_| deadline_lib::config::ini::IniConfig::new());
    let profile = crate::extract_profile(&config);
    let rt = crate::make_runtime()?;
    let result = crate::on_large_stack(|| {
        rt.block_on(async {
            let source = deadline_lib::api::auth::get_credentials_source(profile.as_deref());
            let status =
                deadline_lib::api::auth::check_authentication_status(profile.as_deref()).await;
            let api_available =
                status == deadline_lib::api::auth::AwsAuthenticationStatus::Authenticated;
            (source.to_string(), status.to_string(), api_available)
        })
    })?;
    let dict = pyo3::types::PyDict::new(py);
    dict.set_item("credentials_source", result.0)?;
    dict.set_item("auth_status", result.1)?;
    dict.set_item("api_available", result.2)?;
    Ok(dict.into_any().unbind())
}

#[pyfunction]
#[pyo3(signature = (config_path=None, on_progress=None))]
pub fn check_auth_status_with_progress(
    py: Python<'_>,
    config_path: Option<&str>,
    on_progress: Option<PyObject>,
) -> PyResult<PyObject> {
    let config = crate::load_config(config_path)
        .unwrap_or_else(|_| deadline_lib::config::ini::IniConfig::new());
    let profile = crate::extract_profile(&config);
    let notify = |msg: &str| {
        if let Some(ref cb) = on_progress {
            Python::with_gil(|py| {
                let _ = cb.call1(py, (msg,));
            });
        }
    };
    let rt = crate::make_runtime()?;
    let result = crate::on_large_stack(|| {
        rt.block_on(async {
            notify("Checking credentials source...");
            let source = deadline_lib::api::auth::get_credentials_source(profile.as_deref());
            notify("Checking authentication status...");
            let status =
                deadline_lib::api::auth::check_authentication_status(profile.as_deref()).await;
            let api_available =
                status == deadline_lib::api::auth::AwsAuthenticationStatus::Authenticated;
            notify("Done");
            (source.to_string(), status.to_string(), api_available)
        })
    })?;
    let dict = pyo3::types::PyDict::new(py);
    dict.set_item("credentials_source", result.0)?;
    dict.set_item("auth_status", result.1)?;
    dict.set_item("api_available", result.2)?;
    Ok(dict.into_any().unbind())
}

#[pyfunction]
#[pyo3(signature = (config_path=None))]
pub fn check_api_available(config_path: Option<&str>) -> PyResult<bool> {
    let config = crate::load_config(config_path)
        .unwrap_or_else(|_| deadline_lib::config::ini::IniConfig::new());
    let profile = crate::extract_profile(&config);
    let rt = crate::make_runtime()?;
    let status = crate::on_large_stack(|| {
        rt.block_on(deadline_lib::api::auth::check_authentication_status(
            profile.as_deref(),
        ))
    })?;
    Ok(status == deadline_lib::api::auth::AwsAuthenticationStatus::Authenticated)
}

#[pyfunction]
#[pyo3(signature = (config_path=None, on_pending_authorization=None, on_cancellation_check=None))]
pub fn login(
    config_path: Option<&str>,
    on_pending_authorization: Option<PyObject>,
    on_cancellation_check: Option<PyObject>,
) -> PyResult<String> {
    let config = crate::load_config(config_path)
        .unwrap_or_else(|_| deadline_lib::config::ini::IniConfig::new());
    let profile = crate::extract_profile(&config);
    let monitor_path =
        deadline_lib::config::config_file::get_setting("deadline-cloud-monitor.path", &config)
            .unwrap_or_default();
    let (opt_out, ident) = deadline_lib::api::telemetry::resolve_telemetry_params(&config);
    let telemetry = deadline_lib::api::telemetry::create_telemetry(opt_out, Some(&ident));

    let pending_cb = on_pending_authorization.as_ref().map(|cb| {
        move |source: deadline_lib::api::auth::AwsCredentialsSource| {
            Python::with_gil(|py| {
                let kwargs = pyo3::types::PyDict::new(py);
                let source_enum = py
                    .import("deadline.client._compat")
                    .and_then(|m| m.getattr("AwsCredentialsSource"))
                    .and_then(|cls| cls.call1((source.to_string(),)));
                if let Ok(val) = source_enum {
                    let _ = kwargs.set_item("credentials_source", val);
                }
                let _ = cb.call(py, (), Some(&kwargs));
            });
        }
    });

    let cancel_cb = on_cancellation_check.as_ref().map(|cb| {
        move || -> bool {
            Python::with_gil(|py| {
                cb.call0(py)
                    .map(|r| r.is_truthy(py).unwrap_or(false))
                    .unwrap_or(false)
            })
        }
    });

    let rt = crate::make_runtime()?;
    crate::on_large_stack(|| {
        rt.block_on(deadline_lib::api::auth::login(
            pending_cb
                .as_ref()
                .map(|f| f as &dyn Fn(deadline_lib::api::auth::AwsCredentialsSource)),
            cancel_cb.as_ref().map(|f| f as &dyn Fn() -> bool),
            profile.as_deref(),
            &monitor_path,
            &telemetry,
        ))
    })?
    .map_err(DeadlineOperationError::new_err)
}

#[pyfunction]
#[pyo3(signature = (config_path=None))]
pub fn logout(config_path: Option<&str>) -> PyResult<String> {
    let config = crate::load_config(config_path)
        .unwrap_or_else(|_| deadline_lib::config::ini::IniConfig::new());
    let profile = crate::extract_profile(&config);
    let monitor_path =
        deadline_lib::config::config_file::get_setting("deadline-cloud-monitor.path", &config)
            .unwrap_or_default();
    let (opt_out, ident) = deadline_lib::api::telemetry::resolve_telemetry_params(&config);
    let telemetry = deadline_lib::api::telemetry::create_telemetry(opt_out, Some(&ident));
    deadline_lib::api::auth::logout(profile.as_deref(), &monitor_path, &telemetry)
        .map_err(DeadlineOperationError::new_err)
}
