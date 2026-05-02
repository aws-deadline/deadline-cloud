use pyo3::prelude::*;

use crate::DeadlineOperationError;

#[pyfunction]
#[pyo3(signature = (config_path=None))]
pub fn get_credentials_source(config_path: Option<&str>) -> PyResult<String> {
    let config = crate::load_config(config_path).ok();
    let source = deadline_api::auth::get_credentials_source(config.as_ref());
    Ok(source.to_string())
}

#[pyfunction]
#[pyo3(signature = (config_path=None))]
pub fn check_auth_status(py: Python<'_>, config_path: Option<&str>) -> PyResult<PyObject> {
    let config = crate::load_config(config_path).ok();
    let rt = crate::make_runtime()?;
    let result = rt.block_on(async {
        let source = deadline_api::auth::get_credentials_source(config.as_ref());
        let status = deadline_api::auth::check_authentication_status(config.as_ref()).await;
        let api_available = status == deadline_api::auth::AwsAuthenticationStatus::Authenticated;
        (source.to_string(), status.to_string(), api_available)
    });
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
    let config = crate::load_config(config_path).ok();
    let notify = |msg: &str| {
        if let Some(ref cb) = on_progress {
            Python::with_gil(|py| { let _ = cb.call1(py, (msg,)); });
        }
    };
    let rt = crate::make_runtime()?;
    let result = rt.block_on(async {
        notify("Checking credentials source...");
        let source = deadline_api::auth::get_credentials_source(config.as_ref());
        notify("Checking authentication status...");
        let status = deadline_api::auth::check_authentication_status(config.as_ref()).await;
        let api_available = status == deadline_api::auth::AwsAuthenticationStatus::Authenticated;
        notify("Done");
        (source.to_string(), status.to_string(), api_available)
    });
    let dict = pyo3::types::PyDict::new(py);
    dict.set_item("credentials_source", result.0)?;
    dict.set_item("auth_status", result.1)?;
    dict.set_item("api_available", result.2)?;
    Ok(dict.into_any().unbind())
}

#[pyfunction]
#[pyo3(signature = (config_path=None))]
pub fn check_api_available(config_path: Option<&str>) -> PyResult<bool> {
    let config = crate::load_config(config_path).ok();
    let rt = crate::make_runtime()?;
    let status = rt.block_on(deadline_api::auth::check_authentication_status(config.as_ref()));
    Ok(status == deadline_api::auth::AwsAuthenticationStatus::Authenticated)
}

#[pyfunction]
#[pyo3(signature = (config_path=None))]
pub fn login(config_path: Option<&str>) -> PyResult<String> {
    let config = crate::load_config(config_path).ok();
    let rt = crate::make_runtime()?;
    rt.block_on(deadline_api::auth::login(None, None, config.as_ref(), None))
        .map_err(|e| DeadlineOperationError::new_err(e))
}

#[pyfunction]
#[pyo3(signature = (config_path=None))]
pub fn logout(config_path: Option<&str>) -> PyResult<String> {
    let config = crate::load_config(config_path).ok();
    deadline_api::auth::logout(config.as_ref(), None)
        .map_err(|e| DeadlineOperationError::new_err(e))
}
