use pyo3::prelude::*;

use crate::DeadlineOperationError;

#[pyfunction]
#[pyo3(signature = (config_path=None))]
pub fn list_farms<'py>(py: Python<'py>, config_path: Option<&str>) -> PyResult<Bound<'py, PyAny>> {
    let config = crate::load_config(config_path)?;
    let rt = crate::make_runtime()?;
    let dl = rt.block_on(deadline_api::session::deadline_client(Some(&config)));
    let builder = deadline_api::client::apply_dcm_principal(dl.list_farms(), Some(&config));
    let result = rt.block_on(deadline_api::client::collect_paginated_raw("farms", |token| {
        let builder = builder.clone();
        async move {
            let cap = deadline_api::response_capture::ResponseBodyCapture::new();
            let mut req = builder;
            if let Some(t) = token { req = req.next_token(t); }
            req.customize().interceptor(cap.clone())
                .send().await.map_err(deadline_api::client::deadline_error)?;
            cap.json().map_err(|e| deadline_api::errors::DeadlineError::OperationError(e.to_string()))
        }
    })).map_err(|e| DeadlineOperationError::new_err(e.to_string()))?;
    pythonize::pythonize(py, &result)
        .map_err(|e| DeadlineOperationError::new_err(e.to_string()))
}

#[pyfunction]
#[pyo3(signature = (farm_id, config_path=None))]
pub fn get_farm<'py>(py: Python<'py>, farm_id: &str, config_path: Option<&str>) -> PyResult<Bound<'py, PyAny>> {
    let config = crate::load_config(config_path)?;
    let rt = crate::make_runtime()?;
    let dl = rt.block_on(deadline_api::session::deadline_client(Some(&config)));
    let cap = deadline_api::response_capture::ResponseBodyCapture::new();
    rt.block_on(dl.get_farm().farm_id(farm_id)
        .customize().interceptor(cap.clone())
        .send())
        .map_err(|e| DeadlineOperationError::new_err(deadline_api::client::format_sdk_error(&e)))?;
    let result = cap.json()
        .map_err(|e| DeadlineOperationError::new_err(e.to_string()))?;
    pythonize::pythonize(py, &result)
        .map_err(|e| DeadlineOperationError::new_err(e.to_string()))
}

#[pyfunction]
#[pyo3(signature = (farm_id, config_path=None))]
pub fn list_queues<'py>(py: Python<'py>, farm_id: &str, config_path: Option<&str>) -> PyResult<Bound<'py, PyAny>> {
    let config = crate::load_config(config_path)?;
    let rt = crate::make_runtime()?;
    let dl = rt.block_on(deadline_api::session::deadline_client(Some(&config)));
    let builder = deadline_api::client::apply_dcm_principal(dl.list_queues().farm_id(farm_id), Some(&config));
    let result = rt.block_on(deadline_api::client::collect_paginated_raw("queues", |token| {
        let builder = builder.clone();
        async move {
            let cap = deadline_api::response_capture::ResponseBodyCapture::new();
            let mut req = builder;
            if let Some(t) = token { req = req.next_token(t); }
            req.customize().interceptor(cap.clone())
                .send().await.map_err(deadline_api::client::deadline_error)?;
            cap.json().map_err(|e| deadline_api::errors::DeadlineError::OperationError(e.to_string()))
        }
    })).map_err(|e| DeadlineOperationError::new_err(e.to_string()))?;
    pythonize::pythonize(py, &result)
        .map_err(|e| DeadlineOperationError::new_err(e.to_string()))
}

#[pyfunction]
#[pyo3(signature = (farm_id, queue_id, config_path=None))]
pub fn get_queue<'py>(py: Python<'py>, farm_id: &str, queue_id: &str, config_path: Option<&str>) -> PyResult<Bound<'py, PyAny>> {
    let config = crate::load_config(config_path)?;
    let rt = crate::make_runtime()?;
    let dl = rt.block_on(deadline_api::session::deadline_client(Some(&config)));
    let cap = deadline_api::response_capture::ResponseBodyCapture::new();
    rt.block_on(dl.get_queue().farm_id(farm_id).queue_id(queue_id)
        .customize().interceptor(cap.clone())
        .send())
        .map_err(|e| DeadlineOperationError::new_err(deadline_api::client::format_sdk_error(&e)))?;
    let result = cap.json()
        .map_err(|e| DeadlineOperationError::new_err(e.to_string()))?;
    pythonize::pythonize(py, &result)
        .map_err(|e| DeadlineOperationError::new_err(e.to_string()))
}

#[pyfunction]
#[pyo3(signature = (farm_id, queue_id, config_path=None))]
pub fn list_storage_profiles_for_queue<'py>(
    py: Python<'py>,
    farm_id: &str,
    queue_id: &str,
    config_path: Option<&str>,
) -> PyResult<Bound<'py, PyAny>> {
    let config = crate::load_config(config_path)?;
    let rt = crate::make_runtime()?;
    let result = rt.block_on(deadline_api::api::list_storage_profiles_for_queue(farm_id, queue_id, Some(&config)))
        .map_err(|e| DeadlineOperationError::new_err(e.to_string()))?;
    pythonize::pythonize(py, &result)
        .map_err(|e| DeadlineOperationError::new_err(e.to_string()))
}

#[pyfunction]
#[pyo3(signature = (farm_id, queue_id, config_path=None))]
pub fn get_queue_parameter_definitions<'py>(
    py: Python<'py>,
    farm_id: &str,
    queue_id: &str,
    config_path: Option<&str>,
) -> PyResult<Bound<'py, PyAny>> {
    let config = crate::load_config(config_path)?;
    let rt = crate::make_runtime()?;
    let result = rt.block_on(deadline_api::queue_parameters::get_queue_parameter_definitions(farm_id, queue_id, Some(&config)))
        .map_err(|e| DeadlineOperationError::new_err(e.to_string()))?;
    pythonize::pythonize(py, &result)
        .map_err(|e| DeadlineOperationError::new_err(e.to_string()))
}
