use pyo3::prelude::*;

use crate::DeadlineOperationError;

#[pyfunction]
#[pyo3(signature = (config_path=None))]
pub fn list_farms<'py>(py: Python<'py>, config_path: Option<&str>) -> PyResult<Bound<'py, PyAny>> {
    let config = crate::load_config(config_path)?;
    let rt = crate::make_runtime()?;
    let result = rt.block_on(deadline_api::api::list_farms(Some(&config), None))
        .map_err(|e| DeadlineOperationError::new_err(e.to_string()))?;
    pythonize::pythonize(py, &result)
        .map_err(|e| DeadlineOperationError::new_err(e.to_string()))
}

#[pyfunction]
#[pyo3(signature = (farm_id, config_path=None))]
pub fn get_farm<'py>(py: Python<'py>, farm_id: &str, config_path: Option<&str>) -> PyResult<Bound<'py, PyAny>> {
    let config = crate::load_config(config_path)?;
    let rt = crate::make_runtime()?;
    let result = rt.block_on(deadline_api::api::get_farm(farm_id, Some(&config), None))
        .map_err(|e| DeadlineOperationError::new_err(e.to_string()))?;
    pythonize::pythonize(py, &result)
        .map_err(|e| DeadlineOperationError::new_err(e.to_string()))
}

#[pyfunction]
#[pyo3(signature = (farm_id, config_path=None))]
pub fn list_queues<'py>(py: Python<'py>, farm_id: &str, config_path: Option<&str>) -> PyResult<Bound<'py, PyAny>> {
    let config = crate::load_config(config_path)?;
    let rt = crate::make_runtime()?;
    let result = rt.block_on(deadline_api::api::list_queues(farm_id, Some(&config), None))
        .map_err(|e| DeadlineOperationError::new_err(e.to_string()))?;
    pythonize::pythonize(py, &result)
        .map_err(|e| DeadlineOperationError::new_err(e.to_string()))
}

#[pyfunction]
#[pyo3(signature = (farm_id, queue_id, config_path=None))]
pub fn get_queue<'py>(py: Python<'py>, farm_id: &str, queue_id: &str, config_path: Option<&str>) -> PyResult<Bound<'py, PyAny>> {
    let config = crate::load_config(config_path)?;
    let rt = crate::make_runtime()?;
    let result = rt.block_on(deadline_api::api::get_queue(farm_id, queue_id, Some(&config), None))
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
    let result = rt.block_on(deadline_api::api::list_storage_profiles_for_queue(farm_id, queue_id, Some(&config), None))
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
    let result = rt.block_on(deadline_api::queue_parameters::get_queue_parameter_definitions(farm_id, queue_id, Some(&config), None))
        .map_err(|e| DeadlineOperationError::new_err(e.to_string()))?;
    pythonize::pythonize(py, &result)
        .map_err(|e| DeadlineOperationError::new_err(e.to_string()))
}
