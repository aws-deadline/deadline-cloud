use pyo3::prelude::*;

use crate::DeadlineOperationError;

#[pyfunction]
#[pyo3(signature = (name, config_path=None))]
pub fn get_setting(name: &str, config_path: Option<&str>) -> PyResult<String> {
    let config = crate::load_config(config_path)?;
    deadline_lib::config::config_file::get_setting(name, &config)
        .map_err(|e| DeadlineOperationError::new_err(e.to_string()))
}

#[pyfunction]
#[pyo3(signature = (name, value, config_path=None))]
pub fn set_setting(name: &str, value: &str, config_path: Option<&str>) -> PyResult<()> {
    let path = match config_path {
        Some(p) => std::path::PathBuf::from(p),
        None => deadline_lib::config::config_file::get_config_file_path(),
    };
    let mut config = deadline_lib::config::config_file::read_config_from(&path)
        .map_err(|e| DeadlineOperationError::new_err(e.to_string()))?;
    deadline_lib::config::config_file::set_setting(name, value, &mut config)
        .map_err(|e| DeadlineOperationError::new_err(e.to_string()))?;
    deadline_lib::config::config_file::write_config_to(&config, &path)
        .map_err(|e| DeadlineOperationError::new_err(e.to_string()))
}

#[pyfunction]
#[pyo3(signature = (config_path=None))]
pub fn read_config(py: Python<'_>, config_path: Option<&str>) -> PyResult<Py<PyAny>> {
    let config = crate::load_config(config_path)?;
    let outer = pyo3::types::PyDict::new(py);
    for (section, keys) in config.iter_sections() {
        let inner = pyo3::types::PyDict::new(py);
        for (key, value) in keys {
            inner.set_item(key, value)?;
        }
        outer.set_item(section, inner)?;
    }
    Ok(outer.into_any().unbind())
}
