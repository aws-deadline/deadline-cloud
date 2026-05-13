use pyo3::prelude::*;

use crate::DeadlineOperationError;

#[pyfunction]
#[pyo3(signature = (config_path=None))]
pub fn list_farms<'py>(py: Python<'py>, config_path: Option<&str>) -> PyResult<Bound<'py, PyAny>> {
    let config = crate::load_config(config_path)?;
    let rt = crate::make_runtime()?;
    let dl = rt.block_on(deadline_api::session::deadline_client(&config));
    let builder = deadline_api::client::apply_dcm_principal(dl.list_farms(), &config);
    let pages = rt
        .block_on(deadline_api::client::collect_paginated(
            builder.into_paginator().send(),
        ))
        .map_err(|e| DeadlineOperationError::new_err(e.to_string()))?;
    // AWS SDK method references (e.g. ListFarmsOutput::farms) are unreadably long
    #[allow(clippy::redundant_closure_for_method_calls, reason = "AWS SDK method references are unreadably long")]
    let farms: Vec<serde_json::Value> = pages
        .iter()
        .flat_map(|p| p.farms())
        .map(|f| serde_json::json!({"farmId": f.farm_id(), "displayName": f.display_name(), "createdAt": f.created_at().to_string(), "createdBy": f.created_by()}))
        .collect();
    let result = serde_json::json!({"farms": farms});
    pythonize::pythonize(py, &result).map_err(|e| DeadlineOperationError::new_err(e.to_string()))
}

#[pyfunction]
#[pyo3(signature = (farm_id, config_path=None))]
pub fn get_farm<'py>(
    py: Python<'py>,
    farm_id: &str,
    config_path: Option<&str>,
) -> PyResult<Bound<'py, PyAny>> {
    let config = crate::load_config(config_path)?;
    let rt = crate::make_runtime()?;
    let dl = rt.block_on(deadline_api::session::deadline_client(&config));
    let output = rt
        .block_on(dl.get_farm().farm_id(farm_id).send())
        .map_err(|e| DeadlineOperationError::new_err(deadline_api::client::format_sdk_error(&e)))?;
    let resp = deadline_api::responses::FarmResponse::from(output);
    let result =
        serde_json::to_value(&resp).map_err(|e| DeadlineOperationError::new_err(e.to_string()))?;
    pythonize::pythonize(py, &result).map_err(|e| DeadlineOperationError::new_err(e.to_string()))
}

#[pyfunction]
#[pyo3(signature = (farm_id, config_path=None))]
pub fn list_queues<'py>(
    py: Python<'py>,
    farm_id: &str,
    config_path: Option<&str>,
) -> PyResult<Bound<'py, PyAny>> {
    let config = crate::load_config(config_path)?;
    let rt = crate::make_runtime()?;
    let dl = rt.block_on(deadline_api::session::deadline_client(&config));
    let builder =
        deadline_api::client::apply_dcm_principal(dl.list_queues().farm_id(farm_id), &config);
    let pages = rt
        .block_on(deadline_api::client::collect_paginated(
            builder.into_paginator().send(),
        ))
        .map_err(|e| DeadlineOperationError::new_err(e.to_string()))?;
    #[allow(clippy::redundant_closure_for_method_calls, reason = "AWS SDK method references are unreadably long")]
    let queues: Vec<serde_json::Value> = pages
        .iter()
        .flat_map(|p| p.queues())
        .map(|q| serde_json::json!({"queueId": q.queue_id(), "displayName": q.display_name(), "status": q.status().as_str(), "createdAt": q.created_at().to_string(), "createdBy": q.created_by()}))
        .collect();
    let result = serde_json::json!({"queues": queues});
    pythonize::pythonize(py, &result).map_err(|e| DeadlineOperationError::new_err(e.to_string()))
}

#[pyfunction]
#[pyo3(signature = (farm_id, queue_id, config_path=None))]
pub fn get_queue<'py>(
    py: Python<'py>,
    farm_id: &str,
    queue_id: &str,
    config_path: Option<&str>,
) -> PyResult<Bound<'py, PyAny>> {
    let config = crate::load_config(config_path)?;
    let rt = crate::make_runtime()?;
    let dl = rt.block_on(deadline_api::session::deadline_client(&config));
    let output = rt
        .block_on(dl.get_queue().farm_id(farm_id).queue_id(queue_id).send())
        .map_err(|e| DeadlineOperationError::new_err(deadline_api::client::format_sdk_error(&e)))?;
    let resp = deadline_api::responses::QueueResponse::from(output);
    let result =
        serde_json::to_value(&resp).map_err(|e| DeadlineOperationError::new_err(e.to_string()))?;
    pythonize::pythonize(py, &result).map_err(|e| DeadlineOperationError::new_err(e.to_string()))
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
    let result = rt
        .block_on(async {
            let client = deadline_api::session::deadline_client(&config).await;
            let pages = deadline_api::client::collect_paginated(
                client
                    .list_storage_profiles_for_queue()
                    .farm_id(farm_id)
                    .queue_id(queue_id)
                    .into_paginator()
                    .send(),
            )
            .await?;
            #[allow(
                clippy::redundant_closure_for_method_calls,
                reason = "AWS SDK method references are unreadably long"
            )]
            let profiles: Vec<serde_json::Value> = pages
                .iter()
                .flat_map(|p| p.storage_profiles())
                .map(|sp| {
                    serde_json::json!({
                        "storageProfileId": sp.storage_profile_id(),
                        "displayName": sp.display_name(),
                        "osFamily": sp.os_family().as_str(),
                    })
                })
                .collect();
            Ok::<_, deadline_api::errors::DeadlineError>(
                serde_json::json!({"storageProfiles": profiles}),
            )
        })
        .map_err(|e| DeadlineOperationError::new_err(e.to_string()))?;
    pythonize::pythonize(py, &result).map_err(|e| DeadlineOperationError::new_err(e.to_string()))
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
    let result = rt
        .block_on(
            deadline_api::queue_parameters::get_queue_parameter_definitions(
                farm_id,
                queue_id,
                &config,
            ),
        )
        .map_err(|e| DeadlineOperationError::new_err(e.to_string()))?;
    pythonize::pythonize(py, &result).map_err(|e| DeadlineOperationError::new_err(e.to_string()))
}
