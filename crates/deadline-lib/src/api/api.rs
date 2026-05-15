use crate::api::client::deadline_error;
use crate::api::errors::DeadlineError;
use crate::api::session;
use crate::config::ini::IniConfig;
use aws_sdk_deadline::operation::create_job::CreateJobOutput;
use serde_json::Value;

// ---------------------------------------------------------------------------
// Job
// ---------------------------------------------------------------------------

/// Retrieve all jobs matching a filter expression, paginating via `createdAt`
/// thresholding. Ports Python's `_list_jobs_by_filter_expression` algorithm.
///
/// The `SearchJobs` API returns at most 100 results per call. This function
/// pages through all matching jobs by sorting ascending on `CREATED_AT` and
/// using the last page's max `createdAt` as a `GREATER_THAN_EQUAL_TO` filter
/// for the next page. Jobs are deduped by `jobId`.
pub async fn list_jobs_by_filter_expression(
    farm_id: &str,
    queue_id: &str,
    filter_expression: &Value,
    config: &IniConfig,
) -> Result<Vec<Value>, DeadlineError> {
    use aws_sdk_deadline::types::{
        ComparisonOperator, DateTimeFilterExpression, FieldSortExpression, LogicalOperator,
        SearchFilterExpression, SearchGroupedFilterExpressions, SearchSortExpression, SortOrder,
    };

    let client = session::deadline_client(config).await;

    let provided_filter = build_filter_expressions(filter_expression)?;

    let sort_expr = SearchSortExpression::FieldSort(
        FieldSortExpression::builder()
            .name("CREATED_AT")
            .sort_order(SortOrder::Ascending)
            .build()
            .map_err(|e| DeadlineError::OperationError(format!("Invalid sort: {e}")))?,
    );

    let mut result_jobs: std::collections::HashMap<String, Value> =
        std::collections::HashMap::new();
    let mut threshold_filter: Option<SearchFilterExpression> = None;

    loop {
        // Build the combined filter: provided + optional timestamp threshold
        let mut filters = vec![SearchFilterExpression::GroupFilter(provided_filter.clone())];
        if let Some(ref tf) = threshold_filter {
            filters.push(tf.clone());
        }
        let combined = SearchGroupedFilterExpressions::builder()
            .set_filters(Some(filters))
            .operator(LogicalOperator::And)
            .build()
            .map_err(|e| DeadlineError::OperationError(format!("Invalid filter: {e}")))?;

        let resp = client
            .search_jobs()
            .farm_id(farm_id)
            .queue_ids(queue_id)
            .item_offset(0)
            .page_size(100)
            .filter_expressions(combined)
            .sort_expressions(sort_expr.clone())
            .send()
            .await
            .map_err(deadline_error)?;

        let jobs = resp.jobs();
        let total_results = resp.total_results() as usize;

        for job in jobs {
            if let Some(id) = job.job_id() {
                // Convert to Value for callers that still need Value access
                let mut map = serde_json::Map::new();
                map.insert("jobId".into(), Value::String(id.to_owned()));
                if let Some(name) = job.name() {
                    map.insert("name".into(), Value::String(name.to_owned()));
                }
                if let Some(status) = job.task_run_status() {
                    map.insert(
                        "taskRunStatus".into(),
                        Value::String(status.as_str().to_owned()),
                    );
                }
                if let Some(created_at) = job.created_at() {
                    map.insert("createdAt".into(), Value::String(created_at.to_string()));
                }
                if let Some(started_at) = job.started_at() {
                    map.insert("startedAt".into(), Value::String(started_at.to_string()));
                }
                if let Some(ended_at) = job.ended_at() {
                    map.insert("endedAt".into(), Value::String(ended_at.to_string()));
                }
                if let Some(counts) = job.task_run_status_counts() {
                    let counts_map: serde_json::Map<String, Value> = counts
                        .iter()
                        .map(|(k, v)| (k.as_str().to_owned(), Value::Number((*v).into())))
                        .collect();
                    map.insert("taskRunStatusCounts".into(), Value::Object(counts_map));
                }
                if let Some(queue_id) = job.queue_id() {
                    map.insert("queueId".into(), Value::String(queue_id.to_owned()));
                }
                if let Some(created_by) = job.created_by() {
                    map.insert("createdBy".into(), Value::String(created_by.to_owned()));
                }
                result_jobs.insert(id.to_owned(), Value::Object(map));
            }
        }

        if jobs.len() >= total_results {
            break;
        }

        // Edge case: all jobs on this page have identical createdAt
        let first_ts = jobs.first().and_then(|j| j.created_at());
        let last_ts = jobs.last().and_then(|j| j.created_at());
        if first_ts == last_ts {
            return Err(DeadlineError::OperationError(
                "Failure fetching jobs based on the createdAt field as more than 100 jobs \
                 have the exact same timestamp value."
                    .into(),
            ));
        }

        // Threshold: use the last job's createdAt for the next page
        let ts = *last_ts.expect("set in loop body above");
        threshold_filter = Some(SearchFilterExpression::DateTimeFilter(
            DateTimeFilterExpression::builder()
                .name("CREATED_AT")
                .date_time(ts)
                .operator(ComparisonOperator::GreaterThanEqualTo)
                .build()
                .map_err(|e| DeadlineError::OperationError(format!("Invalid filter: {e}")))?,
        ));
    }

    Ok(result_jobs.into_values().collect())
}

/// Build SDK `SearchGroupedFilterExpressions` from JSON.
pub fn build_filter_expressions(
    json: &Value,
) -> Result<aws_sdk_deadline::types::SearchGroupedFilterExpressions, DeadlineError> {
    use aws_sdk_deadline::types::{
        ComparisonOperator, DateTimeFilterExpression, LogicalOperator, SearchFilterExpression,
        SearchGroupedFilterExpressions, SearchTermFilterExpression, SearchTermMatchingType,
        StringFilterExpression, StringListFilterExpression,
    };

    let operator = match json["operator"].as_str().unwrap_or("AND") {
        "OR" => LogicalOperator::Or,
        _ => LogicalOperator::And,
    };

    let filters = json["filters"].as_array().ok_or_else(|| {
        DeadlineError::OperationError("filterExpressions.filters must be an array".into())
    })?;

    let mut sdk_filters = Vec::new();
    for f in filters {
        if let Some(stf) = f.get("searchTermFilter") {
            let term = stf["searchTerm"].as_str().unwrap_or("").to_owned();
            let match_type =
                SearchTermMatchingType::from(stf["matchType"].as_str().unwrap_or("CONTAINS"));
            sdk_filters.push(SearchFilterExpression::SearchTermFilter(
                SearchTermFilterExpression::builder()
                    .search_term(term)
                    .match_type(match_type)
                    .build()
                    .map_err(|e| DeadlineError::OperationError(format!("Invalid filter: {e}")))?,
            ));
        } else if let Some(sf) = f.get("stringFilter") {
            let name = sf["name"].as_str().unwrap_or("").to_owned();
            let value = sf["value"].as_str().unwrap_or("").to_owned();
            let op = match sf["operator"].as_str().unwrap_or("EQUAL") {
                "NOT_EQUAL" => ComparisonOperator::NotEqual,
                _ => ComparisonOperator::Equal,
            };
            sdk_filters.push(SearchFilterExpression::StringFilter(
                StringFilterExpression::builder()
                    .name(name)
                    .value(value)
                    .operator(op)
                    .build()
                    .map_err(|e| DeadlineError::OperationError(format!("Invalid filter: {e}")))?,
            ));
        } else if let Some(slf) = f.get("stringListFilter") {
            let name = slf["name"].as_str().unwrap_or("").to_owned();
            let values: Vec<String> = slf["values"]
                .as_array()
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(ToOwned::to_owned))
                        .collect()
                })
                .unwrap_or_default();
            let op = match slf["operator"].as_str().unwrap_or("ANY_EQUALS") {
                "ANY_NOT_EQUALS" => ComparisonOperator::NotEqual,
                _ => ComparisonOperator::AnyEquals,
            };
            let mut builder = StringListFilterExpression::builder()
                .name(name)
                .operator(op);
            for v in values {
                builder = builder.values(v);
            }
            sdk_filters.push(SearchFilterExpression::StringListFilter(
                builder
                    .build()
                    .map_err(|e| DeadlineError::OperationError(format!("Invalid filter: {e}")))?,
            ));
        } else if let Some(dtf) = f.get("dateTimeFilter") {
            let name = dtf["name"].as_str().unwrap_or("").to_owned();
            let datetime_str = dtf["dateTime"].as_str().unwrap_or("");
            let datetime = ::aws_smithy_types::DateTime::from_str(
                datetime_str,
                ::aws_smithy_types::date_time::Format::DateTimeWithOffset,
            )
            .map_err(|e| DeadlineError::OperationError(format!("Invalid datetime: {e}")))?;
            let op = match dtf["operator"].as_str().unwrap_or("EQUAL") {
                "GREATER_THAN_EQUAL_TO" => ComparisonOperator::GreaterThanEqualTo,
                "LESS_THAN_EQUAL_TO" => ComparisonOperator::LessThanEqualTo,
                "GREATER_THAN" => ComparisonOperator::GreaterThan,
                "LESS_THAN" => ComparisonOperator::LessThan,
                "NOT_EQUAL" => ComparisonOperator::NotEqual,
                _ => ComparisonOperator::Equal,
            };
            sdk_filters.push(SearchFilterExpression::DateTimeFilter(
                DateTimeFilterExpression::builder()
                    .name(name)
                    .date_time(datetime)
                    .operator(op)
                    .build()
                    .map_err(|e| DeadlineError::OperationError(format!("Invalid filter: {e}")))?,
            ));
        } else if let Some(gf) = f.get("groupFilter") {
            // Nested group filter — recurse into build_filter_expressions
            let nested = build_filter_expressions(gf)?;
            sdk_filters.push(SearchFilterExpression::GroupFilter(nested));
        }
    }

    SearchGroupedFilterExpressions::builder()
        .set_filters(Some(sdk_filters))
        .operator(operator)
        .build()
        .map_err(|e| DeadlineError::OperationError(format!("Invalid filter expressions: {e}")))
}

/// Build SDK `SearchSortExpression` list from JSON.
pub fn build_sort_expressions(
    json: &Value,
) -> Result<Vec<aws_sdk_deadline::types::SearchSortExpression>, DeadlineError> {
    use aws_sdk_deadline::types::{FieldSortExpression, SearchSortExpression, SortOrder};

    let arr = json
        .as_array()
        .ok_or_else(|| DeadlineError::OperationError("sortExpressions must be an array".into()))?;

    let mut result = Vec::new();
    for item in arr {
        if let Some(fs) = item.get("fieldSort") {
            let name = fs["name"].as_str().unwrap_or("CREATED_AT").to_owned();
            let order = match fs["sortOrder"].as_str().unwrap_or("DESCENDING") {
                "ASCENDING" => SortOrder::Ascending,
                _ => SortOrder::Descending,
            };
            result.push(SearchSortExpression::FieldSort(
                FieldSortExpression::builder()
                    .name(name)
                    .sort_order(order)
                    .build()
                    .map_err(|e| DeadlineError::OperationError(format!("Invalid sort: {e}")))?,
            ));
        }
    }
    Ok(result)
}

// ---------------------------------------------------------------------------
// Batch APIs (identifier construction logic)
// ---------------------------------------------------------------------------

/// Send a single `BatchGetStep` request for up to 100 step identifiers.
/// Returns raw JSON with `steps` and `errors` arrays.
pub async fn batch_get_steps_page(
    identifiers: &[Value],
    config: &IniConfig,
) -> Result<Value, DeadlineError> {
    let client = session::deadline_client(config).await;
    let mut ids = Vec::new();
    for id in identifiers {
        let builder = aws_sdk_deadline::types::BatchGetStepIdentifier::builder()
            .farm_id(id["farmId"].as_str().unwrap_or(""))
            .queue_id(id["queueId"].as_str().unwrap_or(""))
            .job_id(id["jobId"].as_str().unwrap_or(""))
            .step_id(id["stepId"].as_str().unwrap_or(""))
            .build()
            .map_err(|e| DeadlineError::OperationError(e.to_string()))?;
        ids.push(builder);
    }
    let output = client
        .batch_get_step()
        .set_identifiers(Some(ids))
        .send()
        .await
        .map_err(deadline_error)?;
    // Convert typed output to Value for the batch_get helper
    let steps: Vec<Value> = output
        .steps()
        .iter()
        .map(|s| {
            let mut m = serde_json::Map::new();
            m.insert("farmId".into(), Value::String(s.farm_id().to_owned()));
            m.insert("queueId".into(), Value::String(s.queue_id().to_owned()));
            m.insert("jobId".into(), Value::String(s.job_id().to_owned()));
            m.insert("stepId".into(), Value::String(s.step_id().to_owned()));
            m.insert("name".into(), Value::String(s.name().to_owned()));
            m.insert(
                "lifecycleStatus".into(),
                Value::String(s.lifecycle_status().as_str().to_owned()),
            );
            if !s.task_run_status().as_str().contains("no value") {
                m.insert(
                    "taskRunStatus".into(),
                    Value::String(s.task_run_status().as_str().to_owned()),
                );
            }
            m.insert(
                "createdAt".into(),
                Value::String(crate::api::responses::format_datetime(s.created_at())),
            );
            if let Some(dt) = s.started_at() {
                m.insert(
                    "startedAt".into(),
                    Value::String(crate::api::responses::format_datetime(dt)),
                );
            }
            if let Some(dt) = s.ended_at() {
                m.insert(
                    "endedAt".into(),
                    Value::String(crate::api::responses::format_datetime(dt)),
                );
            }
            let counts_map: serde_json::Map<String, Value> = s
                .task_run_status_counts()
                .iter()
                .map(|(k, v)| (k.as_str().to_owned(), Value::Number((*v).into())))
                .collect();
            if !counts_map.is_empty() {
                m.insert("taskRunStatusCounts".into(), Value::Object(counts_map));
            }
            Value::Object(m)
        })
        .collect();
    let errors: Vec<Value> = output
        .errors()
        .iter()
        .map(|e| {
            let mut m = serde_json::Map::new();
            m.insert("farmId".into(), Value::String(e.farm_id().to_owned()));
            m.insert("queueId".into(), Value::String(e.queue_id().to_owned()));
            m.insert("jobId".into(), Value::String(e.job_id().to_owned()));
            m.insert("stepId".into(), Value::String(e.step_id().to_owned()));
            m.insert("code".into(), Value::String(e.code().as_str().to_owned()));
            m.insert("message".into(), Value::String(e.message().to_owned()));
            Value::Object(m)
        })
        .collect();
    Ok(serde_json::json!({"steps": steps, "errors": errors}))
}

/// Send a single `BatchGetTask` request for up to 100 task identifiers.
/// Returns raw JSON with `tasks` and `errors` arrays.
pub async fn batch_get_tasks_page(
    identifiers: &[Value],
    config: &IniConfig,
) -> Result<Value, DeadlineError> {
    let client = session::deadline_client(config).await;
    let mut ids = Vec::new();
    for id in identifiers {
        let builder = aws_sdk_deadline::types::BatchGetTaskIdentifier::builder()
            .farm_id(id["farmId"].as_str().unwrap_or(""))
            .queue_id(id["queueId"].as_str().unwrap_or(""))
            .job_id(id["jobId"].as_str().unwrap_or(""))
            .step_id(id["stepId"].as_str().unwrap_or(""))
            .task_id(id["taskId"].as_str().unwrap_or(""))
            .build()
            .map_err(|e| DeadlineError::OperationError(e.to_string()))?;
        ids.push(builder);
    }
    let output = client
        .batch_get_task()
        .set_identifiers(Some(ids))
        .send()
        .await
        .map_err(deadline_error)?;
    let tasks: Vec<Value> = output
        .tasks()
        .iter()
        .map(|t| {
            let mut m = serde_json::Map::new();
            m.insert("farmId".into(), Value::String(t.farm_id().to_owned()));
            m.insert("queueId".into(), Value::String(t.queue_id().to_owned()));
            m.insert("jobId".into(), Value::String(t.job_id().to_owned()));
            m.insert("stepId".into(), Value::String(t.step_id().to_owned()));
            m.insert("taskId".into(), Value::String(t.task_id().to_owned()));
            if let Some(params) = t.parameters() {
                let params_map: serde_json::Map<String, Value> = params
                    .iter()
                    .map(|(k, v)| {
                        let val = match v {
                            aws_sdk_deadline::types::TaskParameterValue::Int(s) => {
                                serde_json::json!({"int": s})
                            }
                            aws_sdk_deadline::types::TaskParameterValue::Float(s) => {
                                serde_json::json!({"float": s})
                            }
                            aws_sdk_deadline::types::TaskParameterValue::String(s) => {
                                serde_json::json!({"string": s})
                            }
                            aws_sdk_deadline::types::TaskParameterValue::Path(s) => {
                                serde_json::json!({"path": s})
                            }
                            _ => Value::Null,
                        };
                        (k.clone(), val)
                    })
                    .collect::<std::collections::BTreeMap<_, _>>()
                    .into_iter()
                    .collect();
                m.insert("parameters".into(), Value::Object(params_map));
            }
            m.insert(
                "createdAt".into(),
                Value::String(crate::api::responses::format_datetime(t.created_at())),
            );
            m.insert(
                "runStatus".into(),
                Value::String(t.run_status().as_str().to_owned()),
            );
            if let Some(dt) = t.started_at() {
                m.insert(
                    "startedAt".into(),
                    Value::String(crate::api::responses::format_datetime(dt)),
                );
            }
            if let Some(dt) = t.ended_at() {
                m.insert(
                    "endedAt".into(),
                    Value::String(crate::api::responses::format_datetime(dt)),
                );
            }
            Value::Object(m)
        })
        .collect();
    let errors: Vec<Value> = output
        .errors()
        .iter()
        .map(|e| {
            let mut m = serde_json::Map::new();
            m.insert("farmId".into(), Value::String(e.farm_id().to_owned()));
            m.insert("queueId".into(), Value::String(e.queue_id().to_owned()));
            m.insert("jobId".into(), Value::String(e.job_id().to_owned()));
            m.insert("stepId".into(), Value::String(e.step_id().to_owned()));
            m.insert("taskId".into(), Value::String(e.task_id().to_owned()));
            m.insert("code".into(), Value::String(e.code().as_str().to_owned()));
            m.insert("message".into(), Value::String(e.message().to_owned()));
            Value::Object(m)
        })
        .collect();
    Ok(serde_json::json!({"tasks": tasks, "errors": errors}))
}

// ---------------------------------------------------------------------------
// Job creation
// ---------------------------------------------------------------------------

/// Convert our JSON attachments into the SDK's typed Attachments struct.
fn build_sdk_attachments(
    att: &Value,
) -> Result<aws_sdk_deadline::types::Attachments, DeadlineError> {
    let fs_str = att
        .get("fileSystem")
        .and_then(|v| v.as_str())
        .unwrap_or("COPIED");
    let file_system: aws_sdk_deadline::types::JobAttachmentsFileSystem = fs_str.into();

    let mut builder = aws_sdk_deadline::types::Attachments::builder().file_system(file_system);

    if let Some(manifests) = att.get("manifests").and_then(|v| v.as_array()) {
        for m in manifests {
            let mut mb = aws_sdk_deadline::types::ManifestProperties::builder();
            if let Some(v) = m.get("rootPath").and_then(|v| v.as_str()) {
                mb = mb.root_path(v);
            }
            if let Some(v) = m.get("rootPathFormat").and_then(|v| v.as_str()) {
                let fmt: aws_sdk_deadline::types::PathFormat = v.into();
                mb = mb.root_path_format(fmt);
            }
            if let Some(v) = m.get("inputManifestPath").and_then(|v| v.as_str()) {
                mb = mb.input_manifest_path(v);
            }
            if let Some(v) = m.get("inputManifestHash").and_then(|v| v.as_str()) {
                mb = mb.input_manifest_hash(v);
            }
            if let Some(v) = m.get("fileSystemLocationName").and_then(|v| v.as_str()) {
                mb = mb.file_system_location_name(v);
            }
            if let Some(dirs) = m
                .get("outputRelativeDirectories")
                .and_then(|v| v.as_array())
            {
                for d in dirs {
                    if let Some(s) = d.as_str() {
                        mb = mb.output_relative_directories(s);
                    }
                }
            }
            builder = builder.manifests(mb.build().map_err(|e| {
                DeadlineError::OperationError(format!("Failed to build manifest properties: {e}"))
            })?);
        }
    }

    builder
        .build()
        .map_err(|e| DeadlineError::OperationError(format!("Failed to build attachments: {e}")))
}

/// Call `CreateJob` API. The `args` map should contain farmId, queueId,
/// template, templateType, priority, and optionally parameters, attachments,
/// storageProfileId, maxFailedTasksCount, maxRetriesPerTask, maxWorkerCount,
/// targetTaskRunStatus.
pub async fn create_job(
    args: &serde_json::Map<String, Value>,
    config: &IniConfig,
) -> Result<CreateJobOutput, DeadlineError> {
    let client = session::deadline_client(config).await;

    let farm_id = args.get("farmId").and_then(|v| v.as_str()).unwrap_or("");
    let queue_id = args.get("queueId").and_then(|v| v.as_str()).unwrap_or("");
    let template = args.get("template").and_then(|v| v.as_str()).unwrap_or("");
    let template_type: aws_sdk_deadline::types::JobTemplateType = args
        .get("templateType")
        .and_then(|v| v.as_str())
        .unwrap_or("YAML")
        .into();
    let priority = args.get("priority").and_then(Value::as_i64).unwrap_or(50) as i32;

    let mut req = client
        .create_job()
        .farm_id(farm_id)
        .queue_id(queue_id)
        .template(template)
        .template_type(template_type)
        .priority(priority);

    if let Some(v) = args.get("storageProfileId").and_then(|v| v.as_str()) {
        req = req.storage_profile_id(v);
    }
    if let Some(v) = args.get("maxFailedTasksCount").and_then(Value::as_i64) {
        req = req.max_failed_tasks_count(v as i32);
    }
    if let Some(v) = args.get("maxRetriesPerTask").and_then(Value::as_i64) {
        req = req.max_retries_per_task(v as i32);
    }
    if let Some(v) = args.get("maxWorkerCount").and_then(Value::as_i64) {
        req = req.max_worker_count(v as i32);
    }
    if let Some(v) = args.get("targetTaskRunStatus").and_then(|v| v.as_str()) {
        let status: aws_sdk_deadline::types::CreateJobTargetTaskRunStatus = v.into();
        req = req.target_task_run_status(status);
    }
    if let Some(params) = args.get("parameters").and_then(|v| v.as_object()) {
        for (name, value) in params {
            let param = if let Some(s) = value.get("string").and_then(|v| v.as_str()) {
                aws_sdk_deadline::types::JobParameter::String(s.to_owned())
            } else if let Some(s) = value.get("int").and_then(|v| v.as_str()) {
                aws_sdk_deadline::types::JobParameter::Int(s.to_owned())
            } else if let Some(s) = value.get("float").and_then(|v| v.as_str()) {
                aws_sdk_deadline::types::JobParameter::Float(s.to_owned())
            } else if let Some(s) = value.get("path").and_then(|v| v.as_str()) {
                aws_sdk_deadline::types::JobParameter::Path(s.to_owned())
            } else {
                continue;
            };
            req = req.parameters(name.clone(), param);
        }
    }
    if let Some(att) = args.get("attachments") {
        let att_builder = build_sdk_attachments(att)?;
        req = req.attachments(att_builder);
    }

    req.send().await.map_err(deadline_error)
}

/// Poll `GetJob` until the job exits `CREATE_IN_PROGRESS`.
/// Returns (success, `lifecycle_status_message`).
pub async fn wait_for_create_job_to_complete(
    farm_id: &str,
    queue_id: &str,
    job_id: &str,
    config: &IniConfig,
    continue_callback: impl Fn() -> bool,
) -> Result<(bool, String), DeadlineError> {
    let initial_delay = std::time::Duration::from_millis(300);
    let max_delay = std::time::Duration::from_secs(5);
    let timeout = std::time::Duration::from_secs(300);

    let start = std::time::Instant::now();
    let mut delay = initial_delay;
    let client = session::deadline_client(config).await;

    tokio::time::sleep(initial_delay).await;

    loop {
        if start.elapsed() >= timeout {
            return Err(DeadlineError::OperationError(format!(
                "Timed out after {} seconds while waiting for Job to be created: {job_id}",
                timeout.as_secs()
            )));
        }

        if !continue_callback() {
            return Err(DeadlineError::OperationError(
                "CreateJob wait was canceled".into(),
            ));
        }

        let job = client
            .get_job()
            .farm_id(farm_id)
            .queue_id(queue_id)
            .job_id(job_id)
            .send()
            .await
            .map_err(deadline_error)?;

        let status = job.lifecycle_status.as_str();
        let message = job.lifecycle_status_message.clone();

        match status {
            "CREATE_IN_PROGRESS" => {
                tokio::time::sleep(delay).await;
                delay = std::cmp::min(delay * 2, max_delay);
            }
            "CREATE_FAILED" => return Ok((false, message)),
            _ => return Ok((true, message)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;
    use wiremock::matchers::{method, path_regex};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    async fn setup_env(server: &MockServer) {
        let url = format!("http://localhost:{}", server.address().port());
        unsafe {
            std::env::set_var("AWS_ENDPOINT_URL_DEADLINE", &url);
            std::env::set_var("AWS_ACCESS_KEY_ID", "AKIAIOSFODNN7EXAMPLE");
            std::env::set_var(
                "AWS_SECRET_ACCESS_KEY",
                "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            );
            std::env::set_var("AWS_DEFAULT_REGION", "us-west-2");
            std::env::set_var("AWS_CONFIG_FILE", "/dev/null");
        }
    }

    #[tokio::test]
    #[serial]
    async fn wait_for_create_job_cancels_when_callback_returns_false() {
        let server = MockServer::start().await;
        setup_env(&server).await;

        // Mock GetJob returning CREATE_IN_PROGRESS (would loop forever without cancellation)
        Mock::given(method("GET"))
            .and(path_regex(".*/jobs/job-abc"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "jobId": "job-abc",
                "lifecycleStatus": "CREATE_IN_PROGRESS",
                "lifecycleStatusMessage": "Creating...",
            })))
            .mount(&server)
            .await;

        // Pass a callback that always returns false (simulates SIGINT)
        let result = wait_for_create_job_to_complete(
            "farm-abc",
            "queue-abc",
            "job-abc",
            &IniConfig::new(),
            || false,
        )
        .await;

        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("canceled"),
            "Expected cancellation error, got: {err}"
        );
    }

    #[test]
    fn build_filter_string_list_filter_produces_filter() {
        // Regression: stringListFilter was silently dropped, causing
        // ValidationException from the real API (empty filters array).
        let json = serde_json::json!({
            "filters": [{
                "stringListFilter": {
                    "name": "TASK_RUN_STATUS",
                    "operator": "ANY_EQUALS",
                    "values": ["READY", "RUNNING"]
                }
            }],
            "operator": "OR"
        });
        let result = build_filter_expressions(&json).unwrap();
        assert_eq!(result.filters().len(), 1);
    }

    #[test]
    fn build_filter_date_time_filter_produces_filter() {
        // Regression: dateTimeFilter was silently dropped.
        let json = serde_json::json!({
            "filters": [{
                "dateTimeFilter": {
                    "name": "ENDED_AT",
                    "dateTime": "2024-06-15T10:00:00+00:00",
                    "operator": "GREATER_THAN_EQUAL_TO"
                }
            }],
            "operator": "AND"
        });
        let result = build_filter_expressions(&json).unwrap();
        assert_eq!(result.filters().len(), 1);
    }

    #[test]
    fn build_filter_unknown_filter_type_produces_empty() {
        // Unknown filter types are silently skipped (not an error).
        let json = serde_json::json!({
            "filters": [{"unknownFilter": {"name": "X"}}],
            "operator": "AND"
        });
        let result = build_filter_expressions(&json).unwrap();
        assert_eq!(result.filters().len(), 0);
    }
}
