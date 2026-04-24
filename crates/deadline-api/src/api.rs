use crate::errors::DeadlineError;
use crate::telemetry::{TelemetryClient, with_telemetry_latency_async};
use crate::{auth, raw_response::ResponseBodyCapture, session};
use deadline_config::ini::IniConfig;
use serde_json::Value;
use std::future::Future;

/// Format any AWS SDK error to include the error code and message.
///
/// Uses the common `ProvideErrorMetadata` trait from `aws-smithy-types`,
/// so this works for errors from any AWS SDK crate (Deadline, CloudWatch,
/// STS, S3, etc.). For `ServiceError`, extracts the error code and message
/// from the response. For transport-level errors (timeout, dispatch failure),
/// uses `DisplayErrorContext` to show the full causal chain instead of just
/// "dispatch failure".
pub fn format_sdk_error<E, R>(err: &aws_sdk_deadline::error::SdkError<E, R>) -> String
where
    E: std::fmt::Display + aws_smithy_types::error::metadata::ProvideErrorMetadata + std::error::Error + 'static,
    R: std::fmt::Debug,
{
    match err {
        aws_sdk_deadline::error::SdkError::ServiceError(e) => {
            let inner = e.err();
            let code = inner.code().unwrap_or("Unknown");
            let msg = inner.message().unwrap_or("No message");
            format!("{code}: {msg}")
        }
        other => format!("{}", aws_smithy_types::error::display::DisplayErrorContext(other)),
    }
}

fn sdk_err<E: std::fmt::Display + aws_smithy_types::error::metadata::ProvideErrorMetadata + std::error::Error + 'static>(
    e: aws_sdk_deadline::error::SdkError<E>,
) -> DeadlineError {
    DeadlineError::OperationError(format_sdk_error(&e))
}

fn capture_err(e: serde_json::Error) -> DeadlineError {
    DeadlineError::OperationError(e.to_string())
}

// ---------------------------------------------------------------------------
// Paginated list helper
// ---------------------------------------------------------------------------

/// Generic paginated list using ResponseBodyCapture + manual nextToken loop.
/// `send_page` is called for each page with an optional nextToken.
/// `items_key` is the JSON key containing the items array (e.g. "farms").
async fn paginated_list<F, Fut>(
    items_key: &str,
    send_page: F,
) -> Result<Value, DeadlineError>
where
    F: Fn(Option<String>) -> Fut,
    Fut: Future<Output = Result<Value, DeadlineError>>,
{
    let mut all_items = Vec::new();
    let mut next_token: Option<String> = None;
    loop {
        let page = send_page(next_token.take()).await?;
        if let Some(items) = page[items_key].as_array() {
            all_items.extend(items.iter().cloned());
        }
        match page.get("nextToken").and_then(|t| t.as_str()) {
            Some(t) => next_token = Some(t.to_string()),
            None => break,
        }
    }
    Ok(serde_json::json!({items_key: all_items}))
}

/// Helper: send a single ResponseBodyCapture request and return parsed JSON.
async fn capture_send<F, R, E>(build: F) -> Result<Value, DeadlineError>
where
    F: FnOnce(ResponseBodyCapture) -> R,
    R: Future<Output = Result<(), aws_sdk_deadline::error::SdkError<E>>>,
    E: std::fmt::Display + aws_sdk_deadline::error::ProvideErrorMetadata + std::error::Error + 'static,
{
    let capture = ResponseBodyCapture::new();
    build(capture.clone()).await.map_err(sdk_err)?;
    capture.json().map_err(capture_err)
}

// ---------------------------------------------------------------------------
// Farm
// ---------------------------------------------------------------------------

pub async fn list_farms(config: Option<&IniConfig>, telemetry: Option<&TelemetryClient>) -> Result<Value, DeadlineError> {
    with_telemetry_latency_async("list_farms", config, telemetry, || async {
        let client = session::deadline_client(config).await;
        let (user_id, _) = auth::get_user_and_identity_store_id(config);
        paginated_list("farms", |token| {
            let client = client.clone();
            let user_id = user_id.clone();
            async move {
                capture_send(|cap| {
                    let mut req = client.list_farms();
                    if let Some(ref uid) = user_id { req = req.principal_id(uid.as_str()); }
                    if let Some(t) = token { req = req.next_token(t); }
                    async move { req.customize().interceptor(cap).send().await.map(|_| ()) }
                }).await
            }
        }).await
    }).await
}

pub async fn get_farm(farm_id: &str, config: Option<&IniConfig>, telemetry: Option<&TelemetryClient>) -> Result<Value, DeadlineError> {
    with_telemetry_latency_async("get_farm", config, telemetry, || async {
        let client = session::deadline_client(config).await;
        capture_send(|cap| async move {
            client.get_farm().farm_id(farm_id)
                .customize().interceptor(cap).send().await.map(|_| ())
        }).await
    }).await
}

// ---------------------------------------------------------------------------
// Queue
// ---------------------------------------------------------------------------

pub async fn list_queues(farm_id: &str, config: Option<&IniConfig>, telemetry: Option<&TelemetryClient>) -> Result<Value, DeadlineError> {
    with_telemetry_latency_async("list_queues", config, telemetry, || async {
        let client = session::deadline_client(config).await;
        let (user_id, _) = auth::get_user_and_identity_store_id(config);
        let farm_id = farm_id.to_string();
        paginated_list("queues", |token| {
            let client = client.clone();
            let user_id = user_id.clone();
            let farm_id = farm_id.clone();
            async move {
                capture_send(|cap| {
                    let mut req = client.list_queues().farm_id(&farm_id);
                    if let Some(ref uid) = user_id { req = req.principal_id(uid.as_str()); }
                    if let Some(t) = token { req = req.next_token(t); }
                    async move { req.customize().interceptor(cap).send().await.map(|_| ()) }
                }).await
            }
        }).await
    }).await
}

pub async fn get_queue(farm_id: &str, queue_id: &str, config: Option<&IniConfig>, telemetry: Option<&TelemetryClient>) -> Result<Value, DeadlineError> {
    with_telemetry_latency_async("get_queue", config, telemetry, || async {
        let client = session::deadline_client(config).await;
        capture_send(|cap| async move {
            client.get_queue().farm_id(farm_id).queue_id(queue_id)
                .customize().interceptor(cap).send().await.map(|_| ())
        }).await
    }).await
}

// ---------------------------------------------------------------------------
// Fleet
// ---------------------------------------------------------------------------

pub async fn list_fleets(farm_id: &str, config: Option<&IniConfig>, telemetry: Option<&TelemetryClient>) -> Result<Value, DeadlineError> {
    with_telemetry_latency_async("list_fleets", config, telemetry, || async {
        let client = session::deadline_client(config).await;
        let (user_id, _) = auth::get_user_and_identity_store_id(config);
        let farm_id = farm_id.to_string();
        paginated_list("fleets", |token| {
            let client = client.clone();
            let user_id = user_id.clone();
            let farm_id = farm_id.clone();
            async move {
                capture_send(|cap| {
                    let mut req = client.list_fleets().farm_id(&farm_id);
                    if let Some(ref uid) = user_id { req = req.principal_id(uid.as_str()); }
                    if let Some(t) = token { req = req.next_token(t); }
                    async move { req.customize().interceptor(cap).send().await.map(|_| ()) }
                }).await
            }
        }).await
    }).await
}

pub async fn get_fleet(farm_id: &str, fleet_id: &str, config: Option<&IniConfig>, telemetry: Option<&TelemetryClient>) -> Result<Value, DeadlineError> {
    with_telemetry_latency_async("get_fleet", config, telemetry, || async {
        let client = session::deadline_client(config).await;
        capture_send(|cap| async move {
            client.get_fleet().farm_id(farm_id).fleet_id(fleet_id)
                .customize().interceptor(cap).send().await.map(|_| ())
        }).await
    }).await
}

// ---------------------------------------------------------------------------
// Job
// ---------------------------------------------------------------------------

pub async fn list_jobs(farm_id: &str, queue_id: &str, config: Option<&IniConfig>, telemetry: Option<&TelemetryClient>) -> Result<Value, DeadlineError> {
    with_telemetry_latency_async("list_jobs", config, telemetry, || async {
        let client = session::deadline_client(config).await;
        let (user_id, _) = auth::get_user_and_identity_store_id(config);
        let farm_id = farm_id.to_string();
        let queue_id = queue_id.to_string();
        paginated_list("jobs", |token| {
            let client = client.clone();
            let user_id = user_id.clone();
            let farm_id = farm_id.clone();
            let queue_id = queue_id.clone();
            async move {
                capture_send(|cap| {
                    let mut req = client.list_jobs().farm_id(&farm_id).queue_id(&queue_id);
                    if let Some(ref uid) = user_id { req = req.principal_id(uid.as_str()); }
                    if let Some(t) = token { req = req.next_token(t); }
                    async move { req.customize().interceptor(cap).send().await.map(|_| ()) }
                }).await
            }
        }).await
    }).await
}

pub async fn search_jobs(
    farm_id: &str,
    queue_ids: &[&str],
    item_offset: i32,
    page_size: i32,
    config: Option<&IniConfig>,
    telemetry: Option<&TelemetryClient>,
) -> Result<Value, DeadlineError> {
    search_jobs_with_filters(farm_id, queue_ids, item_offset, page_size, None, None, config, telemetry).await
}

/// Search jobs with optional filter and sort expressions.
#[allow(clippy::too_many_arguments)]
pub async fn search_jobs_with_filters(
    farm_id: &str,
    queue_ids: &[&str],
    item_offset: i32,
    page_size: i32,
    filter_expressions: Option<&Value>,
    sort_expressions: Option<&Value>,
    config: Option<&IniConfig>,
    telemetry: Option<&TelemetryClient>,
) -> Result<Value, DeadlineError> {
    let filter = filter_expressions.map(build_filter_expressions).transpose()?;
    let sort = sort_expressions.map(build_sort_expressions).transpose()?;
    with_telemetry_latency_async("search_jobs", config, telemetry, || async {
        let client = session::deadline_client(config).await;
        capture_send(|cap| async move {
            let mut req = client
                .search_jobs()
                .farm_id(farm_id)
                .set_queue_ids(Some(queue_ids.iter().map(|s| s.to_string()).collect()))
                .item_offset(item_offset)
                .page_size(page_size);

            if let Some(ref f) = filter {
                req = req.filter_expressions(f.clone());
            }

            if let Some(ref sorts) = sort {
                for s in sorts {
                    req = req.sort_expressions(s.clone());
                }
            } else {
                req = req.sort_expressions(
                    aws_sdk_deadline::types::SearchSortExpression::FieldSort(
                        aws_sdk_deadline::types::FieldSortExpression::builder()
                            .name("CREATED_AT")
                            .sort_order(aws_sdk_deadline::types::SortOrder::Descending)
                            .build()
                            .unwrap(),
                    ),
                );
            }

            req.customize()
                .interceptor(cap)
                .send()
                .await
                .map(|_| ())
        }).await
    }).await
}

/// Retrieve all jobs matching a filter expression, paginating via `createdAt`
/// thresholding. Ports Python's `_list_jobs_by_filter_expression` algorithm.
///
/// The SearchJobs API returns at most 100 results per call. This function
/// pages through all matching jobs by sorting ascending on `CREATED_AT` and
/// using the last page's max `createdAt` as a `GREATER_THAN_EQUAL_TO` filter
/// for the next page. Jobs are deduped by `jobId`.
pub async fn list_jobs_by_filter_expression(
    farm_id: &str,
    queue_id: &str,
    filter_expression: &Value,
    config: Option<&IniConfig>,
) -> Result<Vec<Value>, DeadlineError> {
    let sort = serde_json::json!([{
        "fieldSort": {"name": "CREATED_AT", "sortOrder": "ASCENDING"}
    }]);

    let provided_filter = serde_json::json!({
        "groupFilter": filter_expression,
    });

    // First call: no timestamp threshold
    let mut query_filters = serde_json::json!({
        "filters": [provided_filter.clone()],
        "operator": "AND"
    });

    let mut result_jobs: std::collections::HashMap<String, Value> = std::collections::HashMap::new();

    loop {
        let resp = search_jobs_with_filters(
            farm_id, &[queue_id], 0, 100,
            Some(&query_filters), Some(&sort), config, None,
        ).await?;

        let jobs = resp["jobs"].as_array().cloned().unwrap_or_default();
        let total_results = resp["totalResults"].as_u64().unwrap_or(0) as usize;

        for job in &jobs {
            if let Some(id) = job["jobId"].as_str() {
                result_jobs.insert(id.to_string(), job.clone());
            }
        }

        if jobs.len() >= total_results {
            break;
        }

        // Edge case: all jobs on this page have identical createdAt
        let first_ts = jobs.first().and_then(|j| j["createdAt"].as_str());
        let last_ts = jobs.last().and_then(|j| j["createdAt"].as_str());
        if first_ts == last_ts {
            return Err(DeadlineError::OperationError(
                "Failure fetching jobs based on the createdAt field as more than 100 jobs \
                 have the exact same timestamp value.".into()
            ));
        }

        // Threshold: use the last job's createdAt for the next page.
        // ResponseBodyCapture converts datetimes to display format (space separator),
        // but the SearchJobs API needs RFC 3339 (T separator).
        let threshold = last_ts.unwrap_or_default().replacen(' ', "T", 1);
        query_filters = serde_json::json!({
            "filters": [
                provided_filter.clone(),
                {
                    "dateTimeFilter": {
                        "name": "CREATED_AT",
                        "dateTime": threshold,
                        "operator": "GREATER_THAN_EQUAL_TO"
                    }
                }
            ],
            "operator": "AND"
        });
    }

    Ok(result_jobs.into_values().collect())
}

/// Build SDK SearchGroupedFilterExpressions from JSON.
fn build_filter_expressions(json: &Value) -> Result<aws_sdk_deadline::types::SearchGroupedFilterExpressions, DeadlineError> {
    use aws_sdk_deadline::types::*;

    let operator = match json["operator"].as_str().unwrap_or("AND") {
        "OR" => LogicalOperator::Or,
        _ => LogicalOperator::And,
    };

    let filters = json["filters"].as_array()
        .ok_or_else(|| DeadlineError::OperationError("filterExpressions.filters must be an array".into()))?;

    let mut sdk_filters = Vec::new();
    for f in filters {
        if let Some(stf) = f.get("searchTermFilter") {
            let term = stf["searchTerm"].as_str().unwrap_or("").to_string();
            let match_type = SearchTermMatchingType::from(
                stf["matchType"].as_str().unwrap_or("CONTAINS")
            );
            sdk_filters.push(SearchFilterExpression::SearchTermFilter(
                SearchTermFilterExpression::builder()
                    .search_term(term)
                    .match_type(match_type)
                    .build()
                    .map_err(|e| DeadlineError::OperationError(format!("Invalid filter: {e}")))?,
            ));
        } else if let Some(sf) = f.get("stringFilter") {
            let name = sf["name"].as_str().unwrap_or("").to_string();
            let value = sf["value"].as_str().unwrap_or("").to_string();
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
            let name = slf["name"].as_str().unwrap_or("").to_string();
            let values: Vec<String> = slf["values"].as_array()
                .map(|arr| arr.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect())
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
                builder.build()
                    .map_err(|e| DeadlineError::OperationError(format!("Invalid filter: {e}")))?,
            ));
        } else if let Some(dtf) = f.get("dateTimeFilter") {
            let name = dtf["name"].as_str().unwrap_or("").to_string();
            let datetime_str = dtf["dateTime"].as_str().unwrap_or("");
            let datetime = ::aws_smithy_types::DateTime::from_str(
                datetime_str, ::aws_smithy_types::date_time::Format::DateTimeWithOffset,
            ).map_err(|e| DeadlineError::OperationError(format!("Invalid datetime: {e}")))?;
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

/// Build SDK SearchSortExpression list from JSON.
fn build_sort_expressions(json: &Value) -> Result<Vec<aws_sdk_deadline::types::SearchSortExpression>, DeadlineError> {
    use aws_sdk_deadline::types::*;

    let arr = json.as_array()
        .ok_or_else(|| DeadlineError::OperationError("sortExpressions must be an array".into()))?;

    let mut result = Vec::new();
    for item in arr {
        if let Some(fs) = item.get("fieldSort") {
            let name = fs["name"].as_str().unwrap_or("CREATED_AT").to_string();
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

pub async fn get_job(farm_id: &str, queue_id: &str, job_id: &str, config: Option<&IniConfig>, telemetry: Option<&TelemetryClient>) -> Result<Value, DeadlineError> {
    with_telemetry_latency_async("get_job", config, telemetry, || async {
        let client = session::deadline_client(config).await;
        capture_send(|cap| async move {
            client.get_job().farm_id(farm_id).queue_id(queue_id).job_id(job_id)
                .customize().interceptor(cap).send().await.map(|_| ())
        }).await
    }).await
}

pub async fn get_step(farm_id: &str, queue_id: &str, job_id: &str, step_id: &str, config: Option<&IniConfig>, telemetry: Option<&TelemetryClient>) -> Result<Value, DeadlineError> {
    with_telemetry_latency_async("get_step", config, telemetry, || async {
        let client = session::deadline_client(config).await;
        capture_send(|cap| async move {
            client.get_step().farm_id(farm_id).queue_id(queue_id).job_id(job_id).step_id(step_id)
                .customize().interceptor(cap).send().await.map(|_| ())
        }).await
    }).await
}

pub async fn get_task(farm_id: &str, queue_id: &str, job_id: &str, step_id: &str, task_id: &str, config: Option<&IniConfig>, telemetry: Option<&TelemetryClient>) -> Result<Value, DeadlineError> {
    with_telemetry_latency_async("get_task", config, telemetry, || async {
        let client = session::deadline_client(config).await;
        capture_send(|cap| async move {
            client.get_task().farm_id(farm_id).queue_id(queue_id).job_id(job_id).step_id(step_id).task_id(task_id)
                .customize().interceptor(cap).send().await.map(|_| ())
        }).await
    }).await
}

// ---------------------------------------------------------------------------
// Worker
// ---------------------------------------------------------------------------

pub async fn search_workers(
    farm_id: &str,
    fleet_ids: &[&str],
    item_offset: i32,
    page_size: i32,
    config: Option<&IniConfig>,
    telemetry: Option<&TelemetryClient>,
) -> Result<Value, DeadlineError> {
    with_telemetry_latency_async("search_workers", config, telemetry, || async {
        let client = session::deadline_client(config).await;
        capture_send(|cap| async move {
            client
                .search_workers()
                .farm_id(farm_id)
                .set_fleet_ids(Some(fleet_ids.iter().map(|s| s.to_string()).collect()))
                .item_offset(item_offset)
                .page_size(page_size)
                .customize()
                .interceptor(cap)
                .send()
                .await
                .map(|_| ())
        }).await
    }).await
}

pub async fn get_worker(
    farm_id: &str,
    fleet_id: &str,
    worker_id: &str,
    config: Option<&IniConfig>,
    telemetry: Option<&TelemetryClient>,
) -> Result<Value, DeadlineError> {
    with_telemetry_latency_async("get_worker", config, telemetry, || async {
        let client = session::deadline_client(config).await;
        capture_send(|cap| async move {
            client.get_worker().farm_id(farm_id).fleet_id(fleet_id).worker_id(worker_id)
                .customize().interceptor(cap).send().await.map(|_| ())
        }).await
    }).await
}

// ---------------------------------------------------------------------------
// Session / Step / Task
// ---------------------------------------------------------------------------

pub async fn get_session(
    farm_id: &str,
    queue_id: &str,
    job_id: &str,
    session_id: &str,
    config: Option<&IniConfig>,
    telemetry: Option<&TelemetryClient>,
) -> Result<Value, DeadlineError> {
    with_telemetry_latency_async("get_session", config, telemetry, || async {
        let client = session::deadline_client(config).await;
        capture_send(|cap| async move {
            client.get_session().farm_id(farm_id).queue_id(queue_id).job_id(job_id).session_id(session_id)
                .customize().interceptor(cap).send().await.map(|_| ())
        }).await
    }).await
}

pub async fn list_sessions(
    farm_id: &str,
    queue_id: &str,
    job_id: &str,
    config: Option<&IniConfig>,
    telemetry: Option<&TelemetryClient>,
) -> Result<Value, DeadlineError> {
    with_telemetry_latency_async("list_sessions", config, telemetry, || async {
        let client = session::deadline_client(config).await;
        let farm_id = farm_id.to_string();
        let queue_id = queue_id.to_string();
        let job_id = job_id.to_string();
        paginated_list("sessions", |token| {
            let client = client.clone();
            let farm_id = farm_id.clone();
            let queue_id = queue_id.clone();
            let job_id = job_id.clone();
            async move {
                capture_send(|cap| {
                    let mut req = client.list_sessions().farm_id(&farm_id).queue_id(&queue_id).job_id(&job_id);
                    if let Some(t) = token { req = req.next_token(t); }
                    async move { req.customize().interceptor(cap).send().await.map(|_| ()) }
                }).await
            }
        }).await
    }).await
}

pub async fn list_steps(
    farm_id: &str,
    queue_id: &str,
    job_id: &str,
    config: Option<&IniConfig>,
    telemetry: Option<&TelemetryClient>,
) -> Result<Value, DeadlineError> {
    with_telemetry_latency_async("list_steps", config, telemetry, || async {
        let client = session::deadline_client(config).await;
        let farm_id = farm_id.to_string();
        let queue_id = queue_id.to_string();
        let job_id = job_id.to_string();
        paginated_list("steps", |token| {
            let client = client.clone();
            let farm_id = farm_id.clone();
            let queue_id = queue_id.clone();
            let job_id = job_id.clone();
            async move {
                capture_send(|cap| {
                    let mut req = client.list_steps().farm_id(&farm_id).queue_id(&queue_id).job_id(&job_id);
                    if let Some(t) = token { req = req.next_token(t); }
                    async move { req.customize().interceptor(cap).send().await.map(|_| ()) }
                }).await
            }
        }).await
    }).await
}

pub async fn list_tasks(
    farm_id: &str,
    queue_id: &str,
    job_id: &str,
    step_id: &str,
    config: Option<&IniConfig>,
    telemetry: Option<&TelemetryClient>,
) -> Result<Value, DeadlineError> {
    with_telemetry_latency_async("list_tasks", config, telemetry, || async {
        let client = session::deadline_client(config).await;
        let farm_id = farm_id.to_string();
        let queue_id = queue_id.to_string();
        let job_id = job_id.to_string();
        let step_id = step_id.to_string();
        paginated_list("tasks", |token| {
            let client = client.clone();
            let farm_id = farm_id.clone();
            let queue_id = queue_id.clone();
            let job_id = job_id.clone();
            let step_id = step_id.clone();
            async move {
                capture_send(|cap| {
                    let mut req = client.list_tasks().farm_id(&farm_id).queue_id(&queue_id).job_id(&job_id).step_id(&step_id);
                    if let Some(t) = token { req = req.next_token(t); }
                    async move { req.customize().interceptor(cap).send().await.map(|_| ()) }
                }).await
            }
        }).await
    }).await
}

// ---------------------------------------------------------------------------
// Queue credentials
// ---------------------------------------------------------------------------

pub async fn assume_queue_role_for_user(
    farm_id: &str,
    queue_id: &str,
    config: Option<&IniConfig>,
    telemetry: Option<&TelemetryClient>,
) -> Result<Value, DeadlineError> {
    with_telemetry_latency_async("assume_queue_role_for_user", config, telemetry, || async {
        let client = session::deadline_client(config).await;
        capture_send(|cap| async move {
            client.assume_queue_role_for_user().farm_id(farm_id).queue_id(queue_id)
                .customize().interceptor(cap).send().await.map(|_| ())
        }).await
    }).await
}

pub async fn assume_queue_role_for_read(
    farm_id: &str,
    queue_id: &str,
    config: Option<&IniConfig>,
    telemetry: Option<&TelemetryClient>,
) -> Result<Value, DeadlineError> {
    with_telemetry_latency_async("assume_queue_role_for_read", config, telemetry, || async {
        let client = session::deadline_client(config).await;
        capture_send(|cap| async move {
            client.assume_queue_role_for_read().farm_id(farm_id).queue_id(queue_id)
                .customize().interceptor(cap).send().await.map(|_| ())
        }).await
    }).await
}

// ---------------------------------------------------------------------------
// Storage profile
// ---------------------------------------------------------------------------

pub async fn get_storage_profile_for_queue(
    farm_id: &str,
    queue_id: &str,
    storage_profile_id: &str,
    config: Option<&IniConfig>,
    telemetry: Option<&TelemetryClient>,
) -> Result<Value, DeadlineError> {
    with_telemetry_latency_async("get_storage_profile_for_queue", config, telemetry, || async {
        let client = session::deadline_client(config).await;
        capture_send(|cap| async move {
            client.get_storage_profile_for_queue()
                .farm_id(farm_id).queue_id(queue_id).storage_profile_id(storage_profile_id)
                .customize().interceptor(cap).send().await.map(|_| ())
        }).await
    }).await
}

pub async fn list_storage_profiles_for_queue(
    farm_id: &str,
    queue_id: &str,
    config: Option<&IniConfig>,
    telemetry: Option<&TelemetryClient>,
) -> Result<Value, DeadlineError> {
    with_telemetry_latency_async("list_storage_profiles_for_queue", config, telemetry, || async {
        let client = session::deadline_client(config).await;
        let farm_id = farm_id.to_string();
        let queue_id = queue_id.to_string();
        paginated_list("storageProfiles", |token| {
            let client = client.clone();
            let farm_id = farm_id.clone();
            let queue_id = queue_id.clone();
            async move {
                capture_send(|cap| {
                    let mut req = client.list_storage_profiles_for_queue()
                        .farm_id(&farm_id).queue_id(&queue_id);
                    if let Some(t) = token { req = req.next_token(t); }
                    async move { req.customize().interceptor(cap).send().await.map(|_| ()) }
                }).await
            }
        }).await
    }).await
}

// ---------------------------------------------------------------------------
// Fleet credentials
// ---------------------------------------------------------------------------

pub async fn assume_fleet_role_for_read(
    farm_id: &str,
    fleet_id: &str,
    config: Option<&IniConfig>,
    telemetry: Option<&TelemetryClient>,
) -> Result<Value, DeadlineError> {
    with_telemetry_latency_async("assume_fleet_role_for_read", config, telemetry, || async {
        let client = session::deadline_client(config).await;
        capture_send(|cap| async move {
            client.assume_fleet_role_for_read().farm_id(farm_id).fleet_id(fleet_id)
                .customize().interceptor(cap).send().await.map(|_| ())
        }).await
    }).await
}

// ---------------------------------------------------------------------------
// Session actions
// ---------------------------------------------------------------------------

pub async fn list_session_actions(
    farm_id: &str,
    queue_id: &str,
    job_id: &str,
    session_id: &str,
    config: Option<&IniConfig>,
    telemetry: Option<&TelemetryClient>,
) -> Result<Value, DeadlineError> {
    with_telemetry_latency_async("list_session_actions", config, telemetry, || async {
        let client = session::deadline_client(config).await;
        let farm_id = farm_id.to_string();
        let queue_id = queue_id.to_string();
        let job_id = job_id.to_string();
        let session_id = session_id.to_string();
        paginated_list("sessionActions", |token| {
            let client = client.clone();
            let farm_id = farm_id.clone();
            let queue_id = queue_id.clone();
            let job_id = job_id.clone();
            let session_id = session_id.clone();
            async move {
                capture_send(|cap| {
                    let mut req = client.list_session_actions()
                        .farm_id(&farm_id).queue_id(&queue_id).job_id(&job_id).session_id(&session_id);
                    if let Some(t) = token { req = req.next_token(t); }
                    async move { req.customize().interceptor(cap).send().await.map(|_| ()) }
                }).await
            }
        }).await
    }).await
}

pub async fn get_session_action(
    farm_id: &str,
    queue_id: &str,
    job_id: &str,
    session_action_id: &str,
    config: Option<&IniConfig>,
    telemetry: Option<&TelemetryClient>,
) -> Result<Value, DeadlineError> {
    with_telemetry_latency_async("get_session_action", config, telemetry, || async {
        let client = session::deadline_client(config).await;
        capture_send(|cap| async move {
            client.get_session_action()
                .farm_id(farm_id).queue_id(queue_id).job_id(job_id).session_action_id(session_action_id)
                .customize().interceptor(cap).send().await.map(|_| ())
        }).await
    }).await
}

pub async fn list_queue_environments(
    farm_id: &str,
    queue_id: &str,
    config: Option<&IniConfig>,
    telemetry: Option<&TelemetryClient>,
) -> Result<Value, DeadlineError> {
    with_telemetry_latency_async("list_queue_environments", config, telemetry, || async {
        let client = session::deadline_client(config).await;
        paginated_list("environments", |token| {
            let client = client.clone();
            async move {
                capture_send(|cap| {
                    let mut req = client.list_queue_environments()
                        .farm_id(farm_id).queue_id(queue_id);
                    if let Some(t) = token { req = req.next_token(t); }
                    async move { req.customize().interceptor(cap).send().await.map(|_| ()) }
                }).await
            }
        }).await
    }).await
}

pub async fn get_queue_environment(
    farm_id: &str,
    queue_id: &str,
    queue_environment_id: &str,
    config: Option<&IniConfig>,
    telemetry: Option<&TelemetryClient>,
) -> Result<Value, DeadlineError> {
    with_telemetry_latency_async("get_queue_environment", config, telemetry, || async {
        let client = session::deadline_client(config).await;
        capture_send(|cap| async move {
            client.get_queue_environment()
                .farm_id(farm_id).queue_id(queue_id).queue_environment_id(queue_environment_id)
                .customize().interceptor(cap).send().await.map(|_| ())
        }).await
    }).await
}

// ---------------------------------------------------------------------------
// Queue-Fleet Associations
// ---------------------------------------------------------------------------

pub async fn list_queue_fleet_associations(
    farm_id: &str,
    queue_id: &str,
    config: Option<&IniConfig>,
    telemetry: Option<&TelemetryClient>,
) -> Result<Value, DeadlineError> {
    with_telemetry_latency_async("list_queue_fleet_associations", config, telemetry, || async {
        let client = session::deadline_client(config).await;
        let farm_id = farm_id.to_string();
        let queue_id = queue_id.to_string();
        paginated_list("queueFleetAssociations", |token| {
            let client = client.clone();
            let farm_id = farm_id.clone();
            let queue_id = queue_id.clone();
            async move {
                capture_send(|cap| {
                    let mut req = client.list_queue_fleet_associations()
                        .farm_id(&farm_id).queue_id(&queue_id);
                    if let Some(t) = token { req = req.next_token(t); }
                    async move { req.customize().interceptor(cap).send().await.map(|_| ()) }
                }).await
            }
        }).await
    }).await
}

pub async fn update_job(
    farm_id: &str,
    queue_id: &str,
    job_id: &str,
    target_task_run_status: &str,
    config: Option<&IniConfig>,
    telemetry: Option<&TelemetryClient>,
) -> Result<Value, DeadlineError> {
    with_telemetry_latency_async("update_job", config, telemetry, || async {
        let client = session::deadline_client(config).await;
        let status: aws_sdk_deadline::types::JobTargetTaskRunStatus = target_task_run_status.into();
        capture_send(|cap| async move {
            client.update_job()
                .farm_id(farm_id).queue_id(queue_id).job_id(job_id)
                .target_task_run_status(status)
                .customize().interceptor(cap).send().await.map(|_| ())
        }).await
    }).await
}

pub async fn update_task(
    farm_id: &str,
    queue_id: &str,
    job_id: &str,
    step_id: &str,
    task_id: &str,
    target_run_status: &str,
    config: Option<&IniConfig>,
    telemetry: Option<&TelemetryClient>,
    retry_config: Option<aws_config::retry::RetryConfig>,
) -> Result<Value, DeadlineError> {
    with_telemetry_latency_async("update_task", config, telemetry, || async {
        let client = session::deadline_client(config).await;
        let status: aws_sdk_deadline::types::TaskTargetRunStatus = target_run_status.into();
        capture_send(|cap| async move {
            let mut req = client.update_task()
                .farm_id(farm_id).queue_id(queue_id).job_id(job_id)
                .step_id(step_id).task_id(task_id)
                .target_run_status(status)
                .customize().interceptor(cap);
            if let Some(rc) = retry_config {
                req = req.config_override(aws_sdk_deadline::config::Builder::default().retry_config(rc));
            }
            req.send().await.map(|_| ())
        }).await
    }).await
}

// ---------------------------------------------------------------------------
// Job creation
// ---------------------------------------------------------------------------

/// Convert our JSON attachments into the SDK's typed Attachments struct.
fn build_sdk_attachments(att: &Value) -> Result<aws_sdk_deadline::types::Attachments, DeadlineError> {
    let fs_str = att.get("fileSystem").and_then(|v| v.as_str()).unwrap_or("COPIED");
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
            if let Some(dirs) = m.get("outputRelativeDirectories").and_then(|v| v.as_array()) {
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

    builder.build().map_err(|e| {
        DeadlineError::OperationError(format!("Failed to build attachments: {e}"))
    })
}

/// Call CreateJob API. The `args` map should contain farmId, queueId,
/// template, templateType, priority, and optionally parameters, attachments,
/// storageProfileId, maxFailedTasksCount, maxRetriesPerTask, maxWorkerCount,
/// targetTaskRunStatus.
pub async fn create_job(
    args: &serde_json::Map<String, Value>,
    config: Option<&IniConfig>,
    telemetry: Option<&TelemetryClient>,
) -> Result<Value, DeadlineError> {
    with_telemetry_latency_async("create_job", config, telemetry, || async {
        let client = session::deadline_client(config).await;

        let farm_id = args.get("farmId").and_then(|v| v.as_str()).unwrap_or("");
        let queue_id = args.get("queueId").and_then(|v| v.as_str()).unwrap_or("");
        let template = args.get("template").and_then(|v| v.as_str()).unwrap_or("");
        let template_type: aws_sdk_deadline::types::JobTemplateType = args
            .get("templateType")
            .and_then(|v| v.as_str())
            .unwrap_or("YAML")
            .into();
        let priority = args.get("priority").and_then(|v| v.as_i64()).unwrap_or(50) as i32;

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
        if let Some(v) = args.get("maxFailedTasksCount").and_then(|v| v.as_i64()) {
            req = req.max_failed_tasks_count(v as i32);
        }
        if let Some(v) = args.get("maxRetriesPerTask").and_then(|v| v.as_i64()) {
            req = req.max_retries_per_task(v as i32);
        }
        if let Some(v) = args.get("maxWorkerCount").and_then(|v| v.as_i64()) {
            req = req.max_worker_count(v as i32);
        }
        if let Some(v) = args.get("targetTaskRunStatus").and_then(|v| v.as_str()) {
            let status: aws_sdk_deadline::types::CreateJobTargetTaskRunStatus = v.into();
            req = req.target_task_run_status(status);
        }
        if let Some(params) = args.get("parameters").and_then(|v| v.as_object()) {
            for (name, value) in params {
                let param = if let Some(s) = value.get("string").and_then(|v| v.as_str()) {
                    aws_sdk_deadline::types::JobParameter::String(s.to_string())
                } else if let Some(s) = value.get("int").and_then(|v| v.as_str()) {
                    aws_sdk_deadline::types::JobParameter::Int(s.to_string())
                } else if let Some(s) = value.get("float").and_then(|v| v.as_str()) {
                    aws_sdk_deadline::types::JobParameter::Float(s.to_string())
                } else if let Some(s) = value.get("path").and_then(|v| v.as_str()) {
                    aws_sdk_deadline::types::JobParameter::Path(s.to_string())
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

        capture_send(|cap| async move {
            req.customize().interceptor(cap).send().await.map(|_| ())
        })
        .await
    })
    .await
}

/// Poll GetJob until the job exits CREATE_IN_PROGRESS.
/// Returns (success, lifecycle_status_message).
pub async fn wait_for_create_job_to_complete(
    farm_id: &str,
    queue_id: &str,
    job_id: &str,
    config: Option<&IniConfig>,
    continue_callback: impl Fn() -> bool,
) -> Result<(bool, String), DeadlineError> {
    let initial_delay = std::time::Duration::from_millis(300);
    let max_delay = std::time::Duration::from_secs(5);
    let timeout = std::time::Duration::from_secs(300);

    let start = std::time::Instant::now();
    let mut delay = initial_delay;

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

        let job = get_job(farm_id, queue_id, job_id, config, None).await?;

        let status = job
            .get("lifecycleStatus")
            .or_else(|| job.get("state"))
            .and_then(|v| v.as_str())
            .unwrap_or("");

        let message = job
            .get("lifecycleStatusMessage")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

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
            std::env::set_var("AWS_SECRET_ACCESS_KEY", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
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
            "farm-abc", "queue-abc", "job-abc", None, || false,
        ).await;

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
