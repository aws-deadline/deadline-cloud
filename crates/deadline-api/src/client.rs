//! Client helpers for invoking the Deadline Cloud SDK.
//!
//! Callers use the SDK fluent builder directly. These helpers handle:
//! - Pagination aggregation (`collect_paginated`, `collect_paginated_raw`)
//! - Error mapping (`format_sdk_error`, `deadline_error`)
//! - DCM principal injection (`apply_dcm_principal`)
//!
//! Telemetry is handled by `TelemetryInterceptor` installed on the client
//! at construction time in `session.rs` — not per-call.

use crate::errors::DeadlineError;
use aws_sdk_deadline::error::SdkError;
use aws_smithy_runtime_api::client::orchestrator::HttpResponse;
use aws_smithy_types::error::metadata::ProvideErrorMetadata;
use serde_json::Value;
use std::future::Future;

/// Format any AWS SDK error to include the error code and message.
pub fn format_sdk_error<E, R>(err: &SdkError<E, R>) -> String
where
    E: std::fmt::Display + ProvideErrorMetadata + std::error::Error + 'static,
    R: std::fmt::Debug,
{
    match err {
        SdkError::ServiceError(e) => {
            let inner = e.err();
            let code = inner.code().unwrap_or("Unknown");
            let msg = inner.message().unwrap_or("No message");
            format!("{code}: {msg}")
        }
        other => format!("{}", aws_smithy_types::error::display::DisplayErrorContext(other)),
    }
}

/// Map `SdkError` → `DeadlineError::OperationError`.
pub fn deadline_error<E: std::fmt::Display + ProvideErrorMetadata + std::error::Error + 'static>(
    e: SdkError<E>,
) -> DeadlineError {
    DeadlineError::OperationError(format_sdk_error(&e))
}

/// Drain a native SDK paginator into Vec of page outputs.
pub async fn collect_paginated<O, E>(
    mut stream: aws_smithy_async::future::pagination_stream::PaginationStream<
        Result<O, SdkError<E, HttpResponse>>,
    >,
) -> Result<Vec<O>, DeadlineError>
where
    E: ProvideErrorMetadata + std::error::Error + 'static,
{
    let mut pages = Vec::new();
    while let Some(page) = stream.next().await {
        pages.push(page.map_err(deadline_error)?);
    }
    Ok(pages)
}

/// Raw-paginated drain. Loops on `nextToken`, aggregates items under `items_key`.
pub async fn collect_paginated_raw<F, Fut>(
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
    Ok(serde_json::json!({ items_key: all_items }))
}

/// Trait for fluent builders that accept a `principal_id` filter.
pub trait WithPrincipalId {
    fn principal_id(self, id: impl Into<String>) -> Self;
}

/// Apply the DCM user's principal_id to a list builder if the user is DCM.
pub fn apply_dcm_principal<B: WithPrincipalId>(
    builder: B,
    config: Option<&deadline_config::ini::IniConfig>,
) -> B {
    let (user_id, _) = crate::auth::get_user_and_identity_store_id(config);
    match user_id {
        Some(id) => builder.principal_id(id),
        None => builder,
    }
}

/// Convert PascalCase SDK operation name to snake_case for telemetry.
pub fn pascal_to_snake(s: &str) -> String {
    let mut result = String::with_capacity(s.len() + 4);
    for (i, c) in s.chars().enumerate() {
        if c.is_uppercase() {
            if i > 0 {
                result.push('_');
            }
            result.push(c.to_ascii_lowercase());
        } else {
            result.push(c);
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use serial_test::serial;
    use wiremock::matchers::{method, path_regex};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    async fn setup_env(server: &MockServer) {
        unsafe {
            std::env::set_var("AWS_ENDPOINT_URL_DEADLINE", format!("http://localhost:{}", server.address().port()));
            std::env::set_var("AWS_ACCESS_KEY_ID", "AKIAIOSFODNN7EXAMPLE");
            std::env::set_var("AWS_SECRET_ACCESS_KEY", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
            std::env::set_var("AWS_DEFAULT_REGION", "us-west-2");
            std::env::set_var("AWS_CONFIG_FILE", "/dev/null");
        }
    }

    // --- pascal_to_snake ---

    #[test]
    fn pascal_to_snake_converts_standard_sdk_op_names() {
        assert_eq!(pascal_to_snake("GetFarm"), "get_farm");
        assert_eq!(pascal_to_snake("ListFarms"), "list_farms");
        assert_eq!(pascal_to_snake("CreateJob"), "create_job");
        assert_eq!(pascal_to_snake("UpdateJob"), "update_job");
        assert_eq!(pascal_to_snake("SearchJobs"), "search_jobs");
        assert_eq!(pascal_to_snake("BatchGetStep"), "batch_get_step");
        assert_eq!(pascal_to_snake("ListQueueFleetAssociations"), "list_queue_fleet_associations");
        assert_eq!(pascal_to_snake("AssumeQueueRoleForUser"), "assume_queue_role_for_user");
        assert_eq!(pascal_to_snake("GetStorageProfileForQueue"), "get_storage_profile_for_queue");
        assert_eq!(pascal_to_snake("ListSessionActions"), "list_session_actions");
    }

    #[test]
    fn pascal_to_snake_handles_edge_cases() {
        assert_eq!(pascal_to_snake(""), "");
        assert_eq!(pascal_to_snake("A"), "a");
        assert_eq!(pascal_to_snake("AB"), "a_b");
    }

    // --- collect_paginated_raw ---

    #[tokio::test]
    async fn collect_paginated_raw_aggregates_items_under_key() {
        let page1 = json!({"farms": [{"farmId": "farm-aaa"}, {"farmId": "farm-bbb"}], "nextToken": "tok2"});
        let page2 = json!({"farms": [{"farmId": "farm-ccc"}]});
        let pages = vec![page1, page2];
        let idx = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let result = collect_paginated_raw("farms", |token| {
            let pages = pages.clone();
            let idx = idx.clone();
            async move {
                let i = idx.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                match i {
                    0 => { assert!(token.is_none()); Ok(pages[0].clone()) }
                    1 => { assert_eq!(token.as_deref(), Some("tok2")); Ok(pages[1].clone()) }
                    _ => panic!("too many calls"),
                }
            }
        }).await.unwrap();

        let farms = result["farms"].as_array().unwrap();
        assert_eq!(farms.len(), 3);
        assert_eq!(farms[0]["farmId"], "farm-aaa");
        assert_eq!(farms[2]["farmId"], "farm-ccc");
    }

    #[tokio::test]
    async fn collect_paginated_raw_single_page_no_loop() {
        let result = collect_paginated_raw("queues", |token| {
            async move {
                assert!(token.is_none());
                Ok(json!({"queues": [{"queueId": "q-1"}]}))
            }
        }).await.unwrap();

        assert_eq!(result["queues"].as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn collect_paginated_raw_propagates_error() {
        let result = collect_paginated_raw("farms", |_| async {
            Err(DeadlineError::OperationError("AccessDeniedException: forbidden".into()))
        }).await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("AccessDeniedException"));
    }

    #[tokio::test]
    async fn collect_paginated_raw_empty_items() {
        let result = collect_paginated_raw("jobs", |_| async {
            Ok(json!({"jobs": []}))
        }).await.unwrap();

        assert_eq!(result["jobs"].as_array().unwrap().len(), 0);
    }

    // --- collect_paginated (typed, real SDK paginator against wiremock) ---

    #[tokio::test]
    #[serial]
    async fn collect_paginated_drains_all_pages_via_native_paginator() {
        let server = MockServer::start().await;
        setup_env(&server).await;

        Mock::given(method("GET")).and(path_regex(".*/farms$"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "farms": [
                    {"farmId": "farm-aaa", "displayName": "A", "createdAt": "2024-01-01T00:00:00Z", "createdBy": "u", "updatedAt": "2024-01-01T00:00:00Z", "updatedBy": "u"},
                    {"farmId": "farm-bbb", "displayName": "B", "createdAt": "2024-01-01T00:00:00Z", "createdBy": "u", "updatedAt": "2024-01-01T00:00:00Z", "updatedBy": "u"},
                ],
                "nextToken": "p2"
            })))
            .up_to_n_times(1).mount(&server).await;

        Mock::given(method("GET")).and(path_regex(".*/farms$"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "farms": [
                    {"farmId": "farm-ccc", "displayName": "C", "createdAt": "2024-01-01T00:00:00Z", "createdBy": "u", "updatedAt": "2024-01-01T00:00:00Z", "updatedBy": "u"},
                ]
            })))
            .mount(&server).await;

        let sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .endpoint_url(format!("http://localhost:{}", server.address().port()))
            .load().await;
        let client = aws_sdk_deadline::Client::new(&sdk_config);

        let pages = collect_paginated(client.list_farms().into_paginator().send()).await.unwrap();
        assert_eq!(pages.len(), 2);
        let total: usize = pages.iter().map(|p| p.farms().len()).sum();
        assert_eq!(total, 3);
        assert_eq!(pages[0].farms()[0].farm_id(), "farm-aaa");
        assert_eq!(pages[1].farms()[0].farm_id(), "farm-ccc");
    }

    #[tokio::test]
    #[serial]
    async fn collect_paginated_maps_sdk_error_from_any_page() {
        let server = MockServer::start().await;
        setup_env(&server).await;

        Mock::given(method("GET")).and(path_regex(".*/farms$"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "farms": [{"farmId": "farm-aaa", "displayName": "A", "createdAt": "2024-01-01T00:00:00Z", "createdBy": "u", "updatedAt": "2024-01-01T00:00:00Z", "updatedBy": "u"}],
                "nextToken": "p2"
            })))
            .up_to_n_times(1).mount(&server).await;

        Mock::given(method("GET")).and(path_regex(".*/farms$"))
            .respond_with(ResponseTemplate::new(403).set_body_json(json!({
                "__type": "AccessDeniedException",
                "message": "User is not authorized"
            })))
            .mount(&server).await;

        let sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .endpoint_url(format!("http://localhost:{}", server.address().port()))
            .load().await;
        let client = aws_sdk_deadline::Client::new(&sdk_config);

        let result = collect_paginated(client.list_farms().into_paginator().send()).await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("AccessDeniedException") || err.contains("403"), "got: {err}");
    }

    // --- apply_dcm_principal ---

    #[test]
    #[serial]
    fn apply_dcm_principal_noop_when_no_config() {
        unsafe { std::env::set_var("AWS_CONFIG_FILE", "/dev/null"); }
        struct Fake { id: Option<String> }
        impl WithPrincipalId for Fake {
            fn principal_id(mut self, id: impl Into<String>) -> Self { self.id = Some(id.into()); self }
        }
        let result = apply_dcm_principal(Fake { id: None }, None);
        assert!(result.id.is_none());
    }

    #[test]
    #[serial]
    fn apply_dcm_principal_noop_when_not_dcm_user() {
        unsafe { std::env::set_var("AWS_CONFIG_FILE", "/dev/null"); }
        struct Fake { id: Option<String> }
        impl WithPrincipalId for Fake {
            fn principal_id(mut self, id: impl Into<String>) -> Self { self.id = Some(id.into()); self }
        }
        let config = deadline_config::ini::IniConfig::new();
        let result = apply_dcm_principal(Fake { id: None }, Some(&config));
        assert!(result.id.is_none());
    }

    // --- sdk_err / format_sdk_error ---

    #[tokio::test]
    #[serial]
    async fn sdk_err_maps_service_error_with_code_and_message() {
        let server = MockServer::start().await;
        setup_env(&server).await;

        Mock::given(method("GET")).and(path_regex(".*/farms/farm-nope"))
            .respond_with(ResponseTemplate::new(404).set_body_json(json!({
                "__type": "ResourceNotFoundException",
                "message": "Farm not found: farm-nope"
            })))
            .mount(&server).await;

        let sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .endpoint_url(format!("http://localhost:{}", server.address().port()))
            .load().await;
        let client = aws_sdk_deadline::Client::new(&sdk_config);

        let err = client.get_farm().farm_id("farm-nope").send().await.unwrap_err();
        let formatted = format_sdk_error(&err);
        assert!(formatted.contains("ResourceNotFoundException"), "got: {formatted}");
        assert!(formatted.contains("Farm not found"), "got: {formatted}");

        let deadline_err = deadline_error(err);
        assert!(matches!(deadline_err, DeadlineError::OperationError(_)));
    }
}
