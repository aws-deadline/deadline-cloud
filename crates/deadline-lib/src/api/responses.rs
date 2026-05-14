//! Response structs for typed SDK output → serializable JSON.
//!
//! Each struct maps 1:1 to a Get API output. Uses `#[serde(rename_all = "camelCase")]`
//! for Python/JSON compatibility. `DateTime` fields are pre-formatted as strings.
//!
//! Complex nested SDK types (`FleetConfiguration`, `JobRunAsUser`, etc.) don't
//! implement Serialize. These are converted to `serde_json::Value` by walking
//! typed SDK accessors via helpers in `type_conversions.rs`.

use crate::api::type_conversions::{
    attachments_to_value, dependency_counts_to_value, fleet_configuration_to_value,
    host_properties_to_value, job_attachment_settings_to_value, job_parameter_to_value,
    job_run_as_user_to_value, log_configuration_to_value, parameter_space_to_value,
    scheduling_configuration_to_value, step_required_capabilities_to_value,
    task_parameter_value_to_value,
};

use aws_sdk_deadline::operation::get_farm::GetFarmOutput;
use aws_sdk_deadline::operation::get_fleet::GetFleetOutput;
use aws_sdk_deadline::operation::get_job::GetJobOutput;
use aws_sdk_deadline::operation::get_queue::GetQueueOutput;
use aws_sdk_deadline::operation::get_session::GetSessionOutput;
use aws_sdk_deadline::operation::get_step::GetStepOutput;
use aws_sdk_deadline::operation::get_task::GetTaskOutput;
use aws_sdk_deadline::operation::get_worker::GetWorkerOutput;
use serde::Serialize;
use serde_json::{Map, Value, json};

/// Format an AWS SDK `DateTime` to match Python's display format.
/// Input: ISO 8601 (e.g. "2024-12-18T00:37:38Z" or "2024-12-18T00:37:38.624Z")
/// Output: "2024-12-18 00:37:38+00:00" or "2024-12-18 00:37:38.624+00:00"
pub fn format_datetime(dt: &aws_smithy_types::DateTime) -> String {
    let s = dt
        .fmt(aws_smithy_types::date_time::Format::DateTimeWithOffset)
        .unwrap_or_default();
    s.replace('T', " ").replace('Z', "+00:00")
}

// ---------------------------------------------------------------------------
// FarmResponse
// ---------------------------------------------------------------------------

/// Response struct for `GetFarm` API output.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FarmResponse {
    pub farm_id: String,
    pub display_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kms_key_arn: Option<String>,
    pub created_at: String,
    pub created_by: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_by: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub cost_scale_factor: f32,
}

impl From<GetFarmOutput> for FarmResponse {
    fn from(o: GetFarmOutput) -> Self {
        Self {
            farm_id: o.farm_id,
            display_name: o.display_name,
            kms_key_arn: o.kms_key_arn,
            created_at: format_datetime(&o.created_at),
            created_by: o.created_by,
            updated_at: o.updated_at.as_ref().map(format_datetime),
            updated_by: o.updated_by,
            description: o.description,
            cost_scale_factor: o.cost_scale_factor,
        }
    }
}

// ---------------------------------------------------------------------------
// QueueResponse
// ---------------------------------------------------------------------------

/// Response struct for `GetQueue` API output.
/// Complex nested fields (jobAttachmentSettings, jobRunAsUser, schedulingConfiguration)
/// are converted from typed SDK output via `type_conversions.rs` helpers.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QueueResponse {
    pub farm_id: String,
    pub queue_id: String,
    pub display_name: String,
    pub status: String,
    pub default_budget_action: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blocked_reason: Option<String>,
    pub created_at: String,
    pub created_by: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_by: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub job_attachment_settings: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role_arn: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub required_file_system_location_names: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allowed_storage_profile_ids: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub job_run_as_user: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scheduling_configuration: Option<Value>,
}

impl From<GetQueueOutput> for QueueResponse {
    fn from(o: GetQueueOutput) -> Self {
        let fslns = o
            .required_file_system_location_names
            .filter(|v| !v.is_empty());
        let aspids = o.allowed_storage_profile_ids.filter(|v| !v.is_empty());
        Self {
            farm_id: o.farm_id,
            queue_id: o.queue_id,
            display_name: o.display_name,
            status: o.status.as_str().to_owned(),
            default_budget_action: o.default_budget_action.as_str().to_owned(),
            blocked_reason: o.blocked_reason.map(|r| r.as_str().to_owned()),
            created_at: format_datetime(&o.created_at),
            created_by: o.created_by,
            updated_at: o.updated_at.as_ref().map(format_datetime),
            updated_by: o.updated_by,
            description: o.description,
            job_attachment_settings: o
                .job_attachment_settings
                .as_ref()
                .map(job_attachment_settings_to_value),
            role_arn: o.role_arn,
            required_file_system_location_names: fslns,
            allowed_storage_profile_ids: aspids,
            job_run_as_user: o.job_run_as_user.as_ref().map(job_run_as_user_to_value),
            scheduling_configuration: o
                .scheduling_configuration
                .as_ref()
                .map(scheduling_configuration_to_value),
        }
    }
}

// ---------------------------------------------------------------------------
// FleetResponse
// ---------------------------------------------------------------------------

/// Response struct for `GetFleet` API output.
/// Complex nested fields (configuration, hostConfiguration, capabilities)
/// are converted from typed SDK output via `type_conversions.rs` helpers.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FleetResponse {
    pub fleet_id: String,
    pub farm_id: String,
    pub display_name: String,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status_message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auto_scaling_status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_worker_count: Option<i32>,
    pub worker_count: i32,
    pub min_worker_count: i32,
    pub max_worker_count: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub configuration: Option<Value>,
    pub created_at: String,
    pub created_by: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_by: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host_configuration: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub capabilities: Option<Value>,
    pub role_arn: String,
}

impl From<GetFleetOutput> for FleetResponse {
    fn from(o: GetFleetOutput) -> Self {
        Self {
            fleet_id: o.fleet_id,
            farm_id: o.farm_id,
            display_name: o.display_name,
            status: o.status.as_str().to_owned(),
            status_message: o.status_message,
            auto_scaling_status: o.auto_scaling_status.map(|s| s.as_str().to_owned()),
            target_worker_count: o.target_worker_count,
            worker_count: o.worker_count,
            min_worker_count: o.min_worker_count,
            max_worker_count: o.max_worker_count,
            configuration: o.configuration.as_ref().and_then(|c| {
                let v = fleet_configuration_to_value(c);
                if v == json!({}) { None } else { Some(v) }
            }),
            created_at: format_datetime(&o.created_at),
            created_by: o.created_by,
            updated_at: o.updated_at.as_ref().map(format_datetime),
            updated_by: o.updated_by,
            description: o.description,
            host_configuration: o.host_configuration.as_ref().map(|h| {
                json!({"scriptBody": h.script_body(), "scriptTimeoutSeconds": h.script_timeout_seconds()})
            }),
            capabilities: o.capabilities.as_ref().map(|c| {
                let mut obj = Map::new();
                if c.amounts.is_some() {
                    let amounts: Vec<Value> = c.amounts().iter().map(|a| {
                        let mut ao = Map::new();
                        ao.insert("name".into(), json!(a.name()));
                        ao.insert("min".into(), json!(a.min()));
                        if let Some(max) = a.max() { ao.insert("max".into(), json!(max)); }
                        Value::Object(ao)
                    }).collect();
                    obj.insert("amounts".into(), json!(amounts));
                }
                if c.attributes.is_some() {
                    let attrs: Vec<Value> = c.attributes().iter().map(|a| {
                        json!({"name": a.name(), "values": a.values()})
                    }).collect();
                    obj.insert("attributes".into(), json!(attrs));
                }
                Value::Object(obj)
            }),
            role_arn: o.role_arn,
        }
    }
}

// ---------------------------------------------------------------------------
// JobResponse
// ---------------------------------------------------------------------------

/// Response struct for `GetJob` API output.
/// Complex nested fields (taskRunStatusCounts, parameters, attachments)
/// are converted from typed SDK output via `type_conversions.rs` helpers.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JobResponse {
    pub job_id: String,
    pub name: String,
    pub lifecycle_status: String,
    pub lifecycle_status_message: String,
    pub priority: i32,
    pub created_at: String,
    pub created_by: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_by: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ended_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_run_status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_task_run_status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_run_status_counts: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_failure_retry_count: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_profile_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_failed_tasks_count: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_retries_per_task: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parameters: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attachments: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_worker_count: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_job_id: Option<String>,
}

impl From<GetJobOutput> for JobResponse {
    fn from(o: GetJobOutput) -> Self {
        let task_run_status_counts = o.task_run_status_counts.as_ref().map(|m| {
            let mut pairs: Vec<_> = m
                .iter()
                .map(|(k, v)| (k.as_str().to_owned(), json!(v)))
                .collect();
            pairs.sort_by(|a, b| a.0.cmp(&b.0));
            let obj: Map<String, Value> = pairs.into_iter().collect();
            Value::Object(obj)
        });
        let parameters = o.parameters.as_ref().map(|m| {
            let mut pairs: Vec<_> = m
                .iter()
                .map(|(k, v)| (k.clone(), job_parameter_to_value(v)))
                .collect();
            pairs.sort_by(|a, b| a.0.cmp(&b.0));
            let obj: Map<String, Value> = pairs.into_iter().collect();
            Value::Object(obj)
        });
        Self {
            job_id: o.job_id,
            name: o.name,
            lifecycle_status: o.lifecycle_status.as_str().to_owned(),
            lifecycle_status_message: o.lifecycle_status_message,
            priority: o.priority,
            created_at: format_datetime(&o.created_at),
            created_by: o.created_by,
            updated_at: o.updated_at.as_ref().map(format_datetime),
            updated_by: o.updated_by,
            started_at: o.started_at.as_ref().map(format_datetime),
            ended_at: o.ended_at.as_ref().map(format_datetime),
            task_run_status: o.task_run_status.map(|s| s.as_str().to_owned()),
            target_task_run_status: o.target_task_run_status.map(|s| s.as_str().to_owned()),
            task_run_status_counts,
            task_failure_retry_count: o.task_failure_retry_count,
            storage_profile_id: o.storage_profile_id,
            max_failed_tasks_count: o.max_failed_tasks_count,
            max_retries_per_task: o.max_retries_per_task,
            parameters,
            attachments: o.attachments.as_ref().map(attachments_to_value),
            description: o.description,
            max_worker_count: o.max_worker_count,
            source_job_id: o.source_job_id,
        }
    }
}

// ---------------------------------------------------------------------------
// StepResponse
// ---------------------------------------------------------------------------

/// Response struct for `GetStep` API output.
/// Complex nested fields (taskRunStatusCounts, dependencyCounts,
/// requiredCapabilities, parameterSpace) are converted from typed SDK output.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StepResponse {
    pub step_id: String,
    pub name: String,
    pub lifecycle_status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lifecycle_status_message: Option<String>,
    pub task_run_status: String,
    pub task_run_status_counts: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_failure_retry_count: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_task_run_status: Option<String>,
    pub created_at: String,
    pub created_by: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_by: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ended_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dependency_counts: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub required_capabilities: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parameter_space: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

impl From<GetStepOutput> for StepResponse {
    fn from(o: GetStepOutput) -> Self {
        let task_run_status_counts = {
            let mut pairs: Vec<_> = o
                .task_run_status_counts
                .iter()
                .map(|(k, v)| (k.as_str().to_owned(), json!(v)))
                .collect();
            pairs.sort_by(|a, b| a.0.cmp(&b.0));
            let obj: Map<String, Value> = pairs.into_iter().collect();
            Value::Object(obj)
        };
        Self {
            step_id: o.step_id,
            name: o.name,
            lifecycle_status: o.lifecycle_status.as_str().to_owned(),
            lifecycle_status_message: o.lifecycle_status_message,
            task_run_status: o.task_run_status.as_str().to_owned(),
            task_run_status_counts,
            task_failure_retry_count: o.task_failure_retry_count,
            target_task_run_status: o.target_task_run_status.map(|s| s.as_str().to_owned()),
            created_at: format_datetime(&o.created_at),
            created_by: o.created_by,
            updated_at: o.updated_at.as_ref().map(format_datetime),
            updated_by: o.updated_by,
            started_at: o.started_at.as_ref().map(format_datetime),
            ended_at: o.ended_at.as_ref().map(format_datetime),
            dependency_counts: o.dependency_counts.as_ref().map(dependency_counts_to_value),
            required_capabilities: o
                .required_capabilities
                .as_ref()
                .map(step_required_capabilities_to_value),
            parameter_space: o.parameter_space.as_ref().map(parameter_space_to_value),
            description: o.description,
        }
    }
}

// ---------------------------------------------------------------------------
// TaskResponse
// ---------------------------------------------------------------------------

/// Response struct for `GetTask` API output.
/// Complex nested field (parameters) is stored as raw JSON.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskResponse {
    pub task_id: String,
    pub created_at: String,
    pub created_by: String,
    pub run_status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_run_status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_retry_count: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ended_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_by: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latest_session_action_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parameters: Option<Value>,
}

impl From<GetTaskOutput> for TaskResponse {
    fn from(o: GetTaskOutput) -> Self {
        let parameters = o.parameters.as_ref().map(|m| {
            let obj: Map<String, Value> = m
                .iter()
                .map(|(k, v)| (k.clone(), task_parameter_value_to_value(v)))
                .collect();
            Value::Object(obj)
        });
        Self {
            task_id: o.task_id,
            created_at: format_datetime(&o.created_at),
            created_by: o.created_by,
            run_status: o.run_status.as_str().to_owned(),
            target_run_status: o.target_run_status.map(|s| s.as_str().to_owned()),
            failure_retry_count: o.failure_retry_count,
            started_at: o.started_at.as_ref().map(format_datetime),
            ended_at: o.ended_at.as_ref().map(format_datetime),
            updated_at: o.updated_at.as_ref().map(format_datetime),
            updated_by: o.updated_by,
            latest_session_action_id: o.latest_session_action_id,
            parameters,
        }
    }
}

// ---------------------------------------------------------------------------
// SessionResponse
// ---------------------------------------------------------------------------

/// Response struct for `GetSession` API output.
/// Complex nested fields (log, hostProperties, workerLog) are stored as raw JSON.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionResponse {
    pub session_id: String,
    pub fleet_id: String,
    pub worker_id: String,
    pub started_at: String,
    pub lifecycle_status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ended_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_lifecycle_status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_by: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub log: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host_properties: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worker_log: Option<Value>,
}

impl From<GetSessionOutput> for SessionResponse {
    fn from(o: GetSessionOutput) -> Self {
        Self {
            session_id: o.session_id,
            fleet_id: o.fleet_id,
            worker_id: o.worker_id,
            started_at: format_datetime(&o.started_at),
            lifecycle_status: o.lifecycle_status.as_str().to_owned(),
            ended_at: o.ended_at.as_ref().map(format_datetime),
            target_lifecycle_status: o.target_lifecycle_status.map(|s| s.as_str().to_owned()),
            updated_at: o.updated_at.as_ref().map(format_datetime),
            updated_by: o.updated_by,
            log: o.log.as_ref().and_then(|l| {
                if l.log_driver().is_empty() {
                    None
                } else {
                    Some(log_configuration_to_value(l))
                }
            }),
            host_properties: o.host_properties.as_ref().map(host_properties_to_value),
            worker_log: o.worker_log.as_ref().and_then(|l| {
                if l.log_driver().is_empty() {
                    None
                } else {
                    Some(log_configuration_to_value(l))
                }
            }),
        }
    }
}

// ---------------------------------------------------------------------------
// WorkerResponse
// ---------------------------------------------------------------------------

/// Response struct for `GetWorker` API output.
/// Complex nested fields (hostProperties, log) are stored as raw JSON.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerResponse {
    pub farm_id: String,
    pub fleet_id: String,
    pub worker_id: String,
    pub status: String,
    pub created_at: String,
    pub created_by: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_by: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host_properties: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub log: Option<Value>,
}

impl From<GetWorkerOutput> for WorkerResponse {
    fn from(o: GetWorkerOutput) -> Self {
        Self {
            farm_id: o.farm_id,
            fleet_id: o.fleet_id,
            worker_id: o.worker_id,
            status: o.status.as_str().to_owned(),
            created_at: format_datetime(&o.created_at),
            created_by: o.created_by,
            updated_at: o.updated_at.as_ref().map(format_datetime),
            updated_by: o.updated_by,
            host_properties: o.host_properties.as_ref().map(host_properties_to_value),
            log: o.log.as_ref().and_then(|l| {
                if l.log_driver().is_empty() {
                    None
                } else {
                    Some(log_configuration_to_value(l))
                }
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_datetime_converts_utc_z_suffix() {
        let dt = aws_smithy_types::DateTime::from_str(
            "2024-12-18T00:37:38Z",
            aws_smithy_types::date_time::Format::DateTimeWithOffset,
        )
        .unwrap();
        assert_eq!(format_datetime(&dt), "2024-12-18 00:37:38+00:00");
    }

    #[test]
    fn format_datetime_converts_utc_offset_suffix() {
        let dt = aws_smithy_types::DateTime::from_str(
            "2024-12-18T00:37:38+00:00",
            aws_smithy_types::date_time::Format::DateTimeWithOffset,
        )
        .unwrap();
        assert_eq!(format_datetime(&dt), "2024-12-18 00:37:38+00:00");
    }

    #[test]
    fn format_datetime_preserves_fractional_seconds() {
        let dt = aws_smithy_types::DateTime::from_str(
            "2024-12-18T00:37:38.624Z",
            aws_smithy_types::date_time::Format::DateTimeWithOffset,
        )
        .unwrap();
        assert_eq!(format_datetime(&dt), "2024-12-18 00:37:38.624+00:00");
    }

    #[test]
    fn format_datetime_handles_midnight() {
        let dt = aws_smithy_types::DateTime::from_str(
            "2024-01-01T00:00:00Z",
            aws_smithy_types::date_time::Format::DateTimeWithOffset,
        )
        .unwrap();
        assert_eq!(format_datetime(&dt), "2024-01-01 00:00:00+00:00");
    }

    #[test]
    fn farm_response_serializes_with_camel_case() {
        let resp = FarmResponse {
            farm_id: "farm-abc".into(),
            display_name: "My Farm".into(),
            kms_key_arn: None,
            created_at: "2024-01-01 00:00:00+00:00".into(),
            created_by: "user".into(),
            updated_at: None,
            updated_by: None,
            description: Some("A farm".into()),
            cost_scale_factor: 1.5,
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["farmId"], "farm-abc");
        assert_eq!(json["displayName"], "My Farm");
        assert_eq!(json["costScaleFactor"], 1.5);
        assert_eq!(json["description"], "A farm");
        assert!(json.get("kmsKeyArn").is_none());
        assert!(json.get("updatedAt").is_none());
    }

    #[test]
    fn farm_response_omits_none_fields() {
        let resp = FarmResponse {
            farm_id: "farm-abc".into(),
            display_name: "Farm".into(),
            kms_key_arn: None,
            created_at: "2024-01-01 00:00:00+00:00".into(),
            created_by: "user".into(),
            updated_at: None,
            updated_by: None,
            description: None,
            cost_scale_factor: 1.0,
        };
        let json_str = serde_json::to_string(&resp).unwrap();
        assert!(!json_str.contains("kmsKeyArn"));
        assert!(!json_str.contains("updatedAt"));
        assert!(!json_str.contains("updatedBy"));
        assert!(!json_str.contains("description"));
    }

    #[test]
    fn queue_response_serializes_nested_objects() {
        let resp = QueueResponse {
            farm_id: "farm-abc".into(),
            queue_id: "queue-aaa".into(),
            display_name: "My Queue".into(),
            status: "ACTIVE".into(),
            default_budget_action: "NONE".into(),
            blocked_reason: None,
            created_at: "2024-01-01 00:00:00+00:00".into(),
            created_by: "user".into(),
            updated_at: None,
            updated_by: None,
            description: None,
            job_attachment_settings: Some(serde_json::json!({
                "s3BucketName": "bucket",
                "rootPrefix": "prefix"
            })),
            role_arn: Some("arn:aws:iam::123:role/R".into()),
            required_file_system_location_names: None,
            allowed_storage_profile_ids: None,
            job_run_as_user: None,
            scheduling_configuration: None,
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["jobAttachmentSettings"]["s3BucketName"], "bucket");
        assert_eq!(json["roleArn"], "arn:aws:iam::123:role/R");
    }

    #[test]
    fn fleet_response_includes_required_counts() {
        let resp = FleetResponse {
            fleet_id: "fleet-aaa".into(),
            farm_id: "farm-abc".into(),
            display_name: "Fleet".into(),
            status: "ACTIVE".into(),
            status_message: None,
            auto_scaling_status: Some("STEADY".into()),
            target_worker_count: Some(5),
            worker_count: 3,
            min_worker_count: 0,
            max_worker_count: 10,
            configuration: None,
            created_at: "2024-01-01 00:00:00+00:00".into(),
            created_by: "user".into(),
            updated_at: None,
            updated_by: None,
            description: None,
            host_configuration: None,
            capabilities: None,
            role_arn: "arn:aws:iam::123:role/R".into(),
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["workerCount"], 3);
        assert_eq!(json["minWorkerCount"], 0);
        assert_eq!(json["maxWorkerCount"], 10);
        assert_eq!(json["autoScalingStatus"], "STEADY");
        assert_eq!(json["targetWorkerCount"], 5);
    }

    // -----------------------------------------------------------------------
    // JobResponse
    // -----------------------------------------------------------------------

    #[test]
    fn job_response_serializes_required_fields() {
        let resp = JobResponse {
            job_id: "job-aaa".into(),
            name: "Render Job".into(),
            lifecycle_status: "CREATE_COMPLETE".into(),
            lifecycle_status_message: "Job created".into(),
            priority: 50,
            created_at: "2024-01-01 00:00:00+00:00".into(),
            created_by: "user".into(),
            updated_at: None,
            updated_by: None,
            started_at: None,
            ended_at: None,
            task_run_status: None,
            target_task_run_status: None,
            task_run_status_counts: None,
            task_failure_retry_count: None,
            storage_profile_id: None,
            max_failed_tasks_count: None,
            max_retries_per_task: None,
            parameters: None,
            attachments: None,
            description: None,
            max_worker_count: None,
            source_job_id: None,
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["jobId"], "job-aaa");
        assert_eq!(json["name"], "Render Job");
        assert_eq!(json["lifecycleStatus"], "CREATE_COMPLETE");
        assert_eq!(json["lifecycleStatusMessage"], "Job created");
        assert_eq!(json["priority"], 50);
        assert_eq!(json["createdAt"], "2024-01-01 00:00:00+00:00");
        assert_eq!(json["createdBy"], "user");
    }

    #[test]
    fn job_response_omits_none_fields() {
        let resp = JobResponse {
            job_id: "job-aaa".into(),
            name: "Job".into(),
            lifecycle_status: "CREATE_COMPLETE".into(),
            lifecycle_status_message: String::new(),
            priority: 50,
            created_at: "2024-01-01 00:00:00+00:00".into(),
            created_by: "user".into(),
            updated_at: None,
            updated_by: None,
            started_at: None,
            ended_at: None,
            task_run_status: None,
            target_task_run_status: None,
            task_run_status_counts: None,
            task_failure_retry_count: None,
            storage_profile_id: None,
            max_failed_tasks_count: None,
            max_retries_per_task: None,
            parameters: None,
            attachments: None,
            description: None,
            max_worker_count: None,
            source_job_id: None,
        };
        let json_str = serde_json::to_string(&resp).unwrap();
        assert!(!json_str.contains("updatedAt"));
        assert!(!json_str.contains("startedAt"));
        assert!(!json_str.contains("endedAt"));
        assert!(!json_str.contains("taskRunStatus"));
        assert!(!json_str.contains("parameters"));
        assert!(!json_str.contains("attachments"));
        assert!(!json_str.contains("description"));
        assert!(!json_str.contains("maxWorkerCount"));
        assert!(!json_str.contains("sourceJobId"));
    }

    #[test]
    fn job_response_includes_nested_raw_json() {
        let resp = JobResponse {
            job_id: "job-aaa".into(),
            name: "Job".into(),
            lifecycle_status: "CREATE_COMPLETE".into(),
            lifecycle_status_message: String::new(),
            priority: 50,
            created_at: "2024-01-01 00:00:00+00:00".into(),
            created_by: "user".into(),
            updated_at: None,
            updated_by: None,
            started_at: Some("2024-01-01 01:00:00+00:00".into()),
            ended_at: None,
            task_run_status: Some("RUNNING".into()),
            target_task_run_status: None,
            task_run_status_counts: Some(serde_json::json!({
                "RUNNING": 2, "SUCCEEDED": 5, "PENDING": 3
            })),
            task_failure_retry_count: Some(1),
            storage_profile_id: None,
            max_failed_tasks_count: Some(10),
            max_retries_per_task: Some(3),
            parameters: Some(serde_json::json!({
                "Frames": {"int": "1-10"}
            })),
            attachments: Some(serde_json::json!({
                "manifests": [{"rootPath": "/tmp"}]
            })),
            description: Some("A render job".into()),
            max_worker_count: Some(5),
            source_job_id: None,
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["taskRunStatusCounts"]["RUNNING"], 2);
        assert_eq!(json["parameters"]["Frames"]["int"], "1-10");
        assert_eq!(json["attachments"]["manifests"][0]["rootPath"], "/tmp");
        assert_eq!(json["taskRunStatus"], "RUNNING");
        assert_eq!(json["maxFailedTasksCount"], 10);
        assert_eq!(json["maxRetriesPerTask"], 3);
        assert_eq!(json["maxWorkerCount"], 5);
    }

    // -----------------------------------------------------------------------
    // StepResponse
    // -----------------------------------------------------------------------

    #[test]
    fn step_response_serializes_with_raw_counts() {
        let resp = StepResponse {
            step_id: "step-aaa".into(),
            name: "Render".into(),
            lifecycle_status: "UPDATE_COMPLETE".into(),
            lifecycle_status_message: None,
            task_run_status: "RUNNING".into(),
            task_run_status_counts: serde_json::json!({"RUNNING": 3, "PENDING": 7}),
            task_failure_retry_count: None,
            target_task_run_status: None,
            created_at: "2024-01-01 00:00:00+00:00".into(),
            created_by: "user".into(),
            updated_at: None,
            updated_by: None,
            started_at: Some("2024-01-01 01:00:00+00:00".into()),
            ended_at: None,
            dependency_counts: None,
            required_capabilities: None,
            parameter_space: None,
            description: None,
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["stepId"], "step-aaa");
        assert_eq!(json["name"], "Render");
        assert_eq!(json["taskRunStatus"], "RUNNING");
        assert_eq!(json["taskRunStatusCounts"]["RUNNING"], 3);
        assert_eq!(json["taskRunStatusCounts"]["PENDING"], 7);
        assert!(json.get("dependencyCounts").is_none());
        assert!(json.get("parameterSpace").is_none());
    }

    // -----------------------------------------------------------------------
    // TaskResponse
    // -----------------------------------------------------------------------

    #[test]
    fn task_response_serializes_with_raw_parameters() {
        let resp = TaskResponse {
            task_id: "task-aaa".into(),
            created_at: "2024-01-01 00:00:00+00:00".into(),
            created_by: "user".into(),
            run_status: "SUCCEEDED".into(),
            target_run_status: None,
            failure_retry_count: Some(0),
            started_at: Some("2024-01-01 01:00:00+00:00".into()),
            ended_at: Some("2024-01-01 02:00:00+00:00".into()),
            updated_at: None,
            updated_by: None,
            latest_session_action_id: Some("sessionaction-aaa".into()),
            parameters: Some(serde_json::json!({
                "Frame": {"int": "5"}
            })),
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["taskId"], "task-aaa");
        assert_eq!(json["runStatus"], "SUCCEEDED");
        assert_eq!(json["parameters"]["Frame"]["int"], "5");
        assert_eq!(json["latestSessionActionId"], "sessionaction-aaa");
        assert_eq!(json["failureRetryCount"], 0);
        assert!(json.get("targetRunStatus").is_none());
    }

    // -----------------------------------------------------------------------
    // SessionResponse
    // -----------------------------------------------------------------------

    #[test]
    fn session_response_includes_nested_types() {
        let resp = SessionResponse {
            session_id: "session-aaa".into(),
            fleet_id: "fleet-aaa".into(),
            worker_id: "worker-aaa".into(),
            started_at: "2024-01-01 00:00:00+00:00".into(),
            lifecycle_status: "STARTED".into(),
            ended_at: None,
            target_lifecycle_status: None,
            updated_at: None,
            updated_by: None,
            log: Some(serde_json::json!({
                "logDriver": "awslogs",
                "options": {"logGroupName": "/aws/deadline/queue-aaa"}
            })),
            host_properties: Some(serde_json::json!({
                "ipAddresses": {"ipV4Addresses": ["10.0.0.1"]},
                "hostName": "ip-10-0-0-1"
            })),
            worker_log: None,
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["sessionId"], "session-aaa");
        assert_eq!(json["fleetId"], "fleet-aaa");
        assert_eq!(json["workerId"], "worker-aaa");
        assert_eq!(json["lifecycleStatus"], "STARTED");
        assert_eq!(json["log"]["logDriver"], "awslogs");
        assert_eq!(json["hostProperties"]["hostName"], "ip-10-0-0-1");
        assert!(json.get("endedAt").is_none());
        assert!(json.get("workerLog").is_none());
    }

    // -----------------------------------------------------------------------
    // WorkerResponse
    // -----------------------------------------------------------------------

    #[test]
    fn worker_response_includes_nested_types() {
        let resp = WorkerResponse {
            farm_id: "farm-abc".into(),
            fleet_id: "fleet-aaa".into(),
            worker_id: "worker-aaa".into(),
            status: "RUNNING".into(),
            created_at: "2024-01-01 00:00:00+00:00".into(),
            created_by: "user".into(),
            updated_at: None,
            updated_by: None,
            host_properties: Some(serde_json::json!({
                "ipAddresses": {"ipV4Addresses": ["10.0.0.1"]},
                "hostName": "ip-10-0-0-1"
            })),
            log: Some(serde_json::json!({
                "logDriver": "awslogs",
                "options": {"logGroupName": "/aws/deadline/fleet-aaa"}
            })),
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["farmId"], "farm-abc");
        assert_eq!(json["fleetId"], "fleet-aaa");
        assert_eq!(json["workerId"], "worker-aaa");
        assert_eq!(json["status"], "RUNNING");
        assert_eq!(json["hostProperties"]["hostName"], "ip-10-0-0-1");
        assert_eq!(json["log"]["logDriver"], "awslogs");
        assert!(json.get("updatedAt").is_none());
    }
}
