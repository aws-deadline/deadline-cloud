//! Response structs for typed SDK output → serializable JSON.
//!
//! Each struct maps 1:1 to a Get API output. Uses `#[serde(rename_all = "camelCase")]`
//! for Python/JSON compatibility. DateTime fields are pre-formatted as strings.
//!
//! Complex nested SDK types (FleetConfiguration, JobRunAsUser, etc.) don't
//! implement Serialize. These are stored as `serde_json::Value` extracted from
//! the raw HTTP response body alongside the typed output.

use aws_sdk_deadline::operation::get_farm::GetFarmOutput;
use aws_sdk_deadline::operation::get_fleet::GetFleetOutput;
use aws_sdk_deadline::operation::get_queue::GetQueueOutput;
use serde::Serialize;
use serde_json::Value;

/// Format an AWS SDK DateTime to match Python's display format.
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

/// Response struct for GetFarm API output.
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

/// Response struct for GetQueue API output.
/// Complex nested fields (jobAttachmentSettings, jobRunAsUser, schedulingConfiguration)
/// are stored as raw JSON extracted from the HTTP response body.
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

impl QueueResponse {
    /// Build from typed SDK output + raw JSON (for nested types that lack Serialize).
    pub fn from_output_and_raw(o: GetQueueOutput, raw: &Value) -> Self {
        let fslns = o.required_file_system_location_names.filter(|v| !v.is_empty());
        let aspids = o.allowed_storage_profile_ids.filter(|v| !v.is_empty());
        Self {
            farm_id: o.farm_id,
            queue_id: o.queue_id,
            display_name: o.display_name,
            status: o.status.as_str().to_string(),
            default_budget_action: o.default_budget_action.as_str().to_string(),
            blocked_reason: o.blocked_reason.map(|r| r.as_str().to_string()),
            created_at: format_datetime(&o.created_at),
            created_by: o.created_by,
            updated_at: o.updated_at.as_ref().map(format_datetime),
            updated_by: o.updated_by,
            description: o.description,
            job_attachment_settings: raw.get("jobAttachmentSettings").cloned(),
            role_arn: o.role_arn,
            required_file_system_location_names: fslns,
            allowed_storage_profile_ids: aspids,
            job_run_as_user: raw.get("jobRunAsUser").cloned(),
            scheduling_configuration: raw.get("schedulingConfiguration").cloned(),
        }
    }
}

// ---------------------------------------------------------------------------
// FleetResponse
// ---------------------------------------------------------------------------

/// Response struct for GetFleet API output.
/// Complex nested fields (configuration, hostConfiguration, capabilities)
/// are stored as raw JSON extracted from the HTTP response body.
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

impl FleetResponse {
    /// Build from typed SDK output + raw JSON (for nested types that lack Serialize).
    pub fn from_output_and_raw(o: GetFleetOutput, raw: &Value) -> Self {
        Self {
            fleet_id: o.fleet_id,
            farm_id: o.farm_id,
            display_name: o.display_name,
            status: o.status.as_str().to_string(),
            status_message: o.status_message,
            auto_scaling_status: o.auto_scaling_status.map(|s| s.as_str().to_string()),
            target_worker_count: o.target_worker_count,
            worker_count: o.worker_count,
            min_worker_count: o.min_worker_count,
            max_worker_count: o.max_worker_count,
            configuration: raw.get("configuration").cloned(),
            created_at: format_datetime(&o.created_at),
            created_by: o.created_by,
            updated_at: o.updated_at.as_ref().map(format_datetime),
            updated_by: o.updated_by,
            description: o.description,
            host_configuration: raw.get("hostConfiguration").cloned(),
            capabilities: raw.get("capabilities").cloned(),
            role_arn: o.role_arn,
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
}
