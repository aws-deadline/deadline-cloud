//! Convert SDK types that lack `Serialize` into `serde_json::Value`.
//!
//! Each function walks the typed SDK accessors and builds a JSON value
//! manually. This eliminates the need for `ResponseBodyCapture`.

use aws_sdk_deadline::types::{
    Attachments, DependencyCounts, FleetConfiguration, HostPropertiesResponse,
    JobAttachmentSettings, JobParameter, JobRunAsUser, LogConfiguration, ManifestProperties,
    ParameterSpace, SchedulingConfiguration, StepRequiredCapabilities, TaskParameterValue,
};
use aws_sdk_deadline::operation::get_storage_profile_for_queue::GetStorageProfileForQueueOutput;
use serde_json::{json, Map, Value};

pub fn job_parameter_to_value(p: &JobParameter) -> Value {
    match p {
        JobParameter::Float(v) => json!({"float": v}),
        JobParameter::Int(v) => json!({"int": v}),
        JobParameter::Path(v) => json!({"path": v}),
        JobParameter::String(v) => json!({"string": v}),
        _ => json!({}),
    }
}

pub fn task_parameter_value_to_value(p: &TaskParameterValue) -> Value {
    match p {
        TaskParameterValue::ChunkInt(v) => json!({"chunkInt": v}),
        TaskParameterValue::Float(v) => json!({"float": v}),
        TaskParameterValue::Int(v) => json!({"int": v}),
        TaskParameterValue::Path(v) => json!({"path": v}),
        TaskParameterValue::String(v) => json!({"string": v}),
        _ => json!({}),
    }
}

pub fn job_attachment_settings_to_value(s: &JobAttachmentSettings) -> Value {
    json!({
        "s3BucketName": s.s3_bucket_name(),
        "rootPrefix": s.root_prefix(),
    })
}

pub fn log_configuration_to_value(l: &LogConfiguration) -> Value {
    let mut obj = Map::new();
    obj.insert("logDriver".into(), json!(l.log_driver()));
    if let Some(opts) = l.options() {
        obj.insert("options".into(), json!(opts));
    }
    if let Some(params) = l.parameters() {
        obj.insert("parameters".into(), json!(params));
    }
    if let Some(err) = l.error() {
        obj.insert("error".into(), json!(err));
    }
    Value::Object(obj)
}

pub fn ip_addresses_to_value(a: &aws_sdk_deadline::types::IpAddresses) -> Value {
    let mut obj = Map::new();
    if a.ipv4_addresses.is_some() {
        obj.insert("ipV4Addresses".into(), json!(a.ipv4_addresses()));
    }
    if a.ipv6_addresses.is_some() {
        obj.insert("ipV6Addresses".into(), json!(a.ipv6_addresses()));
    }
    Value::Object(obj)
}

pub fn host_properties_to_value(h: &HostPropertiesResponse) -> Value {
    let mut obj = Map::new();
    if let Some(ip) = h.ip_addresses() {
        obj.insert("ipAddresses".into(), ip_addresses_to_value(ip));
    }
    if let Some(name) = h.host_name() {
        obj.insert("hostName".into(), json!(name));
    }
    if let Some(arn) = h.ec2_instance_arn() {
        obj.insert("ec2InstanceArn".into(), json!(arn));
    }
    if let Some(t) = h.ec2_instance_type() {
        obj.insert("ec2InstanceType".into(), json!(t));
    }
    Value::Object(obj)
}

pub fn posix_user_to_value(u: &aws_sdk_deadline::types::PosixUser) -> Value {
    json!({"user": u.user(), "group": u.group()})
}

pub fn windows_user_to_value(u: &aws_sdk_deadline::types::WindowsUser) -> Value {
    json!({"user": u.user(), "passwordArn": u.password_arn()})
}

pub fn job_run_as_user_to_value(j: &JobRunAsUser) -> Value {
    let mut obj = Map::new();
    if let Some(p) = j.posix() {
        obj.insert("posix".into(), posix_user_to_value(p));
    }
    if let Some(w) = j.windows() {
        obj.insert("windows".into(), windows_user_to_value(w));
    }
    obj.insert("runAs".into(), json!(j.run_as().as_str()));
    Value::Object(obj)
}

pub fn dependency_counts_to_value(d: &DependencyCounts) -> Value {
    json!({
        "dependenciesResolved": d.dependencies_resolved(),
        "dependenciesUnresolved": d.dependencies_unresolved(),
        "consumersResolved": d.consumers_resolved(),
        "consumersUnresolved": d.consumers_unresolved(),
    })
}

pub fn manifest_properties_to_value(m: &ManifestProperties) -> Value {
    let mut obj = Map::new();
    if let Some(name) = m.file_system_location_name() {
        obj.insert("fileSystemLocationName".into(), json!(name));
    }
    obj.insert("rootPath".into(), json!(m.root_path()));
    obj.insert("rootPathFormat".into(), json!(m.root_path_format().as_str()));
    if m.output_relative_directories.is_some() {
        obj.insert(
            "outputRelativeDirectories".into(),
            json!(m.output_relative_directories()),
        );
    }
    if let Some(p) = m.input_manifest_path() {
        obj.insert("inputManifestPath".into(), json!(p));
    }
    if let Some(h) = m.input_manifest_hash() {
        obj.insert("inputManifestHash".into(), json!(h));
    }
    Value::Object(obj)
}

pub fn attachments_to_value(a: &Attachments) -> Value {
    let manifests: Vec<Value> = a.manifests().iter().map(manifest_properties_to_value).collect();
    json!({
        "manifests": manifests,
        "fileSystem": a.file_system().as_str(),
    })
}

pub fn parameter_space_to_value(p: &ParameterSpace) -> Value {
    let params: Vec<Value> = p.parameters().iter().map(|sp| {
        let mut obj = Map::new();
        obj.insert("name".into(), json!(sp.name()));
        obj.insert("type".into(), json!(sp.r#type().as_str()));
        if let Some(chunks) = sp.chunks() {
            let mut c = Map::new();
            c.insert("defaultTaskCount".into(), json!(chunks.default_task_count()));
            if let Some(t) = chunks.target_runtime_seconds() {
                c.insert("targetRuntimeSeconds".into(), json!(t));
            }
            c.insert("rangeConstraint".into(), json!(chunks.range_constraint().as_str()));
            obj.insert("chunks".into(), Value::Object(c));
        }
        Value::Object(obj)
    }).collect();
    let mut obj = Map::new();
    obj.insert("parameters".into(), json!(params));
    if let Some(c) = p.combination() {
        obj.insert("combination".into(), json!(c));
    }
    Value::Object(obj)
}

fn vcpu_count_range_to_value(r: &aws_sdk_deadline::types::VCpuCountRange) -> Value {
    let mut obj = Map::new();
    obj.insert("min".into(), json!(r.min()));
    if let Some(max) = r.max() {
        obj.insert("max".into(), json!(max));
    }
    Value::Object(obj)
}

fn memory_mib_range_to_value(r: &aws_sdk_deadline::types::MemoryMiBRange) -> Value {
    let mut obj = Map::new();
    obj.insert("min".into(), json!(r.min()));
    if let Some(max) = r.max() {
        obj.insert("max".into(), json!(max));
    }
    Value::Object(obj)
}

fn fleet_amount_capability_to_value(c: &aws_sdk_deadline::types::FleetAmountCapability) -> Value {
    let mut obj = Map::new();
    obj.insert("name".into(), json!(c.name()));
    obj.insert("min".into(), json!(c.min()));
    if let Some(max) = c.max() {
        obj.insert("max".into(), json!(max));
    }
    Value::Object(obj)
}

fn fleet_attribute_capability_to_value(c: &aws_sdk_deadline::types::FleetAttributeCapability) -> Value {
    json!({"name": c.name(), "values": c.values()})
}

fn customer_managed_worker_capabilities_to_value(
    w: &aws_sdk_deadline::types::CustomerManagedWorkerCapabilities,
) -> Value {
    let mut obj = Map::new();
    if let Some(v) = w.v_cpu_count() {
        obj.insert("vCpuCount".into(), vcpu_count_range_to_value(v));
    }
    if let Some(m) = w.memory_mib() {
        obj.insert("memoryMiB".into(), memory_mib_range_to_value(m));
    }
    if w.accelerator_types.is_some() {
        let types: Vec<&str> = w.accelerator_types().iter().map(|t| t.as_str()).collect();
        obj.insert("acceleratorTypes".into(), json!(types));
    }
    if let Some(c) = w.accelerator_count() {
        let mut ac = Map::new();
        ac.insert("min".into(), json!(c.min()));
        if let Some(max) = c.max() {
            ac.insert("max".into(), json!(max));
        }
        obj.insert("acceleratorCount".into(), Value::Object(ac));
    }
    if let Some(m) = w.accelerator_total_memory_mib() {
        let mut am = Map::new();
        am.insert("min".into(), json!(m.min()));
        if let Some(max) = m.max() {
            am.insert("max".into(), json!(max));
        }
        obj.insert("acceleratorTotalMemoryMiB".into(), Value::Object(am));
    }
    obj.insert("osFamily".into(), json!(w.os_family().as_str()));
    obj.insert("cpuArchitectureType".into(), json!(w.cpu_architecture_type().as_str()));
    if w.custom_amounts.is_some() {
        let amounts: Vec<Value> = w.custom_amounts().iter().map(fleet_amount_capability_to_value).collect();
        obj.insert("customAmounts".into(), json!(amounts));
    }
    if w.custom_attributes.is_some() {
        let attrs: Vec<Value> = w.custom_attributes().iter().map(fleet_attribute_capability_to_value).collect();
        obj.insert("customAttributes".into(), json!(attrs));
    }
    Value::Object(obj)
}

fn service_managed_ec2_instance_capabilities_to_value(
    c: &aws_sdk_deadline::types::ServiceManagedEc2InstanceCapabilities,
) -> Value {
    let mut obj = Map::new();
    if let Some(v) = c.v_cpu_count() {
        obj.insert("vCpuCount".into(), vcpu_count_range_to_value(v));
    }
    if let Some(m) = c.memory_mib() {
        obj.insert("memoryMiB".into(), memory_mib_range_to_value(m));
    }
    obj.insert("osFamily".into(), json!(c.os_family().as_str()));
    obj.insert("cpuArchitectureType".into(), json!(c.cpu_architecture_type().as_str()));
    if let Some(vol) = c.root_ebs_volume() {
        obj.insert("rootEbsVolume".into(), json!({
            "sizeGiB": vol.size_gib(),
            "iops": vol.iops(),
            "throughputMiB": vol.throughput_mib(),
        }));
    }
    if let Some(acc) = c.accelerator_capabilities() {
        let mut ac = Map::new();
        let sels: Vec<Value> = acc.selections().iter().map(|s| {
            let mut so = Map::new();
            so.insert("name".into(), json!(s.name().as_str()));
            let rt = s.runtime();
            if !rt.is_empty() {
                so.insert("runtime".into(), json!(rt));
            }
            Value::Object(so)
        }).collect();
        ac.insert("selections".into(), json!(sels));
        if let Some(count) = acc.count() {
                let mut co = Map::new();
                co.insert("min".into(), json!(count.min()));
                if let Some(max) = count.max() {
                    co.insert("max".into(), json!(max));
                }
                ac.insert("count".into(), Value::Object(co));
            }
        obj.insert("acceleratorCapabilities".into(), Value::Object(ac));
    }
    if c.allowed_instance_types.is_some() {
        obj.insert("allowedInstanceTypes".into(), json!(c.allowed_instance_types()));
    }
    if c.excluded_instance_types.is_some() {
        obj.insert("excludedInstanceTypes".into(), json!(c.excluded_instance_types()));
    }
    if c.custom_amounts.is_some() {
        let amounts: Vec<Value> = c.custom_amounts().iter().map(fleet_amount_capability_to_value).collect();
        obj.insert("customAmounts".into(), json!(amounts));
    }
    if c.custom_attributes.is_some() {
        let attrs: Vec<Value> = c.custom_attributes().iter().map(fleet_attribute_capability_to_value).collect();
        obj.insert("customAttributes".into(), json!(attrs));
    }
    Value::Object(obj)
}

pub fn fleet_configuration_to_value(c: &FleetConfiguration) -> Value {
    match c {
        FleetConfiguration::CustomerManaged(cm) => {
            let mut obj = Map::new();
            obj.insert("mode".into(), json!(cm.mode().as_str()));
            if let Some(asc) = cm.auto_scaling_configuration() {
                let mut ac = Map::new();
                if let Some(s) = asc.standby_worker_count() {
                    ac.insert("standbyWorkerCount".into(), json!(s));
                }
                ac.insert("workerIdleDurationSeconds".into(), json!(asc.worker_idle_duration_seconds()));
                if let Some(s) = asc.scale_out_workers_per_minute() {
                    ac.insert("scaleOutWorkersPerMinute".into(), json!(s));
                }
                obj.insert("autoScalingConfiguration".into(), Value::Object(ac));
            }
            if let Some(wc) = cm.worker_capabilities() {
                obj.insert("workerCapabilities".into(), customer_managed_worker_capabilities_to_value(wc));
            }
            if let Some(sp) = cm.storage_profile_id() {
                obj.insert("storageProfileId".into(), json!(sp));
            }
            if let Some(tp) = cm.tag_propagation_mode() {
                obj.insert("tagPropagationMode".into(), json!(tp.as_str()));
            }
            json!({"customerManaged": Value::Object(obj)})
        }
        FleetConfiguration::ServiceManagedEc2(sm) => {
            let mut obj = Map::new();
            if let Some(ic) = sm.instance_capabilities() {
                obj.insert("instanceCapabilities".into(), service_managed_ec2_instance_capabilities_to_value(ic));
            }
            if let Some(imo) = sm.instance_market_options() {
                obj.insert("instanceMarketOptions".into(), json!({"type": imo.r#type().as_str()}));
            }
            if let Some(vpc) = sm.vpc_configuration() {
                if vpc.resource_configuration_arns.is_some() {
                    obj.insert("vpcConfiguration".into(), json!({"resourceConfigurationArns": vpc.resource_configuration_arns()}));
                } else {
                    obj.insert("vpcConfiguration".into(), json!({}));
                }
            }
            if let Some(sp) = sm.storage_profile_id() {
                obj.insert("storageProfileId".into(), json!(sp));
            }
            if let Some(asc) = sm.auto_scaling_configuration() {
                let mut ac = Map::new();
                if let Some(s) = asc.standby_worker_count() {
                    ac.insert("standbyWorkerCount".into(), json!(s));
                }
                ac.insert("workerIdleDurationSeconds".into(), json!(asc.worker_idle_duration_seconds()));
                if let Some(s) = asc.scale_out_workers_per_minute() {
                    ac.insert("scaleOutWorkersPerMinute".into(), json!(s));
                }
                obj.insert("autoScalingConfiguration".into(), Value::Object(ac));
            }
            json!({"serviceManagedEc2": Value::Object(obj)})
        }
        _ => json!({}),
    }
}

pub fn scheduling_configuration_to_value(c: &SchedulingConfiguration) -> Value {
    match c {
        SchedulingConfiguration::PriorityFifo(_) => json!({"priorityFifo": {}}),
        SchedulingConfiguration::PriorityBalanced(pb) => {
            json!({"priorityBalanced": {"renderingTaskBuffer": pb.rendering_task_buffer()}})
        }
        SchedulingConfiguration::WeightedBalanced(wb) => {
            let mut obj = Map::new();
            obj.insert("priorityWeight".into(), json!(wb.priority_weight()));
            obj.insert("errorWeight".into(), json!(wb.error_weight()));
            obj.insert("submissionTimeWeight".into(), json!(wb.submission_time_weight()));
            obj.insert("renderingTaskWeight".into(), json!(wb.rendering_task_weight()));
            obj.insert("renderingTaskBuffer".into(), json!(wb.rendering_task_buffer()));
            if let Some(_mpo) = wb.max_priority_override() {
                obj.insert("maxPriorityOverride".into(), json!("ALWAYS_SCHEDULE_FIRST"));
            }
            json!({"weightedBalanced": Value::Object(obj)})
        }
        _ => json!({}),
    }
}

pub fn step_required_capabilities_to_value(c: &StepRequiredCapabilities) -> Value {
    let attributes: Vec<Value> = c.attributes().iter().map(|a| {
        let mut obj = Map::new();
        obj.insert("name".into(), json!(a.name()));
        if a.any_of.is_some() {
            obj.insert("anyOf".into(), json!(a.any_of()));
        }
        if a.all_of.is_some() {
            obj.insert("allOf".into(), json!(a.all_of()));
        }
        Value::Object(obj)
    }).collect();
    let amounts: Vec<Value> = c.amounts().iter().map(|a| {
        let mut obj = Map::new();
        obj.insert("name".into(), json!(a.name()));
        if let Some(min) = a.min() {
            obj.insert("min".into(), json!(min));
        }
        if let Some(max) = a.max() {
            obj.insert("max".into(), json!(max));
        }
        if let Some(val) = a.value() {
            obj.insert("value".into(), json!(val));
        }
        Value::Object(obj)
    }).collect();
    json!({"attributes": attributes, "amounts": amounts})
}

/// Convert GetStorageProfileForQueueOutput to Value matching the API JSON shape.
pub fn storage_profile_output_to_value(output: &GetStorageProfileForQueueOutput) -> Value {
    let mut m = Map::new();
    m.insert("storageProfileId".into(), json!(output.storage_profile_id()));
    m.insert("displayName".into(), json!(output.display_name()));
    m.insert("osFamily".into(), json!(output.os_family().as_str()));
    let locations: Vec<Value> = output.file_system_locations().iter().map(|loc| {
        json!({
            "name": loc.name(),
            "path": loc.path(),
            "type": loc.r#type().as_str(),
        })
    }).collect();
    m.insert("fileSystemLocations".into(), json!(locations));
    Value::Object(m)
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_deadline::types::{
        Attachments, AutoScalingMode, CpuArchitectureType,
        CustomerManagedFleetConfiguration, CustomerManagedFleetOperatingSystemFamily,
        CustomerManagedWorkerCapabilities, DependencyCounts, FleetConfiguration,
        HostPropertiesResponse, IpAddresses, JobAttachmentSettings, JobAttachmentsFileSystem,
        JobParameter, JobRunAsUser, LogConfiguration, ManifestProperties, MemoryMiBRange,
        ParameterSpace, PathFormat, PosixUser, PriorityBalancedSchedulingConfiguration,
        PriorityFifoSchedulingConfiguration, RunAs, SchedulingConfiguration,
        ServiceManagedEc2FleetConfiguration, ServiceManagedEc2InstanceCapabilities,
        ServiceManagedFleetOperatingSystemFamily, StepAmountCapability,
        StepAttributeCapability, StepParameter, StepParameterType, StepRequiredCapabilities,
        TaskParameterValue, VCpuCountRange, WindowsUser,
    };

    // -------------------------------------------------------------------
    // JobParameter (enum with 4 variants)
    // -------------------------------------------------------------------

    #[test]
    fn job_parameter_float() {
        let p = JobParameter::Float("3.14".into());
        let v = job_parameter_to_value(&p);
        assert_eq!(v, json!({"float": "3.14"}));
    }

    #[test]
    fn job_parameter_int() {
        let p = JobParameter::Int("42".into());
        let v = job_parameter_to_value(&p);
        assert_eq!(v, json!({"int": "42"}));
    }

    #[test]
    fn job_parameter_path() {
        let p = JobParameter::Path("/tmp/output".into());
        let v = job_parameter_to_value(&p);
        assert_eq!(v, json!({"path": "/tmp/output"}));
    }

    #[test]
    fn job_parameter_string() {
        let p = JobParameter::String("hello".into());
        let v = job_parameter_to_value(&p);
        assert_eq!(v, json!({"string": "hello"}));
    }

    // -------------------------------------------------------------------
    // TaskParameterValue (enum with 5 variants)
    // -------------------------------------------------------------------

    #[test]
    fn task_parameter_value_chunk_int() {
        let p = TaskParameterValue::ChunkInt("1-10".into());
        let v = task_parameter_value_to_value(&p);
        assert_eq!(v, json!({"chunkInt": "1-10"}));
    }

    #[test]
    fn task_parameter_value_float() {
        let p = TaskParameterValue::Float("2.5".into());
        let v = task_parameter_value_to_value(&p);
        assert_eq!(v, json!({"float": "2.5"}));
    }

    #[test]
    fn task_parameter_value_int() {
        let p = TaskParameterValue::Int("7".into());
        let v = task_parameter_value_to_value(&p);
        assert_eq!(v, json!({"int": "7"}));
    }

    #[test]
    fn task_parameter_value_path() {
        let p = TaskParameterValue::Path("/out".into());
        let v = task_parameter_value_to_value(&p);
        assert_eq!(v, json!({"path": "/out"}));
    }

    #[test]
    fn task_parameter_value_string() {
        let p = TaskParameterValue::String("val".into());
        let v = task_parameter_value_to_value(&p);
        assert_eq!(v, json!({"string": "val"}));
    }

    // -------------------------------------------------------------------
    // JobAttachmentSettings
    // -------------------------------------------------------------------

    #[test]
    fn job_attachment_settings_converts() {
        let s = JobAttachmentSettings::builder()
            .s3_bucket_name("my-bucket")
            .root_prefix("Data/")
            .build()
            .unwrap();
        let v = job_attachment_settings_to_value(&s);
        assert_eq!(v, json!({"s3BucketName": "my-bucket", "rootPrefix": "Data/"}));
    }

    // -------------------------------------------------------------------
    // LogConfiguration
    // -------------------------------------------------------------------

    #[test]
    fn log_configuration_minimal() {
        let l = LogConfiguration::builder()
            .log_driver("awslogs")
            .build()
            .unwrap();
        let v = log_configuration_to_value(&l);
        assert_eq!(v["logDriver"], "awslogs");
        assert!(v.get("options").is_none());
        assert!(v.get("parameters").is_none());
        assert!(v.get("error").is_none());
    }

    #[test]
    fn log_configuration_with_options() {
        let l = LogConfiguration::builder()
            .log_driver("awslogs")
            .options("logGroupName", "/aws/deadline/queue-aaa")
            .build()
            .unwrap();
        let v = log_configuration_to_value(&l);
        assert_eq!(v["logDriver"], "awslogs");
        assert_eq!(v["options"]["logGroupName"], "/aws/deadline/queue-aaa");
    }

    // -------------------------------------------------------------------
    // IpAddresses
    // -------------------------------------------------------------------

    #[test]
    fn ip_addresses_ipv4_only() {
        let a = IpAddresses::builder()
            .ipv4_addresses("10.0.0.1")
            .ipv4_addresses("10.0.0.2")
            .build();
        let v = ip_addresses_to_value(&a);
        assert_eq!(v["ipV4Addresses"], json!(["10.0.0.1", "10.0.0.2"]));
        assert!(v.get("ipV6Addresses").is_none());
    }

    #[test]
    fn ip_addresses_both() {
        let a = IpAddresses::builder()
            .ipv4_addresses("10.0.0.1")
            .ipv6_addresses("::1")
            .build();
        let v = ip_addresses_to_value(&a);
        assert_eq!(v["ipV4Addresses"], json!(["10.0.0.1"]));
        assert_eq!(v["ipV6Addresses"], json!(["::1"]));
    }

    // -------------------------------------------------------------------
    // HostPropertiesResponse
    // -------------------------------------------------------------------

    #[test]
    fn host_properties_full() {
        let h = HostPropertiesResponse::builder()
            .ip_addresses(
                IpAddresses::builder().ipv4_addresses("10.0.0.1").build(),
            )
            .host_name("ip-10-0-0-1")
            .ec2_instance_arn("arn:aws:ec2:us-west-2:123:instance/i-abc")
            .ec2_instance_type("m5.large")
            .build();
        let v = host_properties_to_value(&h);
        assert_eq!(v["hostName"], "ip-10-0-0-1");
        assert_eq!(v["ipAddresses"]["ipV4Addresses"], json!(["10.0.0.1"]));
        assert_eq!(v["ec2InstanceArn"], "arn:aws:ec2:us-west-2:123:instance/i-abc");
        assert_eq!(v["ec2InstanceType"], "m5.large");
    }

    #[test]
    fn host_properties_minimal() {
        let h = HostPropertiesResponse::builder().build();
        let v = host_properties_to_value(&h);
        assert!(v.get("hostName").is_none());
        assert!(v.get("ipAddresses").is_none());
        assert!(v.get("ec2InstanceArn").is_none());
    }

    // -------------------------------------------------------------------
    // PosixUser / WindowsUser / JobRunAsUser
    // -------------------------------------------------------------------

    #[test]
    fn posix_user_converts() {
        let u = PosixUser::builder().user("render").group("artists").build().unwrap();
        let v = posix_user_to_value(&u);
        assert_eq!(v, json!({"user": "render", "group": "artists"}));
    }

    #[test]
    fn windows_user_converts() {
        let u = WindowsUser::builder()
            .user("DOMAIN\\render")
            .password_arn("arn:aws:secretsmanager:us-west-2:123:secret:pw")
            .build()
            .unwrap();
        let v = windows_user_to_value(&u);
        assert_eq!(v["user"], "DOMAIN\\render");
        assert_eq!(v["passwordArn"], "arn:aws:secretsmanager:us-west-2:123:secret:pw");
    }

    #[test]
    fn job_run_as_user_posix_only() {
        let j = JobRunAsUser::builder()
            .posix(PosixUser::builder().user("render").group("artists").build().unwrap())
            .run_as(RunAs::QueueConfiguredUser)
            .build()
            .unwrap();
        let v = job_run_as_user_to_value(&j);
        assert_eq!(v["posix"]["user"], "render");
        assert_eq!(v["runAs"], "QUEUE_CONFIGURED_USER");
        assert!(v.get("windows").is_none());
    }

    // -------------------------------------------------------------------
    // DependencyCounts
    // -------------------------------------------------------------------

    #[test]
    fn dependency_counts_converts() {
        let d = DependencyCounts::builder()
            .dependencies_resolved(5)
            .dependencies_unresolved(2)
            .consumers_resolved(3)
            .consumers_unresolved(1)
            .build()
            .unwrap();
        let v = dependency_counts_to_value(&d);
        assert_eq!(v["dependenciesResolved"], 5);
        assert_eq!(v["dependenciesUnresolved"], 2);
        assert_eq!(v["consumersResolved"], 3);
        assert_eq!(v["consumersUnresolved"], 1);
    }

    // -------------------------------------------------------------------
    // ManifestProperties
    // -------------------------------------------------------------------

    #[test]
    fn manifest_properties_full() {
        let m = ManifestProperties::builder()
            .root_path("/mnt/shared")
            .root_path_format(PathFormat::Posix)
            .file_system_location_name("SharedFS")
            .output_relative_directories("output/")
            .input_manifest_path("manifest.json")
            .input_manifest_hash("abc123")
            .build()
            .unwrap();
        let v = manifest_properties_to_value(&m);
        assert_eq!(v["rootPath"], "/mnt/shared");
        assert_eq!(v["rootPathFormat"], "posix");
        assert_eq!(v["fileSystemLocationName"], "SharedFS");
        assert_eq!(v["outputRelativeDirectories"], json!(["output/"]));
        assert_eq!(v["inputManifestPath"], "manifest.json");
        assert_eq!(v["inputManifestHash"], "abc123");
    }

    #[test]
    fn manifest_properties_minimal() {
        let m = ManifestProperties::builder()
            .root_path("/tmp")
            .root_path_format(PathFormat::Posix)
            .build()
            .unwrap();
        let v = manifest_properties_to_value(&m);
        assert_eq!(v["rootPath"], "/tmp");
        assert_eq!(v["rootPathFormat"], "posix");
        assert!(v.get("fileSystemLocationName").is_none());
        assert!(v.get("inputManifestPath").is_none());
    }

    // -------------------------------------------------------------------
    // Attachments
    // -------------------------------------------------------------------

    #[test]
    fn attachments_converts() {
        let a = Attachments::builder()
            .manifests(
                ManifestProperties::builder()
                    .root_path("/mnt/shared")
                    .root_path_format(PathFormat::Posix)
                    .build()
                    .unwrap(),
            )
            .file_system(JobAttachmentsFileSystem::Copied)
            .build()
            .unwrap();
        let v = attachments_to_value(&a);
        assert_eq!(v["fileSystem"], "COPIED");
        assert_eq!(v["manifests"][0]["rootPath"], "/mnt/shared");
    }

    // -------------------------------------------------------------------
    // ParameterSpace
    // -------------------------------------------------------------------

    #[test]
    fn parameter_space_converts() {
        let p = ParameterSpace::builder()
            .parameters(
                StepParameter::builder()
                    .name("Frame")
                    .r#type(StepParameterType::Int)
                    .build()
                    .unwrap(),
            )
            .combination("Frame")
            .build()
            .unwrap();
        let v = parameter_space_to_value(&p);
        assert_eq!(v["parameters"][0]["name"], "Frame");
        assert_eq!(v["parameters"][0]["type"], "INT");
        assert_eq!(v["combination"], "Frame");
    }

    #[test]
    fn parameter_space_no_combination() {
        let p = ParameterSpace::builder()
            .parameters(
                StepParameter::builder()
                    .name("Chunk")
                    .r#type(StepParameterType::String)
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap();
        let v = parameter_space_to_value(&p);
        assert_eq!(v["parameters"][0]["name"], "Chunk");
        assert!(v.get("combination").is_none());
    }

    // -------------------------------------------------------------------
    // FleetConfiguration (deep nested enum)
    // -------------------------------------------------------------------

    #[test]
    fn fleet_configuration_customer_managed() {
        let c = FleetConfiguration::CustomerManaged(
            CustomerManagedFleetConfiguration::builder()
                .mode(AutoScalingMode::NoScaling)
                .worker_capabilities(
                    CustomerManagedWorkerCapabilities::builder()
                        .v_cpu_count(VCpuCountRange::builder().min(4).max(8).build().unwrap())
                        .memory_mib(MemoryMiBRange::builder().min(8192).max(16384).build().unwrap())
                        .os_family(CustomerManagedFleetOperatingSystemFamily::Linux)
                        .cpu_architecture_type(CpuArchitectureType::X8664)
                        .build()
                        .unwrap(),
                )
                .storage_profile_id("sp-abc")
                .build()
                .unwrap(),
        );
        let v = fleet_configuration_to_value(&c);
        assert_eq!(v["customerManaged"]["mode"], "NO_SCALING");
        assert_eq!(v["customerManaged"]["workerCapabilities"]["vCpuCount"]["min"], 4);
        assert_eq!(v["customerManaged"]["workerCapabilities"]["vCpuCount"]["max"], 8);
        assert_eq!(v["customerManaged"]["workerCapabilities"]["memoryMiB"]["min"], 8192);
        assert_eq!(v["customerManaged"]["workerCapabilities"]["osFamily"], "LINUX");
        assert_eq!(v["customerManaged"]["workerCapabilities"]["cpuArchitectureType"], "x86_64");
        assert_eq!(v["customerManaged"]["storageProfileId"], "sp-abc");
    }

    #[test]
    fn fleet_configuration_service_managed_ec2() {
        let c = FleetConfiguration::ServiceManagedEc2(
            ServiceManagedEc2FleetConfiguration::builder()
                .instance_capabilities(
                    ServiceManagedEc2InstanceCapabilities::builder()
                        .v_cpu_count(VCpuCountRange::builder().min(2).max(16).build().unwrap())
                        .memory_mib(MemoryMiBRange::builder().min(4096).max(32768).build().unwrap())
                        .os_family(ServiceManagedFleetOperatingSystemFamily::Linux)
                        .cpu_architecture_type(CpuArchitectureType::X8664)
                        .allowed_instance_types("m5.large".to_string())
                        .allowed_instance_types("m5.xlarge".to_string())
                        .build()
                        .unwrap()
                )
                .build()
        );
        let v = fleet_configuration_to_value(&c);
        assert_eq!(v["serviceManagedEc2"]["instanceCapabilities"]["vCpuCount"]["min"], 2);
        assert_eq!(v["serviceManagedEc2"]["instanceCapabilities"]["memoryMiB"]["min"], 4096);
        assert_eq!(v["serviceManagedEc2"]["instanceCapabilities"]["osFamily"], "LINUX");
        assert_eq!(
            v["serviceManagedEc2"]["instanceCapabilities"]["allowedInstanceTypes"],
            json!(["m5.large", "m5.xlarge"])
        );
    }

    // -------------------------------------------------------------------
    // SchedulingConfiguration (enum with 3 variants)
    // -------------------------------------------------------------------

    #[test]
    fn scheduling_configuration_priority_fifo() {
        let c = SchedulingConfiguration::PriorityFifo(
            PriorityFifoSchedulingConfiguration::builder().build(),
        );
        let v = scheduling_configuration_to_value(&c);
        assert!(v.get("priorityFifo").is_some());
    }

    #[test]
    fn scheduling_configuration_priority_balanced() {
        let c = SchedulingConfiguration::PriorityBalanced(
            PriorityBalancedSchedulingConfiguration::builder()
                .rendering_task_buffer(3)
                .build(),
        );
        let v = scheduling_configuration_to_value(&c);
        assert_eq!(v["priorityBalanced"]["renderingTaskBuffer"], 3);
    }

    // -------------------------------------------------------------------
    // StepRequiredCapabilities
    // -------------------------------------------------------------------

    #[test]
    fn step_required_capabilities_converts() {
        let c = StepRequiredCapabilities::builder()
            .attributes(
                StepAttributeCapability::builder()
                    .name("attr.worker.os.family")
                    .any_of("linux".to_string())
                    .build()
                    .unwrap(),
            )
            .amounts(
                StepAmountCapability::builder()
                    .name("amount.worker.vcpu")
                    .min(4.0)
                    .max(8.0)
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap();
        let v = step_required_capabilities_to_value(&c);
        assert_eq!(v["attributes"][0]["name"], "attr.worker.os.family");
        assert_eq!(v["attributes"][0]["anyOf"], json!(["linux"]));
        assert_eq!(v["amounts"][0]["name"], "amount.worker.vcpu");
        assert_eq!(v["amounts"][0]["min"], 4.0);
        assert_eq!(v["amounts"][0]["max"], 8.0);
    }
}
