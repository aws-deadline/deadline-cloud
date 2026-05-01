//! Convert SDK types that lack `Serialize` into `serde_json::Value`.
//!
//! Each function walks the typed SDK accessors and builds a JSON value
//! manually. This eliminates the need for `ResponseBodyCapture`.

use aws_sdk_deadline::types::{
    Attachments, DependencyCounts, FleetConfiguration, HostPropertiesResponse,
    JobAttachmentSettings, JobParameter, JobRunAsUser, LogConfiguration, ManifestProperties,
    ParameterSpace, SchedulingConfiguration, StepRequiredCapabilities, TaskParameterValue,
};
use serde_json::{json, Value};

pub fn job_parameter_to_value(p: &JobParameter) -> Value {
    todo!()
}

pub fn task_parameter_value_to_value(p: &TaskParameterValue) -> Value {
    todo!()
}

pub fn job_attachment_settings_to_value(s: &JobAttachmentSettings) -> Value {
    todo!()
}

pub fn log_configuration_to_value(l: &LogConfiguration) -> Value {
    todo!()
}

pub fn ip_addresses_to_value(a: &aws_sdk_deadline::types::IpAddresses) -> Value {
    todo!()
}

pub fn host_properties_to_value(h: &HostPropertiesResponse) -> Value {
    todo!()
}

pub fn posix_user_to_value(u: &aws_sdk_deadline::types::PosixUser) -> Value {
    todo!()
}

pub fn windows_user_to_value(u: &aws_sdk_deadline::types::WindowsUser) -> Value {
    todo!()
}

pub fn job_run_as_user_to_value(j: &JobRunAsUser) -> Value {
    todo!()
}

pub fn dependency_counts_to_value(d: &DependencyCounts) -> Value {
    todo!()
}

pub fn manifest_properties_to_value(m: &ManifestProperties) -> Value {
    todo!()
}

pub fn attachments_to_value(a: &Attachments) -> Value {
    todo!()
}

pub fn parameter_space_to_value(p: &ParameterSpace) -> Value {
    todo!()
}

pub fn fleet_configuration_to_value(c: &FleetConfiguration) -> Value {
    todo!()
}

pub fn scheduling_configuration_to_value(c: &SchedulingConfiguration) -> Value {
    todo!()
}

pub fn step_required_capabilities_to_value(c: &StepRequiredCapabilities) -> Value {
    todo!()
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
