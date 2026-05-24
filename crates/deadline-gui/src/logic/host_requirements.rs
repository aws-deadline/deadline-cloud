//! Host requirements serialization logic.
//!
//! Converts UI state into the JSON format expected by the job template's
//! `hostRequirements` field.

/// OS requirements (operating system family and CPU architecture).
#[derive(Debug, Clone, Default)]
pub struct OsRequirements {
    pub operating_systems: Vec<String>,
    pub cpu_architectures: Vec<String>,
}

impl OsRequirements {
    pub fn serialize(&self) -> Vec<serde_json::Value> {
        let mut result = Vec::new();
        if !self.operating_systems.is_empty() {
            result.push(serde_json::json!({
                "name": "attr.worker.os.family",
                "anyOf": self.operating_systems,
            }));
        }
        if !self.cpu_architectures.is_empty() {
            result.push(serde_json::json!({
                "name": "attr.worker.cpu.arch",
                "anyOf": self.cpu_architectures,
            }));
        }
        result
    }
}

/// Hardware requirements (CPU, memory, GPU, scratch space).
#[derive(Debug, Clone, Default)]
pub struct HardwareRequirements {
    pub cpu_min: Option<i32>,
    pub cpu_max: Option<i32>,
    pub memory_min: Option<i32>,
    pub memory_max: Option<i32>,
    pub gpu_min: Option<i32>,
    pub gpu_max: Option<i32>,
    pub gpu_memory_min: Option<i32>,
    pub gpu_memory_max: Option<i32>,
    pub scratch_min: Option<i32>,
    pub scratch_max: Option<i32>,
}

impl HardwareRequirements {
    pub fn serialize(&self) -> Vec<serde_json::Value> {
        let mut result = Vec::new();
        Self::push_amount(&mut result, "amount.worker.vcpu", self.cpu_min, self.cpu_max);
        Self::push_amount(&mut result, "amount.worker.memory", self.memory_min, self.memory_max);
        Self::push_amount(&mut result, "amount.worker.gpu", self.gpu_min, self.gpu_max);
        Self::push_amount(&mut result, "amount.worker.gpu.memory", self.gpu_memory_min, self.gpu_memory_max);
        Self::push_amount(&mut result, "amount.worker.disk.scratch", self.scratch_min, self.scratch_max);
        result
    }

    fn push_amount(result: &mut Vec<serde_json::Value>, name: &str, min: Option<i32>, max: Option<i32>) {
        if min.is_none() && max.is_none() {
            return;
        }
        let mut obj = serde_json::json!({"name": name});
        if let Some(v) = min {
            obj["min"] = serde_json::json!(v);
        }
        if let Some(v) = max {
            obj["max"] = serde_json::json!(v);
        }
        result.push(obj);
    }
}

/// A custom amount requirement (e.g. render slots, licenses).
#[derive(Debug, Clone)]
pub struct CustomAmountRequirement {
    pub name: String,
    pub min: Option<i32>,
    pub max: Option<i32>,
}

impl CustomAmountRequirement {
    pub fn serialize(&self) -> serde_json::Value {
        let mut obj = serde_json::json!({"name": format!("amount.worker.{}", self.name)});
        if let Some(v) = self.min {
            obj["min"] = serde_json::json!(v);
        }
        if let Some(v) = self.max {
            obj["max"] = serde_json::json!(v);
        }
        obj
    }
}

/// A custom attribute requirement (e.g. department, software).
#[derive(Debug, Clone)]
pub struct CustomAttributeRequirement {
    pub name: String,
    pub option: String, // "anyOf" or "allOf"
    pub values: Vec<String>,
}

impl CustomAttributeRequirement {
    pub fn serialize(&self) -> serde_json::Value {
        let mut obj = serde_json::json!({"name": format!("attr.worker.{}", self.name)});
        obj[&self.option] = serde_json::json!(self.values);
        obj
    }
}

/// Combined host requirements.
#[derive(Debug, Clone, Default)]
pub struct HostRequirements {
    pub os: OsRequirements,
    pub hardware: HardwareRequirements,
    pub custom_amounts: Vec<CustomAmountRequirement>,
    pub custom_attributes: Vec<CustomAttributeRequirement>,
}

impl HostRequirements {
    pub fn serialize(&self) -> serde_json::Value {
        let mut amounts: Vec<serde_json::Value> = Vec::new();
        let mut attributes: Vec<serde_json::Value> = Vec::new();

        attributes.extend(self.os.serialize());
        amounts.extend(self.hardware.serialize());

        for ca in &self.custom_amounts {
            amounts.push(ca.serialize());
        }
        for ca in &self.custom_attributes {
            attributes.push(ca.serialize());
        }

        let mut result = serde_json::Map::new();
        if !amounts.is_empty() {
            result.insert("amounts".to_string(), serde_json::json!(amounts));
        }
        if !attributes.is_empty() {
            result.insert("attributes".to_string(), serde_json::json!(attributes));
        }
        serde_json::Value::Object(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn os_requirements_empty_serializes_to_empty() {
        let req = OsRequirements::default();
        assert!(req.serialize().is_empty());
    }

    #[test]
    fn os_requirements_single_os() {
        let req = OsRequirements {
            operating_systems: vec!["linux".to_string()],
            cpu_architectures: vec![],
        };
        let result = req.serialize();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0]["name"], "attr.worker.os.family");
        assert_eq!(result[0]["anyOf"], serde_json::json!(["linux"]));
    }

    #[test]
    fn os_requirements_multiple_os_and_arch() {
        let req = OsRequirements {
            operating_systems: vec!["linux".to_string(), "windows".to_string()],
            cpu_architectures: vec!["x86_64".to_string()],
        };
        let result = req.serialize();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0]["anyOf"], serde_json::json!(["linux", "windows"]));
        assert_eq!(result[1]["name"], "attr.worker.cpu.arch");
    }

    #[test]
    fn hardware_requirements_default_serializes_to_empty() {
        assert!(HardwareRequirements::default().serialize().is_empty());
    }

    #[test]
    fn hardware_requirements_cpu_min_only() {
        let req = HardwareRequirements { cpu_min: Some(4), ..Default::default() };
        let result = req.serialize();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0]["name"], "amount.worker.vcpu");
        assert_eq!(result[0]["min"], 4);
        assert!(result[0].get("max").is_none());
    }

    #[test]
    fn hardware_requirements_cpu_min_and_max() {
        let req = HardwareRequirements { cpu_min: Some(2), cpu_max: Some(16), ..Default::default() };
        let result = req.serialize();
        assert_eq!(result[0]["min"], 2);
        assert_eq!(result[0]["max"], 16);
    }

    #[test]
    fn hardware_requirements_memory_gpu_scratch() {
        let req = HardwareRequirements {
            memory_min: Some(8192),
            gpu_min: Some(1),
            gpu_memory_min: Some(4096),
            scratch_min: Some(100),
            ..Default::default()
        };
        let result = req.serialize();
        assert_eq!(result.len(), 4);
        let names: Vec<&str> = result.iter().map(|r| r["name"].as_str().unwrap()).collect();
        assert!(names.contains(&"amount.worker.memory"));
        assert!(names.contains(&"amount.worker.gpu"));
        assert!(names.contains(&"amount.worker.gpu.memory"));
        assert!(names.contains(&"amount.worker.disk.scratch"));
    }

    #[test]
    fn custom_amount_min_only() {
        let req = CustomAmountRequirement { name: "render.slots".to_string(), min: Some(2), max: None };
        let result = req.serialize();
        assert_eq!(result["name"], "amount.worker.render.slots");
        assert_eq!(result["min"], 2);
        assert!(result.get("max").is_none());
    }

    #[test]
    fn custom_amount_min_and_max() {
        let req = CustomAmountRequirement { name: "licenses".to_string(), min: Some(1), max: Some(10) };
        let result = req.serialize();
        assert_eq!(result["min"], 1);
        assert_eq!(result["max"], 10);
    }

    #[test]
    fn custom_attribute_any_of() {
        let req = CustomAttributeRequirement {
            name: "department".to_string(),
            option: "anyOf".to_string(),
            values: vec!["lighting".to_string(), "compositing".to_string()],
        };
        let result = req.serialize();
        assert_eq!(result["name"], "attr.worker.department");
        assert_eq!(result["anyOf"], serde_json::json!(["lighting", "compositing"]));
    }

    #[test]
    fn custom_attribute_all_of() {
        let req = CustomAttributeRequirement {
            name: "software".to_string(),
            option: "allOf".to_string(),
            values: vec!["maya".to_string(), "arnold".to_string()],
        };
        let result = req.serialize();
        assert_eq!(result["name"], "attr.worker.software");
        assert_eq!(result["allOf"], serde_json::json!(["maya", "arnold"]));
    }

    #[test]
    fn host_requirements_full_serialization() {
        let req = HostRequirements {
            os: OsRequirements {
                operating_systems: vec!["linux".to_string()],
                cpu_architectures: vec!["x86_64".to_string()],
            },
            hardware: HardwareRequirements { cpu_min: Some(4), memory_min: Some(8192), ..Default::default() },
            custom_amounts: vec![CustomAmountRequirement { name: "slots".to_string(), min: Some(1), max: None }],
            custom_attributes: vec![CustomAttributeRequirement {
                name: "pool".to_string(),
                option: "anyOf".to_string(),
                values: vec!["render".to_string()],
            }],
        };
        let result = req.serialize();
        assert!(result.get("amounts").is_some());
        assert!(result.get("attributes").is_some());
        assert!(result["amounts"].as_array().unwrap().len() >= 3);
        assert!(result["attributes"].as_array().unwrap().len() >= 3);
    }

    #[test]
    fn host_requirements_empty_serializes_to_empty() {
        let req = HostRequirements::default();
        assert!(req.serialize().as_object().unwrap().is_empty());
    }
}
