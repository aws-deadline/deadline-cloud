/// Container for submitter environment metadata.
///
/// Holds information about the application submitting jobs to AWS Deadline Cloud.
/// Only `submitter_name` is required; all other fields are optional.
use std::collections::HashMap;

/// A YAML-safe value that can be arbitrarily nested.
/// Mirrors the Python `YamlValue` type alias.
#[derive(Debug, Clone, PartialEq)]
pub enum YamlValue {
    String(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    Null,
    Map(HashMap<String, YamlValue>),
    List(Vec<YamlValue>),
}

#[derive(Debug, Clone)]
pub struct SubmitterInfo {
    /// Short name of the submitter (e.g., "Blender", "CLI").
    pub submitter_name: String,
    /// Name of the submitter package (e.g., "deadline-cloud-for-blender").
    pub submitter_package_name: Option<String>,
    /// Version of the submitter package.
    pub submitter_package_version: Option<String>,
    /// Name of the host application (e.g., "Maya", "Blender").
    pub host_application_name: Option<String>,
    /// Version of the host application.
    pub host_application_version: Option<String>,
    /// Arbitrary nested metadata from integrations.
    pub additional_info: Option<HashMap<String, YamlValue>>,
}

impl SubmitterInfo {
    pub fn new(submitter_name: impl Into<String>) -> Self {
        Self {
            submitter_name: submitter_name.into(),
            submitter_package_name: None,
            submitter_package_version: None,
            host_application_name: None,
            host_application_version: None,
            additional_info: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // §52 case 1: Create with only submitter_name → all optional fields are None
    #[test]
    fn new_with_name_only_has_none_optional_fields() {
        let info = SubmitterInfo::new("CLI");
        assert_eq!(info.submitter_name, "CLI");
        assert!(info.submitter_package_name.is_none());
        assert!(info.submitter_package_version.is_none());
        assert!(info.host_application_name.is_none());
        assert!(info.host_application_version.is_none());
        assert!(info.additional_info.is_none());
    }

    // §52 case 2: Create with all fields populated → all accessible
    #[test]
    fn all_fields_populated_and_accessible() {
        let info = SubmitterInfo {
            submitter_name: "Blender".into(),
            submitter_package_name: Some("deadline-cloud-for-blender".into()),
            submitter_package_version: Some("0.5.0".into()),
            host_application_name: Some("Blender".into()),
            host_application_version: Some("4.5.21".into()),
            additional_info: Some(HashMap::new()),
        };
        assert_eq!(info.submitter_name, "Blender");
        assert_eq!(
            info.submitter_package_name.as_deref(),
            Some("deadline-cloud-for-blender")
        );
        assert_eq!(info.submitter_package_version.as_deref(), Some("0.5.0"));
        assert_eq!(info.host_application_name.as_deref(), Some("Blender"));
        assert_eq!(info.host_application_version.as_deref(), Some("4.5.21"));
        assert!(info.additional_info.is_some());
    }

    // §52 case 3: additional_info with nested dicts and lists → stored as-is
    #[test]
    fn additional_info_with_nested_data() {
        let mut plugins = HashMap::new();
        plugins.insert("Plugin 1".into(), YamlValue::String("0.5.0".into()));
        plugins.insert("Plugin 2".into(), YamlValue::String("0.7.0".into()));

        let mut additional = HashMap::new();
        additional.insert(
            "render_engine".into(),
            YamlValue::String("Cycles".into()),
        );
        additional.insert("Loaded Plugins".into(), YamlValue::Map(plugins));
        additional.insert(
            "frame_list".into(),
            YamlValue::List(vec![
                YamlValue::Int(1),
                YamlValue::Int(2),
                YamlValue::Int(3),
            ]),
        );

        let info = SubmitterInfo {
            submitter_name: "Blender".into(),
            additional_info: Some(additional),
            ..SubmitterInfo::new("Blender")
        };

        let additional = info.additional_info.as_ref().unwrap();
        assert_eq!(
            additional.get("render_engine"),
            Some(&YamlValue::String("Cycles".into()))
        );
        match additional.get("Loaded Plugins") {
            Some(YamlValue::Map(plugins)) => {
                assert_eq!(
                    plugins.get("Plugin 1"),
                    Some(&YamlValue::String("0.5.0".into()))
                );
            }
            other => panic!("expected Map, got {other:?}"),
        }
        match additional.get("frame_list") {
            Some(YamlValue::List(items)) => assert_eq!(items.len(), 3),
            other => panic!("expected List, got {other:?}"),
        }
    }

    // §52 case 4: additional_info is None → attribute is None
    #[test]
    fn additional_info_defaults_to_none() {
        let info = SubmitterInfo::new("CLI");
        assert!(info.additional_info.is_none());
    }
}
