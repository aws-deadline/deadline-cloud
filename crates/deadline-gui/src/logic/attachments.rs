//! Attachment model logic.
//!
//! Manages AssetReferences: add/remove files and directories,
//! merge auto-detected with user-added, deduplication.

/// Asset references for job attachments.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct AssetReferences {
    pub input_file_paths: Vec<String>,
    pub input_directory_paths: Vec<String>,
    pub output_directory_paths: Vec<String>,
}

impl AssetReferences {
    /// Parse from a JSON value (the format used in asset_references.json).
    pub fn from_json(json: &serde_json::Value) -> Result<Self, String> {
        let extract = |key| -> Vec<String> {
            json.get(key)
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default()
        };
        Ok(Self {
            input_file_paths: extract("inputFilePaths"),
            input_directory_paths: extract("inputDirectoryPaths"),
            output_directory_paths: extract("outputDirectoryPaths"),
        })
    }

    /// Serialize to JSON.
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "inputFilePaths": self.input_file_paths,
            "inputDirectoryPaths": self.input_directory_paths,
            "outputDirectoryPaths": self.output_directory_paths,
        })
    }

    pub fn add_input_file(&mut self, path: &str) {
        if !self.input_file_paths.iter().any(|p| p == path) {
            self.input_file_paths.push(path.to_string());
        }
    }

    pub fn remove_input_file(&mut self, index: usize) {
        if index < self.input_file_paths.len() {
            self.input_file_paths.remove(index);
        }
    }

    pub fn add_input_directory(&mut self, path: &str) {
        if !self.input_directory_paths.iter().any(|p| p == path) {
            self.input_directory_paths.push(path.to_string());
        }
    }

    pub fn remove_input_directory(&mut self, index: usize) {
        if index < self.input_directory_paths.len() {
            self.input_directory_paths.remove(index);
        }
    }

    pub fn add_output_directory(&mut self, path: &str) {
        if !self.output_directory_paths.iter().any(|p| p == path) {
            self.output_directory_paths.push(path.to_string());
        }
    }

    pub fn remove_output_directory(&mut self, index: usize) {
        if index < self.output_directory_paths.len() {
            self.output_directory_paths.remove(index);
        }
    }
}

/// Merge auto-detected and user-added attachments, deduplicating paths.
pub fn merge_attachments(auto: &AssetReferences, user: &AssetReferences) -> AssetReferences {
    let mut merged = auto.clone();
    for p in &user.input_file_paths {
        merged.add_input_file(p);
    }
    for p in &user.input_directory_paths {
        merged.add_input_directory(p);
    }
    for p in &user.output_directory_paths {
        merged.add_output_directory(p);
    }
    merged
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn asset_references_default_is_empty() {
        let ar = AssetReferences::default();
        assert!(ar.input_file_paths.is_empty());
        assert!(ar.input_directory_paths.is_empty());
        assert!(ar.output_directory_paths.is_empty());
    }

    #[test]
    fn asset_references_from_json() {
        let json = serde_json::json!({
            "inputFilePaths": ["/a.exr", "/b.exr"],
            "inputDirectoryPaths": ["/textures"],
            "outputDirectoryPaths": ["/output"]
        });
        let ar = AssetReferences::from_json(&json).unwrap();
        assert_eq!(ar.input_file_paths, vec!["/a.exr", "/b.exr"]);
        assert_eq!(ar.input_directory_paths, vec!["/textures"]);
        assert_eq!(ar.output_directory_paths, vec!["/output"]);
    }

    #[test]
    fn asset_references_from_json_missing_fields_default_to_empty() {
        let json = serde_json::json!({"inputFilePaths": ["/a.exr"]});
        let ar = AssetReferences::from_json(&json).unwrap();
        assert_eq!(ar.input_file_paths, vec!["/a.exr"]);
        assert!(ar.input_directory_paths.is_empty());
        assert!(ar.output_directory_paths.is_empty());
    }

    #[test]
    fn asset_references_to_json_roundtrip() {
        let ar = AssetReferences {
            input_file_paths: vec!["/a.exr".to_string()],
            input_directory_paths: vec!["/tex".to_string()],
            output_directory_paths: vec!["/out".to_string()],
        };
        let json = ar.to_json();
        let roundtrip = AssetReferences::from_json(&json).unwrap();
        assert_eq!(ar, roundtrip);
    }

    #[test]
    fn add_input_file() {
        let mut ar = AssetReferences::default();
        ar.add_input_file("/new/file.exr");
        assert_eq!(ar.input_file_paths, vec!["/new/file.exr"]);
    }

    #[test]
    fn add_input_file_deduplicates() {
        let mut ar = AssetReferences {
            input_file_paths: vec!["/existing.exr".to_string()],
            ..Default::default()
        };
        ar.add_input_file("/existing.exr");
        assert_eq!(ar.input_file_paths.len(), 1);
    }

    #[test]
    fn remove_input_file_by_index() {
        let mut ar = AssetReferences {
            input_file_paths: vec!["/a.exr".to_string(), "/b.exr".to_string()],
            ..Default::default()
        };
        ar.remove_input_file(0);
        assert_eq!(ar.input_file_paths, vec!["/b.exr"]);
    }

    #[test]
    fn remove_input_file_out_of_bounds_is_noop() {
        let mut ar = AssetReferences {
            input_file_paths: vec!["/a.exr".to_string()],
            ..Default::default()
        };
        ar.remove_input_file(5);
        assert_eq!(ar.input_file_paths.len(), 1);
    }

    #[test]
    fn add_input_directory() {
        let mut ar = AssetReferences::default();
        ar.add_input_directory("/textures");
        assert_eq!(ar.input_directory_paths, vec!["/textures"]);
    }

    #[test]
    fn add_input_directory_deduplicates() {
        let mut ar = AssetReferences {
            input_directory_paths: vec!["/textures".to_string()],
            ..Default::default()
        };
        ar.add_input_directory("/textures");
        assert_eq!(ar.input_directory_paths.len(), 1);
    }

    #[test]
    fn remove_input_directory_by_index() {
        let mut ar = AssetReferences {
            input_directory_paths: vec!["/a".to_string(), "/b".to_string()],
            ..Default::default()
        };
        ar.remove_input_directory(0);
        assert_eq!(ar.input_directory_paths, vec!["/b"]);
    }

    #[test]
    fn add_output_directory() {
        let mut ar = AssetReferences::default();
        ar.add_output_directory("/output");
        assert_eq!(ar.output_directory_paths, vec!["/output"]);
    }

    #[test]
    fn remove_output_directory_by_index() {
        let mut ar = AssetReferences {
            output_directory_paths: vec!["/a".to_string(), "/b".to_string()],
            ..Default::default()
        };
        ar.remove_output_directory(1);
        assert_eq!(ar.output_directory_paths, vec!["/a"]);
    }

    #[test]
    fn merge_attachments_combines_both() {
        let auto = AssetReferences {
            input_file_paths: vec!["/auto/scene.ma".to_string()],
            input_directory_paths: vec!["/auto/textures".to_string()],
            output_directory_paths: vec![],
        };
        let user = AssetReferences {
            input_file_paths: vec!["/user/extra.exr".to_string()],
            input_directory_paths: vec![],
            output_directory_paths: vec!["/user/output".to_string()],
        };
        let merged = merge_attachments(&auto, &user);
        assert_eq!(merged.input_file_paths.len(), 2);
        assert_eq!(merged.input_directory_paths.len(), 1);
        assert_eq!(merged.output_directory_paths.len(), 1);
    }

    #[test]
    fn merge_attachments_deduplicates() {
        let auto = AssetReferences {
            input_file_paths: vec!["/shared.exr".to_string()],
            ..Default::default()
        };
        let user = AssetReferences {
            input_file_paths: vec!["/shared.exr".to_string(), "/unique.exr".to_string()],
            ..Default::default()
        };
        let merged = merge_attachments(&auto, &user);
        assert_eq!(merged.input_file_paths.len(), 2);
    }
}
