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
    pub referenced_paths: Vec<String>,
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
            referenced_paths: extract("referencedPaths"),
        })
    }

    /// Parse from the nested bundle format used in asset_references.json/yaml files.
    /// Format: `{"assetReferences": {"inputs": {"filenames": [...], "directories": [...]}, "outputs": {"directories": [...]}, "referencedPaths": [...]}}`
    pub fn from_bundle_json(json: &serde_json::Value) -> Result<Self, String> {
        let ar = json.get("assetReferences").unwrap_or(json);
        let inputs = ar.get("inputs").unwrap_or(&serde_json::Value::Null);
        let outputs = ar.get("outputs").unwrap_or(&serde_json::Value::Null);

        let extract_strings = |v: &serde_json::Value| -> Vec<String> {
            v.as_array()
                .map(|arr| {
                    arr.iter()
                        .filter_map(|s| s.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default()
        };

        Ok(Self {
            input_file_paths: extract_strings(
                inputs.get("filenames").unwrap_or(&serde_json::Value::Null),
            ),
            input_directory_paths: extract_strings(
                inputs
                    .get("directories")
                    .unwrap_or(&serde_json::Value::Null),
            ),
            output_directory_paths: extract_strings(
                outputs
                    .get("directories")
                    .unwrap_or(&serde_json::Value::Null),
            ),
            referenced_paths: extract_strings(
                ar.get("referencedPaths")
                    .unwrap_or(&serde_json::Value::Null),
            ),
        })
    }

    /// Serialize to the nested bundle format.
    pub fn to_bundle_json(&self) -> serde_json::Value {
        let mut input_files = self.input_file_paths.clone();
        let mut input_dirs = self.input_directory_paths.clone();
        let mut output_dirs = self.output_directory_paths.clone();
        let mut ref_paths = self.referenced_paths.clone();
        input_files.sort();
        input_dirs.sort();
        output_dirs.sort();
        ref_paths.sort();

        serde_json::json!({
            "assetReferences": {
                "inputs": {
                    "filenames": input_files,
                    "directories": input_dirs,
                },
                "outputs": {
                    "directories": output_dirs,
                },
                "referencedPaths": ref_paths,
            }
        })
    }

    /// Serialize to JSON.
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "inputFilePaths": self.input_file_paths,
            "inputDirectoryPaths": self.input_directory_paths,
            "outputDirectoryPaths": self.output_directory_paths,
            "referencedPaths": self.referenced_paths,
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
    for p in &user.referenced_paths {
        if !merged.referenced_paths.iter().any(|existing| existing == p) {
            merged.referenced_paths.push(p.clone());
        }
    }
    merged
}

/// Tracks auto-detected and user-added attachments separately.
/// Auto-detected items cannot be removed. User-added items dedup against auto.
#[derive(Debug, Clone, Default)]
pub struct AttachmentState {
    pub auto_detected: AssetReferences,
    pub user_added: AssetReferences,
    pub require_paths_exist: bool,
}

impl AttachmentState {
    /// Add an input file. No-op if already in auto_detected.
    pub fn add_input_file(&mut self, path: &str) {
        if !self
            .auto_detected
            .input_file_paths
            .iter()
            .any(|p| p == path)
        {
            self.user_added.add_input_file(path);
        }
    }

    /// Remove a user-added input file by index. Auto-detected items are unaffected.
    pub fn remove_input_file(&mut self, index: usize) {
        self.user_added.remove_input_file(index);
    }

    /// Add an input directory. No-op if already in auto_detected.
    pub fn add_input_dir(&mut self, path: &str) {
        if !self
            .auto_detected
            .input_directory_paths
            .iter()
            .any(|p| p == path)
        {
            self.user_added.add_input_directory(path);
        }
    }

    /// Remove a user-added input directory by index.
    pub fn remove_input_dir(&mut self, index: usize) {
        self.user_added.remove_input_directory(index);
    }

    /// Add an output directory. No-op if already in auto_detected.
    pub fn add_output_dir(&mut self, path: &str) {
        if !self
            .auto_detected
            .output_directory_paths
            .iter()
            .any(|p| p == path)
        {
            self.user_added.add_output_directory(path);
        }
    }

    /// Remove a user-added output directory by index.
    pub fn remove_output_dir(&mut self, index: usize) {
        self.user_added.remove_output_directory(index);
    }

    /// Return the merged union of auto + user (deduplicated).
    pub fn merged(&self) -> AssetReferences {
        merge_attachments(&self.auto_detected, &self.user_added)
    }
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
            ..Default::default()
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
            ..Default::default()
        };
        let user = AssetReferences {
            input_file_paths: vec!["/user/extra.exr".to_string()],
            output_directory_paths: vec!["/user/output".to_string()],
            ..Default::default()
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

    #[test]
    fn asset_references_bundle_json_roundtrip_with_auto_user_state() {
        // 1. Parse the nested bundle format
        let bundle_json = serde_json::json!({
            "assetReferences": {
                "inputs": {
                    "filenames": ["/scene.ma", "/texture.exr"],
                    "directories": ["/textures"]
                },
                "outputs": {
                    "directories": ["/renders"]
                },
                "referencedPaths": ["/reference/file.abc"]
            }
        });
        let parsed = AssetReferences::from_bundle_json(&bundle_json).unwrap();
        assert_eq!(parsed.input_file_paths, vec!["/scene.ma", "/texture.exr"]);
        assert_eq!(parsed.input_directory_paths, vec!["/textures"]);
        assert_eq!(parsed.output_directory_paths, vec!["/renders"]);
        assert_eq!(parsed.referenced_paths, vec!["/reference/file.abc"]);

        // 2. Serialize back to bundle format (paths sorted)
        let serialized = parsed.to_bundle_json();
        let inputs = &serialized["assetReferences"]["inputs"];
        assert_eq!(inputs["filenames"].as_array().unwrap().len(), 2);
        assert_eq!(inputs["directories"].as_array().unwrap().len(), 1);
        assert_eq!(
            serialized["assetReferences"]["outputs"]["directories"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            serialized["assetReferences"]["referencedPaths"]
                .as_array()
                .unwrap()
                .len(),
            1
        );

        // 3. AttachmentState: add to auto-detected is no-op
        let mut state = AttachmentState {
            auto_detected: parsed.clone(),
            ..Default::default()
        };
        assert!(!state.require_paths_exist); // default is false
        state.add_input_file("/scene.ma"); // already in auto → no-op
        assert!(state.user_added.input_file_paths.is_empty());

        // 4. Add unique file → goes to user_added
        state.add_input_file("/new_file.exr");
        assert_eq!(state.user_added.input_file_paths, vec!["/new_file.exr"]);

        // 5. Remove only affects user_added
        state.add_input_dir("/user_dir");
        state.remove_input_dir(0);
        assert!(state.user_added.input_directory_paths.is_empty());
        assert_eq!(state.auto_detected.input_directory_paths, vec!["/textures"]);

        // 6. Merged combines both, deduplicated
        state.add_input_file("/texture.exr"); // already in auto → no-op
        let merged = state.merged();
        // auto has 2 files + user has 1 = 3 total
        assert_eq!(merged.input_file_paths.len(), 3);
        assert_eq!(merged.input_directory_paths, vec!["/textures"]);
        assert_eq!(merged.output_directory_paths, vec!["/renders"]);
    }
}
