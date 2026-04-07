use deadline_models::errors::DeadlineError;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;

#[derive(Debug, Clone, Default)]
pub struct AssetReferences {
    pub input_filenames: BTreeSet<String>,
    pub input_directories: BTreeSet<String>,
    pub output_directories: BTreeSet<String>,
    pub referenced_paths: BTreeSet<String>,
}

impl AssetReferences {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_non_empty(&self) -> bool {
        !self.input_filenames.is_empty()
            || !self.input_directories.is_empty()
            || !self.output_directories.is_empty()
            || !self.referenced_paths.is_empty()
    }

    #[must_use]
    pub fn union(&self, other: &AssetReferences) -> AssetReferences {
        AssetReferences {
            input_filenames: &self.input_filenames | &other.input_filenames,
            input_directories: &self.input_directories | &other.input_directories,
            output_directories: &self.output_directories | &other.output_directories,
            referenced_paths: &self.referenced_paths | &other.referenced_paths,
        }
    }

    pub fn from_dict(obj: Option<&serde_json::Value>) -> Self {
        let Some(obj) = obj else { return Self::new() };
        let ar = &obj["assetReferences"];
        let extract = |parent: &serde_json::Value, key: &str| -> BTreeSet<String> {
            parent[key]
                .as_array()
                .map(|a| a.iter().filter_map(|v| v.as_str()).map(normalize_path).collect())
                .unwrap_or_default()
        };
        let inputs = &ar["inputs"];
        AssetReferences {
            input_filenames: extract(inputs, "filenames"),
            input_directories: extract(inputs, "directories"),
            output_directories: extract(&ar["outputs"], "directories"),
            referenced_paths: extract(ar, "referencedPaths"),
        }
    }

    #[must_use]
    pub fn to_dict(&self) -> serde_json::Value {
        serde_json::json!({
            "assetReferences": {
                "inputs": {
                    "directories": self.input_directories.iter().collect::<Vec<_>>(),
                    "filenames": self.input_filenames.iter().collect::<Vec<_>>(),
                },
                "outputs": {
                    "directories": self.output_directories.iter().collect::<Vec<_>>(),
                },
                "referencedPaths": self.referenced_paths.iter().collect::<Vec<_>>(),
            }
        })
    }
}

/// Normalize a path: resolve `.` and `..` components without touching the filesystem.
pub fn normalize_path(s: &str) -> String {
    let mut parts: Vec<std::path::Component> = Vec::new();
    for c in Path::new(s).components() {
        match c {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                if matches!(parts.last(), Some(std::path::Component::Normal(_))) {
                    parts.pop();
                } else {
                    parts.push(c);
                }
            }
            _ => parts.push(c),
        }
    }
    if parts.is_empty() {
        ".".into()
    } else {
        parts.iter().collect::<PathBuf>().to_string_lossy().into_owned()
    }
}

const DEFAULT_APP_NAME: &str = "deadline";
const DEFAULT_SUPPORTED_APP_PARAMETER_NAMES: &[&str] = &[
    "targetTaskRunStatus",
    "priority",
    "maxFailedTasksCount",
    "maxRetriesPerTask",
    "maxWorkerCount",
];

#[allow(clippy::type_complexity)]
pub fn split_parameter_args(
    parameters: &[serde_json::Value],
    job_bundle_dir: &str,
    app_name: Option<&str>,
    supported_app_parameter_names: Option<&[&str]>,
) -> Result<
    (
        serde_json::Map<String, serde_json::Value>,
        serde_json::Map<String, serde_json::Value>,
    ),
    DeadlineError,
> {
    let app_name = app_name.unwrap_or(DEFAULT_APP_NAME);
    let supported = supported_app_parameter_names.unwrap_or(DEFAULT_SUPPORTED_APP_PARAMETER_NAMES);
    let prefix = format!("{app_name}:");

    let mut app_parameters = serde_json::Map::new();
    let mut job_parameters = serde_json::Map::new();

    for param in parameters {
        let Some(value) = param.get("value") else { continue };
        let Some(name) = param["name"].as_str() else { continue };

        if let Some(app_param) = name.strip_prefix(&prefix) {
            if supported.contains(&app_param) {
                app_parameters.insert(app_param.into(), value.clone());
            } else {
                return Err(op_err(format!(
                    "Unrecognized parameter named '{name}' from job bundle:\n{job_bundle_dir}"
                )));
            }
        } else if name.contains(':') {
            // Other app prefix — silently drop
        } else {
            let ptype = param.get("type").and_then(|t| t.as_str()).unwrap_or("STRING").to_lowercase();
            let val_str = match value {
                serde_json::Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            job_parameters.insert(name.into(), serde_json::json!({ ptype: val_str }));
        }
    }
    Ok((app_parameters, job_parameters))
}

static FRAME_RANGE_RE: LazyLock<regex::Regex> = LazyLock::new(|| {
    regex::Regex::new(r"^(?P<start>-?\d+)(-(?P<stop>-?\d+)(:(?P<step>-?\d+))?)?$").unwrap()
});

pub fn parse_frame_range(frame_string: &str) -> Result<Vec<i64>, DeadlineError> {
    let caps = FRAME_RANGE_RE
        .captures(frame_string)
        .ok_or_else(|| op_err("Framelist not valid".into()))?;

    let start: i64 = caps["start"].parse().unwrap();
    let stop: i64 = caps.name("stop").map(|m| m.as_str().parse::<i64>().unwrap()).unwrap_or(start);
    let step: i64 = caps.name("step").map(|m| m.as_str().parse::<i64>().unwrap())
        .unwrap_or(if start <= stop { 1 } else { -1 });

    if step == 0 {
        return Err(op_err("Frame step cannot be zero".into()));
    }

    let mut frames = Vec::new();
    let mut cur = start;
    if step > 0 {
        while cur <= stop { frames.push(cur); cur += step; }
    } else {
        while cur >= stop { frames.push(cur); cur += step; }
    }
    Ok(frames)
}

/// Shorthand for the most common error variant.
fn op_err(msg: String) -> DeadlineError {
    DeadlineError::OperationError(msg)
}
