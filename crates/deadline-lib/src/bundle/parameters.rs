use crate::bundle::loader::read_yaml_or_json_object;
use crate::bundle::submission::AssetReferences;
use crate::api::errors::DeadlineError;
use std::collections::{HashMap, HashSet};
use std::path::Path;

const VALID_TYPES: &[&str] = &["STRING", "PATH", "INT", "FLOAT"];
const VALID_UI_CONTROLS: &[&str] = &[
    "CHECK_BOX",
    "CHOOSE_DIRECTORY",
    "CHOOSE_INPUT_FILE",
    "CHOOSE_OUTPUT_FILE",
    "DROPDOWN_LIST",
    "LINE_EDIT",
    "MULTILINE_EDIT",
    "SPIN_BOX",
    "HIDDEN",
];
const VALID_DATA_FLOWS: &[&str] = &["NONE", "IN", "OUT", "INOUT"];

/// Shorthand for the most common error variant.
fn op_err(msg: String) -> DeadlineError {
    DeadlineError::OperationError(msg)
}

fn json_type_name(v: &serde_json::Value) -> &'static str {
    match v {
        serde_json::Value::Null => "NoneType",
        serde_json::Value::Bool(_) => "bool",
        serde_json::Value::Number(_) => "int",
        serde_json::Value::String(_) => "str",
        serde_json::Value::Array(_) => "list",
        serde_json::Value::Object(_) => "dict",
    }
}

pub fn validate_job_parameter(
    input: &serde_json::Value,
    type_required: bool,
    default_required: bool,
) -> Result<(), DeadlineError> {
    let obj = input.as_object().ok_or_else(|| {
        op_err(format!(
            "Expected a dict for job parameter, but got {}",
            json_type_name(input)
        ))
    })?;

    // name
    let name = match obj.get("name") {
        None => {
            return Err(op_err(format!(
                "No \"name\" field in job parameter. Got {input}"
            )));
        }
        Some(v) => v.as_str().ok_or_else(|| {
            op_err(format!(
                "Job parameter had {} for \"name\" but expected str",
                json_type_name(v)
            ))
        })?,
    };
    if name.is_empty() {
        return Err(DeadlineError::OperationError(
            "Job parameter has an empty name".into(),
        ));
    }

    // description
    if let Some(v) = obj.get("description")
        && !v.is_string()
    {
        return Err(op_err(format!(
            "Job parameter \"{name}\" had {} for \"description\" but expected str",
            json_type_name(v)
        )));
    }

    // type
    if let Some(v) = obj.get("type") {
        let t = v.as_str().unwrap_or("");
        if !VALID_TYPES.contains(&t) {
            let quoted: Vec<_> = VALID_TYPES.iter().map(|t| format!("\"{t}\"")).collect();
            return Err(op_err(format!(
                "Job parameter \"{name}\" had \"type\" {t} but expected one of ({})",
                quoted.join(", ")
            )));
        }
    } else if type_required {
        return Err(op_err(format!(
            "Job parameter \"{name}\" is missing required key \"type\""
        )));
    }

    // default
    if let Some(v) = obj.get("default") {
        if v.is_null() {
            return Err(op_err(format!(
                "Job parameter \"{name}\" had None for \"default\" but expected a value"
            )));
        }
    } else if default_required {
        return Err(op_err(format!(
            "Job parameter \"{name}\" is missing required key \"default\""
        )));
    }

    // allowedValues
    if let Some(v) = obj.get("allowedValues")
        && !v.is_array()
    {
        return Err(op_err(format!(
            "Job parameter \"{name}\" got {} for \"allowedValues\" but expected list",
            json_type_name(v)
        )));
    }

    // dataFlow
    if let Some(v) = obj.get("dataFlow") {
        let df = v.as_str().unwrap_or("");
        if !VALID_DATA_FLOWS.contains(&df) {
            return Err(op_err(format!(
                "Job parameter \"{name}\" got \"{df}\" for \"dataFlow\" but expected one of (\"NONE\", \"IN\", \"OUT\", \"INOUT\")"
            )));
        }
    }

    // minLength / maxLength
    for field in &["minLength", "maxLength"] {
        if let Some(v) = obj.get(*field) {
            let n = v.as_i64().ok_or_else(|| {
                op_err(format!(
                    "Job parameter \"{name}\" got {} for \"{field}\" but expected int",
                    json_type_name(v)
                ))
            })?;
            if n < 0 {
                return Err(op_err(format!(
                    "Job parameter \"{name}\" got {n} for \"{field}\" but the value must be non-negative"
                )));
            }
        }
    }

    // minValue / maxValue
    for field in &["minValue", "maxValue"] {
        if let Some(v) = obj.get(*field) {
            if let Some(s) = v.as_str() {
                s.parse::<f64>().map_err(|_| op_err(format!(
                    "Job parameter \"{name}\" has a non-numeric string value for \"{field}\": {s}"
                )))?;
            } else if v.is_boolean() {
                return Err(op_err(format!(
                    "Job parameter \"{name}\" got bool for \"{field}\" but expected int"
                )));
            } else if !v.is_number() {
                return Err(op_err(format!(
                    "Job parameter \"{name}\" got {} for \"{field}\" but expected int",
                    json_type_name(v)
                )));
            }
        }
    }

    // objectType
    if let Some(v) = obj.get("objectType") {
        let ot = v.as_str().unwrap_or("");
        if ot != "FILE" && ot != "DIRECTORY" {
            return Err(op_err(format!(
                "Job parameter \"{name}\" got {ot} for \"objectType\" but expected one of (\"FILE\", \"DIRECTORY\")"
            )));
        }
    }

    // userInterface
    if let Some(v) = obj.get("userInterface") {
        validate_user_interface_spec(v, name)?;
    }

    Ok(())
}

pub fn validate_user_interface_spec(
    input: &serde_json::Value,
    parameter_name: &str,
) -> Result<(), DeadlineError> {
    let obj = input
        .as_object()
        .ok_or_else(|| op_err(format!("Expected a dict but got {}", json_type_name(input))))?;

    if let Some(v) = obj.get("control") {
        let c = v.as_str().unwrap_or("");
        if !VALID_UI_CONTROLS.contains(&c) {
            let quoted: Vec<_> = VALID_UI_CONTROLS
                .iter()
                .map(|c| format!("\"{c}\""))
                .collect();
            return Err(op_err(format!(
                "Job parameter \"{parameter_name}\" got but expected one of ({}) for \"userInterface\" -> \"control\" but got {c}",
                quoted.join(", ")
            )));
        }
    }

    for field in &["label", "groupLabel"] {
        if let Some(v) = obj.get(*field)
            && !v.is_string()
        {
            return Err(op_err(format!(
                "Job parameter \"{parameter_name}\" got {} for \"userInterface\" -> \"{field}\" but expected str",
                json_type_name(v)
            )));
        }
    }

    if let Some(v) = obj.get("decimals") {
        let n = v.as_i64().ok_or_else(|| op_err(format!(
            "Job parameter \"{parameter_name}\" got {} for \"userInterface\" -> \"decimals\" but expected int",
            json_type_name(v)
        )))?;
        if n < 0 {
            return Err(op_err(format!(
                "Job parameter \"{parameter_name}\" got {n} for \"userInterface\" -> \"decimals\" but expected a non-negative int"
            )));
        }
    }

    if let Some(v) = obj.get("singleStepDelta") {
        let n = v.as_f64().ok_or_else(|| op_err(format!(
            "Job parameter \"{parameter_name}\" got but expected float for \"userInterface\" -> \"singleStepDelta\", but got {}",
            json_type_name(v)
        )))?;
        if n <= 0.0 {
            return Err(op_err(format!(
                "Job parameter \"{parameter_name}\" got {v} for \"userInterface\" -> \"singleStepDelta\" but expected a positive number"
            )));
        }
    }

    if let Some(v) = obj.get("fileFilters") {
        let arr = v.as_array().ok_or_else(|| op_err(format!(
            "Job parameter \"{parameter_name}\" got but expected list for \"userInterface\" -> \"fileFilters\", but got {}",
            json_type_name(v)
        )))?;
        for (i, ff) in arr.iter().enumerate() {
            validate_user_interface_file_filter(
                ff,
                parameter_name,
                &format!("\"userInterface\" -> \"fileFilters\" -> [{i}]"),
            )?;
        }
    }

    if let Some(v) = obj.get("fileFilterDefault") {
        validate_user_interface_file_filter(
            v,
            parameter_name,
            "\"userInterface\" -> \"fileFilterDefault\"",
        )?;
    }

    Ok(())
}

pub fn validate_user_interface_file_filter(
    input: &serde_json::Value,
    parameter_name: &str,
    field_path: &str,
) -> Result<(), DeadlineError> {
    let obj = input.as_object().ok_or_else(|| {
        op_err(format!(
            "Job parameter \"{parameter_name}\" got {} for {field_path} but expected a dict",
            json_type_name(input)
        ))
    })?;

    match obj.get("label") {
        None => {
            return Err(op_err(format!(
                "Job parameter \"{parameter_name}\" is missing required key {field_path} -> \"label\""
            )));
        }
        Some(v) if !v.is_string() => {
            return Err(op_err(format!(
                "Job parameter \"{parameter_name}\" got {} for {field_path} -> \"label\" but expected str",
                json_type_name(v)
            )));
        }
        _ => {}
    }

    match obj.get("patterns") {
        None => {
            return Err(op_err(format!(
                "Job parameter \"{parameter_name}\" is missing required key {field_path} -> \"patterns\""
            )));
        }
        Some(v) => {
            let arr = v.as_array().ok_or_else(|| op_err(format!(
                "Job parameter \"{parameter_name}\" got {} for {field_path} -> \"patterns\" but expected list",
                json_type_name(v)
            )))?;
            for (i, pat) in arr.iter().enumerate() {
                let s = pat.as_str().ok_or_else(|| op_err(format!(
                    "Job parameter \"{parameter_name}\" got \"{pat}\" for {field_path} -> \"patterns\" [{i}] but expected str"
                )))?;
                if s.is_empty() || s.len() > 20 {
                    return Err(op_err(format!(
                        "Job parameter \"{parameter_name}\" got \"{s}\" for {field_path} -> \"patterns\" [{i}] but must be between 1 and 20 characters"
                    )));
                }
            }
        }
    }
    Ok(())
}

pub fn validate_job_parameter_value(
    param: &serde_json::Value,
    value: &serde_json::Value,
) -> Result<serde_json::Value, DeadlineError> {
    let name = param["name"].as_str().unwrap_or("<unnamed>");
    let ptype = param["type"].as_str().ok_or_else(|| {
        op_err(format!(
            "The definition for job parameter '{name}' has unsupported type {:?}",
            param.get("type")
        ))
    })?;

    let coerced = match ptype {
        "STRING" | "PATH" => {
            if !value.is_string() {
                return Err(op_err(format!(
                    "Job parameter '{name}' has type {ptype} but got value {value:?} of type {}.",
                    json_type_name(value)
                )));
            }
            value.clone()
        }
        "INT" => {
            if let Some(s) = value.as_str() {
                let i: i64 = s.parse().map_err(|_| op_err(format!(
                    "Job parameter '{name}' has type INT but got value {value:?} which is not an integer."
                )))?;
                serde_json::json!(i)
            } else if let Some(n) = value.as_f64() {
                let i = n as i64;
                // Exact comparison is intentional: we're checking whether the
                // f64 value is a whole number by round-tripping through i64.
                // For integers within f64's 53-bit mantissa range this is exact.
                #[allow(
                    clippy::float_cmp,
                    reason = "integer round-trip check is exact for values < 2^53"
                )]
                if (i as f64) != n {
                    return Err(op_err(format!(
                        "Job parameter '{name}' has type INT but got value {value:?} which is not an integer."
                    )));
                }
                serde_json::json!(i)
            } else {
                return Err(op_err(format!(
                    "Job parameter '{name}' has type INT but got value {value:?} which is not an integer."
                )));
            }
        }
        "FLOAT" => {
            if let Some(s) = value.as_str() {
                let f: f64 = s.parse().map_err(|_| op_err(format!(
                    "Job parameter '{name}' has type FLOAT but got value {value:?} which is not floating point."
                )))?;
                serde_json::json!(f)
            } else if value.is_number() {
                serde_json::json!(value.as_f64().expect("checked is_number"))
            } else {
                return Err(op_err(format!(
                    "Job parameter '{name}' has type FLOAT but got value {value:?} which is not floating point."
                )));
            }
        }
        _ => {
            return Err(op_err(format!(
                "The definition for job parameter '{name}' has unsupported type '{ptype}'"
            )));
        }
    };

    // Constraint checks
    if let Some(min_len) = param.get("minLength").and_then(serde_json::Value::as_i64)
        && let Some(s) = coerced.as_str()
        && (s.len() as i64) < min_len
    {
        return Err(op_err(format!(
            "Job parameter '{name}' value {coerced:?} is shorter than minLength {min_len}."
        )));
    }
    if let Some(max_len) = param.get("maxLength").and_then(serde_json::Value::as_i64)
        && let Some(s) = coerced.as_str()
        && (s.len() as i64) > max_len
    {
        return Err(op_err(format!(
            "Job parameter '{name}' value {coerced:?} is longer than maxLength {max_len}."
        )));
    }
    if let Some(min_val) = param.get("minValue").and_then(serde_json::Value::as_f64)
        && let Some(n) = coerced.as_f64()
        && n < min_val
    {
        return Err(op_err(format!(
            "Job parameter '{name}' value {coerced:?} is less than minValue {}.",
            param["minValue"]
        )));
    }
    if let Some(max_val) = param.get("maxValue").and_then(serde_json::Value::as_f64)
        && let Some(n) = coerced.as_f64()
        && n > max_val
    {
        return Err(op_err(format!(
            "Job parameter '{name}' value {coerced:?} is greater than maxValue {}.",
            param["maxValue"]
        )));
    }
    if let Some(allowed) = param.get("allowedValues").and_then(|v| v.as_array())
        && !allowed.contains(&coerced)
    {
        return Err(op_err(format!(
            "Job parameter '{name}' value {coerced:?} is not an allowed value from {allowed:?}."
        )));
    }

    Ok(coerced)
}

pub fn read_job_bundle_parameters(
    bundle_dir: &Path,
) -> Result<Vec<serde_json::Value>, DeadlineError> {
    let template =
        read_yaml_or_json_object(bundle_dir, "template", true)?.unwrap_or(serde_json::Value::Null);
    let param_values = read_yaml_or_json_object(bundle_dir, "parameter_values", false)?;

    let bundle_dir_display = bundle_dir.display();
    let obj = template.as_object().ok_or_else(|| op_err(format!(
        "Job Template for job bundle {bundle_dir_display}:\nThe document does not contain a top-level object."
    )))?;

    let spec = obj.get("specificationVersion").and_then(|v| v.as_str()).ok_or_else(|| {
        op_err(format!(
            "Job Template for job bundle {bundle_dir_display}:\nDocument does not contain a specificationVersion."
        ))
    })?;
    if spec != "jobtemplate-2023-09" {
        return Err(op_err(format!(
            "Job Template for job bundle {bundle_dir_display}:\nDocument has an unsupported specificationVersion: {spec}"
        )));
    }

    // Build parameter map from template definitions
    let mut params: serde_json::Map<String, serde_json::Value> = serde_json::Map::new();
    if let Some(defs) = obj.get("parameterDefinitions") {
        let arr = defs.as_array().ok_or_else(|| {
            op_err(format!(
                "Job Template for job bundle {bundle_dir_display}:\nJob parameter definitions must be a list."
            ))
        })?;
        for def in arr {
            let name = def["name"].as_str().unwrap_or("").to_owned();
            params.insert(name, def.clone());
        }
    }

    // Merge parameter values
    if let Some(pv) = &param_values
        && let Some(arr) = pv
            .get("parameterValues")
            .and_then(|v: &serde_json::Value| v.as_array())
    {
        for entry in arr {
            let name = entry["name"].as_str().unwrap_or("").to_owned();
            if let Some(existing) = params.get_mut(&name) {
                existing
                    .as_object_mut()
                    .expect("value is object")
                    .insert("value".into(), entry["value"].clone());
            } else {
                params.insert(name, entry.clone());
            }
        }
    }

    // Resolve PATH defaults
    for (name, param) in &mut params {
        let is_path = param.get("type").and_then(|v| v.as_str()) == Some("PATH");
        let has_value = param.get("value").is_some();
        let has_allowed = param.get("allowedValues").is_some();
        if is_path
            && !has_value
            && !has_allowed
            && let Some(default) = param
                .get("default")
                .and_then(|v| v.as_str())
                .map(String::from)
            && !default.is_empty()
        {
            if Path::new(&default).is_absolute() {
                return Err(op_err(format!(
                    "Job Template for job bundle {bundle_dir_display}:\nDefault PATH '{default}' for parameter '{name}' is absolute.\nPATH values must be relative, and must resolve within the Job Bundle directory."
                )));
            }
            let bundle_real = std::fs::canonicalize(bundle_dir)
                .unwrap_or_else(|_| bundle_dir.to_path_buf());
            let joined = bundle_real.join(&default);
            // Use canonicalize if path exists, otherwise normalize manually
            let default_real = std::fs::canonicalize(&joined).unwrap_or_else(|_| {
                // Manual normalization for non-existent paths
                let mut parts = Vec::new();
                for c in joined.components() {
                    match c {
                        std::path::Component::ParentDir => {
                            parts.pop();
                        }
                        std::path::Component::CurDir => {}
                        _ => parts.push(c),
                    }
                }
                parts.iter().collect()
            });
            if !default_real.starts_with(&bundle_real) {
                return Err(op_err(format!(
                    "Job Template for job bundle {bundle_dir_display}:\nDefault PATH '{}' for parameter '{name}' specifies files outside of Job Bundle directory '{}'.\nPATH values must be relative, and must resolve within the Job Bundle directory.",
                    default_real.display(),
                    bundle_real.display()
                )));
            }
            let abs = std::path::absolute(bundle_dir.join(&default))
                .unwrap_or_else(|_| bundle_dir.join(&default));
            let normalized = abs.to_string_lossy().into_owned();
            param
                .as_object_mut()
                .expect("value is object")
                .insert("value".into(), serde_json::json!(normalized));
        }
    }

    // Validate and collect
    let parameters: Vec<serde_json::Value> = params
        .into_iter()
        .map(|(name, mut v)| {
            if v.get("name").is_none() {
                v.as_object_mut()
                    .expect("value is object")
                    .insert("name".into(), serde_json::json!(name));
            }
            let _ = validate_job_parameter(&v, false, false);
            v
        })
        .collect();

    // Validate hidden parameters
    let mut invalid = Vec::new();
    for p in &parameters {
        let is_hidden = p
            .get("userInterface")
            .and_then(|ui| ui.get("control"))
            .and_then(|c| c.as_str())
            == Some("HIDDEN");
        if is_hidden && p.get("value").is_none() && p.get("default").is_none() {
            invalid.push(p["name"].as_str().unwrap_or("").to_owned());
        }
    }
    if !invalid.is_empty() {
        let msg = if invalid.len() == 1 {
            format!(
                "Job bundle validation failed:\nHidden parameter \"{}\" is missing a value.",
                invalid[0]
            )
        } else {
            let list = invalid
                .iter()
                .map(|n| format!("\"{n}\""))
                .collect::<Vec<_>>()
                .join(", ");
            format!("Job bundle validation failed:\nHidden parameters {list} are missing values.")
        };
        return Err(op_err(format!(
            "{msg} Hidden parameters must have either a default value in the template or a value in parameter_values.yaml."
        )));
    }

    Ok(parameters)
}

pub fn apply_job_parameters(
    job_params: &[serde_json::Value],
    job_bundle_dir: &Path,
    parameters: &mut [serde_json::Value],
    asset_references: &mut AssetReferences,
) -> Result<(), DeadlineError> {
    let param_dict: HashMap<String, serde_json::Value> = job_params
        .iter()
        .filter_map(|p| {
            let name = p["name"].as_str()?.to_owned();
            let value = p.get("value")?.clone();
            Some((name, value))
        })
        .collect();

    for param in parameters.iter_mut() {
        let ptype = match param.get("type").and_then(|v| v.as_str()) {
            Some(t) => t.to_owned(),
            None => continue,
        };
        let name = param["name"].as_str().unwrap_or("").to_owned();

        let param_value = if let Some(v) = param_dict.get(&name) {
            if ptype == "PATH" && param.get("allowedValues").is_none() {
                if v.as_str() == Some("") {
                    continue;
                }
                let abs = std::env::current_dir()
                    .unwrap_or_default()
                    .join(v.as_str().unwrap_or(""));
                let abs_str = std::fs::canonicalize(&abs)
                    .unwrap_or(abs)
                    .to_string_lossy()
                    .into_owned();
                param
                    .as_object_mut()
                    .expect("value is object")
                    .insert("value".into(), serde_json::json!(abs_str));
                abs_str
            } else {
                param
                    .as_object_mut()
                    .expect("value is object")
                    .insert("value".into(), v.clone());
                v.as_str().unwrap_or("").to_owned()
            }
        } else {
            let v = param.get("value").or_else(|| param.get("default"));
            match v.and_then(|v| v.as_str()) {
                Some(s) => s.to_owned(),
                None => {
                    return Err(op_err(format!(
                        "Job Template for job bundle {}:\nNo parameter value provided for Job Template parameter {name}, and it has no default value.",
                        job_bundle_dir.display()
                    )));
                }
            }
        };

        if ptype == "PATH" {
            let data_flow = param
                .get("dataFlow")
                .and_then(|v| v.as_str())
                .unwrap_or("NONE");
            if !VALID_DATA_FLOWS.contains(&data_flow) {
                return Err(op_err(format!(
                    "Job Template for job bundle {}:\nJob Template parameter {name} had an incorrect value {data_flow} for 'dataFlow'. Valid values are ['NONE', 'IN', 'OUT', 'INOUT']",
                    job_bundle_dir.display()
                )));
            }
            if data_flow == "NONE" {
                asset_references.referenced_paths.insert(param_value);
            } else if !param_value.is_empty() {
                let obj_type = param.get("objectType").and_then(|v| v.as_str());
                if data_flow.contains("IN") {
                    if obj_type == Some("FILE") {
                        asset_references.input_filenames.insert(param_value.clone());
                    } else {
                        asset_references
                            .input_directories
                            .insert(param_value.clone());
                    }
                }
                if data_flow.contains("OUT") {
                    if obj_type == Some("FILE") {
                        if let Some(parent) = Path::new(&param_value).parent() {
                            asset_references
                                .output_directories
                                .insert(parent.to_string_lossy().into_owned());
                        }
                    } else {
                        asset_references
                            .output_directories
                            .insert(param_value.clone());
                    }
                }
            }
        }
    }
    Ok(())
}

pub fn merge_queue_job_parameters(
    job_params: &[serde_json::Value],
    queue_params: &[serde_json::Value],
    queue_id: Option<&str>,
) -> Result<Vec<serde_json::Value>, DeadlineError> {
    let mut collected: serde_json::Map<String, serde_json::Value> = queue_params
        .iter()
        .filter_map(|p| Some((p["name"].as_str()?.to_owned(), p.clone())))
        .collect();

    let mut mismatches: Vec<(String, Vec<String>)> = Vec::new();

    for jp in job_params {
        let name = jp["name"].as_str().unwrap_or("").to_owned();
        if let Some(existing) = collected.get_mut(&name) {
            // Copy value if present
            if let Some(v) = jp.get("value") {
                existing
                    .as_object_mut()
                    .expect("value is object")
                    .insert("value".into(), v.clone());
                // Value-only parameter — nothing more to merge
                let keys: HashSet<&str> = jp
                    .as_object()
                    .map(|o| o.keys().map(String::as_str).collect())
                    .unwrap_or_default();
                if keys == ["name", "value"].into_iter().collect() {
                    continue;
                }
            }
            // Copy default if present
            if let Some(d) = jp.get("default") {
                existing
                    .as_object_mut()
                    .expect("value is object")
                    .insert("default".into(), d.clone());
            }
            let diffs = parameter_definition_difference(existing, jp, false);
            let diffs: Vec<String> = diffs.into_iter().filter(|d| d != "default").collect();
            if !diffs.is_empty() {
                mismatches.push((name, diffs));
            }
        } else {
            let keys: HashSet<&str> = jp
                .as_object()
                .map(|o| o.keys().map(String::as_str).collect())
                .unwrap_or_default();
            if keys == ["name", "value"].into_iter().collect() && !name.contains(':') {
                return Err(op_err(format!(
                    "Parameter value was provided for an undefined parameter \"{name}\""
                )));
            }
            collected.insert(name, jp.clone());
        }
    }

    if !mismatches.is_empty() {
        let lines: Vec<String> = mismatches
            .iter()
            .map(|(n, d)| format!("\t{n}: differences for fields \"{d:?}\""))
            .collect();
        let queue_str = queue_id.map_or_else(|| "queue".into(), |id| format!("queue ({id})"));
        return Err(op_err(format!(
            "The target {queue_str} and job bundle have conflicting parameter definitions:\n\n{}",
            lines.join("\n")
        )));
    }

    Ok(collected.into_values().collect())
}

pub fn get_ui_control_for_parameter_definition(
    param: &serde_json::Value,
) -> Result<String, DeadlineError> {
    let name = param["name"].as_str().unwrap_or("<unnamed>");
    let ptype = param["type"].as_str().unwrap_or("");
    let explicit = param
        .get("userInterface")
        .and_then(|ui| ui.get("control"))
        .and_then(|c| c.as_str());
    let has_allowed = param.get("allowedValues").is_some();

    let control = if let Some(c) = explicit {
        c.to_owned()
    } else if has_allowed {
        "DROPDOWN_LIST".into()
    } else {
        match ptype {
            "STRING" => return Ok("LINE_EDIT".into()),
            "PATH" => {
                let obj_type = param
                    .get("objectType")
                    .and_then(|v| v.as_str())
                    .unwrap_or("DIRECTORY");
                return Ok(if obj_type == "FILE" {
                    let df = param
                        .get("dataFlow")
                        .and_then(|v| v.as_str())
                        .unwrap_or("NONE");
                    if df == "OUT" {
                        "CHOOSE_OUTPUT_FILE"
                    } else {
                        "CHOOSE_INPUT_FILE"
                    }
                } else {
                    "CHOOSE_DIRECTORY"
                }
                .into());
            }
            "INT" | "FLOAT" => return Ok("SPIN_BOX".into()),
            _ => {
                return Err(op_err(format!(
                    "The job template parameter '{name}' specifies an unsupported type '{ptype}'."
                )));
            }
        }
    };

    // Validate control is supported for type
    let supported = match ptype {
        "STRING" => &[
            "LINE_EDIT",
            "MULTILINE_EDIT",
            "DROPDOWN_LIST",
            "CHECK_BOX",
            "HIDDEN",
        ][..],
        "PATH" => &[
            "CHOOSE_INPUT_FILE",
            "CHOOSE_OUTPUT_FILE",
            "CHOOSE_DIRECTORY",
            "DROPDOWN_LIST",
            "HIDDEN",
        ],
        "INT" | "FLOAT" => &["SPIN_BOX", "DROPDOWN_LIST", "HIDDEN"],
        _ => {
            return Err(op_err(format!(
                "The job template parameter '{name}' specifies an unsupported type '{ptype}'."
            )));
        }
    };
    if !supported.contains(&control.as_str()) {
        return Err(op_err(format!(
            "The job template parameter '{name}' specifies an unsupported control '{control}' for its type '{ptype}'."
        )));
    }
    if control == "DROPDOWN_LIST" && !has_allowed {
        return Err(op_err(format!(
            "The job template parameter '{name}' must supply 'allowedValues' if it uses a DROPDOWN_LIST control."
        )));
    }

    Ok(control)
}

pub fn parameter_definition_difference(
    lhs: &serde_json::Value,
    rhs: &serde_json::Value,
    ignore_missing: bool,
) -> Vec<String> {
    let mut diffs = Vec::new();
    for field in &[
        "name",
        "type",
        "minValue",
        "maxValue",
        "minLength",
        "maxLength",
        "dataFlow",
        "objectType",
    ] {
        if ignore_missing && (lhs.get(*field).is_none() || rhs.get(*field).is_none()) {
            continue;
        }
        if lhs.get(*field) != rhs.get(*field) {
            diffs.push(field.to_string());
        }
    }
    // allowedValues compared as sets
    let lav = lhs.get("allowedValues").and_then(|v| v.as_array());
    let rav = rhs.get("allowedValues").and_then(|v| v.as_array());
    if ignore_missing && (lav.is_none() || rav.is_none()) {
        // skip
    } else {
        let sets_equal = match (lav, rav) {
            (Some(l), Some(r)) => {
                let ls: HashSet<&serde_json::Value> = l.iter().collect();
                let rs: HashSet<&serde_json::Value> = r.iter().collect();
                ls == rs
            }
            (None, None) => true,
            _ => false,
        };
        if !sets_equal {
            diffs.push("allowedValues".into());
        }
    }
    diffs
}
