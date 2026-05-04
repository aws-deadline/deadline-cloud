//! Submission hooks for job bundles.
//!
//! Pre/post-submission hook framework: external scripts run during
//! `bundle submit`. Pre-hooks can modify the `CreateJob` payload (JSON
//! in/out via stdin/stdout), post-hooks run after job creation
//! (failures only warn).

use deadline_api::errors::DeadlineError;
use serde_json::Value;
use std::collections::HashMap;
use std::io::Write as _;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::Instant;

fn op_err(msg: String) -> DeadlineError {
    DeadlineError::OperationError(msg)
}

// ---------------------------------------------------------------------------
// Models
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct HookDefinition {
    pub command: String,
    pub args: Vec<String>,
    pub timeout: u64,
    pub env: HashMap<String, String>,
}

impl HookDefinition {
    pub fn from_dict(data: &Value) -> Self {
        Self {
            command: data["command"].as_str().unwrap_or("").to_owned(),
            args: data.get("args")
                .and_then(|a| a.as_array())
                .map(|a| a.iter().filter_map(|v| v.as_str().map(String::from)).collect())
                .unwrap_or_default(),
            timeout: data.get("timeout").and_then(Value::as_u64).unwrap_or(60),
            env: data.get("env")
                .and_then(|e| e.as_object())
                .map(|m| m.iter().map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_owned())).collect())
                .unwrap_or_default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct HookConfiguration {
    pub version: String,
    pub pre_submission: Vec<HookDefinition>,
    pub post_submission: Vec<HookDefinition>,
}

impl HookConfiguration {
    pub fn from_dict(data: &Value) -> Self {
        let parse_hooks = |key: &str| -> Vec<HookDefinition> {
            data.get(key)
                .and_then(|v| v.as_array())
                .map(|a| a.iter().map(HookDefinition::from_dict).collect())
                .unwrap_or_default()
        };
        Self {
            version: data.get("version").and_then(|v| v.as_str()).unwrap_or("1.0").to_owned(),
            pre_submission: parse_hooks("preSubmission"),
            post_submission: parse_hooks("postSubmission"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct HookMetadata {
    pub job_name: String,
    pub priority: i32,
    pub farm_id: String,
    pub queue_id: String,
    pub job_bundle_dir: String,
    pub parameters: HashMap<String, Value>,
    pub submitter_name: String,
    pub asset_references: Value,
    pub submission_payload: Value,
    pub storage_profile_id: Option<String>,
    pub job_id: Option<String>,
}

impl HookMetadata {
    pub fn to_dict(&self) -> Value {
        let mut m = serde_json::Map::new();
        m.insert("jobName".into(), Value::String(self.job_name.clone()));
        m.insert("priority".into(), serde_json::json!(self.priority));
        m.insert("farmId".into(), Value::String(self.farm_id.clone()));
        m.insert("queueId".into(), Value::String(self.queue_id.clone()));
        m.insert("jobBundleDir".into(), Value::String(self.job_bundle_dir.clone()));
        m.insert("parameters".into(), serde_json::json!(self.parameters));
        m.insert("submitterName".into(), Value::String(self.submitter_name.clone()));
        m.insert("assetReferences".into(), self.asset_references.clone());
        m.insert("submissionPayload".into(), self.submission_payload.clone());
        if let Some(ref sp) = self.storage_profile_id {
            m.insert("storageProfileId".into(), Value::String(sp.clone()));
        }
        if let Some(ref jid) = self.job_id {
            m.insert("jobId".into(), Value::String(jid.clone()));
        }
        Value::Object(m)
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(&self.to_dict()).unwrap_or_default()
    }

    pub fn to_environment_variables(&self) -> HashMap<String, String> {
        let mut env = HashMap::new();
        env.insert("DEADLINE_JOB_NAME".into(), self.job_name.clone());
        env.insert("DEADLINE_PRIORITY".into(), self.priority.to_string());
        env.insert("DEADLINE_FARM_ID".into(), self.farm_id.clone());
        env.insert("DEADLINE_QUEUE_ID".into(), self.queue_id.clone());
        env.insert("DEADLINE_JOB_BUNDLE_DIR".into(), self.job_bundle_dir.clone());
        if let Some(ref sp) = self.storage_profile_id {
            env.insert("DEADLINE_STORAGE_PROFILE_ID".into(), sp.clone());
        }
        if let Some(ref jid) = self.job_id {
            env.insert("DEADLINE_JOB_ID".into(), jid.clone());
        }
        env
    }
}

#[derive(Debug, Clone)]
pub struct HookResult {
    pub exit_code: i32,
    pub stdout: String,
    pub stderr: String,
    pub execution_time: f64,
    pub timed_out: bool,
}

impl HookResult {
    pub fn is_success(&self) -> bool {
        self.exit_code == 0 && !self.timed_out
    }
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

pub fn validate_configuration(config: &Value) -> Result<(), DeadlineError> {
    let version = config.get("version").and_then(|v| v.as_str()).unwrap_or("1.0");
    if version != "1.0" {
        return Err(op_err(format!(
            "Unsupported hooks version '{version}'. Supported: 1.0"
        )));
    }
    for key in &["preSubmission", "postSubmission"] {
        if let Some(val) = config.get(*key) {
            let arr = val.as_array().ok_or_else(|| {
                op_err(format!("Hook configuration '{key}' must be a list"))
            })?;
            for (i, hook) in arr.iter().enumerate() {
                validate_hook(hook, i, key)?;
            }
        }
    }
    Ok(())
}

fn validate_hook(hook: &Value, index: usize, key: &str) -> Result<(), DeadlineError> {
    if !hook.is_object() {
        return Err(op_err(format!("Hook {index} in '{key}' must be an object")));
    }
    match hook.get("command") {
        None => return Err(op_err(format!("Hook {index} in '{key}' missing required 'command' field"))),
        Some(c) if !c.is_string() => return Err(op_err(format!("Hook {index} in '{key}' 'command' must be a string"))),
        _ => {}
    }
    if let Some(a) = hook.get("args")
        && !a.is_array() {
            return Err(op_err(format!("Hook {index} in '{key}' 'args' must be a list")));
        }
    if let Some(t) = hook.get("timeout") {
        let valid = t.as_i64().is_some_and(|v| v > 0);
        if !valid {
            return Err(op_err(format!("Hook {index} in '{key}' 'timeout' must be a positive integer")));
        }
    }
    if let Some(e) = hook.get("env")
        && !e.is_object() {
            return Err(op_err(format!("Hook {index} in '{key}' 'env' must be an object")));
        }
    Ok(())
}

pub fn validate_modified_payload(payload: &Value, hook_name: &str) -> Result<(), DeadlineError> {
    if !payload.is_object() {
        return Err(op_err(format!("Hook '{hook_name}' output must be a JSON object")));
    }
    if let Some(att) = payload.get("attachments") {
        if !att.is_object() {
            return Err(op_err(format!("Hook '{hook_name}' 'attachments' must be an object")));
        }
        if let Some(refs) = att.get("assetReferences") {
            if !refs.is_object() {
                return Err(op_err(format!("Hook '{hook_name}' 'assetReferences' must be an object")));
            }
            for field in &["inputFilenames", "inputDirectories", "outputDirectories", "referencedPaths"] {
                if let Some(v) = refs.get(*field)
                    && !v.is_array() {
                        return Err(op_err(format!(
                            "Hook '{hook_name}' 'assetReferences.{field}' must be a list"
                        )));
                    }
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Merging
// ---------------------------------------------------------------------------

pub fn merge_asset_references(original: Option<&Value>, modified: Option<&Value>) -> Value {
    let empty = serde_json::Map::new();
    let orig = original.and_then(|v| v.as_object()).unwrap_or(&empty);
    let mods = modified.and_then(|v| v.as_object()).unwrap_or(&empty);
    let mut result = orig.clone();
    for (k, v) in mods {
        result.insert(k.clone(), v.clone());
    }
    Value::Object(result)
}

pub fn merge_payload(original: &Value, modified: &Value) -> Value {
    let mut result = original.clone();
    let result_obj = result.as_object_mut().expect("value is object");
    if let Some(mod_obj) = modified.as_object() {
        for (key, value) in mod_obj {
            if key == "attachments" && value.is_object() && value.get("assetReferences").is_some() {
                let orig_att = result_obj.get("attachments")
                    .and_then(|a| a.as_object()).cloned().unwrap_or_default();
                let mut new_att = orig_att.clone();
                new_att.insert("assetReferences".into(), merge_asset_references(
                    orig_att.get("assetReferences"),
                    value.get("assetReferences"),
                ));
                // Merge other attachment fields from modified
                if let Some(mod_att) = value.as_object() {
                    for (k, v) in mod_att {
                        if k != "assetReferences" {
                            new_att.insert(k.clone(), v.clone());
                        }
                    }
                }
                result_obj.insert(key.clone(), Value::Object(new_att));
            } else {
                result_obj.insert(key.clone(), value.clone());
            }
        }
    }
    result
}

// ---------------------------------------------------------------------------
// Manager
// ---------------------------------------------------------------------------

pub struct HookManager {
    pub job_bundle_dir: String,
    pub hooks: Option<HookConfiguration>,
    script_resolve_dir: String,
    print_callback: Box<dyn Fn(&str) + Send>,
}

impl HookManager {
    pub fn new(job_bundle_dir: &str, print_callback: Box<dyn Fn(&str) + Send>) -> Self {
        let script_resolve_dir = get_script_resolve_dir(job_bundle_dir);
        Self {
            job_bundle_dir: job_bundle_dir.to_owned(),
            hooks: None,
            script_resolve_dir,
            print_callback,
        }
    }

    pub fn load_hooks(&mut self) -> Result<Option<&HookConfiguration>, DeadlineError> {
        let config_data = crate::loader::read_yaml_or_json_object(
            &self.job_bundle_dir, "hooks", false,
        )?;
        let Some(data) = config_data else { return Ok(None) };
        validate_configuration(&data)?;
        self.hooks = Some(HookConfiguration::from_dict(&data));
        Ok(self.hooks.as_ref())
    }

    pub fn execute_pre_submission_hooks(
        &self,
        metadata: &mut HookMetadata,
        payload: Value,
    ) -> Result<Value, DeadlineError> {
        let hooks = match &self.hooks {
            Some(h) if !h.pre_submission.is_empty() => &h.pre_submission,
            _ => return Ok(payload),
        };
        metadata.job_bundle_dir.clone_from(&self.script_resolve_dir);
        let mut current = payload;
        for (i, hook) in hooks.iter().enumerate() {
            let hook_name = format_hook_name(hook);
            (self.print_callback)(&format!("Running pre-submission hook [{}]: {hook_name}", i + 1));
            metadata.submission_payload = current.clone();
            let result = execute_hook(hook, metadata, &self.script_resolve_dir)?;
            if result.timed_out {
                report_failure(hook, &result, i + 1, "pre-submission", &self.print_callback);
                return Err(op_err(format!(
                    "Pre-submission hook [{}] timed out after {}s: {hook_name}", i + 1, hook.timeout
                )));
            }
            if !result.is_success() {
                report_failure(hook, &result, i + 1, "pre-submission", &self.print_callback);
                return Err(op_err(format!(
                    "Pre-submission hook [{}] failed with exit code {}: {hook_name}", i + 1, result.exit_code
                )));
            }
            if !result.stdout.trim().is_empty() {
                let modified: Value = serde_json::from_str(result.stdout.trim())
                    .map_err(|e| op_err(format!(
                        "Pre-submission hook [{}] produced invalid JSON: {e}", i + 1
                    )))?;
                validate_modified_payload(&modified, &hook_name)?;
                current = merge_payload(&current, &modified);
            }
        }
        Ok(current)
    }

    pub fn execute_post_submission_hooks(&self, metadata: &HookMetadata) {
        let hooks = match &self.hooks {
            Some(h) if !h.post_submission.is_empty() => &h.post_submission,
            _ => return,
        };
        for (i, hook) in hooks.iter().enumerate() {
            let hook_name = format_hook_name(hook);
            (self.print_callback)(&format!("Running post-submission hook [{}]: {hook_name}", i + 1));
            match execute_hook(hook, metadata, &self.script_resolve_dir) {
                Ok(result) if result.timed_out => {
                    log::warn!("Post-submission hook [{}] timed out after {}s: {hook_name}", i + 1, hook.timeout);
                }
                Ok(result) if !result.is_success() => {
                    log::warn!("Post-submission hook [{}] failed with exit code {}: {hook_name}", i + 1, result.exit_code);
                }
                Ok(_) => {}
                Err(e) => {
                    log::warn!("Post-submission hook [{}] error: {e}", i + 1);
                }
            }
        }
    }
}

pub fn generate_hooks_confirmation_message(hooks: &HookConfiguration, bundle_dir: &str) -> String {
    let mut lines = vec!["This job bundle contains submission hooks that will execute on your machine:\n".into()];
    if !hooks.pre_submission.is_empty() {
        lines.push("  Pre-submission hooks:".into());
        for (i, hook) in hooks.pre_submission.iter().enumerate() {
            lines.push(format!("    [{}] {}", i + 1, format_hook_name(hook)));
        }
        lines.push(String::new());
    }
    if !hooks.post_submission.is_empty() {
        lines.push("  Post-submission hooks:".into());
        for (i, hook) in hooks.post_submission.iter().enumerate() {
            lines.push(format!("    [{}] {}", i + 1, format_hook_name(hook)));
        }
        lines.push(String::new());
    }
    lines.push(format!("  Bundle: {bundle_dir}\n"));
    lines.join("\n")
}

fn format_hook_name(hook: &HookDefinition) -> String {
    let mut name = hook.command.clone();
    if !hook.args.is_empty() {
        name.push(' ');
        name.push_str(&hook.args.join(" "));
    }
    name
}

fn get_script_resolve_dir(job_bundle_dir: &str) -> String {
    let origin_file = Path::new(job_bundle_dir).join(".hooks_origin");
    if origin_file.is_file()
        && let Ok(content) = std::fs::read_to_string(&origin_file) {
            let origin = content.trim().to_owned();
            if Path::new(&origin).is_dir() {
                return origin;
            }
        }
    job_bundle_dir.to_owned()
}

fn resolve_command(command: &str, script_dir: &str) -> Result<String, DeadlineError> {
    if Path::new(command).is_absolute() {
        if Path::new(command).is_file() {
            return Ok(command.to_owned());
        }
        return Err(op_err(format!("Hook command not found: {command}")));
    }
    // Try relative to script resolve dir
    let relative = Path::new(script_dir).join(command);
    if relative.is_file() {
        return Ok(std::fs::canonicalize(&relative)
            .unwrap_or(relative)
            .to_string_lossy().into_owned());
    }
    // Try PATH lookup
    if let Some(resolved) = find_in_path(command) {
        return Ok(resolved);
    }
    Err(op_err(format!("Hook command not found: {command}")))
}

fn find_in_path(command: &str) -> Option<String> {
    let path_var = std::env::var("PATH").unwrap_or_default();
    let sep = if cfg!(windows) { ';' } else { ':' };
    for dir in path_var.split(sep) {
        let candidate = Path::new(dir).join(command);
        if candidate.is_file() {
            return Some(candidate.to_string_lossy().into_owned());
        }
    }
    None
}

fn resolve_args(args: &[String], script_dir: &str) -> Vec<String> {
    args.iter().map(|arg| {
        if !Path::new(arg).is_absolute() {
            let relative = Path::new(script_dir).join(arg);
            if relative.exists() {
                return std::fs::canonicalize(&relative)
                    .unwrap_or(relative)
                    .to_string_lossy().into_owned();
            }
        }
        arg.clone()
    }).collect()
}

fn execute_hook(
    hook: &HookDefinition,
    metadata: &HookMetadata,
    script_dir: &str,
) -> Result<HookResult, DeadlineError> {
    let command = resolve_command(&hook.command, script_dir)?;
    let args = resolve_args(&hook.args, script_dir);
    let mut env: HashMap<String, String> = std::env::vars().collect();
    env.extend(metadata.to_environment_variables());
    env.extend(hook.env.iter().map(|(k, v)| (k.clone(), v.clone())));

    let start = Instant::now();
    let mut child = Command::new(&command)
        .args(&args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .envs(&env)
        .spawn()
        .map_err(|e| op_err(format!("Hook command not found: {}\n{e}", hook.command)))?;

    // Write metadata to stdin, then drop to close
    if let Some(mut stdin) = child.stdin.take() {
        let _ = stdin.write_all(metadata.to_json().as_bytes());
    }

    // Wait with timeout: store PID so we can kill on timeout
    let timeout = std::time::Duration::from_secs(hook.timeout);
    let pid = child.id();
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let output = child.wait_with_output();
        let _ = tx.send(output);
    });

    match rx.recv_timeout(timeout) {
        Ok(Ok(output)) => Ok(HookResult {
            exit_code: output.status.code().unwrap_or(-1),
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
            execution_time: start.elapsed().as_secs_f64(),
            timed_out: false,
        }),
        Ok(Err(e)) => Err(op_err(format!("Failed to execute hook: {}\n{e}", hook.command))),
        Err(_) => {
            // Timed out — force-kill the child process via its PID.
            // SAFETY: SIGKILL is sent to a child process we spawned. The pid
            // is from Child::id() (a u32 cast to i32, safe for valid PIDs).
            // SIGKILL cannot be caught or ignored, so this is a last-resort
            // cleanup after the hook exceeded its timeout.
            #[cfg(unix)]
            #[allow(unsafe_code, reason = "force-killing a timed-out child process requires POSIX kill")]
            unsafe { libc::kill(pid as i32, libc::SIGKILL); }
            #[cfg(not(unix))]
            { /* On non-unix, the thread's Child will be dropped eventually */ }
            Ok(HookResult {
                exit_code: -1,
                stdout: String::new(),
                stderr: String::new(),
                execution_time: start.elapsed().as_secs_f64(),
                timed_out: true,
            })
        }
    }
}

fn report_failure(hook: &HookDefinition, result: &HookResult, index: usize, hook_type: &str, print: &dyn Fn(&str)) {
    let hook_name = format_hook_name(hook);
    print(&format!("\n{} hook [{index}] failed: {hook_name}", capitalize(hook_type)));
    print(&format!("Exit code: {}", result.exit_code));
    if result.timed_out {
        print(&format!("Timed out after {}s", hook.timeout));
    }
    if !result.stdout.is_empty() {
        print(&format!("stdout:\n{}", result.stdout));
    }
    if !result.stderr.is_empty() {
        print(&format!("stderr:\n{}", result.stderr));
    }
}

fn capitalize(s: &str) -> String {
    let mut c = s.chars();
    match c.next() {
        None => String::new(),
        Some(f) => f.to_uppercase().to_string() + c.as_str(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    
    use tempfile::TempDir;

    // ---------------------------------------------------------------
    // Hook data models
    // ---------------------------------------------------------------

    #[test]
    fn hook_definition_from_dict_minimal_defaults() {
        let data = json!({"command": "python"});
        let hook = HookDefinition::from_dict(&data);
        assert_eq!(hook.command, "python");
        assert!(hook.args.is_empty());
        assert_eq!(hook.timeout, 60);
        assert!(hook.env.is_empty());
    }

    #[test]
    fn hook_definition_from_dict_full_fields() {
        let data = json!({
            "command": "python",
            "args": ["-c", "print('hello')"],
            "timeout": 30,
            "env": {"FOO": "bar"}
        });
        let hook = HookDefinition::from_dict(&data);
        assert_eq!(hook.command, "python");
        assert_eq!(hook.args, vec!["-c", "print('hello')"]);
        assert_eq!(hook.timeout, 30);
        assert_eq!(&hook.env["FOO"], "bar");
    }

    #[test]
    fn hook_configuration_from_dict_empty_defaults() {
        let config = HookConfiguration::from_dict(&json!({}));
        assert!(config.pre_submission.is_empty());
        assert!(config.post_submission.is_empty());
        assert_eq!(config.version, "1.0");
    }

    #[test]
    fn hook_configuration_from_dict_explicit_version() {
        let data = json!({"version": "1.0", "preSubmission": [{"command": "test.py"}]});
        let config = HookConfiguration::from_dict(&data);
        assert_eq!(config.version, "1.0");
    }

    #[test]
    fn hook_configuration_from_dict_both_hook_types() {
        let data = json!({
            "preSubmission": [{"command": "validate.py"}],
            "postSubmission": [{"command": "notify.py"}, {"command": "log.py"}]
        });
        let config = HookConfiguration::from_dict(&data);
        assert_eq!(config.pre_submission.len(), 1);
        assert_eq!(config.post_submission.len(), 2);
        assert_eq!(config.pre_submission[0].command, "validate.py");
    }

    #[test]
    fn hook_metadata_to_dict_all_fields() {
        let meta = make_metadata_all_fields();
        let d = meta.to_dict();
        assert_eq!(d["jobName"], "TestJob");
        assert_eq!(d["priority"], 50);
        assert_eq!(d["farmId"], "farm-123");
        assert_eq!(d["storageProfileId"], "sp-789");
        assert_eq!(d["jobId"], "job-abc");
    }

    #[test]
    fn hook_metadata_to_dict_without_optional() {
        let meta = make_metadata_minimal();
        let d = meta.to_dict();
        assert!(d.get("storageProfileId").is_none());
        assert!(d.get("jobId").is_none());
    }

    #[test]
    fn hook_metadata_to_json_roundtrip() {
        let meta = make_metadata_minimal();
        let j = meta.to_json();
        let parsed: Value = serde_json::from_str(&j).unwrap();
        assert_eq!(parsed["jobName"], "TestJob");
    }

    #[test]
    fn hook_metadata_to_env_vars_all_fields() {
        let meta = make_metadata_all_fields();
        let env = meta.to_environment_variables();
        assert_eq!(env["DEADLINE_JOB_NAME"], "TestJob");
        assert_eq!(env["DEADLINE_PRIORITY"], "50");
        assert_eq!(env["DEADLINE_FARM_ID"], "farm-123");
        assert_eq!(env["DEADLINE_QUEUE_ID"], "queue-456");
        assert_eq!(env["DEADLINE_STORAGE_PROFILE_ID"], "sp-789");
        assert_eq!(env["DEADLINE_JOB_ID"], "job-abc");
    }

    #[test]
    fn hook_metadata_to_env_vars_without_optional() {
        let meta = make_metadata_minimal();
        let env = meta.to_environment_variables();
        assert_eq!(env["DEADLINE_JOB_NAME"], "TestJob");
        assert!(!env.contains_key("DEADLINE_STORAGE_PROFILE_ID"));
        assert!(!env.contains_key("DEADLINE_JOB_ID"));
    }

    #[test]
    fn hook_result_success() {
        let r = HookResult { exit_code: 0, stdout: String::new(), stderr: String::new(), execution_time: 1.0, timed_out: false };
        assert!(r.is_success());
    }

    #[test]
    fn hook_result_failure_exit_code() {
        let r = HookResult { exit_code: 1, stdout: String::new(), stderr: String::new(), execution_time: 1.0, timed_out: false };
        assert!(!r.is_success());
    }

    #[test]
    fn hook_result_failure_timeout() {
        let r = HookResult { exit_code: 0, stdout: String::new(), stderr: String::new(), execution_time: 1.0, timed_out: true };
        assert!(!r.is_success());
    }

    // ---------------------------------------------------------------
    // Hook configuration validation
    // ---------------------------------------------------------------

    #[test]
    fn validate_config_valid() {
        let config = json!({
            "preSubmission": [{"command": "python", "args": ["-c", "pass"], "timeout": 30}],
            "postSubmission": [{"command": "echo", "env": {"FOO": "bar"}}]
        });
        validate_configuration(&config).unwrap();
    }

    #[test]
    fn validate_config_pre_not_list() {
        let config = json!({"preSubmission": "not a list"});
        let err = validate_configuration(&config).unwrap_err().to_string();
        assert!(err.contains("must be a list"), "got: {err}");
    }

    #[test]
    fn validate_config_hook_not_dict() {
        let config = json!({"preSubmission": ["not a dict"]});
        let err = validate_configuration(&config).unwrap_err().to_string();
        assert!(err.contains("must be an object"), "got: {err}");
    }

    #[test]
    fn validate_config_missing_command() {
        let config = json!({"preSubmission": [{"args": []}]});
        let err = validate_configuration(&config).unwrap_err().to_string();
        assert!(err.contains("missing required 'command'"), "got: {err}");
    }

    #[test]
    fn validate_config_command_not_string() {
        let config = json!({"preSubmission": [{"command": 123}]});
        let err = validate_configuration(&config).unwrap_err().to_string();
        assert!(err.contains("'command' must be a string"), "got: {err}");
    }

    #[test]
    fn validate_config_args_not_list() {
        let config = json!({"preSubmission": [{"command": "echo", "args": "not list"}]});
        let err = validate_configuration(&config).unwrap_err().to_string();
        assert!(err.contains("'args' must be a list"), "got: {err}");
    }

    #[test]
    fn validate_config_timeout_zero() {
        let config = json!({"preSubmission": [{"command": "echo", "timeout": 0}]});
        let err = validate_configuration(&config).unwrap_err().to_string();
        assert!(err.contains("'timeout' must be a positive integer"), "got: {err}");
    }

    #[test]
    fn validate_config_timeout_negative() {
        let config = json!({"preSubmission": [{"command": "echo", "timeout": -1}]});
        let err = validate_configuration(&config).unwrap_err().to_string();
        assert!(err.contains("'timeout' must be a positive integer"), "got: {err}");
    }

    #[test]
    fn validate_config_env_not_dict() {
        let config = json!({"preSubmission": [{"command": "echo", "env": "not dict"}]});
        let err = validate_configuration(&config).unwrap_err().to_string();
        assert!(err.contains("'env' must be an object"), "got: {err}");
    }

    #[test]
    fn validate_config_valid_version() {
        let config = json!({"version": "1.0", "preSubmission": [{"command": "echo"}]});
        validate_configuration(&config).unwrap();
    }

    #[test]
    fn validate_config_unsupported_version() {
        let config = json!({"version": "2.0", "preSubmission": [{"command": "echo"}]});
        let err = validate_configuration(&config).unwrap_err().to_string();
        assert!(err.contains("Unsupported hooks version"), "got: {err}");
    }

    // --- validate_modified_payload ---

    #[test]
    fn validate_payload_valid() {
        validate_modified_payload(&json!({"priority": 100}), "test_hook").unwrap();
    }

    #[test]
    fn validate_payload_not_dict() {
        let err = validate_modified_payload(&json!("not a dict"), "test_hook").unwrap_err().to_string();
        assert!(err.contains("must be a JSON object"), "got: {err}");
    }

    #[test]
    fn validate_payload_attachments_not_dict() {
        let err = validate_modified_payload(&json!({"attachments": "not dict"}), "test_hook").unwrap_err().to_string();
        assert!(err.contains("'attachments' must be an object"), "got: {err}");
    }

    #[test]
    fn validate_payload_asset_refs_not_dict() {
        let payload = json!({"attachments": {"assetReferences": "not dict"}});
        let err = validate_modified_payload(&payload, "test_hook").unwrap_err().to_string();
        assert!(err.contains("'assetReferences' must be an object"), "got: {err}");
    }

    #[test]
    fn validate_payload_input_filenames_not_list() {
        let payload = json!({"attachments": {"assetReferences": {"inputFilenames": "not list"}}});
        let err = validate_modified_payload(&payload, "test_hook").unwrap_err().to_string();
        assert!(err.contains("must be a list"), "got: {err}");
    }

    // ---------------------------------------------------------------
    // Hook payload merging
    // ---------------------------------------------------------------

    #[test]
    fn merge_refs_both_none() {
        let result = merge_asset_references(None, None);
        assert_eq!(result, json!({}));
    }

    #[test]
    fn merge_refs_original_only() {
        let original = json!({"inputFilenames": ["/a.txt", "/b.txt"]});
        let result = merge_asset_references(Some(&original), None);
        assert_eq!(result["inputFilenames"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn merge_refs_modified_only() {
        let modified = json!({"inputFilenames": ["/c.txt"]});
        let result = merge_asset_references(None, Some(&modified));
        assert_eq!(result["inputFilenames"], json!(["/c.txt"]));
    }

    #[test]
    fn merge_refs_modified_replaces() {
        let original = json!({"inputFilenames": ["/a.txt", "/b.txt"]});
        let modified = json!({"inputFilenames": ["/b.txt", "/c.txt"]});
        let result = merge_asset_references(Some(&original), Some(&modified));
        assert_eq!(result["inputFilenames"], json!(["/b.txt", "/c.txt"]));
    }

    #[test]
    fn merge_refs_all_fields() {
        let original = json!({
            "inputFilenames": ["/a.txt"],
            "inputDirectories": ["/dir1"],
            "outputDirectories": ["/out1"],
            "referencedPaths": ["/ref1"]
        });
        let modified = json!({
            "inputFilenames": ["/b.txt"],
            "inputDirectories": ["/dir2"],
            "outputDirectories": ["/out2"],
            "referencedPaths": ["/ref2"]
        });
        let result = merge_asset_references(Some(&original), Some(&modified));
        assert_eq!(result["inputFilenames"], json!(["/b.txt"]));
        assert_eq!(result["inputDirectories"], json!(["/dir2"]));
        assert_eq!(result["outputDirectories"], json!(["/out2"]));
        assert_eq!(result["referencedPaths"], json!(["/ref2"]));
    }

    #[test]
    fn merge_payload_simple_override() {
        let original = json!({"priority": 50, "farmId": "farm-123"});
        let modified = json!({"priority": 100});
        let result = merge_payload(&original, &modified);
        assert_eq!(result["priority"], 100);
        assert_eq!(result["farmId"], "farm-123");
    }

    #[test]
    fn merge_payload_new_field() {
        let original = json!({"priority": 50});
        let modified = json!({"maxWorkerCount": 10});
        let result = merge_payload(&original, &modified);
        assert_eq!(result["priority"], 50);
        assert_eq!(result["maxWorkerCount"], 10);
    }

    #[test]
    fn merge_payload_asset_references() {
        let original = json!({
            "attachments": {
                "assetReferences": {"inputFilenames": ["/a.txt"]},
                "fileSystem": "COPIED"
            }
        });
        let modified = json!({"attachments": {"assetReferences": {"inputFilenames": ["/b.txt"]}}});
        let result = merge_payload(&original, &modified);
        assert_eq!(result["attachments"]["assetReferences"]["inputFilenames"], json!(["/b.txt"]));
        assert_eq!(result["attachments"]["fileSystem"], "COPIED");
    }

    // ---------------------------------------------------------------
    // Hook loading and execution
    // ---------------------------------------------------------------

    #[test]
    fn load_hooks_no_file() {
        let dir = TempDir::new().unwrap();
        let mut mgr = HookManager::new(dir.path().to_str().unwrap(), Box::new(|_| {}));
        let result = mgr.load_hooks().unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn load_hooks_yaml() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("hooks.yaml"),
            "preSubmission:\n  - command: python\n    args: [\"-c\", \"pass\"]\n"
        ).unwrap();
        let mut mgr = HookManager::new(dir.path().to_str().unwrap(), Box::new(|_| {}));
        let hooks = mgr.load_hooks().unwrap().unwrap();
        assert_eq!(hooks.pre_submission.len(), 1);
        assert_eq!(hooks.pre_submission[0].command, "python");
    }

    #[test]
    fn load_hooks_json() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("hooks.json"),
            r#"{"postSubmission": [{"command": "echo", "args": ["done"]}]}"#
        ).unwrap();
        let mut mgr = HookManager::new(dir.path().to_str().unwrap(), Box::new(|_| {}));
        let hooks = mgr.load_hooks().unwrap().unwrap();
        assert_eq!(hooks.post_submission.len(), 1);
    }

    #[test]
    fn load_hooks_both_error() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("hooks.yaml"), "preSubmission: []\n").unwrap();
        std::fs::write(dir.path().join("hooks.json"), "{}").unwrap();
        let mut mgr = HookManager::new(dir.path().to_str().unwrap(), Box::new(|_| {}));
        let err = mgr.load_hooks().unwrap_err().to_string();
        assert!(err.contains("both hooks.json and hooks.yaml"), "got: {err}");
    }

    // --- Pre-submission hook execution ---

    /// Get a command that works as a hook interpreter on this platform.
    fn sh_cmd() -> &'static str {
        if cfg!(windows) { "cmd.exe" } else { "sh" }
    }

    /// Write a shell script that exits 0 with no output.
    fn write_noop_hook(dir: &Path) -> String {
        let script = dir.join("noop.sh");
        std::fs::write(&script, "#!/bin/sh\nexit 0\n").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        script.to_str().unwrap().to_owned()
    }

    fn write_hooks_yaml(dir: &Path, content: &str) {
        std::fs::write(dir.join("hooks.yaml"), content).unwrap();
    }

    fn make_metadata_for_dir(dir: &str) -> HookMetadata {
        HookMetadata {
            job_name: "Test".into(),
            priority: 50,
            farm_id: "farm-123".into(),
            queue_id: "queue-456".into(),
            job_bundle_dir: dir.into(),
            parameters: HashMap::new(),
            submitter_name: "Test".into(),
            asset_references: json!({}),
            submission_payload: json!({}),
            storage_profile_id: None,
            job_id: None,
        }
    }

    #[test]
    fn pre_hook_success_no_output() {
        let dir = TempDir::new().unwrap();
        let dir_str = dir.path().to_str().unwrap();
        write_hooks_yaml(dir.path(), &format!(
            "preSubmission:\n  - command: {}\n    args: [\"-c\", \"exit 0\"]\n", sh_cmd()
        ));
        let messages: std::sync::Arc<std::sync::Mutex<Vec<String>>> = Default::default();
        let msgs = messages.clone();
        let mut mgr = HookManager::new(dir_str, Box::new(move |s| msgs.lock().unwrap().push(s.to_owned())));
        mgr.load_hooks().unwrap();
        let mut meta = make_metadata_for_dir(dir_str);
        let result = mgr.execute_pre_submission_hooks(&mut meta, json!({"priority": 50})).unwrap();
        assert_eq!(result["priority"], 50);
        let msgs = messages.lock().unwrap();
        assert!(msgs.iter().any(|m| m.contains("Running pre-submission hook")));
    }

    #[test]
    fn pre_hook_modifies_payload() {
        let dir = TempDir::new().unwrap();
        let dir_str = dir.path().to_str().unwrap();
        // Hook outputs JSON to stdout that changes priority
        write_hooks_yaml(dir.path(), &format!(
            "preSubmission:\n  - command: {}\n    args: [\"-c\", \"echo '{{\\\"priority\\\": 100}}'\"]\n", sh_cmd()
        ));
        let mut mgr = HookManager::new(dir_str, Box::new(|_| {}));
        mgr.load_hooks().unwrap();
        let mut meta = make_metadata_for_dir(dir_str);
        let result = mgr.execute_pre_submission_hooks(&mut meta, json!({"priority": 50})).unwrap();
        assert_eq!(result["priority"], 100);
    }

    #[test]
    fn pre_hook_failure_blocks() {
        let dir = TempDir::new().unwrap();
        let dir_str = dir.path().to_str().unwrap();
        write_hooks_yaml(dir.path(), &format!(
            "preSubmission:\n  - command: {}\n    args: [\"-c\", \"exit 1\"]\n", sh_cmd()
        ));
        let mut mgr = HookManager::new(dir_str, Box::new(|_| {}));
        mgr.load_hooks().unwrap();
        let mut meta = make_metadata_for_dir(dir_str);
        let err = mgr.execute_pre_submission_hooks(&mut meta, json!({})).unwrap_err().to_string();
        assert!(err.contains("failed with exit code"), "got: {err}");
    }

    #[test]
    fn pre_hook_timeout() {
        let dir = TempDir::new().unwrap();
        let dir_str = dir.path().to_str().unwrap();
        write_hooks_yaml(dir.path(), &format!(
            "preSubmission:\n  - command: {}\n    args: [\"-c\", \"sleep 10\"]\n    timeout: 1\n", sh_cmd()
        ));
        let mut mgr = HookManager::new(dir_str, Box::new(|_| {}));
        mgr.load_hooks().unwrap();
        let mut meta = make_metadata_for_dir(dir_str);
        let err = mgr.execute_pre_submission_hooks(&mut meta, json!({})).unwrap_err().to_string();
        assert!(err.contains("timed out"), "got: {err}");
    }

    #[test]
    fn pre_hook_invalid_json() {
        let dir = TempDir::new().unwrap();
        let dir_str = dir.path().to_str().unwrap();
        write_hooks_yaml(dir.path(), &format!(
            "preSubmission:\n  - command: {}\n    args: [\"-c\", \"echo 'not json'\"]\n", sh_cmd()
        ));
        let mut mgr = HookManager::new(dir_str, Box::new(|_| {}));
        mgr.load_hooks().unwrap();
        let mut meta = make_metadata_for_dir(dir_str);
        let err = mgr.execute_pre_submission_hooks(&mut meta, json!({})).unwrap_err().to_string();
        assert!(err.contains("invalid JSON"), "got: {err}");
    }

    #[test]
    fn pre_hook_receives_stdin() {
        let dir = TempDir::new().unwrap();
        let dir_str = dir.path().to_str().unwrap();
        let output_file = dir.path().join("output.txt");
        let escaped = output_file.to_str().unwrap().replace('\\', "\\\\");
        write_hooks_yaml(dir.path(), &format!(
            "preSubmission:\n  - command: python3\n    args: [\"-c\", \"import sys,json; d=json.load(sys.stdin); open('{escaped}', 'w').write(d['jobName'])\"]\n"
        ));
        let mut mgr = HookManager::new(dir_str, Box::new(|_| {}));
        mgr.load_hooks().unwrap();
        let mut meta = make_metadata_for_dir(dir_str);
        meta.job_name = "StdinTestJob".into();
        mgr.execute_pre_submission_hooks(&mut meta, json!({})).unwrap();
        let content = std::fs::read_to_string(&output_file).unwrap();
        assert_eq!(content, "StdinTestJob");
    }

    #[test]
    fn pre_hook_receives_env_vars() {
        let dir = TempDir::new().unwrap();
        let dir_str = dir.path().to_str().unwrap();
        let output_file = dir.path().join("env_out.txt");
        let escaped = output_file.to_str().unwrap().replace('\\', "\\\\");
        write_hooks_yaml(dir.path(), &format!(
            "preSubmission:\n  - command: sh\n    args: [\"-c\", \"echo $DEADLINE_JOB_NAME > '{escaped}'\"]\n"
        ));
        let mut mgr = HookManager::new(dir_str, Box::new(|_| {}));
        mgr.load_hooks().unwrap();
        let mut meta = make_metadata_for_dir(dir_str);
        meta.job_name = "MyTestJob".into();
        mgr.execute_pre_submission_hooks(&mut meta, json!({})).unwrap();
        let content = std::fs::read_to_string(&output_file).unwrap().trim().to_owned();
        assert_eq!(content, "MyTestJob");
    }

    #[test]
    fn pre_hook_receives_custom_env() {
        let dir = TempDir::new().unwrap();
        let dir_str = dir.path().to_str().unwrap();
        let output_file = dir.path().join("custom_env.txt");
        let escaped = output_file.to_str().unwrap().replace('\\', "\\\\");
        write_hooks_yaml(dir.path(), &format!(
            "preSubmission:\n  - command: sh\n    args: [\"-c\", \"echo $CUSTOM_VAR > '{escaped}'\"]\n    env:\n      CUSTOM_VAR: custom_value\n"
        ));
        let mut mgr = HookManager::new(dir_str, Box::new(|_| {}));
        mgr.load_hooks().unwrap();
        let mut meta = make_metadata_for_dir(dir_str);
        mgr.execute_pre_submission_hooks(&mut meta, json!({})).unwrap();
        let content = std::fs::read_to_string(&output_file).unwrap().trim().to_owned();
        assert_eq!(content, "custom_value");
    }

    #[test]
    fn pre_hook_command_not_found() {
        let dir = TempDir::new().unwrap();
        let dir_str = dir.path().to_str().unwrap();
        write_hooks_yaml(dir.path(), "preSubmission:\n  - command: nonexistent_command_xyz\n");
        let mut mgr = HookManager::new(dir_str, Box::new(|_| {}));
        mgr.load_hooks().unwrap();
        let mut meta = make_metadata_for_dir(dir_str);
        let err = mgr.execute_pre_submission_hooks(&mut meta, json!({})).unwrap_err().to_string();
        assert!(err.contains("not found"), "got: {err}");
    }

    #[test]
    fn pre_hook_absolute_command() {
        let dir = TempDir::new().unwrap();
        let dir_str = dir.path().to_str().unwrap();
        // Use sh with absolute path
        let sh_path = if cfg!(windows) { "C:\\Windows\\System32\\cmd.exe" } else { "/bin/sh" };
        write_hooks_yaml(dir.path(), &format!(
            "preSubmission:\n  - command: {sh_path}\n    args: [\"-c\", \"exit 0\"]\n"
        ));
        let mut mgr = HookManager::new(dir_str, Box::new(|_| {}));
        mgr.load_hooks().unwrap();
        let mut meta = make_metadata_for_dir(dir_str);
        mgr.execute_pre_submission_hooks(&mut meta, json!({})).unwrap();
    }

    #[test]
    fn pre_hook_hooks_origin_resolution() {
        let dir = TempDir::new().unwrap();
        // Create original bundle dir with script
        let original_dir = dir.path().join("original");
        std::fs::create_dir_all(&original_dir).unwrap();
        let script = original_dir.join("myscript.sh");
        std::fs::write(&script, "#!/bin/sh\nexit 0\n").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        // Create history bundle dir with hooks.yaml and .hooks_origin
        let history_dir = dir.path().join("history");
        std::fs::create_dir_all(&history_dir).unwrap();
        write_hooks_yaml(&history_dir, "preSubmission:\n  - command: myscript.sh\n");
        std::fs::write(history_dir.join(".hooks_origin"), original_dir.to_str().unwrap()).unwrap();

        let history_str = history_dir.to_str().unwrap();
        let mut mgr = HookManager::new(history_str, Box::new(|_| {}));
        mgr.load_hooks().unwrap();
        let mut meta = make_metadata_for_dir(history_str);
        // Should resolve myscript.sh from original_dir via .hooks_origin
        mgr.execute_pre_submission_hooks(&mut meta, json!({})).unwrap();
    }

    // --- Post-submission hook execution ---

    #[test]
    fn post_hook_failure_warns() {
        let dir = TempDir::new().unwrap();
        let dir_str = dir.path().to_str().unwrap();
        write_hooks_yaml(dir.path(), &format!(
            "postSubmission:\n  - command: {}\n    args: [\"-c\", \"exit 1\"]\n", sh_cmd()
        ));
        let mut mgr = HookManager::new(dir_str, Box::new(|_| {}));
        mgr.load_hooks().unwrap();
        let meta = make_metadata_for_dir(dir_str);
        // Should NOT panic or return error
        mgr.execute_post_submission_hooks(&meta);
    }

    #[test]
    fn post_hook_timeout_warns() {
        let dir = TempDir::new().unwrap();
        let dir_str = dir.path().to_str().unwrap();
        write_hooks_yaml(dir.path(), &format!(
            "postSubmission:\n  - command: {}\n    args: [\"-c\", \"sleep 5\"]\n    timeout: 1\n", sh_cmd()
        ));
        let mut mgr = HookManager::new(dir_str, Box::new(|_| {}));
        mgr.load_hooks().unwrap();
        let meta = make_metadata_for_dir(dir_str);
        // Should NOT panic or return error
        mgr.execute_post_submission_hooks(&meta);
    }

    #[test]
    fn post_hook_not_called_on_failure() {
        let dir = TempDir::new().unwrap();
        let marker = dir.path().join("post_hook_ran");
        let marker_escaped = marker.to_str().unwrap().replace('\\', "\\\\");
        let script = dir.path().join("marker.sh");
        std::fs::write(&script, format!("#!/bin/sh\ntouch '{marker_escaped}'\n")).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        write_hooks_yaml(dir.path(), &format!(
            "postSubmission:\n  - command: {}\n", script.to_str().unwrap()
        ));
        let mut mgr = HookManager::new(dir.path().to_str().unwrap(), Box::new(|_| {}));
        mgr.load_hooks().unwrap();
        // Simulate CreateJob failure: don't call execute_post_submission_hooks
        assert!(!marker.exists());
    }

    #[test]
    fn post_hook_runs_after_success() {
        let dir = TempDir::new().unwrap();
        let marker = dir.path().join("post_hook_ran");
        let marker_escaped = marker.to_str().unwrap().replace('\\', "\\\\");
        let script = dir.path().join("marker.sh");
        std::fs::write(&script, format!("#!/bin/sh\ntouch '{marker_escaped}'\n")).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        write_hooks_yaml(dir.path(), &format!(
            "postSubmission:\n  - command: {}\n", script.to_str().unwrap()
        ));
        let mut mgr = HookManager::new(dir.path().to_str().unwrap(), Box::new(|_| {}));
        mgr.load_hooks().unwrap();
        let mut meta = make_metadata_for_dir(dir.path().to_str().unwrap());
        meta.job_id = Some("job-789".into());
        mgr.execute_post_submission_hooks(&meta);
        assert!(marker.exists());
    }

    // --- Confirmation message ---

    #[test]
    fn confirmation_message_format() {
        let hooks = HookConfiguration {
            version: "1.0".into(),
            pre_submission: vec![HookDefinition {
                command: "python".into(),
                args: vec!["validate.py".into()],
                timeout: 60,
                env: HashMap::new(),
            }],
            post_submission: vec![HookDefinition {
                command: "bash".into(),
                args: vec!["notify.sh".into()],
                timeout: 60,
                env: HashMap::new(),
            }],
        };
        let msg = generate_hooks_confirmation_message(&hooks, "/path/to/bundle");
        assert!(msg.contains("Pre-submission hooks:"), "got: {msg}");
        assert!(msg.contains("python validate.py"), "got: {msg}");
        assert!(msg.contains("Post-submission hooks:"), "got: {msg}");
        assert!(msg.contains("bash notify.sh"), "got: {msg}");
        assert!(msg.contains("/path/to/bundle"), "got: {msg}");
    }

    // ---------------------------------------------------------------
    // Test helpers
    // ---------------------------------------------------------------

    fn make_metadata_all_fields() -> HookMetadata {
        HookMetadata {
            job_name: "TestJob".into(),
            priority: 50,
            farm_id: "farm-123".into(),
            queue_id: "queue-456".into(),
            job_bundle_dir: "/path/to/bundle".into(),
            parameters: HashMap::from([("Param1".into(), json!("value1"))]),
            submitter_name: "TestSubmitter".into(),
            asset_references: json!({"inputFilenames": ["/file.txt"]}),
            submission_payload: json!({"farmId": "farm-123"}),
            storage_profile_id: Some("sp-789".into()),
            job_id: Some("job-abc".into()),
        }
    }

    fn make_metadata_minimal() -> HookMetadata {
        HookMetadata {
            job_name: "TestJob".into(),
            priority: 50,
            farm_id: "farm-123".into(),
            queue_id: "queue-456".into(),
            job_bundle_dir: "/path/to/bundle".into(),
            parameters: HashMap::new(),
            submitter_name: "TestSubmitter".into(),
            asset_references: json!({}),
            submission_payload: json!({}),
            storage_profile_id: None,
            job_id: None,
        }
    }
}
