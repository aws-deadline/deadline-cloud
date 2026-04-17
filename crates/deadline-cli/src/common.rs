// Common utilities for the deadline CLI.

use regex::Regex;
use std::sync::LazyLock;

// ---------------------------------------------------------------------------
// Markdown stripping
// ---------------------------------------------------------------------------

static RE_REF_DEF: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?m)^\s*\[[^\]]+\]:\s*\S+.*$").unwrap());
static RE_INLINE_LINK: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\[([^\]]+)\]\(([^)]+)\)").unwrap());
static RE_REF_LINK: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\[([^\]]+)\]\[[^\]]*\]").unwrap());
static RE_BOLD: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\*\*([^*]+)\*\*").unwrap());
static RE_BOLD_UNDER: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"__([^_]+)__").unwrap());
static RE_ITALIC: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?m)(?P<pre>[^*\n])\*(?P<inner>[^*\n]+)\*").unwrap());
static RE_BLANK_LINES: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\n{3,}").unwrap());

/// Strip markdown syntax for clean terminal display.
pub fn strip_markdown_for_terminal(text: &str) -> String {
    let text = RE_REF_DEF.replace_all(text, "");
    let text = RE_INLINE_LINK.replace_all(&text, "$1 ($2)");
    let text = RE_REF_LINK.replace_all(&text, "$1");
    let text = RE_BOLD.replace_all(&text, "$1");
    let text = RE_BOLD_UNDER.replace_all(&text, "$1");
    let text = RE_ITALIC.replace_all(&text, "$pre$inner");
    let text = RE_BLANK_LINES.replace_all(&text, "\n\n");
    text.trim().to_string()
}

// ---------------------------------------------------------------------------
// CLI options → config
// ---------------------------------------------------------------------------

/// CLI flag values that override config settings.
#[derive(Default)]
pub struct CliOptions {
    pub profile: Option<String>,
    pub farm_id: Option<String>,
    pub queue_id: Option<String>,
    pub job_id: Option<String>,
    pub storage_profile_id: Option<String>,
    pub conflict_resolution: Option<String>,
    pub yes: bool,
}

/// Error from `apply_cli_options_to_config`.
#[derive(Debug)]
pub enum CliConfigError {
    /// A config operation failed.
    Operation(String),
    /// A required option is missing (should exit code 2).
    MissingRequired(String),
}

/// Apply CLI flag overrides to a config and validate required options.
pub fn apply_cli_options_to_config(
    config: &mut deadline_config::ini::IniConfig,
    options: &CliOptions,
    required: &[&str],
) -> Result<(), CliConfigError> {
    use deadline_config::config_file;

    if let Some(ref v) = options.profile {
        config_file::set_setting_in_config("defaults.aws_profile_name", v, config)
            .map_err(|e| CliConfigError::Operation(e.to_string()))?;
    }
    if let Some(ref v) = options.farm_id {
        config_file::set_setting_in_config("defaults.farm_id", v, config)
            .map_err(|e| CliConfigError::Operation(e.to_string()))?;
    }
    if let Some(ref v) = options.queue_id {
        config_file::set_setting_in_config("defaults.queue_id", v, config)
            .map_err(|e| CliConfigError::Operation(e.to_string()))?;
    }
    if let Some(ref v) = options.job_id {
        config_file::set_setting_in_config("defaults.job_id", v, config)
            .map_err(|e| CliConfigError::Operation(e.to_string()))?;
    }
    if options.yes {
        config_file::set_setting_in_config("settings.auto_accept", "true", config)
            .map_err(|e| CliConfigError::Operation(e.to_string()))?;
    }
    if let Some(ref v) = options.storage_profile_id {
        config_file::set_setting_in_config("settings.storage_profile_id", v, config)
            .map_err(|e| CliConfigError::Operation(e.to_string()))?;
    }
    if let Some(ref v) = options.conflict_resolution {
        config_file::set_setting_in_config("settings.conflict_resolution", v, config)
            .map_err(|e| CliConfigError::Operation(e.to_string()))?;
    }

    for &req in required {
        let setting_name = format!("defaults.{req}");
        let value = config_file::get_setting_with_config(&setting_name, config)
            .map_err(|e| CliConfigError::Operation(e.to_string()))?;

        if value.is_empty() {
            let flag = req.replace('_', "-");
            return Err(CliConfigError::MissingRequired(format!(
                "Missing '--{flag}' or default {} configuration",
                match req {
                    "farm_id" => "Farm ID",
                    "queue_id" => "Queue ID",
                    "job_id" => "Job ID",
                    other => other,
                }
            )));
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// JSON output formatting
// ---------------------------------------------------------------------------

/// Serialize a JSON value with spaces after `:` and `,` to match Python's
/// `json.dumps()` default format. `serde_json`'s compact format omits spaces;
/// `to_string_pretty` adds newlines. This produces single-line spaced JSON.
pub fn json_with_spaces(value: &serde_json::Value) -> String {
    let compact = serde_json::to_string(value).unwrap_or_else(|_| format!("{value}"));
    // Insert space after : and , that aren't inside strings.
    let mut result = String::with_capacity(compact.len() * 2);
    let mut in_string = false;
    let mut prev = '\0';
    for ch in compact.chars() {
        if ch == '"' && prev != '\\' {
            in_string = !in_string;
        }
        result.push(ch);
        if !in_string && (ch == ':' || ch == ',') {
            result.push(' ');
        }
        prev = ch;
    }
    result
}

// ---------------------------------------------------------------------------
// YAML output formatting
// ---------------------------------------------------------------------------

/// Regex matching a bare YAML 1.1 boolean as a mapping value or sequence item.
/// serde_yaml follows YAML 1.2 (only true/false are booleans), so it leaves
/// ON/OFF/YES/NO etc. unquoted. Downstream YAML 1.1 parsers (PyYAML) would
/// interpret them as booleans, corrupting data.
static YAML_11_BOOL_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?m)(: |^[ \t]*- )(y|Y|yes|Yes|YES|n|N|no|No|NO|true|True|TRUE|false|False|FALSE|on|On|ON|off|Off|OFF)$").unwrap()
});

/// Format a JSON value as YAML for CLI output.
/// Multi-line strings that don't end with \n get one appended so YAML
/// uses |-style block scalars. Strings matching YAML 1.1 boolean literals
/// are single-quoted to prevent misinterpretation by YAML 1.1 parsers.
pub fn cli_object_repr(obj: &serde_json::Value) -> String {
    let fixed = fix_multiline_strings(obj);
    let yaml = serde_yaml::to_string(&fixed).unwrap_or_else(|_| format!("{obj}"));
    YAML_11_BOOL_RE.replace_all(&yaml, "$1'$2'").into_owned()
}

fn fix_multiline_strings(val: &serde_json::Value) -> serde_json::Value {
    match val {
        serde_json::Value::String(s) if s.contains('\n') && !s.ends_with('\n') => {
            serde_json::Value::String(format!("{s}\n"))
        }
        serde_json::Value::Object(map) => {
            serde_json::Value::Object(
                map.iter()
                    .map(|(k, v)| (k.clone(), fix_multiline_strings(v)))
                    .collect(),
            )
        }
        serde_json::Value::Array(arr) => {
            serde_json::Value::Array(arr.iter().map(fix_multiline_strings).collect())
        }
        other => other.clone(),
    }
}

// ---------------------------------------------------------------------------
// File and parameter parsing
// ---------------------------------------------------------------------------

use std::collections::HashMap;
use std::path::Path;

/// Parse a file as JSON (if .json extension) or YAML (otherwise) into a map.
pub fn parse_file_parameter(path: &Path) -> Result<HashMap<String, serde_json::Value>, String> {
    let path = if path.starts_with("~") {
        if let Some(home) = std::env::var_os("HOME") {
            std::path::PathBuf::from(home).join(path.strip_prefix("~").unwrap())
        } else {
            path.to_path_buf()
        }
    } else {
        path.to_path_buf()
    };

    if !path.exists() {
        return Err(format!("Provided file '{}' does not exist.", path.display()));
    }
    if !path.is_file() {
        return Err(format!("Provided file '{}' is not a file.", path.display()));
    }

    let content = std::fs::read_to_string(&path)
        .map_err(|e| format!("Could not open file '{}': {e}", path.display()))?;

    let data: serde_json::Value = if path.extension().is_some_and(|ext| ext.eq_ignore_ascii_case("json")) {
        serde_json::from_str(&content)
            .map_err(|e| format!("File '{}' is formatted incorrectly: {e}", path.display()))?
    } else {
        serde_yaml::from_str(&content)
            .map_err(|e| format!("File '{}' is formatted incorrectly: {e}", path.display()))?
    };

    match data {
        serde_json::Value::Object(map) => {
            Ok(map.into_iter().collect())
        }
        _ => Err(format!("File '{}' should contain a dictionary.", path.display())),
    }
}

/// Parse a list of parameters in mixed formats: key=value, inline JSON, file://path.
pub fn parse_multi_format_parameters(params: &[String]) -> Result<HashMap<String, serde_json::Value>, String> {
    let mut result = HashMap::new();

    for param in params {
        let param = param.trim();

        if let Some(file_path) = param.strip_prefix("file://") {
            let data = parse_file_parameter(Path::new(file_path))?;
            result.extend(data);
        } else if let Ok(data) = serde_json::from_str::<serde_json::Value>(param) {
            // Try inline JSON
            match data {
                serde_json::Value::Object(map) => {
                    result.extend(map.into_iter().collect::<HashMap<_, _>>());
                }
                _ => {
                    return Err(format!(
                        "Argument ('{param}') must contain a dictionary mapping keys to their values."
                    ));
                }
            }
        } else if let Some((key, val)) = param.split_once('=') {
            result.insert(key.to_string(), serde_json::Value::String(val.to_string()));
        } else {
            return Err(format!(
                "Parameter ('{param}') not formatted correctly. It must be key=value pairs, \
                 inline JSON, or a path to a JSON or YAML document prefixed with 'file://'."
            ));
        }
    }

    Ok(result)
}

// ---------------------------------------------------------------------------
// SIGINT handling
// ---------------------------------------------------------------------------

use std::sync::atomic::{AtomicBool, Ordering};

/// Global flag set to false on SIGINT. Long-running operations check this
/// to gracefully stop.
static CONTINUE_OPERATION: AtomicBool = AtomicBool::new(true);

/// Install the SIGINT handler. Safe to call multiple times.
pub fn install_sigint_handler() {
    unsafe {
        libc::signal(libc::SIGINT, sigint_handler as *const () as usize);
    }
}

extern "C" fn sigint_handler(_sig: libc::c_int) {
    CONTINUE_OPERATION.store(false, Ordering::SeqCst);
}

/// Check whether the operation should continue (no SIGINT received).
pub fn should_continue() -> bool {
    CONTINUE_OPERATION.load(Ordering::SeqCst)
}

/// Reset the flag (for testing).
#[cfg(test)]
fn reset_sigint_flag() {
    CONTINUE_OPERATION.store(true, Ordering::SeqCst);
}

// ---------------------------------------------------------------------------
// Progress bar
// ---------------------------------------------------------------------------

use indicatif::{ProgressBar, ProgressStyle};

/// Manages a progress bar lifecycle: created on first callback, updated
/// incrementally, closed at 100% or on SIGINT.
pub struct ProgressBarManager {
    length: u64,
    label: String,
    bar: Option<ProgressBar>,
}

impl ProgressBarManager {
    pub fn new(length: u64, label: &str) -> Self {
        Self {
            length,
            label: label.to_string(),
            bar: None,
        }
    }

    /// Update progress. Returns whether the operation should continue.
    pub fn callback(&mut self, progress: u64) -> bool {
        if self.bar.is_none() {
            let bar = ProgressBar::new(self.length);
            bar.set_style(
                ProgressStyle::default_bar()
                    .template(&format!("{{bar:40}} {{pos}}/{{len}} {}", self.label))
                    .unwrap_or_else(|_| ProgressStyle::default_bar()),
            );
            self.bar = Some(bar);
        }

        if let Some(ref bar) = self.bar {
            bar.set_position(progress);

            if progress >= self.length || !should_continue() {
                bar.finish_and_clear();
                self.bar.take();
            }
        }

        should_continue()
    }

    /// Whether the bar has been created and not yet closed.
    #[cfg(test)]
    fn is_active(&self) -> bool {
        self.bar.is_some()
    }
}

// ---------------------------------------------------------------------------
// Timestamp formatting
// ---------------------------------------------------------------------------

use chrono::{DateTime, FixedOffset, Local, Utc};

/// Timestamp display format.
pub enum TimestampFormat {
    /// ISO 8601 in UTC
    Utc,
    /// ISO 8601 in local timezone
    Local,
    /// Time delta from a reference start time
    Relative { reference: DateTime<FixedOffset> },
}

impl TimestampFormat {
    /// Create a formatter, validating that the reference time has a timezone.
    pub fn new_relative(reference: DateTime<FixedOffset>) -> Self {
        Self::Relative { reference }
    }

    /// Format a timezone-aware timestamp.
    pub fn format(&self, ts: &DateTime<FixedOffset>) -> String {
        match self {
            Self::Utc => ts.with_timezone(&Utc).to_rfc3339(),
            Self::Local => ts.with_timezone(&Local).to_rfc3339(),
            Self::Relative { reference } => {
                let delta = *ts - *reference;
                format_timedelta(delta)
            }
        }
    }
}

/// Format a chrono::TimeDelta like Python's str(timedelta).
fn format_timedelta(d: chrono::TimeDelta) -> String {
    let total_secs = d.num_seconds();
    let nanos = d.subsec_nanos();
    let is_negative = total_secs < 0 || (total_secs == 0 && nanos < 0);
    let abs_secs = total_secs.unsigned_abs();
    let hours = abs_secs / 3600;
    let mins = (abs_secs % 3600) / 60;
    let secs = abs_secs % 60;
    let micros = nanos.unsigned_abs() / 1000;
    let prefix = if is_negative { "-" } else { "" };
    if micros > 0 {
        format!("{prefix}{hours}:{mins:02}:{secs:02}.{micros:06}")
    } else {
        format!("{prefix}{hours}:{mins:02}:{secs:02}")
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- strip_markdown_for_terminal --

    #[test]
    fn strip_markdown_inline_link_becomes_text_and_url() {
        assert_eq!(
            strip_markdown_for_terminal("[Deadline Cloud](https://example.com)"),
            "Deadline Cloud (https://example.com)"
        );
    }

    #[test]
    fn strip_markdown_reference_link_becomes_text() {
        let input = "[Deadline Cloud][dc]\n\n[dc]: https://example.com";
        let result = strip_markdown_for_terminal(input);
        assert!(result.contains("Deadline Cloud"));
        assert!(!result.contains("[dc]"));
        assert!(!result.contains("[dc]: https://example.com"));
    }

    #[test]
    fn strip_markdown_bold_markers_removed() {
        assert_eq!(strip_markdown_for_terminal("**bold text**"), "bold text");
    }

    #[test]
    fn strip_markdown_italic_markers_removed() {
        assert_eq!(
            strip_markdown_for_terminal("some *italic* words"),
            "some italic words"
        );
    }

    #[test]
    fn strip_markdown_plain_text_unchanged() {
        assert_eq!(
            strip_markdown_for_terminal("no markdown here"),
            "no markdown here"
        );
    }

    // -- apply_cli_options_to_config --

    fn empty_config() -> deadline_config::ini::IniConfig {
        deadline_config::ini::IniConfig::new()
    }

    #[test]
    fn apply_options_all_provided_updates_config() {
        let mut config = empty_config();
        let opts = CliOptions {
            profile: Some("my-profile".into()),
            farm_id: Some("farm-abc".into()),
            queue_id: Some("queue-xyz".into()),
            job_id: None,
            yes: false,
            ..Default::default()
        };
        apply_cli_options_to_config(&mut config, &opts, &[]).unwrap();

        assert_eq!(
            deadline_config::config_file::get_setting_with_config(
                "defaults.aws_profile_name",
                &config
            )
            .unwrap(),
            "my-profile"
        );
        assert_eq!(
            deadline_config::config_file::get_setting_with_config("defaults.farm_id", &config)
                .unwrap(),
            "farm-abc"
        );
    }

    #[test]
    fn apply_options_none_provided_leaves_config_unchanged() {
        let mut config = empty_config();
        let opts = CliOptions::default();
        apply_cli_options_to_config(&mut config, &opts, &[]).unwrap();

        // aws_profile_name should still be the default "(default)"
        let val = deadline_config::config_file::get_setting_with_config(
            "defaults.aws_profile_name",
            &config,
        )
        .unwrap();
        assert_eq!(val, "(default)");
    }

    #[test]
    fn apply_options_missing_required_farm_id_returns_error() {
        let mut config = empty_config();
        let opts = CliOptions::default();
        let result = apply_cli_options_to_config(&mut config, &opts, &["farm_id"]);
        match result {
            Err(CliConfigError::MissingRequired(msg)) => assert!(msg.contains("--farm-id")),
            other => panic!("expected MissingRequired, got {other:?}"),
        }
    }

    #[test]
    fn apply_options_missing_required_queue_id_returns_error() {
        let mut config = empty_config();
        let opts = CliOptions::default();
        let result = apply_cli_options_to_config(&mut config, &opts, &["queue_id"]);
        match result {
            Err(CliConfigError::MissingRequired(msg)) => assert!(msg.contains("--queue-id")),
            other => panic!("expected MissingRequired, got {other:?}"),
        }
    }

    #[test]
    fn apply_options_missing_required_job_id_returns_error() {
        let mut config = empty_config();
        let opts = CliOptions::default();
        let result = apply_cli_options_to_config(&mut config, &opts, &["job_id"]);
        match result {
            Err(CliConfigError::MissingRequired(msg)) => assert!(msg.contains("--job-id")),
            other => panic!("expected MissingRequired, got {other:?}"),
        }
    }

    #[test]
    fn apply_options_yes_flag_sets_auto_accept() {
        let mut config = empty_config();
        let opts = CliOptions {
            yes: true,
            ..Default::default()
        };
        apply_cli_options_to_config(&mut config, &opts, &[]).unwrap();

        let val = deadline_config::config_file::get_setting_with_config(
            "settings.auto_accept",
            &config,
        )
        .unwrap();
        assert_eq!(val, "true");
    }

    // AUDIT-022: storage_profile_id should be applied to config
    #[test]
    fn apply_options_storage_profile_id_sets_config() {
        let mut config = empty_config();
        let opts = CliOptions {
            storage_profile_id: Some("sp-abc".into()),
            ..Default::default()
        };
        apply_cli_options_to_config(&mut config, &opts, &[]).unwrap();
        let val = deadline_config::config_file::get_setting_with_config(
            "settings.storage_profile_id", &config,
        ).unwrap();
        assert_eq!(val, "sp-abc");
    }

    // AUDIT-022: conflict_resolution should be applied to config
    #[test]
    fn apply_options_conflict_resolution_sets_config() {
        let mut config = empty_config();
        let opts = CliOptions {
            conflict_resolution: Some("CREATE_COPY".into()),
            ..Default::default()
        };
        apply_cli_options_to_config(&mut config, &opts, &[]).unwrap();
        let val = deadline_config::config_file::get_setting_with_config(
            "settings.conflict_resolution", &config,
        ).unwrap();
        assert_eq!(val, "CREATE_COPY");
    }

    // -- parse_file_parameter --

    #[test]
    fn parse_file_parameter_valid_json() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("params.json");
        std::fs::write(&path, r#"{"key": "value"}"#).unwrap();
        let result = parse_file_parameter(&path).unwrap();
        assert_eq!(result["key"], serde_json::Value::String("value".into()));
    }

    #[test]
    fn parse_file_parameter_valid_yaml() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("params.yaml");
        std::fs::write(&path, "key: value\n").unwrap();
        let result = parse_file_parameter(&path).unwrap();
        assert_eq!(result["key"], serde_json::Value::String("value".into()));
    }

    #[test]
    fn parse_file_parameter_missing_file_returns_error() {
        let result = parse_file_parameter(Path::new("/nonexistent/file.json"));
        assert!(result.unwrap_err().contains("does not exist"));
    }

    #[test]
    fn parse_file_parameter_directory_returns_error() {
        let dir = tempfile::TempDir::new().unwrap();
        let result = parse_file_parameter(dir.path());
        assert!(result.unwrap_err().contains("is not a file"));
    }

    #[test]
    fn parse_file_parameter_invalid_content_returns_error() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("bad.json");
        std::fs::write(&path, "not valid json{{{").unwrap();
        let result = parse_file_parameter(&path);
        assert!(result.unwrap_err().contains("formatted incorrectly"));
    }

    #[test]
    fn parse_file_parameter_list_not_dict_returns_error() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("list.json");
        std::fs::write(&path, "[1, 2, 3]").unwrap();
        let result = parse_file_parameter(&path);
        assert!(result.unwrap_err().contains("should contain a dictionary"));
    }

    // -- parse_multi_format_parameters --

    #[test]
    fn parse_multi_format_key_value_pairs() {
        let params = vec!["key1=value1".into(), "key2=value2".into()];
        let result = parse_multi_format_parameters(&params).unwrap();
        assert_eq!(result["key1"], serde_json::Value::String("value1".into()));
        assert_eq!(result["key2"], serde_json::Value::String("value2".into()));
    }

    #[test]
    fn parse_multi_format_inline_json() {
        let params = vec![r#"{"key": "value"}"#.into()];
        let result = parse_multi_format_parameters(&params).unwrap();
        assert_eq!(result["key"], serde_json::Value::String("value".into()));
    }

    #[test]
    fn parse_multi_format_file_prefix() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("params.json");
        std::fs::write(&path, r#"{"from_file": "yes"}"#).unwrap();
        let params = vec![format!("file://{}", path.display())];
        let result = parse_multi_format_parameters(&params).unwrap();
        assert_eq!(result["from_file"], serde_json::Value::String("yes".into()));
    }

    #[test]
    fn parse_multi_format_mixed_later_overrides() {
        let params = vec!["key=first".into(), r#"{"key": "second"}"#.into()];
        let result = parse_multi_format_parameters(&params).unwrap();
        assert_eq!(result["key"], serde_json::Value::String("second".into()));
    }

    #[test]
    fn parse_multi_format_malformed_returns_error() {
        let params = vec!["no-equals-no-json".into()];
        let result = parse_multi_format_parameters(&params);
        assert!(result.unwrap_err().contains("not formatted correctly"));
    }

    #[test]
    fn parse_multi_format_non_dict_json_returns_error() {
        let params = vec!["[1, 2, 3]".into()];
        let result = parse_multi_format_parameters(&params);
        assert!(result.unwrap_err().contains("must contain a dictionary"));
    }

    // -- cli_object_repr --

    #[test]
    fn cli_object_repr_dict_with_string_values_formats_as_yaml() {
        let obj = serde_json::json!({"name": "My Farm", "id": "farm-abc"});
        let result = cli_object_repr(&obj);
        assert!(result.contains("name: My Farm"));
        assert!(result.contains("id: farm-abc"));
    }

    #[test]
    fn cli_object_repr_multiline_string_uses_block_scalar() {
        let obj = serde_json::json!({"log": "line1\nline2"});
        let result = cli_object_repr(&obj);
        // The |-style block scalar indicator should appear
        assert!(result.contains('|'), "should use block scalar: {result}");
    }

    // C-3: YAML 1.1 boolean-like strings must be single-quoted to prevent
    // data corruption when parsed by YAML 1.1 consumers (e.g. PyYAML).
    #[test]
    fn cli_object_repr_yaml_11_booleans_are_quoted() {
        let obj = serde_json::json!({
            "enabled": "ON",
            "disabled": "OFF",
            "answer": "YES",
            "negative": "NO",
            "normal": "hello",
        });
        let result = cli_object_repr(&obj);
        assert!(result.contains("'ON'"), "ON should be quoted: {result}");
        assert!(result.contains("'OFF'"), "OFF should be quoted: {result}");
        assert!(result.contains("'YES'"), "YES should be quoted: {result}");
        assert!(result.contains("'NO'"), "NO should be quoted: {result}");
        assert!(!result.contains("'hello'"), "normal string should not be quoted: {result}");
    }

    #[test]
    fn cli_object_repr_yaml_11_booleans_in_nested_values() {
        let obj = serde_json::json!({
            "params": {"multiFrame": {"string": "OFF"}, "other": {"string": "ON"}}
        });
        let result = cli_object_repr(&obj);
        assert!(result.contains("'OFF'"), "nested OFF should be quoted: {result}");
        assert!(result.contains("'ON'"), "nested ON should be quoted: {result}");
    }

    // -- TimestampFormat --

    #[test]
    fn timestamp_utc_formats_as_rfc3339_utc() {
        let ts = DateTime::parse_from_rfc3339("2024-06-15T10:30:00-07:00").unwrap();
        let result = TimestampFormat::Utc.format(&ts);
        assert!(result.contains("17:30:00"), "should be converted to UTC: {result}");
        assert!(result.ends_with("+00:00"), "should have UTC offset: {result}");
    }

    #[test]
    fn timestamp_local_formats_in_local_timezone() {
        let ts = DateTime::parse_from_rfc3339("2024-06-15T10:30:00+00:00").unwrap();
        let result = TimestampFormat::Local.format(&ts);
        // Can't assert exact timezone, but should be valid RFC 3339
        assert!(result.contains("2024"), "should contain year: {result}");
    }

    #[test]
    fn timestamp_relative_formats_as_timedelta() {
        let ref_time = DateTime::parse_from_rfc3339("2024-06-15T10:00:00+00:00").unwrap();
        let ts = DateTime::parse_from_rfc3339("2024-06-15T11:30:45+00:00").unwrap();
        let fmt = TimestampFormat::new_relative(ref_time);
        assert_eq!(fmt.format(&ts), "1:30:45");
    }

    // AUDIT-023: Negative timedelta should produce "-H:MM:SS"
    #[test]
    fn format_timedelta_negative_thirty_minutes() {
        assert_eq!(format_timedelta(chrono::TimeDelta::minutes(-30)), "-0:30:00");
    }

    #[test]
    fn format_timedelta_negative_one_hour_fifteen() {
        assert_eq!(format_timedelta(chrono::TimeDelta::minutes(-75)), "-1:15:00");
    }

    // Cases 38-39: In Rust, DateTime<FixedOffset> always has a timezone.
    // The "no timezone" error cases from Python are prevented at compile time
    // — you cannot construct a DateTime<FixedOffset> without a timezone.

    // -- SigIntHandler --
    // These tests share global CONTINUE_OPERATION state and must run serially.

    use serial_test::serial;

    #[test]
    #[serial]
    fn sigint_continue_operation_defaults_to_true() {
        reset_sigint_flag();
        assert!(should_continue());
    }

    #[test]
    #[serial]
    fn sigint_signal_sets_continue_to_false() {
        reset_sigint_flag();
        install_sigint_handler();
        assert!(should_continue());

        // Send SIGINT to ourselves
        unsafe { libc::raise(libc::SIGINT); }

        assert!(!should_continue());
        // Restore for other tests
        reset_sigint_flag();
    }

    // Case 47 (singleton): In Rust, CONTINUE_OPERATION is a static AtomicBool.
    // There's no instantiation — it's inherently a single global value.

    // -- ProgressBarManager --

    #[test]
    #[serial]
    fn progress_bar_first_callback_creates_bar() {
        reset_sigint_flag();
        let mut mgr = ProgressBarManager::new(100, "test");
        assert!(!mgr.is_active());
        mgr.callback(0);
        assert!(mgr.is_active());
    }

    #[test]
    #[serial]
    fn progress_bar_update_advances_position() {
        reset_sigint_flag();
        let mut mgr = ProgressBarManager::new(100, "test");
        let cont = mgr.callback(50);
        assert!(cont);
        assert!(mgr.is_active());
    }

    #[test]
    #[serial]
    fn progress_bar_100_percent_closes_bar() {
        reset_sigint_flag();
        let mut mgr = ProgressBarManager::new(100, "test");
        mgr.callback(50);
        mgr.callback(100);
        assert!(!mgr.is_active());
    }

    #[test]
    #[serial]
    fn progress_bar_sigint_closes_and_returns_false() {
        reset_sigint_flag();
        install_sigint_handler();
        let mut mgr = ProgressBarManager::new(100, "test");
        mgr.callback(10);

        // Simulate SIGINT
        CONTINUE_OPERATION.store(false, Ordering::SeqCst);
        let cont = mgr.callback(20);
        assert!(!cont);
        assert!(!mgr.is_active());
        reset_sigint_flag();
    }

    #[test]
    #[serial]
    fn progress_bar_callback_after_close_returns_continue() {
        reset_sigint_flag();
        let mut mgr = ProgressBarManager::new(100, "test");
        mgr.callback(100); // closes
        let cont = mgr.callback(100); // after close
        assert!(cont); // should_continue is still true
    }
}
