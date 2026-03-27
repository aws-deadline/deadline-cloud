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
    pub yes: bool,
}

/// Apply CLI flag overrides to a config and validate required options.
pub fn apply_cli_options_to_config(
    config: &mut deadline_config::ini::IniConfig,
    options: &CliOptions,
    required: &[&str],
) -> Result<(), String> {
    use deadline_config::config_file;

    if let Some(ref v) = options.profile {
        config_file::set_setting_in_config("defaults.aws_profile_name", v, config)
            .map_err(|e| e.to_string())?;
    }
    if let Some(ref v) = options.farm_id {
        config_file::set_setting_in_config("defaults.farm_id", v, config)
            .map_err(|e| e.to_string())?;
    }
    if let Some(ref v) = options.queue_id {
        config_file::set_setting_in_config("defaults.queue_id", v, config)
            .map_err(|e| e.to_string())?;
    }
    if let Some(ref v) = options.job_id {
        config_file::set_setting_in_config("defaults.job_id", v, config)
            .map_err(|e| e.to_string())?;
    }
    if options.yes {
        config_file::set_setting_in_config("settings.auto_accept", "true", config)
            .map_err(|e| e.to_string())?;
    }

    for &req in required {
        let setting_name = format!("defaults.{req}");
        let value = config_file::get_setting_with_config(&setting_name, config)
            .map_err(|e| e.to_string())?;

        if value.is_empty() {
            let flag = req.replace('_', "-");
            return Err(format!(
                "Missing '--{flag}' or default {} configuration",
                match req {
                    "farm_id" => "Farm ID",
                    "queue_id" => "Queue ID",
                    "job_id" => "Job ID",
                    other => other,
                }
            ));
        }
    }

    Ok(())
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
    let hours = total_secs / 3600;
    let mins = (total_secs % 3600) / 60;
    let secs = total_secs % 60;
    let micros = d.subsec_nanos() / 1000;
    if micros > 0 {
        format!("{hours}:{mins:02}:{secs:02}.{micros:06}")
    } else {
        format!("{hours}:{mins:02}:{secs:02}")
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
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("--farm-id"));
    }

    #[test]
    fn apply_options_missing_required_queue_id_returns_error() {
        let mut config = empty_config();
        let opts = CliOptions::default();
        let result = apply_cli_options_to_config(&mut config, &opts, &["queue_id"]);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("--queue-id"));
    }

    #[test]
    fn apply_options_missing_required_job_id_returns_error() {
        let mut config = empty_config();
        let opts = CliOptions::default();
        let result = apply_cli_options_to_config(&mut config, &opts, &["job_id"]);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("--job-id"));
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

    // Cases 38-39: In Rust, DateTime<FixedOffset> always has a timezone.
    // The "no timezone" error cases from Python are prevented at compile time
    // — you cannot construct a DateTime<FixedOffset> without a timezone.

    // -- SigIntHandler --

    #[test]
    fn sigint_continue_operation_defaults_to_true() {
        reset_sigint_flag();
        assert!(should_continue());
    }

    #[test]
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
}
