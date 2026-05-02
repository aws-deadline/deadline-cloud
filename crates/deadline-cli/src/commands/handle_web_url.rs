use clap::Args;
use std::collections::HashMap;
use std::io::BufRead;

use deadline_config::config_file;

use super::config::CliError;
use super::job::download_output_impl;

const DEADLINE_URL_SCHEME: &str = "deadline";

#[derive(Args)]
pub(crate) struct HandleWebUrlArgs {
    /// deadline:// protocol URL
    pub url: Option<String>,

    /// Register as deadline:// URL handler
    #[arg(long)]
    pub install: bool,

    /// Unregister URL handler
    #[arg(long)]
    pub uninstall: bool,

    /// System-wide install/uninstall
    #[arg(long)]
    pub all_users: bool,

    /// Wait for keypress at end
    #[arg(long)]
    pub prompt_when_complete: bool,
}

pub(crate) fn run(args: HandleWebUrlArgs) -> Result<(), CliError> {
    let result = tokio::runtime::Runtime::new()
        .map_err(|e| CliError::Operation(e.to_string()))?
        .block_on(run_async(&args));

    if args.prompt_when_complete {
        eprint!("Press Enter To Exit");
        let _ = std::io::stdin().lock().read_line(&mut String::new());
    }

    result
}

async fn run_async(args: &HandleWebUrlArgs) -> Result<(), CliError> {
    if let Some(ref url) = args.url {
        // URL mode — cannot combine with install/uninstall flags
        if args.install || args.uninstall || args.all_users {
            return Err(CliError::Operation(
                "The --install, --uninstall and --all-users options cannot be used with a provided URL.".into()
            ));
        }
        handle_url(url).await
    } else if args.install && args.uninstall {
        Err(CliError::Operation(
            "Only one of the --install and --uninstall options may be provided.".into()
        ))
    } else if args.install {
        install_handler(args.all_users)?;
        println!("Web URL handler installed successfully.");
        Ok(())
    } else if args.uninstall {
        uninstall_handler(args.all_users)?;
        println!("Web URL handler uninstalled successfully.");
        Ok(())
    } else {
        Err(CliError::Operation(
            "At least one of a URL, --install, or --uninstall must be provided.".into()
        ))
    }
}

async fn handle_url(url: &str) -> Result<(), CliError> {
    // Split URL: scheme://netloc?query
    let (scheme, rest) = url.split_once("://")
        .ok_or_else(|| CliError::Operation(format!(
            "URL scheme is not supported. Only {DEADLINE_URL_SCHEME} is supported."
        )))?;

    if scheme != DEADLINE_URL_SCHEME {
        return Err(CliError::Operation(format!(
            "URL scheme {scheme} is not supported. Only {DEADLINE_URL_SCHEME} is supported."
        )));
    }

    let (command, query) = rest.split_once('?').unwrap_or((rest, ""));

    match command {
        "download-output" => handle_download_output(query).await,
        _ => Err(CliError::Operation(format!(
            "Command {command} is not supported through handle-web-url."
        ))),
    }
}

async fn handle_download_output(query: &str) -> Result<(), CliError> {
    let params = parse_query_string(
        query,
        &["farm-id", "queue-id", "job-id", "step-id", "task-id", "profile"],
        &["farm-id", "queue-id", "job-id"],
    ).map_err(CliError::Operation)?;

    // Validate resource IDs (exclude profile)
    let id_params: HashMap<String, String> = params.iter()
        .filter(|(k, _)| *k != "profile")
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    validate_resource_ids(&id_params).map_err(CliError::Operation)?;

    let farm_id = &params["farm_id"];
    let queue_id = &params["queue_id"];
    let job_id = &params["job_id"];
    let step_id = params.get("step_id").map(String::as_str);
    let task_id = params.get("task_id").map(String::as_str);

    // Resolve AWS profile: use URL-provided profile, or find best match
    let profile = if let Some(p) = params.get("profile") { p.clone() } else {
        let config = config_file::read_config()
            .map_err(|e| CliError::Operation(e.to_string()))?;
        let aws_profiles = read_aws_profile_names();
        let profile_refs: Vec<&str> = aws_profiles.iter().map(String::as_str).collect();
        config_file::get_best_profile_for_farm(&config, &profile_refs, farm_id, Some(queue_id))
    };

    // Build config with the resolved profile
    let mut config = config_file::read_config()
        .map_err(|e| CliError::Operation(e.to_string()))?;
    config_file::set_setting("defaults.aws_profile_name", &profile, &mut config)
        .map_err(|e| CliError::Operation(e.to_string()))?;

    download_output_impl(&config, farm_id, queue_id, job_id, step_id, task_id, None, false, false).await
}

// -----------------------------------------------------------------------
// Install / uninstall
// -----------------------------------------------------------------------

#[cfg(target_os = "windows")]
fn install_handler(all_users: bool) -> Result<(), CliError> {
    use std::path::PathBuf;

    let exe = std::env::current_exe()
        .map_err(|e| CliError::Operation(format!("Failed to determine CLI path: {e}")))?;
    let exe_str = exe.display().to_string();

    if !exe.exists() {
        return Err(CliError::Operation(format!(
            "Error determining the AWS Deadline Cloud CLI program, {exe_str} does not exist."
        )));
    }

    let command_value = format!("\"{exe_str}\" handle-web-url \"%1\" --prompt-when-complete");

    // Use winreg crate for Windows registry operations
    use winreg::enums::*;
    use winreg::RegKey;

    let result = (|| -> std::io::Result<()> {
        let hkey = if all_users {
            RegKey::predef(HKEY_CLASSES_ROOT).create_subkey(DEADLINE_URL_SCHEME)?.0
        } else {
            RegKey::predef(HKEY_CURRENT_USER)
                .create_subkey(format!("Software\\Classes\\{DEADLINE_URL_SCHEME}"))?.0
        };
        hkey.set_value("", &"URL:AWS Deadline Cloud Protocol")?;
        hkey.set_value("URL Protocol", &"")?;
        let cmd_key = hkey.create_subkey("shell\\open\\command")?.0;
        cmd_key.set_value("", &command_value)?;
        Ok(())
    })();

    result.map_err(|e| {
        if all_users && e.raw_os_error() == Some(5) {
            CliError::Operation(format!(
                "Administrator access is required to install the {DEADLINE_URL_SCHEME} URL handler for all users:\n{e}"
            ))
        } else {
            CliError::Operation(format!(
                "Failed to install the handler for {DEADLINE_URL_SCHEME} URLs:\n{e}"
            ))
        }
    })
}

#[cfg(target_os = "linux")]
fn install_handler(all_users: bool) -> Result<(), CliError> {
    use std::process::Command;

    // Check update-desktop-database is available
    if which::which("update-desktop-database").is_err() {
        return Err(CliError::Operation(format!(
            "Failed to install the handler for {DEADLINE_URL_SCHEME} URLs: update-desktop-database is not installed."
        )));
    }

    let exe = std::env::current_exe()
        .map_err(|e| CliError::Operation(format!("Failed to determine CLI path: {e}")))?;
    let exe_str = exe.display().to_string();

    let (entry_dir, mimeapps_path) = if all_users {
        ("/usr/share/applications".into(), "/usr/share/applications/mimeapps.list".into())
    } else {
        let home = std::env::var("HOME").unwrap_or_default();
        (
            format!("{home}/.local/share/applications"),
            format!("{home}/.config/mimeapps.list"),
        )
    };

    std::fs::create_dir_all(&entry_dir)
        .map_err(|e| CliError::Operation(format!("Failed to create a directory: {e}")))?;

    let desktop_content = format!(
        "[Desktop Entry]\nType=Application\nName={DEADLINE_URL_SCHEME}\nExec={exe_str} handle-web-url %u\nTerminal=true\nMimeType=x-scheme-handler/{DEADLINE_URL_SCHEME}\n"
    );
    let mimeapps_content = format!(
        "[Default Applications]\nx-scheme-handler/{DEADLINE_URL_SCHEME}={DEADLINE_URL_SCHEME}.desktop;\n"
    );

    std::fs::write(format!("{entry_dir}/{DEADLINE_URL_SCHEME}.desktop"), desktop_content)
        .map_err(|e| CliError::Operation(format!("Failed to write desktop file: {e}")))?;
    std::fs::write(&mimeapps_path, mimeapps_content)
        .map_err(|e| CliError::Operation(format!("Failed to write mimeapps.list: {e}")))?;

    Command::new("update-desktop-database").arg(&entry_dir).status()
        .map_err(|e| CliError::Operation(format!(
            "Failed to install the handler for {DEADLINE_URL_SCHEME} URLs:\n{e}"
        )))?;

    Ok(())
}

#[cfg(not(any(target_os = "windows", target_os = "linux")))]
fn install_handler(_all_users: bool) -> Result<(), CliError> {
    Err(CliError::Operation(
        "Installing the web URL handler is only supported on Windows and Linux".into()
    ))
}

#[cfg(target_os = "windows")]
fn uninstall_handler(all_users: bool) -> Result<(), CliError> {
    use winreg::enums::*;
    use winreg::RegKey;

    let result = (|| -> std::io::Result<()> {
        if all_users {
            let hkey = RegKey::predef(HKEY_CLASSES_ROOT);
            hkey.delete_subkey_all(DEADLINE_URL_SCHEME)?;
        } else {
            let hkey = RegKey::predef(HKEY_CURRENT_USER)
                .open_subkey("Software\\Classes")?;
            hkey.delete_subkey_all(DEADLINE_URL_SCHEME)?;
        }
        Ok(())
    })();

    match result {
        Ok(()) => Ok(()),
        Err(e) if e.raw_os_error() == Some(2) => {
            println!("Nothing to uninstall, no handler for {DEADLINE_URL_SCHEME} URLs was installed");
            Ok(())
        }
        Err(e) => Err(CliError::Operation(format!(
            "Failed to uninstall handler for {DEADLINE_URL_SCHEME} URLs:\n{e}"
        ))),
    }
}

#[cfg(target_os = "linux")]
fn uninstall_handler(all_users: bool) -> Result<(), CliError> {
    use std::process::Command;

    if which::which("update-desktop-database").is_err() {
        return Err(CliError::Operation(format!(
            "Failed to uninstall the handler for {DEADLINE_URL_SCHEME} URLs: update-desktop-database is not installed."
        )));
    }

    let entry_dir = if all_users {
        "/usr/share/applications".into()
    } else {
        let home = std::env::var("HOME").unwrap_or_default();
        format!("{home}/.local/share/applications")
    };

    let desktop_file = format!("{entry_dir}/{DEADLINE_URL_SCHEME}.desktop");
    match std::fs::remove_file(&desktop_file) {
        Ok(()) => println!("Removed {desktop_file}"),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            println!("{desktop_file} not found, nothing to remove");
        }
        Err(e) => return Err(CliError::Operation(format!("Failed to remove {desktop_file}: {e}"))),
    }

    Command::new("update-desktop-database").arg(&entry_dir).status()
        .map_err(|e| CliError::Operation(format!(
            "Failed to uninstall the handler for {DEADLINE_URL_SCHEME} URLs:\n{e}"
        )))?;

    Ok(())
}

#[cfg(not(any(target_os = "windows", target_os = "linux")))]
fn uninstall_handler(_all_users: bool) -> Result<(), CliError> {
    Err(CliError::Operation(
        "Uninstalling the web URL handler is only supported on Windows and Linux".into()
    ))
}

// -----------------------------------------------------------------------
// URL parsing helpers
// -----------------------------------------------------------------------

/// Read AWS profile names from ~/.aws/config.
fn read_aws_profile_names() -> Vec<String> {
    let home = std::env::var("HOME").unwrap_or_default();
    let path = std::path::PathBuf::from(home).join(".aws").join("config");
    let content = std::fs::read_to_string(&path).unwrap_or_default();
    content.lines()
        .filter_map(|line| {
            let line = line.trim();
            if let Some(rest) = line.strip_prefix("[profile ") {
                rest.strip_suffix(']').map(ToOwned::to_owned)
            } else if line == "[default]" {
                Some("default".to_owned())
            } else {
                None
            }
        })
        .collect()
}

/// Parse a URL query string into a map, validating required/allowed params.
/// Dashes in parameter names are converted to underscores in the result.
pub(crate) fn parse_query_string(
    query: &str,
    parameter_names: &[&str],
    required_names: &[&str],
) -> Result<HashMap<String, String>, String> {
    let mut parsed: HashMap<String, Vec<String>> = HashMap::new();

    if !query.is_empty() {
        for pair in query.split('&') {
            let (key, value) = pair.split_once('=')
                .ok_or_else(|| format!("Malformed query parameter: {pair}"))?;
            parsed.entry(key.to_owned()).or_default().push(value.to_owned());
        }
    }

    // Check required
    let missing: Vec<&str> = required_names.iter()
        .filter(|name| !parsed.contains_key(**name))
        .copied()
        .collect();
    if !missing.is_empty() {
        return Err(format!(
            "The URL query did not contain the required parameter(s) {missing:?}"
        ));
    }

    let mut result = HashMap::new();
    for &name in parameter_names {
        if let Some(values) = parsed.remove(name) {
            if values.len() > 1 {
                return Err(format!(
                    "The URL query parameter {name} was provided multiple times, it may only be provided once."
                ));
            }
            result.insert(name.replace('-', "_"), values.into_iter().next().expect("values is non-empty"));
        }
    }

    // Reject unknown params
    if !parsed.is_empty() {
        let unknown: Vec<&String> = parsed.keys().collect();
        return Err(format!(
            "The URL query contained unsupported parameter names {unknown:?}"
        ));
    }

    Ok(result)
}

/// Validate that a resource ID has the correct format.
/// Standard: `<resource>-<32 hex chars>`.
/// Task: `task-<32 hex chars>-<0 or number up to 10 digits>`.
pub(crate) fn validate_id_format(resource_type: &str, full_id: &str) -> bool {
    const VALID_RESOURCES: &[&str] = &["farm", "queue", "job", "step", "task"];
    if !VALID_RESOURCES.contains(&resource_type) {
        return false;
    }

    let prefix = format!("{resource_type}-");
    let Some(id_part) = full_id.strip_prefix(&prefix) else {
        return false;
    };

    if resource_type == "task" {
        let Some((hex, num)) = id_part.rsplit_once('-') else {
            return false;
        };
        hex.len() == 32
            && hex.chars().all(|c| c.is_ascii_hexdigit())
            && !num.is_empty()
            && num.len() <= 10
            && (num == "0" || !num.starts_with('0'))
            && num.chars().all(|c| c.is_ascii_digit())
    } else {
        id_part.len() == 32 && id_part.chars().all(|c| c.is_ascii_hexdigit())
    }
}

/// Validate a map of `{resource_type_id: full_id_string}` entries.
pub(crate) fn validate_resource_ids(ids: &HashMap<String, String>) -> Result<(), String> {
    for (id_name, id_str) in ids {
        let resource_type = id_str.split('-').next().unwrap_or("");
        if !id_name.starts_with(resource_type) || !validate_id_format(resource_type, id_str) {
            return Err(format!(
                "The given resource ID \"{id_name}\": \"{id_str}\" has invalid format."
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // -- parse_query_string --

    #[test]
    fn parse_query_string_valid_params() {
        let result = parse_query_string(
            "ab-c=def&x=73&xyz=testing-value",
            &["ab-c", "x", "xyz"],
            &[],
        ).unwrap();
        assert_eq!(result["ab_c"], "def");
        assert_eq!(result["x"], "73");
        assert_eq!(result["xyz"], "testing-value");
    }

    #[test]
    fn parse_query_string_with_required_params() {
        let result = parse_query_string("a=b&c=d", &["a", "c", "f", "g"], &["a", "c"]).unwrap();
        assert_eq!(result["a"], "b");
        assert_eq!(result["c"], "d");
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn parse_query_string_empty_query_no_required() {
        let result = parse_query_string("", &["a", "b"], &[]).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn parse_query_string_empty_query_with_required_errors() {
        let err = parse_query_string("", &["a"], &["a"]).unwrap_err();
        assert!(err.contains("did not contain the required parameter"));
    }

    #[test]
    fn parse_query_string_missing_required_errors() {
        let err = parse_query_string(
            "a-1=b&c=d",
            &["a-1", "c", "missing-required", "g"],
            &["a-1", "c", "missing-required"],
        ).unwrap_err();
        assert!(err.contains("did not contain the required parameter"));
        assert!(err.contains("missing-required"));
    }

    #[test]
    fn parse_query_string_multiple_missing_required_errors() {
        let err = parse_query_string(
            "a=b&c=d",
            &["a", "c", "missing-required", "also-not-here", "not-required"],
            &["a", "c", "missing-required", "also-not-here"],
        ).unwrap_err();
        assert!(err.contains("did not contain the required parameter"));
        assert!(err.contains("missing-required"));
        assert!(err.contains("also-not-here"));
        assert!(!err.contains("not-required"));
    }

    #[test]
    fn parse_query_string_extra_params_errors() {
        let err = parse_query_string(
            "a=b&c=d&extra-parameter=3",
            &["a", "c"],
            &["a"],
        ).unwrap_err();
        assert!(err.contains("contained unsupported parameter"));
        assert!(err.contains("extra-parameter"));
    }

    #[test]
    fn parse_query_string_duplicate_params_errors() {
        let err = parse_query_string(
            "duplicated-param=b&c=d&duplicated-param=e",
            &["duplicated-param", "c"],
            &["c"],
        ).unwrap_err();
        assert!(err.contains("provided multiple times"));
        assert!(err.contains("duplicated-param"));
    }

    #[test]
    fn parse_query_string_dash_to_underscore_conversion() {
        let result = parse_query_string("farm-id=abc", &["farm-id"], &[]).unwrap();
        assert!(result.contains_key("farm_id"));
        assert!(!result.contains_key("farm-id"));
    }

    // -- validate_id_format --

    #[test]
    fn validate_id_format_valid_resources() {
        assert!(validate_id_format("farm", "farm-0123456789abcdef0123456789abcdef"));
        assert!(validate_id_format("queue", "queue-0123456789abcdef0123456789abcdef"));
        assert!(validate_id_format("job", "job-0123456789abcdef0123456789abcdef"));
        assert!(validate_id_format("step", "step-0123456789abcdef0123456789abcdef"));
        assert!(validate_id_format("task", "task-0123456789abcdef0123456789abcdef-99"));
        assert!(validate_id_format("task", "task-0123456789abcdef0123456789abcdef-0"));
    }

    #[test]
    fn validate_id_format_invalid_cases() {
        assert!(!validate_id_format("farm", ""));
        assert!(!validate_id_format("farm", "farm-123"));
        assert!(!validate_id_format("farm", "farm0123456789abcdefabcdefabcdefabcd"));
        assert!(!validate_id_format("farm", "farm--0123456789abcdefabcdefabcdefabcd"));
        assert!(!validate_id_format("farm", "farm-0123456789abcdefabcdefabcdefabcd00000"));
        assert!(!validate_id_format("farm", "queue-0123456789abcdefabcdefabcdefabcd"));
        assert!(!validate_id_format("farm", "farm-0123456789abcdefabcdefabcdezxvzx"));
        assert!(!validate_id_format("farmfarm", "farmfarm-0123456789abcdefabcdefabcdefabcd"));
        assert!(!validate_id_format("mission", "mission-0123456789abcdefabcdefabcdefabcd"));
        assert!(!validate_id_format("task", "task-0123456789abcdefabcdefabcdefabcd"));
        assert!(!validate_id_format("task", "task-0123456789abcdefabcdefabcdefabcd-00"));
        assert!(!validate_id_format("task", "task-0123456789abcdefabcdefabcdefabcd-12345678912345"));
    }

    // -- validate_resource_ids --

    #[test]
    fn validate_resource_ids_valid() {
        let ids = HashMap::from([
            ("farm_id".into(), "farm-0123456789abcdef0123456789abcdef".into()),
            ("queue_id".into(), "queue-0123456789abcdef0123456789abcdef".into()),
            ("job_id".into(), "job-0123456789abcdef0123456789abcdef".into()),
        ]);
        assert!(validate_resource_ids(&ids).is_ok());
    }

    #[test]
    fn validate_resource_ids_with_task() {
        let ids = HashMap::from([
            ("farm_id".into(), "farm-0123456789abcdef0123456789abcdef".into()),
            ("task_id".into(), "task-0123456789abcdef0123456789abcdef-99".into()),
        ]);
        assert!(validate_resource_ids(&ids).is_ok());
    }

    #[test]
    fn validate_resource_ids_invalid_format() {
        let ids = HashMap::from([
            ("farm_id".into(), "farm-123".into()),
        ]);
        let err = validate_resource_ids(&ids).unwrap_err();
        assert!(err.contains("invalid format"));
        assert!(err.contains("farm-123"));
    }

    #[test]
    fn validate_resource_ids_mismatched_prefix() {
        let ids = HashMap::from([
            ("farm_id".into(), "queue-0123456789abcdef0123456789abcdef".into()),
        ]);
        let err = validate_resource_ids(&ids).unwrap_err();
        assert!(err.contains("invalid format"));
    }

    #[test]
    fn read_aws_profile_names_parses_config() {
        let dir = tempfile::tempdir().unwrap();
        let aws_dir = dir.path().join(".aws");
        std::fs::create_dir_all(&aws_dir).unwrap();
        std::fs::write(aws_dir.join("config"), "[default]\nregion=us-west-2\n\n[profile my-profile]\nregion=us-east-1\n").unwrap();
        let old_home = std::env::var("HOME").ok();
        unsafe { std::env::set_var("HOME", dir.path()); }
        let profiles = read_aws_profile_names();
        match old_home {
            Some(h) => unsafe { std::env::set_var("HOME", h); },
            None => unsafe { std::env::remove_var("HOME"); },
        }
        assert!(profiles.contains(&"default".to_owned()));
        assert!(profiles.contains(&"my-profile".to_owned()));
        assert_eq!(profiles.len(), 2);
    }
}
