use deadline_config::config_file;
use deadline_config::ini::IniConfig;

use super::config::CliError;

/// Apply --profile option to config, returning Some(config) if profile was set.
pub fn apply_profile(profile: Option<String>) -> Result<Option<IniConfig>, CliError> {
    match profile {
        Some(p) => {
            let mut config = config_file::read_config()?;
            config_file::set_setting_in_config("defaults.aws_profile_name", &p, &mut config)?;
            Ok(Some(config))
        }
        None => Ok(None),
    }
}

/// Get a required setting from CLI arg, config, or return an error.
pub fn require_setting(
    name: &str,
    cli_arg: Option<String>,
    setting_name: &str,
    config: Option<&IniConfig>,
) -> Result<String, CliError> {
    if let Some(v) = cli_arg {
        return Ok(v);
    }
    let v = match config {
        Some(c) => config_file::get_setting_with_config(setting_name, c).unwrap_or_default(),
        None => config_file::get_setting(setting_name).unwrap_or_default(),
    };
    if v.is_empty() {
        Err(CliError::Operation(format!(
            "Missing '--{n}' or default {label} configuration",
            n = name.replace('_', "-"),
            label = name.replace('_', " ").replace("id", "ID"),
        )))
    } else {
        Ok(v)
    }
}
