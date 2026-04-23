use deadline_config::config_file;
use textwrap;
use crate::common::json_with_spaces;

/// CLI-specific error type that distinguishes known operation errors
/// from unexpected errors for the error handler in main.rs.
#[derive(Debug, thiserror::Error)]
#[allow(dead_code)]
pub enum CliError {
    #[error("{0}")]
    Operation(String),
    #[error("{0}")]
    Config(#[from] deadline_config::config_file::ConfigError),
    /// Exit with a specific code after printing output normally.
    /// The message (if any) has already been printed by the command.
    #[error("{message}")]
    ExitCode { code: i32, message: String },
}

impl From<crate::common::CliConfigError> for CliError {
    fn from(e: crate::common::CliConfigError) -> Self {
        match e {
            crate::common::CliConfigError::Operation(msg) => CliError::Operation(msg),
            crate::common::CliConfigError::MissingRequired(msg) => {
                CliError::ExitCode { code: 2, message: msg }
            }
        }
    }
}

#[derive(clap::Subcommand)]
pub enum ConfigAction {
    /// Show all workstation configuration settings and current values
    Show {
        #[arg(long, default_value = "verbose")]
        output: OutputFormat,
    },
    /// Print the value of a workstation configuration setting
    Get {
        setting_name: String,
    },
    /// Set a workstation configuration setting
    Set {
        setting_name: String,
        value: String,
    },
    /// Clear a workstation configuration setting to restore its default
    Clear {
        setting_name: String,
    },
}

#[derive(Clone, clap::ValueEnum)]
pub enum OutputFormat {
    Verbose,
    Json,
}

pub fn run(action: ConfigAction) -> Result<(), CliError> {
    match action {
        ConfigAction::Show { output } => show(output),
        ConfigAction::Get { setting_name } => get(&setting_name),
        ConfigAction::Set { setting_name, value } => set(&setting_name, &value),
        ConfigAction::Clear { setting_name } => clear(&setting_name),
    }
}

fn show(output: OutputFormat) -> Result<(), CliError> {
    let config = config_file::read_config()?;

    match output {
        OutputFormat::Verbose => {
            println!(
                "AWS Deadline Cloud configuration file:\n   {}",
                config_file::get_config_file_path().display()
            );
            println!();

            for name in config_file::setting_names() {
                let value = config_file::get_setting(name, &config)?;
                let default = config_file::get_setting_default(name, &config)?;
                let suffix = if value == default { "(default)" } else { "" };

                println!("{name}: {value} {suffix}");

                let desc = config_file::setting_description(name);
                for line in textwrap::wrap(desc, 77) {
                    println!("   {line}");
                }
                println!();
            }
        }
        OutputFormat::Json => {
            let mut map = serde_json::Map::new();
            map.insert(
                "settings.config_file_path".into(),
                serde_json::Value::String(
                    config_file::get_config_file_path().to_string_lossy().into(),
                ),
            );
            for name in config_file::setting_names() {
                let value = config_file::get_setting(name, &config)?;
                map.insert(name.into(), serde_json::Value::String(value));
            }
            println!("{}", json_with_spaces(&serde_json::Value::Object(map)));
        }
    }
    Ok(())
}

fn get(setting_name: &str) -> Result<(), CliError> {
    let value = config_file::get_setting_from_disk(setting_name)?;
    println!("{value}");
    Ok(())
}

fn set(setting_name: &str, value: &str) -> Result<(), CliError> {
    config_file::set_setting_to_disk(setting_name, value)?;
    Ok(())
}

fn clear(setting_name: &str) -> Result<(), CliError> {
    config_file::clear_setting_to_disk(setting_name)?;
    Ok(())
}
