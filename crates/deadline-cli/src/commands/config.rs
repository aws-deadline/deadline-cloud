use deadline_config::config_file;
use std::fmt;
use textwrap;

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

#[derive(Clone)]
pub enum OutputFormat {
    Verbose,
    Json,
}

impl fmt::Display for OutputFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OutputFormat::Verbose => write!(f, "verbose"),
            OutputFormat::Json => write!(f, "json"),
        }
    }
}

impl std::str::FromStr for OutputFormat {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "verbose" => Ok(OutputFormat::Verbose),
            "json" => Ok(OutputFormat::Json),
            _ => Err(format!("invalid output format: {s}")),
        }
    }
}

pub fn run(action: ConfigAction) -> Result<(), Box<dyn std::error::Error>> {
    match action {
        ConfigAction::Show { output } => show(output),
        ConfigAction::Get { setting_name } => get(&setting_name),
        ConfigAction::Set { setting_name, value } => set(&setting_name, &value),
        ConfigAction::Clear { setting_name } => clear(&setting_name),
    }
}

fn show(output: OutputFormat) -> Result<(), Box<dyn std::error::Error>> {
    let config = config_file::read_config()?;

    match output {
        OutputFormat::Verbose => {
            println!(
                "AWS Deadline Cloud configuration file:\n   {}",
                config_file::get_config_file_path().display()
            );
            println!();

            for name in config_file::setting_names() {
                let value = config_file::get_setting_with_config(name, &config)?;
                let default = config_file::get_setting_default_with_config(name, &config)?;
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
                let value = config_file::get_setting_with_config(name, &config)?;
                map.insert(name.into(), serde_json::Value::String(value));
            }
            println!("{}", serde_json::Value::Object(map));
        }
    }
    Ok(())
}

fn get(setting_name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let value = config_file::get_setting(setting_name)?;
    println!("{value}");
    Ok(())
}

fn set(setting_name: &str, value: &str) -> Result<(), Box<dyn std::error::Error>> {
    config_file::set_setting(setting_name, value)?;
    Ok(())
}

fn clear(setting_name: &str) -> Result<(), Box<dyn std::error::Error>> {
    config_file::clear_setting(setting_name)?;
    Ok(())
}
