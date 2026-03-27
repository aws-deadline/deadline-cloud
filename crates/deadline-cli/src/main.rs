use clap::Parser;
use log::debug;

mod commands;
mod common;

const VALID_LOG_LEVELS: &[&str] = &["ERROR", "WARNING", "INFO", "DEBUG"];

#[derive(Parser)]
#[command(name = "deadline", version, about = "Interact with AWS Deadline Cloud")]
struct Cli {
    /// Set the logging level
    #[arg(long, value_parser = parse_log_level)]
    log_level: Option<String>,

    #[command(subcommand)]
    command: Option<Commands>,
}

fn parse_log_level(s: &str) -> Result<String, String> {
    let upper = s.to_uppercase();
    if VALID_LOG_LEVELS.contains(&upper.as_str()) {
        Ok(upper)
    } else {
        Err(format!("invalid value '{s}' for '--log-level': valid values: ERROR, WARNING, INFO, DEBUG"))
    }
}

#[derive(clap::Subcommand)]
enum Commands {
    /// View and update workstation configuration
    Config {
        #[command(subcommand)]
        action: commands::config::ConfigAction,
    },
}

fn resolve_log_level(cli_level: Option<&str>) -> String {
    if let Some(level) = cli_level {
        return level.to_string();
    }

    // Read from config
    let config_level = deadline_config::config_file::get_setting("settings.log_level")
        .unwrap_or_default()
        .to_uppercase();

    if VALID_LOG_LEVELS.contains(&config_level.as_str()) {
        config_level
    } else {
        eprintln!(
            "Log Level '{}' not in {:?}. Defaulting to WARNING",
            config_level, VALID_LOG_LEVELS
        );
        "WARNING".to_string()
    }
}

fn init_logging(level: &str) {
    // Map our level names to env_logger filter levels
    // WARNING → WARN for env_logger compatibility
    let filter = match level {
        "WARNING" => "warn",
        other => other,
    };

    env_logger::Builder::new()
        .filter_level(filter.parse().unwrap_or(log::LevelFilter::Warn))
        .format_target(false)
        .format_timestamp(None)
        .target(env_logger::Target::Stderr)
        .init();
}

fn main() {
    let cli = Cli::parse();

    let log_level = resolve_log_level(cli.log_level.as_deref());
    init_logging(&log_level);

    if log_level == "DEBUG" {
        debug!("Debug logging is on");
    }

    if let Some(command) = cli.command {
        let result = match command {
            Commands::Config { action } => commands::config::run(action),
        };
        if let Err(e) = result {
            eprintln!("{e}");
            std::process::exit(1);
        }
    }
}
