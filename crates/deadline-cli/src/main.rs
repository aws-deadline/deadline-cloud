use clap::Parser;
use log::debug;
use std::fs::OpenOptions;

mod commands;
mod common;

const VALID_LOG_LEVELS: &[&str] = &["ERROR", "WARNING", "INFO", "DEBUG"];

#[derive(Parser)]
#[command(
    name = "deadline",
    version,
    about = common::strip_markdown_for_terminal(
        "Interact with **AWS Deadline Cloud** to submit, monitor, and manage render jobs.\n\n\
         Learn more about [Deadline Cloud](https://docs.aws.amazon.com/deadline-cloud/latest/userguide/what-is-deadline-cloud.html)"
    )
)]
struct Cli {
    /// Set the logging level
    #[arg(long, value_parser = parse_log_level)]
    log_level: Option<String>,

    /// Redirect stdout and stderr to the specified file
    #[arg(long)]
    redirect_output: Option<String>,

    /// When using --redirect-output, append (default) or replace the file
    #[arg(long, default_value = "append")]
    redirect_mode: RedirectMode,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Clone, clap::ValueEnum)]
enum RedirectMode {
    Append,
    Replace,
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

    // Set up output redirection before anything prints
    if let Some(ref path) = cli.redirect_output {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(matches!(cli.redirect_mode, RedirectMode::Append))
            .truncate(matches!(cli.redirect_mode, RedirectMode::Replace))
            .open(path)
            .unwrap_or_else(|e| {
                eprintln!("Failed to open redirect file '{path}': {e}");
                std::process::exit(1);
            });

        // Redirect stdout to the file. We use unsafe to set the global
        // file descriptor — this is the Rust equivalent of Python's
        // `sys.stdout = open(...)`.
        use std::os::unix::io::AsRawFd;
        let fd = file.as_raw_fd();
        unsafe {
            // dup2 stdout (fd 1) to our file
            libc::dup2(fd, 1);
            // dup2 stderr (fd 2) to our file
            libc::dup2(fd, 2);
        }
        // Keep the file open for the process lifetime
        std::mem::forget(file);
    }

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
            // Known operation/config errors: print message to stdout (matching Python CLI)
            println!("{e}");
            std::process::exit(1);
        }
    }
}
