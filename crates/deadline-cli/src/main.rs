// CLI binary — printing to stdout/stderr is its primary job.
#![allow(clippy::print_stdout, clippy::print_stderr, reason = "CLI binary outputs to stdout/stderr by design")]

use clap::Parser;
use log::debug;
use std::fs::OpenOptions;

mod commands;
mod common;

const VALID_LOG_LEVELS: &[&str] = &["ERROR", "WARNING", "INFO", "DEBUG"];

#[derive(Parser)]
#[command(
    name = "deadline,",
    bin_name = "deadline",
    version = concat!("version ", env!("CARGO_PKG_VERSION")),
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
    /// Manage authentication for Deadline Cloud
    Auth {
        #[command(subcommand)]
        action: commands::auth::AuthAction,
    },
    /// List or get Deadline Cloud farms
    Farm {
        #[command(subcommand)]
        action: commands::farm::FarmAction,
    },
    /// List or get Deadline Cloud fleets
    Fleet {
        #[command(subcommand)]
        action: commands::fleet::FleetAction,
    },
    /// List or get Deadline Cloud queues
    Queue {
        #[command(subcommand)]
        action: commands::queue::QueueAction,
    },
    /// List or get Deadline Cloud jobs
    Job {
        #[command(subcommand)]
        action: commands::job::JobAction,
    },
    /// List or get Deadline Cloud workers
    Worker {
        #[command(subcommand)]
        action: commands::worker::WorkerAction,
    },
    /// BETA - Upload or download job attachment data files
    Attachment {
        #[command(subcommand)]
        action: commands::attachment::AttachmentAction,
    },
    /// BETA - Create, compare, download, and upload job attachment manifests
    Manifest {
        #[command(subcommand)]
        action: commands::manifest::ManifestAction,
    },
    /// Submit Open Job Description job bundles to a Deadline Cloud queue
    Bundle {
        #[command(subcommand)]
        action: commands::bundle::BundleAction,
    },
    /// Handle deadline:// protocol URLs from web applications
    #[command(name = "handle-web-url")]
    HandleWebUrl(commands::handle_web_url::HandleWebUrlArgs),
    /// EXPERIMENTAL - Start the MCP (Model Context Protocol) server
    #[command(name = "mcp-server")]
    McpServer,
}

fn resolve_log_level(cli_level: Option<&str>) -> String {
    if let Some(level) = cli_level {
        return level.to_owned();
    }

    // Read from config
    let config_level = deadline_config::config_file::get_setting_from_disk("settings.log_level")
        .unwrap_or_default()
        .to_uppercase();

    if VALID_LOG_LEVELS.contains(&config_level.as_str()) {
        config_level
    } else {
        eprintln!(
            "Log Level '{config_level}' not in {VALID_LOG_LEVELS:?}. Defaulting to WARNING"
        );
        "WARNING".to_owned()
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

/// Map parsed command to its dot-separated path for user-agent tracking.
/// E.g. `Commands::Farm` { List { .. } } → "deadline.farm.list"
fn command_name(cmd: &Commands) -> String {
    let (group, action) = match cmd {
        Commands::Config { action } => ("config", match action {
            commands::config::ConfigAction::Show { .. } => "show",
            commands::config::ConfigAction::Get { .. } => "get",
            commands::config::ConfigAction::Set { .. } => "set",
            commands::config::ConfigAction::Clear { .. } => "clear",
            commands::config::ConfigAction::Gui { .. } => "gui",
        }),
        Commands::Auth { action } => ("auth", match action {
            commands::auth::AuthAction::Login => "login",
            commands::auth::AuthAction::Logout => "logout",
            commands::auth::AuthAction::Status { .. } => "status",
        }),
        Commands::Farm { action } => ("farm", match action {
            commands::farm::FarmAction::List { .. } => "list",
            commands::farm::FarmAction::Get { .. } => "get",
        }),
        Commands::Fleet { action } => ("fleet", match action {
            commands::fleet::FleetAction::List { .. } => "list",
            commands::fleet::FleetAction::Get { .. } => "get",
        }),
        Commands::Queue { action } => ("queue", match action {
            commands::queue::QueueAction::List { .. } => "list",
            commands::queue::QueueAction::Get { .. } => "get",
            commands::queue::QueueAction::ExportCredentials { .. } => "export-credentials",
            commands::queue::QueueAction::GetStorageProfile { .. } => "get-storage-profile",
            commands::queue::QueueAction::Paramdefs { .. } => "paramdefs",
            commands::queue::QueueAction::SyncOutput { .. } => "sync-output",
        }),
        Commands::Job { action } => ("job", match action {
            commands::job::JobAction::List { .. } => "list",
            commands::job::JobAction::Get { .. } => "get",
            commands::job::JobAction::GetSession { .. } => "get-session",
            commands::job::JobAction::ListSessions { .. } => "list-sessions",
            commands::job::JobAction::ListSteps { .. } => "list-steps",
            commands::job::JobAction::ListTasks { .. } => "list-tasks",
            commands::job::JobAction::Wait { .. } => "wait",
            commands::job::JobAction::Logs { .. } => "logs",
            commands::job::JobAction::Cancel { .. } => "cancel",
            commands::job::JobAction::RequeueTasks { .. } => "requeue-tasks",
            commands::job::JobAction::Search { .. } => "search",
            commands::job::JobAction::TraceSchedule { .. } => "trace-schedule",
            commands::job::JobAction::DownloadOutput { .. } => "download-output",
        }),
        Commands::Worker { action } => ("worker", match action {
            commands::worker::WorkerAction::List { .. } => "list",
            commands::worker::WorkerAction::Get { .. } => "get",
        }),
        Commands::Attachment { action } => ("attachment", match action {
            commands::attachment::AttachmentAction::Download { .. } => "download",
            commands::attachment::AttachmentAction::Upload { .. } => "upload",
        }),
        Commands::Manifest { action } => ("manifest", match action {
            commands::manifest::ManifestAction::Snapshot { .. } => "snapshot",
            commands::manifest::ManifestAction::Diff { .. } => "diff",
            commands::manifest::ManifestAction::Download { .. } => "download",
            commands::manifest::ManifestAction::Upload { .. } => "upload",
        }),
        Commands::Bundle { action } => ("bundle", match action {
            commands::bundle::BundleAction::Submit { .. } => "submit",
            commands::bundle::BundleAction::GuiSubmit { .. } => "gui-submit",
        }),
        Commands::HandleWebUrl(_) => ("handle-web-url", ""),
        Commands::McpServer => ("mcp-server", ""),
    };
    if action.is_empty() {
        format!("deadline.{group}")
    } else {
        format!("deadline.{group}.{action}")
    }
}

/// Redirect stdout (fd 1) and stderr (fd 2) to the given file.
#[cfg(unix)]
fn redirect_std_to_file(file: &std::fs::File) {
    use std::os::unix::io::AsRawFd;
    let fd = file.as_raw_fd();
    unsafe {
        libc::dup2(fd, 1);
        libc::dup2(fd, 2);
    }
}

#[cfg(windows)]
fn redirect_std_to_file(file: &std::fs::File) {
    use std::os::windows::io::AsRawHandle;
    let handle = file.as_raw_handle();
    unsafe {
        // SetStdHandle(STD_OUTPUT_HANDLE, handle)
        extern "system" { fn SetStdHandle(nStdHandle: u32, hHandle: *mut std::ffi::c_void) -> i32; }
        SetStdHandle(0xFFFF_FFF5, handle as *mut _); // STD_OUTPUT_HANDLE = -11 as u32
        SetStdHandle(0xFFFF_FFF4, handle as *mut _); // STD_ERROR_HANDLE = -12 as u32
    }
}

fn main() {
    // Rewrite `-ie` → `--include-exclude-config` before clap parses args.
    // Python click supports multi-char short flags; clap does not.
    let args: Vec<String> = std::env::args()
        .map(|a| if a == "-ie" { "--include-exclude-config".into() } else { a })
        .collect();
    let cli = Cli::parse_from(args);

    // Install SIGINT handler for graceful cancellation of long-running operations
    common::install_sigint_handler();

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

        // Redirect stdout and stderr to the file.
        redirect_std_to_file(&file);
        // Keep the file open for the process lifetime
        std::mem::forget(file);
    }

    let log_level = resolve_log_level(cli.log_level.as_deref());
    init_logging(&log_level);

    if log_level == "DEBUG" {
        debug!("Debug logging is on");
    }

    let mut exit_code: i32 = 0;

    if let Some(command) = cli.command {
        // Set CLI command name for user-agent tracking.
        // Python equivalent: ContextTrackingCommand sets
        // session_context["cli-command-name"] = ctx.command_path.replace(" ", ".")
        let cmd_name = command_name(&command);
        deadline_api::session::set_cli_command_name(&cmd_name);

        let result = match command {
            Commands::Config { action } => commands::config::run(action),
            Commands::Auth { action } => commands::auth::run(action),
            Commands::Farm { action } => commands::farm::run(action),
            Commands::Fleet { action } => commands::fleet::run(action),
            Commands::Queue { action } => commands::queue::run(action),
            Commands::Job { action } => commands::job::run(action),
            Commands::Worker { action } => commands::worker::run(action),
            Commands::Attachment { action } => commands::attachment::run(action),
            Commands::Manifest { action } => commands::manifest::run(action),
            Commands::Bundle { action } => commands::bundle::run(action),
            Commands::HandleWebUrl(args) => commands::handle_web_url::run(args),
            Commands::McpServer => commands::mcp::run(),
        };
        if let Err(e) = result {
            if let commands::config::CliError::ExitCode { code, ref message } = e {
                if !message.is_empty() {
                    println!("{message}");
                }
                exit_code = code;
            } else {
                // Known operation/config errors: print message to stdout (matching Python CLI)
                println!("{e}");
                exit_code = 1;
            }
        }
    }

    // All output is complete. println! uses LineWriter which flushes on
    // each newline, so output is already written. For non-zero exit codes
    // we must call process::exit since main() returns ().
    if exit_code != 0 {
        std::process::exit(exit_code);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Commands with positional args must still produce only the command path,
    // not include arg values. This is the case the argv-based approach got wrong.
    #[test]
    fn command_name_config_set_excludes_positional_args() {
        let cmd = Commands::Config {
            action: commands::config::ConfigAction::Set {
                setting_name: "defaults.farm_id".into(),
                value: "farm-abc".into(),
            },
        };
        assert_eq!(command_name(&cmd), "deadline.config.set");
    }

    #[test]
    fn command_name_farm_list_simple() {
        let cmd = Commands::Farm {
            action: commands::farm::FarmAction::List { profile: None },
        };
        assert_eq!(command_name(&cmd), "deadline.farm.list");
    }

    #[test]
    fn command_name_queue_export_credentials_uses_kebab_case() {
        let cmd = Commands::Queue {
            action: commands::queue::QueueAction::ExportCredentials {
                profile: None,
                farm_id: None,
                queue_id: None,
                mode: "USER".into(),
                output_format: "credentials_process".into(),
            },
        };
        assert_eq!(command_name(&cmd), "deadline.queue.export-credentials");
    }
}
