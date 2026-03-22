use clap::Parser;

mod commands;

#[derive(Parser)]
#[command(name = "deadline", version, about = "Interact with AWS Deadline Cloud")]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(clap::Subcommand)]
enum Commands {
    /// View and update workstation configuration
    Config {
        #[command(subcommand)]
        action: commands::config::ConfigAction,
    },
}

fn main() {
    let cli = Cli::parse();

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
