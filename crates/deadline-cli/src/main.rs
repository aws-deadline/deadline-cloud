use clap::Parser;

#[derive(Parser)]
#[command(name = "deadline", version, about = "Interact with AWS Deadline Cloud")]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(clap::Subcommand)]
enum Commands {}

fn main() {
    let _cli = Cli::parse();
}
