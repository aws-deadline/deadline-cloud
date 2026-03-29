use clap::Subcommand;
use deadline_client::{auth, session};

use super::config::CliError;
use super::helpers::apply_profile;

#[derive(Subcommand)]
pub enum AuthAction {
    /// Check authentication status
    Status {
        /// The AWS profile to use
        #[arg(long)]
        profile: Option<String>,
        /// Output format: verbose or json
        #[arg(long, default_value = "verbose")]
        output: String,
    },
}

pub fn run(action: AuthAction) -> Result<(), CliError> {
    tokio::runtime::Runtime::new()
        .map_err(|e| CliError::Operation(e.to_string()))?
        .block_on(run_async(action))
}

async fn run_async(action: AuthAction) -> Result<(), CliError> {
    match action {
        AuthAction::Status { profile, output } => status(profile, &output).await,
    }
}

async fn status(profile: Option<String>, output: &str) -> Result<(), CliError> {
    let config = apply_profile(profile)?;
    let config_ref = config.as_ref();

    let profile_name = session::display_profile_name(config_ref);
    let creds_source = auth::get_credentials_source(config_ref);
    let auth_status = auth::check_authentication_status(config_ref).await;
    let api_available = auth::check_deadline_api_available(config_ref).await;

    if output == "json" {
        let json = serde_json::json!({
            "profile_name": profile_name,
            "source": creds_source.to_string(),
            "status": auth_status.to_string(),
            "api_availability": api_available,
        });
        println!("{}", serde_json::to_string(&json).unwrap());
    } else {
        let w = 17;
        println!("{:>w$} {profile_name}", "Profile Name:");
        println!("{:>w$} {creds_source}", "Source:");
        println!("{:>w$} {auth_status}", "Status:");
        println!("{:>w$} {api_available}", "API Availability:");
    }
    Ok(())
}
