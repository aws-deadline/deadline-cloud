use clap::Subcommand;
use deadline_lib::api::{auth, session};
use deadline_lib::config::config_file;
use deadline_lib::config::ini::IniConfig;

use super::config::CliError;

#[derive(Subcommand)]
pub(crate) enum AuthAction {
    /// Log in via Deadline Cloud Monitor
    Login,
    /// Log out of Deadline Cloud Monitor
    Logout,
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

pub(crate) fn run(action: AuthAction) -> Result<(), CliError> {
    tokio::runtime::Runtime::new()
        .map_err(|e| CliError::Operation(e.to_string()))?
        .block_on(run_async(action))
}

async fn run_async(action: AuthAction) -> Result<(), CliError> {
    match action {
        AuthAction::Login => {
            let config = config_file::read_config().unwrap_or_else(|_| IniConfig::new());
            let profile = session::resolve_profile_name(&config);
            let profile_name = profile.as_deref().unwrap_or("(default)");
            let monitor_path = config_file::get_setting("deadline-cloud-monitor.path", &config)
                .unwrap_or_default();
            let (opt_out, ident) = deadline_lib::api::telemetry::resolve_telemetry_params(&config);
            let telemetry = deadline_lib::api::telemetry::create_telemetry(opt_out, Some(&ident));
            println!("Logging into AWS Profile '{profile_name}' for AWS Deadline Cloud");
            let on_pending = |_source: auth::AwsCredentialsSource| {
                println!("Opening Deadline Cloud monitor. Please log in and then return here.");
            };
            let message = auth::login(
                Some(&on_pending),
                None,
                profile.as_deref(),
                &monitor_path,
                &telemetry,
            )
            .await
            .map_err(CliError::Operation)?;
            println!("\nSuccessfully logged in: {message}\n");
            Ok(())
        }
        AuthAction::Logout => {
            let config = config_file::read_config().unwrap_or_else(|_| IniConfig::new());
            let profile = session::resolve_profile_name(&config);
            let monitor_path = config_file::get_setting("deadline-cloud-monitor.path", &config)
                .unwrap_or_default();
            let (opt_out, ident) = deadline_lib::api::telemetry::resolve_telemetry_params(&config);
            let telemetry = deadline_lib::api::telemetry::create_telemetry(opt_out, Some(&ident));
            auth::logout(profile.as_deref(), &monitor_path, &telemetry)
                .map_err(CliError::Operation)?;
            println!("Successfully logged out of all Deadline Cloud monitor AWS profiles");
            Ok(())
        }
        AuthAction::Status { profile, output } => status(profile, &output).await,
    }
}

async fn status(profile: Option<String>, output: &str) -> Result<(), CliError> {
    let mut config = config_file::read_config().map_err(|e| CliError::Operation(e.to_string()))?;
    if let Some(p) = profile {
        config_file::set_setting("defaults.aws_profile_name", &p, &mut config)
            .map_err(|e| CliError::Operation(e.to_string()))?;
    }

    let profile_name = session::display_profile_name(&config);
    let resolved_profile = session::resolve_profile_name(&config);
    let creds_source = auth::get_credentials_source(resolved_profile.as_deref());
    let auth_status = auth::check_authentication_status(resolved_profile.as_deref()).await;
    // Auth check uses ListFarms, so AUTHENTICATED implies API available.
    let api_available = auth_status == auth::AwsAuthenticationStatus::Authenticated;

    if output.eq_ignore_ascii_case("json") {
        let json = serde_json::json!({
            "profile_name": profile_name,
            "source": creds_source.to_string(),
            "status": auth_status.to_string(),
            "api_availability": api_available,
        });
        println!("{}", crate::common::json_with_spaces(&json));
    } else {
        let w = 17;
        println!("{:>w$} {profile_name}", "Profile Name:");
        println!("{:>w$} {creds_source}", "Source:");
        println!("{:>w$} {auth_status}", "Status:");
        println!(
            "{:>w$} {}",
            "API Availability:",
            if api_available { "True" } else { "False" }
        );
    }
    Ok(())
}
