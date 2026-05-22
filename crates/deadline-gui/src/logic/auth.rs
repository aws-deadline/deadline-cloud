//! Auth status derivation logic for the config dialog.
//!
//! Pure functions that derive display state from credential/auth/API checks.
//! Testable without Qt.

use deadline_lib::api::auth::{AwsAuthenticationStatus, AwsCredentialsSource};

/// The visual state of the auth status bar.
#[derive(Debug, Clone, PartialEq)]
pub enum AuthState {
    Refreshing,
    AuthenticatedReady,
    AuthenticatedNoApi,
    NeedsLogin,
    ConfigurationError,
    UnexpectedError,
}

/// Derive the auth display state from the three async check results.
/// Any `None` value means that check hasn't completed yet.
pub fn derive_auth_state(
    creds_source: Option<AwsCredentialsSource>,
    auth_status: Option<AwsAuthenticationStatus>,
    api_available: Option<bool>,
) -> AuthState {
    let (Some(_creds), Some(auth), Some(api)) = (creds_source, auth_status, api_available) else {
        return AuthState::Refreshing;
    };

    match (auth, api) {
        (AwsAuthenticationStatus::Authenticated, true) => AuthState::AuthenticatedReady,
        (AwsAuthenticationStatus::Authenticated, false) => AuthState::AuthenticatedNoApi,
        (AwsAuthenticationStatus::NeedsLogin, false) => AuthState::NeedsLogin,
        (AwsAuthenticationStatus::ConfigurationError, _) => AuthState::ConfigurationError,
        _ => AuthState::UnexpectedError,
    }
}

/// Generate the status text shown in the auth bar for a given state.
pub fn auth_status_text(state: &AuthState, profile: &str) -> String {
    match state {
        AuthState::Refreshing | AuthState::AuthenticatedReady => profile.to_string(),
        AuthState::AuthenticatedNoApi => {
            format!("{profile} doesn't have access permissions to submit a job.")
        }
        AuthState::NeedsLogin => format!("{profile}  -  You are logged out."),
        AuthState::ConfigurationError => {
            format!(
                "A configuration error was received while accessing credentials for the profile '{profile}'."
            )
        }
        AuthState::UnexpectedError => "There was an error with authentication".to_string(),
    }
}

/// Whether the logout button should be visible.
pub fn should_show_logout(creds_source: Option<AwsCredentialsSource>) -> bool {
    creds_source == Some(AwsCredentialsSource::DeadlineCloudMonitorLogin)
}

/// Whether the login button should be visible.
pub fn should_show_login(state: &AuthState) -> bool {
    *state == AuthState::NeedsLogin
}

/// Whether the "More info" button should be visible.
pub fn should_show_more_info(state: &AuthState) -> bool {
    matches!(
        state,
        AuthState::AuthenticatedNoApi | AuthState::ConfigurationError | AuthState::UnexpectedError
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    // ─────────────────────────────────────────────────────────────
    // derive_auth_state
    // ─────────────────────────────────────────────────────────────

    #[test]
    fn derive_auth_state_all_none_is_refreshing() {
        assert_eq!(derive_auth_state(None, None, None), AuthState::Refreshing);
    }

    #[test]
    fn derive_auth_state_partial_none_is_refreshing() {
        assert_eq!(
            derive_auth_state(Some(AwsCredentialsSource::HostProvided), None, Some(true)),
            AuthState::Refreshing
        );
    }

    #[test]
    fn derive_auth_state_authenticated_api_available() {
        assert_eq!(
            derive_auth_state(
                Some(AwsCredentialsSource::HostProvided),
                Some(AwsAuthenticationStatus::Authenticated),
                Some(true)
            ),
            AuthState::AuthenticatedReady
        );
    }

    #[test]
    fn derive_auth_state_authenticated_no_api() {
        assert_eq!(
            derive_auth_state(
                Some(AwsCredentialsSource::HostProvided),
                Some(AwsAuthenticationStatus::Authenticated),
                Some(false)
            ),
            AuthState::AuthenticatedNoApi
        );
    }

    #[test]
    fn derive_auth_state_needs_login() {
        assert_eq!(
            derive_auth_state(
                Some(AwsCredentialsSource::DeadlineCloudMonitorLogin),
                Some(AwsAuthenticationStatus::NeedsLogin),
                Some(false)
            ),
            AuthState::NeedsLogin
        );
    }

    #[test]
    fn derive_auth_state_configuration_error() {
        assert_eq!(
            derive_auth_state(
                Some(AwsCredentialsSource::NotValid),
                Some(AwsAuthenticationStatus::ConfigurationError),
                Some(false)
            ),
            AuthState::ConfigurationError
        );
    }

    #[test]
    fn derive_auth_state_unexpected_combination() {
        // NeedsLogin but api_available=true is contradictory
        assert_eq!(
            derive_auth_state(
                Some(AwsCredentialsSource::HostProvided),
                Some(AwsAuthenticationStatus::NeedsLogin),
                Some(true)
            ),
            AuthState::UnexpectedError
        );
    }

    // ─────────────────────────────────────────────────────────────
    // auth_status_text
    // ─────────────────────────────────────────────────────────────

    #[test]
    fn auth_status_text_refreshing_shows_profile() {
        let text = auth_status_text(&AuthState::Refreshing, "my-profile");
        assert_eq!(text, "my-profile");
    }

    #[test]
    fn auth_status_text_authenticated_ready_shows_profile() {
        let text = auth_status_text(&AuthState::AuthenticatedReady, "prod");
        assert_eq!(text, "prod");
    }

    #[test]
    fn auth_status_text_authenticated_no_api() {
        let text = auth_status_text(&AuthState::AuthenticatedNoApi, "prod");
        assert!(text.contains("prod"));
        assert!(text.contains("access permissions"));
    }

    #[test]
    fn auth_status_text_needs_login() {
        let text = auth_status_text(&AuthState::NeedsLogin, "dcm-profile");
        assert!(text.contains("dcm-profile"));
        assert!(text.contains("logged out"));
    }

    #[test]
    fn auth_status_text_configuration_error() {
        let text = auth_status_text(&AuthState::ConfigurationError, "broken");
        assert!(text.contains("broken"));
        assert!(text.contains("configuration error"));
    }

    #[test]
    fn auth_status_text_unexpected_error() {
        let text = auth_status_text(&AuthState::UnexpectedError, "x");
        assert!(text.contains("error"));
    }

    // ─────────────────────────────────────────────────────────────
    // should_show_logout
    // ─────────────────────────────────────────────────────────────

    #[test]
    fn should_show_logout_dcm_true() {
        assert!(should_show_logout(Some(
            AwsCredentialsSource::DeadlineCloudMonitorLogin
        )));
    }

    #[test]
    fn should_show_logout_host_provided_false() {
        assert!(!should_show_logout(Some(
            AwsCredentialsSource::HostProvided
        )));
    }

    #[test]
    fn should_show_logout_none_false() {
        assert!(!should_show_logout(None));
    }

    #[test]
    fn should_show_logout_not_valid_false() {
        assert!(!should_show_logout(Some(AwsCredentialsSource::NotValid)));
    }

    // ─────────────────────────────────────────────────────────────
    // should_show_login
    // ─────────────────────────────────────────────────────────────

    #[test]
    fn should_show_login_needs_login_true() {
        assert!(should_show_login(&AuthState::NeedsLogin));
    }

    #[test]
    fn should_show_login_authenticated_false() {
        assert!(!should_show_login(&AuthState::AuthenticatedReady));
    }

    #[test]
    fn should_show_login_refreshing_false() {
        assert!(!should_show_login(&AuthState::Refreshing));
    }

    // ─────────────────────────────────────────────────────────────
    // should_show_more_info
    // ─────────────────────────────────────────────────────────────

    #[test]
    fn should_show_more_info_no_api_true() {
        assert!(should_show_more_info(&AuthState::AuthenticatedNoApi));
    }

    #[test]
    fn should_show_more_info_config_error_true() {
        assert!(should_show_more_info(&AuthState::ConfigurationError));
    }

    #[test]
    fn should_show_more_info_unexpected_error_true() {
        assert!(should_show_more_info(&AuthState::UnexpectedError));
    }

    #[test]
    fn should_show_more_info_authenticated_ready_false() {
        assert!(!should_show_more_info(&AuthState::AuthenticatedReady));
    }

    #[test]
    fn should_show_more_info_refreshing_false() {
        assert!(!should_show_more_info(&AuthState::Refreshing));
    }
}
