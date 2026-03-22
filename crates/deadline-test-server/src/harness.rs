use assert_cmd::Command;
use tempfile::TempDir;
use wiremock::MockServer;

/// Test harness for CLI subprocess tests.
///
/// Encapsulates a fake AWS server, an isolated config directory, and
/// pre-configured environment variables. Each test gets a fresh harness
/// so tests are fully independent.
///
/// # Example
///
/// ```rust,no_run
/// use deadline_test_server::TestHarness;
///
/// #[tokio::test]
/// async fn farm_list_prints_table() {
///     let harness = TestHarness::new().await;
///     // ... mount mocks on harness.server ...
///     harness.cli(&["farm", "list"])
///         .assert()
///         .success();
/// }
/// ```
pub struct TestHarness {
    /// The wiremock server — mount API mocks on this.
    pub server: MockServer,
    /// Isolated temp directory containing the config file and cache.
    pub config_dir: TempDir,
    /// Path to the config file within `config_dir`.
    pub config_path: String,
}

impl TestHarness {
    /// Create a new harness with a running fake server and empty config.
    pub async fn new() -> Self {
        let server = MockServer::start().await;
        let config_dir = TempDir::new().expect("failed to create temp dir");

        let config_path = config_dir.path().join("config");
        std::fs::write(&config_path, "").expect("failed to write empty config");

        let config_path_str = config_path.to_str().unwrap().to_string();

        Self {
            server,
            config_dir,
            config_path: config_path_str,
        }
    }

    /// Create a new harness with pre-populated config file content.
    pub async fn with_config(content: &str) -> Self {
        let harness = Self::new().await;
        std::fs::write(&harness.config_path, content).expect("failed to write config");
        harness
    }

    /// Build a `Command` for the `deadline` CLI binary with all environment
    /// variables pre-configured to use the fake server and isolated config.
    ///
    /// The returned command has:
    /// - `AWS_ENDPOINT_URL_DEADLINE` → fake server
    /// - `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` → dummy credentials
    /// - `AWS_DEFAULT_REGION` → us-west-2
    /// - `DEADLINE_CONFIG_FILE_PATH` → isolated temp config
    /// - `HOME` → isolated temp dir (prevents leaking real user config)
    /// - Common AWS env vars cleared to prevent interference
    pub fn cli(&self, args: &[&str]) -> Command {
        let mut cmd = Command::cargo_bin("deadline").expect(
            "deadline binary not found — run `cargo build` first",
        );

        // Point at the fake server
        cmd.env("AWS_ENDPOINT_URL_DEADLINE", self.server.uri());

        // Dummy credentials (the fake server doesn't validate signatures)
        cmd.env("AWS_ACCESS_KEY_ID", "AKIAIOSFODNN7EXAMPLE");
        cmd.env(
            "AWS_SECRET_ACCESS_KEY",
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        );
        cmd.env("AWS_DEFAULT_REGION", "us-west-2");

        // Isolated config
        cmd.env("DEADLINE_CONFIG_FILE_PATH", &self.config_path);
        cmd.env("HOME", self.config_dir.path());

        // Prevent interference from the host environment
        cmd.env_remove("AWS_PROFILE");
        cmd.env_remove("AWS_DEFAULT_PROFILE");
        cmd.env_remove("AWS_CONFIG_FILE");
        cmd.env_remove("AWS_SHARED_CREDENTIALS_FILE");
        cmd.env_remove("AWS_SESSION_TOKEN");
        cmd.env_remove("AWS_SECURITY_TOKEN");

        cmd.args(args);
        cmd
    }

    /// Convenience: get the URI of the fake server.
    pub fn server_uri(&self) -> String {
        self.server.uri()
    }
}
