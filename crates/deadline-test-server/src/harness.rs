use assert_cmd::Command;
use tempfile::TempDir;
use wiremock::MockServer;

/// Test harness for CLI subprocess tests.
///
/// The Deadline Cloud SDK prepends `management.` or `scheduling.` to the
/// endpoint hostname (Smithy `@endpoint(hostPrefix)`). To make this work
/// with a local wiremock server, we set `endpoint_url` to
/// `http://localhost:{port}`. The SDK then connects to
/// `http://management.localhost:{port}`, which resolves to 127.0.0.1 on
/// macOS and most Linux systems via the `.localhost` TLD (RFC 6761).
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

    /// The endpoint URL using `localhost` so that the SDK's host prefix
    /// (e.g. `management.localhost`) resolves to 127.0.0.1.
    fn endpoint_url(&self) -> String {
        let port = self.server.address().port();
        format!("http://localhost:{port}")
    }

    /// Build a `Command` for the `deadline` CLI binary with all environment
    /// variables pre-configured to use the fake server and isolated config.
    pub fn cli(&self, args: &[&str]) -> Command {
        let mut cmd = Command::cargo_bin("deadline").expect(
            "deadline binary not found — run `cargo build` first",
        );

        let endpoint = self.endpoint_url();

        // Point at the fake server using localhost so SDK host prefixes
        // (management.localhost, scheduling.localhost) resolve correctly.
        cmd.env("AWS_ENDPOINT_URL_DEADLINE", &endpoint);
        cmd.env("AWS_ENDPOINT_URL_STS", &endpoint);

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
        cmd.env_remove("AWS_ENDPOINT_URL");

        cmd.args(args);
        cmd
    }

    /// Convenience: get the URI of the fake server.
    pub fn server_uri(&self) -> String {
        self.server.uri()
    }
}
