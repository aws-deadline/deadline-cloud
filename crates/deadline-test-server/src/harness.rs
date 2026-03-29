use assert_cmd::Command;
use tempfile::TempDir;
use wiremock::MockServer;

/// Test harness for CLI subprocess tests.
pub struct TestHarness {
    /// The wiremock server — mount API mocks on this.
    pub server: MockServer,
    /// Isolated temp directory containing the config file and cache.
    pub config_dir: TempDir,
    /// Path to the config file within `config_dir`.
    pub config_path: String,
}

/// Env vars to remove so the host environment doesn't interfere.
const CLEAN_VARS: &[&str] = &[
    "AWS_PROFILE",
    "AWS_DEFAULT_PROFILE",
    "AWS_CONFIG_FILE",
    "AWS_SHARED_CREDENTIALS_FILE",
    "AWS_SESSION_TOKEN",
    "AWS_SECURITY_TOKEN",
    "AWS_ENDPOINT_URL",
];

impl TestHarness {
    pub async fn new() -> Self {
        let server = MockServer::start().await;
        let config_dir = TempDir::new().expect("failed to create temp dir");
        let config_path = config_dir.path().join("config");
        std::fs::write(&config_path, "").expect("failed to write empty config");
        Self {
            server,
            config_dir,
            config_path: config_path.to_str().unwrap().to_string(),
        }
    }

    pub async fn with_config(content: &str) -> Self {
        let harness = Self::new().await;
        std::fs::write(&harness.config_path, content).expect("failed to write config");
        harness
    }

    fn endpoint_url(&self) -> String {
        let port = self.server.address().port();
        format!("http://localhost:{port}")
    }

    /// Build an `assert_cmd::Command` for `.assert()` chains.
    pub fn cli(&self, args: &[&str]) -> Command {
        let mut cmd = Command::cargo_bin("deadline")
            .expect("deadline binary not found — run `cargo build` first");
        let ep = self.endpoint_url();
        cmd.env("AWS_ENDPOINT_URL_DEADLINE", &ep);
        cmd.env("AWS_ENDPOINT_URL_STS", &ep);
        cmd.env("AWS_ACCESS_KEY_ID", "AKIAIOSFODNN7EXAMPLE");
        cmd.env("AWS_SECRET_ACCESS_KEY", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
        cmd.env("AWS_DEFAULT_REGION", "us-west-2");
        cmd.env("DEADLINE_CONFIG_FILE_PATH", &self.config_path);
        cmd.env("HOME", self.config_dir.path());
        for var in CLEAN_VARS { cmd.env_remove(var); }
        cmd.args(args);
        cmd
    }

    /// Build a `std::process::Command` for `insta_cmd::assert_cmd_snapshot!`.
    pub fn cmd(&self, args: &[&str]) -> std::process::Command {
        let bin = assert_cmd::cargo::cargo_bin("deadline");
        let mut cmd = std::process::Command::new(bin);
        let ep = self.endpoint_url();
        cmd.env("AWS_ENDPOINT_URL_DEADLINE", &ep);
        cmd.env("AWS_ENDPOINT_URL_STS", &ep);
        cmd.env("AWS_ACCESS_KEY_ID", "AKIAIOSFODNN7EXAMPLE");
        cmd.env("AWS_SECRET_ACCESS_KEY", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
        cmd.env("AWS_DEFAULT_REGION", "us-west-2");
        cmd.env("DEADLINE_CONFIG_FILE_PATH", &self.config_path);
        cmd.env("HOME", self.config_dir.path());
        for var in CLEAN_VARS { cmd.env_remove(var); }
        cmd.args(args);
        cmd
    }

    pub fn server_uri(&self) -> String {
        self.server.uri()
    }
}
