//! Update checker for Deadline Cloud integrations.
//!
//! Fetches a remote manifest from downloads.deadlinecloud.amazonaws.com,
//! compares the installed version against the latest, and returns a
//! structured result. Never panics — all errors are captured in the
//! result struct.
//!
//! Python source: `deadline.client.api._update_checker`

use deadline_config::config_file;
use deadline_config::ini::IniConfig;
use serde_json::Value;
use std::time::Duration;

pub const MANIFEST_URL: &str =
    "https://downloads.deadlinecloud.amazonaws.com/submitters/manifest.json";
pub const DOWNLOAD_BASE_URL: &str =
    "https://downloads.deadlinecloud.amazonaws.com/submitters";
const MANIFEST_TIMEOUT_SECONDS: u64 = 5;

/// Status of the update check operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UpdateCheckStatus {
    Success,
    NetworkError,
    TimeoutError,
    ParseError,
    InvalidVersion,
    IntegrationNotFound,
    UnexpectedError,
}

/// Result of an update check operation.
#[derive(Debug, Clone)]
pub struct UpdateCheckResult {
    pub status: UpdateCheckStatus,
    pub current_version: String,
    pub update_available: bool,
    pub latest_version: Option<String>,
    pub download_url: Option<String>,
    pub error_message: Option<String>,
}

/// Detect the current operating system.
/// Returns `"linux"`, `"macos"`, or `"windows"`.
pub fn get_current_platform() -> &'static str {
    if cfg!(target_os = "macos") {
        "macos"
    } else if cfg!(target_os = "windows") {
        "windows"
    } else {
        "linux"
    }
}

/// Normalize a version string for semver parsing.
/// Strips `v` prefix and local segments (`+...`) to match Python's
/// `packaging.version.Version` normalization for the subset we need.
fn normalize_version(v: &str) -> &str {
    let v = v.strip_prefix('v').unwrap_or(v);
    // Strip local segment (+...) — semver crate rejects build metadata
    // in comparisons, and Python's packaging ignores local segments.
    match v.find('+') {
        Some(i) => &v[..i],
        None => v,
    }
}

fn fetch_manifest(url: &str) -> Result<Value, (UpdateCheckStatus, String)> {
    let agent = ureq::Agent::config_builder()
        .timeout_global(Some(Duration::from_secs(MANIFEST_TIMEOUT_SECONDS)))
        .build()
        .into();

    let response = ureq::Agent::get(&agent, url).call().map_err(|e| match &e {
        ureq::Error::Timeout(_) => (UpdateCheckStatus::TimeoutError, "Request timed out".into()),
        _ => (UpdateCheckStatus::NetworkError, format!("Network error: {e}")),
    })?;

    let body = response
        .into_body()
        .read_to_string()
        .map_err(|e| (UpdateCheckStatus::ParseError, format!("Failed to parse manifest: {e}")))?;

    serde_json::from_str(&body)
        .map_err(|e| (UpdateCheckStatus::ParseError, format!("Failed to parse manifest: {e}")))
}

fn build_download_url(platform_data: &Value) -> Option<String> {
    let installer = platform_data["installer"].as_str()?;
    let sep = if installer.starts_with('/') { "" } else { "/" };
    Some(format!("{DOWNLOAD_BASE_URL}{sep}{installer}"))
}

/// Check if a newer version of a Deadline Cloud integration is available.
///
/// This is a *safe* wrapper that never panics. All errors are captured
/// and returned as an `UpdateCheckResult` with the appropriate status
/// and `error_message`.
///
/// If `settings.submitter_update_notification` is `"false"`, the check
/// is skipped and a result with `update_available=false` is returned.
///
/// `manifest_url` allows overriding the URL for testing (pointed at wiremock).
pub fn safe_check_for_updates(
    integration_name: &str,
    current_version: &str,
    config: Option<&IniConfig>,
    manifest_url: Option<&str>,
) -> UpdateCheckResult {
    let base = || UpdateCheckResult {
        status: UpdateCheckStatus::Success,
        current_version: current_version.to_owned(),
        update_available: false,
        latest_version: None,
        download_url: None,
        error_message: None,
    };

    // Check config opt-out
    let notification_setting = match config {
        Some(c) => config_file::get_setting("settings.submitter_update_notification", c),
        None => config_file::get_setting_from_disk("settings.submitter_update_notification"),
    };
    let notification_enabled = notification_setting
        .and_then(|v| config_file::str2bool(&v))
        .unwrap_or(true);

    if !notification_enabled {
        return base();
    }

    let url = manifest_url.unwrap_or(MANIFEST_URL);

    // Fetch manifest
    let manifest = match fetch_manifest(url) {
        Ok(m) => m,
        Err((status, msg)) => {
            return UpdateCheckResult {
                status,
                error_message: Some(msg),
                ..base()
            };
        }
    };

    let platform = get_current_platform();

    // Navigate: DeadlineCloudSubmitter.versions.latest.{platform}
    let platform_data =
        &manifest["DeadlineCloudSubmitter"]["versions"]["latest"][platform];
    if platform_data.is_null() {
        return UpdateCheckResult {
            status: UpdateCheckStatus::ParseError,
            error_message: Some(format!(
                "Platform '{platform}' not found in manifest"
            )),
            ..base()
        };
    }

    // Look up integration version
    let Some(latest_str) = platform_data["componentVersions"][integration_name].as_str() else {
        return UpdateCheckResult {
            status: UpdateCheckStatus::IntegrationNotFound,
            error_message: Some(format!(
                "Integration '{integration_name}' not found in manifest"
            )),
            ..base()
        };
    };

    // Parse and compare versions
    let current_normalized = normalize_version(current_version);
    let latest_normalized = normalize_version(latest_str);

    let current_ver = match semver::Version::parse(current_normalized) {
        Ok(v) => v,
        Err(e) => {
            return UpdateCheckResult {
                status: UpdateCheckStatus::InvalidVersion,
                latest_version: Some(latest_str.to_owned()),
                error_message: Some(format!("Invalid version: {e}")),
                ..base()
            };
        }
    };

    let latest_ver = match semver::Version::parse(latest_normalized) {
        Ok(v) => v,
        Err(e) => {
            return UpdateCheckResult {
                status: UpdateCheckStatus::InvalidVersion,
                latest_version: Some(latest_str.to_owned()),
                error_message: Some(format!("Invalid version: {e}")),
                ..base()
            };
        }
    };

    let mut update_available = latest_ver > current_ver;
    let download_url = build_download_url(platform_data);

    // Python behavior: if update available but no installer URL, suppress
    if update_available && download_url.is_none() {
        update_available = false;
    }

    UpdateCheckResult {
        status: UpdateCheckStatus::Success,
        current_version: current_version.to_owned(),
        update_available,
        latest_version: Some(latest_str.to_owned()),
        download_url,
        error_message: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// Sample manifest matching the real API structure.
    /// Python tests use the same shape at `SAMPLE_MANIFEST`.
    fn sample_manifest() -> Value {
        json!({
            "DeadlineCloudSubmitter": {
                "versions": {
                    "latest": {
                        "linux": {
                            "componentVersions": {
                                "deadline-cloud": "0.54.2",
                                "deadline-cloud-for-blender": "0.6.1",
                                "deadline-cloud-for-cinema-4d": "0.10.0",
                                "deadline-cloud-for-maya": "0.15.13",
                                "deadline-cloud-for-nuke": "0.18.16"
                            },
                            "installer": "/latest/linux/DeadlineCloudSubmitter-linux-x64-installer.run",
                            "sha256": "/latest/linux/DeadlineCloudSubmitter-linux-x64-installer.run.sha256"
                        },
                        "macos": {
                            "componentVersions": {
                                "deadline-cloud": "0.54.2",
                                "deadline-cloud-for-cinema-4d": "0.10.0",
                                "deadline-cloud-for-maya": "0.15.13",
                                "deadline-cloud-for-nuke": "0.18.16"
                            },
                            "installer": "/latest/macos/DeadlineCloudSubmitter-osx-installer.app.zip",
                            "sha256": "/latest/macos/DeadlineCloudSubmitter-osx-installer.app.zip.sha256"
                        },
                        "windows": {
                            "componentVersions": {
                                "deadline-cloud": "0.54.2",
                                "deadline-cloud-for-cinema-4d": "0.10.0",
                                "deadline-cloud-for-maya": "0.15.13",
                                "deadline-cloud-for-nuke": "0.18.16"
                            },
                            "installer": "/latest/windows/DeadlineCloudSubmitter-windows-x64-installer.exe",
                            "sha256": "/latest/windows/DeadlineCloudSubmitter-windows-x64-installer.exe.sha256"
                        }
                    }
                }
            }
        })
    }

    async fn start_manifest_server(manifest: &Value) -> MockServer {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/submitters/manifest.json"))
            .respond_with(ResponseTemplate::new(200).set_body_json(manifest))
            .mount(&server)
            .await;
        server
    }

    fn manifest_url(server: &MockServer) -> String {
        format!("{}/submitters/manifest.json", server.uri())
    }

    // ── Platform detection (Spec #1-3) ──────────────────────

    #[test]
    fn get_current_platform_returns_known_value() {
        let platform = get_current_platform();
        assert!(
            ["linux", "macos", "windows"].contains(&platform),
            "expected linux/macos/windows, got: {platform}"
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn get_current_platform_macos() {
        assert_eq!(get_current_platform(), "macos");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn get_current_platform_linux() {
        assert_eq!(get_current_platform(), "linux");
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn get_current_platform_windows() {
        assert_eq!(get_current_platform(), "windows");
    }

    // ── Happy path: update available (Spec #4) ─────────────

    #[tokio::test]
    async fn safe_check_newer_version_available() {
        let server = start_manifest_server(&sample_manifest()).await;
        let url = manifest_url(&server);

        let result = safe_check_for_updates(
            "deadline-cloud-for-cinema-4d",
            "0.9.1",
            None,
            Some(&url),
        );

        assert_eq!(result.status, UpdateCheckStatus::Success);
        assert!(result.update_available);
        assert_eq!(result.current_version, "0.9.1");
        assert_eq!(result.latest_version.as_deref(), Some("0.10.0"));
        assert!(result.download_url.is_some());
        assert!(result.error_message.is_none());
    }

    // ── Happy path: current is latest (Spec #5) ────────────

    #[tokio::test]
    async fn safe_check_current_is_latest() {
        let server = start_manifest_server(&sample_manifest()).await;
        let url = manifest_url(&server);

        let result = safe_check_for_updates(
            "deadline-cloud-for-cinema-4d",
            "0.10.0",
            None,
            Some(&url),
        );

        assert_eq!(result.status, UpdateCheckStatus::Success);
        assert!(!result.update_available);
        assert_eq!(result.latest_version.as_deref(), Some("0.10.0"));
    }

    // ── Happy path: current newer than manifest (Spec #6) ──

    #[tokio::test]
    async fn safe_check_current_newer_than_manifest() {
        let server = start_manifest_server(&sample_manifest()).await;
        let url = manifest_url(&server);

        let result = safe_check_for_updates(
            "deadline-cloud-for-cinema-4d",
            "1.0.0",
            None,
            Some(&url),
        );

        assert_eq!(result.status, UpdateCheckStatus::Success);
        assert!(!result.update_available);
    }

    // ── Error: network timeout (Spec #7) ────────────────────

    #[tokio::test]
    async fn safe_check_network_timeout() {
        let server = MockServer::start().await;
        // Mount a delayed response that exceeds the timeout
        Mock::given(method("GET"))
            .and(path("/submitters/manifest.json"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(sample_manifest())
                    .set_delay(Duration::from_secs(30)),
            )
            .mount(&server)
            .await;
        let url = manifest_url(&server);

        let result = safe_check_for_updates(
            "deadline-cloud-for-cinema-4d",
            "0.9.1",
            None,
            Some(&url),
        );

        assert_eq!(result.status, UpdateCheckStatus::TimeoutError);
        assert!(!result.update_available);
        assert!(result.error_message.is_some());
    }

    // ── Error: network unreachable (Spec #8) ────────────────

    #[test]
    fn safe_check_network_error() {
        // Point at a URL that will fail to connect
        let result = safe_check_for_updates(
            "deadline-cloud-for-cinema-4d",
            "0.9.1",
            None,
            Some("http://localhost:1/submitters/manifest.json"),
        );

        assert_eq!(result.status, UpdateCheckStatus::NetworkError);
        assert!(!result.update_available);
        assert!(result.error_message.is_some());
    }

    // ── Error: malformed JSON (Spec #9) ─────────────────────

    #[tokio::test]
    async fn safe_check_malformed_json() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/submitters/manifest.json"))
            .respond_with(ResponseTemplate::new(200).set_body_string("not valid json{{{"))
            .mount(&server)
            .await;
        let url = manifest_url(&server);

        let result = safe_check_for_updates(
            "deadline-cloud-for-cinema-4d",
            "0.9.1",
            None,
            Some(&url),
        );

        assert_eq!(result.status, UpdateCheckStatus::ParseError);
        assert!(!result.update_available);
        assert!(result.error_message.is_some());
    }

    // ── Error: integration not found (Spec #10) ─────────────

    #[tokio::test]
    async fn safe_check_integration_not_found() {
        let server = start_manifest_server(&sample_manifest()).await;
        let url = manifest_url(&server);

        let result = safe_check_for_updates(
            "deadline-cloud-for-houdini",
            "1.0.0",
            None,
            Some(&url),
        );

        assert_eq!(result.status, UpdateCheckStatus::IntegrationNotFound);
        assert!(!result.update_available);
        assert!(result.error_message.is_some());
        assert!(result.error_message.as_ref().unwrap().contains("not found"));
    }

    // ── Error: invalid version string (Spec #11) ────────────

    #[tokio::test]
    async fn safe_check_invalid_version() {
        let server = start_manifest_server(&sample_manifest()).await;
        let url = manifest_url(&server);

        let result = safe_check_for_updates(
            "deadline-cloud-for-cinema-4d",
            "bad-version",
            None,
            Some(&url),
        );

        assert_eq!(result.status, UpdateCheckStatus::InvalidVersion);
        assert!(!result.update_available);
        assert_eq!(result.current_version, "bad-version");
        assert!(result.error_message.is_some());
    }

    // ── Happy path: platform-specific entries (Spec #12) ────

    #[tokio::test]
    async fn safe_check_platform_specific_entries() {
        let server = start_manifest_server(&sample_manifest()).await;
        let url = manifest_url(&server);

        // The sample manifest has blender on linux but not macos/windows.
        // On the current platform, cinema-4d is available on all platforms.
        let result = safe_check_for_updates(
            "deadline-cloud-for-cinema-4d",
            "0.9.0",
            None,
            Some(&url),
        );

        assert_eq!(result.status, UpdateCheckStatus::Success);
        assert!(result.update_available);
        // Download URL should contain the current platform
        let url = result.download_url.unwrap();
        let platform = get_current_platform();
        assert!(
            url.contains(platform),
            "download URL should contain platform '{platform}': {url}"
        );
    }

    // ── Config opt-out ──────────────────────────────────────

    #[tokio::test]
    async fn safe_check_notification_disabled() {
        let server = start_manifest_server(&sample_manifest()).await;
        let url = manifest_url(&server);

        // Create a config with notification disabled
        let config = IniConfig::parse(
            "[settings]\nsubmitter_update_notification = false\n",
        )
        .unwrap();

        let result = safe_check_for_updates(
            "deadline-cloud-for-cinema-4d",
            "0.9.1",
            Some(&config),
            Some(&url),
        );

        assert_eq!(result.status, UpdateCheckStatus::Success);
        assert!(!result.update_available);
        // Should NOT have fetched the manifest (server received 0 requests)
        assert_eq!(server.received_requests().await.unwrap().len(), 0);
    }

    // ── Missing installer → no update ───────────────────────

    #[tokio::test]
    async fn safe_check_missing_installer_no_update() {
        let manifest = json!({
            "DeadlineCloudSubmitter": {
                "versions": {
                    "latest": {
                        "macos": {
                            "componentVersions": {
                                "deadline-cloud-for-cinema-4d": "0.10.0"
                            }
                            // No "installer" key
                        },
                        "linux": {
                            "componentVersions": {
                                "deadline-cloud-for-cinema-4d": "0.10.0"
                            }
                        },
                        "windows": {
                            "componentVersions": {
                                "deadline-cloud-for-cinema-4d": "0.10.0"
                            }
                        }
                    }
                }
            }
        });
        let server = start_manifest_server(&manifest).await;
        let url = manifest_url(&server);

        let result = safe_check_for_updates(
            "deadline-cloud-for-cinema-4d",
            "0.9.1",
            None,
            Some(&url),
        );

        // Python behavior: update_available forced to false when no installer URL
        assert_eq!(result.status, UpdateCheckStatus::Success);
        assert!(!result.update_available);
        assert!(result.download_url.is_none());
    }

    // ── Platform not in manifest ────────────────────────────

    #[tokio::test]
    async fn safe_check_platform_not_in_manifest() {
        // Manifest with only a platform that doesn't match current OS
        let manifest = json!({
            "DeadlineCloudSubmitter": {
                "versions": {
                    "latest": {
                        "unknown_os": {
                            "componentVersions": {
                                "deadline-cloud-for-cinema-4d": "0.10.0"
                            },
                            "installer": "/latest/unknown_os/installer"
                        }
                    }
                }
            }
        });
        let server = start_manifest_server(&manifest).await;
        let url = manifest_url(&server);

        let result = safe_check_for_updates(
            "deadline-cloud-for-cinema-4d",
            "0.9.1",
            None,
            Some(&url),
        );

        assert_eq!(result.status, UpdateCheckStatus::ParseError);
        assert!(!result.update_available);
        assert!(result.error_message.as_ref().unwrap().contains("not found in manifest"));
    }

    // ── Invalid version in manifest ─────────────────────────

    #[tokio::test]
    async fn safe_check_invalid_version_in_manifest() {
        let manifest = json!({
            "DeadlineCloudSubmitter": {
                "versions": {
                    "latest": {
                        "macos": {
                            "componentVersions": {
                                "deadline-cloud-for-cinema-4d": "not.a.version!"
                            }
                        },
                        "linux": {
                            "componentVersions": {
                                "deadline-cloud-for-cinema-4d": "not.a.version!"
                            }
                        },
                        "windows": {
                            "componentVersions": {
                                "deadline-cloud-for-cinema-4d": "not.a.version!"
                            }
                        }
                    }
                }
            }
        });
        let server = start_manifest_server(&manifest).await;
        let url = manifest_url(&server);

        let result = safe_check_for_updates(
            "deadline-cloud-for-cinema-4d",
            "0.9.1",
            None,
            Some(&url),
        );

        assert_eq!(result.status, UpdateCheckStatus::InvalidVersion);
        assert!(!result.update_available);
    }

    // ── HTTP 500 from server ────────────────────────────────

    #[tokio::test]
    async fn safe_check_server_error() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/submitters/manifest.json"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;
        let url = manifest_url(&server);

        let result = safe_check_for_updates(
            "deadline-cloud-for-cinema-4d",
            "0.9.1",
            None,
            Some(&url),
        );

        // Server errors are network-level failures
        assert!(
            result.status == UpdateCheckStatus::NetworkError
                || result.status == UpdateCheckStatus::UnexpectedError,
            "expected NetworkError or UnexpectedError, got: {:?}",
            result.status
        );
        assert!(!result.update_available);
    }

    // ── Download URL construction ───────────────────────────

    #[tokio::test]
    async fn safe_check_download_url_constructed_correctly() {
        let server = start_manifest_server(&sample_manifest()).await;
        let url = manifest_url(&server);

        let result = safe_check_for_updates(
            "deadline-cloud-for-cinema-4d",
            "0.9.0",
            None,
            Some(&url),
        );

        assert!(result.update_available);
        let download_url = result.download_url.unwrap();
        // URL should be DOWNLOAD_BASE_URL + installer path
        assert!(
            download_url.starts_with(DOWNLOAD_BASE_URL),
            "download URL should start with {DOWNLOAD_BASE_URL}: {download_url}"
        );
        assert!(
            download_url.contains("DeadlineCloudSubmitter"),
            "download URL should contain installer filename: {download_url}"
        );
    }

    // ── Config setting default ──────────────────────────────

    #[test]
    fn submitter_update_notification_defaults_to_true() {
        let def = deadline_config::settings::find_setting("settings.submitter_update_notification");
        assert!(def.is_some(), "setting should be defined");
        assert_eq!(def.unwrap().default, "true");
    }

    // ── Uncommon but valid version formats (from Python TestUncommonVersionFormats) ──

    #[tokio::test]
    async fn safe_check_pre_release_version_is_older() {
        // Pre-release of 0.10.0 is less than 0.10.0 release
        let server = start_manifest_server(&sample_manifest()).await;
        let url = manifest_url(&server);

        let result = safe_check_for_updates(
            "deadline-cloud-for-cinema-4d",
            "0.10.0-alpha.1",
            None,
            Some(&url),
        );

        assert_eq!(result.status, UpdateCheckStatus::Success);
        assert!(result.update_available, "pre-release should be older than release");
    }

    #[tokio::test]
    async fn safe_check_local_segment_ignored_in_comparison() {
        // Local segments (+patch1) are ignored in version comparison.
        // 0.10.0+patch1 == 0.10.0, so no update available.
        let server = start_manifest_server(&sample_manifest()).await;
        let url = manifest_url(&server);

        let result = safe_check_for_updates(
            "deadline-cloud-for-cinema-4d",
            "0.10.0+patch1",
            None,
            Some(&url),
        );

        assert_eq!(result.status, UpdateCheckStatus::Success);
        assert!(!result.update_available);
    }

    #[tokio::test]
    async fn safe_check_v_prefix_stripped() {
        // "v0.9.0" should be treated as "0.9.0" — older than 0.10.0
        let server = start_manifest_server(&sample_manifest()).await;
        let url = manifest_url(&server);

        let result = safe_check_for_updates(
            "deadline-cloud-for-cinema-4d",
            "v0.9.0",
            None,
            Some(&url),
        );

        assert_eq!(result.status, UpdateCheckStatus::Success);
        assert!(result.update_available);
    }

    // ── Malformed version strings (from Python TestUncommonVersionFormats) ──

    #[tokio::test]
    async fn safe_check_empty_version_string() {
        let server = start_manifest_server(&sample_manifest()).await;
        let url = manifest_url(&server);

        let result = safe_check_for_updates(
            "deadline-cloud-for-cinema-4d",
            "",
            None,
            Some(&url),
        );

        assert_eq!(result.status, UpdateCheckStatus::InvalidVersion);
        assert!(!result.update_available);
    }

    #[tokio::test]
    async fn safe_check_garbage_version_string() {
        let server = start_manifest_server(&sample_manifest()).await;
        let url = manifest_url(&server);

        let result = safe_check_for_updates(
            "deadline-cloud-for-cinema-4d",
            "abc.def.ghi",
            None,
            Some(&url),
        );

        assert_eq!(result.status, UpdateCheckStatus::InvalidVersion);
        assert!(!result.update_available);
        assert!(result.error_message.is_some());
    }
}
