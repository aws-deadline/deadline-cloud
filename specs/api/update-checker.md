# Update Checker

Checks if a newer version of a Deadline Cloud DCC integration is available
by fetching a remote manifest and comparing versions.

Python source: `deadline.client.api._update_checker`

## Behavioral Contract

`safe_check_for_updates(integration_name, current_version, config, manifest_url)`
returns `UpdateCheckResult`. **Never panics.** All errors are captured in the
result struct with an appropriate `UpdateCheckStatus`.

### Flow

1. Check `settings.submitter_update_notification` — if `"false"`, return
   immediately with `update_available=false` (no network call).
2. Fetch manifest JSON from `MANIFEST_URL` (5s timeout).
3. Navigate `DeadlineCloudSubmitter.versions.latest.{platform}`.
4. Look up `componentVersions[integration_name]` → latest version string.
5. Parse and compare versions using `semver`.
6. Build download URL from `DOWNLOAD_BASE_URL + platform_data["installer"]`.
7. If update available but no installer URL, force `update_available=false`.

### Status Codes

| Status | When |
|--------|------|
| `Success` | Check completed (update may or may not be available) |
| `NetworkError` | Connection refused, DNS failure, HTTP error |
| `TimeoutError` | Request exceeded 5s timeout |
| `ParseError` | Invalid JSON, platform not in manifest |
| `InvalidVersion` | Current or manifest version can't be parsed |
| `IntegrationNotFound` | Integration name not in manifest for this platform |
| `UnexpectedError` | Reserved for FFI consumers; not produced in Rust |

## Consumers

- GUI FFI (`deadline-gui-ffi`) — DCC submitters call this at startup
- Not exposed via CLI — no `deadline check-update` command

## Differences from Python

- **Version comparison:** Rust uses `semver` crate; Python uses
  `packaging.version.Version` (PEP 440). Real manifest versions are
  simple semver. Exotic PEP 440 formats (post-release, epoch) return
  `InvalidVersion` in Rust instead of comparing — safe behavior.
- **SSL/TLS:** Python uses botocore's bundled CA cert for DCC environments.
  Rust uses `ureq` with `rustls` + compiled-in `webpki-roots` (includes
  Amazon Root CA 1). No runtime CA bundle needed.
- **`manifest_url` parameter:** Rust adds this for test injection (point
  at wiremock). Python mocks `_fetch_manifest` internally.

## Config Setting

`settings.submitter_update_notification` — default `"true"`. When `"false"`,
`safe_check_for_updates` skips the network call entirely.
