# deadline-test-server

Test-only crate. Provides a fake AWS Deadline Cloud server and test harness
for CLI subprocess tests (Level 3).

## TestHarness

Each CLI test creates a `TestHarness` which encapsulates:

- A running `wiremock::MockServer`
- An isolated `TempDir` with an empty config file
- A `cli(&[args])` method that returns an `assert_cmd::Command` with all
  env vars pre-configured (fake server URL, dummy credentials, isolated
  config path, cleared AWS env vars)

### `TestHarness::new()`

Starts server, creates temp dir with empty config.

### `TestHarness::with_config(content)`

Same as `new()` but pre-populates the config file with the given INI content.

### `TestHarness::cli(&[args])`

Returns a `Command` pointed at the `deadline` binary with:
- `AWS_ENDPOINT_URL_DEADLINE` → fake server
- `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` → dummy values
- `AWS_DEFAULT_REGION` → us-west-2
- `DEADLINE_CONFIG_FILE_PATH` → isolated temp config
- `HOME` → isolated temp dir
- AWS profile/session env vars cleared

## API Mock Modules

- `deadline_api::farms` — ListFarms, GetFarm mocks
- `deadline_api::queues` — ListQueues, GetQueue mocks
- `deadline_api::errors` — Throttling, 404, 403, 500 responses

## Dependencies

| Crate | Purpose |
|-------|---------|
| `wiremock` | HTTP mock server |
| `tempfile` | Isolated temp directories |
| `assert_cmd` | CLI binary execution |
| `serde_json` | Response body construction |
