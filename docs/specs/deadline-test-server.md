# deadline-test-server

Test-only crate. Provides a stub AWS Deadline Cloud server and test harness
for CLI subprocess tests (Level 2).

## TestHarness

Each CLI test creates a `TestHarness` which encapsulates:

- A running `wiremock::MockServer` acting as a stub server
- An isolated `TempDir` with an empty config file
- A `cli(&[args])` method that returns an `assert_cmd::Command` with all
  env vars pre-configured (stub server URL, dummy credentials, isolated
  config path, cleared AWS env vars)

### `TestHarness::new()`

Starts stub server, creates temp dir with empty config.

### `TestHarness::with_config(content)`

Same as `new()` but pre-populates the config file with the given INI content.

### `TestHarness::cli(&[args])`

Returns a `Command` pointed at the `deadline` binary with:
- `AWS_ENDPOINT_URL_DEADLINE` → stub server
- `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` → dummy values
- `AWS_DEFAULT_REGION` → us-west-2
- `DEADLINE_CONFIG_FILE_PATH` → isolated temp config
- `HOME` → isolated temp dir
- AWS profile/session env vars cleared

## API Stub Modules

- `deadline_api::farms` — ListFarms, GetFarm response stubs
- `deadline_api::queues` — ListQueues, GetQueue response stubs
- `deadline_api::errors` — Throttling, 404, 403, 500 responses

## Dependencies

| Crate | Purpose |
|-------|---------|
| `wiremock` | HTTP stub server |
| `tempfile` | Isolated temp directories |
| `assert_cmd` | CLI binary execution |
| `serde_json` | Response body construction |
