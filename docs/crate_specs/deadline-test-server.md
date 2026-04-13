# deadline-test-server

Test-only crate. Provides a stub AWS Deadline Cloud server and test harness
for CLI subprocess tests (Level 2).

## TestHarness

Each CLI test creates a `TestHarness` which encapsulates:

- A running `wiremock::MockServer` acting as a stub server
- An isolated `TempDir` with an empty config file
- `cli(&[args])` → `assert_cmd::Command` for `.assert()` chains and
  file side-effect checks
- `cmd(&[args])` → `std::process::Command` for `insta_cmd::assert_cmd_snapshot!`

Both methods configure the same env vars (stub server URL, dummy
credentials, isolated config path, cleared AWS env vars).

### `TestHarness::new()`

Starts stub server, creates temp dir with empty config.

### `TestHarness::with_config(content)`

Same as `new()` but pre-populates the config file with the given INI content.

### `TestHarness::cli(&[args])` / `TestHarness::cmd(&[args])`

Returns a `Command` pointed at the `deadline` binary with:
- `AWS_ENDPOINT_URL_DEADLINE` / `AWS_ENDPOINT_URL_STS` → stub server
- `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` → dummy values
- `AWS_DEFAULT_REGION` → us-west-2
- `DEADLINE_CONFIG_FILE_PATH` → isolated temp config
- `HOME` → isolated temp dir
- AWS profile/session env vars cleared

Use `cmd()` for snapshot tests, `cli()` for everything else.

## API Stub Modules

- `deadline_api::farms` — ListFarms (with pagination, principalId), GetFarm
- `deadline_api::queues` — ListQueues, GetQueue
- `deadline_api::fleets` — ListFleets, GetFleet
- `deadline_api::jobs` — ListJobs, GetJob
- `deadline_api::workers` — SearchWorkers, GetWorker
- `deadline_api::sts` — GetCallerIdentity (success/failure)
- `deadline_api::errors` — AccessDenied, ResourceNotFound, SearchWorkers errors

## Dependencies

| Crate | Purpose |
|-------|---------|
| `wiremock` | HTTP stub server |
| `tempfile` | Isolated temp directories |
| `assert_cmd` | CLI binary execution and `cargo_bin` path resolution |
| `serde_json` | Response body construction |
