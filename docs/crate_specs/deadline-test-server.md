# deadline-test-server

Test-only crate providing a fake AWS environment for CLI subprocess tests
(Level 2 tests).

## Role in the System

Dev-dependency of `deadline-cli`. Provides `TestHarness` — a self-contained
test environment with a wiremock stub server, isolated config, and dummy
credentials. Every Level 2 CLI test uses this to run the `deadline` binary
as a subprocess against controlled API responses.

Consumers: `deadline-cli` (tests only).

## Key Concepts

**Complete environment isolation.** Each test gets its own wiremock server
(random port), temp directory (config file, cache), and sanitized
environment (all AWS profile/session env vars stripped). Tests cannot
interfere with each other or with the developer's real AWS credentials.

**Two command builders for different assertion styles.** `cli()` returns
an `assert_cmd::Command` for programmatic assertions (exit code, stdout
contains, etc.). `cmd()` returns a `std::process::Command` for
`insta_cmd::assert_cmd_snapshot!` snapshot tests. Both configure the same
environment — the difference is only in how you assert on the output.

**Stub modules are organized by API resource.** Each file in
`deadline_api/` provides wiremock `Mock` builders for one resource type
(farms, queues, jobs, etc.). Tests mount the mocks they need — unmounted
endpoints return wiremock's default 404.

**The `.localhost` TLD trick.** The Deadline SDK prepends `management.` or
`scheduling.` to the endpoint hostname (Smithy host prefix behavior). In
tests, `endpoint_url` is `http://localhost:PORT`, so the SDK connects to
`http://management.localhost:PORT`. This works because `.localhost` is a
reserved TLD (RFC 6761) that resolves to 127.0.0.1. No `/etc/hosts`
hacking needed.

## Behavior & Contracts

**`TestHarness::new()`** starts the stub server and creates an empty
config file. The config is valid but has no settings — tests that need
specific config use `with_config(content)`.

**`TestHarness::with_config(content)`** pre-populates the config file
with the given INI content. Use this when testing commands that read
farm/queue/job IDs from config.

**Environment variables set on every command:**
- `AWS_ENDPOINT_URL_DEADLINE` / `AWS_ENDPOINT_URL_STS` /
  `AWS_ENDPOINT_URL_CLOUDWATCHLOGS` → stub server
- `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` → dummy values
- `AWS_DEFAULT_REGION` → us-west-2
- `DEADLINE_CONFIG_FILE_PATH` → isolated temp config
- `HOME` → isolated temp dir (prevents reading real `~/.aws/config`)

**Environment variables removed:** `AWS_PROFILE`, `AWS_DEFAULT_PROFILE`,
`AWS_CONFIG_FILE`, `AWS_SHARED_CREDENTIALS_FILE`, `AWS_SESSION_TOKEN`,
`AWS_SECURITY_TOKEN`, `AWS_ENDPOINT_URL`.

## Design Decisions

**No shared mock state between tests.** Each `TestHarness` is independent.
This means tests can run in parallel without coordination. The cost is
slightly more setup per test, but the reliability gain is worth it.

**Mock responses include all fields the real API returns.** When adding
new mock endpoints, include the full response shape (not just the fields
the test cares about). This catches issues where code accidentally
depends on field presence/absence.

**Error mocks are separate from success mocks.** The `errors.rs` module
provides reusable error response builders (AccessDenied,
ResourceNotFound, ValidationException) that any test can mount alongside
success mocks for other endpoints.

## Gotchas & Constraints

- The `deadline` binary must be built before running tests (`cargo build`
  or `cargo test` handles this). If the binary is stale, tests may pass
  against old behavior.

- SDK retry behavior can cause test hangs. If a mock returns a retryable
  error (403, 500) and the test doesn't account for SDK retries, the
  subprocess may retry with backoff until timeout. Error-path tests should
  either configure the SDK to not retry or ensure the mock handles
  repeated requests.

- wiremock's default behavior for unmounted routes is 404 with an empty
  body. This can produce confusing SDK deserialization errors in test
  output if you forget to mount a required mock.

## Status & Gaps

Implemented and actively extended as new CLI commands are added. Stub
coverage includes: farms, queues, fleets, jobs, workers, sessions,
session actions, queue environments, storage profiles, STS, CloudWatch
Logs, telemetry, and various error responses.
