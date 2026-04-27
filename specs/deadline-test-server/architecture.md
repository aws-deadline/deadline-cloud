# deadline-test-server Architecture

## Crate Position in the Workspace

```
deadline-cli ──[dev-dependency]──► deadline-test-server    ← this crate
```

Test-only. Not published or used at runtime.

## Module Layout

```
src/
├── lib.rs              # Re-exports: harness, deadline_api
├── harness.rs          # TestHarness: temp config dir, wiremock server,
│                       #   assert_cmd Command builder, env var setup
└── deadline_api/
    ├── mod.rs          # Mock router: mounts all API endpoint handlers
    ├── sts.rs          # STS GetCallerIdentity mock
    ├── farms.rs        # ListFarms, GetFarm mocks
    ├── queues.rs       # ListQueues mock
    ├── queue_resources.rs  # GetQueue, ListQueueEnvironments, GetQueueEnvironment,
    │                       #   ExportCredentials mocks
    ├── fleets.rs       # ListFleets, GetFleet mocks
    ├── workers.rs      # ListWorkers mock
    ├── jobs.rs         # ListJobs, GetJob mocks
    ├── sessions.rs     # ListSessions, ListSessionActions mocks
    ├── telemetry.rs    # PutMetricData mock (accepts and discards)
    ├── cloudwatch.rs   # CloudWatch Logs GetLogEvents mock
    ├── bundle.rs       # CreateJob mock for bundle submit tests
    ├── s3.rs           # S3 PutObject/GetObject/HeadObject mocks
    └── errors.rs       # Error response builders (AccessDenied, ResourceNotFound,
                        #   ValidationException, ThrottlingException)
```

## TestHarness

Creates an isolated test environment:
1. Temp directory with a fresh `~/.deadline/config`
2. Wiremock server with all Deadline API endpoints mounted
3. Environment variables set: `DEADLINE_CONFIG_FILE_PATH`, `AWS_ENDPOINT_URL_DEADLINE`,
   `AWS_ENDPOINT_URL_STS`, `AWS_ENDPOINT_URL_CLOUDWATCHLOGS`
4. `assert_cmd::Command` builder pre-configured with the `deadline` binary
   and the test environment

The Deadline SDK prepends `management.` or `scheduling.` to the endpoint
hostname (Smithy host prefix). The `.localhost` TLD resolves to 127.0.0.1
per RFC 6761.

## Adding a New Mock Endpoint

1. Create or edit a file in `deadline_api/` (e.g., `jobs.rs`)
2. Add a wiremock `Mock::given(method(...)).and(path(...)).respond_with(...)` handler
3. Mount it in `deadline_api/mod.rs` by calling your mount function from `mount_all`
4. The mock receives the raw HTTP request — match on method, path, headers, and body
5. Use `errors.rs` helpers (`access_denied_response`, `not_found_response`, etc.)
   for error test cases
