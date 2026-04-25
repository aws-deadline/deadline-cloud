# deadline-api Architecture

## Crate Position in the Workspace

```
deadline-cli ──► deadline-api    ← this crate
deadline-python-bindings ──► deadline-api
deadline-mcp ──► deadline-api
```

All AWS API calls go through this crate — no other crate imports AWS SDK
service crates for Deadline, STS, or CloudWatch. Exception:
`deadline-job-attachments` owns its own S3 and STS clients independently.

## Module Layout

```
src/
├── lib.rs              # Re-exports: session, auth, api, job_monitoring,
│                       #   log_retrieval, queue_parameters, raw_response,
│                       #   errors, submitter_info, path_utils, telemetry,
│                       #   update_checker
├── session.rs          # SessionCache (LazyLock<Mutex>), SessionContext (user-agent),
│                       #   SdkConfig caching per profile, queue credential provider,
│                       #   get_queue_scoped_config, get_queue_user_config
├── auth.rs             # AwsCredentialsSource, AwsAuthenticationStatus,
│                       #   get_credentials_source (parses ~/.aws/config for monitor_id),
│                       #   check_authentication_status (STS GetCallerIdentity),
│                       #   login/logout (spawns DCM process), check_deadline_api_available
├── api.rs              # All Deadline Cloud API calls: list/get/search for farms, queues,
│                       #   fleets, jobs, workers, sessions, steps, tasks; update_job,
│                       #   update_task, create_job; paginated_list helper; format_sdk_error
├── raw_response.rs     # ResponseBodyCapture interceptor: captures raw HTTP response body,
│                       #   converts datetimes, removes nulls
├── job_monitoring.rs   # wait_for_job_completion: polls GetJob until terminal state,
│                       #   collects failed task details, exponential backoff
├── log_retrieval.rs    # get_session_logs, get_worker_logs: CloudWatch Logs integration,
│                       #   session auto-selection, fleet-scoped credential resolution
├── queue_parameters.rs # get_queue_parameter_definitions: fetches queue environments,
│                       #   parses YAML templates, extracts and deduplicates parameters
├── errors.rs           # DeadlineError (6 variants), formerly in deadline-models
├── submitter_info.rs   # SubmitterInfo struct, YamlValue enum, formerly in deadline-models
├── path_utils.rs       # human_readable_file_size, summarize_paths, sanitize_path_for_filename,
│                       #   formerly in deadline-common
├── telemetry.rs        # TelemetryClient: background thread + mpsc channel + ureq HTTP,
│                       #   formerly in deadline-common
└── update_checker.rs   # safe_check_for_updates: fetch remote manifest, compare versions,
                        #   config opt-out, never panics. Used by GUI FFI / DCC submitters.
```

## Key Design Decisions

**Raw JSON response capture.** All API calls use `ResponseBodyCapture` interceptor
that grabs the raw HTTP response body as `serde_json::Value`. SDK output types
don't implement `Serialize` (upstream issue). The interceptor post-processes
datetime strings and removes null values. New API fields appear automatically
without code changes.

**Global session cache.** `LazyLock<Mutex<SessionCache>>` holds cached `SdkConfig`
and queue credential configs. Cache keyed by profile name — changing profile
invalidates the cache. Queue configs cached by `(farm_id, queue_id)`. Invalidated
on logout or `force_refresh`.

**Manual pagination loops.** Uses `nextToken` loops (not SDK paginators) because
paginators don't support `.customize().interceptor()`. A shared `paginated_list`
helper eliminates duplicated loop logic.

**Telemetry on every API call.** All public API functions accept optional
`&TelemetryClient` and record latency events via `with_telemetry_latency_async`.
If `None`, an ephemeral client is created internally. Telemetry never affects
the API call result.

**DCM detection via `~/.aws/config` parsing.** `get_credentials_source` reads
the AWS config file directly (not via the SDK) to check for `monitor_id` in the
profile section. If the profile section doesn't exist, returns `NotValid`.

**Login/logout spawns DCM process.** `auth::login()` spawns the Deadline Cloud
Monitor binary (path from `deadline-cloud-monitor.path` config setting) with
`login` args. Polls `check_authentication_status` in a 0.5s loop. `logout()`
spawns with `logout` args and invalidates the session cache.
