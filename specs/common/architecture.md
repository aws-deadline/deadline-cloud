# deadline-common Architecture

## Crate Position in the Workspace

```
deadline-cli ──► deadline-common
deadline-job-bundle ──► deadline-common
deadline-gui-ffi ──► deadline-common
```

Utility crate. Depends on `deadline-config` (for telemetry config reads)
and `deadline-models` (for `SubmitterInfo`). No AWS SDK dependency — the
telemetry module uses `ureq` for blocking HTTP directly.

## Module Layout

```
src/
├── lib.rs          # Re-exports: path_utils, telemetry
├── path_utils.rs   # File size formatting, numbered path detection, sequence grouping,
│                   #   path summarization, sanitization
└── telemetry.rs    # TelemetryClient: background thread + mpsc channel + ureq HTTP,
                    #   convenience helpers for latency recording
```

## Public API Surface

### path_utils.rs

- `human_readable_file_size(u64) -> String` — SI prefixes (1 KB = 1000 bytes),
  integer for bytes, one decimal for larger units.
- `summarize_paths(&[String]) -> Vec<String>` — Groups numbered file sequences
  (e.g., `frame_001.png` through `frame_100.png` → `frame_[001-100].png`).
  Detects zero-padded numbering and variable padding widths.
- `sanitize_path_for_filename(name) -> String` — Replaces non-alphanumeric
  characters (except space, hyphen, underscore) for safe directory names.
  Truncates to 128 characters.

### telemetry.rs

- `TelemetryClient` — Background event sender. Spawns an OS thread with a
  bounded `mpsc::SyncSender` (capacity 25). Events are serialized as JSON
  and sent via `ureq` POST to the Deadline Cloud PutMetricData endpoint.
  Retries with exponential backoff (0.5s base, doubles each attempt, 10s max,
  4 attempts max).
- `TelemetryClient::new(package_name, package_ver, config)` — Reads
  `telemetry.opt_out` and `telemetry.identifier` from config. Generates
  a new UUID4 identifier if none exists and persists it to config. Package
  version truncated to 3 components (major.minor.patch only).
- `TelemetryClient::record_event(event_type, details, is_usage)` — Non-blocking
  send to background thread. Enriches with `common_details` (deadline-cloud-version)
  and `system_metadata`.
- `create_telemetry(config) -> TelemetryClient` — Convenience constructor.
  Reads endpoint from `AWS_ENDPOINT_URL_DEADLINE`, prepends `management.`
  prefix to match the Deadline SDK's Smithy host prefix.
- `record_latency(&TelemetryClient, name, nanos)` — Records a latency event
  with type `com.amazon.rum.deadline.latency` and details
  `{latency: <nanoseconds>, function_call: "<name>"}`.
- `with_telemetry_latency(client, name, f)` — Wraps a sync function with
  latency measurement and recording.
- `with_telemetry_latency_async(client, name, f)` — Async variant. Required
  because Rust's type system distinguishes sync and async at compile time.
- `Drop` impl flushes the queue — closes the channel and joins the background
  thread, ensuring queued events are sent before the process exits.

## Key Design Decisions

**OS thread + ureq, not tokio.** Telemetry must work from both sync (config
commands) and async (API commands) contexts. A dedicated OS thread with
blocking HTTP avoids coupling to the tokio runtime. The bounded channel
(25 events) prevents unbounded memory growth if the network is slow.

**Fire-and-forget.** All telemetry errors are silently swallowed. A telemetry
failure never affects the caller's return value or exit code. This matches
the Python implementation's behavior.

**SI prefixes for file sizes.** 1 KB = 1000 bytes (not 1024). Matches the
Python implementation and AWS console conventions.
