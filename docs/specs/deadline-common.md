# deadline-common

Utility functions and shared infrastructure across crates.

## Status: In Progress

Implemented: path utilities, telemetry client.

## Dependencies

| Crate | Purpose |
|-------|---------|
| `deadline-models` | Shared types |
| `deadline-config` | Config for telemetry opt-out and identifier |
| `ureq` | Blocking HTTP POST for telemetry |
| `uuid` | Session ID, telemetry ID, batch ID |
| `serde_json` | Telemetry request body serialization |

## Path Utilities (`path_utils.rs`)

`human_readable_file_size()` and related path helpers.

## Telemetry (`telemetry.rs`)

Background telemetry client matching Python's `_telemetry.py`. Uses
`std::thread` + `std::sync::mpsc` + `ureq` (blocking HTTP) — no async.

### Design

`TelemetryClient` struct with:
- Bounded `mpsc::SyncSender` (capacity 25, matching Python's `MAX_QUEUE_SIZE`)
- Background `std::thread` reads events and POSTs to the telemetry endpoint
- Retry on HTTP 429/500 with exponential backoff + jitter (max 4 attempts)
- Fire-and-forget: `record_event` silently drops if queue full or not initialized
- Opt-out via `DEADLINE_CLOUD_TELEMETRY_OPT_OUT` env var or `telemetry.opt_out` config
- Telemetry identifier: UUID4 from config, generated if missing/invalid
- Package version truncated to first 3 components
- Endpoint: Deadline service URL with `management.` prefix after `https://`

### Consumers

Every crate that calls Deadline APIs or performs user-facing operations
records telemetry events. The `TelemetryClient` lives in `deadline-common`
so all crates can access it without depending on `deadline-client`.
