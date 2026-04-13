# deadline-common

Shared utilities and infrastructure used across crates: file size
formatting, path sequence detection, and background telemetry.

## Role in the System

The utility layer that sits between `deadline-models` (pure types) and
the business logic crates. Provides two unrelated capabilities: path
utilities for human-readable output, and the telemetry client that all
crates use for usage tracking.

Consumers: `deadline-cli`, `deadline-client`, `deadline-job-attachments`,
`deadline-gui-ffi`.

## Key Concepts

**Telemetry is fire-and-forget.** The `TelemetryClient` uses a background
OS thread with a bounded channel (capacity 25). Events are dropped
silently if the queue is full, the client isn't initialized, or the user
has opted out. A telemetry failure never affects the caller's return value
or exit code. This is a hard invariant — no telemetry code path should
ever surface an error to the user.

**Telemetry metadata comes from the caller.** The telemetry client lives
in `deadline-common` to avoid circular dependencies, but it needs
`user_id`, `monitor_id`, and `account_id` that only `deadline-client`
can provide (via auth checks and STS). The caller passes these during
initialization. This is an intentional architectural seam — don't try to
have `deadline-common` resolve credentials itself.

**Path utilities are for display, not logic.** `human_readable_file_size`
uses SI prefixes (1 KB = 1000 bytes). The numbered path detection groups
files like `frame_001.png` through `frame_100.png` into sequence
representations for compact display. These are output formatting helpers,
not filesystem operations.

## Behavior & Contracts

**Telemetry opt-out:** Checked via `DEADLINE_CLOUD_TELEMETRY_OPT_OUT` env
var OR `telemetry.opt_out` config setting. If either is truthy, no events
are sent and the background thread is never started.

**Telemetry endpoint:** The Deadline service URL with `management.`
prefixed after `https://`, plus `/2023-10-12/telemetry`. The endpoint
prefix is hardcoded.

**Telemetry retry:** HTTP 429 and 500 responses trigger exponential
backoff with jitter, up to 4 attempts. All other failures are swallowed
immediately.

**Telemetry identifier:** A UUID4 stored in the config file. Generated on
first use if missing or invalid. This persists across sessions for usage
correlation.

**`Drop` flushes the queue.** When the `TelemetryClient` is dropped, it
closes the channel and joins the background thread, ensuring queued events
are sent before the process exits.

**File size formatting:** Values near a threshold round up (999999 bytes →
"1.0 MB", not "1000.0 KB"). Bytes show as integers, larger units show one
decimal place.

## Design Decisions

**No async, no tokio for telemetry.** Uses `std::thread` + `mpsc` +
`ureq` (blocking HTTP). Telemetry is a background concern that shouldn't
force an async runtime on callers. The blocking HTTP call happens on a
dedicated thread that never blocks the caller.

**Convenience helpers wrap the common patterns.** `with_telemetry_latency`
(sync) and `with_telemetry_latency_async` (async) exist because every API
function needs the same timing + recording boilerplate. They resolve the
client, time the call, and record the event. Two variants are needed
because Rust's type system distinguishes sync and async at compile time.

**Package version is truncated to 3 components.** The telemetry payload
only includes major.minor.patch, stripping any pre-release or build
metadata. This normalizes version strings for aggregation.

## Gotchas & Constraints

- The telemetry background thread joins on `Drop`. If the main thread
  panics before drop runs, queued events may be lost. This is acceptable
  — telemetry is best-effort.

- `record_event` uses `try_send` (non-blocking). If the channel is full,
  the event is silently dropped. This prevents telemetry from ever
  blocking the hot path.

- The `common_details` map is included in every event. Adding fields here
  affects all telemetry payloads — use sparingly.

- Path utilities assume SI prefixes (powers of 1000, not 1024). This
  matches the convention used in the Deadline Cloud console UI.

## Status & Gaps

Fully implemented. No known gaps.
