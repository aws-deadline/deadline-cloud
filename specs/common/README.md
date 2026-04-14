# deadline-common Crate Specifications

Utility functions shared across the workspace. Two modules: path utilities
(file size formatting, sequence detection, path summarization) and telemetry
(background CloudWatch event sender).

Consumers: `deadline-cli`, `deadline-job-bundle`, `deadline-job-attachments`,
`deadline-gui-ffi`.

## Document Index

| Document | Description |
|----------|-------------|
| [architecture.md](architecture.md) | Module layout, public API surface, design decisions |

## Status

Fully implemented. No known gaps.

## Gotchas & Constraints

- The telemetry background thread joins on `Drop`. If the main thread
  panics before drop runs, queued events may be lost. Acceptable —
  telemetry is best-effort.

- `record_event` uses `try_send` (non-blocking). If the channel is full
  (25 events), the event is silently dropped. Telemetry never blocks the
  hot path.

- Path utilities assume SI prefixes (powers of 1000, not 1024). Matches
  the Deadline Cloud console UI convention.
