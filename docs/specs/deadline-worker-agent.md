# deadline-worker-agent

Binary crate. Runs on worker machines to execute Deadline Cloud jobs.

## Status: Not started (Phase 2)

## Scope

- Poll for work assignments via `deadline-client`
- Download job input attachments via `deadline-job-attachments`
- Execute session actions (run processes, manage environments)
- Upload job output attachments via `deadline-job-attachments`
- Report progress and status via `deadline-client`
- File permission management for downloaded/uploaded files
- Telemetry reporting

## Dependencies

| Crate | Purpose |
|-------|---------|
| `deadline-config` | Config file operations |
| `deadline-client` | AWS API calls, session management |
| `deadline-job-attachments` | Manifest decoding, hashing, S3 transfer, caches, progress tracking |
| `deadline-models` | Shared types |
| `deadline-common` | Utilities |
