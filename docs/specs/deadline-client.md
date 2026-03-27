# deadline-client

AWS API interaction layer. Owns the SDK/HTTP calls to the Deadline Cloud service.

## Status: Stub

Not yet implemented.

## Consumers

- `deadline-cli` — CLI commands that call Deadline Cloud APIs
- `deadline-worker-agent` — worker agent polling, session management, progress reporting
- `deadline-gui-ffi` — GUI dropdown population (list farms/queues/storage profiles), submission

## Dependencies

| Crate | Purpose |
|-------|---------|
| `deadline-config` | Config for endpoint/credential resolution |
| `deadline-models` | Shared types |
