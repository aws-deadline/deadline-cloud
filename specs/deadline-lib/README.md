# deadline-lib

Unified library crate for AWS Deadline Cloud. Contains all business logic
as four modules:

| Module | Role |
|--------|------|
| `config` | INI config read/write, hierarchical setting resolution |
| `api` | AWS SDK client, auth, session, telemetry, responses |
| `bundle` | Job bundle parsing, parameter validation, submission orchestration |
| `attachments` | S3 transfer, manifests, hash/check caches, content-addressed storage |

## Consumers

- `deadline-cli` — headless CLI binary
- `deadline-python-bindings` — PyO3 extension module for GUI and DCC submitters

## Internal Module Dependencies

```
config          (no internal deps)
api             → config
attachments     → config
bundle          → api, attachments, config
```

## External Dependencies

- `openjd-snapshots` — hashing, manifest codec, hash cache, S3 data cache,
  upload/download engine
- `openjd-expr` — path mapping rule application
- `aws-sdk-deadline`, `aws-sdk-s3`, `aws-sdk-sts`, `aws-sdk-cloudwatchlogs`

## Document Index

| Directory | Contents |
|-----------|----------|
| [config/](config/) | Config architecture, setting definitions |
| [api/](api/) | API architecture, session cache, credential scoping, log retrieval |
| [bundle/](bundle/) | Submission pipeline, hooks, parameter validation, template loading |
| [attachments/](attachments/) | S3 transfer, manifests, hash cache, path mapping, incremental download |
