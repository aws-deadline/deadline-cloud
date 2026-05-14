# bundle Module

Job bundle parsing, validation, and submission. Handles the full lifecycle:
load bundle directory → validate template → resolve parameters → upload
attachments → CreateJob → poll for completion.

## What It Does

The single entry point for job submission. Consumers hand it a bundle
directory and parameters; it gives back a job ID.

Submission is a multi-stage orchestration:
1. **Load & validate** — detect template format, check symlink containment
2. **Merge parameters** — fetch queue env params, merge with template, validate types
3. **Upload attachments** — hash inputs, create manifests, upload to S3 CAS
4. **CreateJob** — call the Deadline API with the assembled payload
5. **Poll** — wait for the job to leave `CREATE_IN_PROGRESS` state

All three entry points (CLI, GUI, MCP) use this module — the pipeline
is not duplicated.

## Consumers

- `deadline-cli` — `bundle submit` command
- `deadline-python-bindings` — submission dialog
- `deadline-cli` MCP server — `submit_job` tool

## Dependencies

- `deadline-lib::api` — API calls (CreateJob, queue parameters)
- `deadline-lib::attachments` — hashing, S3 upload
- `deadline-lib::config` — setting resolution

## Document Index

| Document | Description |
|----------|-------------|
| [architecture.md](architecture.md) | Module layout, template loading, design decisions, public API |
| [parameter-validation.md](parameter-validation.md) | Type system, coercion, constraints, queue environment merge |
| [submission-pipeline.md](submission-pipeline.md) | Full orchestration flow, SubmitJobParams, SubmissionHandler |
| [submission-hooks.md](submission-hooks.md) | Pre/post-submission hook framework |

## Status

Fully implemented. No known gaps.

## Gotchas

- Only one of `template.yaml` or `template.json` may exist in a bundle.
- Parameter name conflicts between queue environments and job templates
  are only errors if the *type* differs.
- App-specific parameters (names with `:`) from unknown prefixes are silently dropped.
- `parse_frame_range` uses a strict regex — no whitespace tolerance.
