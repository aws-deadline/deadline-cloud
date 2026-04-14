# deadline-job-bundle Crate Specifications

Job bundle parsing, validation, and submission. Handles the full lifecycle:
load bundle directory → validate template → resolve parameters → upload
attachments → CreateJob → poll for completion.

The single entry point for job submission. Consumers hand it a bundle
directory and parameters; it gives back a job ID.

Consumers: `deadline-cli` (bundle submit), `deadline-gui-ffi` (submission
dialog), `deadline-mcp` (submit_job tool).

Dependencies: `deadline-api` (API calls), `deadline-job-attachments`
(hashing, S3 upload), `deadline-models` (shared types).

## Document Index

| Document | Description |
|----------|-------------|
| [architecture.md](architecture.md) | Module layout, public API surface, design decisions |
| [template-loading.md](template-loading.md) | Bundle discovery, symlink containment, template validation |
| [parameter-validation.md](parameter-validation.md) | Type system, coercion, constraints, queue environment merge |
| [submission-pipeline.md](submission-pipeline.md) | Full orchestration: validate → merge → upload → CreateJob → poll |

## Status

Implemented: template loading, parameter validation and merging, PATH
resolution, asset reference extraction, job history directory creation,
YAML serialization, frame range parsing, job submission orchestration.

Gaps:
- `--json` output format for submission results
- `--save-debug-snapshot` mode

## Gotchas & Constraints

- Only one of `template.yaml` or `template.json` may exist in a bundle.
  Both present → error. Neither present → error.

- Parameter name conflicts between queue environments and job templates
  are only errors if the *type* differs. Different defaults are silently
  accepted (queue environment wins).

- App-specific parameters (names with `:`) from unknown prefixes are
  silently dropped. Only `deadline:` prefix parameters are validated.

- The `parse_frame_range` function uses a strict regex. Whitespace in
  frame range strings is not tolerated.

- `read_yaml_or_json_object` returns `None` for absent optional files.
  Callers must handle the `None` case — don't assume files exist.

- The crate transitively includes the AWS SDK (via `deadline-api` and
  `deadline-job-attachments`). This is intentional: submission is the
  primary use case, and separating parsing from orchestration would force
  all three consumers to duplicate the pipeline.

## Relationship to the Python Library

Mirrors the Python `deadline.client.job_bundle` module. Key differences:
- Python uses `click` for parameter parsing; Rust uses `serde_json::Value`
  with runtime validation
- Python's `create_job_from_job_bundle` is a single function; Rust's is
  the same pattern but takes a `SubmitJobParams` struct instead of kwargs
- Python resolves PATH defaults via `os.path.join`; Rust uses
  `std::path::Path::join` with explicit containment checks
- Python's queue parameter merge uses Pydantic models; Rust uses raw JSON
  with manual type checking
