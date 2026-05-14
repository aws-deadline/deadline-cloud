# bundle Module Architecture

## Module Position

```
deadline-lib
├── bundle    ← this module
│   ├── api
│   ├── attachments
│   └── config
└── ...

deadline-cli ──► deadline-lib::bundle
deadline-python-bindings ──► deadline-lib::bundle
```

Composes `api` (API), `attachments` (S3 upload),
and its own parsing logic into a complete submission pipeline.

## Module Layout

```
src/bundle/
├── mod.rs          # Re-exports key types and the submission entry point
├── loader.rs       # Bundle loading, template detection, symlink containment
├── parameters.rs   # Parameter validation, type coercion, queue parameter merging
├── submission.rs   # Full submission pipeline, job creation polling
├── hooks.rs        # Pre/post-submission hook framework
└── history.rs      # Job history directory creation and naming
```

## Key Design Decisions

**Parameters are `serde_json::Value`, not typed structs.** A parameter
definition has ~15 optional fields with complex validation rules. JSON
values with runtime validation are simpler and match the OpenJD spec's
JSON Schema approach.

**Orchestration lives here, not in the API layer.** The submission
pipeline composes API calls, bundle parsing, and attachment handling.
All three entry points depend on this module for submission — the
pipeline is not duplicated.

**Symlink containment enforced.** All paths in a bundle directory are
resolved via `fs::canonicalize` and checked to ensure they stay within
the bundle root. This prevents path traversal via symlinks.

**Sorted sets for asset references.** Input filenames, directories,
output directories, and referenced paths use `BTreeSet` for
deterministic iteration and serialization.

## Template Loading

A job bundle is a directory containing:
- `template.yaml` or `template.json` (exactly one, not both)
- Optional `parameter_values.yaml` or `parameter_values.json`
- Referenced asset files

`read_yaml_or_json(bundle_dir, filename, required)` handles detection:
- Both `.json` and `.yaml` exist → error
- Neither exists and `required=true` → error
- Neither exists and `required=false` → returns `None`
- One exists → reads and parses content

`validate_directory_symlink_containment(job_bundle_dir)` recursively
walks all entries, canonicalizes each, and verifies they resolve within
the bundle root. Symlinked directories are not recursed into.

Template must have `specificationVersion: jobtemplate-2023-09`.

## Public API Surface

### Submitting a job

```rust
let job_id = create_job_from_job_bundle(SubmitJobParams {
    job_bundle_dir: PathBuf::from("/path/to/bundle"),
    job_parameters: vec![serde_json::json!({"Key": "Value"})],
    config: &config,
    handler: &my_handler,  // impl SubmissionHandler
    hashing_progress_callback: Some(Box::new(|processed, total| true)),
    upload_progress_callback: Some(Box::new(|processed, total| true)),
    ..Default::default()
}).await?;
```

### Other entry points

| Function | Purpose |
|----------|---------|
| `validate_job_parameter()` | Validate a single parameter against its definition |
| `merge_queue_job_parameters()` | Merge queue environment parameters with job template parameters |
| `apply_job_parameters()` | Apply parameter values to template; extracts asset references from PATH parameters |
| `parse_frame_range()` | Parse frame range strings like `"1-10:2"` |
| `create_job_history_bundle_dir()` | Create a timestamped history directory for a submission |
