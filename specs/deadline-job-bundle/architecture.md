# deadline-job-bundle Architecture

## Crate Position

```
deadline-cli ──► deadline-job-bundle    ← this crate
deadline-python-bindings ──► deadline-job-bundle
```

Composes `deadline-api` (API), `deadline-job-attachments` (S3 upload),
and its own parsing logic into a complete submission pipeline.

## Module Layout

The crate has four modules covering the submission lifecycle: loading
bundles from disk, validating and merging parameters, orchestrating the
full submission pipeline, and recording job history.

Bundle loading (`loader.rs`) handles template detection (YAML vs JSON),
symlink containment checks, and file I/O. Parameter validation
(`parameters.rs`) implements the OpenJD type system with coercion rules
and merges queue environment parameters with job template parameters.
Submission orchestration (`submission.rs`) ties everything together —
it's the single entry point that all three consumers (CLI, GUI, MCP)
call to submit a job.

```
src/
├── lib.rs          # Re-exports key types and the submission entry point
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
All three entry points depend on this crate for submission — the
pipeline is not duplicated.

**Symlink containment enforced.** All paths in a bundle directory are
resolved via `fs::canonicalize` and checked to ensure they stay within
the bundle root. This prevents path traversal via symlinks.

**Sorted sets for asset references.** Input filenames, directories,
output directories, and referenced paths use `BTreeSet` for
deterministic iteration and serialization.

**Template file detection.** Both `.json` and `.yaml` extensions are
checked. Both present → error. Neither present and required → error.

## Public API Surface

### Submitting a job

The single entry point for all consumers:

```rust
let job_id = create_job_from_job_bundle(SubmitJobParams {
    job_bundle_dir: "/path/to/bundle".into(),
    job_parameters: vec![serde_json::json!({"Key": "Value"})],
    config: Some(&config),
    print_callback: Box::new(|msg| println!("{msg}")),
    hashing_progress_callback: Some(Box::new(|progress| { /* update UI */ true })),
    upload_progress_callback: Some(Box::new(|progress| { /* update UI */ true })),
    continue_callback: Some(Box::new(|| !cancelled.load(Ordering::Relaxed))),
    ..Default::default()
}).await?;
```

`SubmitJobParams` carries all submission inputs including callbacks for
progress reporting, cancellation, and interactive confirmation. The CLI,
GUI FFI, and MCP server each wire these callbacks differently — the CLI
uses progress bars, the GUI emits Qt signals, the MCP server ignores them.

Progress callbacks return `bool` — returning `false` cancels the
operation. The `continue_callback` is checked between stages.

### Other entry points

| Function | Purpose |
|----------|---------|
| `validate_job_parameter()` | Validate a single parameter against its definition |
| `merge_queue_parameters()` | Merge queue environment parameters with job template parameters |
| `extract_asset_references()` | Extract input/output file references from PATH parameters |
| `parse_frame_range()` | Parse frame range strings like `"1-10:2"` |
| `create_job_history_bundle_dir()` | Create a timestamped history directory for a submission |
