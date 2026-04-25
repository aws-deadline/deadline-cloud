# deadline-job-bundle Architecture

## Crate Position in the Workspace

```
deadline-cli ──► deadline-job-bundle    ← this crate
deadline-python-bindings ──► deadline-job-bundle
deadline-mcp ──► deadline-job-bundle
```

Composes `deadline-api` (API), `deadline-job-attachments` (S3 upload),
and its own parsing logic into a complete submission pipeline.

## Module Layout

```
src/
├── lib.rs           # Re-exports: loader, parameters, submission, history
│                    #   Key re-exports: AssetReferences, create_job_from_job_bundle,
│                    #   SubmitJobParams
├── loader.rs        # validate_directory_symlink_containment (recursive walk with
│                    #   canonicalize), read_yaml_or_json (detects .json/.yaml,
│                    #   rejects both present), read_yaml_or_json_object,
│                    #   parse_yaml_or_json_content, save_yaml_or_json_to_file,
│                    #   deadline_yaml_dump
├── parameters.rs    # validate_job_parameter (name, type, default, constraints),
│                    #   validate_and_coerce_value (STRING/PATH/INT/FLOAT coercion),
│                    #   merge_queue_parameters (queue env + job template merge),
│                    #   extract_asset_references (PATH dataFlow/objectType → asset sets),
│                    #   infer_ui_control, parse_frame_range
├── submission.rs    # AssetReferences (BTreeSet-based, deterministic iteration),
│                    #   SubmitJobParams (all submission inputs), create_job_from_job_bundle
│                    #   (full orchestration pipeline), poll_create_job (exponential backoff),
│                    #   expand_input_directories, known_asset_path_deduplication
└── history.rs       # create_job_history_dir: {history_dir}/YYYY-MM/YYYY-MM-DD-{NN}-
                     #   {submitter}-{job_name}, sequential numbering, name sanitization
```

## Key Design Decisions

**Parameters are `serde_json::Value`, not typed structs.** A parameter definition
has ~15 optional fields with complex validation rules. JSON values with runtime
validation are simpler and match the OpenJD spec's JSON Schema approach.

**Orchestration lives here, not in the API layer.** The submission pipeline
composes API calls, bundle parsing, and attachment handling. All three entry
points (CLI, GUI FFI, MCP) depend on this crate for submission.

**Symlink containment enforced.** `validate_directory_symlink_containment` uses
`fs::canonicalize` to resolve all paths, then checks each resolved path starts
with the resolved bundle root. Symlinked directories are not recursed into
(only their resolved target is checked).

**`BTreeSet` for asset references.** `input_filenames`, `input_directories`,
`output_directories`, `referenced_paths` are sorted sets for deterministic
iteration and serialization. `AssetReferences::union` combines two sets.

**Template file detection.** `read_yaml_or_json` checks for both `.json` and
`.yaml` extensions. Both present → error. Neither present and required → error.
Neither present and optional → returns empty.
