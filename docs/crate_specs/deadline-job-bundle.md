# deadline-job-bundle

Job bundle parsing, validation, and submission. Handles the full lifecycle
of a job bundle: loading the directory, validating templates, resolving
parameters, and orchestrating the submission pipeline (attachment upload,
CreateJob API call, creation polling).

## Role in the System

The single entry point for job submission. Consumers hand it a bundle
directory and parameters; it gives back a job ID. Internally it composes
the API layer, the attachment engine, and its own parsing logic into a
complete pipeline.

Consumers: `deadline-cli` (bundle submit), `deadline-gui-ffi`
(submission dialog), `deadline-mcp` (submit_job tool).

Dependencies: `deadline-client` (API calls, credentials, telemetry),
`deadline-job-attachments` (hashing, S3 upload), `deadline-models`
(shared types and errors).

## Key Concepts

**A job bundle is a directory, not a file.** It contains a template
(YAML or JSON), optional parameter values, and referenced assets. The
loader discovers files by convention (`template.yaml`/`template.json`,
`parameter_values.yaml`/`parameter_values.json`) — only one format per
file is allowed.

**Symlink containment is enforced.** All files within a bundle must
resolve (after following symlinks) to a location under the bundle root.
The bundle directory itself may be a symlink, but nothing inside can
escape it. This prevents path traversal attacks in user-provided bundles.

**Parameters have a type system.** Four types: STRING, PATH, INT, FLOAT.
Values are validated and coerced (string `"19"` → integer 19 for INT
parameters). Constraints (min/max length, min/max value, allowedValues)
are checked after coercion.

**PATH parameters drive asset references.** A PATH parameter's `dataFlow`
and `objectType` determine whether it contributes to input files, input
directories, output directories, or referenced paths. This is how the
attachment engine knows what to upload without parsing the template's
step scripts.

**Queue environment parameters merge with job parameters.** Queue
environments can define parameters that override or supplement the job
template's parameters. Same-name parameters must agree on type. The merge
logic handles conflicts and app-specific parameters (names containing
`:` like `deadline:priority`).

**Submission is a pipeline, not a single call.** The orchestration
validates the bundle, loads and merges parameters, resolves attachments,
uploads to S3, calls CreateJob, and polls until the job exits
CREATE_IN_PROGRESS. Each phase can fail independently with a specific
error.

## Behavior & Contracts

**Template validation:** Must have `specificationVersion: jobtemplate-2023-09`.
Other versions are rejected.

**Parameter validation rules:**
- Name is required and non-empty
- Type must be STRING, PATH, INT, or FLOAT
- Default value cannot be null
- INT values must be actual integers (float 3.7 → error)
- FLOAT values accept both integer and decimal strings
- allowedValues is checked as a set membership test
- File filter patterns in UI specs must be 1-20 characters

**PATH parameter resolution:** PATH parameters with a default but no
explicit value and no allowedValues have their default resolved relative
to the bundle directory. The resolved path must be relative and must stay
within the bundle. PATH values provided by the user are made absolute
relative to CWD.

**Asset references output:** `AssetReferences` contains four sorted sets:
`input_filenames`, `input_directories`, `output_directories`,
`referenced_paths`. Sorted (via `BTreeSet`) for deterministic iteration
and serialization.

**Job history directory format:**
`{history_dir}/YYYY-MM/YYYY-MM-DD-{NN}-{submitter}-{job_name}` where NN
is a sequential number determined by scanning existing directories.
Submitter and job names are sanitized (alphanumeric, space, hyphen,
underscore only). Job name truncated to 128 characters.

**YAML serialization:** `deadline_yaml_dump` preserves insertion order
(no key sorting) and uses block literal style for multi-line strings.

**Job submission:** Orchestrates the full submission pipeline: validates
the bundle, loads and merges parameters, expands input directories to
file lists, builds the known asset paths list, hashes and uploads
attachments, calls CreateJob, and polls until the job exits
CREATE_IN_PROGRESS. Input directory expansion walks recursively; empty
directories become referenced paths, missing directories error or warn
depending on whether the caller requires paths to exist. Known asset
paths are deduplicated via TRIE-based prefix filtering. When files exist
outside known paths, the behavior depends on auto_accept: with
auto_accept and no GUI, submission is canceled; without auto_accept, the
caller's confirmation callback is invoked. Debug snapshot mode saves the
CreateJob args and scripts to disk without submitting.

**Job creation polling:** After calling CreateJob, polls GetJob with
exponential backoff (0.3s initial, doubles each iteration, capped at 5s,
300s timeout). Checks `lifecycleStatus` first, falls back to legacy
`state` field. Returns success/failure with the status message. Supports
cancellation via a caller-provided callback.

## Design Decisions

**Parameters are `serde_json::Value`, not typed structs.** A parameter
definition has ~15 optional fields with complex validation rules. Typed
structs would require extensive `Option<T>` fields and custom
deserialization. JSON values with runtime validation are simpler and match
the OpenJD spec's JSON Schema approach.

**UI control inference from type.** If a parameter doesn't specify a UI
control, one is inferred: STRING→LINE_EDIT, PATH→CHOOSE_DIRECTORY or
CHOOSE_INPUT_FILE (based on objectType/dataFlow), INT/FLOAT→SPIN_BOX,
anything with allowedValues→DROPDOWN_LIST.

**Orchestration lives here, not in the API layer.** The submission
pipeline composes API calls, bundle parsing, and attachment handling into
a single operation. Placing it in this crate keeps the API layer thin
(just API calls) and avoids a separate orchestration crate. All three
entry points (CLI, GUI FFI, MCP) depend on this crate for submission.

**Storage profile parsed from raw JSON.** The storage profile API returns
raw JSON. The submission orchestration parses this into a typed struct
rather than adding serde derives to the model types. This keeps the
parsing localized to the one call site that needs it.

## Gotchas & Constraints

- Only one of `template.yaml` or `template.json` may exist in a bundle.
  Both present → error. Neither present → error.

- Parameter name conflicts between queue environments and job templates
  are only errors if the *type* differs. Different defaults are silently
  accepted (queue environment wins).

- App-specific parameters (names with `:`) from unknown prefixes are
  silently dropped. Only `deadline:` prefix parameters are validated
  against a known list.

- The `parse_frame_range` function uses a strict regex. Whitespace in
  frame range strings is not tolerated.

- `read_yaml_or_json_object` returns `None` for absent optional files.
  Callers must handle the `None` case — don't assume files exist.

- The submission orchestration depends on `deadline-client` for API calls
  and `deadline-job-attachments` for S3 upload. This makes the crate
  heavier than a pure parsing library — it transitively includes the AWS
  SDK. This is intentional: submission is the primary use case for bundle
  parsing, and separating them would force all three consumers to
  duplicate the orchestration.

## Status & Gaps

Implemented: template loading, parameter validation and merging, PATH
resolution, asset reference extraction, job history directory creation,
YAML serialization, frame range parsing, job submission orchestration
(bundle validation → parameter merging → attachment upload → CreateJob →
creation polling).

Gaps:
- Asset path summary message (file count and path listing before upload)
- Unknown path confirmation prompt (interactive callback flow)
- `--json` output format for submission results
- `--save-debug-snapshot` mode
