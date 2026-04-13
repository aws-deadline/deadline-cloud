# deadline-job-bundle

Job bundle directory parsing, template loading, parameter validation and
resolution, asset reference extraction, and job history directory creation.

## Role in the System

Handles everything about a job bundle *before* it touches AWS. Parses the
bundle directory structure, validates templates against the OpenJD spec,
resolves parameters (merging user values, queue environment values, and
defaults), and extracts asset references for the attachment subsystem.

Consumers: `deadline-cli` (bundle submit, gui-submit), `deadline-gui-ffi`
(submission dialog).

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
attachment subsystem knows what to upload/download without parsing the
template's step scripts.

**Queue environment parameters merge with job parameters.** Queue
environments can define parameters that override or supplement the job
template's parameters. Same-name parameters must agree on type. The merge
logic handles conflicts and app-specific parameters (names containing
`:` like `deadline:priority`).

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

**No dependency on `deadline-config`.** The history directory path is
passed as a parameter by the CLI layer. This keeps the crate focused on
bundle logic without coupling to config file mechanics.

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

## Status & Gaps

Fully implemented for the current scope.

Gaps:
- `create_job_from_job_bundle` orchestration (the actual submission API
  call) lives in `deadline-client`, not here. This crate only prepares
  the bundle data.
