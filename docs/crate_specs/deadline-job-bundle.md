# deadline-job-bundle

Job bundle directory parsing, template loading, parameter validation and
resolution, asset references, YAML/JSON serialization, and job history
directory creation.

## Status: Done

## Dependencies

| Crate | Purpose |
|-------|---------|
| `deadline-models` | Shared error types (`DeadlineError`) |

Note: `deadline-config` is not a direct dependency. `create_job_history_bundle_dir`
takes the history directory path as a parameter. Config lookup happens at the CLI layer.

## Module Layout

```
src/
├── lib.rs           // re-exports
├── loader.rs        // file discovery, symlink validation, YAML/JSON I/O
├── parameters.rs    // parameter validation, resolution, merging, UI controls
├── submission.rs    // AssetReferences, split_parameter_args, parse_frame_range
└── history.rs       // create_job_history_bundle_dir
```

## loader — File Discovery and I/O

### Symlink containment validation

`validate_directory_symlink_containment(job_bundle_dir)` resolves the
bundle directory to its real path, then walks all files and directories
within it. Every path must resolve (after following symlinks) to a
location under the resolved bundle root. The bundle directory itself may
be a symlink. If the path is not a directory, returns an error.

### File discovery

`read_yaml_or_json(job_bundle_dir, filename, required)` checks for
`{filename}.json` and `{filename}.yaml` in the bundle directory:

- Both exist → error ("only one is permitted")
- One exists → returns `(contents, "JSON"|"YAML")`
- Neither exists, required → error ("lacks a {filename}.json or ...")
- Neither exists, not required → returns `("", "")`

Files are read as UTF-8.

### Parsing

`parse_yaml_or_json_content(contents, file_type, bundle_dir, filename)`
parses JSON or YAML content. Unknown file types produce a
`RuntimeError`-equivalent ("Unexpected file type").

`read_yaml_or_json_object(bundle_dir, filename, required)` combines
reading and parsing. Returns `Option<serde_json::Value>` — `None` when
the file is absent and not required.

### Saving

`save_yaml_or_json_to_file(bundle_dir, filename, file_type, data)` writes
data as `{filename}.json` (indent=2) or `{filename}.yaml` (using
`deadline_yaml_dump`). Unknown file types produce an error.

### YAML serialization

`deadline_yaml_dump(data)` serializes a `serde_json::Value` to YAML with:
- `sort_keys=false` (preserves insertion order via `serde_json`'s `preserve_order` feature)
- Multi-line strings use block literal (`|-`) style — handled natively by `serde_yaml`, no post-processing needed

## parameters — Validation and Resolution

### Data representation

Job parameters are represented as `serde_json::Value` (JSON objects),
not typed Rust structs. This matches Python's dict-based approach and
avoids 15+ `Option<T>` fields. Validation functions extract and check
fields from the Value, mutating in place where needed.

### validate_job_parameter

Validates a single parameter definition against the OpenJD schema.
Checks: name (required, non-empty string), type (one of STRING/PATH/
INT/FLOAT), description (string), default (not null), allowedValues
(list), dataFlow (NONE/IN/OUT/INOUT), minLength/maxLength (non-negative
int), minValue/maxValue (numeric), objectType (FILE/DIRECTORY),
userInterface (sub-object). Flags `type_required` and `default_required`
control whether those fields are mandatory.

### validate_job_parameter_value

Validates and coerces a value for a parameter definition:
- STRING/PATH: must be string
- INT: string "19" → integer 19; float 3.7 → error (not an integer)
- FLOAT: string "3.14" → float 3.14

Then checks constraints: minLength, maxLength, minValue, maxValue,
allowedValues.

### validate_user_interface_spec / validate_user_interface_file_filter

Validates the `userInterface` sub-object and its `fileFilters` entries.
File filter patterns must be 1-20 characters.

### read_job_bundle_parameters

Reads template and parameter_values from the bundle directory. Validates
the template has `specificationVersion: jobtemplate-2023-09`. Merges
parameter values into template definitions. For PATH parameters with a
default but no value and no allowedValues, resolves the default relative
to the bundle directory (must be relative, must resolve within bundle).
Validates hidden parameters have values or defaults.

### apply_job_parameters

Applies user-provided parameter values to the parameter list. PATH
values without allowedValues are made absolute relative to CWD. Empty
PATH values are skipped. PATH parameters with dataFlow populate
asset_references:
- IN + FILE → input_filenames
- IN + DIRECTORY → input_directories
- OUT + FILE → parent directory added to output_directories
- OUT + DIRECTORY → output_directories
- INOUT → both input and output
- NONE → referenced_paths

### merge_queue_job_parameters

Merges queue environment parameters with job bundle parameters. Same-name
parameters must agree on type (differences in default are ignored).
Value-only parameters (name + value, no type) for names not in queue
params and without `:` in the name produce an error. Names with `:` are
treated as app-specific and accepted.

### get_ui_control_for_parameter_definition

Returns the UI control for a parameter, using explicit control if set,
otherwise inferring from type: STRING→LINE_EDIT, PATH→CHOOSE_DIRECTORY/
CHOOSE_INPUT_FILE/CHOOSE_OUTPUT_FILE (based on objectType and dataFlow),
INT/FLOAT→SPIN_BOX, any with allowedValues→DROPDOWN_LIST. Validates
control is supported for the parameter type.

### parameter_definition_difference

Compares two parameter definitions field by field. allowedValues compared
as sets. `ignore_missing` flag skips fields absent from either side.

## submission — Asset References and Helpers

### AssetReferences

Struct with four `BTreeSet<String>` fields: input_filenames,
input_directories, output_directories, referenced_paths. BTreeSet gives
deterministic sorted iteration for `to_dict()`.

Methods: `bool()` (any non-empty), `union()`, `from_dict()` (normalizes
paths), `to_dict()` (sorted lists).

### split_parameter_args

Splits parameters into app-specific (e.g. `deadline:priority`) and job
parameters. App parameter names are validated against a supported list.
Other app prefixes (e.g. `maya:`) are silently dropped. Parameter types
are lowercased in output.

### parse_frame_range

Parses frame range strings like `"1-10"` or `"1-10:2"` into a list of
integers. Uses regex: `^(-?\d+)(-(-?\d+)(:(-?\d+))?)?$`.

## history — Job History Directory

### create_job_history_bundle_dir

Creates a dated, sequentially-numbered directory under the configured
`settings.job_history_dir`. Format:
`{job_history_dir}/YYYY-MM/YYYY-MM-DD-{NN}-{submitter}-{job_name}`

Submitter and job names are sanitized (keep alphanumeric, space, hyphen,
underscore). Job name truncated to 128 chars. Sequential number
determined by scanning existing directories for the date prefix.
