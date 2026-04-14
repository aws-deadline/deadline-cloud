# Job Bundle — Loading, Parameters & History

> Part of [AWS Deadline Cloud Client — Behavioral Test Specification](index.md)
>
> This document describes observable behavior through public interfaces.
> Error descriptions use category labels (e.g., "returns error") rather than
> language-specific exception types. The Rust implementation should produce
> equivalent observable outcomes, not necessarily identical internal mechanics.

---

## Section 15: Job bundle — loading & parsing

> **Rust crate:** `deadline-job-bundle` · **Module:** `loader`
>
> **Logic under test:** File discovery rules (JSON vs YAML, mutual exclusion), symlink
> containment validation, YAML/JSON parsing, and the `deadline_yaml_dump` serializer
> that uses block-literal style for multi-line strings.
> See [data_flow.md § Job Bundle Format](data_flow.md#job-bundle-format).

### `validate_directory_symlink_containment(job_bundle_dir)`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 1 | Happy path | Bundle directory with no symlinks | No error | |
| 2 | Happy path | Bundle directory is itself a symlink to a valid directory | No error (resolved root is used) | |
| 3 | Error handling | Bundle path is not a directory | Returns error "not a directory" | |
| 4 | Error handling | Bundle contains a symlink pointing outside the resolved bundle root | Returns error with "resolves outside" message | |
| 5 | Happy path | Bundle contains a symlink pointing to a file inside the bundle | No error | |
| 6 | Happy path | Bundle contains nested subdirectories with no symlinks | No error | |

### `read_yaml_or_json(job_bundle_dir, filename, required) -> (string, string)`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 7 | Happy path | Only `{filename}.json` exists | Returns `(file_contents, "JSON")` | |
| 8 | Happy path | Only `{filename}.yaml` exists | Returns `(file_contents, "YAML")` | |
| 9 | Error handling | Both `{filename}.json` and `{filename}.yaml` exist | Returns error "only one is permitted" | |
| 10 | Error handling | Neither exists and `required=true` | Returns error "lacks a {filename}.json or {filename}.yaml" | |
| 11 | Happy path | Neither exists and `required=false` | Returns `("", "")` | |
| 12 | Happy path | File is read with UTF-8 encoding | Content is correctly decoded | |

### `parse_yaml_or_json_content(file_contents, file_type, bundle_dir, filename)`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 13 | Happy path | Valid JSON content with `file_type="JSON"` | Returns parsed object | |
| 14 | Happy path | Valid YAML content with `file_type="YAML"` | Returns parsed object | |
| 15 | Error handling | Invalid JSON content | Returns error with "Error loading" | |
| 16 | Error handling | Invalid YAML content | Returns error with "Error loading" | |
| 17 | Error handling | Unknown `file_type` (e.g., `"XML"`) | Returns error "Unexpected file type" | |

### `read_yaml_or_json_object(bundle_dir, filename, required) -> optional dict`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 18 | Happy path | File exists and is valid | Returns parsed dict | |
| 19 | Happy path | File doesn't exist and `required=false` | Returns none | |

### `save_yaml_or_json_to_file(bundle_dir, filename, file_type, data)`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 20 | Happy path | Save as YAML | File written as `{filename}.yaml` with block-literal multi-line style | |
| 21 | Happy path | Save as JSON | File written as `{filename}.json` with indent=2 | |
| 22 | Error handling | Unknown file_type | Returns error "Unexpected file type" | |

### `deadline_yaml_dump(data, stream?) -> string`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 23 | Happy path | Data contains multi-line strings | Multi-line strings use `\|` style in output | |
| 24 | Happy path | Data contains single-line strings | Normal YAML string representation | |

---

## Section 16: Job bundle — parameters

> **Rust crate:** `deadline-job-bundle` · **Module:** `parameters`
>
> **Logic under test:** OpenJD parameter validation (types, constraints, UI controls),
> value coercion (string→int, string→float), parameter merging between job bundle and
> queue environments, PATH parameter resolution (relative→absolute), and asset reference
> population from PATH parameters with dataFlow metadata.

### `validate_job_parameter(input, type_required?, default_required?) -> JobParameter`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 1 | Happy path | Valid parameter with name, type `"STRING"`, and default | Returns validated parameter | |
| 2 | Happy path | Valid parameter with type `"PATH"` | Returns validated parameter | |
| 3 | Happy path | Valid parameter with type `"INT"` | Returns validated parameter | |
| 4 | Happy path | Valid parameter with type `"FLOAT"` | Returns validated parameter | |
| 5 | Missing/invalid args | Input is not a dict | Returns error (wrong type) | |
| 6 | Missing/invalid args | Missing `"name"` field | Returns error containing "No name field" | |
| 7 | Missing/invalid args | `"name"` is not a string | Returns error (wrong type) | |
| 8 | Boundary values | `"name"` is empty string | Returns error "empty name" | |
| 9 | Missing/invalid args | `"type"` is `"UNKNOWN"` | Returns error listing valid types | |
| 10 | Missing/invalid args | `type_required=true` and no `"type"` field | Returns error "missing required key type" | |
| 11 | Happy path | `type_required=false` and no `"type"` field | No error; returns parameter without type | |
| 12 | Missing/invalid args | `default_required=true` and no `"default"` field | Returns error "missing required key default" | |
| 13 | Missing/invalid args | `"default"` is null/none | Returns error "had None for default" | |
| 14 | Missing/invalid args | `"description"` is not a string | Returns error (wrong type) | |
| 15 | Missing/invalid args | `"allowedValues"` is not a list | Returns error (wrong type) | |
| 16 | Missing/invalid args | `"dataFlow"` is `"INVALID"` | Returns error listing valid values | |
| 17 | Happy path | `"dataFlow"` is `"INOUT"` | Returns validated parameter | |
| 18 | Missing/invalid args | `"minLength"` is not an int | Returns error (wrong type) | |
| 19 | Missing/invalid args | `"minLength"` is negative | Returns error "must be non-negative" | |
| 20 | Missing/invalid args | `"maxLength"` is not an int | Returns error (wrong type) | |
| 21 | Missing/invalid args | `"maxLength"` is negative | Returns error "must be non-negative" | |
| 22 | Missing/invalid args | `"minValue"` is a non-numeric string like `"abc"` | Returns error "non-numeric string" | |
| 23 | Happy path | `"minValue"` is a numeric string like `"3.14"` | No error | |
| 24 | Missing/invalid args | `"minValue"` is a bool | Returns error (wrong type) | |
| 25 | Missing/invalid args | `"maxValue"` is a non-numeric string | Returns error (invalid value) | |
| 26 | Missing/invalid args | `"objectType"` is `"SYMLINK"` | Returns error listing valid values | |
| 27 | Happy path | `"objectType"` is `"FILE"` | Returns validated parameter | |
| 28 | Happy path | Parameter with valid `"userInterface"` sub-object | Returns validated parameter with userInterface | |

### `validate_user_interface_spec(input, parameter_name) -> UserInterfaceSpec`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 29 | Happy path | Valid spec with `"control": "LINE_EDIT"` | Returns validated spec | |
| 30 | Missing/invalid args | Input is not a dict | Returns error (wrong type) | |
| 31 | Missing/invalid args | `"control"` is `"INVALID_CONTROL"` | Returns error listing valid controls | |
| 32 | Missing/invalid args | `"label"` is not a string | Returns error (wrong type) | |
| 33 | Missing/invalid args | `"groupLabel"` is not a string | Returns error (wrong type) | |
| 34 | Missing/invalid args | `"decimals"` is not an int | Returns error (wrong type) | |
| 35 | Missing/invalid args | `"decimals"` is negative | Returns error "non-negative" | |
| 36 | Missing/invalid args | `"singleStepDelta"` is a string | Returns error (wrong type) | |
| 37 | Missing/invalid args | `"singleStepDelta"` is zero or negative | Returns error "positive number" | |
| 38 | Missing/invalid args | `"fileFilters"` is not a list | Returns error (wrong type) | |
| 39 | Happy path | `"fileFilters"` is a valid list of filter objects | Returns validated spec | |

### `validate_user_interface_file_filter(input, parameter_name, field_path) -> UserInterfaceFileFilter`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 40 | Happy path | Valid filter with `"label"` and `"patterns"` | Returns validated filter | |
| 41 | Missing/invalid args | Input is not a dict | Returns error (wrong type) | |
| 42 | Missing/invalid args | Missing `"label"` | Returns error "missing required key" | |
| 43 | Missing/invalid args | `"label"` is not a string | Returns error (wrong type) | |
| 44 | Missing/invalid args | Missing `"patterns"` | Returns error "missing required key" | |
| 45 | Missing/invalid args | `"patterns"` is not a list | Returns error (wrong type) | |
| 46 | Missing/invalid args | Pattern entry is not a string | Returns error (wrong type) | |
| 47 | Boundary values | Pattern is empty string | Returns error "between 1 and 20 characters" | |
| 48 | Boundary values | Pattern is longer than 20 characters | Returns error "between 1 and 20 characters" | |

### `validate_job_parameter_value(job_parameter, value) -> string | int | float`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 49 | Happy path | STRING parameter with string value | Returns the string value | |
| 50 | Happy path | PATH parameter with string value | Returns the string value | |
| 51 | Happy path | INT parameter with integer value | Returns the integer | |
| 52 | Happy path | INT parameter with string `"19"` | Returns integer `19` | |
| 53 | Missing/invalid args | INT parameter with float `3.7` | Returns error "not an integer" | |
| 54 | Missing/invalid args | INT parameter with string `"abc"` | Returns error (invalid value) | |
| 55 | Happy path | FLOAT parameter with float value | Returns the float | |
| 56 | Happy path | FLOAT parameter with string `"3.14"` | Returns float `3.14` | |
| 57 | Missing/invalid args | FLOAT parameter with non-numeric string | Returns error (invalid value) | |
| 58 | Missing/invalid args | STRING parameter with non-string value | Returns error (wrong type) | |
| 59 | Missing/invalid args | Parameter with unsupported type | Returns error "unsupported type" | |
| 60 | Boundary values | Value shorter than `minLength` | Returns error "shorter than minLength" | |
| 61 | Boundary values | Value longer than `maxLength` | Returns error "longer than maxLength" | |
| 62 | Boundary values | Value less than `minValue` | Returns error "less than minValue" | |
| 63 | Boundary values | Value greater than `maxValue` | Returns error "greater than maxValue" | |
| 64 | Missing/invalid args | Value not in `allowedValues` | Returns error "not an allowed value" | |
| 65 | Happy path | Value is in `allowedValues` | Returns the value | |

### `read_job_bundle_parameters(bundle_dir) -> list of job parameters`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 66 | Happy path | Valid template with parameters and parameter_values | Returns merged list with values applied | |
| 67 | Error handling | Template is not a dict | Returns error "does not contain a top-level object" | |
| 68 | Error handling | Template missing `specificationVersion` | Returns error "does not contain a specificationVersion" | |
| 69 | Error handling | Template has unsupported `specificationVersion` | Returns error "unsupported specificationVersion" | |
| 70 | Error handling | `parameterDefinitions` is not a list | Returns error "must be a list" | |
| 71 | Happy path | Template has no `parameterDefinitions` | Returns empty list (or only parameter_values entries) | |
| 72 | Happy path | Parameter value exists for a name not in template | Kept in the result (may be queue or app-specific parameter) | |
| 73 | Happy path | PATH parameter with relative default and no value | Value is set to the absolute path resolved from bundle directory | |
| 74 | Error handling | PATH parameter with absolute default path | Returns error "is absolute" | |
| 75 | Error handling | PATH parameter with relative default resolving outside bundle | Returns error "outside of Job Bundle directory" | |
| 76 | Happy path | PATH parameter with `allowedValues` and relative default | Default is NOT made absolute (allowedValues constraint skips path resolution) | |
| 77 | Error handling | HIDDEN parameter with no value and no default | Returns error "Hidden parameter ... is missing a value" | |
| 78 | Error handling | Multiple HIDDEN parameters missing values | Error message lists all missing parameter names | |
| 79 | Happy path | HIDDEN parameter with a default value | No error | |
| 80 | Happy path | No `parameter_values` file exists | Parameters returned with only template defaults | |

### `apply_job_parameters(job_parameters, job_bundle_dir, parameters, asset_references)`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 81 | Happy path | Job parameter provides a value for a template parameter | Parameter's `"value"` is updated | |
| 82 | Happy path | PATH parameter value is relative | Made absolute relative to current working directory | |
| 83 | Happy path | PATH parameter with `allowedValues` and relative value | Value is NOT made absolute | |
| 84 | Boundary values | PATH parameter value is empty string | Parameter is skipped (not added to asset references) | |
| 85 | Error handling | Parameter has no value and no default | Returns error "No parameter value provided" | |
| 86 | Happy path | PATH parameter with `dataFlow="IN"` and `objectType="DIRECTORY"` | Path added to `input_directories` | |
| 87 | Happy path | PATH parameter with `dataFlow="IN"` and `objectType="FILE"` | Path added to `input_filenames` | |
| 88 | Happy path | PATH parameter with `dataFlow="OUT"` and `objectType="DIRECTORY"` | Path added to `output_directories` | |
| 89 | Happy path | PATH parameter with `dataFlow="OUT"` and `objectType="FILE"` | Parent directory added to `output_directories` | |
| 90 | Happy path | PATH parameter with `dataFlow="INOUT"` | Path added to both input and output sets | |
| 91 | Happy path | PATH parameter with `dataFlow="NONE"` | Path added to `referenced_paths` | |
| 92 | Error handling | PATH parameter with invalid `dataFlow` value | Returns error listing valid values | |
| 93 | Happy path | Non-PATH parameter (e.g., STRING) | No asset reference modification | |

### `merge_queue_job_parameters(job_parameters, queue_parameters, queue_id?) -> list of job parameters`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 94 | Happy path | No overlapping parameter names | Returns union of both lists | |
| 95 | Happy path | Overlapping parameter with same type | Merged without error; job bundle's default takes priority | |
| 96 | Error handling | Overlapping parameter with different types | Returns error "conflicting parameter definitions" | |
| 97 | Happy path | Job parameter has value, queue parameter has same name | Value is copied to the merged parameter | |
| 98 | Happy path | Job parameter is value-only (name + value, no type) for a queue parameter | Value is merged; no type comparison needed | |
| 99 | Error handling | Value-only parameter for a name not in queue parameters and no `:` in name | Returns error "undefined parameter" | |
| 100 | Happy path | Value-only parameter with `:` in name (app-specific) | Added to result without error | |
| 101 | Happy path | `queue_id` is provided and there's a mismatch | Error message includes the queue_id | |
| 102 | Happy path | `queue_id` is not provided and there's a mismatch | Error message says "queue" without ID | |
| 103 | Happy path | Overlapping parameter with different defaults but same type | Merged without error; default difference is ignored | |

### `get_ui_control_for_parameter_definition(param_def) -> string`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 104 | Happy path | Parameter has explicit `userInterface.control` set | Returns the explicit control value | |
| 105 | Happy path | STRING parameter with no control and no `allowedValues` | Returns `"LINE_EDIT"` | |
| 106 | Happy path | PATH parameter with `objectType="DIRECTORY"` and no control | Returns `"CHOOSE_DIRECTORY"` | |
| 107 | Happy path | PATH parameter with `objectType="FILE"` and `dataFlow="OUT"` | Returns `"CHOOSE_OUTPUT_FILE"` | |
| 108 | Happy path | PATH parameter with `objectType="FILE"` and `dataFlow="IN"` | Returns `"CHOOSE_INPUT_FILE"` | |
| 109 | Happy path | INT parameter with no control and no `allowedValues` | Returns `"SPIN_BOX"` | |
| 110 | Happy path | Any parameter with `allowedValues` and no explicit control | Returns `"DROPDOWN_LIST"` | |
| 111 | Error handling | Explicit control is `"SPIN_BOX"` for a STRING parameter | Returns error "unsupported control" | |
| 112 | Error handling | Explicit control is `"DROPDOWN_LIST"` but no `allowedValues` | Returns error "must supply allowedValues" | |
| 113 | Error handling | Unsupported parameter type | Returns error "unsupported type" | |

### `parameter_definition_difference(lhs, rhs, ignore_missing?) -> list of strings`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 114 | Happy path | Two identical parameters | Returns empty list | |
| 115 | Happy path | Parameters differ in `type` field | Returns `["type"]` | |
| 116 | Happy path | Parameters differ in `allowedValues` (compared as sets) | Returns `["allowedValues"]` | |
| 117 | Happy path | `ignore_missing=true` and one parameter lacks a field | That field is not reported as a difference | |
| 118 | Happy path | `ignore_missing=false` and one parameter lacks a field | That field is reported as a difference | |

---

## Section 17: Job bundle — submission & asset references

> **Rust crate:** `deadline-job-bundle` · **Module:** `submission`
>
> **Logic under test:** The `AssetReferences` data structure (set-based union, dict
> serialization with path normalization), `split_parameter_args` for extracting
> app-specific parameters (e.g., `deadline:priority`), and `parse_frame_range`.

### `AssetReferences`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 1 | Happy path | Create with all fields populated | All sets contain the provided values | |
| 2 | Happy path | Create with no arguments | All sets are empty | |
| 3 | Happy path | Boolean check when any set is non-empty | Returns true | |
| 4 | Happy path | Boolean check when all sets are empty | Returns false | |
| 5 | Happy path | `union()` of two AssetReferences | Returns new object with union of all sets | |

### `AssetReferences.from_dict(obj)`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 6 | Happy path | Valid dict with all fields | All paths are normalized and stored in sets | |
| 7 | Happy path | Dict with missing optional fields (e.g., no `outputs`) | Missing fields default to empty sets | |
| 8 | Happy path | `obj` is none/null | Returns empty `AssetReferences` | |
| 9 | Boundary values | Paths contain `..` or redundant separators | Paths are normalized | |

### `AssetReferences.to_dict()`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 10 | Happy path | All fields populated | Returns dict with sorted lists under `assetReferences` | |
| 11 | Boundary values | All fields empty | Returns dict with empty lists | |

### `split_parameter_args(parameters, job_bundle_dir, app_name?, supported_app_parameter_names?) -> (dict, dict)`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 12 | Happy path | Parameter with `deadline:priority` name | Extracted as app parameter `{"priority": value}` | |
| 13 | Happy path | Parameter with `deadline:maxFailedTasksCount` | Extracted as app parameter | |
| 14 | Happy path | Regular parameter (no colon prefix) | Added to job_parameters as `{name: {type: value}}` | |
| 15 | Error handling | `deadline:unknownParam` (unsupported app parameter) | Returns error "Unrecognized parameter" | |
| 16 | Happy path | Parameter with `otherApp:something` prefix | Silently dropped (not `deadline:` prefix) | |
| 17 | Happy path | Parameter without `value` key | Skipped entirely | `if "value" in parameter` guard |
| 18 | Happy path | Empty parameters list | Returns `({}, {})` | |
| 19 | Happy path | Custom `app_name` (e.g., `"maya"`) | Parameters with `maya:` prefix are extracted | |
| 20 | Happy path | Parameter type is uppercased (e.g., `"STRING"`) | Lowercased in output: `{"string": value}` | |

### `parse_frame_range(frame_string) -> list of ints`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 21 | Happy path | `"1-10"` | Returns `[1, 2, 3, ..., 10]` | Default step=1 |
| 22 | Happy path | `"1-10:2"` | Returns `[1, 3, 5, 7, 9]` | Step=2 |

---

## Section 18: Job bundle — history directory

> **Rust crate:** `deadline-job-bundle` · **Module:** `submission`
>
> **Logic under test:** Creating a dated, sequentially-numbered directory for saving
> a copy of each submitted job bundle. Submitter and job names are sanitized (non-alnum
> stripped except space/hyphen/underscore), job name truncated to 128 chars.
> See [data_flow.md § Job History Directory](data_flow.md#job-history-directory).

### `create_job_history_bundle_dir(submitter_name, job_name) -> string`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 1 | Happy path | First submission of the day | Creates `YYYY-MM/YYYY-MM-DD-01-SubmitterName-JobName/` directory | |
| 2 | Happy path | Second submission of the day | Creates directory with `-02-` in the name | Sequential numbering |
| 3 | Happy path | Submitter name has special characters `@#$` | Non-alphanumeric chars (except space, hyphen, underscore) are stripped | |
| 4 | Happy path | Job name is longer than 128 characters | Truncated to 128 characters | |
| 5 | Happy path | Job name has special characters | Cleaned same as submitter name | |
| 6 | Happy path | Month directory doesn't exist yet | Created recursively | |
| 7 | Happy path | Month directory already exists | No error; new submission dir created inside | |
| 8 | Boundary values | Existing directories have gaps in numbering (e.g., 01, 03) | New directory uses max+1 (04) | Scans all existing dirs |
| 9 | Boundary values | No existing directories for the date | Starts at 01 | |
| 10 | Config interaction | `settings.job_history_dir` is configured | Uses the configured directory (with `{aws_profile_name}` substituted) | |

---
