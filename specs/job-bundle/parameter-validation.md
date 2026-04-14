# Parameter Validation

## Type System

Four types: `STRING`, `PATH`, `INT`, `FLOAT`. Validated against `VALID_TYPES`.

## Validation Rules (`validate_job_parameter`)

- `name` — required, must be a string, must be non-empty
- `type` — required (when `type_required=true`), must be one of the four valid types
- `default` — cannot be null. Type-specific validation:
  - INT: must be an actual integer (float 3.7 → error, string "19" → ok)
  - FLOAT: accepts both integer and decimal strings
  - STRING/PATH: must be a string
- `description` — optional, must be a string if present
- `allowedValues` — optional, must be an array if present. Each element validated
  against the parameter type
- `minValue`/`maxValue` — INT/FLOAT only
- `minLength`/`maxLength` — STRING only, must be non-negative integers
- `userInterface.control` — must be one of: CHECK_BOX, CHOOSE_DIRECTORY,
  CHOOSE_INPUT_FILE, CHOOSE_OUTPUT_FILE, DROPDOWN_LIST, LINE_EDIT,
  MULTILINE_EDIT, SPIN_BOX, HIDDEN
- `userInterface.fileFilters` — each filter label must be 1-20 characters

## Value Coercion

`validate_and_coerce_value` converts user-provided values to the parameter's type:
- STRING → no coercion needed
- INT → parse string as integer, reject floats
- FLOAT → parse string as float, accept integers
- PATH → no coercion, but resolved relative to bundle or CWD

After coercion, constraints are checked:
- `allowedValues` → set membership test
- `minValue`/`maxValue` → numeric range
- `minLength`/`maxLength` → string length

## PATH Parameter Resolution

PATH parameters with a default but no explicit value and no `allowedValues`
have their default resolved relative to the bundle directory. The resolved
path must be relative and stay within the bundle. User-provided PATH values
are made absolute relative to CWD.

## Queue Environment Merge

`merge_queue_parameters` combines queue environment parameters with job template
parameters:
- Same-name parameters must agree on type (different types → error)
- Different defaults are silently accepted (queue environment wins)
- App-specific parameters (names containing `:` like `deadline:priority`)
  from unknown prefixes are silently dropped — only `deadline:` prefix validated

## Asset Reference Extraction

`extract_asset_references` maps PATH parameter `dataFlow` and `objectType` to
asset reference sets:
- `IN` + `FILE` → `input_filenames`
- `IN` + `DIRECTORY` → `input_directories`
- `OUT` + `DIRECTORY` → `output_directories`
- `INOUT` → both input and output
- `NONE` → `referenced_paths`

## UI Control Inference

If no `userInterface.control` specified:
- STRING → LINE_EDIT
- PATH with objectType=DIRECTORY → CHOOSE_DIRECTORY
- PATH with dataFlow=IN → CHOOSE_INPUT_FILE
- PATH with dataFlow=OUT → CHOOSE_OUTPUT_FILE
- INT/FLOAT → SPIN_BOX
- Any type with `allowedValues` → DROPDOWN_LIST
