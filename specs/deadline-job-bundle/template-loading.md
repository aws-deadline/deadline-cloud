# Template Loading

## Bundle Discovery

A job bundle is a directory containing:
- `template.yaml` or `template.json` (exactly one, not both)
- Optional `parameter_values.yaml` or `parameter_values.json`
- Referenced asset files

`read_yaml_or_json(bundle_dir, filename, required)` handles detection:
- Both `.json` and `.yaml` exist → error with specific message
- Neither exists and `required=true` → error
- Neither exists and `required=false` → returns empty (caller gets `None`)
- One exists → reads content, returns `(content, "JSON"|"YAML")`

`read_yaml_or_json_object` wraps this to parse into `serde_json::Value`.

## Symlink Containment

`validate_directory_symlink_containment(job_bundle_dir)`:

1. `fs::canonicalize` the bundle directory to get the resolved root
2. Recursively walk all entries in the directory
3. For each entry: `fs::canonicalize` → check `resolved.starts_with(resolved_root)`
4. If any path resolves outside the root → error with detailed message showing
   the path in the bundle, the resolved path, and the resolved root
5. Symlinked directories are not recursed into (only their target is checked)

This prevents path traversal attacks in user-provided bundles.

## Template Validation

Must have `specificationVersion: jobtemplate-2023-09`. Other versions rejected.

## YAML Serialization

`deadline_yaml_dump` uses `serde_yaml::to_string`. Used when saving parameter
values or job history snapshots back to disk.
