# deadline-config

Manages the `~/.deadline/config` INI file — reading, writing, and resolving
settings through a hierarchical section naming scheme scoped to the active
AWS profile, farm, and queue.

Rust equivalent of Python's `deadline.client.config.config_file`.

## Modules

### `ini` — INI parser/writer

`IniConfig` parses and serializes standard INI format: `[section]` headers,
`key = value` pairs, `#`/`;` comments. Section names may contain spaces
(required for the hierarchical naming scheme). Output is sorted by section
then key for deterministic serialization.

### `settings` — Setting definitions

Static table of all 18 Deadline Cloud settings. Each `SettingDef` has:

- `default` — Default value. May contain `{aws_profile_name}` for substitution.
- `depend` — Parent setting that controls section scoping. `None` = top-level.
- `section_format` — How this setting's value formats into child section names.
- `description` — Human-readable, used by `deadline config show`.

The dependency chain defines hierarchical section naming:

```
defaults.aws_profile_name  →  "profile-{}"
  └─ defaults.farm_id      →  "{}"
       └─ defaults.queue_id →  "{}"
```

Example: `defaults.job_id` with profile `myprofile`, farm `farm-abc`,
queue `queue-123` resolves to section `[profile-myprofile farm-abc queue-123 defaults]`.

### `config_file` — Core logic

#### File location

`get_config_file_path()` checks `DEADLINE_CONFIG_FILE_PATH` env var, falls
back to `~/.deadline/config`. Tilde expansion applied to both.

#### Read / Write

Two tiers — path-explicit (primary) and convenience wrappers:

| Function | Disk I/O |
|----------|----------|
| `read_config_from(path)` | Reads specific file. Empty config if missing. |
| `read_config()` | Reads from default path. |
| `write_config_to(config, path)` | Atomic write (temp + rename). Creates dirs. 0o600 on POSIX. |
| `write_config(config)` | Writes to default path. |

No global cache. The CLI reads once per invocation and threads the `IniConfig`
through all calls.

#### Setting operations

Two tiers — config-explicit (primary) and convenience wrappers:

| Function | Takes config? | Disk I/O |
|----------|---------------|----------|
| `get_setting_with_config(name, &config)` | Yes | None |
| `get_setting(name)` | No (reads disk) | Read |
| `set_setting_in_config(name, value, &mut config)` | Yes | None |
| `set_setting(name, value)` | No | Read + Write |
| `clear_setting_in_config(name, &mut config)` | Yes | None |
| `clear_setting(name)` | No | Read + Write |
| `get_setting_default_with_config(name, &config)` | Yes | None |
| `get_setting_default(name)` | No (reads disk) | Read |

`clear_setting` writes the default value back rather than removing the key
(matches Python behavior — see data_flow.md observation #6).

#### Validation

Setting names must contain a dot. Unknown names produce errors distinguishing
"not valid" (no dot) from "has no setting" (valid format, unknown name).

#### str2bool

Accepts case-insensitive: `yes/no`, `on/off`, `true/false`, `1/0`.
Everything else is an error.

#### Helpers

- `setting_names()` — Iterator over all setting names in definition order.
- `setting_description(name)` — Human-readable description for a setting.

## Dependencies

| Crate | Purpose |
|-------|---------|
| `thiserror` | Error type derivation |
