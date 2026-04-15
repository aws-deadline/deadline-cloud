# deadline-config Architecture

## Crate Position in the Workspace

```
deadline-cli ──► deadline-config    ← this crate
deadline-api ──► deadline-config
deadline-gui-ffi ──► deadline-config
deadline-job-attachments ──► (indirect via deadline-api)
```

The foundational crate. Provides config file I/O and setting resolution.
No AWS SDK dependencies — pure filesystem and string operations.

## Module Layout

```
src/
├── lib.rs          # Re-exports: config_file, ini, settings
├── ini.rs          # IniConfig struct (BTreeMap-based INI parser/writer)
├── settings.rs     # Static SETTINGS table (18 setting definitions), find_setting()
└── config_file.rs  # Read/write/get/set API, hierarchical section resolution,
                    #   profile resolution, str2bool, setting iteration
```

## Public API Surface

### ini.rs

- `IniConfig` — `BTreeMap<String, BTreeMap<String, String>>` wrapper. Methods:
  `new()`, `parse(text)`, `get(section, key)`, `set(section, key, value)`,
  `remove(section, key)`, `sections()`, `keys(section)`. Implements `Display`
  for deterministic sorted INI output.
- `IniParseError` — line number + message.

### settings.rs

- `SettingDef` — `{ default, depend, section_format, description }`. Static
  metadata for each setting.
- `SETTINGS: &[(&str, SettingDef)]` — All 18 settings in definition order.
- `find_setting(name) -> Option<&SettingDef>` — Lookup by name.

### config_file.rs

Primary API (config-explicit, no disk I/O):
- `get_setting_with_config(name, &IniConfig) -> Result<String>`
- `set_setting_in_config(name, value, &mut IniConfig) -> Result<()>`
- `clear_setting_in_config(name, &mut IniConfig) -> Result<()>`
- `get_setting_default_with_config(name, &IniConfig) -> Result<String>`

Convenience wrappers (read/write disk):
- `get_setting(name)`, `set_setting(name, value)`, `clear_setting(name)`
- `read_config() -> Result<IniConfig>`, `write_config(&IniConfig) -> Result<()>`
- `read_config_from(path)`, `write_config_to(&IniConfig, path)`

Utilities:
- `get_config_file_path() -> PathBuf` — env var `DEADLINE_CONFIG_FILE_PATH` or `~/.deadline/config`
- `get_cache_directory() -> PathBuf` — `~/.deadline/cache`
- `str2bool(value) -> Result<bool>` — case-insensitive yes/no/on/off/true/false/1/0
- `get_best_profile_for_farm(&IniConfig, &[&str], farm_id, queue_id) -> String`
- `setting_names() -> impl Iterator<Item = &str>`, `setting_description(name) -> &str`

## Hierarchical Section Resolution

Settings are scoped by AWS profile → farm → queue. The dependency chain:

```
defaults.aws_profile_name  (section_format: "profile-{}")
  └── defaults.farm_id     (section_format: "{}")
        ├── defaults.queue_id  (section_format: "{}")
        │     └── defaults.job_id
        └── settings.storage_profile_id
```

### All 18 Settings

| Setting | Default | Depends On | Description |
|---------|---------|------------|-------------|
| `deadline-cloud-monitor.path` | `""` | — | Filesystem path to DCM binary |
| `defaults.aws_profile_name` | `"(default)"` | — | AWS profile name |
| `settings.job_history_dir` | `~/.deadline/job_history/{aws_profile_name}` | aws_profile_name | Job submission history directory |
| `defaults.farm_id` | `""` | aws_profile_name | Default Farm ID |
| `settings.storage_profile_id` | `""` | farm_id | Storage profile for this workstation |
| `defaults.queue_id` | `""` | farm_id | Default Queue ID |
| `defaults.job_id` | `""` | queue_id | Default Job ID (updated on submission) |
| `settings.auto_accept` | `"false"` | — | Skip confirmation prompts |
| `settings.conflict_resolution` | `"NOT_SELECTED"` | — | Download conflict handling |
| `settings.log_level` | `"WARNING"` | — | CLI/GUI logging level |
| `telemetry.opt_out` | `"false"` | — | Disable telemetry |
| `telemetry.identifier` | `""` | — | Random UUID for telemetry correlation |
| `defaults.job_attachments_file_system` | `"COPIED"` | farm_id | COPIED or VIRTUAL |
| `settings.s3_max_pool_connections` | `"50"` | — | S3 connection pool size |
| `settings.small_file_threshold_multiplier` | `"20"` | — | Small vs large file threshold |
| `settings.known_asset_paths` | `""` | — | Paths that skip upload warnings |
| `settings.locale` | `""` | — | UI locale override |
| `settings.force_s3_check` | `"false"` | — | Always verify files in S3 via HEAD |

Example: with profile `"MyProfile"` and farm `"farm-abc"`, the setting
`defaults.queue_id` resolves to INI section `"profile-MyProfile farm-abc defaults"`,
key `"queue_id"`.

`get_section_prefixes()` walks the dependency chain recursively, building the
section name prefix list. `full_section_name()` joins prefixes with the setting's
own section part.

## Key Design Decisions

**Two-tier API.** Every operation exists in config-explicit form (takes `&IniConfig`)
and convenience form (reads/writes disk). The CLI uses the config-explicit form
exclusively — reads once at startup, threads through all calls. Convenience wrappers
exist for simple scripts and the GUI FFI.

**BTreeMap for deterministic output.** `IniConfig` uses `BTreeMap` (not `HashMap`)
so sections and keys are sorted alphabetically when written. This produces clean
diffs and prevents config file churn on write.

**Atomic writes.** `write_config_to` writes to a temp file then renames. On POSIX,
sets 0o600 permissions. This prevents partial writes from corrupting the config.

**Clear writes the default, doesn't remove the key.** Preserves the key's presence
as a signal that the setting has been touched.

**Profile resolution is pure.** `get_best_profile_for_farm` works on a cloned config
and takes the profile list as a parameter. No AWS SDK dependency — the caller provides
the profile list from whatever source they have.
