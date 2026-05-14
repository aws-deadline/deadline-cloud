# config Module Architecture

## Module Position

```
deadline-lib
├── config    ← this module
├── api       ──► config
├── bundle    ──► config
└── attachments ──► config
```

The foundational module. Provides config file I/O and setting resolution.
No AWS SDK dependencies — pure filesystem and string operations.

## Module Layout

The crate has three modules: an INI parser/writer, a static table of
setting definitions, and the config file API that ties them together.
The config file API provides two tiers — a primary API that takes an
`IniConfig` reference (no disk I/O), and convenience wrappers that
read/write disk on each call. The CLI uses the primary API exclusively,
reading config once at startup and threading it through all calls.

```
src/
├── lib.rs          # Re-exports all public modules
├── ini.rs          # INI parser/writer (BTreeMap-based, deterministic output)
├── settings.rs     # Static table of all 18 setting definitions
└── config_file.rs  # Get/set/clear API, hierarchical section resolution, profile resolution
```

## Hierarchical Section Resolution

Settings are scoped by AWS profile → farm → queue. Each setting has a
dependency chain that determines which INI section it resolves to. For
example, with profile `"MyProfile"` and farm `"farm-abc"`, the setting
`defaults.queue_id` resolves to INI section
`"profile-MyProfile farm-abc defaults"`, key `"queue_id"`.

The dependency chain:

```
defaults.aws_profile_name  (section_format: "profile-{}")
  ├── settings.job_history_dir
  └── defaults.farm_id     (section_format: "{}")
        ├── defaults.queue_id  (section_format: "{}")
        │     └── defaults.job_id
        ├── settings.storage_profile_id
        └── defaults.job_attachments_file_system
```

### All 21 Settings

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
| `settings.allow_bundle_hooks` | `"false"` | — | Allow execution of hooks from job bundle hooks files |
| `settings.allow_environment_hooks` | `"false"` | — | Allow execution of hooks from DEADLINE_HOOKS_DIR |
| `settings.submitter_update_notification` | `"true"` | — | Enable DCC submitter update notification checks |

## Key Design Decisions

**Two-tier API.** Every operation exists in config-explicit form (takes
`&IniConfig`, no disk I/O) and convenience form (reads/writes disk).
The CLI uses the config-explicit form exclusively. Convenience wrappers
exist for simple scripts and the GUI FFI.

**BTreeMap for deterministic output.** The INI writer uses sorted maps
so sections and keys are alphabetically ordered. This produces clean
diffs and prevents config file churn on write.

**Keys are case-insensitive.** Keys are lowercased on parse, set, and
get — matching Python's `ConfigParser` default behavior. Section names
remain case-sensitive. This ensures config files written by Python are
read correctly.

**Atomic writes.** Config writes go to a temp file then rename. On
POSIX, permissions are set to 0o600. This prevents partial writes from
corrupting the config.

**Clear writes the default, doesn't remove the key.** Preserves the
key's presence as a signal that the setting has been touched.

**Profile resolution is pure.** `get_best_profile_for_farm` works on a
cloned config and takes the profile list as a parameter. No AWS SDK
dependency — the caller provides the profile list.

## Public API Surface

### Reading and writing settings

Primary API — takes `&IniConfig`, no disk I/O:

```rust
let config = config_file::read_config()?;
let farm_id = config_file::get_setting("defaults.farm_id", &config)?;

let mut config = config_file::read_config()?;
config_file::set_setting("defaults.farm_id", "farm-abc", &mut config)?;
config_file::write_config(&config)?;
```

Convenience wrappers — read/write disk on each call:

```rust
let farm_id = config_file::get_setting_from_disk("defaults.farm_id")?;
```

### Core types

| Type | Purpose |
|------|---------|
| `IniConfig` | In-memory representation of the config file (BTreeMap-based, deterministic output) |
| `ConfigError` | Error enum for parse failures, unknown settings, invalid values |
| `SettingDef` | Static metadata for a setting: default value, dependency, section format, description |

### Utilities

| Function | Purpose |
|----------|---------|
| `get_config_file_path()` | Returns config path — respects `DEADLINE_CONFIG_FILE_PATH` env var, defaults to `~/.deadline/config` |
| `get_cache_directory()` | Returns `~/.deadline/cache` |
| `str2bool(value)` | Parses boolean-like strings: yes/no, on/off, true/false, 1/0 (case-insensitive) |
| `get_best_profile_for_farm()` | Finds the best AWS profile for a given farm/queue combination |
