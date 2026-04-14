# Config — Settings & Profile Resolution

> Part of [AWS Deadline Cloud Client — Behavioral Test Specification](index.md)
>
> This document describes observable behavior through public interfaces.
> Error descriptions use category labels (e.g., "returns error") rather than
> language-specific exception types. The Rust implementation should produce
> equivalent observable outcomes, not necessarily identical internal mechanics.

---

## Section 1: Config — get/set/clear/read/write settings

> **Rust crate:** `deadline-config` · **Module:** `config_file`
>
> **Logic under test:** INI config file read/write with hierarchical section naming,
> mtime-based caching, atomic writes, and environment variable override for file path.
> See [data_flow.md § Configuration File](data_flow.md#configuration-file) for the INI
> format, section naming conventions, and atomic write strategy.

### `get_config_file_path() -> filesystem path`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 1 | Happy path | Call with no env var set | Returns `~/.deadline/config` with tilde expanded to home directory | |
| 2 | Config interaction | `DEADLINE_CONFIG_FILE_PATH` env var is set to `/tmp/my_config` | Returns `/tmp/my_config` | |
| 3 | Config interaction | `DEADLINE_CONFIG_FILE_PATH` env var is set to `~/custom/config` | Returns the path with tilde expanded | |
| 4 | Boundary values | `DEADLINE_CONFIG_FILE_PATH` env var is set to empty string | Falls back to default `~/.deadline/config` | |
| 5 | Config interaction | `DEADLINE_CONFIG_FILE_PATH` env var is set, then unset mid-session | Subsequent calls return the default path | |

### `get_cache_directory() -> string`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 6 | Happy path | Call with default HOME | Returns `~/.deadline/cache` with tilde expanded | |

### `read_config() -> parsed config`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 7 | Happy path | Config file exists with valid INI content | Returns parsed config with sections and keys | |
| 8 | Happy path | Config file does not exist | Returns empty config (no error) | |
| 9 | Happy path | Config file exists but is empty | Returns empty config | |
| 10 | Error handling | Config file contains malformed INI (e.g., no section header) | Returns error indicating malformed config file | |
| 11 | Caching | Call twice without modifying the file | Second call returns the same result without re-reading from disk | Mtime-based cache check |
| 12 | Caching | Call, modify the file on disk, call again | Second call returns updated content reflecting the file change | Mtime change triggers re-read |
| 13 | Caching | Call, delete the file on disk, call again | Returns fresh empty config | |
| 14 | Config interaction | `DEADLINE_CONFIG_FILE_PATH` changes between calls | Reads from the new path | |
| 15 | Boundary values | Config file contains UTF-8 encoded content | Parses correctly | |

### `write_config(config)`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 16 | Happy path | Write config with sections and keys to an existing config path | File is written atomically (temp file + rename); content matches the config state | |
| 17 | Happy path | Write when parent directory does not exist | Parent directories are created recursively and file is written | |
| 18 | Happy path | Write when parent directory already exists | No error; file is written normally | |
| 19 | Cross-platform | On POSIX, written file has `0o600` permissions | File permissions are owner-read-write only | |
| 20 | Cross-platform | On Windows, newly created parent directory gets restricted DACL | DACL allows only current user, Administrators, and SYSTEM with full access | Only applies when directory is first created |
| 21 | Cross-platform | On Windows, existing parent directory permissions are not modified | Existing DACL entries are preserved | |
| 22 | Error handling | Disk is read-only or path is not writable | Returns OS-level permission error | No special handling in code |

### `get_setting(setting_name, config?) -> string`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 23 | Happy path | Get `defaults.aws_profile_name` with no config file | Returns default `"(default)"` | |
| 24 | Happy path | Get `defaults.aws_profile_name` after setting it to `"MyProfile"` | Returns `"MyProfile"` | |
| 25 | Happy path | Get `defaults.farm_id` (profile-scoped) with default profile | Returns `""` (empty default) | Stored in `[profile-(default) defaults]` section |
| 26 | Happy path | Get `defaults.farm_id` after setting profile to `"MyProfile"` and farm to `"farm-123"` | Returns `"farm-123"` | Stored in `[profile-MyProfile defaults]` section |
| 27 | Happy path | Get `defaults.queue_id` (farm-scoped) with profile and farm set | Returns the value from the correct hierarchical section | Section: `[profile-X farm-Y defaults]` |
| 28 | Happy path | Get `defaults.job_id` (queue-scoped) with full hierarchy set | Returns the value from the correct 3-level section | Section: `[profile-X farm-Y queue-Z defaults]` |
| 29 | Happy path | Get `settings.storage_profile_id` (farm-scoped) | Returns value from `[profile-X farm-Y settings]` section | |
| 30 | Happy path | Get `settings.job_history_dir` with default profile | Returns default with `{aws_profile_name}` substituted to `"(default)"` | Template substitution in default value |
| 31 | Happy path | Get `settings.job_history_dir` with profile set to `"MyProfile"` | Returns default with `{aws_profile_name}` substituted to `"MyProfile"` | |
| 32 | Config interaction | Pass explicit config override instead of None | Uses the provided config instead of reading from disk | |
| 33 | Config interaction | Setting exists in config file | Returns the configured value, not the default | |
| 34 | Config interaction | Setting does not exist in config file | Returns the default value | |
| 35 | Hierarchy | Set farm_id under profile A, switch to profile B, get farm_id | Returns default `""` because profile B has no farm_id set | Hierarchical scoping isolates values |
| 36 | Hierarchy | Set queue_id under farm X, switch to farm Y, get queue_id | Returns default `""` because farm Y has no queue_id set | |
| 37 | Missing/invalid args | Setting name has no dot (e.g., `"bad_name"`) | Returns error containing "is not valid" | |
| 38 | Missing/invalid args | Setting name has valid format but doesn't exist (e.g., `"settings.nonexistent"`) | Returns error containing "has no setting" | |
| 39 | Missing/invalid args | Section name is misspelled (e.g., `"setitngs.log_level"`) | Returns error containing "has no setting" | |

### `get_setting_default(setting_name, config?) -> string`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 40 | Happy path | Get default for `defaults.aws_profile_name` | Returns `"(default)"` | |
| 41 | Happy path | Get default for `settings.auto_accept` | Returns `"false"` | |
| 42 | Happy path | Get default for `settings.conflict_resolution` | Returns `"NOT_SELECTED"` | |
| 43 | Happy path | Get default for `settings.log_level` | Returns `"WARNING"` | |
| 44 | Happy path | Get default for `settings.job_history_dir` | Returns path with `{aws_profile_name}` substituted | |
| 45 | Missing/invalid args | Nonexistent setting name | Returns error (unknown setting) | |

### `set_setting(setting_name, value, config?)`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 46 | Happy path | Set `defaults.aws_profile_name` to `"NewProfile"` without explicit config | Value is persisted to disk; subsequent `get_setting` returns `"NewProfile"` | Writes to disk when no config override provided |
| 47 | Happy path | Set `defaults.farm_id` to `"farm-abc"` with profile already set | Value stored in correct hierarchical section | |
| 48 | Config interaction | Set with explicit config override | Value is set in the config object but NOT written to disk | |
| 49 | Config interaction | Set a setting that creates a new section | Section is created in the config | |
| 50 | Missing/invalid args | Setting name has no dot | Returns error containing "is not valid" | |
| 51 | Missing/invalid args | Setting name doesn't exist in known settings | Returns error containing "has no setting" | |
| 52 | Missing/invalid args | Section name is misspelled | Returns error containing "has no setting" | |

### `clear_setting(setting_name, config?)`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 53 | Happy path | Clear `defaults.aws_profile_name` after setting it | Value reverts to default `"(default)"` | ⚠️ Writes the default value back rather than removing the key. See data_flow.md observation #6. |
| 54 | Happy path | Clear a farm-scoped setting | Value reverts to its default | |
| 55 | Config interaction | Clear with explicit config override | Sets default in config without writing to disk | |
| 56 | Missing/invalid args | Setting name has no dot | Returns error containing "is not valid" | |
| 57 | Missing/invalid args | Nonexistent setting name | Returns error containing "has no setting" | |

### `str2bool(value: string) -> bool`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 58 | Happy path | `"true"` | Returns true | |
| 59 | Happy path | `"false"` | Returns false | |
| 60 | Happy path | `"on"` / `"off"` | Returns true / false | |
| 61 | Happy path | `"yes"` / `"no"` | Returns true / false | |
| 62 | Happy path | `"1"` / `"0"` | Returns true / false | |
| 63 | Happy path | `"TrUe"` (mixed case) | Returns true (case-insensitive) | |
| 64 | Happy path | `"FaLsE"` (mixed case) | Returns false (case-insensitive) | |
| 65 | Error handling | `"not_boolean"` | Returns error (invalid boolean string) | |
| 66 | Boundary values | `""` (empty string) | Returns error (invalid boolean string) | |
| 67 | Missing/invalid args | `"2"` or `"maybe"` | Returns error (invalid boolean string) | |

### Settings hierarchy (integration behavior)

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 68 | Hierarchy | Set storage_profile_id, queue_id, farm_id under default profile, then switch profile | All child settings return defaults under the new profile | |
| 69 | Hierarchy | Switch back to original profile | Previously set child settings are restored | |
| 70 | Hierarchy | Clear farm_id (revert to default) | queue_id and other farm-scoped settings revert to their values under the default farm | |

---

## Section 2: Config — profile resolution & best profile

> **Rust crate:** `deadline-config` · **Module:** `config_file`
>
> **Logic under test:** Given a farm ID (and optional queue ID), find the best-matching
> AWS profile by checking: (1) default profile's farm, (2) any profile matching both
> farm+queue, (3) any profile matching farm only, (4) fallback to default profile.

### `get_best_profile_for_farm(farm_id, queue_id?) -> string`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 1 | Happy path | Default profile's farm matches the requested farm_id | Returns the default profile name | Priority 1: default profile match |
| 2 | Happy path | Default profile doesn't match, but another profile matches both farm_id and queue_id | Returns the matching profile name | Priority 2: exact farm+queue match |
| 3 | Happy path | Default profile doesn't match, another profile matches farm_id only | Returns the first farm-matching profile | Priority 3: farm-only match |
| 4 | Happy path | No profile matches the farm_id | Returns the default profile name | Fallback behavior |
| 5 | Happy path | Multiple profiles match the farm_id, one also matches queue_id | Returns the profile matching both farm and queue | Exact match takes priority |
| 6 | Happy path | Multiple profiles match the farm_id, none match queue_id | Returns the first profile that matched the farm | |
| 7 | Happy path | Default profile matches farm_id, queue_id doesn't match | Returns the default profile (farm match is sufficient for priority 1) | Priority 1 doesn't check queue_id |
| 8 | Cross-resource references | farm_id is `"farm-missing"` (no profile has it) | Returns the default profile | |
| 9 | Cross-resource references | farm_id matches but queue_id is `"queue-missing"` | Returns the first farm-matching profile | Falls through to priority 3 |
| 10 | Boundary values | queue_id is not provided | Only matches on farm_id; returns first farm match or default | queue_id check is skipped |
| 11 | Boundary values | queue_id is empty string `""` | Treated as falsy; same behavior as not provided | |
| 12 | Config interaction | Calling get_best_profile_for_farm does not modify the default profile setting | After the call, `defaults.aws_profile_name` is unchanged | |
| 13 | Config interaction | Profile has farm_id set but empty queue_id | Matches on farm_id; does not match on queue_id | |
| 14 | Config interaction | Profile has empty farm_id | Does not match any farm_id query | |
| 15 | Happy path | Default profile matches farm-3, Profile4 also matches farm-3 | Returns default profile (priority 1 wins) | Default profile checked first |
| 16 | Happy path | Default profile is Profile5 (farm-3), query farm-3 with queue-4 | Returns Profile4 (exact farm+queue match beats default farm-only) | ⚠️ Actually returns Profile5 because priority 1 (default profile farm match) is checked first and doesn't check queue_id. Verify intent. |
| 17 | Happy path | Two profiles share the same farm, different queues, query with first queue | Returns the profile with the matching queue | |
| 18 | Boundary values | No AWS profiles exist in credential configuration | Returns the default profile (iteration over empty set) | |

---
