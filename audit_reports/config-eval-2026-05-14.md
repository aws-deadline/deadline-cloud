# config Evaluation Report

**Date:** 2026-05-14
**Module:** `config`
**Files reviewed:** `src/config/config_file.rs`, `src/config/ini.rs`, `src/config/settings.rs`, `src/config/path_normalization.rs`

## Summary
INI config file management with hierarchical section naming, atomic writes, and environment variable overrides. Clean implementation using `IndexMap` for order preservation. The hierarchical section resolution is elegant. Grade: **A**.

## Findings

### Critical (must fix)
- None identified.

### Important (should fix)

1. **`write_config_to` temp file naming uses PID only** (`config_file.rs:~130`): If two processes with the same PID (reuse after crash) write simultaneously, they'd conflict. Extremely unlikely but could use `tempfile` crate for safety. Low priority.

2. **`expand_tilde` only handles `~/` prefix**: Doesn't handle `~user/` syntax. This matches Python's behavior and is intentional, but worth documenting.

### Minor (nice to have)

3. **`IniConfig::parse` doesn't preserve comments**: Comments are stripped during parsing and lost on write-back. This matches Python's `ConfigParser` behavior but means user comments in `~/.deadline/config` are lost on any programmatic write. Document this limitation.

4. **`str2bool` accepts many variants**: `"true"`, `"yes"`, `"1"`, `"on"` all map to true. This is correct Python parity but could be simplified to just `"true"`/`"false"` for a Rust-native API. Low priority since it's for config file compatibility.

5. **`get_section_prefixes` recursion**: The recursive dependency resolution is clean but could stack overflow with circular dependencies in the settings definition. Since settings are statically defined, this can't happen in practice.

## Test Coverage Assessment

| Function/Area | Happy path | Error cases | Edge cases |
|---|---|---|---|
| `IniConfig::parse` | ✅ (inline) | ✅ | ✅ |
| `IniConfig::get/set` | ✅ (inline) | ✅ | ✅ |
| `read_config_from` | ✅ (inline) | ✅ | ✅ |
| `write_config_to` | ✅ (inline) | ❌ | ❌ |
| `get_setting` (hierarchical) | ✅ (inline) | ✅ | ✅ |
| `set_setting` | ✅ (inline) | ✅ | ❌ |
| `normalize_path_for_config` | ✅ (inline) | ❌ | ❌ |
| `str2bool` | ✅ (inline) | ✅ | ✅ |

Excellent inline test coverage. This is the best-tested module in the library.

## Recommended Changes

1. [S] Document that comments are not preserved on write-back
2. [S] Document `expand_tilde` limitation (no `~user/` support)
3. [S] Consider using `tempfile::NamedTempFile` in `write_config_to` for robustness
