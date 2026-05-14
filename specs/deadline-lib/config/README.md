# config Module

Manages the `~/.deadline/config` INI file — reading, writing, and resolving
settings through a hierarchical section naming scheme.

## What It Does

- Reads and writes the Deadline Cloud config file (INI format)
- Resolves settings through a profile → farm → queue hierarchy
- Provides a two-tier API: config-explicit (takes `&IniConfig`) and
  convenience wrappers (read/write disk)
- Shared format with the Python CLI, GUI, and DCC plugins

## Consumers

All modules in `deadline-lib` plus `deadline-cli` and `deadline-python-bindings`.

## Document Index

| Document | Description |
|----------|-------------|
| [architecture.md](architecture.md) | Module layout, hierarchical resolution, settings table, public API, design decisions |

## Status

Fully implemented. No known gaps.

## Gotchas

- Section names can contain spaces (required by the hierarchical scheme).
- `"(default)"`, `"default"`, and `""` all map to the default credential
  chain (no named profile).
- The config file format is shared with the Python CLI, GUI, and DCC
  plugins. Changes to section naming or key semantics are breaking changes
  across the entire Deadline Cloud client ecosystem.
- Keys are case-insensitive (lowercased on parse/get/set), matching
  Python's `ConfigParser` behavior. Section names remain case-sensitive.
