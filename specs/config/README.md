# deadline-config Crate Specifications

Manages the `~/.deadline/config` INI file — reading, writing, and resolving
settings through a hierarchical section naming scheme. The foundational crate
that nearly every other crate depends on. No AWS dependencies.

Consumers: `deadline-cli`, `deadline-client`, `deadline-gui-ffi`,
`deadline-job-attachments`, `deadline-common`.

## Document Index

| Document | Description |
|----------|-------------|
| [architecture.md](architecture.md) | Module layout, public API surface, hierarchical section resolution, design decisions |

## Status

Fully implemented. No known gaps.

## Gotchas & Constraints

- Section names can contain spaces (required by the hierarchical scheme).
  The INI parser must handle this — standard INI libraries that reject
  spaces in section names won't work.

- `"(default)"`, `"default"`, and `""` all map to the default credential
  chain (no named profile). This normalization happens in profile
  resolution and affects section name construction.

- The config file format is shared with the Python CLI, GUI, and DCC
  plugins. Changes to section naming or key semantics are breaking changes
  across the entire Deadline Cloud client ecosystem.

## Relationship to the Python Library

The Rust crate mirrors the Python `deadline.client.config` module's public
API. Key differences:
- Python uses `configparser.ConfigParser`; Rust uses a custom `IniConfig`
  (BTreeMap-based) because configparser lowercases keys and doesn't handle
  spaces in section names correctly.
- Python has an mtime-based cache (`_CachedConfig`); Rust has no cache —
  the CLI reads once and threads through.
- Python's `get_setting` reads from disk on every call; Rust's convenience
  wrapper does the same, but the primary API takes `&IniConfig`.
