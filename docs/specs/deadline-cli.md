# deadline-cli

Binary crate. Clap-based argument parsing, subcommand dispatch, output
formatting. Contains no business logic — delegates to library crates.

## Status: Stub

Currently prints a placeholder message. Phase 2 will add:

- `deadline --version` / `deadline --help`
- `deadline config show` (verbose and JSON)
- `deadline config get <setting>`
- `deadline config set <setting> <value>`
- `deadline config clear <setting>`

## Design

Each subcommand group lives in `src/commands/<group>.rs`. The main entry point
parses args with clap, loads config once, applies CLI flag overrides in memory,
then dispatches to the appropriate command handler.

## Dependencies

| Crate | Purpose |
|-------|---------|
| `clap` | Argument parsing |
| `serde_json` | JSON output for `--output json` |
| `deadline-config` | Config file operations |
| `deadline-client` | AWS API calls (future) |
| `deadline-models` | Shared types |
| `deadline-common` | Utilities |
| `deadline-job-bundle` | Bundle submission (future) |
| `deadline-job-attachments` | Attachment handling (future) |
