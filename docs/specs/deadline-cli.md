# deadline-cli

Binary crate. Clap-based argument parsing, subcommand dispatch, output
formatting. Contains no business logic — delegates to library crates.

## Root Command

The `deadline` binary is the single entry point. It accepts global options
before any subcommand:

- `--version` — prints `deadline <version>` and exits 0
- `-h` / `--help` — prints help text listing subcommands and global options, exits 0
- `--log-level <LEVEL>` — sets logging verbosity (ERROR, WARNING, INFO, DEBUG).
  If omitted, reads `settings.log_level` from config. If the config value is
  invalid, falls back to WARNING.

The help text includes a short description and a "common workflows" section
showing typical command sequences.

Both `-h` and `--help` are accepted (matching the Python CLI's behavior).

## Subcommands

Each subcommand group lives in `src/commands/<group>.rs`.

### `deadline config`

Manages the Deadline Cloud configuration file. All subcommands operate on the
config file at `DEADLINE_CONFIG_FILE_PATH` (or `~/.deadline/config` by default).

- `deadline config show` — prints every known setting with its current value,
  whether it's the default, and a short description. With `--output json`,
  prints a JSON object instead.
- `deadline config get <setting>` — prints the current value of a single setting.
  If not explicitly set, prints the default.
- `deadline config set <setting> <value>` — persists a value to the config file.
- `deadline config clear <setting>` — reverts a setting to its default by writing
  the default value back to the config file (does not remove the key).

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
