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

- `deadline config show` — verbose mode (default) prints the config file path,
  then for each setting: `name: value (default)` (the suffix only when value
  equals the default), followed by the description indented with 3 spaces.
  With `--output json`, prints a JSON object containing
  `settings.config_file_path` and all setting name/value pairs.
- `deadline config get <setting>` — prints the current value of a single setting.
  If not explicitly set, prints the default.
- `deadline config set <setting> <value>` — persists a value to the config file.
- `deadline config clear <setting>` — reverts a setting to its default by writing
  the default value back to the config file (does not remove the key).
- `deadline config gui` — spawns a Python process that loads the GUI widget
  package and `deadline-gui-ffi` shared library, then shows the config dialog.

### `deadline bundle gui-submit`

Spawns a Python process that loads the GUI widget package and
`deadline-gui-ffi` shared library, then shows the job submission dialog.

## Dependencies

| Crate | Purpose |
|-------|---------|
| `clap` | Argument parsing |
| `serde_json` | JSON output for `--output json` |
| `deadline-config` | Config file operations |
| `deadline-client` | AWS API calls |
| `deadline-models` | Shared types |
| `deadline-common` | Utilities |
| `deadline-job-bundle` | Bundle submission |
| `deadline-job-attachments` | Attachment handling |
