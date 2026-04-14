# config Commands

## Subcommands

| Subcommand | Status | Description |
|------------|--------|-------------|
| `config show` | ✅ | Display all settings with values, defaults, and descriptions |
| `config get <SETTING>` | ✅ | Print current value of a single setting |
| `config set <SETTING> <VALUE>` | ✅ | Persist a value to the config file |
| `config clear <SETTING>` | ✅ | Write the default value back (does not remove key) |

Note: `config gui` exists in the Python CLI but is not yet implemented in the
Rust CLI. It will spawn a Python process with the GUI config dialog when added.

## `config show`

Options: `--output verbose|json` (default verbose).

**Verbose output:**
```
AWS Deadline Cloud configuration file:
   /home/user/.deadline/config

defaults.aws_profile_name: (default) (default)
   The AWS profile name to use by default. ...

defaults.farm_id:  (default)
   The Farm ID to use by default.
```

Each setting shows: name, current value, "(default)" suffix if value equals
the default, and description wrapped to 77 characters (via `textwrap`).

**JSON output:** Flat object with `settings.config_file_path` and all 18
setting key/value pairs. Uses `json_with_spaces()` for Python-compatible
formatting (spaces after `:` and `,`).

## `config get`

Reads from disk via `config_file::get_setting()` (convenience wrapper).
Prints the value and nothing else — suitable for scripting.

## `config set`

Writes to disk via `config_file::set_setting()`. Atomic write (temp file + rename).

## `config clear`

Writes the default value back via `config_file::clear_setting()`. Does not
remove the key — preserves its presence as a signal that the setting has been
touched.

## Error Handling

All four commands propagate `ConfigError` which converts to `CliError::Config`.
Invalid setting names produce specific messages:
- No dot in name → "The setting name 'X' is not valid."
- Unknown name → "AWS Deadline Cloud configuration has no setting named 'X'."
