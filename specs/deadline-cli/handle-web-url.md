# deadline handle-web-url

Protocol handler for `deadline://` URLs. Bridges the Deadline Cloud web
console to the local CLI — clicking "Download Output" in the browser
opens a `deadline://download-output?...` URL that the OS routes to this
command.

## Modes

| Mode | Trigger | Behavior |
|------|---------|----------|
| URL dispatch | Positional URL argument | Parse URL, validate, delegate to `download_output_impl` |
| Install | `--install` | Register CLI as OS protocol handler |
| Uninstall | `--uninstall` | Remove OS protocol handler registration |

Modes are mutually exclusive. URL + any install flag → error.
`--install` + `--uninstall` → error. No arguments → error.

## URL Dispatch

URL format: `deadline://<command>?<key>=<value>&...`

Currently supported commands: `download-output` only. Unsupported
commands produce: `"Command {name} is not supported through handle-web-url."`

### download-output parameters

| Parameter | Required | Description |
|-----------|----------|-------------|
| `farm-id` | Yes | Farm ID |
| `queue-id` | Yes | Queue ID |
| `job-id` | Yes | Job ID |
| `step-id` | No | Step ID for step-level download |
| `task-id` | No | Task ID for task-level download |
| `profile` | No | AWS profile name |

Query string validation:
- Missing required parameters → error listing missing names
- Duplicate parameters → error naming the duplicate
- Unknown parameters → error listing unknown names
- Dashes in parameter names converted to underscores internally

### Resource ID validation

All resource IDs (excluding `profile`) are validated:
- Standard format: `<resource>-<32 hex chars>` (farm, queue, job, step)
- Task format: `task-<32 hex chars>-<number>` where number is 0 or
  1-10 digits with no leading zeros

### Profile resolution

If the URL includes `profile`, that profile is used. Otherwise,
`get_best_profile_for_farm` selects the best match from
`~/.aws/config` profiles by matching farm and queue IDs against
each profile's configured defaults.

## Install / Uninstall

Platform-specific OS protocol handler registration.

| Platform | Install location (current user) | Install location (all users) |
|----------|-------------------------------|------------------------------|
| Windows | `HKCU\Software\Classes\deadline` | `HKCR\deadline` |
| Linux | `~/.local/share/applications/deadline.desktop` | `/usr/share/applications/deadline.desktop` |
| macOS | Not supported | Not supported |

The registered command is: `<exe> handle-web-url <url> --prompt-when-complete`

## --prompt-when-complete

Prints "Press Enter To Exit" and waits for input before exiting.
Prevents the terminal from flash-closing when launched from a browser.
Runs after the command completes (success or failure).

## Differences from Python

- **List formatting in error messages:** Python uses single quotes
  (`['queue-id', 'job-id']`), Rust uses double quotes
  (`["queue-id", "job-id"]`). This is a language-level display difference
  (`list.__repr__` vs `Vec Debug`), not a behavioral difference. The
  parameter names are identical.
