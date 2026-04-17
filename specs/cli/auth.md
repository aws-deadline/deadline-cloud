# auth Commands

## Subcommands

| Subcommand | Status | Description |
|------------|--------|-------------|
| `auth login` | ✅ | Log in via Deadline Cloud Monitor |
| `auth logout` | ✅ | Log out of Deadline Cloud Monitor |
| `auth status` | ✅ | Check authentication status |

## `auth login`

Only works for DCM (Deadline Cloud Monitor) profiles. Non-DCM profiles
cannot use this command.

Flow:
1. Reads the current profile name via `session::display_profile_name`
2. Prints "Logging into AWS Profile 'X' for AWS Deadline Cloud"
3. Calls `auth::login()` with an `on_pending` callback that prints
   "Opening Deadline Cloud monitor. Please log in and then return here."
4. `login()` spawns the DCM process and polls auth status in a loop
5. On success: prints "Successfully logged in: {message}"
6. On failure: returns `CliError::Operation` with the error message
   (e.g., DCM not installed, login timed out, user canceled)

## `auth logout`

Only works for DCM profiles. Calls `auth::logout()`. On success prints
"Successfully logged out of all Deadline Cloud monitor AWS profiles".
Invalidates the session cache so subsequent API calls re-resolve credentials.

## `auth status`

Options: `--profile`, `--output verbose|json` (default verbose).
The `--output` comparison is case-insensitive (`JSON`, `Json`, `json` all work).

Determines four pieces of information:
- `profile_name` — display name of the active AWS profile
- `source` — `AwsCredentialsSource`: NOT_VALID, HOST_PROVIDED, or DEADLINE_CLOUD_MONITOR_LOGIN
  - NOT_VALID: the named profile doesn't exist in `~/.aws/config`
  - HOST_PROVIDED: profile exists but has no `monitor_id` key
  - DEADLINE_CLOUD_MONITOR_LOGIN: profile has `monitor_id` in its config section
  - For the default profile (`(default)` or empty), checks the `[default]`
    section in `~/.aws/config` for `monitor_id`. This matches Python's
    `ConfigParser.get_scoped_config()` which always checks the default section.
- `status` — `AwsAuthenticationStatus`: CONFIGURATION_ERROR, AUTHENTICATED, or NEEDS_LOGIN
  - Determined by calling STS `GetCallerIdentity`
- `api_availability` — boolean, whether the Deadline API endpoint is reachable
  - Determined by `auth::check_deadline_api_available()` (lightweight API probe)

**Verbose output:**
```
   Profile Name: my-profile
         Source: DEADLINE_CLOUD_MONITOR_LOGIN
         Status: AUTHENTICATED
API Availability: True
```

Right-aligned labels at 17 characters width.

**JSON output:** Compact single-line JSON (not pretty-printed) with all four
fields. Uses `serde_json::to_string` (not `to_string_pretty`).
