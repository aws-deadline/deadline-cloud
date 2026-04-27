# Config and Auth Commands

Settings management and authentication commands.
## auth Commands

| Subcommand | Status | Description |
|------------|--------|-------------|
| `auth login` | ✅ | Log in via Deadline Cloud Monitor |
| `auth logout` | ✅ | Log out of Deadline Cloud Monitor |
| `auth status` | ✅ | Check authentication status |

### `auth login`

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

### `auth logout`

Only works for DCM profiles. Calls `auth::logout()`. On success prints
"Successfully logged out of all Deadline Cloud monitor AWS profiles".
Invalidates the session cache so subsequent API calls re-resolve credentials.

### `auth status`

Options: `--profile`, `--output verbose|json` (default verbose).
The `--output` comparison is case-insensitive.

Determines four pieces of information:
- `profile_name` — display name of the active AWS profile
- `source` — `AwsCredentialsSource`: NOT_VALID, HOST_PROVIDED, or DEADLINE_CLOUD_MONITOR_LOGIN
- `status` — `AwsAuthenticationStatus`: CONFIGURATION_ERROR, AUTHENTICATED, or NEEDS_LOGIN
- `api_availability` — boolean, whether the Deadline API endpoint is reachable

**Verbose output:**
```
   Profile Name: my-profile
         Source: DEADLINE_CLOUD_MONITOR_LOGIN
         Status: AUTHENTICATED
API Availability: True
```

Right-aligned labels at 17 characters width.

**JSON output:** Compact single-line JSON with all four fields.
