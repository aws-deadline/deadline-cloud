# farm Commands

## Subcommands

| Subcommand | Status | Description |
|------------|--------|-------------|
| `farm list` | ✅ | List available farms |
| `farm get` | ✅ | Get details of a specific farm |

## `farm list`

Options: `--profile`.

No required settings — lists all farms accessible to the caller. Output
shows farmId and displayName for each farm.

## `farm get`

Options: `--profile`, `--farm-id`.

Requires: farm_id (from flag or config). Calls GetFarm. Full response
via `cli_object_repr`. On error, `suggest_resources_on_client_error`
lists available farms.
