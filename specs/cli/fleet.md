# fleet Commands

## Subcommands

| Subcommand | Status | Description |
|------------|--------|-------------|
| `fleet list` | ✅ | List fleets in a farm |
| `fleet get` | ✅ | Get details of a specific fleet |

## `fleet list`

Options: `--profile`, `--farm-id`.

Requires: farm_id. Calls ListFleets. Output shows fleetId and displayName.

## `fleet get`

Options: `--profile`, `--farm-id`, `--fleet-id`.

Requires: farm_id, fleet_id. Note: `--fleet-id` is not part of the shared
`CliOptions` struct, so it's handled manually (not stored in config). Missing
fleet_id produces an Operation error, not a usage error (exit 1, not exit 2).

On error, `suggest_resources_on_client_error` lists available fleets.
