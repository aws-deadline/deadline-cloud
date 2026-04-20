# fleet Commands

## Subcommands

| Subcommand | Status | Description |
|------------|--------|-------------|
| `fleet list` | ✅ | List fleets in a farm |
| `fleet get` | ✅ | Get details of a specific fleet or all fleets for a queue |

## `fleet list`

Options: `--profile`, `--farm-id`.

Requires: farm_id. Calls ListFleets. Output shows fleetId and displayName.

## `fleet get`

Options: `--profile`, `--farm-id`, `--fleet-id`, `--queue-id`.

### Modes

| Mode | Trigger | Behavior |
|------|---------|----------|
| Fleet mode | `--fleet-id` provided | Get single fleet details |
| Queue mode | `--queue-id` provided (or default queue from config) | List all fleets associated with the queue |

Modes are mutually exclusive. Both `--fleet-id` and `--queue-id` → error.
Neither flag and no default queue → error.

### Fleet mode (`--fleet-id`)

Calls `GetFleet(farmId, fleetId)`. Prints full fleet details as YAML.
On error, `suggest_resources_on_client_error` lists available fleets.

### Queue mode (`--queue-id`)

1. Calls `GetQueue(farmId, queueId)` to get the queue display name
2. Calls `ListQueueFleetAssociations(farmId, queueId)` (paginated)
3. For each association, calls `GetFleet(farmId, fleetId)`
4. Appends `queueFleetAssociationStatus` from the association to each
   fleet response
5. Prints header: `"Showing all fleets (N total) associated with queue: {name}"`
6. Prints each fleet separated by blank lines

### Queue ID resolution

Priority: CLI `--queue-id` arg > `defaults.queue_id` from config.
Empty config values are treated as absent.

### Differences from Python

- Python has a bug where `--queue-id` CLI argument is overwritten by
  `defaults.queue_id` from config (line 90 of `fleet_group.py`). Rust
  correctly prioritizes the CLI argument.
