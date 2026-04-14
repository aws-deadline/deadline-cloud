# worker Commands

## Subcommands

| Subcommand | Status | Description |
|------------|--------|-------------|
| `worker list` | ✅ | List workers in a fleet (paginated) |
| `worker get` | ✅ | Get details of a specific worker |

## `worker list`

Options: `--profile`, `--farm-id`, `--fleet-id` (required), `--page-size`
(default 5), `--item-offset` (default 0).

Requires: farm_id, fleet_id. Calls SearchWorkers (not ListWorkers). Output
shows a summary line ("Displaying N of M workers starting at offset")
followed by workerId, status, and createdAt for each worker.

## `worker get`

Options: `--profile`, `--farm-id`, `--fleet-id` (required), `--worker-id`
(required).

Requires: farm_id, fleet_id, worker_id. Calls GetWorker. Full response
via `cli_object_repr`.
