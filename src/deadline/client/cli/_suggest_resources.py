# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Helper functions for suggesting available resources when CLI commands fail
due to incorrect resource IDs.
"""

from __future__ import annotations

from configparser import ConfigParser
from typing import Optional

from botocore.exceptions import ClientError

from .. import api


def _format_resource_suggestions(
    items: list,
    id_field: str,
    name_field: str,
    header: str,
    max_items: int = 10,
) -> list[str]:
    """Format a list of resources as suggestion lines."""
    if not items:
        return []
    lines = [f"\n{header}"]
    for item in items[:max_items]:
        lines.append(f"  {item[id_field]}  {item.get(name_field, '')}")
    if len(items) > max_items:
        lines.append(f"  ... and {len(items) - max_items} more")
    return lines


def _try_suggestion_chain(
    fetchers: list[tuple],
    config: Optional[ConfigParser],
) -> tuple[list[str], bool]:
    """
    Try a chain of fetchers until one succeeds.
    Each fetcher is a tuple of (fetch_func, response_key, id_field, name_field, header).
    Returns (suggestion_lines, all_failed).
    """
    for fetch_func, response_key, id_field, name_field, header in fetchers:
        try:
            response = fetch_func()
            items = response.get(response_key, [])
            suggestions = _format_resource_suggestions(items, id_field, name_field, header)
            if suggestions:
                return suggestions, False
        except ClientError:
            continue
    return [], True


# Mapping of operation names to their suggestion configuration.
_OPERATION_GROUPS: dict[str, dict[str, tuple]] = {
    "queue": {
        "operations": ("GetQueue", "ListQueues", "ListQueueEnvironments"),
        "requires": ("farm_id",),
    },
    "farm": {
        "operations": ("GetFarm", "ListFarms"),
        "requires": (),
    },
    "fleet": {
        "operations": ("GetFleet", "ListFleets"),
        "requires": ("farm_id",),
    },
    "worker": {
        "operations": ("GetWorker", "SearchWorkers"),
        "requires": ("farm_id", "fleet_id"),
    },
    "job": {
        "operations": ("GetJob", "ListJobs", "SearchJobs"),
        "requires": ("farm_id", "queue_id"),
    },
    "storage_profile": {
        "operations": ("GetStorageProfileForQueue", "ListStorageProfilesForQueue"),
        "requires": ("farm_id", "queue_id"),
    },
}


def _suggest_resources_on_client_error(
    exc: ClientError,
    farm_id: Optional[str] = None,
    queue_id: Optional[str] = None,
    fleet_id: Optional[str] = None,
    worker_id: Optional[str] = None,
    config: Optional[ConfigParser] = None,
) -> str:
    """
    When a ClientError occurs (e.g., AccessDeniedException), attempt to list
    available resources to help users identify typos in resource IDs.

    Returns a suggestion string to append to the error message, or empty string
    if no suggestions could be generated.
    """
    error_code = exc.response.get("Error", {}).get("Code", "")

    # Only handle access/not-found type errors that indicate a wrong resource ID
    if error_code not in (
        "AccessDeniedException",
        "ResourceNotFoundException",
        "ValidationException",
    ):
        return ""

    operation_name = getattr(exc, "operation_name", "") or ""

    # Build fetcher functions that capture the config and resource IDs
    def list_farms():
        return api.list_farms(config=config)

    def list_queues():
        return api.list_queues(farmId=farm_id, config=config)

    def list_fleets():
        return api.list_fleets(farmId=farm_id, config=config)

    def list_jobs():
        return api.list_jobs(farmId=farm_id, queueId=queue_id, config=config)

    def list_storage_profiles():
        return api.list_storage_profiles_for_queue(farmId=farm_id, queueId=queue_id, config=config)

    def search_workers():
        deadline = api.get_boto3_client("deadline", config=config)
        search_kwargs: dict = {"farmId": farm_id, "fleetIds": [fleet_id], "itemOffset": 0}
        if worker_id:
            search_kwargs["filterExpressions"] = {
                "filters": [{"searchTermFilter": {"searchTerm": worker_id}}],
                "operator": "AND",
            }
        return deadline.search_workers(**search_kwargs)

    # Define suggestion chains for each operation group
    suggestions: list[str] = []
    list_failed = False

    if operation_name in _OPERATION_GROUPS["queue"]["operations"] and farm_id:
        suggestions, list_failed = _try_suggestion_chain(
            [
                (
                    list_queues,
                    "queues",
                    "queueId",
                    "displayName",
                    f"Available queues in farm {farm_id}:",
                ),
                (
                    list_farms,
                    "farms",
                    "farmId",
                    "displayName",
                    f"Farm {farm_id} may be incorrect. Available farms:",
                ),
            ],
            config,
        )

    elif operation_name in _OPERATION_GROUPS["farm"]["operations"]:
        suggestions, list_failed = _try_suggestion_chain(
            [(list_farms, "farms", "farmId", "displayName", "Available farms:")],
            config,
        )

    elif operation_name in _OPERATION_GROUPS["fleet"]["operations"] and farm_id:
        suggestions, list_failed = _try_suggestion_chain(
            [
                (
                    list_fleets,
                    "fleets",
                    "fleetId",
                    "displayName",
                    f"Available fleets in farm {farm_id}:",
                ),
                (
                    list_farms,
                    "farms",
                    "farmId",
                    "displayName",
                    f"Farm {farm_id} may be incorrect. Available farms:",
                ),
            ],
            config,
        )

    elif operation_name in _OPERATION_GROUPS["worker"]["operations"] and farm_id and fleet_id:
        # Workers use search_workers with special handling for totalResults
        try:
            response = search_workers()
            workers = response.get("workers", [])
            if workers:
                suggestions = [f"\nAvailable workers in fleet {fleet_id}:"]
                for w in workers[:10]:
                    suggestions.append(f"  {w['workerId']}  {w.get('status', '')}")
                total = response.get("totalResults", len(workers))
                if total > 10:
                    suggestions.append(f"  ... and {total - 10} more")
        except ClientError:
            suggestions, list_failed = _try_suggestion_chain(
                [
                    (
                        list_fleets,
                        "fleets",
                        "fleetId",
                        "displayName",
                        f"Fleet {fleet_id} may be incorrect. Available fleets:",
                    )
                ],
                config,
            )

    elif operation_name in _OPERATION_GROUPS["job"]["operations"] and farm_id and queue_id:
        suggestions, list_failed = _try_suggestion_chain(
            [
                (list_jobs, "jobs", "jobId", "name", f"Recent jobs in queue {queue_id}:"),
                (
                    list_queues,
                    "queues",
                    "queueId",
                    "displayName",
                    f"Queue {queue_id} may be incorrect. Available queues:",
                ),
                (
                    list_farms,
                    "farms",
                    "farmId",
                    "displayName",
                    f"Farm {farm_id} may be incorrect. Available farms:",
                ),
            ],
            config,
        )

    elif (
        operation_name in _OPERATION_GROUPS["storage_profile"]["operations"]
        and farm_id
        and queue_id
    ):
        suggestions, list_failed = _try_suggestion_chain(
            [
                (
                    list_storage_profiles,
                    "storageProfiles",
                    "storageProfileId",
                    "displayName",
                    f"Available storage profiles for queue {queue_id}:",
                )
            ],
            config,
        )

    # If we couldn't list resources, add a hint about permissions
    if list_failed and not suggestions:
        suggestions = [
            "\nCould not list available resources to suggest alternatives.",
            "This may indicate your IAM policy is missing List permissions.",
        ]

    return "\n".join(suggestions)
