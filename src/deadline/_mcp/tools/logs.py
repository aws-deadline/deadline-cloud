# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Deadline Cloud log retrieval tools.
"""

from typing import Any, Dict

from ...client.api import get_session, get_session_logs, get_worker_logs


def get_session_and_worker_logs(
    farm_id: str,
    queue_id: str,
    job_id: str,
    session_id: str,
    limit: int = 100,
) -> Dict[str, Any]:
    """
    Get BOTH session logs AND worker logs for a specific session in one call.

    This is the recommended way to retrieve logs for troubleshooting. It fetches the session
    details to find the workerId and fleetId, then retrieves both log streams together,
    ensuring the correct worker logs are matched to the correct session.

    Session logs contain task execution stdout/stderr.
    Worker logs contain infrastructure events: spot interruptions, instance terminations,
    agent crashes, OOM kills, and environment setup failures.

    Args:
        farm_id: The ID of the farm.
        queue_id: The ID of the queue.
        job_id: The ID of the job.
        session_id: The ID of the session to get logs for.
        limit: Maximum number of log lines to return per log stream (default 100).

    Returns:
        Dictionary with session details, session_logs, and worker_logs.
    """
    # Get session details to find the worker and fleet
    session_details = get_session(
        farm_id=farm_id, queue_id=queue_id, job_id=job_id, session_id=session_id
    )
    worker_id = session_details.get("workerId")
    fleet_id = session_details.get("fleetId")

    result: Dict[str, Any] = {
        "session_id": session_id,
        "worker_id": worker_id,
        "fleet_id": fleet_id,
        "lifecycle_status": session_details.get("lifecycleStatus"),
        "host_properties": session_details.get("hostProperties"),
    }

    # Get session logs
    session_log_result = get_session_logs(
        farm_id=farm_id, queue_id=queue_id, session_id=session_id, limit=limit
    )
    result["session_logs"] = {
        "log_group": session_log_result.log_group,
        "events": [
            {"timestamp": str(e.timestamp), "message": e.message} for e in session_log_result.events
        ],
        "count": session_log_result.count,
    }

    # Get worker logs for the same time period
    result["worker_logs"] = {"events": [], "count": 0, "error": None}
    if worker_id and fleet_id:
        try:
            worker_log_result = get_worker_logs(
                farm_id=farm_id, fleet_id=fleet_id, worker_id=worker_id, limit=limit
            )
            result["worker_logs"] = {
                "log_group": worker_log_result.log_group,
                "events": [
                    {"timestamp": str(e.timestamp), "message": e.message}
                    for e in worker_log_result.events
                ],
                "count": worker_log_result.count,
            }
        except Exception as e:
            result["worker_logs"]["error"] = str(e)

    return result
