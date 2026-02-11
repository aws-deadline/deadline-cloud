# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for the MCP logs tools.
"""

import datetime
from unittest.mock import patch, MagicMock

from deadline._mcp.tools.logs import get_session_and_worker_logs


MOCK_FARM_ID = "farm-0123456789abcdefabcdefabcdefabcd"
MOCK_QUEUE_ID = "queue-0123456789abcdefabcdefabcdefabcd"
MOCK_JOB_ID = "job-0123456789abcdefabcdefabcdefabcd"
MOCK_SESSION_ID = "session-0123456789abcdefabcdefabcdefabcd"
MOCK_WORKER_ID = "worker-0123456789abcdefabcdefabcdefabcd"
MOCK_FLEET_ID = "fleet-0123456789abcdefabcdefabcdefabcd"


def _make_log_result(events_count=2, log_group="group", log_stream="stream"):
    result = MagicMock()
    result.log_group = log_group
    result.log_stream = log_stream
    result.count = events_count
    result.events = [
        MagicMock(
            timestamp=datetime.datetime(2023, 1, 1, 12, i, 0, tzinfo=datetime.timezone.utc),
            message=f"Log message {i}",
        )
        for i in range(events_count)
    ]
    return result


@patch("deadline._mcp.tools.logs.get_worker_logs")
@patch("deadline._mcp.tools.logs.get_session_logs")
@patch("deadline._mcp.tools.logs.get_session")
def test_get_session_and_worker_logs_basic(mock_get_session, mock_session_logs, mock_worker_logs):
    """Test that combined tool fetches session details and both log streams."""
    mock_get_session.return_value = {
        "workerId": MOCK_WORKER_ID,
        "fleetId": MOCK_FLEET_ID,
        "lifecycleStatus": "ENDED",
        "hostProperties": {"hostName": "test-host"},
    }
    mock_session_logs.return_value = _make_log_result(
        2, f"/aws/deadline/{MOCK_FARM_ID}/{MOCK_QUEUE_ID}", MOCK_SESSION_ID
    )
    mock_worker_logs.return_value = _make_log_result(
        3, f"/aws/deadline/{MOCK_FARM_ID}/{MOCK_FLEET_ID}", MOCK_WORKER_ID
    )

    result = get_session_and_worker_logs(
        farm_id=MOCK_FARM_ID,
        queue_id=MOCK_QUEUE_ID,
        job_id=MOCK_JOB_ID,
        session_id=MOCK_SESSION_ID,
    )

    assert result["session_id"] == MOCK_SESSION_ID
    assert result["worker_id"] == MOCK_WORKER_ID
    assert result["fleet_id"] == MOCK_FLEET_ID
    assert result["session_logs"]["count"] == 2
    assert result["worker_logs"]["count"] == 3

    mock_get_session.assert_called_once_with(
        farm_id=MOCK_FARM_ID, queue_id=MOCK_QUEUE_ID, job_id=MOCK_JOB_ID, session_id=MOCK_SESSION_ID
    )
    mock_worker_logs.assert_called_once_with(
        farm_id=MOCK_FARM_ID, fleet_id=MOCK_FLEET_ID, worker_id=MOCK_WORKER_ID, limit=100
    )


@patch("deadline._mcp.tools.logs.get_worker_logs")
@patch("deadline._mcp.tools.logs.get_session_logs")
@patch("deadline._mcp.tools.logs.get_session")
def test_get_session_and_worker_logs_worker_error(
    mock_get_session, mock_session_logs, mock_worker_logs
):
    """Test that worker log errors are captured without failing the whole call."""
    mock_get_session.return_value = {
        "workerId": MOCK_WORKER_ID,
        "fleetId": MOCK_FLEET_ID,
        "lifecycleStatus": "ENDED",
    }
    mock_session_logs.return_value = _make_log_result(1)
    mock_worker_logs.side_effect = Exception("Access denied")

    result = get_session_and_worker_logs(
        farm_id=MOCK_FARM_ID,
        queue_id=MOCK_QUEUE_ID,
        job_id=MOCK_JOB_ID,
        session_id=MOCK_SESSION_ID,
    )

    assert result["session_logs"]["count"] == 1
    assert result["worker_logs"]["count"] == 0
    assert result["worker_logs"]["error"] == "Access denied"
