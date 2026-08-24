# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests that the incremental download helpers scope their deadline clients to the
resolved farm region (multi-region support).
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from deadline.client.api._session import _ADAPTIVE_RETRIES_CLIENT_CONFIG
from deadline.client.cli import _incremental_download
from ..shared_constants import MOCK_FARM_ID, MOCK_QUEUE_ID

MOCK_TIMESTAMP = datetime(2025, 1, 1, tzinfo=timezone.utc)


@pytest.mark.parametrize("region", ["eu-central-1", None])
def test_get_download_candidate_jobs_threads_region(region):
    """_get_download_candidate_jobs passes its region argument through to the
    _list_jobs_by_filter_expression calls so the deadline client is region-scoped."""
    boto3_session = MagicMock()

    with patch.object(
        _incremental_download, "_list_jobs_by_filter_expression", return_value=[]
    ) as mock_list_jobs:
        _incremental_download._get_download_candidate_jobs(
            boto3_session,
            MOCK_FARM_ID,
            MOCK_QUEUE_ID,
            MOCK_TIMESTAMP,
            region=region,
        )

    # Both filter-expression calls (active jobs and recently-ended jobs) must carry the region.
    assert mock_list_jobs.call_count == 2
    for call_args in mock_list_jobs.call_args_list:
        assert call_args.kwargs["region"] == region


@pytest.mark.parametrize("region", ["ap-northeast-1", None])
def test_get_job_sessions_scopes_deadline_client_to_region(region):
    """_get_job_sessions builds its deadline client via get_session_client with the
    resolved region."""
    boto3_session = MagicMock()
    boto3_session_for_s3 = MagicMock()

    checkpoint = MagicMock()
    checkpoint.jobs = []
    checkpoint.downloads_started_timestamp = MOCK_TIMESTAMP
    checkpoint.eventual_consistency_max_seconds = 0

    categorized = MagicMock()
    # No jobs to retrieve sessions for keeps the test focused on client construction.
    categorized.completed = set()
    categorized.added = set()
    categorized.updated = set()

    with patch.object(
        _incremental_download, "get_session_client", return_value=MagicMock()
    ) as mock_get_session_client:
        _incremental_download._get_job_sessions(
            boto3_session,
            boto3_session_for_s3,
            MOCK_FARM_ID,
            {"queueId": MOCK_QUEUE_ID},
            {},
            categorized,
            checkpoint,
            {},
            region=region,
        )

    mock_get_session_client.assert_called_once_with(
        boto3_session,
        "deadline",
        region=region,
        client_config=_ADAPTIVE_RETRIES_CLIENT_CONFIG,
    )
