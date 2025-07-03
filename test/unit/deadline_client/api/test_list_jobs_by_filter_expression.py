# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from random import randrange
from unittest.mock import MagicMock

import pytest

from deadline.client.api._list_jobs_by_filter_expression import (
    JobFetchFailure,
    _list_jobs_by_filter_expression,
)

# Constants for testing
from ..mock_deadline_job_apis import create_fake_job_list, mock_search_jobs_for_set
from ..shared_constants import MOCK_FARM_ID, MOCK_QUEUE_ID

MOCK_TIMESTAMP = datetime(2023, 1, 2, 3, 4, 5, tzinfo=timezone.utc)


@pytest.fixture
def mock_boto3_session():
    """Create a mock boto3 session for tests."""
    session = MagicMock()
    session.client.return_value = MagicMock()
    return session


def test_list_jobs_by_filter_with_incorrect_filter_expression(mock_boto3_session):
    """Test parameter validation of the timestamp"""
    # Mock SearchJobs to assert it wasn't called
    mock_boto3_session.client().search_jobs.return_value = {}

    # Test when both timestamp and look_back_duration are missing
    with pytest.raises(ValueError) as excinfo:
        _list_jobs_by_filter_expression(
            boto3_session=mock_boto3_session,
            farm_id=MOCK_FARM_ID,
            queue_id=MOCK_QUEUE_ID,
            filter_expression=None,  # type: ignore
        )
    assert "The provided filter expression must be a dict" in str(excinfo.value)

    # Test when both timestamp and look_back_duration are provided
    with pytest.raises(ValueError) as excinfo:
        _list_jobs_by_filter_expression(
            boto3_session=mock_boto3_session,
            farm_id=MOCK_FARM_ID,
            queue_id=MOCK_QUEUE_ID,
            # Missing the "operator"
            filter_expression={"filters": []},
        )
    assert "The provided filter expression must contain 'filters' and 'operator'" in str(
        excinfo.value
    )


@pytest.mark.parametrize("use_look_back", [True, False])
@pytest.mark.parametrize("timestamp_field_name", ["createdAt", "endedAt", "startedAt", "updatedAt"])
@pytest.mark.parametrize(
    "job_count, expected_call_count",
    [
        (0, 1),
        (1, 1),
        (10, 1),
        (70, 1),
        (99, 1),
        (100, 1),
        (101, 2),
        (199, 2),
        (200, 3),
        (201, 3),
        (298, 3),
        (299, 4),
        (300, 4),
    ],
)
def test_list_jobs_recent_jobs_timestamp(
    fresh_deadline_config,
    mock_boto3_session,
    use_look_back,
    timestamp_field_name,
    job_count,
    expected_call_count,
):
    """Test cases calling with with a matrix of variations"""
    updated_at_min = datetime(2025, 1, 1, tzinfo=timezone.utc)
    updated_at_max = datetime(2025, 1, 1, 23, tzinfo=timezone.utc)
    jobs = create_fake_job_list(job_count, updated_at_min, updated_at_max)

    # Create the fake deadline:SearchJobs API, wrapped in a mock we can inspect
    mock_search_jobs = MagicMock(wraps=mock_search_jobs_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, jobs))
    mock_boto3_session.client().search_jobs = mock_search_jobs

    # The function under test
    result = _list_jobs_by_filter_expression(
        boto3_session=mock_boto3_session,
        farm_id=MOCK_FARM_ID,
        queue_id=MOCK_QUEUE_ID,
        filter_expression={
            "filters": [
                {
                    "dateTimeFilter": {
                        "name": timestamp_field_name.replace("At", "_at").upper(),
                        "dateTime": updated_at_min,
                        "operator": "GREATER_THAN_EQUAL_TO",
                    }
                }
            ],
            "operator": "AND",
        },
    )

    assert mock_search_jobs.call_count == expected_call_count
    assert sorted(result, key=lambda v: v[timestamp_field_name]) == sorted(
        jobs, key=lambda v: v[timestamp_field_name]
    )


def test_list_jobs_recent_jobs_timestamp_partial_jobs_return(
    fresh_deadline_config, mock_boto3_session
):
    """Test cases calling with timestamp parameter, where it's somewhere in the middle of the timestamp range"""
    updated_at_min = datetime(2025, 1, 1, tzinfo=timezone.utc)
    updated_at_max = datetime(2025, 1, 1, 23, tzinfo=timezone.utc)
    updated_at_micros_range = int((updated_at_max - updated_at_min).total_seconds() * 1000000)
    jobs = create_fake_job_list(1000, updated_at_min, updated_at_max)

    # Create the fake deadline:SearchJobs API, wrapped in a mock we can inspect
    mock_search_jobs = MagicMock(wraps=mock_search_jobs_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, jobs))
    mock_boto3_session.client().search_jobs = mock_search_jobs

    # Run a few random cases
    for _ in range(10):
        mock_search_jobs.reset_mock()
        # Create a random value for the filter
        random_micros = randrange(0, updated_at_micros_range) if updated_at_micros_range > 0 else 0
        updated_at_filter = updated_at_min + timedelta(microseconds=random_micros)

        # The function under test
        result = _list_jobs_by_filter_expression(
            boto3_session=mock_boto3_session,
            farm_id=MOCK_FARM_ID,
            queue_id=MOCK_QUEUE_ID,
            filter_expression={
                "filters": [
                    {
                        "dateTimeFilter": {
                            "name": "ENDED_AT",
                            "dateTime": updated_at_filter,
                            "operator": "GREATER_THAN_EQUAL_TO",
                        }
                    }
                ],
                "operator": "AND",
            },
        )

        # The resulting set of jobs should match
        filtered_jobs = (job for job in jobs if job["endedAt"] >= updated_at_filter)
        assert sorted(result, key=lambda v: v["createdAt"]) == sorted(
            filtered_jobs, key=lambda v: v["createdAt"]
        )


def test_list_jobs_recent_edge_case_many_equal_timestamps(
    fresh_deadline_config, mock_boto3_session
):
    """Test edge case when there are > 100 exactly equal timestamps."""
    updated_at_min = datetime(2025, 1, 1, tzinfo=timezone.utc)
    updated_at_max = datetime(2025, 1, 1, 23, tzinfo=timezone.utc)
    # Repeat the midpoint a bunch of times
    updated_at_repeated = updated_at_min + 0.5 * (updated_at_max - updated_at_min)
    jobs = create_fake_job_list(100, updated_at_min, updated_at_max)
    jobs.extend(create_fake_job_list(101, updated_at_repeated, updated_at_repeated))

    # Sorted by timestamp, the jobs dataset has ~50 jobs with random timestamps, 101 jobs with repeated timestamp,
    # and then ~50 more jobs with random timestamps again. The first SearchJobs call returns about half of its
    # jobs with repeated timestamps, and then the second call returns the first 100 jobs with repeated timestamp and
    # so misses the 101st.

    # Create the fake deadline:SearchJobs API, wrapped in a mock we can inspect
    mock_search_jobs = MagicMock(wraps=mock_search_jobs_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, jobs))
    mock_boto3_session.client().search_jobs = mock_search_jobs

    with pytest.raises(JobFetchFailure) as excinfo:
        _list_jobs_by_filter_expression(
            boto3_session=mock_boto3_session,
            farm_id=MOCK_FARM_ID,
            queue_id=MOCK_QUEUE_ID,
            filter_expression={
                "filters": [
                    {
                        "dateTimeFilter": {
                            "name": "UPDATED_AT",
                            "dateTime": updated_at_min,
                            "operator": "GREATER_THAN_EQUAL_TO",
                        }
                    }
                ],
                "operator": "AND",
            },
        )
    assert "more then 100 jobs have the exact same timestamp value" in str(excinfo.value)

    assert mock_search_jobs.call_count == 2
    # The way the algorithm and test data set are structured, the first call will filter from the provided parameter
    # timestamp, and the second call will filter from the repeated timestamp
    assert len(mock_search_jobs.call_args_list[0].kwargs["filterExpressions"]["filters"]) == 1

    assert len(mock_search_jobs.call_args_list[1].kwargs["filterExpressions"]["filters"]) == 2
    assert (
        mock_search_jobs.call_args_list[1].kwargs["filterExpressions"]["filters"][1][
            "dateTimeFilter"
        ]["dateTime"]
        == updated_at_repeated
    )


@pytest.mark.parametrize("job_count", [0, 1, 10, 70, 99, 100, 101, 199, 200, 201, 298, 500, 1000])
def test_list_jobs_by_filter_expression(
    fresh_deadline_config,
    mock_boto3_session,
    job_count,
):
    """Test cases calling with with a matrix of variations"""
    test_status_values = ["READY", "ASSIGNED", "STARTING"]

    jobs = sorted(create_fake_job_list(job_count), key=lambda job: job["jobId"])

    # Create the fake deadline:SearchJobs API, wrapped in a mock we can inspect
    mock_search_jobs = MagicMock(wraps=mock_search_jobs_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, jobs))
    mock_boto3_session.client().search_jobs = mock_search_jobs

    # The function under test
    result = _list_jobs_by_filter_expression(
        boto3_session=mock_boto3_session,
        farm_id=MOCK_FARM_ID,
        queue_id=MOCK_QUEUE_ID,
        filter_expression={
            "filters": [
                {
                    "stringFilter": {
                        "name": "TASK_RUN_STATUS",
                        "operator": "EQUAL",
                        "value": status_value,
                    },
                }
                for status_value in test_status_values
            ],
            "operator": "OR",
        },
    )

    # The result should be the full set of jobs matching the filter criteria. The order is arbitrary, so we sort to compare.
    assert sorted(result, key=lambda job: job["jobId"]) == [
        job for job in jobs if job["taskRunStatus"] in test_status_values
    ]
