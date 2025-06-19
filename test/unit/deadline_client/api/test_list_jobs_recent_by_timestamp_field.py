# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

import uuid
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from random import randrange
from typing import Any
from unittest.mock import MagicMock

import pytest

from deadline.client.api._list_jobs_recent_by_timestamp_field import (
    JobFetchFailure,
    _list_jobs_recent_by_timestamp_field,
)

# Constants for testing
from ..shared_constants import MOCK_FARM_ID, MOCK_QUEUE_ID

MOCK_TIMESTAMP = datetime(2023, 1, 2, 3, 4, 5, tzinfo=timezone.utc)


@pytest.fixture
def mock_boto3_session():
    """Create a mock boto3 session for tests."""
    session = MagicMock()
    session.client.return_value = MagicMock()
    return session


def fake_search_jobs_for_set(farmIdForJobs, queueIdForJobs, jobs):
    """Returns a fake "search_jobs" API that emulates a subset of Deadline Cloud's
    SearchJobs on the provided set of jobs.

    These fake jobs only have the timestamp fields. If we decide to generalize this
    function for more cases, we could move this to a conftest.py and extend it.

    See https://docs.aws.amazon.com/deadline-cloud/latest/APIReference/API_SearchJobs.html
    """

    def _fake_search_jobs(
        farmId, queueIds, itemOffset, pageSize=100, filterExpressions={}, sortExpressions=[]
    ):
        # Make deep copies of these parameters so we can destructively assert about them
        filterExpressions = deepcopy(filterExpressions)
        sortExpressions = deepcopy(sortExpressions)

        assert farmId == farmIdForJobs
        assert queueIdForJobs in queueIds
        # We're only implementing ascending order by one of the timestamp fields
        field_sort_name = sortExpressions[0]["fieldSort"].pop("name")
        assert field_sort_name in ["CREATED_AT", "ENDED_AT", "STARTED_AT", "UPDATED_AT"]
        assert sortExpressions == [{"fieldSort": {"sortOrder": "ASCENDING"}}]
        # Only implementing a filter that is "<timestamp field> >= threshold"
        threshold = filterExpressions["filters"][0]["dateTimeFilter"].pop("dateTime")
        assert isinstance(threshold, datetime)
        field_filter_name = filterExpressions["filters"][0]["dateTimeFilter"].pop("name")
        assert field_filter_name in ["CREATED_AT", "ENDED_AT", "STARTED_AT", "UPDATED_AT"]
        assert filterExpressions == {
            "filters": [
                {
                    "dateTimeFilter": {
                        "operator": "GREATER_THAN_EQUAL_TO",
                    }
                }
            ],
            "operator": "AND",
        }

        # Sort the jobs
        result_jobs = sorted(jobs, key=lambda j: j[field_sort_name.lower().replace("_a", "A")])
        # Filter the jobs
        result_jobs = [
            j for j in result_jobs if j[field_filter_name.lower().replace("_a", "A")] >= threshold
        ]
        # Construct the API response
        nextItemOffset = min(len(result_jobs), itemOffset + pageSize)
        response = {
            "jobs": deepcopy(result_jobs[itemOffset:nextItemOffset]),
            "totalResults": len(result_jobs),
        }
        if nextItemOffset < len(result_jobs):
            response["nextItemOffset"] = nextItemOffset

        return response

    return _fake_search_jobs


def create_fake_job_list(job_count, updated_at_min: datetime, updated_at_max: datetime):
    """See https://docs.aws.amazon.com/deadline-cloud/latest/APIReference/API_JobSearchSummary.html
    The list_jobs_recently_updated function only uses jobId and timestamps, so we're only populating
    those values.
    """
    jobs: list[dict[str, Any]] = [
        {"jobId": f"job-{str(uuid.uuid4()).replace('-', '')}"} for _ in range(job_count)
    ]

    updated_at_micros_range = int((updated_at_max - updated_at_min).total_seconds() * 1000000)

    # Populate all the various timestamp fields
    for timestamp_field_name in ["createdAt", "endedAt", "startedAt", "updatedAt"]:
        for job in jobs:
            random_micros = (
                randrange(0, updated_at_micros_range) if updated_at_micros_range > 0 else 0
            )
            job[timestamp_field_name] = updated_at_min + timedelta(microseconds=random_micros)

        if job_count >= 2:
            # Ensure two randomly-selected jobs have the min and max values
            jobs[randrange(0, job_count // 2)][timestamp_field_name] = updated_at_min
            jobs[randrange(job_count // 2, job_count)][timestamp_field_name] = updated_at_max

    return jobs


def test_list_jobs_recent_timestamp_param_error(mock_boto3_session):
    """Test parameter validation of the timestamp"""
    # Mock SearchJobs to assert it wasn't called
    mock_boto3_session.client().search_jobs.return_value = {}

    # Test when both timestamp and look_back_duration are missing
    with pytest.raises(ValueError) as excinfo:
        _list_jobs_recent_by_timestamp_field(
            boto3_session=mock_boto3_session,
            farm_id=MOCK_FARM_ID,
            queue_id=MOCK_QUEUE_ID,
            timestamp_field_name="createdAt",
        )
    assert "One of timestamp or look_back_duration must be provided" in str(excinfo.value)

    # Test when both timestamp and look_back_duration are provided
    with pytest.raises(ValueError) as excinfo:
        _list_jobs_recent_by_timestamp_field(
            boto3_session=mock_boto3_session,
            farm_id=MOCK_FARM_ID,
            queue_id=MOCK_QUEUE_ID,
            timestamp_field_name="createdAt",
            timestamp=datetime.now(timezone.utc),
            look_back_duration=timedelta(minutes=5),
        )
    assert "Only one of timestamp and look_back_duration may be provided" in str(excinfo.value)

    # Test when timestamp is missing a timezone
    with pytest.raises(ValueError) as excinfo:
        _list_jobs_recent_by_timestamp_field(
            boto3_session=mock_boto3_session,
            farm_id=MOCK_FARM_ID,
            queue_id=MOCK_QUEUE_ID,
            timestamp_field_name="createdAt",
            timestamp=datetime.now(),
        )
    assert "is missing a required time zone" in str(excinfo.value)

    # Test when timestamp is missing a timezone
    with pytest.raises(ValueError) as excinfo:
        _list_jobs_recent_by_timestamp_field(
            boto3_session=mock_boto3_session,
            farm_id=MOCK_FARM_ID,
            queue_id=MOCK_QUEUE_ID,
            timestamp_field_name="introducedAt",
            timestamp=datetime.now(timezone.utc),
        )
    assert "The provided timestamp field name" in str(excinfo.value)
    assert "is not one of" in str(excinfo.value)

    # Every case should have raised before calling SearchJobs
    assert mock_boto3_session.client().search_jobs.call_count == 0


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
    mock_search_jobs = MagicMock(wraps=fake_search_jobs_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, jobs))
    mock_boto3_session.client().search_jobs = mock_search_jobs

    if use_look_back:
        # This lookback time is the duration since the first job, plus 5 minutes to account for testing variance
        look_back_duration = (datetime.now(timezone.utc) - updated_at_min) + timedelta(minutes=5)
        timestamp = None
    else:
        look_back_duration = None
        timestamp = updated_at_min

    # The function under test
    result = _list_jobs_recent_by_timestamp_field(
        boto3_session=mock_boto3_session,
        farm_id=MOCK_FARM_ID,
        queue_id=MOCK_QUEUE_ID,
        timestamp_field_name=timestamp_field_name,
        timestamp=timestamp,
        look_back_duration=look_back_duration,
    )

    assert mock_search_jobs.call_count == expected_call_count
    assert (
        mock_search_jobs.call_args_list[0].kwargs["filterExpressions"]["filters"][0][
            "dateTimeFilter"
        ]["dateTime"]
        <= updated_at_min
    )
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
    mock_search_jobs = MagicMock(wraps=fake_search_jobs_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, jobs))
    mock_boto3_session.client().search_jobs = mock_search_jobs

    # Run a few random cases
    for _ in range(10):
        mock_search_jobs.reset_mock()
        # Create a random value for the filter
        random_micros = randrange(0, updated_at_micros_range) if updated_at_micros_range > 0 else 0
        updated_at_filter = updated_at_min + timedelta(microseconds=random_micros)

        # The function under test
        result = _list_jobs_recent_by_timestamp_field(
            boto3_session=mock_boto3_session,
            farm_id=MOCK_FARM_ID,
            queue_id=MOCK_QUEUE_ID,
            timestamp_field_name="createdAt",
            timestamp=updated_at_filter,
        )

        # The first SearchJobs call should be at the provided filter
        assert (
            mock_search_jobs.call_args_list[0].kwargs["filterExpressions"]["filters"][0][
                "dateTimeFilter"
            ]["dateTime"]
            == updated_at_filter
        )
        # Each subsequent SearchJobs call should be with a greater timestamp
        for i in range(1, mock_search_jobs.call_count):
            cur_datetime = mock_search_jobs.call_args_list[i].kwargs["filterExpressions"][
                "filters"
            ][0]["dateTimeFilter"]["dateTime"]
            prev_datetime = mock_search_jobs.call_args_list[i - 1].kwargs["filterExpressions"][
                "filters"
            ][0]["dateTimeFilter"]["dateTime"]
            assert cur_datetime > prev_datetime
        filtered_jobs = (job for job in jobs if job["createdAt"] >= updated_at_filter)
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
    mock_search_jobs = MagicMock(wraps=fake_search_jobs_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, jobs))
    mock_boto3_session.client().search_jobs = mock_search_jobs

    with pytest.raises(JobFetchFailure) as excinfo:
        _list_jobs_recent_by_timestamp_field(
            boto3_session=mock_boto3_session,
            farm_id=MOCK_FARM_ID,
            queue_id=MOCK_QUEUE_ID,
            timestamp_field_name="updatedAt",
            timestamp=updated_at_min,
        )
    assert "more then 100 jobs have the exact same timestamp value" in str(excinfo.value)

    assert mock_search_jobs.call_count == 2
    # The way the algorithm and test data set are structured, the first call will filter from the provided parameter
    # timestamp, and the second call will filter from the repeated timestamp
    assert (
        mock_search_jobs.call_args_list[0].kwargs["filterExpressions"]["filters"][0][
            "dateTimeFilter"
        ]["dateTime"]
        == updated_at_min
    )
    assert (
        mock_search_jobs.call_args_list[1].kwargs["filterExpressions"]["filters"][0][
            "dateTimeFilter"
        ]["dateTime"]
        == updated_at_repeated
    )
