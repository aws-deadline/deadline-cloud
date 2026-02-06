# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
This file implements a partial re-implementation of the deadline:SearchJobs and deadline:GetJob APIs,
so that we can unit tests queries that depend on its behavior in more complex ways.

The function create_fake_job_list uses a random number generator to produce a list of
job dictionaries. The mock_search_jobs_for_set takes such a list of jobs, and returns
a function that will return responses by sorting, filtering, and slicing that list.
"""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
from random import randrange, choice
from typing import Any, Callable, Optional
import uuid

import botocore.exceptions

from .testing_utilities import snake_to_camel

__all__ = ["create_fake_job_list", "mock_search_jobs_for_set"]


TASK_RUN_STATUS_VALUES = [
    "PENDING",
    "READY",
    "ASSIGNED",
    "STARTING",
    "SCHEDULED",
    "INTERRUPTING",
    "RUNNING",
    "SUSPENDED",
    "CANCELED",
    "FAILED",
    "SUCCEEDED",
    "NOT_COMPATIBLE",
]


def _build_filter_function(filter) -> Callable[[dict], bool]:
    try:
        # This only supports the subset of filters used in the functions we're testing.
        # You can extend it to support more, to be able to test more cases!
        if "stringFilter" in filter:
            filter = filter["stringFilter"]

            field_name = snake_to_camel(filter["name"].lower())
            operator = filter["operator"]
            value = filter["value"]

            assert isinstance(value, str)

            if operator == "EQUAL":

                def _filter_function(job) -> bool:
                    return field_name in job and job[field_name] == value
            elif operator == "GREATER_THAN_EQUAL_TO":

                def _filter_function(job) -> bool:
                    return field_name in job and job[field_name] >= value
            else:
                raise ValueError(f"fake SearchJobs filter has not implemented operator {operator}")
        elif "stringListFilter" in filter:
            filter = filter["stringListFilter"]

            field_name = snake_to_camel(filter["name"].lower())
            operator = filter["operator"]
            values = filter["values"]

            assert isinstance(values, list)

            if operator == "ANY_EQUALS":

                def _filter_function(job) -> bool:
                    return field_name in job and job[field_name] in values
            elif operator == "ALL_NOT_EQUALS":

                def _filter_function(job) -> bool:
                    return field_name in job and job[field_name] not in values
            else:
                raise ValueError(f"fake SearchJobs filter has not implemented operator {operator}")
        elif "dateTimeFilter" in filter:
            filter = filter["dateTimeFilter"]

            assert filter["name"] in ["CREATED_AT", "ENDED_AT", "STARTED_AT", "UPDATED_AT"]
            field_name = snake_to_camel(filter["name"].lower())
            operator = filter["operator"]
            value = filter["dateTime"]

            assert isinstance(value, datetime)

            if operator == "EQUAL":

                def _filter_function(job) -> bool:
                    return field_name in job and job[field_name] == value
            elif operator == "GREATER_THAN_EQUAL_TO":

                def _filter_function(job) -> bool:
                    return field_name in job and job[field_name] >= value
            else:
                raise ValueError(f"fake SearchJobs filter has not implemented operator {operator}")
        elif "groupFilter" in filter:
            return _build_filter_function_from_group(filter["groupFilter"])
        else:
            raise ValueError(f"fake SearchJobs filter not implemented for {filter}")

        return _filter_function
    except KeyError as e:
        raise ValueError(f"fake SearchJobs filter not implemented for {filter} - {e}")


def _build_filter_function_from_reduction(filter_list, all_or_any) -> Callable[[dict], bool]:
    def _filter_function(job) -> bool:
        return all_or_any(filter(job) for filter in filter_list)

    return _filter_function


def _build_filter_function_from_group(filter_expressions) -> Callable[[dict], bool]:
    """Given a filterExpressions parameter value for deadline:SearchJobs, constructs a Python
    callable that returns True/False depending on whether the job matches the filter."""

    filters = [_build_filter_function(filter) for filter in filter_expressions["filters"]]

    if len(filters) == 1:
        # If there's just a single filter
        return filters[0]

    operator = filter_expressions["operator"]
    if operator == "OR":
        all_or_any = any
    elif operator == "AND":
        all_or_any = all
    else:
        raise ValueError(f"filter expressions operator value {operator} is incorrect")

    return _build_filter_function_from_reduction(filters, all_or_any)


def mock_search_jobs_for_set(farmIdForJobs, queueIdForJobs, jobs):
    """Returns a fake "search_jobs" API that emulates a subset of Deadline Cloud's
    SearchJobs on the provided set of jobs.

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
        filter = _build_filter_function_from_group(filterExpressions)

        # Sort the jobs
        result_jobs = sorted(jobs, key=lambda j: j[field_sort_name.lower().replace("_a", "A")])
        # Filter the jobs
        result_jobs = [j for j in result_jobs if filter(j)]
        # Construct the API response
        nextItemOffset = min(len(result_jobs), itemOffset + pageSize)
        response_jobs = deepcopy(result_jobs[itemOffset:nextItemOffset])

        # Remove all "attachments" properties from the response, they are not returned by deadline:SearchJobs
        for job in response_jobs:
            if "attachments" in job:
                del job["attachments"]

        response = {
            "jobs": response_jobs,
            "totalResults": len(result_jobs),
        }
        if nextItemOffset < len(result_jobs):
            response["nextItemOffset"] = nextItemOffset

        return response

    return _fake_search_jobs


def mock_get_job_for_set(farmIdForJobs, queueIdForJobs, jobs):
    """Returns a fake "get_job" API that emulates a subset of Deadline Cloud's
    GetJob on the provided set of jobs.

    See https://docs.aws.amazon.com/deadline-cloud/latest/APIReference/API_GetJob.html
    """

    def _fake_get_job(farmId, queueId, jobId):
        assert farmId == farmIdForJobs
        assert queueId == queueIdForJobs

        matching_jobs = [job for job in jobs if job["jobId"] == jobId]

        if matching_jobs:
            return matching_jobs[0]
        else:
            error_class = botocore.exceptions.from_code(404)
            raise error_class(f"Resource of type job with id {jobId} does not exist.", "GetJob")

    return _fake_get_job


def create_fake_job_list(
    job_count: int,
    timestamp_min: Optional[datetime] = None,
    timestamp_max: Optional[datetime] = None,
):
    """See https://docs.aws.amazon.com/deadline-cloud/latest/APIReference/API_JobSearchSummary.html
    The list_jobs_recently_updated function only uses jobId and timestamps, so we're only populating
    those values.
    """
    if timestamp_min is None:
        timestamp_min = datetime(2025, 1, 1, tzinfo=timezone.utc)
    if timestamp_max is None:
        timestamp_max = datetime(2025, 1, 1, 23, tzinfo=timezone.utc)

    jobs: list[dict[str, Any]] = [
        {"jobId": f"job-{str(uuid.uuid4()).replace('-', '')}"} for _ in range(job_count)
    ]

    updated_at_micros_range = int((timestamp_max - timestamp_min).total_seconds() * 1000000)

    # Populate all the various timestamp fields
    for timestamp_field_name in ["createdAt", "endedAt", "startedAt", "updatedAt"]:
        for job in jobs:
            random_micros = (
                randrange(0, updated_at_micros_range) if updated_at_micros_range > 0 else 0
            )
            job[timestamp_field_name] = timestamp_min + timedelta(microseconds=random_micros)

        if job_count >= 2:
            # Ensure two randomly-selected jobs have the min and max values
            jobs[randrange(0, job_count // 2)][timestamp_field_name] = timestamp_min
            jobs[randrange(job_count // 2, job_count)][timestamp_field_name] = timestamp_max

    for job in jobs:
        job["taskRunStatus"] = choice(TASK_RUN_STATUS_VALUES)

    return jobs
