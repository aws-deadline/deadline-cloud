# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

__all__ = ["_list_jobs_by_filter_expression"]

from typing import Any
import boto3

from deadline.client.api._session import get_default_client_config
from botocore.exceptions import ClientError
from deadline.client.exceptions import DeadlineOperationError


class JobFetchFailure(RuntimeError):
    """
    Failure fetching jobs updated since timestamp
    """


def _list_jobs_by_filter_expression(
    boto3_session: boto3.Session,
    farm_id: str,
    queue_id: str,
    filter_expression: dict[str, Any],
) -> list[dict[str, Any]]:
    """
    This function retrieves all jobs in the queue that satisfy a provided filter expression, except potentially
    some jobs updated recently due to eventual consistency. The value is the same as the boto3 filterExpression to
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/deadline/client/search_jobs.html,
    except it may not contain a nested groupFilter.

    CAUTION:
        Eventual consistency in the deadline:SearchJobs API means that the result set can be missing jobs
        with a timestamp close to the current time.

    TODO: This is an experimental function under development, and is exposed under the internal-only name
          deadline.client.api._list_jobs_by_filter_expression._list_jobs_by_filter_expression.
          If it proves useful, deadline.client.api.list_jobs_active is the likely public function.

    NOTE:
        There's an edge case where 100 jobs with identical createdAt timestamps will cause the function to raise
        a JobFetchFailure exception. Because this would require all 100 jobs be created/updated with
        identical timestamp recorded at millisecond precision, we do not expect this to occur in practice.

    Example:
        boto3_session = boto3.Session()
        farm_id = ...
        queue_id = ...
        saved_timestamp = ...

        # Get the jobs created in the last 5 minutes
        all_active_jobs = _list_jobs_by_filter_expression(
            boto3_session,
            farm_id,
            queue_id,
        )

    Args:
      boto3_session (boto3.Session): The boto3 Session for AWS API access.
      farm_id (str): The Farm ID.
      queue_id (str): The Queue ID.
      filter_expressions (dict[str, Any]): The filter expression to apply to jobs. This is nested one level in a
            filter expression provided to deadline:SearchJobs, so cannot include a groupFilter.

    Returns:
      The list of all jobs in the queue that satisfy the provided filter expression. Each job is as returned by the deadline:SearchJobs API.
    """
    # This function uses deadline:SearchJobs as a primitive to union subsets of up to 100 jobs at a time,
    # designed to guarantee we end up with the full correct set. Ordering and thresholding by the createdAt
    # timestamp ensures no jobs get missed as might occur if we incremented itemOffset instead.
    #
    # Let J = {j ∈ queue | filter_expression(j)} be the set we want
    #     J' = union {J_i | i ∈ range(search_count)} be the set the algorithm produces,
    #            where J_0 = {j ∈ queue | filter_expression(j)} [limit 100, ordered by createdAt asc]
    #                  J_i = {j ∈ queue | filter_expression(j) && j["createdAt"] >= timestamp_i} [when i > 0, limit 100, ordered by createdAt asc]
    #                  timestamp_i = max {j["timestamp_field_name"]: j in J_{i-1}} [when i >= 1]
    #
    # To prove these sets are equal, we show they are each a subset of the other.
    #
    # J' is a subset of J, because each J_i is from a deadline:SearchJobs that filters to a subset of J.
    #
    # Consider a job j ∈ J. The createdAt timestamp of a job is always available, and never changes. By construction,
    # the set J_0 is equivalent to {j ∈ queue | filter_expression(j) && j["createdAt"] <= timestamp_1}, and each
    # subsequent J_i is equivalent to {j ∈ queue | filter_expression(j) && timestamp_i <= j["createdAt"] <= timestamp_[i+1]},
    # except for when there are more than 100 jobs with identical createdAt timestamp, in which case the algorithm
    # raises an exception. All of the createdAt timestamps in J are within the range of timestamps covered by these sets,
    # so therefore J is also a subset of J'.
    #
    # The value of filter_expression(j) may change between subsequent calls to deadline:SearchJob. If it does, the job
    # is within the eventual consistency window.

    # Do some basic parameter validation on filter_expression. The rest is left up to the deadline:SearchJobs API handler.
    if not isinstance(filter_expression, dict):
        raise ValueError("The provided filter expression must be a dict")

    if sorted(filter_expression.keys()) != ["filters", "operator"]:
        raise ValueError(
            f"The provided filter expression must contain 'filters' and 'operator', got {sorted(filter_expression.keys())}"
        )

    # This holds {job_id: job_from_search_jobs_call, ...}
    result_jobs = {}

    deadline = boto3_session.client("deadline", config=get_default_client_config())

    # Sort jobs in ascending order of the timestamp field
    sort_expressions = [{"fieldSort": {"name": "CREATED_AT", "sortOrder": "ASCENDING"}}]

    # Filter for any of the active statuses READY, ASSIGNED, STARTING, SCHEDULED, or RUNNING
    provided_filter = {
        "groupFilter": filter_expression,
    }

    # The first time we call deadline.search_jobs, there is no timestamp filter so it
    # will return the earliest jobs satisfying the filter, ordered by createdAt.
    query_filter_expressions = {
        "filters": [provided_filter],
        "operator": "AND",
    }

    # Continue until we've processed all jobs
    while True:
        try:
            # The pageSize defaults to its maximum, 100, so we leave it out of the call.
            response = deadline.search_jobs(
                farmId=farm_id,
                queueIds=[queue_id],
                itemOffset=0,
                filterExpressions=query_filter_expressions,
                sortExpressions=sort_expressions,
            )
        except ClientError as exc:
            raise DeadlineOperationError(f"Failed to get Jobs from Deadline:\n{exc}") from exc

        # This is up to the first 100 of jobs that satisfy the query
        jobs = response.get("jobs", [])
        # This is the total number of jobs that satisfied the query
        total_results = response.get("totalResults", 0)

        result_jobs.update({job["jobId"]: job for job in jobs})

        if len(jobs) == total_results:
            # If the jobs we got are the total results, result_jobs is now the full set
            break
        elif jobs[0]["createdAt"] == jobs[-1]["createdAt"]:
            # Rare edge case where the timestamp field is the same for all 100 jobs in the page, that
            # we expect to never see in practice. The timestamp value is stored with
            # millisecond precision, and jobs are scheduled independently from each other,
            # with updates of running jobs generally being multiple seconds apart.
            raise JobFetchFailure(
                "Failure fetching jobs based on the createdAt field as more then 100 jobs have the exact same timestamp value."
            )
        else:
            # Continue processing from the largest timestamp value we saw so far
            threshold_timestamp = jobs[-1]["createdAt"]

            # Update jobs to have job["createdAt"] >= threshold_timestamp
            query_filter_expressions = {
                "filters": [
                    provided_filter,
                    {
                        "dateTimeFilter": {
                            "name": "CREATED_AT",
                            "dateTime": threshold_timestamp,
                            "operator": "GREATER_THAN_EQUAL_TO",
                        }
                    },
                ],
                "operator": "AND",
            }

    return list(result_jobs.values())
