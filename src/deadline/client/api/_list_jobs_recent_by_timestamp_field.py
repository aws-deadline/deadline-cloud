# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

__all__ = ["_list_jobs_recent_by_timestamp_field"]

from typing import Any, Optional
import boto3

from deadline.client.api._session import get_default_client_config
from botocore.exceptions import ClientError
from datetime import datetime, timedelta, timezone
from deadline.client.exceptions import DeadlineOperationError


class JobFetchFailure(RuntimeError):
    """
    Failure fetching jobs updated since timestamp
    """


def _list_jobs_recent_by_timestamp_field(
    boto3_session: boto3.Session,
    farm_id: str,
    queue_id: str,
    timestamp_field_name: str,
    *,
    timestamp: Optional[datetime] = None,
    look_back_duration: Optional[timedelta] = None,
) -> list[dict[str, Any]]:
    """
    This function retrieves jobs that have a value for `timestamp_field_name` timestamp equal or newer than
    the provided timestamp. It returns a list of jobs in the form returned by the deadline:SearchJobs API.

    CAUTION:
        Eventual consistency in the deadline:SearchJobs API means that the result set can be missing jobs
        with a timestamp close to the current time. If you are repeatedly calling this function to collect
        all jobs over time, use a sufficiently large overlap window to account for this eventual consistency.

    TODO: This is an experimental function under development, and is exposed under the internal-only name
          deadline.client.api._list_jobs_recent_by_timestamp_field._list_jobs_recent_by_timestamp_field.
          If it proves useful, deadline.client.api.list_jobs_recent_by_timestamp_field is the likely public function.

    NOTE:
        There's an edge case where 100 jobs with identical timestamps will cause the function to raise
        a JobFetchFailure exception. Because this would require all 100 jobs be created/updated with
        identical timestamp recorded at millisecond precision, we do not expect this to occur in practice.

    Example:
        boto3_session = boto3.Session()
        farm_id = ...
        queue_id = ...
        saved_timestamp = ...

        # Get the jobs created in the last 5 minutes
        since_5minutes = _list_jobs_recent_by_timestamp_field(
            boto3_session,
            farm_id,
            queue_id,
            "createdAt",
            look_back_duration=timedelta(minutes=5)
        )

        # Continuously get jobs created, querying every 5 minutes
        job_ids = set()
        timestamp = <starting_timestamp>
        while True:
            # Determine the next threshold timestamp before calling the function
            # using a 90 second overlap to account for eventual consistency.
            next_timestamp = datetime.now(timezone.utc) - timedelta(seconds=90)

            jobs = _list_jobs_recent_by_timestamp_field(
                boto3_session,
                farm_id,
                queue_id,
                "endedAt",
                timestamp=timestamp
            )
            # Jobs can be seen more than once, so add them to a set
            job_ids.update(job["jobId"] for job in jobs)

            # Sleep for 5 minutes
            time.sleep(5*60)


    Args:
      boto3_session (boto3.Session): The boto3 Session for AWS API access.
      farm_id (str): The Farm ID.
      queue_id (str): The Queue ID.
      timestamp_field_name (str): The name of the job timestamp field to use. Can be
            "createdAt", "endedAt", "startedAt", or "updatedAt". See
            https://docs.aws.amazon.com/deadline-cloud/latest/APIReference/API_GetJob.html#API_GetJob_ResponseSyntax
      timestamp (Optional[datetime]): The timestamp from which to retrieve jobs. Either timestamp
            or look_back_duration must be provided.
      look_back_duration (Optional[timedelta]): A timedelta subtracted from the current time to
            get the timestamp from which to retrieve jobs. Either timestamp or look_back_duration
            must be provided.

    Returns:
      The list of all jobs in the queue whose timestamp field is equal or greater than the provided
      timestamp. Each job is as returned by the deadline:SearchJobs API.
    """
    # This function gets the full set of jobs whose timestamp field is newer than the threshold timestamp,
    # except potentially jobs whose timestamp is near datetime.now(), due to eventual consistency.
    # It uses deadline:SearchJobs as a primitive, unioning subsets of up to 100 jobs at a time,
    # designed to guarantee we end up with the full correct set.
    #
    # Let J = {j ∈ queue | j["timestamp_field_name"] >= threshold_timestamp} be the set we want
    #     J' = union {J_i | i ∈ range(search_count)} be the set the algorithm produces,
    #            where J_i = {j ∈ queue | j["timestamp_field_name"] >= timestamp_i} [limit 100, ordered by timestamp_field_name asc]
    #                  timestamp_0 = input_timestamp
    #                  timestamp_i = max {j["timestamp_field_name"]: j in J_{i-1}}
    #
    # To prove these sets are equal, we show they are each a subset of the other.
    #
    # J' is a subset of J, because each J_i is from a deadline:SearchJobs that filters to a subset of J.
    #
    # Consider a job j ∈ J. A property of the system is that if the timestamp field is updated, it will be updated to the current time.
    #    1. If the timestamp field of j is not updated during the algorithm, and is visible to deadline:SearchJobs, it will be
    #       in one of the sets J_i because by construction the union of the query time intervals
    #       cover the time interval for J. If it is not visible to deadline:SearchJobs, its timestamp is in the eventual
    #       consistency window close to datetime.now().
    #    2. If j has the timestamp field created or modified during the algorithm, its timestamp will be set to the current time.
    #       and depending on how quickly the deadline:SearchJobs API updates, it may appear in a later set J_n. Eventual
    #       consistency of deadline:SearchJobs means it may not end up in J.
    #
    # These are all the possibilities, so therefore j is in J' and J is a subset of J' except for some jobs whose timestamps
    # are within the eventual consistency window close to datetime.now().

    # This holds {job_id: job_from_search_jobs_call, ...}
    result_jobs = {}

    # Initialize threshold timestamp with the input timestamp/timedelta
    if timestamp is not None and look_back_duration is not None:
        raise ValueError("Only one of timestamp and look_back_duration may be provided.")
    elif timestamp is not None:
        if timestamp.tzinfo is None:
            raise ValueError(f"The input timestamp {timestamp} is missing a required time zone.")
        threshold_timestamp = timestamp
    elif look_back_duration is not None:
        threshold_timestamp = datetime.now(timezone.utc) - look_back_duration
    else:
        raise ValueError("One of timestamp or look_back_duration must be provided.")

    permitted_field_names = ["createdAt", "endedAt", "startedAt", "updatedAt"]
    if timestamp_field_name not in permitted_field_names:
        raise ValueError(
            f"The provided timestamp field name {timestamp_field_name!r} is not one of {permitted_field_names}"
        )

    deadline = boto3_session.client("deadline", config=get_default_client_config())

    # The endedAt field is called ENDED_AT in sort and filter expressions
    expression_field_name = timestamp_field_name.replace("At", "_at").upper()

    # Sort jobs in ascending order of the timestamp field
    sort_expressions = [{"fieldSort": {"name": expression_field_name, "sortOrder": "ASCENDING"}}]

    # Continue until we've processed all jobs
    while True:
        # Filter jobs to have job[timestamp_field_name] >= threshold_timestamp
        filter_expressions = {
            "filters": [
                {
                    "dateTimeFilter": {
                        "name": expression_field_name,
                        "dateTime": threshold_timestamp,
                        "operator": "GREATER_THAN_EQUAL_TO",
                    }
                },
            ],
            "operator": "AND",
        }

        try:
            # The pageSize defaults to its maximum, 100, so we leave it out of the call.
            response = deadline.search_jobs(
                farmId=farm_id,
                queueIds=[queue_id],
                itemOffset=0,
                filterExpressions=filter_expressions,
                sortExpressions=sort_expressions,
            )
        except ClientError as exc:
            raise DeadlineOperationError(f"Failed to get Jobs from Deadline:\n{exc}") from exc

        # This is up to the first 100 of jobs that satisfy the query
        jobs = response.get("jobs", [])
        # This is the total number of jobs that satisfied the query
        total_results = response.get("totalResults", 0)

        # Some jobs may not have the requested time stamp field.
        jobs = [job for job in jobs if timestamp_field_name in job]

        result_jobs.update({job["jobId"]: job for job in jobs})

        if len(jobs) == total_results:
            # If the jobs we got are the total results, result_jobs is now the full set
            break
        elif jobs[0][timestamp_field_name] == jobs[-1][timestamp_field_name]:
            # Rare edge case where the timestamp field is the same for all 100 jobs in the page, that
            # we expect to never see in practice. The timestamp value is stored with
            # millisecond precision, and jobs are scheduled independently from each other,
            # with updates of running jobs generally being multiple seconds apart.
            raise JobFetchFailure(
                f"Failure fetching recent jobs based on the {timestamp_field_name} field as more then 100 jobs have the exact same timestamp value."
            )
        else:
            # Continue processing from the largest timestamp value we saw so far
            threshold_timestamp = jobs[-1][timestamp_field_name]

    return list(result_jobs.values())
