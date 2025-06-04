# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Functions for monitoring job status and retrieving job information.
"""

from __future__ import annotations
import datetime
import time
from typing import List, Dict, Any, Optional, Callable
from configparser import ConfigParser
from dataclasses import dataclass

from botocore.exceptions import ClientError

from deadline.client.exceptions import DeadlineOperationError, DeadlineOperationTimedOut
from deadline.client.api._session import (
    get_boto3_client,
    get_queue_user_boto3_session,
    get_user_and_identity_store_id,
)


@dataclass
class FailedTask:
    """
    Represents a failed task in a job.
    """

    step_id: str
    task_id: str
    step_name: str
    parameters: Dict[str, Any]
    session_id: Optional[str] = None


@dataclass
class JobCompletionResult:
    """
    Result of waiting for a job to complete.
    """

    status: str
    failed_tasks: List[FailedTask]
    elapsed_time: float


@dataclass
class LogEvent:
    """
    Represents a single log event from CloudWatch Logs.
    """

    timestamp: datetime.datetime
    message: str
    ingestion_time: Optional[datetime.datetime] = None
    event_id: Optional[str] = None


@dataclass
class SessionLogResult:
    """
    Result of retrieving logs for a session.
    """

    events: List[LogEvent]
    next_token: Optional[str]
    log_group: str
    log_stream: str
    count: int


def wait_for_job_completion(
    farm_id: str,
    queue_id: str,
    job_id: str,
    max_poll_interval: int = 120,
    timeout: int = 0,
    config: Optional[ConfigParser] = None,
    status_callback: Optional[Callable] = None,
) -> JobCompletionResult:
    """
    Wait for a job to complete and return information about its status and any failed tasks.

    This function blocks until the job's taskRunStatus reaches a terminal state
    (SUCCEEDED, FAILED, CANCELED, SUSPENDED, or NOT_COMPATIBLE), then returns a JobCompletionResult
    object containing the final status and any failed tasks.

    The function uses exponential backoff for polling, starting at 0.5 seconds and doubling
    the interval after each check until it reaches the maximum polling interval.

    Args:
        farm_id: The ID of the farm containing the job.
        queue_id: The ID of the queue containing the job.
        job_id: The ID of the job to wait for.
        max_poll_interval: Maximum time in seconds between status checks (default: 120).
        timeout: Maximum time in seconds to wait (0 for no timeout).
        config: Optional configuration object.
        status_callback: Optional callback function that receives the current status during polling.

    Returns:
        A JobCompletionResult object containing the job's final status and any failed tasks.

    Raises:
        DeadlineOperationError: If the timeout is reached or there's an error retrieving job information.
    """
    deadline = get_boto3_client("deadline", config=config)

    start_time = datetime.datetime.now()
    terminal_states = ["SUCCEEDED", "FAILED", "CANCELED", "SUSPENDED", "NOT_COMPATIBLE"]
    status = ""

    # Initial polling interval of 0.5 seconds
    current_interval = 0.5

    while True:
        # Check timeout
        if timeout > 0:
            elapsed = (datetime.datetime.now() - start_time).total_seconds()
            if elapsed > timeout:
                raise DeadlineOperationTimedOut(
                    f"Timeout waiting for job {job_id} to complete after {elapsed:.1f} seconds"
                )

        # Get job status
        try:
            job = deadline.get_job(farmId=farm_id, queueId=queue_id, jobId=job_id)
            status = job.get("taskRunStatus", "")

            # Call the status callback if provided with elapsed time and timeout info
            if status_callback:
                elapsed = (datetime.datetime.now() - start_time).total_seconds()
                status_callback(status, elapsed, timeout)

        except ClientError as exc:
            raise DeadlineOperationError(f"Failed to get job status: {exc}") from exc

        if status in terminal_states:
            break

        # Sleep using current interval
        time.sleep(current_interval)

        # Exponential backoff with a maximum interval
        current_interval = min(current_interval * 2, max_poll_interval)

    elapsed_time = (datetime.datetime.now() - start_time).total_seconds()

    # If job failed, collect failed tasks
    failed_tasks = []
    if status != "SUCCEEDED":
        try:
            # Get all steps with pagination
            paginator = deadline.get_paginator("list_steps")
            for page in paginator.paginate(farmId=farm_id, queueId=queue_id, jobId=job_id):
                # For each step, get tasks and filter for failed ones client-side
                for step in page["steps"]:
                    step_id = step["stepId"]
                    step_name = step.get("name", "")

                    # Only query for tasks if the step has any failed tasks
                    if step.get("taskRunStatusCounts", {}).get("FAILED", 0) > 0:
                        # Get all tasks with pagination
                        task_paginator = deadline.get_paginator("list_tasks")
                        for tasks_page in task_paginator.paginate(
                            farmId=farm_id, queueId=queue_id, jobId=job_id, stepId=step_id
                        ):
                            # Filter failed tasks client-side
                            for task in tasks_page["tasks"]:
                                if task.get("runStatus") == "FAILED":
                                    # Extract session ID from latestSessionActionId
                                    session_id = None
                                    latest_session_action_id = task.get("latestSessionActionId")
                                    if latest_session_action_id:
                                        # Format is typically "sessionaction-{session_id}-{action_number}"
                                        # Extract the session ID part
                                        parts = latest_session_action_id.split("-")
                                        if len(parts) >= 3 and parts[0] == "sessionaction":
                                            session_id = f"session-{parts[1]}"

                                    failed_tasks.append(
                                        FailedTask(
                                            step_id=step_id,
                                            task_id=task["taskId"],
                                            step_name=step_name,
                                            parameters=task.get("parameters", {}),
                                            session_id=session_id,
                                        )
                                    )
        except ClientError as exc:
            raise DeadlineOperationError(f"Failed to retrieve failed tasks: {exc}") from exc

    return JobCompletionResult(status=status, failed_tasks=failed_tasks, elapsed_time=elapsed_time)


def get_session_logs(
    farm_id: str,
    queue_id: str,
    session_id: str,
    limit: int = 100,
    start_time: Optional[datetime.datetime] = None,
    end_time: Optional[datetime.datetime] = None,
    next_token: Optional[str] = None,
    config: Optional[ConfigParser] = None,
) -> SessionLogResult:
    """
    Get CloudWatch logs for a specific session.

    This function retrieves logs from CloudWatch for the specified session ID.
    By default, it returns the most recent 100 log lines, but this can be
    adjusted using the limit parameter.

    Args:
        farm_id: The ID of the farm containing the session.
        queue_id: The ID of the queue containing the session.
        session_id: The ID of the session to get logs for.
        limit: Maximum number of log lines to return.
        start_time: Optional start time for logs as a datetime object.
        end_time: Optional end time for logs as a datetime object.
        next_token: Optional token for pagination of results.
        config: Optional configuration object.

    Returns:
        A SessionLogResult object containing the log events and metadata.

    Raises:
        DeadlineOperationError: If there's an error retrieving the logs.
    """
    # Get the Deadline client to use for getting queue credentials
    deadline = get_boto3_client("deadline", config=config)

    # Check if we have user and identity store ID (from Deadline Cloud monitor)
    user_id, identity_store_id = get_user_and_identity_store_id(config=config)

    # Create logs client - either with queue credentials or directly
    if user_id and identity_store_id:
        # Get a session with queue user credentials
        try:
            queue_session = get_queue_user_boto3_session(
                deadline=deadline, config=config, farm_id=farm_id, queue_id=queue_id
            )
            logs_client = queue_session.client("logs")
        except Exception as e:
            raise DeadlineOperationError(f"Failed to get queue credentials: {e}")
    else:
        # Use the same boto session as for deadline
        logs_client = get_boto3_client("logs", config=config)

    # Construct the log group name
    log_group_name = f"/aws/deadline/{farm_id}/{queue_id}"

    # Prepare parameters for GetLogEvents
    params = {
        "logGroupName": log_group_name,
        "logStreamName": session_id,
        "limit": limit,
        "startFromHead": False,  # Get the most recent logs first
    }

    # Add next_token if provided
    if next_token:
        params["nextToken"] = next_token

    # Add optional time parameters if provided
    if start_time:
        try:
            # Convert datetime to milliseconds since epoch
            start_timestamp = int(start_time.timestamp() * 1000)
            params["startTime"] = start_timestamp
        except (ValueError, AttributeError) as e:
            raise DeadlineOperationError(f"Invalid start time: {e}")

    if end_time:
        try:
            # Convert datetime to milliseconds since epoch
            end_timestamp = int(end_time.timestamp() * 1000)
            params["endTime"] = end_timestamp
        except (ValueError, AttributeError) as e:
            raise DeadlineOperationError(f"Invalid end time: {e}")

    try:
        response = logs_client.get_log_events(**params)

        # Convert to strongly typed objects
        events = []
        for event in response.get("events", []):
            events.append(
                LogEvent(
                    timestamp=datetime.datetime.fromtimestamp(
                        event["timestamp"] / 1000, tz=datetime.timezone.utc
                    ),
                    message=event["message"].rstrip(),
                    ingestion_time=(
                        datetime.datetime.fromtimestamp(event["ingestionTime"] / 1000)
                        if "ingestionTime" in event
                        else None
                    ),
                    event_id=event.get("eventId"),
                )
            )

        return SessionLogResult(
            events=events,
            next_token=response.get("nextForwardToken"),
            log_group=log_group_name,
            log_stream=session_id,
            count=len(events),
        )

    except logs_client.exceptions.ResourceNotFoundException:
        # Return an empty result if the log group or stream doesn't exist
        return SessionLogResult(
            events=[],
            next_token=None,
            log_group=log_group_name,
            log_stream=session_id,
            count=0,
        )
    except Exception as e:
        raise DeadlineOperationError(f"Failed to retrieve logs: {e}")
