# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for the job monitoring API functions.
"""

import datetime
from unittest.mock import patch, MagicMock
import pytest

from deadline.client.api._job_monitoring import (
    wait_for_job_completion,
    get_session_logs,
    JobCompletionResult,
    SessionLogResult,
)
from deadline.client.exceptions import DeadlineOperationError

from ..shared_constants import (
    MOCK_FARM_ID,
    MOCK_JOB_ID,
    MOCK_QUEUE_ID,
)

MOCK_JOB_RUNNING = {
    "jobId": MOCK_JOB_ID,
    "name": "Test Job",
    "taskRunStatus": "RUNNING",
    "lifecycleStatus": "ACTIVE",
    "createdBy": "test-user",
    "createdAt": datetime.datetime(2023, 1, 27, 7, 34, 41),
    "startedAt": datetime.datetime(2023, 1, 27, 7, 37, 53),
}

MOCK_JOB_SUCCEEDED = {
    "jobId": MOCK_JOB_ID,
    "name": "Test Job",
    "taskRunStatus": "SUCCEEDED",
    "lifecycleStatus": "ACTIVE",
    "createdBy": "test-user",
    "createdAt": datetime.datetime(2023, 1, 27, 7, 34, 41),
    "startedAt": datetime.datetime(2023, 1, 27, 7, 37, 53),
    "endedAt": datetime.datetime(2023, 1, 27, 7, 39, 17),
}

MOCK_JOB_FAILED = {
    "jobId": MOCK_JOB_ID,
    "name": "Test Job",
    "taskRunStatus": "FAILED",
    "lifecycleStatus": "ACTIVE",
    "createdBy": "test-user",
    "createdAt": datetime.datetime(2023, 1, 27, 7, 34, 41),
    "startedAt": datetime.datetime(2023, 1, 27, 7, 37, 53),
    "endedAt": datetime.datetime(2023, 1, 27, 7, 39, 17),
}

MOCK_STEPS = {
    "steps": [
        {
            "stepId": "step-123",
            "name": "Step 1",
            "lifecycleStatus": "CREATE_COMPLETE",
            "taskRunStatus": "SUCCEEDED",
            "taskRunStatusCounts": {"FAILED": 0},
        },
        {
            "stepId": "step-456",
            "name": "Step 2",
            "lifecycleStatus": "CREATE_COMPLETE",
            "taskRunStatus": "FAILED",
            "taskRunStatusCounts": {"FAILED": 2},
        },
    ]
}

MOCK_TASKS = {
    "tasks": [
        {
            "taskId": "task-123",
            "runStatus": "FAILED",
            "latestSessionActionId": "sessionaction-abc123-0",
        },
        {
            "taskId": "task-456",
            "runStatus": "FAILED",
            "latestSessionActionId": "sessionaction-def456-1",
        },
        {
            "taskId": "task-789",
            "runStatus": "SUCCEEDED",
            "latestSessionActionId": "sessionaction-ghi789-2",
        },
    ]
}


def test_wait_for_job_completion_success():
    """
    Test that wait_for_job_completion works correctly when job succeeds.
    """
    with patch("deadline.client.api._job_monitoring.get_boto3_client") as mock_get_client:
        deadline_mock = MagicMock()
        mock_get_client.return_value = deadline_mock

        # First call returns RUNNING, second call returns SUCCEEDED
        deadline_mock.get_job.side_effect = [MOCK_JOB_RUNNING, MOCK_JOB_SUCCEEDED]

        # Mock time.sleep to avoid waiting in tests
        with patch("time.sleep"):
            # Mock datetime.now to simulate elapsed time
            start_time = datetime.datetime(2023, 1, 1, 12, 0, 0)
            end_time = datetime.datetime(2023, 1, 1, 12, 0, 10)

            with patch("datetime.datetime") as dt_mock:
                dt_mock.now.side_effect = [start_time, end_time]

                result = wait_for_job_completion(
                    farm_id=MOCK_FARM_ID,
                    queue_id=MOCK_QUEUE_ID,
                    job_id=MOCK_JOB_ID,
                    max_poll_interval=1,
                )

                assert isinstance(result, JobCompletionResult)
                assert result.status == "SUCCEEDED"
                assert result.elapsed_time == pytest.approx(10.0)
                assert len(result.failed_tasks) == 0

                # Verify the correct parameters were used
                deadline_mock.get_job.assert_called_with(
                    farmId=MOCK_FARM_ID, queueId=MOCK_QUEUE_ID, jobId=MOCK_JOB_ID
                )


def test_wait_for_job_completion_failure():
    """
    Test that wait_for_job_completion works correctly when job fails.
    """
    with patch("deadline.client.api._job_monitoring.get_boto3_client") as mock_get_client:
        deadline_mock = MagicMock()
        mock_get_client.return_value = deadline_mock

        # First call returns RUNNING, second call returns FAILED
        deadline_mock.get_job.side_effect = [MOCK_JOB_RUNNING, MOCK_JOB_FAILED]

        # Set up paginator mock for list_steps
        steps_paginator_mock = MagicMock()
        steps_paginator_mock.paginate.return_value = [MOCK_STEPS]

        # Set up paginator mock for list_tasks
        tasks_paginator_mock = MagicMock()
        tasks_paginator_mock.paginate.return_value = [MOCK_TASKS]

        # Configure get_paginator to return the appropriate paginator based on the operation
        def get_paginator_side_effect(operation):
            if operation == "list_steps":
                return steps_paginator_mock
            elif operation == "list_tasks":
                return tasks_paginator_mock
            return MagicMock()

        deadline_mock.get_paginator.side_effect = get_paginator_side_effect

        # Mock time.sleep to avoid waiting in tests
        with patch("time.sleep"):
            # Mock datetime.now to simulate elapsed time
            start_time = datetime.datetime(2023, 1, 1, 12, 0, 0)
            end_time = datetime.datetime(2023, 1, 1, 12, 0, 15)

            with patch("datetime.datetime") as dt_mock:
                dt_mock.now.side_effect = [start_time, end_time]

                result = wait_for_job_completion(
                    farm_id=MOCK_FARM_ID,
                    queue_id=MOCK_QUEUE_ID,
                    job_id=MOCK_JOB_ID,
                    max_poll_interval=1,
                )

                assert isinstance(result, JobCompletionResult)
                assert result.status == "FAILED"
                assert result.elapsed_time == pytest.approx(15.0)
                assert len(result.failed_tasks) == 2

                # Verify the failed tasks have the correct data
                assert result.failed_tasks[0].step_id == "step-456"
                assert result.failed_tasks[0].task_id == "task-123"
                assert result.failed_tasks[0].step_name == "Step 2"
                assert result.failed_tasks[0].session_id == "session-abc123"

                assert result.failed_tasks[1].step_id == "step-456"
                assert result.failed_tasks[1].task_id == "task-456"
                assert result.failed_tasks[1].step_name == "Step 2"
                assert result.failed_tasks[1].session_id == "session-def456"

                # Verify paginators were called with correct parameters
                deadline_mock.get_paginator.assert_any_call("list_steps")
                steps_paginator_mock.paginate.assert_called_with(
                    farmId=MOCK_FARM_ID, queueId=MOCK_QUEUE_ID, jobId=MOCK_JOB_ID
                )

                deadline_mock.get_paginator.assert_any_call("list_tasks")
                tasks_paginator_mock.paginate.assert_called_with(
                    farmId=MOCK_FARM_ID, queueId=MOCK_QUEUE_ID, jobId=MOCK_JOB_ID, stepId="step-456"
                )


def test_wait_for_job_completion_timeout():
    """
    Test that wait_for_job_completion times out correctly.
    """
    with patch("deadline.client.api._job_monitoring.get_boto3_client") as mock_get_client:
        deadline_mock = MagicMock()
        mock_get_client.return_value = deadline_mock

        # Always return RUNNING to trigger timeout
        deadline_mock.get_job.return_value = MOCK_JOB_RUNNING

        # Mock time.sleep to avoid waiting in tests
        with patch("time.sleep"):
            # Mock datetime.now to simulate time passing beyond timeout
            start_time = datetime.datetime(2023, 1, 1, 12, 0, 0)
            check_time = datetime.datetime(2023, 1, 1, 12, 0, 3)

            with patch("datetime.datetime") as dt_mock:
                dt_mock.now.side_effect = [start_time, check_time]

                with patch.object(dt_mock, "now", side_effect=[start_time, check_time]):
                    try:
                        wait_for_job_completion(
                            farm_id=MOCK_FARM_ID,
                            queue_id=MOCK_QUEUE_ID,
                            job_id=MOCK_JOB_ID,
                            max_poll_interval=1,
                            timeout=2,
                        )
                        assert False, "Expected DeadlineOperationError was not raised"
                    except DeadlineOperationError as e:
                        assert "Timeout waiting for job" in str(e)


def test_wait_for_job_completion_with_pagination():
    """
    Test that wait_for_job_completion correctly handles pagination for steps and tasks.
    """
    with patch("deadline.client.api._job_monitoring.get_boto3_client") as mock_get_client:
        deadline_mock = MagicMock()
        mock_get_client.return_value = deadline_mock

        # Return FAILED job status
        deadline_mock.get_job.return_value = MOCK_JOB_FAILED

        # Set up paginator mock for list_steps with multiple pages
        steps_page1 = {"steps": [MOCK_STEPS["steps"][0]]}
        steps_page2 = {"steps": [MOCK_STEPS["steps"][1]]}
        steps_paginator_mock = MagicMock()
        steps_paginator_mock.paginate.return_value = [steps_page1, steps_page2]

        # Set up paginator mock for list_tasks with multiple pages
        tasks_page1 = {"tasks": [MOCK_TASKS["tasks"][0]]}
        tasks_page2 = {"tasks": [MOCK_TASKS["tasks"][1], MOCK_TASKS["tasks"][2]]}
        tasks_paginator_mock = MagicMock()
        tasks_paginator_mock.paginate.return_value = [tasks_page1, tasks_page2]

        # Configure get_paginator to return the appropriate paginator based on the operation
        def get_paginator_side_effect(operation):
            if operation == "list_steps":
                return steps_paginator_mock
            elif operation == "list_tasks":
                return tasks_paginator_mock
            return MagicMock()

        deadline_mock.get_paginator.side_effect = get_paginator_side_effect

        # Mock time.sleep to avoid waiting in tests
        with patch("time.sleep"):
            # Mock datetime.now to simulate elapsed time
            start_time = datetime.datetime(2023, 1, 1, 12, 0, 0)
            end_time = datetime.datetime(2023, 1, 1, 12, 0, 5)

            with patch("datetime.datetime") as dt_mock:
                dt_mock.now.side_effect = [start_time, end_time]

                result = wait_for_job_completion(
                    farm_id=MOCK_FARM_ID,
                    queue_id=MOCK_QUEUE_ID,
                    job_id=MOCK_JOB_ID,
                    max_poll_interval=1,
                )

                assert isinstance(result, JobCompletionResult)
                assert result.status == "FAILED"
                assert result.elapsed_time == pytest.approx(5.0)
                assert len(result.failed_tasks) == 2

                # Verify paginators were called with correct parameters
                deadline_mock.get_paginator.assert_any_call("list_steps")
                steps_paginator_mock.paginate.assert_called_with(
                    farmId=MOCK_FARM_ID, queueId=MOCK_QUEUE_ID, jobId=MOCK_JOB_ID
                )

                deadline_mock.get_paginator.assert_any_call("list_tasks")
                tasks_paginator_mock.paginate.assert_called_with(
                    farmId=MOCK_FARM_ID, queueId=MOCK_QUEUE_ID, jobId=MOCK_JOB_ID, stepId="step-456"
                )


# Mock CloudWatch log events for testing
MOCK_LOG_EVENTS = [
    {
        "timestamp": 1672574400000,  # 2023-01-01 12:00:00 UTC
        "message": "Log message 1",
        "ingestionTime": 1672574410000,  # 2023-01-01 12:00:10 UTC
        "eventId": "event-1",
    },
    {
        "timestamp": 1672574460000,  # 2023-01-01 12:01:00 UTC
        "message": "Log message 2",
        "ingestionTime": 1672574470000,  # 2023-01-01 12:01:10 UTC
        "eventId": "event-2",
    },
]

# Mock CloudWatch GetLogEvents response
MOCK_GET_LOG_EVENTS_RESPONSE = {
    "events": MOCK_LOG_EVENTS,
    "nextForwardToken": "next-token",
    "nextBackwardToken": "back-token",
}


def test_wait_for_job_completion_exponential_backoff():
    """
    Test that wait_for_job_completion uses exponential backoff correctly.
    """
    with patch("deadline.client.api._job_monitoring.get_boto3_client") as mock_get_client:
        deadline_mock = MagicMock()
        mock_get_client.return_value = deadline_mock

        # Return RUNNING for several calls, then SUCCEEDED
        deadline_mock.get_job.side_effect = [
            MOCK_JOB_RUNNING,
            MOCK_JOB_RUNNING,
            MOCK_JOB_RUNNING,
            MOCK_JOB_SUCCEEDED,
        ]

        # Mock time.sleep to track the intervals
        sleep_intervals = []

        def mock_sleep(interval):
            sleep_intervals.append(interval)

        with patch("time.sleep", side_effect=mock_sleep):
            # Mock datetime.now to simulate elapsed time
            start_time = datetime.datetime(2023, 1, 1, 12, 0, 0)
            end_time = datetime.datetime(2023, 1, 1, 12, 0, 10)

            with patch("datetime.datetime") as dt_mock:
                dt_mock.now.side_effect = [start_time, end_time]

                result = wait_for_job_completion(
                    farm_id=MOCK_FARM_ID,
                    queue_id=MOCK_QUEUE_ID,
                    job_id=MOCK_JOB_ID,
                    max_poll_interval=10,
                )

                assert isinstance(result, JobCompletionResult)
                assert result.status == "SUCCEEDED"

                # Verify exponential backoff: 0.5, 1.0, 2.0
                assert len(sleep_intervals) == pytest.approx(3)
                assert sleep_intervals[0] == pytest.approx(0.5)
                assert sleep_intervals[1] == pytest.approx(1.0)
                assert sleep_intervals[2] == pytest.approx(2.0)


def test_wait_for_job_completion_max_interval_cap():
    """
    Test that wait_for_job_completion caps at max_poll_interval.
    """
    with patch("deadline.client.api._job_monitoring.get_boto3_client") as mock_get_client:
        deadline_mock = MagicMock()
        mock_get_client.return_value = deadline_mock

        # Return RUNNING for many calls, then SUCCEEDED
        deadline_mock.get_job.side_effect = [MOCK_JOB_RUNNING] * 10 + [MOCK_JOB_SUCCEEDED]

        # Mock time.sleep to track the intervals
        sleep_intervals = []

        def mock_sleep(interval):
            sleep_intervals.append(interval)

        with patch("time.sleep", side_effect=mock_sleep):
            # Mock datetime.now to simulate elapsed time
            start_time = datetime.datetime(2023, 1, 1, 12, 0, 0)
            end_time = datetime.datetime(2023, 1, 1, 12, 0, 10)

            with patch("datetime.datetime") as dt_mock:
                dt_mock.now.side_effect = [start_time, end_time]

                result = wait_for_job_completion(
                    farm_id=MOCK_FARM_ID,
                    queue_id=MOCK_QUEUE_ID,
                    job_id=MOCK_JOB_ID,
                    max_poll_interval=3,  # Small max to test capping
                )

                assert isinstance(result, JobCompletionResult)
                assert result.status == "SUCCEEDED"

                # Verify intervals cap at max_poll_interval: 0.5, 1.0, 2.0, 3.0, 3.0, ...
                assert len(sleep_intervals) == 10
                assert sleep_intervals[0] == pytest.approx(0.5)
                assert sleep_intervals[1] == pytest.approx(1.0)
                assert sleep_intervals[2] == pytest.approx(2.0)
                # All subsequent intervals should be capped at 3.0
                for i in range(3, 10):
                    assert sleep_intervals[i] == pytest.approx(3.0)


def test_get_session_logs_basic():
    """
    Test that get_session_logs works correctly with basic parameters.
    """
    with patch("deadline.client.api._job_monitoring.get_boto3_client") as mock_get_client, patch(
        "deadline.client.api._job_monitoring.get_user_and_identity_store_id"
    ) as mock_get_user:
        # Mock user and identity store ID to be None (standard credentials path)
        mock_get_user.return_value = (None, None)

        # Set up logs client mock
        logs_client_mock = MagicMock()
        logs_client_mock.get_log_events.return_value = MOCK_GET_LOG_EVENTS_RESPONSE
        mock_get_client.return_value = logs_client_mock

        # Call the function
        result = get_session_logs(
            farm_id=MOCK_FARM_ID,
            queue_id=MOCK_QUEUE_ID,
            session_id="session-test-session",
            limit=100,
        )

        # Verify the result
        assert isinstance(result, SessionLogResult)
        assert len(result.events) == 2
        assert result.events[0].message == "Log message 1"
        assert result.events[1].message == "Log message 2"
        assert result.next_token == "next-token"
        assert result.log_group == f"/aws/deadline/{MOCK_FARM_ID}/{MOCK_QUEUE_ID}"
        assert result.log_stream == "session-test-session"
        assert result.count == 2

        # Verify the logs client was called with correct parameters
        logs_client_mock.get_log_events.assert_called_once_with(
            logGroupName=f"/aws/deadline/{MOCK_FARM_ID}/{MOCK_QUEUE_ID}",
            logStreamName="session-test-session",
            limit=100,
            startFromHead=False,
        )


def test_get_session_logs_with_datetime_params():
    """
    Test that get_session_logs works correctly with datetime parameters.
    """
    with patch("deadline.client.api._job_monitoring.get_boto3_client") as mock_get_client, patch(
        "deadline.client.api._job_monitoring.get_user_and_identity_store_id"
    ) as mock_get_user:
        # Mock user and identity store ID to be None (standard credentials path)
        mock_get_user.return_value = (None, None)

        # Set up logs client mock
        logs_client_mock = MagicMock()
        logs_client_mock.get_log_events.return_value = MOCK_GET_LOG_EVENTS_RESPONSE
        mock_get_client.return_value = logs_client_mock

        # Create datetime objects for start and end times
        start_time = datetime.datetime(2023, 1, 1, 12, 0, 0)
        end_time = datetime.datetime(2023, 1, 1, 13, 0, 0)

        # Call the function with datetime parameters
        result = get_session_logs(
            farm_id=MOCK_FARM_ID,
            queue_id=MOCK_QUEUE_ID,
            session_id="session-test-session",
            limit=100,
            start_time=start_time,
            end_time=end_time,
        )

        # Verify the result
        assert isinstance(result, SessionLogResult)
        assert len(result.events) == 2

        # Verify the logs client was called with correct parameters
        logs_client_mock.get_log_events.assert_called_once_with(
            logGroupName=f"/aws/deadline/{MOCK_FARM_ID}/{MOCK_QUEUE_ID}",
            logStreamName="session-test-session",
            limit=100,
            startFromHead=False,
            startTime=int(start_time.timestamp() * 1000),
            endTime=int(end_time.timestamp() * 1000),
        )


def test_get_session_logs_with_monitor_credentials():
    """
    Test that get_session_logs works correctly with Deadline Cloud monitor credentials.
    """
    with patch("deadline.client.api._job_monitoring.get_boto3_client") as mock_get_client, patch(
        "deadline.client.api._job_monitoring.get_user_and_identity_store_id"
    ) as mock_get_user, patch(
        "deadline.client.api._job_monitoring.get_queue_user_boto3_session"
    ) as mock_get_session:
        # Mock user and identity store ID to simulate monitor credentials
        mock_get_user.return_value = ("user-123", "identity-store-456")

        # Set up queue session mock
        queue_session_mock = MagicMock()
        logs_client_mock = MagicMock()
        logs_client_mock.get_log_events.return_value = MOCK_GET_LOG_EVENTS_RESPONSE
        queue_session_mock.client.return_value = logs_client_mock
        mock_get_session.return_value = queue_session_mock

        # Set up deadline client mock
        deadline_mock = MagicMock()
        mock_get_client.return_value = deadline_mock

        # Call the function
        result = get_session_logs(
            farm_id=MOCK_FARM_ID,
            queue_id=MOCK_QUEUE_ID,
            session_id="session-test-session",
            limit=100,
        )

        # Verify the result
        assert isinstance(result, SessionLogResult)
        assert len(result.events) == 2

        # Verify the queue session was created with correct parameters
        mock_get_session.assert_called_once_with(
            deadline=deadline_mock,
            config=None,
            farm_id=MOCK_FARM_ID,
            queue_id=MOCK_QUEUE_ID,
        )

        # Verify the logs client was created from the queue session
        queue_session_mock.client.assert_called_once_with("logs")

        # Verify the logs client was called with correct parameters
        logs_client_mock.get_log_events.assert_called_once_with(
            logGroupName=f"/aws/deadline/{MOCK_FARM_ID}/{MOCK_QUEUE_ID}",
            logStreamName="session-test-session",
            limit=100,
            startFromHead=False,
        )


def test_get_session_logs_with_next_token():
    """
    Test that get_session_logs works correctly with pagination token.
    """
    with patch("deadline.client.api._job_monitoring.get_boto3_client") as mock_get_client, patch(
        "deadline.client.api._job_monitoring.get_user_and_identity_store_id"
    ) as mock_get_user:
        # Mock user and identity store ID to be None (standard credentials path)
        mock_get_user.return_value = (None, None)

        # Set up logs client mock
        logs_client_mock = MagicMock()
        logs_client_mock.get_log_events.return_value = MOCK_GET_LOG_EVENTS_RESPONSE
        mock_get_client.return_value = logs_client_mock

        # Call the function with next_token
        result = get_session_logs(
            farm_id=MOCK_FARM_ID,
            queue_id=MOCK_QUEUE_ID,
            session_id="session-test-session",
            limit=100,
            next_token="previous-token",
        )

        # Verify the result
        assert isinstance(result, SessionLogResult)
        assert result.next_token == "next-token"

        # Verify the logs client was called with correct parameters
        logs_client_mock.get_log_events.assert_called_once_with(
            logGroupName=f"/aws/deadline/{MOCK_FARM_ID}/{MOCK_QUEUE_ID}",
            logStreamName="session-test-session",
            limit=100,
            startFromHead=False,
            nextToken="previous-token",
        )


def test_get_session_logs_resource_not_found():
    """
    Test that get_session_logs handles ResourceNotFoundException correctly.
    """
    with patch("deadline.client.api._job_monitoring.get_boto3_client") as mock_get_client, patch(
        "deadline.client.api._job_monitoring.get_user_and_identity_store_id"
    ) as mock_get_user:
        # Mock user and identity store ID to be None (standard credentials path)
        mock_get_user.return_value = (None, None)

        # Set up logs client mock with ResourceNotFoundException
        logs_client_mock = MagicMock()
        logs_client_mock.exceptions.ResourceNotFoundException = type(
            "ResourceNotFoundException", (Exception,), {}
        )
        logs_client_mock.get_log_events.side_effect = (
            logs_client_mock.exceptions.ResourceNotFoundException()
        )
        mock_get_client.return_value = logs_client_mock

        # Call the function
        result = get_session_logs(
            farm_id=MOCK_FARM_ID,
            queue_id=MOCK_QUEUE_ID,
            session_id="session-test-session",
            limit=100,
        )

        # Verify the result is empty but valid
        assert isinstance(result, SessionLogResult)
        assert len(result.events) == 0
        assert result.next_token is None
        assert result.log_group == f"/aws/deadline/{MOCK_FARM_ID}/{MOCK_QUEUE_ID}"
        assert result.log_stream == "session-test-session"
        assert result.count == 0


def test_get_session_logs_invalid_datetime():
    """
    Test that get_session_logs handles invalid datetime objects correctly.
    """
    with patch("deadline.client.api._job_monitoring.get_boto3_client") as mock_get_client, patch(
        "deadline.client.api._job_monitoring.get_user_and_identity_store_id"
    ) as mock_get_user:
        # Mock user and identity store ID to be None (standard credentials path)
        mock_get_user.return_value = (None, None)

        # Set up logs client mock
        logs_client_mock = MagicMock()
        mock_get_client.return_value = logs_client_mock

        # Create an invalid datetime object (None with timestamp attribute that raises)
        invalid_datetime = MagicMock()
        invalid_datetime.timestamp.side_effect = AttributeError(
            "'NoneType' object has no attribute 'timestamp'"
        )

        # Call the function with invalid datetime and verify it raises an error
        try:
            get_session_logs(
                farm_id=MOCK_FARM_ID,
                queue_id=MOCK_QUEUE_ID,
                session_id="session-test-session",
                start_time=invalid_datetime,
            )
            assert False, "Expected DeadlineOperationError was not raised"
        except DeadlineOperationError as e:
            assert "Invalid start time" in str(e)
