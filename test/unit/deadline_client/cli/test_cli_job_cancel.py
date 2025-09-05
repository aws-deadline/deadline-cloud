# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for the CLI job cancel command.
"""

import pytest
from unittest.mock import patch

import click
from click.testing import CliRunner

from deadline.client.cli import main
from ..shared_constants import (
    MOCK_FARM_ID,
    MOCK_JOB_ID,
    MOCK_QUEUE_ID,
)


def add_mocks_for_job_cancel(deadline_mock):
    """
    Adds mock return values to the deadline_mock for sharing across
    the different 'deadline job cancel' tests.
    """
    # These mock returns only contain the properties that job cancel needs
    deadline_mock.get_job.return_value = {
        "jobId": MOCK_JOB_ID,
        "name": "Test Job Name",
        "taskRunStatus": "RUNNING",
        "taskRunStatusCounts": {
            "SUCCEEDED": 2,
            "RUNNING": 3,
            "FAILED": 1,
            "SUSPENDED": 0,  # Will be filtered out in display
            "CANCELED": 0,  # Will be filtered out in display
        },
        "startedAt": "2024-01-01T10:00:00Z",
        "endedAt": "",
        "createdBy": "test-user",
        "createdAt": "2024-01-01T09:00:00Z",
    }
    deadline_mock.update_job.return_value = {}


def test_cli_job_cancel_default(fresh_deadline_config, deadline_mock):
    """
    Tests that 'deadline job cancel' cancels a job with default CANCELED mark-as status.
    """
    add_mocks_for_job_cancel(deadline_mock)

    with patch.object(click, "confirm") as mock_confirm:
        mock_confirm.return_value = True
        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "job",
                "cancel",
                "--farm-id",
                MOCK_FARM_ID,
                "--queue-id",
                MOCK_QUEUE_ID,
                "--job-id",
                MOCK_JOB_ID,
            ],
        )

    # Verify the job summary output (filtered taskRunStatusCounts with no zero values)
    expected_output = f"""name: Test Job Name
jobId: {MOCK_JOB_ID}
taskRunStatus: RUNNING
taskRunStatusCounts:
  SUCCEEDED: 2
  RUNNING: 3
  FAILED: 1
startedAt: '2024-01-01T10:00:00Z'
endedAt: ''
createdBy: test-user
createdAt: '2024-01-01T09:00:00Z'

Canceling job...
"""

    assert result.output == expected_output
    assert result.exit_code == 0

    # Verify confirmation prompt appears with correct message for CANCELED status
    mock_confirm.assert_called_once_with("Are you sure you want to cancel this job?", default=None)

    # Verify deadline.get_job() and deadline.update_job() are called with correct parameters
    deadline_mock.get_job.assert_called_once_with(
        farmId=MOCK_FARM_ID, queueId=MOCK_QUEUE_ID, jobId=MOCK_JOB_ID
    )
    deadline_mock.update_job.assert_called_once_with(
        farmId=MOCK_FARM_ID,
        queueId=MOCK_QUEUE_ID,
        jobId=MOCK_JOB_ID,
        targetTaskRunStatus="CANCELED",
    )


def test_cli_job_cancel_user_declines(fresh_deadline_config, deadline_mock):
    """
    Tests that 'deadline job cancel' exits with code 1 and displays appropriate message when user declines.
    """
    add_mocks_for_job_cancel(deadline_mock)

    with patch.object(click, "confirm") as mock_confirm:
        mock_confirm.return_value = False
        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "job",
                "cancel",
                "--farm-id",
                MOCK_FARM_ID,
                "--queue-id",
                MOCK_QUEUE_ID,
                "--job-id",
                MOCK_JOB_ID,
            ],
        )

    # Verify the job summary output followed by "Job not canceled." message
    expected_output = f"""name: Test Job Name
jobId: {MOCK_JOB_ID}
taskRunStatus: RUNNING
taskRunStatusCounts:
  SUCCEEDED: 2
  RUNNING: 3
  FAILED: 1
startedAt: '2024-01-01T10:00:00Z'
endedAt: ''
createdBy: test-user
createdAt: '2024-01-01T09:00:00Z'

Job not canceled.
"""

    assert result.output == expected_output
    assert result.exit_code == 1

    # Verify confirmation prompt appears
    mock_confirm.assert_called_once_with("Are you sure you want to cancel this job?", default=None)

    # Verify deadline.get_job() is called but deadline.update_job() is NOT called
    deadline_mock.get_job.assert_called_once_with(
        farmId=MOCK_FARM_ID, queueId=MOCK_QUEUE_ID, jobId=MOCK_JOB_ID
    )
    deadline_mock.update_job.assert_not_called()


def test_cli_job_cancel_with_yes_flag(fresh_deadline_config, deadline_mock):
    """
    Tests that 'deadline job cancel' with --yes flag skips confirmation prompt and cancels successfully.
    """
    add_mocks_for_job_cancel(deadline_mock)

    with patch.object(click, "confirm") as mock_confirm:
        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "job",
                "cancel",
                "--farm-id",
                MOCK_FARM_ID,
                "--queue-id",
                MOCK_QUEUE_ID,
                "--job-id",
                MOCK_JOB_ID,
                "--yes",
            ],
        )

    # Verify the job summary output and cancellation message
    expected_output = f"""name: Test Job Name
jobId: {MOCK_JOB_ID}
taskRunStatus: RUNNING
taskRunStatusCounts:
  SUCCEEDED: 2
  RUNNING: 3
  FAILED: 1
startedAt: '2024-01-01T10:00:00Z'
endedAt: ''
createdBy: test-user
createdAt: '2024-01-01T09:00:00Z'

Canceling job...
"""

    assert result.output == expected_output
    assert result.exit_code == 0

    # Verify confirmation prompt is skipped when --yes flag is used
    mock_confirm.assert_not_called()

    # Verify deadline.get_job() and deadline.update_job() are called with correct parameters
    deadline_mock.get_job.assert_called_once_with(
        farmId=MOCK_FARM_ID, queueId=MOCK_QUEUE_ID, jobId=MOCK_JOB_ID
    )
    deadline_mock.update_job.assert_called_once_with(
        farmId=MOCK_FARM_ID,
        queueId=MOCK_QUEUE_ID,
        jobId=MOCK_JOB_ID,
        targetTaskRunStatus="CANCELED",
    )


@pytest.mark.parametrize("mark_as", ["CANCELED", "SUSPENDED", "FAILED", "SUCCEEDED"])
def test_cli_job_cancel_mark_as_status(mark_as, fresh_deadline_config, deadline_mock):
    """
    Tests that 'deadline job cancel' with different --mark-as status options works correctly.
    """
    add_mocks_for_job_cancel(deadline_mock)

    with patch.object(click, "confirm") as mock_confirm:
        mock_confirm.return_value = True
        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "job",
                "cancel",
                "--farm-id",
                MOCK_FARM_ID,
                "--queue-id",
                MOCK_QUEUE_ID,
                "--job-id",
                MOCK_JOB_ID,
                "--mark-as",
                mark_as,
            ],
        )

    # Verify the job summary output (filtered taskRunStatusCounts with no zero values)
    job_summary = f"""name: Test Job Name
jobId: {MOCK_JOB_ID}
taskRunStatus: RUNNING
taskRunStatusCounts:
  SUCCEEDED: 2
  RUNNING: 3
  FAILED: 1
startedAt: '2024-01-01T10:00:00Z'
endedAt: ''
createdBy: test-user
createdAt: '2024-01-01T09:00:00Z'

"""

    # Verify correct status message is displayed during cancellation
    if mark_as == "CANCELED":
        status_message = "Canceling job..."
        confirmation_message = "Are you sure you want to cancel this job?"
    else:
        status_message = f"Canceling job and marking as {mark_as}..."
        confirmation_message = (
            f"Are you sure you want to cancel this job and mark its taskRunStatus as {mark_as}?"
        )

    expected_output = job_summary + status_message + "\n"

    assert result.output == expected_output
    assert result.exit_code == 0

    # Verify appropriate confirmation message for each status type
    mock_confirm.assert_called_once_with(confirmation_message, default=None)

    # Verify correct targetTaskRunStatus parameter is passed to deadline.update_job()
    deadline_mock.get_job.assert_called_once_with(
        farmId=MOCK_FARM_ID, queueId=MOCK_QUEUE_ID, jobId=MOCK_JOB_ID
    )
    deadline_mock.update_job.assert_called_once_with(
        farmId=MOCK_FARM_ID, queueId=MOCK_QUEUE_ID, jobId=MOCK_JOB_ID, targetTaskRunStatus=mark_as
    )
