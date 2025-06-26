# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for the CLI queue incremental output download command.
"""

import os
import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime

import boto3
from freezegun import freeze_time
from click.testing import CliRunner
from deadline.client.cli import main
import deadline.client
import psutil

from ..shared_constants import MOCK_FARM_ID, MOCK_QUEUE_ID, MOCK_JOB_ID
from ..mock_deadline_job_apis import (
    mock_search_jobs_for_set,
    create_fake_job_list,
    mock_get_job_for_set,
)

ISO_FREEZE_TIME = "2025-05-26 12:00:00+00:00"


# Fixtures for shared resources
@pytest.fixture(scope="module")
def checkpoint_dir(tmp_path_factory):
    """Create a checkpoint directory for all tests to use."""
    checkpoint_dir = tmp_path_factory.mktemp("checkpoint")
    yield str(checkpoint_dir)
    # No cleanup needed here as tmp_path_factory handles it automatically


@pytest.fixture(scope="module")
def boto3_session():
    """Create a mock boto3 session for all tests to use."""
    mock_session = MagicMock(spec=boto3.Session)
    mock_session.client().get_queue.return_value = {"displayName": "Mock Queue"}
    with patch.object(boto3, "Session", return_value=mock_session), patch.object(
        deadline.client.api, "get_deadline_cloud_library_telemetry_client"
    ):
        yield mock_session


@pytest.fixture
def pid_lock_file(checkpoint_dir):
    """Create a PID lock file path for tests that need it."""
    pid_file_path = os.path.join(checkpoint_dir, f"{MOCK_QUEUE_ID}_incremental_output_download.pid")
    yield pid_file_path
    # Clean up
    if os.path.exists(pid_file_path):
        os.remove(pid_file_path)


@pytest.fixture
def with_incremental_download_enabled():
    """Set the ENABLE_INCREMENTAL_OUTPUT_DOWNLOAD environment variable to 1 for testing the incremental download command."""
    os.environ["ENABLE_INCREMENTAL_OUTPUT_DOWNLOAD"] = "1"
    yield None
    del os.environ["ENABLE_INCREMENTAL_OUTPUT_DOWNLOAD"]


def test_incremental_output_download_requires_beta_acknowledgement(boto3_session, checkpoint_dir):
    # Run the CLI command once to bootstrap the operation
    runner = CliRunner()
    with freeze_time(ISO_FREEZE_TIME):
        result = runner.invoke(
            main,
            [
                "queue",
                "incremental-output-download",
                "--farm-id",
                MOCK_FARM_ID,
                "--queue-id",
                MOCK_QUEUE_ID,
                "--checkpoint-dir",
                checkpoint_dir,
            ],
        )

    # Assert the command executed successfully
    assert result.exit_code == 1, result.output

    assert (
        "The incremental-output-download command is not fully implemented. You must set the environment variable ENABLE_INCREMENTAL_OUTPUT_DOWNLOAD to 1 to acknowledge this."
        in result.output
    ), result.output


def test_incremental_output_download_simple_success(
    with_incremental_download_enabled, boto3_session, checkpoint_dir
):
    """Test successful execution of incremental_output_download"""
    mock_jobs = create_fake_job_list(1)
    mock_jobs[0]["name"] = "Mock Job"
    mock_jobs[0]["jobId"] = MOCK_JOB_ID
    mock_jobs[0]["taskRunStatus"] = "READY"
    mock_jobs[0]["taskRunStatusCounts"] = {
        "SUCCEEDED": 1,
        "READY": 1,
    }
    mock_jobs[0]["attachments"] = {
        "manifests": [
            {"rootPath": "/", "rootPathFormat": "posix", "outputRelativeDirectories": ["."]}
        ],
        "fileSystem": "VIRTUAL",
    }
    del mock_jobs[0]["endedAt"]
    boto3_session.client().search_jobs = mock_search_jobs_for_set(
        MOCK_FARM_ID, MOCK_QUEUE_ID, mock_jobs
    )
    boto3_session.client().get_job = mock_get_job_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, mock_jobs)

    # Run the CLI command once to bootstrap the operation
    runner = CliRunner()
    with freeze_time(ISO_FREEZE_TIME):
        result = runner.invoke(
            main,
            [
                "queue",
                "incremental-output-download",
                "--farm-id",
                MOCK_FARM_ID,
                "--queue-id",
                MOCK_QUEUE_ID,
                "--checkpoint-dir",
                checkpoint_dir,
            ],
        )

    # Assert the command executed successfully
    assert result.exit_code == 0, result.output

    # Assert that the output contained information about the bootstrapping and the mocked resources
    assert "Started incremental download for queue: Mock Queue" in result.output, result.output
    assert (
        f"Checkpoint: {os.path.join(checkpoint_dir, MOCK_QUEUE_ID + '_download_checkpoint.json')}"
        in result.output
    ), result.output
    assert "Checkpoint not found, lookback is 0.0 minutes" in result.output, result.output
    # Need to convert the freeze time to the local time zone for this print assertion
    assert (
        f"Initializing from: {datetime.fromisoformat(ISO_FREEZE_TIME).astimezone().isoformat()}"
        in result.output
    ), result.output
    assert f"NEW Job: Mock Job ({MOCK_JOB_ID})" in result.output, result.output
    assert "Succeeded tasks: 1 / 2" in result.output, result.output
    assert "Jobs added: 1" in result.output, result.output

    # Edit the mock job to complete the task
    mock_jobs[0]["taskRunStatus"] = "SUCCEEDED"
    mock_jobs[0]["taskRunStatusCounts"] = {
        "SUCCEEDED": 2,
        "READY": 0,
    }
    mock_jobs[0]["endedAt"] = datetime.fromisoformat(ISO_FREEZE_TIME)

    # Run the CLI command again to "complete" the download that was started
    with freeze_time(ISO_FREEZE_TIME):
        result = runner.invoke(
            main,
            [
                "queue",
                "incremental-output-download",
                "--farm-id",
                MOCK_FARM_ID,
                "--queue-id",
                MOCK_QUEUE_ID,
                "--checkpoint-dir",
                checkpoint_dir,
            ],
        )

    # Assert the command executed successfully
    assert result.exit_code == 0, result.output

    # Assert that the output contained information about loading the checkpoint and the mocked resources
    assert "Started incremental download for queue: Mock Queue" in result.output, result.output
    assert (
        f"Checkpoint: {os.path.join(checkpoint_dir, MOCK_QUEUE_ID + '_download_checkpoint.json')}"
        in result.output
    ), result.output
    assert "Checkpoint found" in result.output, result.output
    # Need to convert the freeze time to the local time zone for this print assertion
    assert (
        f"Continuing from: {datetime.fromisoformat(ISO_FREEZE_TIME).astimezone().isoformat()}"
        in result.output
    ), result.output
    assert f"EXISTING Job: Mock Job ({MOCK_JOB_ID})" in result.output, result.output
    assert "Succeeded tasks (before): 1 / 2" in result.output, result.output
    assert "Succeeded tasks (now)   : 2 / 2" in result.output, result.output
    assert "Jobs updated: 1" in result.output, result.output


def test_incremental_output_download_pid_lock_already_held_error(
    with_incremental_download_enabled, boto3_session, checkpoint_dir, pid_lock_file
):
    """Test incremental_output_download when PidLockAlreadyHeld is raised"""
    # Write a fake PID to the file
    with open(pid_lock_file, "w") as f:
        f.write("12345678")  # Use a fake PID

    # Run the CLI command
    runner = CliRunner()
    with patch.object(psutil, "pid_exists") as mock_pid_exists:
        # Make psutil.pid_exists return True to simulate the process is running
        mock_pid_exists.return_value = True
        result = runner.invoke(
            main,
            [
                "queue",
                "incremental-output-download",
                "--farm-id",
                MOCK_FARM_ID,
                "--queue-id",
                MOCK_QUEUE_ID,
                "--checkpoint-dir",
                checkpoint_dir,
            ],
        )

    # Assert the command did not execute successfully and wrote a message about another download in progress
    assert result.exit_code == 1, result.output
    assert (
        f"Unable to perform incremental output download as process with pid 12345678 already holds the lock {os.path.join(checkpoint_dir, MOCK_QUEUE_ID + '_incremental_output_download.pid')}"
        in result.output
    ), result.output

    # Verify the PID file still exists since we're simulating another process holding the lock
    assert os.path.exists(pid_lock_file)
