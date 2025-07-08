# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for the CLI queue incremental output download command.
"""

import os
import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta

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
from deadline.job_attachments._incremental_downloads.incremental_download_state import (
    EVENTUAL_CONSISTENCY_MAX_SECONDS,
)

ISO_FREEZE_TIME_MINUS_5MIN = "2025-05-26 11:55:00+00:00"
ISO_FREEZE_TIME_MINUS_1MIN = "2025-05-26 11:59:00+00:00"
ISO_FREEZE_TIME = "2025-05-26 12:00:00+00:00"
ISO_FREEZE_TIME_PLUS_1MIN = "2025-05-26 12:01:00+00:00"
ISO_FREEZE_TIME_PLUS_3MIN = "2025-05-26 12:03:00+00:00"
ISO_FREEZE_TIME_PLUS_5MIN = "2025-05-26 12:05:00+00:00"
ISO_FREEZE_TIME_PLUS_7MIN = "2025-05-26 12:07:00+00:00"


# Fixtures for shared resources
@pytest.fixture
def checkpoint_dir(tmp_path_factory):
    """Create a checkpoint directory for all tests to use."""
    checkpoint_dir = tmp_path_factory.mktemp("checkpoint")
    yield str(checkpoint_dir)
    # No cleanup needed here as tmp_path_factory handles it automatically


@pytest.fixture
def boto3_session():
    """Create a mock boto3 session for all tests to use."""
    mock_session = MagicMock(spec=boto3.Session)
    mock_session.client().get_queue.return_value = {
        "queueId": MOCK_QUEUE_ID,
        "displayName": "Mock Queue",
        "jobAttachmentSettings": {
            "rootPrefix": "MockRootPrefix",
            "s3BucketName": "mock-s3-bucket",
        },
    }
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


def test_incremental_output_download_requires_beta_acknowledgement(
    fresh_deadline_config, boto3_session, checkpoint_dir
):
    # Make sure the acknowledgement env var is not defined
    if "ENABLE_INCREMENTAL_OUTPUT_DOWNLOAD" in os.environ:
        del os.environ["ENABLE_INCREMENTAL_OUTPUT_DOWNLOAD"]

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


def test_incremental_output_download_requires_queue_with_job_attachments(
    fresh_deadline_config, boto3_session, with_incremental_download_enabled, checkpoint_dir
):
    # The response does not include the "jobAttachmentSettings" field
    boto3_session.client().get_queue.return_value = {
        "queueId": MOCK_QUEUE_ID,
        "displayName": "Mock Queue",
    }

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

    assert "Queue 'Mock Queue' does not have job attachments configured." in result.output, (
        result.output
    )


def test_incremental_output_download_pid_lock_already_held_error(
    fresh_deadline_config,
    with_incremental_download_enabled,
    boto3_session,
    checkpoint_dir,
    pid_lock_file,
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


def test_incremental_output_download_bootstrap_and_completion(
    fresh_deadline_config, with_incremental_download_enabled, boto3_session, checkpoint_dir
):
    """Test a new job through bootstrap, completion, and retirement."""
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

    # RUN 1: Run the CLI command once to bootstrap the operation
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
    assert "added: 1" in result.output, result.output

    # Edit the mock job to complete the task
    mock_jobs[0]["taskRunStatus"] = "SUCCEEDED"
    mock_jobs[0]["taskRunStatusCounts"] = {
        "SUCCEEDED": 2,
        "READY": 0,
    }
    mock_jobs[0]["endedAt"] = datetime.fromisoformat(ISO_FREEZE_TIME)

    # RUN 2: Run the CLI command again to "complete" the download that was started
    # 3 minutes later is after the consistency window, so that the call after this
    # sees the job being retired.
    with freeze_time(ISO_FREEZE_TIME_PLUS_3MIN):
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
    assert "completed: 1" in result.output, result.output

    # RUN 3: Run the CLI command again with a later timestamp to retire the job from the checkpoint
    # 5 minutes later is outside the eventual consistency window.
    with freeze_time(ISO_FREEZE_TIME_PLUS_5MIN):
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
        f"Continuing from: {(datetime.fromisoformat(ISO_FREEZE_TIME_PLUS_3MIN) - timedelta(seconds=EVENTUAL_CONSISTENCY_MAX_SECONDS)).astimezone().isoformat()}"
        in result.output
    ), result.output
    assert f"DROPPED Job: Mock Job ({MOCK_JOB_ID})" in result.output, result.output
    assert "Job succeeded" in result.output, result.output
    assert "inactive: 1" in result.output, result.output

    # RUN 4: Run the CLI command again with a later timestamp to see the job stay inactive
    with freeze_time(ISO_FREEZE_TIME_PLUS_7MIN):
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
        f"Continuing from: {(datetime.fromisoformat(ISO_FREEZE_TIME_PLUS_5MIN) - timedelta(seconds=EVENTUAL_CONSISTENCY_MAX_SECONDS)).astimezone().isoformat()}"
        in result.output
    ), result.output
    # Because this test didn't model any sessions and session actions, there is no session endedAt
    # timestamp, so no job needs to be further tracked as inactive in this case.
    assert "inactive: 0" in result.output, result.output


def test_incremental_output_download_bootstrap_retire_job_without_attachments(
    fresh_deadline_config, with_incremental_download_enabled, boto3_session, checkpoint_dir
):
    """Test a new job through bootstrap and completion over two incremental download commands."""
    mock_jobs = create_fake_job_list(1)
    mock_jobs[0]["name"] = "Mock Job"
    mock_jobs[0]["jobId"] = MOCK_JOB_ID
    mock_jobs[0]["taskRunStatus"] = "READY"
    mock_jobs[0]["taskRunStatusCounts"] = {
        "SUCCEEDED": 1,
        "READY": 1,
    }
    del mock_jobs[0]["endedAt"]
    boto3_session.client().search_jobs = mock_search_jobs_for_set(
        MOCK_FARM_ID, MOCK_QUEUE_ID, mock_jobs
    )
    boto3_session.client().get_job = mock_get_job_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, mock_jobs)

    # RUN 1: Run the CLI command once to bootstrap the operation
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
    assert "not using job attachments: 1" in result.output, result.output

    # Edit the mock job to complete the task
    mock_jobs[0]["taskRunStatus"] = "SUCCEEDED"
    mock_jobs[0]["taskRunStatusCounts"] = {
        "SUCCEEDED": 2,
        "READY": 0,
    }
    mock_jobs[0]["endedAt"] = datetime.fromisoformat(ISO_FREEZE_TIME)

    # RUN 2: Run the CLI command again after the job has all tasks completed
    # 3 minutes later is after the consistency window, so that the call after this
    # sees the job being retired.
    with freeze_time(ISO_FREEZE_TIME_PLUS_3MIN):
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
    assert "not using job attachments: 1" in result.output, result.output

    # RUN 3: Run the CLI command again with a later timestamp to retire the job from the checkpoint
    # 5 minutes later is outside the eventual consistency window.
    with freeze_time(ISO_FREEZE_TIME_PLUS_5MIN):
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
        f"Continuing from: {(datetime.fromisoformat(ISO_FREEZE_TIME_PLUS_3MIN) - timedelta(seconds=EVENTUAL_CONSISTENCY_MAX_SECONDS)).astimezone().isoformat()}"
        in result.output
    ), result.output
    assert "inactive: 1" in result.output, result.output


def test_incremental_output_download_job_unchanged(
    fresh_deadline_config, with_incremental_download_enabled, boto3_session, checkpoint_dir
):
    """Test a new job through bootstrap and an 'UNCHANGED' message."""
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

    # RUN 1: Run the CLI command once to bootstrap the operation
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
    assert "added: 1" in result.output, result.output

    # RUN 2: Run the CLI command again to see that the job is unchanged
    with freeze_time(ISO_FREEZE_TIME_PLUS_3MIN):
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
    assert f"UNCHANGED Job: Mock Job ({MOCK_JOB_ID})" in result.output, result.output
    assert "unchanged: 1" in result.output, result.output


def test_incremental_output_download_job_canceled(
    fresh_deadline_config, with_incremental_download_enabled, boto3_session, checkpoint_dir
):
    """Test a new job through bootstrap and cancelation before it's complete"""
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

    # RUN 1: Run the CLI command once to bootstrap the operation
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
    assert "added: 1" in result.output, result.output

    # RUN 2: Run the CLI command again to see that the job is canceled
    mock_jobs[0]["taskRunStatus"] = "CANCELED"
    mock_jobs[0]["taskRunStatusCounts"] = {
        "SUCCEEDED": 1,
        "CANCELED": 1,
    }
    with freeze_time(ISO_FREEZE_TIME_PLUS_3MIN):
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
    assert f"DROPPED Job: Mock Job ({MOCK_JOB_ID})" in result.output, result.output
    assert (
        "Job is not a download candidate anymore (likely suspended, canceled or failed)"
        in result.output
    ), result.output
    assert "inactive: 1" in result.output, result.output


def test_incremental_output_download_job_completed_then_requeued(
    fresh_deadline_config, with_incremental_download_enabled, boto3_session, checkpoint_dir
):
    """Test a new job through bootstrap, retirement, then requeue."""
    iso_freeze_time = datetime.fromisoformat(ISO_FREEZE_TIME)
    mock_jobs = create_fake_job_list(1, iso_freeze_time - timedelta(minutes=5), iso_freeze_time)
    mock_jobs[0]["name"] = "Mock Job"
    mock_jobs[0]["jobId"] = MOCK_JOB_ID
    mock_jobs[0]["taskRunStatus"] = "SUCCEEDED"
    mock_jobs[0]["taskRunStatusCounts"] = {
        "SUCCEEDED": 2,
        "READY": 0,
    }
    mock_jobs[0]["attachments"] = {
        "manifests": [
            {"rootPath": "/", "rootPathFormat": "posix", "outputRelativeDirectories": ["."]}
        ],
        "fileSystem": "VIRTUAL",
    }
    mock_jobs[0]["endedAt"] = iso_freeze_time - timedelta(minutes=3)
    boto3_session.client().search_jobs = mock_search_jobs_for_set(
        MOCK_FARM_ID, MOCK_QUEUE_ID, mock_jobs
    )
    boto3_session.client().get_job = mock_get_job_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, mock_jobs)

    # RUN 1: Run the CLI command once to bootstrap the operation
    # We've set up the job and timestamps so it bootstraps as completed
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
                "--bootstrap-lookback-minutes",
                "4.5",
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
    assert "Checkpoint not found, lookback is 4.5 minutes" in result.output, result.output
    # Need to convert the freeze time to the local time zone for this print assertion
    assert (
        f"Initializing from: {(iso_freeze_time - timedelta(minutes=4.5)).astimezone().isoformat()}"
        in result.output
    ), result.output
    assert f"NEW Job: Mock Job ({MOCK_JOB_ID})" in result.output, result.output
    assert "Succeeded tasks: 2 / 2" in result.output, result.output
    assert "completed: 1" in result.output, result.output

    # RUN 2: Run the CLI command again to see that the job becomes inactive
    with freeze_time(ISO_FREEZE_TIME_PLUS_5MIN):
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
        f"Continuing from: {(iso_freeze_time - timedelta(seconds=EVENTUAL_CONSISTENCY_MAX_SECONDS)).astimezone().isoformat()}"
        in result.output
    ), result.output
    assert "inactive: 1" in result.output, result.output

    # RUN 3: Run the CLI command again after requeuing tasks
    mock_jobs[0]["taskRunStatus"] = "READY"
    mock_jobs[0]["taskRunStatusCounts"] = {
        "SUCCEEDED": 1,
        "READY": 1,
    }
    del mock_jobs[0]["endedAt"]
    with freeze_time(ISO_FREEZE_TIME_PLUS_5MIN):
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
        f"Continuing from: {(datetime.fromisoformat(ISO_FREEZE_TIME_PLUS_5MIN) - timedelta(seconds=EVENTUAL_CONSISTENCY_MAX_SECONDS)).astimezone().isoformat()}"
        in result.output
    ), result.output
    assert f"NEW Job: Mock Job ({MOCK_JOB_ID})" in result.output, result.output
    assert "Succeeded tasks: 1 / 2" in result.output, result.output
    assert "added: 1" in result.output, result.output


def test_incremental_output_download_dry_run(
    fresh_deadline_config, with_incremental_download_enabled, boto3_session, checkpoint_dir
):
    """Test a new job through bootstrap, completion, and retirement."""
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

    # RUN 1: Run the CLI command once to bootstrap the operation
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
                "--dry-run",
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
    assert "Skipping downloads due to DRY RUN" in result.output, result.output
    assert (
        "Summary of DRY RUN for incremental output download (no files were downloaded to the file system):"
        in result.output
    ), result.output
    assert "This is a DRY RUN so the checkpoint was not saved" in result.output, result.output
