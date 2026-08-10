# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for the CLI queue incremental output download command.
"""

import json
import os
import sys
import pytest
from unittest.mock import patch
from datetime import datetime, timedelta
from typing import Callable, NamedTuple

from freezegun import freeze_time
from click.testing import CliRunner
from deadline.client.cli import main
import psutil

from ..shared_constants import (
    MOCK_FARM_ID,
    MOCK_QUEUE_ID,
    MOCK_JOB_ID,
    MOCK_STORAGE_PROFILE_ID,
    MOCK_FLEET_ID,
    MOCK_WORKER_ID,
)
from ..mock_deadline_job_apis import (
    mock_search_jobs_for_set,
    create_fake_job_list,
    mock_get_job_for_set,
)
from deadline.job_attachments._incremental_downloads.incremental_download_state import (
    EVENTUAL_CONSISTENCY_MAX_SECONDS,
    IncrementalDownloadState,
)
from deadline.job_attachments.models import StorageProfileOperatingSystemFamily
import deadline.client.api

ISO_FREEZE_TIME_MINUS_5MIN = "2025-05-26 11:55:00+00:00"
ISO_FREEZE_TIME_MINUS_1MIN = "2025-05-26 11:59:00+00:00"
ISO_FREEZE_TIME = "2025-05-26 12:00:00+00:00"
ISO_FREEZE_TIME_PLUS_1MIN = "2025-05-26 12:01:00+00:00"
ISO_FREEZE_TIME_PLUS_3MIN = "2025-05-26 12:03:00+00:00"
ISO_FREEZE_TIME_PLUS_5MIN = "2025-05-26 12:05:00+00:00"
ISO_FREEZE_TIME_PLUS_7MIN = "2025-05-26 12:07:00+00:00"

MOCK_STORAGE_PROFILE_ID_LOCAL = "sp-a123456789abcdefabcdefabcdefabcf"
MOCK_SESSION_ID = "session-0123456789abcdefabcdefabcdefabcd"
MOCK_SESSION_ACTION_ID_1 = "sessionaction-0123456789abcdefabcdefabcdefabcd-0"
MOCK_SESSION_ACTION_ID_2 = "sessionaction-0123456789abcdefabcdefabcdefabcd-1"

_STEP_ID = "step-b1764261dff54214aace3932bde8ae7e"
_OUTPUT_FILE_NAMES = ("beauty.exr", "depth.exr", "normal.exr")


def _ignore_profiles_status_file(checkpoint_dir):
    """Path of the status file written in --ignore-storage-profiles mode."""
    return os.path.join(
        checkpoint_dir, f"{MOCK_QUEUE_ID}_ignore-storage-profiles_download_status.json"
    )


# Fixtures for shared resources
@pytest.fixture
def checkpoint_dir(tmp_path_factory):
    """Create a checkpoint directory for all tests to use."""
    checkpoint_dir = tmp_path_factory.mktemp("checkpoint")
    yield str(checkpoint_dir)
    # No cleanup needed here as tmp_path_factory handles it automatically


@pytest.fixture
def restore_sigint_handler():
    """Restores SigIntHandler.continue_operation after a test that clears it.

    SigIntHandler is a process-wide singleton, so a test that simulates Ctrl+C would leave
    every later test in this session looking cancelled.
    """
    from deadline.client.cli._common import sigint_handler

    previous = sigint_handler.continue_operation
    try:
        yield sigint_handler
    finally:
        sigint_handler.continue_operation = previous


@pytest.fixture
def deadline_telemetry_client_mock():
    with patch.object(deadline.client.api, "get_deadline_cloud_library_telemetry_client") as m:
        yield m


@pytest.mark.skipif(
    sys.version_info < (3, 9), reason="Incremental output download requires Python >= 3.9"
)
def test_incremental_output_download_requires_queue_with_job_attachments(
    fresh_deadline_config, deadline_mock, checkpoint_dir
):
    # The response does not include the "jobAttachmentSettings" field
    deadline_mock.get_queue.return_value = {
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
                "sync-output",
                "--ignore-storage-profiles",
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


@pytest.mark.skipif(
    sys.version_info < (3, 9), reason="Incremental output download requires Python >= 3.9"
)
def test_incremental_output_download_pid_lock_already_held_error(
    fresh_deadline_config,
    deadline_mock,
    checkpoint_dir,
):
    """Test incremental_output_download when PidLockAlreadyHeld is raised"""
    # Write a fake PID to the file
    pid_lock_file = os.path.join(
        checkpoint_dir, f"{MOCK_QUEUE_ID}_ignore-storage-profiles_download_checkpoint.json.pid"
    )
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
                "sync-output",
                "--ignore-storage-profiles",
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
        f"Unable to perform incremental output download as process with pid 12345678 already holds the lock {os.path.join(checkpoint_dir, MOCK_QUEUE_ID + '_ignore-storage-profiles_download_checkpoint.json.pid')}"
        in result.output
    ), result.output

    # Verify the PID file still exists since we're simulating another process holding the lock
    assert os.path.exists(pid_lock_file)


@pytest.mark.skipif(
    sys.version_info < (3, 9), reason="Incremental output download requires Python >= 3.9"
)
def test_incremental_output_download_storage_profile_options_mutually_exclusive(
    fresh_deadline_config,
    deadline_mock,
    checkpoint_dir,
):
    """Test that --storage-profile-id and --ignore-storage-profiles can't be provided together"""

    # Run the CLI command
    runner = CliRunner()
    with patch.object(psutil, "pid_exists") as mock_pid_exists:
        # Make psutil.pid_exists return True to simulate the process is running
        mock_pid_exists.return_value = True
        result = runner.invoke(
            main,
            [
                "queue",
                "sync-output",
                "--ignore-storage-profiles",
                "--storage-profile-id",
                MOCK_STORAGE_PROFILE_ID,
                "--farm-id",
                MOCK_FARM_ID,
                "--queue-id",
                MOCK_QUEUE_ID,
                "--checkpoint-dir",
                checkpoint_dir,
            ],
        )

    assert result.exit_code != 0, result.output
    assert (
        "Options '--storage-profile-id' and '--ignore-storage-profiles' cannot be provided together"
        in result.output
    ), result.output


@pytest.mark.skipif(
    sys.version_info < (3, 9), reason="Incremental output download requires Python >= 3.9"
)
@pytest.mark.parametrize("storage_profile_id", [None, MOCK_STORAGE_PROFILE_ID])
def test_incremental_output_download_bootstrap_and_completion(
    fresh_deadline_config,
    deadline_mock,
    checkpoint_dir,
    storage_profile_id,
):
    """Test a new job through bootstrap, completion, and retirement. Both without storage profiles,
    and with the job storage profile matching the local one."""
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
    deadline_mock.search_jobs = mock_search_jobs_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, mock_jobs)
    deadline_mock.get_job = mock_get_job_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, mock_jobs)

    # RUN 1: Run the CLI command once to bootstrap the operation
    if storage_profile_id is None:
        storage_profile_options = ["--ignore-storage-profiles"]
    else:
        storage_profile_options = ["--storage-profile-id", storage_profile_id]
        mock_jobs[0]["storageProfileId"] = storage_profile_id
    runner = CliRunner()
    with freeze_time(ISO_FREEZE_TIME):
        result = runner.invoke(
            main,
            [
                "queue",
                "sync-output",
            ]
            + storage_profile_options
            + [
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

    if storage_profile_id is None:
        storage_profile_in_message = "ignore-storage-profiles"
    else:
        storage_profile_in_message = storage_profile_id

    # Assert that information is or isn't printed about the storage profile.
    if storage_profile_id is None:
        assert "Local storage profile is" not in result.output, result.output
        assert (
            "download candidate jobs have the same storage profile and will be downloaded to their original specified paths"
            not in result.output
        ), result.output
    else:
        assert "Local storage profile is" in result.output, result.output
        assert f"({storage_profile_id})" in result.output, result.output
        assert (
            "1 download candidate jobs have the same storage profile and will be downloaded to their original specified paths"
            in result.output
        ), result.output
    # Assert that the output contained information about the bootstrapping and the mocked resources
    assert "Started incremental download for queue: Mock Queue" in result.output, result.output
    assert (
        f"Checkpoint: {os.path.join(checkpoint_dir, MOCK_QUEUE_ID + '_' + storage_profile_in_message + '_download_checkpoint.json')}"
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
                "sync-output",
            ]
            + storage_profile_options
            + [
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
        f"Checkpoint: {os.path.join(checkpoint_dir, MOCK_QUEUE_ID + '_' + storage_profile_in_message + '_download_checkpoint.json')}"
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
                "sync-output",
            ]
            + storage_profile_options
            + [
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
        f"Checkpoint: {os.path.join(checkpoint_dir, MOCK_QUEUE_ID + '_' + storage_profile_in_message + '_download_checkpoint.json')}"
        in result.output
    ), result.output
    assert "Checkpoint found" in result.output, result.output
    # Need to convert the freeze time to the local time zone for this print assertion
    assert (
        f"Continuing from: {(datetime.fromisoformat(ISO_FREEZE_TIME_PLUS_3MIN) - timedelta(seconds=EVENTUAL_CONSISTENCY_MAX_SECONDS)).astimezone().isoformat()}"
        in result.output
    ), result.output
    assert f"FINISHED TRACKING Job: Mock Job ({MOCK_JOB_ID})" in result.output, result.output
    assert "Job succeeded" in result.output, result.output
    assert "inactive: 1" in result.output, result.output

    # RUN 4: Run the CLI command again with a later timestamp to see the job stay inactive
    with freeze_time(ISO_FREEZE_TIME_PLUS_7MIN):
        result = runner.invoke(
            main,
            [
                "queue",
                "sync-output",
            ]
            + storage_profile_options
            + [
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
        f"Checkpoint: {os.path.join(checkpoint_dir, MOCK_QUEUE_ID + '_' + storage_profile_in_message + '_download_checkpoint.json')}"
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


@pytest.mark.skipif(
    sys.version_info < (3, 9), reason="Incremental output download requires Python >= 3.9"
)
def test_incremental_output_download_storage_profile_path_mapping(
    fresh_deadline_config,
    tmp_path,
    deadline_mock,
    checkpoint_dir,
):
    """Test a new job with a different storage profile on the job than
    configured locally so as to get some path mapping rules."""
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
    mock_jobs[0]["storageProfileId"] = MOCK_STORAGE_PROFILE_ID
    del mock_jobs[0]["endedAt"]
    deadline_mock.search_jobs = mock_search_jobs_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, mock_jobs)
    deadline_mock.get_job = mock_get_job_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, mock_jobs)

    # Mock enough of get_storage_profile_for_queue two return two for mapping between them
    def mock_get_storage_profile_for_queue(farmId: str, queueId: str, storageProfileId: str):
        assert farmId == MOCK_FARM_ID
        assert queueId == MOCK_QUEUE_ID
        if storageProfileId == MOCK_STORAGE_PROFILE_ID:
            return {
                "storageProfileId": MOCK_STORAGE_PROFILE_ID,
                "displayName": "Mock-Storage-Profile-For-Job",
                "osFamily": "MACOS",
                "fileSystemLocations": [
                    {"name": "Location1", "path": "/Volumes/loc1", "type": "LOCAL"},
                    {"name": "Location2", "path": "/Home/user", "type": "LOCAL"},
                ],
            }
        else:
            return {
                "storageProfileId": MOCK_STORAGE_PROFILE_ID_LOCAL,
                "displayName": "Mock-Storage-Profile-For-Local",
                "osFamily": StorageProfileOperatingSystemFamily.get_host_os_family().value.upper(),
                "fileSystemLocations": [
                    {"name": "Location1", "path": str(tmp_path / "Location1"), "type": "LOCAL"},
                    {"name": "Location2", "path": str(tmp_path / "Location2"), "type": "LOCAL"},
                ],
            }

    deadline_mock.get_storage_profile_for_queue = mock_get_storage_profile_for_queue
    # Create the local storage profile directories so pre-flight validation passes
    (tmp_path / "Location1").mkdir(exist_ok=True)
    (tmp_path / "Location2").mkdir(exist_ok=True)
    # Mock list_sessions to return one session
    deadline_mock.list_sessions.return_value = {
        "sessions": [
            {
                "sessionId": MOCK_SESSION_ID,
                "fleetId": MOCK_FLEET_ID,
                "workerId": MOCK_WORKER_ID,
                "startedAt": "2025-08-06T00:15:45.712000+00:00",
                "lifecycleStatus": "STARTED",
            }
        ]
    }
    # Mock list_session_actions to return one task run session action
    deadline_mock.list_session_actions.return_value = {
        "sessionActions": [
            {
                "sessionActionId": MOCK_SESSION_ACTION_ID_1,
                "status": "SUCCEEDED",
                "startedAt": "2025-08-06T00:20:58.454000+00:00",
                "endedAt": "2025-08-06T00:20:59.992000+00:00",
                "progressPercent": 100.0,
                "definition": {
                    "taskRun": {
                        "taskId": "task-b1764261dff54214aace3932bde8ae7e-0",
                        "stepId": "step-b1764261dff54214aace3932bde8ae7e",
                    }
                },
                # This test doesn't go into the S3 object layer, so the manifests list is empty.
                "manifests": [],
            },
            {
                "sessionActionId": MOCK_SESSION_ACTION_ID_2,
                "status": "RUNNING",
                "startedAt": "2025-08-06T00:20:59.997000+00:00",
                "progressPercent": 20.0,
                "definition": {
                    "taskRun": {
                        "taskId": "task-b1764261dff54214aace3932bde8ae7e-1",
                        "stepId": "step-b1764261dff54214aace3932bde8ae7e",
                    }
                },
            },
        ]
    }

    # RUN 1: Run the CLI command once to bootstrap the operation
    runner = CliRunner()
    with freeze_time(ISO_FREEZE_TIME):
        result = runner.invoke(
            main,
            [
                "queue",
                "sync-output",
                "--storage-profile-id",
                MOCK_STORAGE_PROFILE_ID_LOCAL,
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

    # Assert that both storage profiles were retrieved for local and the job
    assert (
        f"Local storage profile is Mock-Storage-Profile-For-Local ({MOCK_STORAGE_PROFILE_ID_LOCAL})"
        in result.output
    ), result.output
    assert (
        "0 download candidate jobs have the same storage profile and will be downloaded to their original specified paths"
        in result.output
    ), result.output
    assert (
        f"Path mapping rules for 1 download candidate jobs with storage profile Mock-Storage-Profile-For-Job ({MOCK_STORAGE_PROFILE_ID})"
        in result.output
    ), result.output
    assert "job storage profile: Mock-Storage-Profile-For-Job (MACOS)" in result.output, (
        result.output
    )
    assert (
        f"local storage profile: Mock-Storage-Profile-For-Local ({StorageProfileOperatingSystemFamily.get_host_os_family().value.upper()})"
        in result.output
    ), result.output
    assert "- from: /Volumes/loc1" in result.output, result.output
    assert f" to:   {tmp_path / 'Location1'}" in result.output, result.output
    assert "- from: /Home/user" in result.output, result.output
    assert f"to:   {tmp_path / 'Location2'}" in result.output, result.output

    # Assert that it warned about the lack of outputs
    assert (
        f"WARNING: Job Mock Job ({MOCK_JOB_ID}) ran 1 / 1 session actions with no output."
        in result.output
    ), result.output
    assert (
        "This may indicate steps in the job that strictly perform validation or save results elsewhere like a shared file system or S3."
        in result.output
    ), result.output


@pytest.mark.skipif(
    sys.version_info < (3, 9), reason="Incremental output download requires Python >= 3.9"
)
def test_incremental_output_download_bootstrap_retire_job_without_attachments(
    fresh_deadline_config, deadline_mock, checkpoint_dir
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
    deadline_mock.search_jobs = mock_search_jobs_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, mock_jobs)
    deadline_mock.get_job = mock_get_job_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, mock_jobs)

    # RUN 1: Run the CLI command once to bootstrap the operation
    runner = CliRunner()
    with freeze_time(ISO_FREEZE_TIME):
        result = runner.invoke(
            main,
            [
                "queue",
                "sync-output",
                "--ignore-storage-profiles",
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
        f"Checkpoint: {os.path.join(checkpoint_dir, MOCK_QUEUE_ID + '_ignore-storage-profiles_download_checkpoint.json')}"
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
                "sync-output",
                "--ignore-storage-profiles",
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
        f"Checkpoint: {os.path.join(checkpoint_dir, MOCK_QUEUE_ID + '_ignore-storage-profiles_download_checkpoint.json')}"
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
                "sync-output",
                "--ignore-storage-profiles",
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
        f"Checkpoint: {os.path.join(checkpoint_dir, MOCK_QUEUE_ID + '_ignore-storage-profiles_download_checkpoint.json')}"
        in result.output
    ), result.output
    assert "Checkpoint found" in result.output, result.output
    # Need to convert the freeze time to the local time zone for this print assertion
    assert (
        f"Continuing from: {(datetime.fromisoformat(ISO_FREEZE_TIME_PLUS_3MIN) - timedelta(seconds=EVENTUAL_CONSISTENCY_MAX_SECONDS)).astimezone().isoformat()}"
        in result.output
    ), result.output
    assert "inactive: 1" in result.output, result.output


@pytest.mark.skipif(
    sys.version_info < (3, 9), reason="Incremental output download requires Python >= 3.9"
)
def test_incremental_output_download_job_unchanged(
    fresh_deadline_config, deadline_mock, checkpoint_dir
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
    deadline_mock.search_jobs = mock_search_jobs_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, mock_jobs)
    deadline_mock.get_job = mock_get_job_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, mock_jobs)

    # RUN 1: Run the CLI command once to bootstrap the operation
    runner = CliRunner()
    with freeze_time(ISO_FREEZE_TIME):
        result = runner.invoke(
            main,
            [
                "queue",
                "sync-output",
                "--ignore-storage-profiles",
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
        f"Checkpoint: {os.path.join(checkpoint_dir, MOCK_QUEUE_ID + '_ignore-storage-profiles_download_checkpoint.json')}"
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
                "sync-output",
                "--ignore-storage-profiles",
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
        f"Checkpoint: {os.path.join(checkpoint_dir, MOCK_QUEUE_ID + '_ignore-storage-profiles_download_checkpoint.json')}"
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


@pytest.mark.skipif(
    sys.version_info < (3, 9), reason="Incremental output download requires Python >= 3.9"
)
def test_incremental_output_download_job_canceled(
    fresh_deadline_config, deadline_mock, checkpoint_dir
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
    deadline_mock.search_jobs = mock_search_jobs_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, mock_jobs)
    deadline_mock.get_job = mock_get_job_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, mock_jobs)

    # RUN 1: Run the CLI command once to bootstrap the operation
    runner = CliRunner()
    with freeze_time(ISO_FREEZE_TIME):
        result = runner.invoke(
            main,
            [
                "queue",
                "sync-output",
                "--ignore-storage-profiles",
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
        f"Checkpoint: {os.path.join(checkpoint_dir, MOCK_QUEUE_ID + '_ignore-storage-profiles_download_checkpoint.json')}"
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
                "sync-output",
                "--ignore-storage-profiles",
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
        f"Checkpoint: {os.path.join(checkpoint_dir, MOCK_QUEUE_ID + '_ignore-storage-profiles_download_checkpoint.json')}"
        in result.output
    ), result.output
    assert "Checkpoint found" in result.output, result.output
    # Need to convert the freeze time to the local time zone for this print assertion
    assert (
        f"Continuing from: {datetime.fromisoformat(ISO_FREEZE_TIME).astimezone().isoformat()}"
        in result.output
    ), result.output
    assert f"FINISHED TRACKING Job: Mock Job ({MOCK_JOB_ID})" in result.output, result.output
    assert (
        "Job is not a download candidate anymore (likely suspended, canceled or failed)"
        in result.output
    ), result.output
    assert "inactive: 1" in result.output, result.output


@pytest.mark.skipif(
    sys.version_info < (3, 9), reason="Incremental output download requires Python >= 3.9"
)
def test_incremental_output_download_job_completed_then_requeued(
    fresh_deadline_config, deadline_mock, checkpoint_dir
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
    deadline_mock.search_jobs = mock_search_jobs_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, mock_jobs)
    deadline_mock.get_job = mock_get_job_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, mock_jobs)

    # RUN 1: Run the CLI command once to bootstrap the operation
    # We've set up the job and timestamps so it bootstraps as completed
    runner = CliRunner()
    with freeze_time(ISO_FREEZE_TIME):
        result = runner.invoke(
            main,
            [
                "queue",
                "sync-output",
                "--ignore-storage-profiles",
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
        f"Checkpoint: {os.path.join(checkpoint_dir, MOCK_QUEUE_ID + '_ignore-storage-profiles_download_checkpoint.json')}"
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
                "sync-output",
                "--ignore-storage-profiles",
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
        f"Checkpoint: {os.path.join(checkpoint_dir, MOCK_QUEUE_ID + '_ignore-storage-profiles_download_checkpoint.json')}"
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
                "sync-output",
                "--ignore-storage-profiles",
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
        f"Checkpoint: {os.path.join(checkpoint_dir, MOCK_QUEUE_ID + '_ignore-storage-profiles_download_checkpoint.json')}"
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


@pytest.mark.skipif(
    sys.version_info < (3, 9), reason="Incremental output download requires Python >= 3.9"
)
def test_incremental_output_download_dry_run(fresh_deadline_config, deadline_mock, checkpoint_dir):
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
    deadline_mock.search_jobs = mock_search_jobs_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, mock_jobs)
    deadline_mock.get_job = mock_get_job_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, mock_jobs)

    # RUN 1: Run the CLI command once to bootstrap the operation
    runner = CliRunner()
    with freeze_time(ISO_FREEZE_TIME):
        result = runner.invoke(
            main,
            [
                "queue",
                "sync-output",
                "--ignore-storage-profiles",
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
        f"Checkpoint: {os.path.join(checkpoint_dir, MOCK_QUEUE_ID + '_ignore-storage-profiles_download_checkpoint.json')}"
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


@pytest.mark.skipif(
    sys.version_info < (3, 9), reason="Incremental output download requires Python >= 3.9"
)
def test_incremental_output_download_stats_telemetry(
    fresh_deadline_config,
    deadline_mock,
    checkpoint_dir,
    deadline_telemetry_client_mock,
):
    """Verifies the telemetry event for statistics matches the expected format"""
    mock_job = create_fake_job_list(1)[0]
    mock_job.update(
        {
            "name": "Mock Job",
            "jobId": MOCK_JOB_ID,
            "taskRunStatus": "READY",
            "taskRunStatusCounts": {"SUCCEEDED": 1},
            "storageProfileId": MOCK_STORAGE_PROFILE_ID,
            "attachments": {
                "manifests": [{"rootPath": "/", "rootPathFormat": "posix"}],
                "fileSystem": "VIRTUAL",
            },
        }
    )
    del mock_job["endedAt"]
    deadline_mock.search_jobs = mock_search_jobs_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, [mock_job])
    deadline_mock.get_job = mock_get_job_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, [mock_job])

    runner = CliRunner()
    with freeze_time(ISO_FREEZE_TIME):
        runner.invoke(
            main,
            [
                "queue",
                "sync-output",
                "--farm-id",
                MOCK_FARM_ID,
                "--queue-id",
                MOCK_QUEUE_ID,
                "--storage-profile-id",
                MOCK_STORAGE_PROFILE_ID,
                "--checkpoint-dir",
                checkpoint_dir,
            ],
        )

    deadline_telemetry_client_mock().record_event.assert_called_once_with(
        event_type="com.amazon.rum.deadline.queue_sync_output_stats",
        event_details={
            # All latencies will be zero due to freeze_time()
            "latencies": {
                "_get_download_candidate_jobs": 0,
                "_categorize_jobs_in_checkpoint": 0,
                "_get_job_sessions": 0,
                "_update_checkpoint_jobs_list": 0,
                "_download_all_manifests_with_absolute_paths": 0,
                "download": 0,
                "path_mapping": 0,
            },
            "dry_run": False,
            "downloaded_session_actions": 0,
            "downloaded_files": 0,
            "downloaded_bytes": 0,
            "jobs_with_downloads": {"completed": 0, "added": 1, "updated": 0},
            "jobs_without_downloads": {
                "not_using_job_attachments": 0,
                "missing_storage_profile": 0,
                "unchanged": 0,
                "inactive": 0,
            },
            "unmapped_paths": 0,
        },
    )


def test_incremental_output_download_unmapped_paths_without_storage_profile(
    fresh_deadline_config, deadline_mock, checkpoint_dir
):
    """
    Regression test: when running with --ignore-storage-profiles (no storage profile),
    outputs whose manifest paths are dropped (e.g. by the job attachments path-traversal
    containment check) are surfaced via `unmapped_paths`. The warning that reports them
    must not reference a storage profile that does not exist. Previously this raised
    UnboundLocalError: cannot access local variable 'storage_profiles'.
    """
    mock_jobs = create_fake_job_list(1)
    mock_jobs[0]["name"] = "Mock Job"
    mock_jobs[0]["jobId"] = MOCK_JOB_ID
    mock_jobs[0]["taskRunStatus"] = "SUCCEEDED"
    mock_jobs[0]["taskRunStatusCounts"] = {"SUCCEEDED": 1, "READY": 0}
    mock_jobs[0]["attachments"] = {
        "manifests": [
            {"rootPath": "/", "rootPathFormat": "posix", "outputRelativeDirectories": ["."]}
        ],
        "fileSystem": "COPIED",
    }
    mock_jobs[0]["endedAt"] = datetime.fromisoformat(ISO_FREEZE_TIME)
    deadline_mock.search_jobs = mock_search_jobs_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, mock_jobs)
    deadline_mock.get_job = mock_get_job_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, mock_jobs)

    deadline_mock.list_sessions.return_value = {
        "sessions": [
            {
                "sessionId": MOCK_SESSION_ID,
                "fleetId": MOCK_FLEET_ID,
                "workerId": MOCK_WORKER_ID,
                "startedAt": datetime.fromisoformat("2025-08-06T00:15:45.712000+00:00"),
                "endedAt": datetime.fromisoformat("2025-08-06T00:20:59.992000+00:00"),
                "lifecycleStatus": "ENDED",
            }
        ]
    }
    deadline_mock.list_session_actions.return_value = {
        "sessionActions": [
            {
                "sessionActionId": MOCK_SESSION_ACTION_ID_1,
                "status": "SUCCEEDED",
                "startedAt": "2025-08-06T00:20:58.454000+00:00",
                "endedAt": "2025-08-06T00:20:59.992000+00:00",
                "progressPercent": 100.0,
                "definition": {
                    "taskRun": {
                        "taskId": "task-b1764261dff54214aace3932bde8ae7e-0",
                        "stepId": "step-b1764261dff54214aace3932bde8ae7e",
                    }
                },
                "manifests": [],
            },
        ]
    }

    # Simulate the job attachments layer dropping an out-of-root output path into
    # unmapped_paths (as the path-traversal containment check does), returning no
    # downloadable manifests.
    def fake_download_all_manifests(
        queue,
        download_candidate_jobs,
        job_sessions,
        path_mapping_rule_appliers,
        output_unmapped_paths,
        boto3_session_for_s3,
        print_function_callback=lambda msg: None,
    ):
        output_unmapped_paths[MOCK_JOB_ID] = ["/etc/cron.d/evil"]
        return []

    runner = CliRunner()
    with (
        patch(
            "deadline.client.cli._incremental_download._download_all_manifests_with_absolute_paths",
            side_effect=fake_download_all_manifests,
        ),
        freeze_time(ISO_FREEZE_TIME),
    ):
        result = runner.invoke(
            main,
            [
                "queue",
                "sync-output",
                "--ignore-storage-profiles",
                "--force-bootstrap",
                "--bootstrap-lookback-minutes",
                "120",
                "--farm-id",
                MOCK_FARM_ID,
                "--queue-id",
                MOCK_QUEUE_ID,
                "--checkpoint-dir",
                checkpoint_dir,
            ],
        )

    # Must not crash (previously raised UnboundLocalError on 'storage_profiles').
    assert result.exit_code == 0, result.output
    assert "WARNING: THE FOLLOWING FILES WILL NOT BE DOWNLOADED" in result.output, result.output
    assert "/etc/cron.d/evil" in result.output, result.output


@pytest.mark.skipif(
    sys.version_info < (3, 9), reason="Incremental output download requires Python >= 3.9"
)
def test_incremental_output_download_manifest_mismatch_still_downloads(
    fresh_deadline_config, deadline_mock, checkpoint_dir
):
    """Decouple guard: if the manifest lists ever diverge in length (skip_attribution),
    downloads must still proceed — the files are downloaded from downloaded_manifests via a
    fallback bucket, and per-job attribution is skipped only for reporting. Previously a
    mismatch left job_manifest_paths empty and silently downloaded nothing."""
    from deadline.job_attachments.asset_manifests.v2023_03_03.asset_manifest import (
        AssetManifest,
        ManifestPath,
    )
    from deadline.job_attachments.asset_manifests import HashAlgorithm

    mock_jobs = create_fake_job_list(1)
    mock_jobs[0]["name"] = "Mock Job"
    mock_jobs[0]["jobId"] = MOCK_JOB_ID
    mock_jobs[0]["taskRunStatus"] = "SUCCEEDED"
    mock_jobs[0]["taskRunStatusCounts"] = {"SUCCEEDED": 1, "READY": 0}
    mock_jobs[0]["attachments"] = {
        "manifests": [
            {"rootPath": "/", "rootPathFormat": "posix", "outputRelativeDirectories": ["."]}
        ],
        "fileSystem": "COPIED",
    }
    mock_jobs[0]["endedAt"] = datetime.fromisoformat(ISO_FREEZE_TIME)
    deadline_mock.search_jobs = mock_search_jobs_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, mock_jobs)
    deadline_mock.get_job = mock_get_job_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, mock_jobs)

    deadline_mock.list_sessions.return_value = {
        "sessions": [
            {
                "sessionId": MOCK_SESSION_ID,
                "fleetId": MOCK_FLEET_ID,
                "workerId": MOCK_WORKER_ID,
                "startedAt": datetime.fromisoformat("2025-08-06T00:15:45.712000+00:00"),
                "endedAt": datetime.fromisoformat("2025-08-06T00:20:59.992000+00:00"),
                "lifecycleStatus": "ENDED",
            }
        ]
    }
    deadline_mock.list_session_actions.return_value = {
        "sessionActions": [
            {
                "sessionActionId": MOCK_SESSION_ACTION_ID_1,
                "status": "SUCCEEDED",
                "startedAt": "2025-08-06T00:20:58.454000+00:00",
                "endedAt": "2025-08-06T00:20:59.992000+00:00",
                "progressPercent": 100.0,
                "definition": {
                    "taskRun": {
                        "taskId": "task-b1764261dff54214aace3932bde8ae7e-0",
                        "stepId": "step-b1764261dff54214aace3932bde8ae7e",
                    }
                },
                "manifests": [{"outputManifestPath": "task-0/manifest"}],
            },
        ]
    }

    downloaded_file = "/tmp/mismatch_test_output.exr"
    downloaded_manifests = [
        (
            datetime.fromisoformat(ISO_FREEZE_TIME),
            AssetManifest(
                hash_alg=HashAlgorithm.XXH128,
                total_size=1,
                paths=[ManifestPath(path=downloaded_file, hash="h", size=1, mtime=1)],
            ),
        )
    ]

    def fake_download_all_manifests(
        queue,
        download_candidate_jobs,
        job_sessions,
        path_mapping_rule_appliers,
        output_unmapped_paths,
        boto3_session_for_s3,
        print_function_callback=lambda msg: None,
    ):
        return downloaded_manifests  # length 1

    # Force a length mismatch: return 2 tuples here vs 1 downloaded manifest -> skip_attribution.
    def fake_get_manifests_to_download(*args, **kwargs):
        return [
            (None, MOCK_JOB_ID, "/", "prefix/step/task-0/manifest"),
            (None, MOCK_JOB_ID, "/", "prefix/step/task-1/manifest"),
        ]

    downloaded_files_seen: list = []

    def fake_download_manifest_paths(
        files,
        hash_algorithm,
        queue,
        session,
        conflict,
        on_downloading_files,
        print_function_callback,
    ):
        # Record what got handed to the downloader — proves downloads proceed despite the mismatch.
        downloaded_files_seen.extend(f.path for f in files)

    runner = CliRunner()
    with (
        patch(
            "deadline.client.cli._incremental_download._download_all_manifests_with_absolute_paths",
            side_effect=fake_download_all_manifests,
        ),
        patch(
            "deadline.client.cli._incremental_download._get_manifests_to_download",
            side_effect=fake_get_manifests_to_download,
        ),
        patch(
            "deadline.client.cli._incremental_download._download_manifest_paths",
            side_effect=fake_download_manifest_paths,
        ),
        freeze_time(ISO_FREEZE_TIME),
    ):
        result = runner.invoke(
            main,
            [
                "queue",
                "sync-output",
                "--ignore-storage-profiles",
                "--force-bootstrap",
                "--bootstrap-lookback-minutes",
                "120",
                "--farm-id",
                MOCK_FARM_ID,
                "--queue-id",
                MOCK_QUEUE_ID,
                "--checkpoint-dir",
                checkpoint_dir,
            ],
        )

    assert result.exit_code == 0, result.output
    # The mismatch warning fired, but the file was STILL downloaded (via the fallback bucket).
    assert "Manifest list length mismatch" in result.output, result.output
    assert downloaded_file in downloaded_files_seen, (
        f"Expected {downloaded_file} to be downloaded despite skip_attribution; "
        f"got {downloaded_files_seen}"
    )
    # A successful fallback run must still report what it downloaded — the retained "" bucket
    # feeds the run-level stats, so a real download never reports "0 files downloaded".
    assert "Downloaded files: 1" in result.output, result.output
    # ...and the run is reported successful, with no synthetic "" job leaking into the entries.
    status_path = os.path.join(
        checkpoint_dir, f"{MOCK_QUEUE_ID}_ignore-storage-profiles_download_status.json"
    )
    with open(status_path) as f:
        status = json.load(f)
    assert status["sync_metadata"]["last_run_status"] == "success", status
    assert "" not in status["jobs"], status["jobs"]


def test_incremental_output_download_fallback_failure_marks_run_failed(
    fresh_deadline_config, deadline_mock, checkpoint_dir
):
    """When attribution is skipped (manifest mismatch) AND the fallback download fails, the
    failure must not be silently swallowed with the synthetic "" bucket: the run is reported
    failed and the checkpoint timestamp is held back so the lost window is retried next run."""
    from deadline.job_attachments.asset_manifests.v2023_03_03.asset_manifest import (
        AssetManifest,
        ManifestPath,
    )
    from deadline.job_attachments.asset_manifests import HashAlgorithm

    mock_jobs = create_fake_job_list(1)
    mock_jobs[0]["name"] = "Mock Job"
    mock_jobs[0]["jobId"] = MOCK_JOB_ID
    mock_jobs[0]["taskRunStatus"] = "SUCCEEDED"
    mock_jobs[0]["taskRunStatusCounts"] = {"SUCCEEDED": 1, "READY": 0}
    mock_jobs[0]["attachments"] = {
        "manifests": [
            {"rootPath": "/", "rootPathFormat": "posix", "outputRelativeDirectories": ["."]}
        ],
        "fileSystem": "COPIED",
    }
    mock_jobs[0]["endedAt"] = datetime.fromisoformat(ISO_FREEZE_TIME)
    deadline_mock.search_jobs = mock_search_jobs_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, mock_jobs)
    deadline_mock.get_job = mock_get_job_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, mock_jobs)

    deadline_mock.list_sessions.return_value = {
        "sessions": [
            {
                "sessionId": MOCK_SESSION_ID,
                "fleetId": MOCK_FLEET_ID,
                "workerId": MOCK_WORKER_ID,
                "startedAt": datetime.fromisoformat("2025-08-06T00:15:45.712000+00:00"),
                "endedAt": datetime.fromisoformat("2025-08-06T00:20:59.992000+00:00"),
                "lifecycleStatus": "ENDED",
            }
        ]
    }
    deadline_mock.list_session_actions.return_value = {
        "sessionActions": [
            {
                "sessionActionId": MOCK_SESSION_ACTION_ID_1,
                "status": "SUCCEEDED",
                "startedAt": "2025-08-06T00:20:58.454000+00:00",
                "endedAt": "2025-08-06T00:20:59.992000+00:00",
                "progressPercent": 100.0,
                "definition": {
                    "taskRun": {
                        "taskId": "task-b1764261dff54214aace3932bde8ae7e-0",
                        "stepId": "step-b1764261dff54214aace3932bde8ae7e",
                    }
                },
                "manifests": [{"outputManifestPath": "task-0/manifest"}],
            },
        ]
    }

    downloaded_file = "/tmp/mismatch_fail_output.exr"
    downloaded_manifests = [
        (
            datetime.fromisoformat(ISO_FREEZE_TIME),
            AssetManifest(
                hash_alg=HashAlgorithm.XXH128,
                total_size=1,
                paths=[ManifestPath(path=downloaded_file, hash="h", size=1, mtime=1)],
            ),
        )
    ]

    def fake_download_all_manifests(
        queue,
        download_candidate_jobs,
        job_sessions,
        path_mapping_rule_appliers,
        output_unmapped_paths,
        boto3_session_for_s3,
        print_function_callback=lambda msg: None,
    ):
        return downloaded_manifests

    # Force a length mismatch (2 vs 1) so attribution is skipped -> fallback bucket.
    def fake_get_manifests_to_download(*args, **kwargs):
        return [
            (None, MOCK_JOB_ID, "/", "prefix/step/task-0/manifest"),
            (None, MOCK_JOB_ID, "/", "prefix/step/task-1/manifest"),
        ]

    def fake_download_manifest_paths(*args, **kwargs):
        raise PermissionError("Access is denied")  # fallback download fails

    runner = CliRunner()
    with (
        patch(
            "deadline.client.cli._incremental_download._download_all_manifests_with_absolute_paths",
            side_effect=fake_download_all_manifests,
        ),
        patch(
            "deadline.client.cli._incremental_download._get_manifests_to_download",
            side_effect=fake_get_manifests_to_download,
        ),
        patch(
            "deadline.client.cli._incremental_download._download_manifest_paths",
            side_effect=fake_download_manifest_paths,
        ),
        freeze_time(ISO_FREEZE_TIME),
    ):
        result = runner.invoke(
            main,
            [
                "queue",
                "sync-output",
                "--ignore-storage-profiles",
                "--force-bootstrap",
                "--bootstrap-lookback-minutes",
                "120",
                "--farm-id",
                MOCK_FARM_ID,
                "--queue-id",
                MOCK_QUEUE_ID,
                "--checkpoint-dir",
                checkpoint_dir,
            ],
        )

    assert result.exit_code == 0, result.output

    # The run is reported failed — not silently swallowed with the "" bucket.
    status_path = os.path.join(
        checkpoint_dir, f"{MOCK_QUEUE_ID}_ignore-storage-profiles_download_status.json"
    )
    with open(status_path) as f:
        status = json.load(f)
    assert status["sync_metadata"]["last_run_status"] == "failed", status
    # The synthetic "" bucket never leaks into the per-job entries.
    assert "" not in status["jobs"], status["jobs"]

    # The checkpoint timestamp is held back to the bootstrap start (completed == started), not
    # advanced, so the lost window is re-attempted on the next run. A successful run would have
    # advanced completed to (now - eventual_consistency), strictly after started.
    checkpoint_path = os.path.join(
        checkpoint_dir, f"{MOCK_QUEUE_ID}_ignore-storage-profiles_download_checkpoint.json"
    )
    saved = IncrementalDownloadState.from_file(checkpoint_path)
    assert saved.downloads_completed_timestamp == saved.downloads_started_timestamp, (
        f"Expected timestamp held back to bootstrap start for retry, got "
        f"completed={saved.downloads_completed_timestamp.isoformat()} "
        f"started={saved.downloads_started_timestamp.isoformat()}"
    )


@pytest.mark.skipif(
    sys.version_info < (3, 9), reason="Incremental output download requires Python >= 3.9"
)
def test_incremental_output_download_per_task_error_isolation(
    fresh_deadline_config, deadline_mock, checkpoint_dir, tmp_path
):
    """Per-task download error isolation: when one task's files fail to download,
    only that task is marked failed — the other tasks succeed independently.

    Simulates a 3-task job where task-1's output path is inaccessible. The status
    file must show task-0 and task-2 as 'downloaded' and task-1 as 'failed' with the
    correct error code, while the job-level status is 'failed'.

    Also asserts the run summary counts the two successful tasks' files (2 of 3) rather
    than dropping the whole partially-failed job to zero.
    """
    from deadline.job_attachments.asset_manifests.v2023_03_03.asset_manifest import (
        AssetManifest,
        ManifestPath,
    )
    from deadline.job_attachments.asset_manifests import HashAlgorithm

    step_id = "step-b1764261dff54214aace3932bde8ae7e"
    task_ids = [f"task-b1764261dff54214aace3932bde8ae7e-{i}" for i in range(3)]
    # Each task writes one file to its own subdirectory under tmp_path.
    task_file_paths = [str(tmp_path / f"frame_{i}" / "beauty.exr") for i in range(3)]
    locked_path = task_file_paths[1]  # task-1's file is the one that fails

    mock_jobs = create_fake_job_list(1)
    mock_jobs[0]["name"] = "Mock Job"
    mock_jobs[0]["jobId"] = MOCK_JOB_ID
    mock_jobs[0]["taskRunStatus"] = "SUCCEEDED"
    mock_jobs[0]["taskRunStatusCounts"] = {"SUCCEEDED": 3, "READY": 0}
    mock_jobs[0]["attachments"] = {
        "manifests": [
            {"rootPath": "/", "rootPathFormat": "posix", "outputRelativeDirectories": ["."]}
        ],
        "fileSystem": "COPIED",
    }
    mock_jobs[0]["endedAt"] = datetime.fromisoformat(ISO_FREEZE_TIME)
    deadline_mock.search_jobs = mock_search_jobs_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, mock_jobs)
    deadline_mock.get_job = mock_get_job_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, mock_jobs)

    deadline_mock.list_sessions.return_value = {
        "sessions": [
            {
                "sessionId": MOCK_SESSION_ID,
                "fleetId": MOCK_FLEET_ID,
                "workerId": MOCK_WORKER_ID,
                "startedAt": datetime.fromisoformat("2025-08-06T00:15:45.712000+00:00"),
                "endedAt": datetime.fromisoformat("2025-08-06T00:20:59.992000+00:00"),
                "lifecycleStatus": "ENDED",
            }
        ]
    }
    deadline_mock.list_session_actions.return_value = {
        "sessionActions": [
            {
                "sessionActionId": f"sessionaction-0123456789abcdefabcdefabcdefabcd-{i}",
                "status": "SUCCEEDED",
                "startedAt": "2025-08-06T00:20:58.454000+00:00",
                "endedAt": "2025-08-06T00:20:59.992000+00:00",
                "progressPercent": 100.0,
                "definition": {"taskRun": {"taskId": task_ids[i], "stepId": step_id}},
                "manifests": [{"outputManifestPath": f"{task_ids[i]}/manifest"}],
            }
            for i in range(3)
        ]
    }

    # One manifest per task, each containing that task's single output file.
    downloaded_manifests = [
        (
            datetime.fromisoformat(ISO_FREEZE_TIME),
            AssetManifest(
                hash_alg=HashAlgorithm.XXH128,
                total_size=1,
                paths=[ManifestPath(path=task_file_paths[i], hash="h", size=1, mtime=1)],
            ),
        )
        for i in range(3)
    ]
    # The (applier, job_id, root_path, s3_key) tuples correlate positionally with the
    # manifests above. The S3 key embeds the task id so per-task attribution works.
    manifests_to_download = [
        (None, MOCK_JOB_ID, "/", f"prefix/{step_id}/{task_ids[i]}/manifest") for i in range(3)
    ]

    def fake_download_all_manifests(
        queue,
        download_candidate_jobs,
        job_sessions,
        path_mapping_rule_appliers,
        output_unmapped_paths,
        boto3_session_for_s3,
        print_function_callback=lambda msg: None,
    ):
        return downloaded_manifests

    def fake_get_manifests_to_download(*args, **kwargs):
        return manifests_to_download

    def fake_download_manifest_paths(
        files,
        hash_algorithm,
        queue,
        session,
        conflict,
        on_downloading_files,
        print_function_callback,
    ):
        # Simulate the download: create files on disk, but raise if task-1's locked
        # file is in this batch (per-task isolation calls this once per task).
        for f in files:
            if f.path == locked_path:
                raise PermissionError(f"[Errno 13] Permission denied: '{locked_path}'")
            os.makedirs(os.path.dirname(f.path), exist_ok=True)
            with open(f.path, "w") as fh:
                fh.write("output")

    runner = CliRunner()
    with (
        patch(
            "deadline.client.cli._incremental_download._download_all_manifests_with_absolute_paths",
            side_effect=fake_download_all_manifests,
        ),
        patch(
            "deadline.client.cli._incremental_download._get_manifests_to_download",
            side_effect=fake_get_manifests_to_download,
        ),
        patch(
            "deadline.client.cli._incremental_download._download_manifest_paths",
            side_effect=fake_download_manifest_paths,
        ),
        freeze_time(ISO_FREEZE_TIME),
    ):
        result = runner.invoke(
            main,
            [
                "queue",
                "sync-output",
                "--ignore-storage-profiles",
                "--force-bootstrap",
                "--bootstrap-lookback-minutes",
                "120",
                "--farm-id",
                MOCK_FARM_ID,
                "--queue-id",
                MOCK_QUEUE_ID,
                "--checkpoint-dir",
                checkpoint_dir,
            ],
        )

    assert result.exit_code == 0, result.output

    # The run summary counts the succeeded tasks even though the job carries an error_code:
    # task-0 and task-2 landed their files, so 2 (not 0) files are reported downloaded.
    assert "Downloaded files: 2" in result.output, result.output

    # The status file records per-task isolation: task-1 failed, task-0 and task-2 succeeded.
    status_file_path = os.path.join(
        checkpoint_dir, f"{MOCK_QUEUE_ID}_ignore-storage-profiles_download_status.json"
    )
    with open(status_file_path) as f:
        status = json.load(f)

    job_entry = status["jobs"][MOCK_JOB_ID]
    assert job_entry["download_status"] == "failed", job_entry
    assert job_entry["error_code"] == "PERMISSION_DENIED", job_entry

    tasks = job_entry["tasks"]
    assert tasks[task_ids[0]]["download_status"] == "downloaded", tasks
    assert tasks[task_ids[0]]["error_code"] is None, tasks
    assert tasks[task_ids[2]]["download_status"] == "downloaded", tasks
    assert tasks[task_ids[2]]["error_code"] is None, tasks
    assert tasks[task_ids[1]]["download_status"] == "failed", tasks
    assert tasks[task_ids[1]]["error_code"] == "PERMISSION_DENIED", tasks


@pytest.mark.skipif(
    sys.version_info < (3, 9), reason="Incremental output download requires Python >= 3.9"
)
def test_incremental_output_download_farm_failed_task_reaches_status_file(
    fresh_deadline_config, deadline_mock, checkpoint_dir, tmp_path
):
    """End-to-end: a task that FAILED on the farm flows through _get_job_sessions (tuple
    unpack at :1349), the cross-thread merge, the recording loop, and write_download_status_file
    so the written JSON shows download_status="farm_failed" for that task.

    Uses a 2-task job where task-0 SUCCEEDED (has output to download) and task-1 FAILED on the
    farm (no output). The status file must record task-0 as "downloaded" and task-1 as
    "farm_failed" with no error_code, while the job itself is "downloaded" (one task's output
    landed successfully; farm failures are not download failures).
    """
    from deadline.job_attachments.asset_manifests.v2023_03_03.asset_manifest import (
        AssetManifest,
        ManifestPath,
    )
    from deadline.job_attachments.asset_manifests import HashAlgorithm

    step_id = "step-b1764261dff54214aace3932bde8ae7e"
    task_ids = [f"task-b1764261dff54214aace3932bde8ae7e-{i}" for i in range(2)]
    task_file_path = str(tmp_path / "frame_0" / "beauty.exr")

    mock_jobs = create_fake_job_list(1)
    mock_jobs[0]["name"] = "Mock Job"
    mock_jobs[0]["jobId"] = MOCK_JOB_ID
    mock_jobs[0]["taskRunStatus"] = "FAILED"
    mock_jobs[0]["taskRunStatusCounts"] = {"SUCCEEDED": 1, "FAILED": 1, "READY": 0}
    mock_jobs[0]["attachments"] = {
        "manifests": [
            {"rootPath": "/", "rootPathFormat": "posix", "outputRelativeDirectories": ["."]}
        ],
        "fileSystem": "COPIED",
    }
    mock_jobs[0]["endedAt"] = datetime.fromisoformat(ISO_FREEZE_TIME)
    deadline_mock.search_jobs = mock_search_jobs_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, mock_jobs)
    deadline_mock.get_job = mock_get_job_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, mock_jobs)

    deadline_mock.list_sessions.return_value = {
        "sessions": [
            {
                "sessionId": MOCK_SESSION_ID,
                "fleetId": MOCK_FLEET_ID,
                "workerId": MOCK_WORKER_ID,
                "startedAt": datetime.fromisoformat("2025-08-06T00:15:45.712000+00:00"),
                "endedAt": datetime.fromisoformat("2025-08-06T00:20:59.992000+00:00"),
                "lifecycleStatus": "ENDED",
            }
        ]
    }
    # task-0 SUCCEEDED (has a manifest), task-1 FAILED on the farm (no manifest).
    deadline_mock.list_session_actions.return_value = {
        "sessionActions": [
            {
                "sessionActionId": "sessionaction-0123456789abcdefabcdefabcdefabcd-0",
                "status": "SUCCEEDED",
                "startedAt": "2025-08-06T00:20:58.454000+00:00",
                "endedAt": "2025-08-06T00:20:59.992000+00:00",
                "progressPercent": 100.0,
                "definition": {"taskRun": {"taskId": task_ids[0], "stepId": step_id}},
                "manifests": [{"outputManifestPath": f"{task_ids[0]}/manifest"}],
            },
            {
                "sessionActionId": "sessionaction-0123456789abcdefabcdefabcdefabcd-1",
                "status": "FAILED",
                "startedAt": "2025-08-06T00:20:58.454000+00:00",
                "endedAt": "2025-08-06T00:20:59.992000+00:00",
                "progressPercent": 0.0,
                "definition": {"taskRun": {"taskId": task_ids[1], "stepId": step_id}},
            },
        ]
    }

    downloaded_manifests = [
        (
            datetime.fromisoformat(ISO_FREEZE_TIME),
            AssetManifest(
                hash_alg=HashAlgorithm.XXH128,
                total_size=1,
                paths=[ManifestPath(path=task_file_path, hash="h", size=1, mtime=1)],
            ),
        )
    ]
    manifests_to_download = [(None, MOCK_JOB_ID, "/", f"prefix/{step_id}/{task_ids[0]}/manifest")]

    def fake_download_all_manifests(*args, **kwargs):
        return downloaded_manifests

    def fake_get_manifests_to_download(*args, **kwargs):
        return manifests_to_download

    def fake_download_manifest_paths(
        files,
        hash_algorithm,
        queue,
        session,
        conflict,
        on_downloading_files,
        print_function_callback,
    ):
        for f in files:
            os.makedirs(os.path.dirname(f.path), exist_ok=True)
            with open(f.path, "w") as fh:
                fh.write("output")

    runner = CliRunner()
    with (
        patch(
            "deadline.client.cli._incremental_download._download_all_manifests_with_absolute_paths",
            side_effect=fake_download_all_manifests,
        ),
        patch(
            "deadline.client.cli._incremental_download._get_manifests_to_download",
            side_effect=fake_get_manifests_to_download,
        ),
        patch(
            "deadline.client.cli._incremental_download._download_manifest_paths",
            side_effect=fake_download_manifest_paths,
        ),
        freeze_time(ISO_FREEZE_TIME),
    ):
        result = runner.invoke(
            main,
            [
                "queue",
                "sync-output",
                "--ignore-storage-profiles",
                "--force-bootstrap",
                "--bootstrap-lookback-minutes",
                "120",
                "--farm-id",
                MOCK_FARM_ID,
                "--queue-id",
                MOCK_QUEUE_ID,
                "--checkpoint-dir",
                checkpoint_dir,
            ],
        )

    assert result.exit_code == 0, result.output

    status_file_path = os.path.join(
        checkpoint_dir, f"{MOCK_QUEUE_ID}_ignore-storage-profiles_download_status.json"
    )
    with open(status_file_path) as f:
        status = json.load(f)

    job_entry = status["jobs"][MOCK_JOB_ID]
    # The job succeeded overall — one task's output landed, the farm failure is not a download error.
    assert job_entry["download_status"] == "downloaded", job_entry

    tasks = job_entry["tasks"]
    assert tasks[task_ids[0]]["download_status"] == "downloaded", tasks
    assert tasks[task_ids[0]]["error_code"] is None, tasks
    assert tasks[task_ids[1]]["download_status"] == "farm_failed", tasks
    assert tasks[task_ids[1]]["error_code"] is None, tasks


@pytest.mark.skipif(
    sys.version_info < (3, 9), reason="Incremental output download requires Python >= 3.9"
)
def test_incremental_output_download_stale_farm_failed_cleared_when_task_later_succeeds(
    fresh_deadline_config, deadline_mock, checkpoint_dir, tmp_path
):
    """Run 1 writes farm_failed for a task. The task is requeued and SUCCEEDS with no output
    on run 2. The stale farm_failed entry must be cleared from the status file.

    This tests the gap Phillip identified: _get_job_sessions subtracts the succeeded task from
    farm_failed_task_ids so nothing new is written, but the prior disk entry is carried forward
    by the merge unless succeeded_task_ids explicitly overwrites it.
    """
    step_id = "step-b1764261dff54214aace3932bde8ae7e"
    task_id = "task-b1764261dff54214aace3932bde8ae7e-0"

    status_file_path = os.path.join(
        checkpoint_dir, f"{MOCK_QUEUE_ID}_ignore-storage-profiles_download_status.json"
    )

    # Pre-populate the status file with a farm_failed entry from a prior run.
    os.makedirs(checkpoint_dir, exist_ok=True)
    with open(status_file_path, "w") as f:
        json.dump(
            {
                "schema_version": 1,
                "sync_metadata": {
                    "queue_id": MOCK_QUEUE_ID,
                    "storage_profile_id": None,
                    "last_sync_completed_at": "2025-08-06T00:00:00+00:00",
                    "last_run_status": "success",
                    "hostname": "worker",
                },
                "jobs": {
                    MOCK_JOB_ID: {
                        "queue_id": MOCK_QUEUE_ID,
                        "download_status": "downloaded",
                        "total_files": 0,
                        "downloaded_files": 0,
                        "failed_files": 0,
                        "last_updated": "2025-08-06T00:00:00+00:00",
                        "error_code": None,
                        "error_message": None,
                        "skip_reason": None,
                        "tasks": {
                            task_id: {
                                "download_status": "farm_failed",
                                "total_files": 0,
                                "downloaded_files": 0,
                                "error_code": None,
                                "error_message": None,
                            }
                        },
                    }
                },
            },
            f,
        )

    # Run 2: same task now SUCCEEDS with no output manifest.
    mock_jobs = create_fake_job_list(1)
    mock_jobs[0]["name"] = "Mock Job"
    mock_jobs[0]["jobId"] = MOCK_JOB_ID
    mock_jobs[0]["taskRunStatus"] = "SUCCEEDED"
    mock_jobs[0]["taskRunStatusCounts"] = {"SUCCEEDED": 1, "FAILED": 0, "READY": 0}
    mock_jobs[0]["attachments"] = {
        "manifests": [
            {"rootPath": "/", "rootPathFormat": "posix", "outputRelativeDirectories": ["."]}
        ],
        "fileSystem": "COPIED",
    }
    mock_jobs[0]["endedAt"] = datetime.fromisoformat(ISO_FREEZE_TIME)
    deadline_mock.search_jobs = mock_search_jobs_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, mock_jobs)
    deadline_mock.get_job = mock_get_job_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, mock_jobs)
    deadline_mock.list_sessions.return_value = {
        "sessions": [
            {
                "sessionId": MOCK_SESSION_ID,
                "fleetId": MOCK_FLEET_ID,
                "workerId": MOCK_WORKER_ID,
                "startedAt": datetime.fromisoformat("2025-08-06T00:15:45.712000+00:00"),
                "endedAt": datetime.fromisoformat("2025-08-06T00:20:59.992000+00:00"),
                "lifecycleStatus": "ENDED",
            }
        ]
    }
    deadline_mock.list_session_actions.return_value = {
        "sessionActions": [
            {
                "sessionActionId": MOCK_SESSION_ACTION_ID_1,
                "status": "SUCCEEDED",
                "startedAt": "2025-08-06T00:20:58.454000+00:00",
                "endedAt": "2025-08-06T00:20:59.992000+00:00",
                "progressPercent": 100.0,
                "definition": {"taskRun": {"taskId": task_id, "stepId": step_id}},
                "manifests": [{}],
            }
        ]
    }

    runner = CliRunner()
    with freeze_time(ISO_FREEZE_TIME):
        result = runner.invoke(
            main,
            [
                "queue",
                "sync-output",
                "--ignore-storage-profiles",
                "--force-bootstrap",
                "--bootstrap-lookback-minutes",
                "120",
                "--farm-id",
                MOCK_FARM_ID,
                "--queue-id",
                MOCK_QUEUE_ID,
                "--checkpoint-dir",
                checkpoint_dir,
            ],
        )

    assert result.exit_code == 0, result.output

    with open(status_file_path) as f:
        status = json.load(f)

    tasks = status["jobs"][MOCK_JOB_ID]["tasks"]
    # The stale farm_failed must be cleared — the task SUCCEEDED, even with no output.
    # The entry is removed entirely (consistent with succeeded-with-no-output tasks never
    # being recorded in the first place) rather than being rewritten as "downloaded".
    assert task_id not in tasks, tasks


@pytest.mark.skipif(
    sys.version_info < (3, 9), reason="Incremental output download requires Python >= 3.9"
)
def test_incremental_output_download_leftover_non_task_paths_are_downloaded(
    fresh_deadline_config, deadline_mock, checkpoint_dir, tmp_path
):
    """A job with a mix of task-attributed and non-task-attributed manifests must
    download both. The task-attributed path goes through per-task isolation; the
    leftover path (its manifest key carries no step-/task- segment) must still be
    downloaded rather than silently dropped just because the job also has task files.
    """
    from deadline.job_attachments.asset_manifests.v2023_03_03.asset_manifest import (
        AssetManifest,
        ManifestPath,
    )
    from deadline.job_attachments.asset_manifests import HashAlgorithm

    step_id = "step-b1764261dff54214aace3932bde8ae7e"
    task_id = "task-b1764261dff54214aace3932bde8ae7e-0"
    task_file_path = str(tmp_path / "frame_0" / "beauty.exr")
    leftover_file_path = str(tmp_path / "aux" / "report.txt")

    mock_jobs = create_fake_job_list(1)
    mock_jobs[0]["name"] = "Mock Job"
    mock_jobs[0]["jobId"] = MOCK_JOB_ID
    mock_jobs[0]["taskRunStatus"] = "SUCCEEDED"
    mock_jobs[0]["taskRunStatusCounts"] = {"SUCCEEDED": 1, "READY": 0}
    mock_jobs[0]["attachments"] = {
        "manifests": [
            {"rootPath": "/", "rootPathFormat": "posix", "outputRelativeDirectories": ["."]}
        ],
        "fileSystem": "COPIED",
    }
    mock_jobs[0]["endedAt"] = datetime.fromisoformat(ISO_FREEZE_TIME)
    deadline_mock.search_jobs = mock_search_jobs_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, mock_jobs)
    deadline_mock.get_job = mock_get_job_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, mock_jobs)

    deadline_mock.list_sessions.return_value = {
        "sessions": [
            {
                "sessionId": MOCK_SESSION_ID,
                "fleetId": MOCK_FLEET_ID,
                "workerId": MOCK_WORKER_ID,
                "startedAt": datetime.fromisoformat("2025-08-06T00:15:45.712000+00:00"),
                "endedAt": datetime.fromisoformat("2025-08-06T00:20:59.992000+00:00"),
                "lifecycleStatus": "ENDED",
            }
        ]
    }
    deadline_mock.list_session_actions.return_value = {
        "sessionActions": [
            {
                "sessionActionId": "sessionaction-0123456789abcdefabcdefabcdefabcd-0",
                "status": "SUCCEEDED",
                "startedAt": "2025-08-06T00:20:58.454000+00:00",
                "endedAt": "2025-08-06T00:20:59.992000+00:00",
                "progressPercent": 100.0,
                "definition": {"taskRun": {"taskId": task_id, "stepId": step_id}},
                "manifests": [{"outputManifestPath": f"{task_id}/manifest"}],
            }
        ]
    }

    downloaded_manifests = [
        (
            datetime.fromisoformat(ISO_FREEZE_TIME),
            AssetManifest(
                hash_alg=HashAlgorithm.XXH128,
                total_size=1,
                paths=[ManifestPath(path=task_file_path, hash="h", size=1, mtime=1)],
            ),
        ),
        (
            datetime.fromisoformat(ISO_FREEZE_TIME),
            AssetManifest(
                hash_alg=HashAlgorithm.XXH128,
                total_size=1,
                paths=[ManifestPath(path=leftover_file_path, hash="h2", size=1, mtime=1)],
            ),
        ),
    ]
    # First key carries a step-/task- segment (attributed); second does not (leftover).
    manifests_to_download = [
        (None, MOCK_JOB_ID, "/", f"prefix/{step_id}/{task_id}/manifest"),
        (None, MOCK_JOB_ID, "/", "prefix/no-task-segment/manifest"),
    ]

    def fake_download_all_manifests(*args, **kwargs):
        return downloaded_manifests

    def fake_get_manifests_to_download(*args, **kwargs):
        return manifests_to_download

    downloaded_paths: list[str] = []

    def fake_download_manifest_paths(
        files,
        hash_algorithm,
        queue,
        session,
        conflict,
        on_downloading_files,
        print_function_callback,
    ):
        for f in files:
            downloaded_paths.append(f.path)
            os.makedirs(os.path.dirname(f.path), exist_ok=True)
            with open(f.path, "w") as fh:
                fh.write("output")

    runner = CliRunner()
    with (
        patch(
            "deadline.client.cli._incremental_download._download_all_manifests_with_absolute_paths",
            side_effect=fake_download_all_manifests,
        ),
        patch(
            "deadline.client.cli._incremental_download._get_manifests_to_download",
            side_effect=fake_get_manifests_to_download,
        ),
        patch(
            "deadline.client.cli._incremental_download._download_manifest_paths",
            side_effect=fake_download_manifest_paths,
        ),
        freeze_time(ISO_FREEZE_TIME),
    ):
        result = runner.invoke(
            main,
            [
                "queue",
                "sync-output",
                "--ignore-storage-profiles",
                "--force-bootstrap",
                "--bootstrap-lookback-minutes",
                "120",
                "--farm-id",
                MOCK_FARM_ID,
                "--queue-id",
                MOCK_QUEUE_ID,
                "--checkpoint-dir",
                checkpoint_dir,
            ],
        )

    assert result.exit_code == 0, result.output
    # Both the task-attributed file and the leftover file must have been downloaded.
    assert task_file_path in downloaded_paths, downloaded_paths
    assert leftover_file_path in downloaded_paths, downloaded_paths
    assert os.path.exists(leftover_file_path), "leftover non-task path was silently dropped"
    assert "Downloaded files: 2" in result.output, result.output


@pytest.mark.skipif(
    sys.version_info < (3, 9), reason="Incremental output download requires Python >= 3.9"
)
def test_incremental_output_download_same_path_across_tasks_downloaded_once(
    fresh_deadline_config, deadline_mock, checkpoint_dir, tmp_path
):
    """When two tasks of the same job emit the same output path (e.g. a requeue re-ran
    a task under a new task id), the path must be downloaded once and attributed to the
    newer task — not downloaded twice and double-counted.
    """
    from deadline.job_attachments.asset_manifests.v2023_03_03.asset_manifest import (
        AssetManifest,
        ManifestPath,
    )
    from deadline.job_attachments.asset_manifests import HashAlgorithm

    step_id = "step-b1764261dff54214aace3932bde8ae7e"
    task_ids = [f"task-b1764261dff54214aace3932bde8ae7e-{i}" for i in range(2)]
    shared_file_path = str(tmp_path / "frame_0" / "beauty.exr")

    mock_jobs = create_fake_job_list(1)
    mock_jobs[0]["name"] = "Mock Job"
    mock_jobs[0]["jobId"] = MOCK_JOB_ID
    mock_jobs[0]["taskRunStatus"] = "SUCCEEDED"
    mock_jobs[0]["taskRunStatusCounts"] = {"SUCCEEDED": 2, "READY": 0}
    mock_jobs[0]["attachments"] = {
        "manifests": [
            {"rootPath": "/", "rootPathFormat": "posix", "outputRelativeDirectories": ["."]}
        ],
        "fileSystem": "COPIED",
    }
    mock_jobs[0]["endedAt"] = datetime.fromisoformat(ISO_FREEZE_TIME)
    deadline_mock.search_jobs = mock_search_jobs_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, mock_jobs)
    deadline_mock.get_job = mock_get_job_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, mock_jobs)

    deadline_mock.list_sessions.return_value = {
        "sessions": [
            {
                "sessionId": MOCK_SESSION_ID,
                "fleetId": MOCK_FLEET_ID,
                "workerId": MOCK_WORKER_ID,
                "startedAt": datetime.fromisoformat("2025-08-06T00:15:45.712000+00:00"),
                "endedAt": datetime.fromisoformat("2025-08-06T00:20:59.992000+00:00"),
                "lifecycleStatus": "ENDED",
            }
        ]
    }
    deadline_mock.list_session_actions.return_value = {
        "sessionActions": [
            {
                "sessionActionId": f"sessionaction-0123456789abcdefabcdefabcdefabcd-{i}",
                "status": "SUCCEEDED",
                "startedAt": "2025-08-06T00:20:58.454000+00:00",
                "endedAt": "2025-08-06T00:20:59.992000+00:00",
                "progressPercent": 100.0,
                "definition": {"taskRun": {"taskId": task_ids[i], "stepId": step_id}},
                "manifests": [{"outputManifestPath": f"{task_ids[i]}/manifest"}],
            }
            for i in range(2)
        ]
    }

    # Two manifests, older then newer, both writing the SAME path under different tasks.
    downloaded_manifests = [
        (
            datetime.fromisoformat("2025-08-06T00:10:00.000000+00:00"),
            AssetManifest(
                hash_alg=HashAlgorithm.XXH128,
                total_size=1,
                paths=[ManifestPath(path=shared_file_path, hash="h", size=1, mtime=1)],
            ),
        ),
        (
            datetime.fromisoformat("2025-08-06T00:20:00.000000+00:00"),
            AssetManifest(
                hash_alg=HashAlgorithm.XXH128,
                total_size=1,
                paths=[ManifestPath(path=shared_file_path, hash="h", size=1, mtime=1)],
            ),
        ),
    ]
    manifests_to_download = [
        (None, MOCK_JOB_ID, "/", f"prefix/{step_id}/{task_ids[i]}/manifest") for i in range(2)
    ]

    def fake_download_all_manifests(*args, **kwargs):
        return downloaded_manifests

    def fake_get_manifests_to_download(*args, **kwargs):
        return manifests_to_download

    downloaded_paths: list[str] = []

    def fake_download_manifest_paths(
        files,
        hash_algorithm,
        queue,
        session,
        conflict,
        on_downloading_files,
        print_function_callback,
    ):
        for f in files:
            downloaded_paths.append(f.path)
            os.makedirs(os.path.dirname(f.path), exist_ok=True)
            with open(f.path, "w") as fh:
                fh.write("output")

    runner = CliRunner()
    with (
        patch(
            "deadline.client.cli._incremental_download._download_all_manifests_with_absolute_paths",
            side_effect=fake_download_all_manifests,
        ),
        patch(
            "deadline.client.cli._incremental_download._get_manifests_to_download",
            side_effect=fake_get_manifests_to_download,
        ),
        patch(
            "deadline.client.cli._incremental_download._download_manifest_paths",
            side_effect=fake_download_manifest_paths,
        ),
        freeze_time(ISO_FREEZE_TIME),
    ):
        result = runner.invoke(
            main,
            [
                "queue",
                "sync-output",
                "--ignore-storage-profiles",
                "--force-bootstrap",
                "--bootstrap-lookback-minutes",
                "120",
                "--farm-id",
                MOCK_FARM_ID,
                "--queue-id",
                MOCK_QUEUE_ID,
                "--checkpoint-dir",
                checkpoint_dir,
            ],
        )

    assert result.exit_code == 0, result.output
    # The shared path is downloaded once, not once per task.
    assert downloaded_paths.count(shared_file_path) == 1, downloaded_paths
    assert "Downloaded files: 1" in result.output, result.output

    # It is attributed to the newer task only; the older task no longer owns it.
    status_file_path = os.path.join(
        checkpoint_dir, f"{MOCK_QUEUE_ID}_ignore-storage-profiles_download_status.json"
    )
    with open(status_file_path) as f:
        status = json.load(f)
    tasks = status["jobs"][MOCK_JOB_ID]["tasks"]
    assert task_ids[1] in tasks, tasks
    assert tasks[task_ids[1]]["downloaded_files"] == 1, tasks
    assert task_ids[0] not in tasks, tasks


@pytest.mark.skipif(
    sys.version_info < (3, 9), reason="Incremental output download requires Python >= 3.9"
)
def test_incremental_output_download_shared_path_newest_wins_regardless_of_order(
    fresh_deadline_config, deadline_mock, checkpoint_dir, tmp_path
):
    """When two jobs write the same output path, the job with the newer manifest
    (by S3 LastModified) is credited — even when its manifest appears earlier in the
    download list. The attribution loop must sort by timestamp, not rely on list order.
    """
    from deadline.job_attachments.asset_manifests.v2023_03_03.asset_manifest import (
        AssetManifest,
        ManifestPath,
    )
    from deadline.job_attachments.asset_manifests import HashAlgorithm

    job_id_old = MOCK_JOB_ID
    job_id_new = "job-0123456789abcdefabcdefabcdefab99"
    step_id = "step-b1764261dff54214aace3932bde8ae7e"
    task_id_old = "task-b1764261dff54214aace3932bde8ae7e-0"
    task_id_new = "task-b1764261dff54214aace3932bde8ae7e-1"
    shared_path = str(tmp_path / "shared" / "beauty.exr")

    mock_jobs = create_fake_job_list(2)
    for job, jid, name in (
        (mock_jobs[0], job_id_old, "Old Job"),
        (mock_jobs[1], job_id_new, "New Job"),
    ):
        job["name"] = name
        job["jobId"] = jid
        job["taskRunStatus"] = "SUCCEEDED"
        job["taskRunStatusCounts"] = {"SUCCEEDED": 1, "READY": 0}
        job["attachments"] = {
            "manifests": [
                {"rootPath": "/", "rootPathFormat": "posix", "outputRelativeDirectories": ["."]}
            ],
            "fileSystem": "COPIED",
        }
        job["endedAt"] = datetime.fromisoformat(ISO_FREEZE_TIME)
    deadline_mock.search_jobs = mock_search_jobs_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, mock_jobs)
    deadline_mock.get_job = mock_get_job_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, mock_jobs)

    def sessions_for(job_id):
        return {
            "sessions": [
                {
                    "sessionId": MOCK_SESSION_ID,
                    "fleetId": MOCK_FLEET_ID,
                    "workerId": MOCK_WORKER_ID,
                    "startedAt": datetime.fromisoformat("2025-08-06T00:15:45.712000+00:00"),
                    "endedAt": datetime.fromisoformat("2025-08-06T00:20:59.992000+00:00"),
                    "lifecycleStatus": "ENDED",
                }
            ]
        }

    task_by_job = {job_id_old: task_id_old, job_id_new: task_id_new}
    deadline_mock.list_sessions.side_effect = lambda **kwargs: sessions_for(kwargs.get("jobId"))
    deadline_mock.list_session_actions.side_effect = lambda **kwargs: {
        "sessionActions": [
            {
                "sessionActionId": "sessionaction-0123456789abcdefabcdefabcdefabcd-0",
                "status": "SUCCEEDED",
                "startedAt": "2025-08-06T00:20:58.454000+00:00",
                "endedAt": "2025-08-06T00:20:59.992000+00:00",
                "progressPercent": 100.0,
                "definition": {
                    "taskRun": {"taskId": task_by_job[kwargs.get("jobId")], "stepId": step_id}
                },
                "manifests": [
                    {"outputManifestPath": f"{task_by_job[kwargs.get('jobId')]}/manifest"}
                ],
            }
        ]
    }

    older_ts = datetime.fromisoformat(ISO_FREEZE_TIME_MINUS_5MIN)
    newer_ts = datetime.fromisoformat(ISO_FREEZE_TIME)

    def make_manifest():
        return AssetManifest(
            hash_alg=HashAlgorithm.XXH128,
            total_size=1,
            paths=[ManifestPath(path=shared_path, hash="h", size=1, mtime=1)],
        )

    # The NEW job's manifest (newer timestamp) is placed FIRST — list order is the
    # reverse of chronological order, so relying on iteration order would credit the
    # OLD job. The fix sorts by timestamp so the NEW job wins.
    downloaded_manifests = [
        (newer_ts, make_manifest()),
        (older_ts, make_manifest()),
    ]
    manifests_to_download = [
        (None, job_id_new, "/", f"prefix/{step_id}/{task_id_new}/manifest"),
        (None, job_id_old, "/", f"prefix/{step_id}/{task_id_old}/manifest"),
    ]

    def fake_download_all_manifests(*args, **kwargs):
        return downloaded_manifests

    def fake_get_manifests_to_download(*args, **kwargs):
        return manifests_to_download

    def fake_download_manifest_paths(
        files,
        hash_algorithm,
        queue,
        session,
        conflict,
        on_downloading_files,
        print_function_callback,
    ):
        for f in files:
            os.makedirs(os.path.dirname(f.path), exist_ok=True)
            with open(f.path, "w") as fh:
                fh.write("output")

    runner = CliRunner()
    with (
        patch(
            "deadline.client.cli._incremental_download._download_all_manifests_with_absolute_paths",
            side_effect=fake_download_all_manifests,
        ),
        patch(
            "deadline.client.cli._incremental_download._get_manifests_to_download",
            side_effect=fake_get_manifests_to_download,
        ),
        patch(
            "deadline.client.cli._incremental_download._download_manifest_paths",
            side_effect=fake_download_manifest_paths,
        ),
        freeze_time(ISO_FREEZE_TIME),
    ):
        result = runner.invoke(
            main,
            [
                "queue",
                "sync-output",
                "--ignore-storage-profiles",
                "--force-bootstrap",
                "--bootstrap-lookback-minutes",
                "120",
                "--farm-id",
                MOCK_FARM_ID,
                "--queue-id",
                MOCK_QUEUE_ID,
                "--checkpoint-dir",
                checkpoint_dir,
            ],
        )

    assert result.exit_code == 0, result.output

    # The newer job owns the shared path and performs the actual download; the older job
    # is deduped out and downloads nothing. With list-order attribution (the bug), the
    # older job — appearing later in the list — would incorrectly claim the path.
    assert "for job: New Job" in result.output, result.output
    assert "for job: Old Job" not in result.output, result.output

    # The deduped (losing) job must still report filesystem-based status, not total=0/downloaded=0.
    # Its paths were claimed by the winning job and pruned from the download lists, so its status
    # depends on all_task_file_paths being retained (not derived from the pruned lists). The shared
    # file is on disk, so the loser reports it as downloaded.
    status_file_path = os.path.join(
        checkpoint_dir, f"{MOCK_QUEUE_ID}_ignore-storage-profiles_download_status.json"
    )
    with open(status_file_path) as f:
        status = json.load(f)
    loser_entry = status["jobs"][job_id_old]
    assert loser_entry["download_status"] == "downloaded", loser_entry
    assert loser_entry["total_files"] == 1, loser_entry
    assert loser_entry["downloaded_files"] == 1, loser_entry
    assert loser_entry["tasks"][task_id_old]["downloaded_files"] == 1, loser_entry


@pytest.mark.skipif(
    sys.version_info < (3, 9), reason="Incremental output download requires Python >= 3.9"
)
def test_incremental_output_download_deduped_job_inherits_winning_jobs_error(
    fresh_deadline_config, deadline_mock, checkpoint_dir, tmp_path
):
    """When the job that WON a shared output path fails to download it, the deduped (losing)
    job reports the winner's actual error rather than a generic UNKNOWN.

    The loser performs no download of its own, so its status is synthesized from the
    filesystem. The file is missing, and the reason is the winner's failure — surfacing
    UNKNOWN there would tell the artist "not found on disk" when the real cause is a
    permission error, sending them down the wrong path.
    """
    from deadline.job_attachments.asset_manifests.v2023_03_03.asset_manifest import (
        AssetManifest,
        ManifestPath,
    )
    from deadline.job_attachments.asset_manifests import HashAlgorithm

    job_id_old = MOCK_JOB_ID
    job_id_new = "job-0123456789abcdefabcdefabcdefab99"
    task_id_old = "task-b1764261dff54214aace3932bde8ae7e-0"
    task_id_new = "task-b1764261dff54214aace3932bde8ae7e-1"
    shared_path = str(tmp_path / "shared" / "beauty.exr")

    mock_jobs = create_fake_job_list(2)
    for job, jid, name in (
        (mock_jobs[0], job_id_old, "Old Job"),
        (mock_jobs[1], job_id_new, "New Job"),
    ):
        job["name"] = name
        job["jobId"] = jid
        job["taskRunStatus"] = "SUCCEEDED"
        job["taskRunStatusCounts"] = {"SUCCEEDED": 1, "READY": 0}
        job["attachments"] = {
            "manifests": [
                {"rootPath": "/", "rootPathFormat": "posix", "outputRelativeDirectories": ["."]}
            ],
            "fileSystem": "COPIED",
        }
        job["endedAt"] = datetime.fromisoformat(ISO_FREEZE_TIME)
    deadline_mock.search_jobs = mock_search_jobs_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, mock_jobs)
    deadline_mock.get_job = mock_get_job_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, mock_jobs)
    deadline_mock.list_sessions.return_value = {
        "sessions": [
            {
                "sessionId": MOCK_SESSION_ID,
                "fleetId": MOCK_FLEET_ID,
                "workerId": MOCK_WORKER_ID,
                "startedAt": datetime.fromisoformat("2025-08-06T00:15:45.712000+00:00"),
                "endedAt": datetime.fromisoformat("2025-08-06T00:20:59.992000+00:00"),
                "lifecycleStatus": "ENDED",
            }
        ]
    }
    task_by_job = {job_id_old: task_id_old, job_id_new: task_id_new}
    deadline_mock.list_session_actions.side_effect = lambda **kwargs: {
        "sessionActions": [
            {
                "sessionActionId": "sessionaction-0123456789abcdefabcdefabcdefabcd-0",
                "status": "SUCCEEDED",
                "startedAt": "2025-08-06T00:20:58.454000+00:00",
                "endedAt": "2025-08-06T00:20:59.992000+00:00",
                "progressPercent": 100.0,
                "definition": {
                    "taskRun": {"taskId": task_by_job[kwargs.get("jobId")], "stepId": _STEP_ID}
                },
                "manifests": [
                    {"outputManifestPath": f"{task_by_job[kwargs.get('jobId')]}/manifest"}
                ],
            }
        ]
    }

    def make_manifest():
        return AssetManifest(
            hash_alg=HashAlgorithm.XXH128,
            total_size=1,
            paths=[ManifestPath(path=shared_path, hash="h", size=1, mtime=1)],
        )

    # The newer job wins the shared path; the older job is deduped out.
    downloaded_manifests = [
        (datetime.fromisoformat(ISO_FREEZE_TIME), make_manifest()),
        (datetime.fromisoformat(ISO_FREEZE_TIME_MINUS_5MIN), make_manifest()),
    ]
    manifests_to_download = [
        (None, job_id_new, "/", f"prefix/{_STEP_ID}/{task_id_new}/manifest"),
        (None, job_id_old, "/", f"prefix/{_STEP_ID}/{task_id_old}/manifest"),
    ]

    def failing_downloader(
        files,
        hash_algorithm,
        queue,
        session,
        conflict,
        on_downloading_files,
        print_function_callback,
    ):
        # The winner's download fails, so the shared file never lands on disk.
        raise PermissionError(f"[Errno 13] Permission denied: '{shared_path}'")

    runner = CliRunner()
    with (
        patch(
            "deadline.client.cli._incremental_download._download_all_manifests_with_absolute_paths",
            side_effect=lambda *a, **k: downloaded_manifests,
        ),
        patch(
            "deadline.client.cli._incremental_download._get_manifests_to_download",
            side_effect=lambda *a, **k: manifests_to_download,
        ),
        patch(
            "deadline.client.cli._incremental_download._download_manifest_paths",
            side_effect=failing_downloader,
        ),
        freeze_time(ISO_FREEZE_TIME),
    ):
        result = runner.invoke(
            main,
            [
                "queue",
                "sync-output",
                "--ignore-storage-profiles",
                "--force-bootstrap",
                "--bootstrap-lookback-minutes",
                "120",
                "--farm-id",
                MOCK_FARM_ID,
                "--queue-id",
                MOCK_QUEUE_ID,
                "--checkpoint-dir",
                checkpoint_dir,
            ],
        )

    assert result.exit_code == 0, result.output
    assert not os.path.exists(shared_path), "the winner's download was supposed to fail"

    with open(_ignore_profiles_status_file(checkpoint_dir)) as f:
        status = json.load(f)

    # The winning job reports its own download failure.
    winner_entry = status["jobs"][job_id_new]
    assert winner_entry["download_status"] == "failed", winner_entry
    assert winner_entry["error_code"] == "PERMISSION_DENIED", winner_entry

    # The losing job downloaded nothing itself, so its status is synthesized from the
    # filesystem — and it inherits the winner's real error for the shared path, NOT UNKNOWN.
    loser_task = status["jobs"][job_id_old]["tasks"][task_id_old]
    assert loser_task["error_code"] == "PERMISSION_DENIED", loser_task
    assert loser_task["total_files"] == 1, loser_task
    assert loser_task["downloaded_files"] == 0, loser_task


class _TwoJobEnv(NamedTuple):
    """Mocks for a two-job run where each job has one task writing one or more files."""

    mock_jobs: list
    downloaded_manifests: list
    manifests_to_download: list
    downloader: Callable
    downloaded_paths: list
    job_ids: list
    task_ids: list
    job_file_paths: list  # job_file_paths[i] = output paths owned by job i


def _two_job_download_env(
    tmp_path,
    files_per_job=(1, 1),
    failing_job_path=None,
    cancel=False,
    cancel_after_path=None,
    sigint_only_after_path=None,
):
    """Builds the mocks for a two-job run, each job having one task.

    ``files_per_job`` sets how many output files each job writes, which is what makes
    per-job count attribution observable. If ``failing_job_path`` is given the fake
    downloader raises PermissionError for that path; if ``cancel`` is True it raises
    AssetSyncCancelledError for every path.

    ``cancel_after_path`` models a real Ctrl+C: that path downloads and is written to disk,
    then the SIGINT flag is cleared and AssetSyncCancelledError is raised for the next path.
    Use it to observe what survives a cancellation partway through a run.

    ``sigint_only_after_path`` clears the SIGINT flag after that path downloads but lets the
    downloader keep returning normally — the race where the signal arrives just as a call
    finishes, so no AssetSyncCancelledError comes up from underneath.
    """
    from deadline.job_attachments.asset_manifests.v2023_03_03.asset_manifest import (
        AssetManifest,
        ManifestPath,
    )
    from deadline.job_attachments.asset_manifests import HashAlgorithm
    from deadline.job_attachments.exceptions import AssetSyncCancelledError

    job_ids = [MOCK_JOB_ID, "job-0123456789abcdefabcdefabcdefab99"]
    task_ids = [f"task-b1764261dff54214aace3932bde8ae7e-{i}" for i in range(2)]
    job_file_paths = [
        [str(tmp_path / f"job_{i}" / _OUTPUT_FILE_NAMES[k]) for k in range(files_per_job[i])]
        for i in range(2)
    ]

    mock_jobs = create_fake_job_list(2)
    for i, (job, jid) in enumerate(zip(mock_jobs, job_ids)):
        job["name"] = f"Job {i}"
        job["jobId"] = jid
        job["taskRunStatus"] = "SUCCEEDED"
        job["taskRunStatusCounts"] = {"SUCCEEDED": 1, "READY": 0}
        job["attachments"] = {
            "manifests": [
                {"rootPath": "/", "rootPathFormat": "posix", "outputRelativeDirectories": ["."]}
            ],
            "fileSystem": "COPIED",
        }
        job["endedAt"] = datetime.fromisoformat(ISO_FREEZE_TIME)

    downloaded_manifests = [
        (
            datetime.fromisoformat(ISO_FREEZE_TIME),
            AssetManifest(
                hash_alg=HashAlgorithm.XXH128,
                total_size=len(job_file_paths[i]),
                paths=[ManifestPath(path=p, hash="h", size=1, mtime=1) for p in job_file_paths[i]],
            ),
        )
        for i in range(2)
    ]
    manifests_to_download = [
        (None, job_ids[i], "/", f"prefix/{_STEP_ID}/{task_ids[i]}/manifest") for i in range(2)
    ]

    downloaded_paths: list[str] = []
    cancel_requested: list[bool] = []

    def fake_download_manifest_paths(
        files,
        hash_algorithm,
        queue,
        session,
        conflict,
        on_downloading_files,
        print_function_callback,
    ):
        from deadline.client.cli._common import sigint_handler

        for f in files:
            if cancel:
                raise AssetSyncCancelledError("File download cancelled.")
            # Ctrl+C already delivered on an earlier path: everything after it aborts, which is
            # what job_attachments does once its progress callback returns False.
            if cancel_requested and cancel_after_path is not None:
                raise AssetSyncCancelledError("File download cancelled.")
            if failing_job_path is not None and f.path == failing_job_path:
                raise PermissionError(f"[Errno 13] Permission denied: '{f.path}'")
            downloaded_paths.append(f.path)
            os.makedirs(os.path.dirname(f.path), exist_ok=True)
            with open(f.path, "w") as fh:
                fh.write("output")
            if f.path in (cancel_after_path, sigint_only_after_path):
                # The file landed on disk before the signal — that output must survive.
                sigint_handler.continue_operation = False
                cancel_requested.append(True)

    return _TwoJobEnv(
        mock_jobs=mock_jobs,
        downloaded_manifests=downloaded_manifests,
        manifests_to_download=manifests_to_download,
        downloader=fake_download_manifest_paths,
        downloaded_paths=downloaded_paths,
        job_ids=job_ids,
        task_ids=task_ids,
        job_file_paths=job_file_paths,
    )


def _run_two_job_sync(deadline_mock, checkpoint_dir, env):
    """Wires ``env``'s mocks onto deadline_mock and runs `queue sync-output` to completion.

    Each job reports a single succeeded task run whose output manifest is the one
    ``env`` built for it, so the attribution path sees one manifest per job.
    """
    deadline_mock.search_jobs = mock_search_jobs_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, env.mock_jobs)
    deadline_mock.get_job = mock_get_job_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, env.mock_jobs)
    deadline_mock.list_sessions.return_value = {
        "sessions": [
            {
                "sessionId": MOCK_SESSION_ID,
                "fleetId": MOCK_FLEET_ID,
                "workerId": MOCK_WORKER_ID,
                "startedAt": datetime.fromisoformat("2025-08-06T00:15:45.712000+00:00"),
                "endedAt": datetime.fromisoformat("2025-08-06T00:20:59.992000+00:00"),
                "lifecycleStatus": "ENDED",
            }
        ]
    }
    task_by_job = dict(zip(env.job_ids, env.task_ids))
    deadline_mock.list_session_actions.side_effect = lambda **kwargs: {
        "sessionActions": [
            {
                "sessionActionId": "sessionaction-0123456789abcdefabcdefabcdefabcd-0",
                "status": "SUCCEEDED",
                "startedAt": "2025-08-06T00:20:58.454000+00:00",
                "endedAt": "2025-08-06T00:20:59.992000+00:00",
                "progressPercent": 100.0,
                "definition": {
                    "taskRun": {"taskId": task_by_job[kwargs.get("jobId")], "stepId": _STEP_ID}
                },
                "manifests": [
                    {"outputManifestPath": f"{task_by_job[kwargs.get('jobId')]}/manifest"}
                ],
            }
        ]
    }

    runner = CliRunner()
    with (
        patch(
            "deadline.client.cli._incremental_download._download_all_manifests_with_absolute_paths",
            side_effect=lambda *a, **k: env.downloaded_manifests,
        ),
        patch(
            "deadline.client.cli._incremental_download._get_manifests_to_download",
            side_effect=lambda *a, **k: env.manifests_to_download,
        ),
        patch(
            "deadline.client.cli._incremental_download._download_manifest_paths",
            side_effect=env.downloader,
        ),
        freeze_time(ISO_FREEZE_TIME),
    ):
        return runner.invoke(
            main,
            [
                "queue",
                "sync-output",
                "--ignore-storage-profiles",
                "--force-bootstrap",
                "--bootstrap-lookback-minutes",
                "120",
                "--farm-id",
                MOCK_FARM_ID,
                "--queue-id",
                MOCK_QUEUE_ID,
                "--checkpoint-dir",
                checkpoint_dir,
            ],
        )


@pytest.mark.skipif(
    sys.version_info < (3, 9), reason="Incremental output download requires Python >= 3.9"
)
def test_incremental_output_download_cross_job_error_isolation(
    fresh_deadline_config, deadline_mock, checkpoint_dir, tmp_path
):
    """Two independent jobs, one raises on download: the other job's files still download
    and it reports 'downloaded', while the failing job reports 'failed'. A failure in one
    job must not taint a sibling job in the same run."""
    env = _two_job_download_env(
        tmp_path, failing_job_path=str(tmp_path / "job_0" / _OUTPUT_FILE_NAMES[0])
    )

    result = _run_two_job_sync(deadline_mock, checkpoint_dir, env)

    assert result.exit_code == 0, result.output
    # The healthy job's file downloaded despite the sibling's failure.
    assert env.job_file_paths[1][0] in env.downloaded_paths, env.downloaded_paths

    with open(_ignore_profiles_status_file(checkpoint_dir)) as f:
        status = json.load(f)

    jobs = status["jobs"]
    assert jobs[env.job_ids[0]]["download_status"] == "failed", jobs
    assert jobs[env.job_ids[0]]["error_code"] == "PERMISSION_DENIED", jobs
    assert jobs[env.job_ids[1]]["download_status"] == "downloaded", jobs
    assert jobs[env.job_ids[1]]["error_code"] is None, jobs
    # The run is flagged failed overall because at least one job failed.
    assert status["sync_metadata"]["last_run_status"] == "failed", status["sync_metadata"]


@pytest.mark.skipif(
    sys.version_info < (3, 9), reason="Incremental output download requires Python >= 3.9"
)
def test_incremental_output_download_cancellation_propagates(
    fresh_deadline_config, deadline_mock, checkpoint_dir, tmp_path
):
    """An AssetSyncCancelledError from the downloader propagates as a cancellation of the
    whole run — it is NOT swallowed and recorded as a per-job download failure."""
    from deadline.job_attachments.exceptions import AssetSyncCancelledError

    env = _two_job_download_env(tmp_path, cancel=True)

    result = _run_two_job_sync(deadline_mock, checkpoint_dir, env)

    # Cancellation aborts the run (non-zero exit) rather than completing successfully and
    # recording per-job download failures. The AssetSyncCancelledError propagates out of the
    # download loop and is surfaced by the CLI's error handler.
    assert result.exit_code != 0, result.output
    assert AssetSyncCancelledError.__name__ in result.output, result.output
    assert not env.downloaded_paths, "cancellation must abort before writing outputs"
    # No status file claims these jobs succeeded — the run never reached a clean completion.
    assert not os.path.exists(_ignore_profiles_status_file(checkpoint_dir)), (
        "cancelled run must not write a status file"
    )


@pytest.mark.skipif(
    sys.version_info < (3, 9), reason="Incremental output download requires Python >= 3.9"
)
def test_incremental_output_download_cancellation_keeps_already_downloaded_files(
    fresh_deadline_config, deadline_mock, checkpoint_dir, tmp_path, restore_sigint_handler
):
    """Ctrl+C partway through: files already written stay on disk and the checkpoint does not
    advance.

    Cancellation must not roll back completed work — the artist keeps the frames that finished.
    But it also must not record them, because the checkpoint is the only thing that makes the
    next run re-scan this window. Advancing it on a partial run would strand the remaining
    frames until the job's session actions are rediscovered some other way.
    """
    from deadline.job_attachments.exceptions import AssetSyncCancelledError

    # Two jobs, two files each. The first path of job_0 downloads, then Ctrl+C lands.
    env = _two_job_download_env(
        tmp_path,
        files_per_job=(2, 2),
        cancel_after_path=str(tmp_path / "job_0" / _OUTPUT_FILE_NAMES[0]),
    )
    checkpoint_file = os.path.join(
        checkpoint_dir, f"{MOCK_QUEUE_ID}_ignore-storage-profiles_download_checkpoint.json"
    )
    assert not os.path.exists(checkpoint_file), "guard: forced bootstrap starts with no checkpoint"

    result = _run_two_job_sync(deadline_mock, checkpoint_dir, env)

    assert result.exit_code != 0, result.output
    assert AssetSyncCancelledError.__name__ in result.output, result.output
    # The file that finished before the signal is still on disk — cancellation is not a rollback.
    survivor = str(tmp_path / "job_0" / _OUTPUT_FILE_NAMES[0])
    assert survivor in env.downloaded_paths, env.downloaded_paths
    assert os.path.exists(survivor), "a file downloaded before Ctrl+C must not be removed"
    # Nothing was recorded, so the next run re-scans this window and picks up the rest.
    assert not os.path.exists(checkpoint_file), "cancelled run must not advance the checkpoint"
    assert not os.path.exists(_ignore_profiles_status_file(checkpoint_dir)), (
        "cancelled run must not write a status file"
    )


@pytest.mark.skipif(
    sys.version_info < (3, 9), reason="Incremental output download requires Python >= 3.9"
)
def test_incremental_output_download_cancellation_stops_remaining_work(
    fresh_deadline_config, deadline_mock, checkpoint_dir, tmp_path, restore_sigint_handler
):
    """Ctrl+C stops the downloads queued behind it rather than draining them, and records no
    partial success.

    The executor is shut down with cancel_futures=True and the loop breaks, so pending work
    never runs. Without that, Ctrl+C would appear to hang while every remaining file finished
    downloading — and any per-job result collected along the way would be written to the status
    file as a success.
    """
    # Three files in the cancelled job's task, so there is still work pending behind the file
    # the signal lands on.
    env = _two_job_download_env(
        tmp_path,
        files_per_job=(3, 3),
        cancel_after_path=str(tmp_path / "job_0" / _OUTPUT_FILE_NAMES[0]),
    )

    result = _run_two_job_sync(deadline_mock, checkpoint_dir, env)

    assert result.exit_code != 0, result.output
    # The two remaining files of the cancelled job's own task are abandoned, not drained. (Its
    # sibling job downloads concurrently, so how far that one got before the flag propagated is
    # a race — cancellation granularity is one download call, which is the real behavior.)
    assert str(tmp_path / "job_0" / _OUTPUT_FILE_NAMES[0]) in env.downloaded_paths
    for name in _OUTPUT_FILE_NAMES[1:3]:
        abandoned = str(tmp_path / "job_0" / name)
        assert abandoned not in env.downloaded_paths, env.downloaded_paths
        assert not os.path.exists(abandoned), (
            "cancellation must not keep downloading work queued behind it"
        )
    # No partial success is recorded for either job — no status file at all.
    assert not os.path.exists(_ignore_profiles_status_file(checkpoint_dir)), (
        "cancelled run must not record partial per-job results as success"
    )


@pytest.mark.skipif(
    sys.version_info < (3, 9), reason="Incremental output download requires Python >= 3.9"
)
def test_incremental_output_download_sigint_flag_alone_cancels_the_run(
    fresh_deadline_config, deadline_mock, checkpoint_dir, tmp_path, restore_sigint_handler
):
    """Ctrl+C that arrives just as a download call returns normally still cancels the run.

    job_attachments only raises AssetSyncCancelledError if the signal interrupts a transfer in
    progress. When it lands between calls the downloader returns success, so the flag is the
    only evidence. Trusting the return value alone would let the run finish, write a status file
    claiming every job downloaded, and advance the checkpoint past outputs never fetched.
    """
    from deadline.job_attachments.exceptions import AssetSyncCancelledError

    env = _two_job_download_env(
        tmp_path,
        files_per_job=(1, 1),
        sigint_only_after_path=str(tmp_path / "job_0" / _OUTPUT_FILE_NAMES[0]),
    )

    result = _run_two_job_sync(deadline_mock, checkpoint_dir, env)

    assert result.exit_code != 0, result.output
    assert AssetSyncCancelledError.__name__ in result.output, result.output
    # The downloader never raised — the post-call flag check is what turned this into a cancel.
    assert str(tmp_path / "job_0" / _OUTPUT_FILE_NAMES[0]) in env.downloaded_paths
    assert not os.path.exists(_ignore_profiles_status_file(checkpoint_dir)), (
        "a run cancelled by flag alone must not write a status file"
    )
    assert not os.path.exists(
        os.path.join(
            checkpoint_dir, f"{MOCK_QUEUE_ID}_ignore-storage-profiles_download_checkpoint.json"
        )
    ), "a run cancelled by flag alone must not advance the checkpoint"


@pytest.mark.skipif(
    sys.version_info < (3, 9), reason="Incremental output download requires Python >= 3.9"
)
def test_incremental_output_download_per_job_counts_are_correctly_attributed(
    fresh_deadline_config, deadline_mock, checkpoint_dir, tmp_path
):
    """Two jobs with DIFFERENT file counts download successfully, and each job's
    downloaded_files/total_files are attributed to that job — not lumped together.

    This pins the attribution block that correlates manifests_to_download with
    downloaded_manifests by index: job_0 owns 2 files, job_1 owns 1 file. A regression
    that mis-indexed the correlation (e.g. crediting every file to the last job) shows up
    here as wrong per-job counts even though the run-wide total is unchanged.
    """
    env = _two_job_download_env(tmp_path, files_per_job=(2, 1))

    result = _run_two_job_sync(deadline_mock, checkpoint_dir, env)

    assert result.exit_code == 0, result.output
    # All three files downloaded across the two jobs.
    assert sorted(env.downloaded_paths) == sorted(env.job_file_paths[0] + env.job_file_paths[1]), (
        env.downloaded_paths
    )

    with open(_ignore_profiles_status_file(checkpoint_dir)) as f:
        status = json.load(f)

    # The load-bearing assertion: counts are attributed per job, not lumped together.
    jobs = status["jobs"]
    for i, expected_count in enumerate((2, 1)):
        entry = jobs[env.job_ids[i]]
        assert entry["download_status"] == "downloaded", entry
        assert entry["total_files"] == expected_count, entry
        assert entry["downloaded_files"] == expected_count, entry
        # The per-task entry carries the same count, since each job has exactly one task.
        assert entry["tasks"][env.task_ids[i]]["downloaded_files"] == expected_count, entry
    assert status["sync_metadata"]["last_run_status"] == "success", status["sync_metadata"]


@pytest.mark.skipif(
    sys.version_info < (3, 9), reason="Incremental output download requires Python >= 3.9"
)
def test_incremental_output_download_retries_failed_job_via_get_job(
    fresh_deadline_config, deadline_mock, checkpoint_dir
):
    """A job tracked as previously-failed but outside the search_jobs window is re-fetched
    individually via GetJob and injected into the download candidates.

    A second tracked job that no longer exists (GetJob → ResourceNotFoundException) is
    dropped from the tracker instead of being retried forever. A third tracked job that has
    hit the retry cap is suppressed even though it is returned by search_jobs.
    """
    from botocore.exceptions import ClientError
    from deadline.client.cli._incremental_download import _MAX_FAILED_JOB_RETRIES

    retried_job_id = "job-0123456789abcdefabcdefabcdefab01"
    deleted_job_id = "job-0123456789abcdefabcdefabcdefab02"
    abandoned_job_id = "job-0123456789abcdefabcdefabcdefab03"

    # Pre-seed the failed-jobs tracker file the CLI reads on startup.
    failed_jobs_file = os.path.join(
        checkpoint_dir, f"{MOCK_QUEUE_ID}_ignore-storage-profiles_failed_jobs.json"
    )
    with open(failed_jobs_file, "w") as f:
        json.dump(
            {
                retried_job_id: 1,
                deleted_job_id: 1,
                abandoned_job_id: _MAX_FAILED_JOB_RETRIES,  # at the cap → abandoned
            },
            f,
        )

    # search_jobs returns only the abandoned job (in-window); the retried/deleted jobs are
    # outside the window and only reachable via GetJob.
    abandoned_job = create_fake_job_list(1)[0]
    abandoned_job["jobId"] = abandoned_job_id
    abandoned_job["name"] = "Abandoned Job"
    abandoned_job["taskRunStatus"] = "SUCCEEDED"
    abandoned_job["taskRunStatusCounts"] = {"SUCCEEDED": 1, "READY": 0}
    abandoned_job["attachments"] = {"manifests": []}
    abandoned_job["endedAt"] = datetime.fromisoformat(ISO_FREEZE_TIME)
    deadline_mock.search_jobs = mock_search_jobs_for_set(
        MOCK_FARM_ID, MOCK_QUEUE_ID, [abandoned_job]
    )

    retried_job = create_fake_job_list(1)[0]
    retried_job["jobId"] = retried_job_id
    retried_job["name"] = "Retried Job"
    retried_job["taskRunStatus"] = "SUCCEEDED"
    retried_job["taskRunStatusCounts"] = {"SUCCEEDED": 1, "READY": 0}
    retried_job["attachments"] = {"manifests": []}
    retried_job["endedAt"] = datetime.fromisoformat(ISO_FREEZE_TIME)

    def fake_get_job(farmId, queueId, jobId):
        # Only the deleted job is gone; every other GetJob (retried job, plus any
        # categorize-time lookups) resolves normally.
        if jobId == deleted_job_id:
            raise ClientError(
                {"Error": {"Code": "ResourceNotFoundException", "Message": "gone"}}, "GetJob"
            )
        if jobId == retried_job_id:
            return retried_job
        return abandoned_job

    deadline_mock.get_job.side_effect = fake_get_job

    runner = CliRunner()
    with freeze_time(ISO_FREEZE_TIME):
        result = runner.invoke(
            main,
            [
                "queue",
                "sync-output",
                "--ignore-storage-profiles",
                "--bootstrap-lookback-minutes",
                "120",
                "--farm-id",
                MOCK_FARM_ID,
                "--queue-id",
                MOCK_QUEUE_ID,
                "--checkpoint-dir",
                checkpoint_dir,
            ],
        )

    assert result.exit_code == 0, result.output
    # --force-bootstrap is intentionally omitted: it clears the failed-jobs tracker for a
    # fresh start. Without it (and with no checkpoint file) the run still bootstraps but
    # preserves the tracker, so the previously-failed jobs are retried.
    assert "Retrying 2 previously failed job(s)" in result.output, result.output
    # The re-fetched job was pulled in via GetJob.
    assert retried_job_id in [c.kwargs.get("jobId") for c in deadline_mock.get_job.call_args_list]

    # The abandoned job is suppressed even though search_jobs returned it in-window, so it
    # never reaches the status file as a download candidate.
    status_file_path = _ignore_profiles_status_file(checkpoint_dir)
    if os.path.exists(status_file_path):
        with open(status_file_path) as f:
            status = json.load(f)
        assert abandoned_job_id not in status["jobs"], status["jobs"]

    # The deleted job was removed from the tracker (GetJob → ResourceNotFoundException), so
    # it stops being retried forever. The abandoned job's count is retained at the cap
    # rather than dropped, which is what keeps a timestamp-window rediscovery suppressed.
    with open(failed_jobs_file) as f:
        tracker_after = json.load(f)
    assert deleted_job_id not in tracker_after, tracker_after
    assert tracker_after.get(abandoned_job_id) == _MAX_FAILED_JOB_RETRIES, tracker_after


@pytest.mark.skipif(
    sys.version_info < (3, 9), reason="Incremental output download requires Python >= 3.9"
)
def test_incremental_output_download_force_bootstrap_clears_failed_jobs_tracker(
    fresh_deadline_config, deadline_mock, checkpoint_dir
):
    """--force-bootstrap deletes the failed-jobs tracker, giving abandoned jobs a fresh start.

    This is the escape hatch for a job stuck at the retry cap: without it, an abandoned job
    stays suppressed forever. The consequence is that a forced bootstrap does NOT retry
    previously-failed jobs individually — the tracker is gone before that step runs.
    """
    from deadline.client.cli._incremental_download import _MAX_FAILED_JOB_RETRIES

    abandoned_job_id = "job-0123456789abcdefabcdefabcdefab03"
    failed_jobs_file = os.path.join(
        checkpoint_dir, f"{MOCK_QUEUE_ID}_ignore-storage-profiles_failed_jobs.json"
    )
    with open(failed_jobs_file, "w") as f:
        json.dump({abandoned_job_id: _MAX_FAILED_JOB_RETRIES}, f)

    deadline_mock.search_jobs = mock_search_jobs_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, [])

    runner = CliRunner()
    with freeze_time(ISO_FREEZE_TIME):
        result = runner.invoke(
            main,
            [
                "queue",
                "sync-output",
                "--ignore-storage-profiles",
                "--force-bootstrap",
                "--bootstrap-lookback-minutes",
                "120",
                "--farm-id",
                MOCK_FARM_ID,
                "--queue-id",
                MOCK_QUEUE_ID,
                "--checkpoint-dir",
                checkpoint_dir,
            ],
        )

    assert result.exit_code == 0, result.output
    # The seeded tracker was cleared, so no retry pass ran for the previously-failed job.
    assert "previously failed job(s)" not in result.output, result.output
    # The seeded counts are gone. The end-of-run save rewrites the file unconditionally, so it
    # may exist again — but empty, with the abandoned job no longer suppressed.
    if os.path.exists(failed_jobs_file):
        with open(failed_jobs_file) as f:
            assert json.load(f) == {}, "forced bootstrap must clear the tracked counts"


class _ManifestPlan(NamedTuple):
    """One output manifest to feed the attribution loop.

    ``order`` is the manifest's S3 LastModified rank — attribution sorts by it, so it is what
    decides which job/task owns a path emitted by more than one manifest. It is deliberately
    independent of list position so tests can shuffle the download order.
    """

    order: int
    job_id: str
    task_id: str
    paths: list[str]


def _run_manifest_plan(deadline_mock, checkpoint_dir, plans, downloader=None):
    """Runs `queue sync-output` over an arbitrary set of per-task output manifests.

    Generalizes the two-job harness: any number of jobs, each with any number of tasks and
    paths, with explicit manifest timestamps. Returns (result, downloaded_paths).
    """
    from deadline.job_attachments.asset_manifests.v2023_03_03.asset_manifest import (
        AssetManifest,
        ManifestPath,
    )
    from deadline.job_attachments.asset_manifests import HashAlgorithm

    job_ids = list(dict.fromkeys(plan.job_id for plan in plans))
    tasks_by_job: dict[str, list[str]] = {}
    for plan in plans:
        tasks_by_job.setdefault(plan.job_id, [])
        if plan.task_id not in tasks_by_job[plan.job_id]:
            tasks_by_job[plan.job_id].append(plan.task_id)

    mock_jobs = create_fake_job_list(len(job_ids))
    for i, (job, job_id) in enumerate(zip(mock_jobs, job_ids)):
        job["name"] = f"Job {i}"
        job["jobId"] = job_id
        job["taskRunStatus"] = "SUCCEEDED"
        job["taskRunStatusCounts"] = {"SUCCEEDED": len(tasks_by_job[job_id]), "READY": 0}
        job["attachments"] = {
            "manifests": [
                {"rootPath": "/", "rootPathFormat": "posix", "outputRelativeDirectories": ["."]}
            ],
            "fileSystem": "COPIED",
        }
        job["endedAt"] = datetime.fromisoformat(ISO_FREEZE_TIME)

    deadline_mock.search_jobs = mock_search_jobs_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, mock_jobs)
    deadline_mock.get_job = mock_get_job_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, mock_jobs)
    deadline_mock.list_sessions.return_value = {
        "sessions": [
            {
                "sessionId": MOCK_SESSION_ID,
                "fleetId": MOCK_FLEET_ID,
                "workerId": MOCK_WORKER_ID,
                "startedAt": datetime.fromisoformat("2025-08-06T00:15:45.712000+00:00"),
                "endedAt": datetime.fromisoformat("2025-08-06T00:20:59.992000+00:00"),
                "lifecycleStatus": "ENDED",
            }
        ]
    }
    deadline_mock.list_session_actions.side_effect = lambda **kwargs: {
        "sessionActions": [
            {
                "sessionActionId": f"sessionaction-0123456789abcdefabcdefabcdefabcd-{i}",
                "status": "SUCCEEDED",
                "startedAt": "2025-08-06T00:20:58.454000+00:00",
                "endedAt": "2025-08-06T00:20:59.992000+00:00",
                "progressPercent": 100.0,
                "definition": {"taskRun": {"taskId": task_id, "stepId": _STEP_ID}},
                "manifests": [{"outputManifestPath": f"{task_id}/manifest"}],
            }
            for i, task_id in enumerate(tasks_by_job[kwargs.get("jobId")])
        ]
    }

    # The manifest list is passed in the caller's (possibly shuffled) order — attribution must
    # derive ordering from the timestamps, not from list position.
    # Anchored before the frozen "now" so no manifest looks like it was written in the future,
    # which the checkpoint advance would reject.
    base = datetime.fromisoformat(ISO_FREEZE_TIME_MINUS_5MIN)
    downloaded_manifests = [
        (
            base + timedelta(minutes=plan.order),
            AssetManifest(
                hash_alg=HashAlgorithm.XXH128,
                total_size=len(plan.paths),
                paths=[ManifestPath(path=p, hash="h", size=1, mtime=1) for p in plan.paths],
            ),
        )
        for plan in plans
    ]
    manifests_to_download = [
        (None, plan.job_id, "/", f"prefix/{_STEP_ID}/{plan.task_id}/manifest") for plan in plans
    ]

    downloaded_paths: list[str] = []

    def default_downloader(
        files,
        hash_algorithm,
        queue,
        session,
        conflict,
        on_downloading_files,
        print_function_callback,
    ):
        for f in files:
            downloaded_paths.append(f.path)
            os.makedirs(os.path.dirname(f.path), exist_ok=True)
            with open(f.path, "w") as fh:
                fh.write("output")

    runner = CliRunner()
    with (
        patch(
            "deadline.client.cli._incremental_download._download_all_manifests_with_absolute_paths",
            side_effect=lambda *a, **k: downloaded_manifests,
        ),
        patch(
            "deadline.client.cli._incremental_download._get_manifests_to_download",
            side_effect=lambda *a, **k: manifests_to_download,
        ),
        patch(
            "deadline.client.cli._incremental_download._download_manifest_paths",
            side_effect=downloader(downloaded_paths) if downloader else default_downloader,
        ),
        freeze_time(ISO_FREEZE_TIME),
    ):
        result = runner.invoke(
            main,
            [
                "queue",
                "sync-output",
                "--ignore-storage-profiles",
                "--force-bootstrap",
                "--bootstrap-lookback-minutes",
                "120",
                "--farm-id",
                MOCK_FARM_ID,
                "--queue-id",
                MOCK_QUEUE_ID,
                "--checkpoint-dir",
                checkpoint_dir,
            ],
        )
    return result, downloaded_paths


@pytest.mark.skipif(
    sys.version_info < (3, 9), reason="Incremental output download requires Python >= 3.9"
)
def test_incremental_output_download_shared_path_across_three_jobs_newest_wins(
    fresh_deadline_config, deadline_mock, checkpoint_dir, tmp_path
):
    """One path in three jobs' manifests, list order shuffled relative to timestamps: the
    newest manifest's job/task owns it and it is downloaded exactly once.

    Two jobs was enough to catch a plain last-in-list bug; three with a shuffled order catches
    a sort that is only accidentally right, and proves the winner is the newest rather than
    just "not the first".
    """
    shared_path = str(tmp_path / "shared" / "beauty.exr")
    job_ids = [
        MOCK_JOB_ID,
        "job-0123456789abcdefabcdefabcdefab98",
        "job-0123456789abcdefabcdefabcdefab99",
    ]
    task_ids = [f"task-b1764261dff54214aace3932bde8ae7e-{i}" for i in range(3)]
    # Newest (order=3) is deliberately in the middle of the list.
    plans = [
        _ManifestPlan(order=1, job_id=job_ids[0], task_id=task_ids[0], paths=[shared_path]),
        _ManifestPlan(order=3, job_id=job_ids[2], task_id=task_ids[2], paths=[shared_path]),
        _ManifestPlan(order=2, job_id=job_ids[1], task_id=task_ids[1], paths=[shared_path]),
    ]

    result, downloaded_paths = _run_manifest_plan(deadline_mock, checkpoint_dir, plans)

    assert result.exit_code == 0, result.output
    assert downloaded_paths.count(shared_path) == 1, downloaded_paths
    assert "Downloaded files: 1" in result.output, result.output

    with open(_ignore_profiles_status_file(checkpoint_dir)) as f:
        status = json.load(f)

    # The newest manifest's job owns the download; it is credited exactly once.
    winner_tasks = status["jobs"][job_ids[2]]["tasks"]
    assert winner_tasks[task_ids[2]]["downloaded_files"] == 1, winner_tasks
    # The two losers still report their own status from the filesystem — the file IS on disk,
    # so they are downloaded, not failed and not silently missing from the file.
    for loser_index in (0, 1):
        loser = status["jobs"][job_ids[loser_index]]
        assert loser["download_status"] == "downloaded", loser
        loser_task = loser["tasks"][task_ids[loser_index]]
        assert loser_task["download_status"] == "downloaded", loser_task
        assert loser_task["error_code"] is None, loser_task


@pytest.mark.skipif(
    sys.version_info < (3, 9), reason="Incremental output download requires Python >= 3.9"
)
def test_incremental_output_download_cross_job_then_within_job_overwrite(
    fresh_deadline_config, deadline_mock, checkpoint_dir, tmp_path
):
    """Interleaved ownership transfer: job A → job B → back within job B under a newer task.

    Two dedup rules apply to the same path in one run (cross-job transfer, then a within-job
    retry). The final owner must be the newest task of the newest job, with no double download
    and no stale credit left behind on either earlier owner.
    """
    shared_path = str(tmp_path / "shared" / "beauty.exr")
    job_a, job_b = MOCK_JOB_ID, "job-0123456789abcdefabcdefabcdefab99"
    task_a = "task-b1764261dff54214aace3932bde8ae7e-0"
    task_b_old = "task-b1764261dff54214aace3932bde8ae7e-1"
    task_b_new = "task-b1764261dff54214aace3932bde8ae7e-2"
    plans = [
        _ManifestPlan(order=1, job_id=job_a, task_id=task_a, paths=[shared_path]),
        _ManifestPlan(order=2, job_id=job_b, task_id=task_b_old, paths=[shared_path]),
        _ManifestPlan(order=3, job_id=job_b, task_id=task_b_new, paths=[shared_path]),
    ]

    result, downloaded_paths = _run_manifest_plan(deadline_mock, checkpoint_dir, plans)

    assert result.exit_code == 0, result.output
    assert downloaded_paths.count(shared_path) == 1, downloaded_paths
    assert "Downloaded files: 1" in result.output, result.output

    with open(_ignore_profiles_status_file(checkpoint_dir)) as f:
        status = json.load(f)

    b_tasks = status["jobs"][job_b]["tasks"]
    assert b_tasks[task_b_new]["downloaded_files"] == 1, b_tasks
    # The superseded task of the same job keeps no credit for the path it lost.
    assert b_tasks.get(task_b_old, {}).get("downloaded_files", 0) == 0, b_tasks
    # Job B downloaded the file once in total, not once per task that ever claimed it.
    assert status["jobs"][job_b]["downloaded_files"] == 1, status["jobs"][job_b]
    # Job A lost the path entirely but is still reported, from the filesystem.
    assert status["jobs"][job_a]["download_status"] == "downloaded", status["jobs"][job_a]
    assert status["jobs"][job_a]["tasks"][task_a]["download_status"] == "downloaded"


@pytest.mark.skipif(
    sys.version_info < (3, 9), reason="Incremental output download requires Python >= 3.9"
)
def test_incremental_output_download_case_differing_paths_treated_as_one(
    fresh_deadline_config, deadline_mock, checkpoint_dir, tmp_path, monkeypatch
):
    """On a case-insensitive filesystem, two manifests whose paths differ only in case name the
    same file and must be downloaded once.

    A Windows submitter and a macOS submitter can spell the same output path differently. Two
    threads writing that one file concurrently under OVERWRITE would interleave and corrupt it,
    so attribution keys on os.path.normcase. This test forces case-insensitive normcase so the
    behavior is exercised on POSIX CI too, not just on Windows and macOS.
    """
    monkeypatch.setattr(os.path, "normcase", lambda p: p.lower())

    upper_path = str(tmp_path / "Shared" / "Beauty.EXR")
    lower_path = str(tmp_path / "shared" / "beauty.exr")
    job_ids = [MOCK_JOB_ID, "job-0123456789abcdefabcdefabcdefab99"]
    task_ids = [f"task-b1764261dff54214aace3932bde8ae7e-{i}" for i in range(2)]
    plans = [
        _ManifestPlan(order=1, job_id=job_ids[0], task_id=task_ids[0], paths=[upper_path]),
        _ManifestPlan(order=2, job_id=job_ids[1], task_id=task_ids[1], paths=[lower_path]),
    ]

    result, downloaded_paths = _run_manifest_plan(deadline_mock, checkpoint_dir, plans)

    assert result.exit_code == 0, result.output
    # One download total — the two spellings are one file, so only the newest owner fetches it.
    assert len(downloaded_paths) == 1, downloaded_paths
    assert downloaded_paths[0] == lower_path, downloaded_paths
    assert "Downloaded files: 1" in result.output, result.output

    with open(_ignore_profiles_status_file(checkpoint_dir)) as f:
        status = json.load(f)
    # The newest spelling's job owns the fetch and is credited for it.
    winner = status["jobs"][job_ids[1]]
    assert winner["downloaded_files"] == 1, winner
    assert winner["tasks"][task_ids[1]]["downloaded_files"] == 1, winner
    # The loser reports its own spelling from the filesystem, so its count is whatever the real
    # filesystem says about "Beauty.EXR". On a case-insensitive volume (macOS) the file exists
    # and the loser shows "downloaded". On a case-sensitive volume (Linux CI) "Beauty.EXR" is
    # absent (the winner wrote "beauty.exr") so the loser shows "failed". Both are correct for
    # their filesystem — assert only what holds on both.
    loser = status["jobs"][job_ids[0]]
    assert loser["download_status"] in ("downloaded", "failed"), loser
    assert loser["downloaded_files"] <= 1, loser


@pytest.mark.skipif(
    sys.version_info < (3, 9), reason="Incremental output download requires Python >= 3.9"
)
def test_incremental_output_download_every_path_downloaded_exactly_once(
    fresh_deadline_config, deadline_mock, checkpoint_dir, tmp_path
):
    """Over a tangle of overlapping manifests, every distinct path is downloaded exactly once
    and the per-task counts sum to the job total.

    Covers the whole attribution loop at once: shared paths across jobs, a within-job task
    retry, partially-overlapping path sets, and a manifest whose paths are entirely claimed by
    others. The properties asserted — no path fetched twice, none dropped, counts that add up —
    are the ones the per-task progress bar depends on.
    """

    def p(name):
        return str(tmp_path / "out" / name)

    job_a, job_b = MOCK_JOB_ID, "job-0123456789abcdefabcdefabcdefab99"
    tasks = [f"task-b1764261dff54214aace3932bde8ae7e-{i}" for i in range(4)]
    plans = [
        # Wholly superseded later by job B's task-2.
        _ManifestPlan(order=1, job_id=job_a, task_id=tasks[0], paths=[p("a.exr"), p("b.exr")]),
        # Retried under a new task of the same job; keeps c.exr, loses b.exr to task-2.
        _ManifestPlan(order=2, job_id=job_a, task_id=tasks[1], paths=[p("b.exr"), p("c.exr")]),
        # Cross-job: claims a.exr and b.exr from job A.
        _ManifestPlan(order=4, job_id=job_b, task_id=tasks[2], paths=[p("a.exr"), p("b.exr")]),
        # Unique paths, untouched by anyone else. Out of timestamp order in the list.
        _ManifestPlan(order=3, job_id=job_b, task_id=tasks[3], paths=[p("d.exr"), p("e.exr")]),
    ]
    all_paths = {p(n) for n in ("a.exr", "b.exr", "c.exr", "d.exr", "e.exr")}

    result, downloaded_paths = _run_manifest_plan(deadline_mock, checkpoint_dir, plans)

    assert result.exit_code == 0, result.output
    # Every distinct path fetched exactly once — none skipped, none fetched twice.
    assert sorted(downloaded_paths) == sorted(all_paths), downloaded_paths
    assert len(downloaded_paths) == len(set(downloaded_paths)), downloaded_paths
    assert "Downloaded files: 5" in result.output, result.output
    for path in all_paths:
        assert os.path.exists(path), path

    with open(_ignore_profiles_status_file(checkpoint_dir)) as f:
        status = json.load(f)

    # Per-job counts sum to the run total, with each path credited to exactly one job.
    per_job_downloaded = {
        job_id: status["jobs"][job_id]["downloaded_files"] for job_id in (job_a, job_b)
    }
    assert sum(per_job_downloaded.values()) == len(all_paths), per_job_downloaded
    # And within each job, the per-task counts sum to that job's total.
    for job_id in (job_a, job_b):
        entry = status["jobs"][job_id]
        assert (
            sum(t["downloaded_files"] for t in entry["tasks"].values())
            == (entry["downloaded_files"])
        ), entry
        assert entry["download_status"] == "downloaded", entry
    # Job A kept only c.exr (task-1); its task-0 lost both paths to later manifests.
    assert per_job_downloaded[job_a] == 1, per_job_downloaded
    assert status["jobs"][job_a]["tasks"][tasks[1]]["downloaded_files"] == 1
    assert tasks[0] not in status["jobs"][job_a]["tasks"], status["jobs"][job_a]["tasks"]


@pytest.mark.skipif(
    sys.version_info < (3, 9), reason="Incremental output download requires Python >= 3.9"
)
def test_incremental_output_download_status_file_shape_is_stable(
    fresh_deadline_config, deadline_mock, checkpoint_dir, tmp_path
):
    """Contract test on the JSON the producer actually writes.

    The Deadline Cloud monitor reads this file; the keys and value domains below are the
    interface. Asserting them on a real end-to-end run (rather than on a hand-built dict passed
    to the builder) is what catches a producer that stops populating a field the consumer
    depends on, or a status string that drifts out of the agreed set.
    """
    job_ok, job_bad = MOCK_JOB_ID, "job-0123456789abcdefabcdefabcdefab99"
    tasks = [f"task-b1764261dff54214aace3932bde8ae7e-{i}" for i in range(2)]
    good_path = str(tmp_path / "out" / "good.exr")
    bad_path = str(tmp_path / "out" / "bad.exr")
    plans = [
        _ManifestPlan(order=1, job_id=job_ok, task_id=tasks[0], paths=[good_path]),
        _ManifestPlan(order=2, job_id=job_bad, task_id=tasks[1], paths=[bad_path]),
    ]

    def failing_downloader(downloaded_paths):
        def download(
            files,
            hash_algorithm,
            queue,
            session,
            conflict,
            on_downloading_files,
            print_function_callback,
        ):
            for f in files:
                if f.path == bad_path:
                    raise PermissionError(f"[Errno 13] Permission denied: '{f.path}'")
                downloaded_paths.append(f.path)
                os.makedirs(os.path.dirname(f.path), exist_ok=True)
                with open(f.path, "w") as fh:
                    fh.write("output")

        return download

    # One succeeding and one failing job, so both the success and failure shapes are covered.
    result, _ = _run_manifest_plan(
        deadline_mock, checkpoint_dir, plans, downloader=failing_downloader
    )
    assert result.exit_code == 0, result.output

    with open(_ignore_profiles_status_file(checkpoint_dir)) as f:
        status = json.load(f)

    assert set(status) == {"schema_version", "sync_metadata", "jobs"}, status.keys()
    assert status["schema_version"] == 1, status["schema_version"]

    metadata = status["sync_metadata"]
    assert set(metadata) == {
        "queue_id",
        "storage_profile_id",
        "last_sync_completed_at",
        "last_run_status",
        "hostname",
    }, metadata.keys()
    assert metadata["queue_id"] == MOCK_QUEUE_ID
    assert metadata["storage_profile_id"] is None  # --ignore-storage-profiles
    assert metadata["last_run_status"] == "failed"  # one job failed
    datetime.fromisoformat(metadata["last_sync_completed_at"])  # parseable ISO-8601
    assert isinstance(metadata["hostname"], str) and metadata["hostname"]

    job_keys = {
        "download_status",
        "total_files",
        "downloaded_files",
        "failed_files",
        "last_updated",
        "error_code",
        "error_message",
        "skip_reason",
        "tasks",
    }
    task_keys = {
        "download_status",
        "total_files",
        "downloaded_files",
        "error_code",
        "error_message",
    }
    assert set(status["jobs"]) == {job_ok, job_bad}, status["jobs"].keys()
    for job_id, entry in status["jobs"].items():
        assert set(entry) == job_keys, (job_id, entry.keys())
        assert entry["download_status"] in (
            "downloaded",
            "failed",
            "in_progress",
            "skipped",
        ), entry
        for field in ("total_files", "downloaded_files", "failed_files"):
            assert isinstance(entry[field], int) and entry[field] >= 0, (job_id, field, entry)
        datetime.fromisoformat(entry["last_updated"])
        for task_id, task in entry["tasks"].items():
            assert set(task) == task_keys, (task_id, task.keys())
            assert task["download_status"] in (
                "downloaded",
                "failed",
                "farm_failed",
            ), task

    ok_entry = status["jobs"][job_ok]
    assert ok_entry["download_status"] == "downloaded"
    assert (ok_entry["error_code"], ok_entry["error_message"]) == (None, None), ok_entry
    assert ok_entry["skip_reason"] is None, ok_entry

    bad_entry = status["jobs"][job_bad]
    assert bad_entry["download_status"] == "failed"
    assert bad_entry["error_code"] == "PERMISSION_DENIED", bad_entry
    assert isinstance(bad_entry["error_message"], str) and bad_entry["error_message"], bad_entry
    assert bad_entry["tasks"][tasks[1]]["error_code"] == "PERMISSION_DENIED", bad_entry


@pytest.mark.skipif(
    sys.version_info < (3, 9), reason="Incremental output download requires Python >= 3.9"
)
def test_incremental_output_download_transient_getjob_error_keeps_job_in_tracker(
    fresh_deadline_config, deadline_mock, checkpoint_dir
):
    """A throttling or auth error while re-fetching a tracked job leaves it in the tracker.

    Only ResourceNotFoundException means the job is really gone. Treating a
    ThrottlingException the same way would silently drop a job that still exists and still has
    undownloaded output — a throttled control plane would quietly abandon work. The count must
    also not advance on a failure to even fetch the job, or a few throttles would burn the
    whole retry budget without a single download attempt.
    """
    from botocore.exceptions import ClientError

    throttled_job_id = "job-0123456789abcdefabcdefabcdefab04"
    unauthorized_job_id = "job-0123456789abcdefabcdefabcdefab05"
    failed_jobs_file = os.path.join(
        checkpoint_dir, f"{MOCK_QUEUE_ID}_ignore-storage-profiles_failed_jobs.json"
    )
    with open(failed_jobs_file, "w") as f:
        json.dump({throttled_job_id: 1, unauthorized_job_id: 2}, f)

    deadline_mock.search_jobs = mock_search_jobs_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, [])

    errors = {
        throttled_job_id: ClientError(
            {"Error": {"Code": "ThrottlingException", "Message": "slow down"}}, "GetJob"
        ),
        unauthorized_job_id: ClientError(
            {"Error": {"Code": "AccessDeniedException", "Message": "denied"}}, "GetJob"
        ),
    }

    def fake_get_job(farmId, queueId, jobId):
        raise errors[jobId]

    deadline_mock.get_job.side_effect = fake_get_job

    runner = CliRunner()
    with freeze_time(ISO_FREEZE_TIME):
        result = runner.invoke(
            main,
            [
                "queue",
                "sync-output",
                "--ignore-storage-profiles",
                "--bootstrap-lookback-minutes",
                "120",
                "--farm-id",
                MOCK_FARM_ID,
                "--queue-id",
                MOCK_QUEUE_ID,
                "--checkpoint-dir",
                checkpoint_dir,
            ],
        )

    # A control-plane error on the retry path must not fail the whole sync — the other jobs in
    # the run still have output to fetch.
    assert result.exit_code == 0, result.output
    assert "Retrying 2 previously failed job(s)" in result.output, result.output

    with open(failed_jobs_file) as f:
        tracker_after = json.load(f)
    # Both jobs are still tracked, at their original counts: a failure to fetch is not a
    # download attempt, so it must neither drop the job nor consume a retry.
    assert tracker_after == {throttled_job_id: 1, unauthorized_job_id: 2}, tracker_after


class TestFailedJobsTrackerDurability:
    """The tracker is a retry optimization, so its I/O failures must degrade, not propagate.

    Everything downstream of it — the downloads, the checkpoint advance, the status file — has
    to keep working when the checkpoint volume is read-only or the file was left corrupt by a
    crash. These are the only two ways the tracker can take the whole sync down with it.
    """

    def test_save_to_unwritable_location_does_not_raise(self, tmp_path):
        from deadline.client.cli._incremental_download import _FailedJobsTracker

        # A regular file where the tracker expects a directory: os.makedirs raises
        # NotADirectoryError, standing in for a read-only or full volume.
        blocker = tmp_path / "not_a_dir"
        blocker.write_text("")
        tracker = _FailedJobsTracker(str(blocker / "failed_jobs.json"))
        tracker.record_failures({"job-0123456789abcdefabcdefabcdefab04"}, lambda msg: None)

        tracker.save()  # must not raise — a lost tracker only costs cross-run retry memory

        # The in-memory count survives for the rest of this run even though it can't persist.
        assert tracker.get_tracked_job_ids() == {"job-0123456789abcdefabcdefabcdefab04"}
        # No half-written temp file left in the destination directory.
        assert [p.name for p in tmp_path.iterdir()] == ["not_a_dir"], list(tmp_path.iterdir())

    def test_corrupt_tracker_file_is_ignored_not_fatal(self, tmp_path):
        from deadline.client.cli._incremental_download import _FailedJobsTracker

        # Truncated mid-write by a crash or a full disk.
        tracker_file = tmp_path / "failed_jobs.json"
        tracker_file.write_text('{"job-0123456789abcdefabcdefabcdefab04": 2')

        tracker = _FailedJobsTracker(str(tracker_file))

        # Unreadable counts are dropped rather than crashing the sync. The cost is one extra
        # retry pass for whatever was tracked, which is the safe direction to fail.
        assert tracker.get_tracked_job_ids() == set()
        tracker.record_failures({"job-0123456789abcdefabcdefabcdefab05"}, lambda msg: None)
        tracker.save()
        with open(tracker_file) as f:
            assert json.load(f) == {"job-0123456789abcdefabcdefabcdefab05": 1}


@pytest.mark.skipif(
    sys.version_info < (3, 9), reason="Incremental output download requires Python >= 3.9"
)
def test_incremental_output_download_succeeded_task_with_no_output_is_not_a_failure(
    fresh_deadline_config, deadline_mock, checkpoint_dir
):
    """A job whose only succeeded task produced no output manifest reports downloaded, 0 files.

    A validation-only or save-elsewhere step legitimately emits nothing. Its session action is
    filtered out before attribution, so the job reaches the status file with no task entries at
    all. That must read as "nothing to download" — not as a failure, and not as a task stuck
    in_progress that the monitor would show as a spinner forever.
    """
    mock_jobs = create_fake_job_list(1)
    mock_jobs[0]["name"] = "Validation Only"
    mock_jobs[0]["jobId"] = MOCK_JOB_ID
    mock_jobs[0]["taskRunStatus"] = "SUCCEEDED"
    mock_jobs[0]["taskRunStatusCounts"] = {"SUCCEEDED": 1, "READY": 0}
    mock_jobs[0]["attachments"] = {
        "manifests": [
            {"rootPath": "/", "rootPathFormat": "posix", "outputRelativeDirectories": ["."]}
        ],
        "fileSystem": "COPIED",
    }
    mock_jobs[0]["endedAt"] = datetime.fromisoformat(ISO_FREEZE_TIME)

    deadline_mock.search_jobs = mock_search_jobs_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, mock_jobs)
    deadline_mock.get_job = mock_get_job_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, mock_jobs)
    deadline_mock.list_sessions.return_value = {
        "sessions": [
            {
                "sessionId": MOCK_SESSION_ID,
                "fleetId": MOCK_FLEET_ID,
                "workerId": MOCK_WORKER_ID,
                "startedAt": datetime.fromisoformat("2025-08-06T00:15:45.712000+00:00"),
                "endedAt": datetime.fromisoformat("2025-08-06T00:20:59.992000+00:00"),
                "lifecycleStatus": "ENDED",
            }
        ]
    }
    # SUCCEEDED on the farm, but the session action carries no output manifest.
    deadline_mock.list_session_actions.return_value = {
        "sessionActions": [
            {
                "sessionActionId": MOCK_SESSION_ACTION_ID_1,
                "status": "SUCCEEDED",
                "startedAt": "2025-08-06T00:20:58.454000+00:00",
                "endedAt": "2025-08-06T00:20:59.992000+00:00",
                "progressPercent": 100.0,
                "definition": {
                    "taskRun": {
                        "taskId": "task-b1764261dff54214aace3932bde8ae7e-0",
                        "stepId": _STEP_ID,
                    }
                },
                "manifests": [{}],
            }
        ]
    }

    runner = CliRunner()
    with freeze_time(ISO_FREEZE_TIME):
        result = runner.invoke(
            main,
            [
                "queue",
                "sync-output",
                "--ignore-storage-profiles",
                "--force-bootstrap",
                "--bootstrap-lookback-minutes",
                "120",
                "--farm-id",
                MOCK_FARM_ID,
                "--queue-id",
                MOCK_QUEUE_ID,
                "--checkpoint-dir",
                checkpoint_dir,
            ],
        )

    assert result.exit_code == 0, result.output
    assert "ran 1 / 1 session actions with no output" in result.output, result.output
    assert "Downloaded files: 0" in result.output, result.output

    with open(_ignore_profiles_status_file(checkpoint_dir)) as f:
        status = json.load(f)

    entry = status["jobs"][MOCK_JOB_ID]
    assert entry["download_status"] == "downloaded", entry
    assert (entry["total_files"], entry["downloaded_files"], entry["failed_files"]) == (0, 0, 0), (
        entry
    )
    assert entry["error_code"] is None, entry
    # A succeeded-with-no-output task is deliberately absent rather than recorded as
    # farm_failed: nothing failed, so there is no per-task download row to show.
    assert entry["tasks"] == {}, entry
    # And the whole run is a success — an empty download is not an error.
    assert status["sync_metadata"]["last_run_status"] == "success", status["sync_metadata"]


@pytest.mark.skipif(
    sys.version_info < (3, 9), reason="Incremental output download requires Python >= 3.9"
)
def test_incremental_output_download_mixed_output_and_no_output_steps(
    fresh_deadline_config, deadline_mock, checkpoint_dir, tmp_path
):
    """A job with one output-producing task and one output-free task records only the former.

    The per-task numerator must count tasks that actually had something to download, not every
    SUCCEEDED task on the farm — otherwise a job with a validation step permanently reads as
    "1 of 2 tasks downloaded" and looks stuck to the artist.
    """
    from deadline.job_attachments.asset_manifests.v2023_03_03.asset_manifest import (
        AssetManifest,
        ManifestPath,
    )
    from deadline.job_attachments.asset_manifests import HashAlgorithm

    producing_task = "task-b1764261dff54214aace3932bde8ae7e-0"
    silent_task = "task-b1764261dff54214aace3932bde8ae7e-1"
    output_path = str(tmp_path / "out" / "beauty.exr")

    mock_jobs = create_fake_job_list(1)
    mock_jobs[0]["name"] = "Mixed Job"
    mock_jobs[0]["jobId"] = MOCK_JOB_ID
    mock_jobs[0]["taskRunStatus"] = "SUCCEEDED"
    mock_jobs[0]["taskRunStatusCounts"] = {"SUCCEEDED": 2, "READY": 0}
    mock_jobs[0]["attachments"] = {
        "manifests": [
            {"rootPath": "/", "rootPathFormat": "posix", "outputRelativeDirectories": ["."]}
        ],
        "fileSystem": "COPIED",
    }
    mock_jobs[0]["endedAt"] = datetime.fromisoformat(ISO_FREEZE_TIME)

    deadline_mock.search_jobs = mock_search_jobs_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, mock_jobs)
    deadline_mock.get_job = mock_get_job_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, mock_jobs)
    deadline_mock.list_sessions.return_value = {
        "sessions": [
            {
                "sessionId": MOCK_SESSION_ID,
                "fleetId": MOCK_FLEET_ID,
                "workerId": MOCK_WORKER_ID,
                "startedAt": datetime.fromisoformat("2025-08-06T00:15:45.712000+00:00"),
                "endedAt": datetime.fromisoformat("2025-08-06T00:20:59.992000+00:00"),
                "lifecycleStatus": "ENDED",
            }
        ]
    }
    deadline_mock.list_session_actions.return_value = {
        "sessionActions": [
            {
                "sessionActionId": MOCK_SESSION_ACTION_ID_1,
                "status": "SUCCEEDED",
                "startedAt": "2025-08-06T00:20:58.454000+00:00",
                "endedAt": "2025-08-06T00:20:59.992000+00:00",
                "progressPercent": 100.0,
                "definition": {"taskRun": {"taskId": producing_task, "stepId": _STEP_ID}},
                "manifests": [{"outputManifestPath": f"{producing_task}/manifest"}],
            },
            {
                "sessionActionId": MOCK_SESSION_ACTION_ID_2,
                "status": "SUCCEEDED",
                "startedAt": "2025-08-06T00:20:58.454000+00:00",
                "endedAt": "2025-08-06T00:20:59.992000+00:00",
                "progressPercent": 100.0,
                "definition": {"taskRun": {"taskId": silent_task, "stepId": _STEP_ID}},
                "manifests": [{}],
            },
        ]
    }

    downloaded_manifests = [
        (
            datetime.fromisoformat(ISO_FREEZE_TIME),
            AssetManifest(
                hash_alg=HashAlgorithm.XXH128,
                total_size=1,
                paths=[ManifestPath(path=output_path, hash="h", size=1, mtime=1)],
            ),
        )
    ]
    manifests_to_download = [
        (None, MOCK_JOB_ID, "/", f"prefix/{_STEP_ID}/{producing_task}/manifest")
    ]

    downloaded_paths: list[str] = []

    def fake_download_manifest_paths(
        files,
        hash_algorithm,
        queue,
        session,
        conflict,
        on_downloading_files,
        print_function_callback,
    ):
        for f in files:
            downloaded_paths.append(f.path)
            os.makedirs(os.path.dirname(f.path), exist_ok=True)
            with open(f.path, "w") as fh:
                fh.write("output")

    runner = CliRunner()
    with (
        patch(
            "deadline.client.cli._incremental_download._download_all_manifests_with_absolute_paths",
            side_effect=lambda *a, **k: downloaded_manifests,
        ),
        patch(
            "deadline.client.cli._incremental_download._get_manifests_to_download",
            side_effect=lambda *a, **k: manifests_to_download,
        ),
        patch(
            "deadline.client.cli._incremental_download._download_manifest_paths",
            side_effect=fake_download_manifest_paths,
        ),
        freeze_time(ISO_FREEZE_TIME),
    ):
        result = runner.invoke(
            main,
            [
                "queue",
                "sync-output",
                "--ignore-storage-profiles",
                "--force-bootstrap",
                "--bootstrap-lookback-minutes",
                "120",
                "--farm-id",
                MOCK_FARM_ID,
                "--queue-id",
                MOCK_QUEUE_ID,
                "--checkpoint-dir",
                checkpoint_dir,
            ],
        )

    assert result.exit_code == 0, result.output
    assert downloaded_paths == [output_path], downloaded_paths
    # The output-free session action is filtered, not downloaded, and the run says so.
    assert "ran 1 / 2 session actions with no output" in result.output, result.output

    with open(_ignore_profiles_status_file(checkpoint_dir)) as f:
        status = json.load(f)

    entry = status["jobs"][MOCK_JOB_ID]
    assert entry["download_status"] == "downloaded", entry
    assert (entry["total_files"], entry["downloaded_files"]) == (1, 1), entry
    # Only the producing task gets a row — the silent task is not a download failure and not a
    # phantom 0-of-0 entry, even though the farm counted 2 SUCCEEDED tasks.
    assert set(entry["tasks"]) == {producing_task}, entry["tasks"]
    assert entry["tasks"][producing_task]["downloaded_files"] == 1, entry["tasks"]
    assert entry["tasks"][producing_task]["error_code"] is None, entry["tasks"]
