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


# Fixtures for shared resources
@pytest.fixture
def checkpoint_dir(tmp_path_factory):
    """Create a checkpoint directory for all tests to use."""
    checkpoint_dir = tmp_path_factory.mktemp("checkpoint")
    yield str(checkpoint_dir)
    # No cleanup needed here as tmp_path_factory handles it automatically


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
