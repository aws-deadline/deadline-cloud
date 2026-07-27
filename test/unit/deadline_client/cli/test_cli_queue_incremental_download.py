# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for the CLI queue incremental output download command.
"""

import json
import os
import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta, timezone

import boto3
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
    MOCK_BUCKET_NAME,
)
from ..mock_deadline_job_apis import (
    mock_search_jobs_for_set,
    create_fake_job_list,
    mock_get_job_for_set,
)
from deadline.job_attachments._incremental_downloads.incremental_download_state import (
    EVENTUAL_CONSISTENCY_MAX_SECONDS,
    IncrementalDownloadState,
    IncrementalDownloadJob,
)
from deadline.job_attachments.asset_manifests.hash_algorithms import HashAlgorithm
from deadline.job_attachments.asset_manifests.v2023_03_03.asset_manifest import (
    AssetManifest,
    ManifestPath,
)
from deadline.job_attachments.models import StorageProfileOperatingSystemFamily
import deadline.client.api
import deadline.client.cli._incremental_download as mod
from deadline.client.cli._incremental_download import (
    CategorizedJobIds,
    _update_checkpoint_jobs_list,
    _filter_session_actions_without_manifests_from_job_sessions,
    _get_job_sessions,
)

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


# ---------------------------------------------------------------------------
# Unit tests for _update_checkpoint_jobs_list (checkpoint session-ended timestamps)
# ---------------------------------------------------------------------------


def _make_categorized_job_ids(**kwargs):
    """Build a CategorizedJobIds with all categories reset to fresh empty sets.

    CategorizedJobIds defines its sets as class attributes, so instances share
    them unless reassigned. Reset every category to avoid cross-test contamination.
    """
    cats = CategorizedJobIds()
    for name in (
        "added",
        "updated",
        "unchanged",
        "completed",
        "inactive",
        "missing_storage_profile",
        "attachments_free",
    ):
        setattr(cats, name, set(kwargs.get(name, set())))
    return cats


def test_update_checkpoint_jobs_list_does_not_corrupt_session_ended_timestamp():
    """Each job's session_ended_timestamp must reflect its own sessions.

    Regression for the stale-variable bug: a leftover max_session_ended_timestamp
    from the last job of the first loop was written to every job in a second loop,
    corrupting the checkpoint timestamps.
    """
    t0 = datetime(2025, 1, 1, tzinfo=timezone.utc)
    t_a = datetime(2025, 1, 2, tzinfo=timezone.utc)
    t_b = datetime(2025, 1, 3, tzinfo=timezone.utc)

    checkpoint = IncrementalDownloadState(
        local_storage_profile_id=None, downloads_started_timestamp=t0
    )
    download_candidate_jobs = {
        "job-a": {"jobId": "job-a", "name": "A"},
        "job-b": {"jobId": "job-b", "name": "B"},
    }
    job_sessions = {
        "job-a": [
            {"sessionId": "s-a", "endedAt": t_a, "sessionActions": [{"sessionActionIndex": 1}]}
        ],
        "job-b": [
            {"sessionId": "s-b", "endedAt": t_b, "sessionActions": [{"sessionActionIndex": 1}]}
        ],
    }
    cats = _make_categorized_job_ids(added={"job-a", "job-b"})

    _update_checkpoint_jobs_list(checkpoint, download_candidate_jobs, cats, job_sessions)

    result = {job.job_id: job.session_ended_timestamp for job in checkpoint.jobs}
    assert result["job-a"] == t_a, result
    assert result["job-b"] == t_b, result


def test_update_checkpoint_running_only_job_keeps_saved_timestamp():
    """A job whose current sessions are all still running (no endedAt) must keep
    the session_ended_timestamp saved in the previous checkpoint rather than have
    it overwritten with None."""
    t0 = datetime(2025, 1, 1, tzinfo=timezone.utc)
    t_saved = datetime(2025, 1, 2, tzinfo=timezone.utc)

    prior_job = IncrementalDownloadJob({"jobId": "job-a", "name": "A"}, t_saved, {})
    checkpoint = IncrementalDownloadState(
        local_storage_profile_id=None, downloads_started_timestamp=t0, jobs=[prior_job]
    )
    download_candidate_jobs = {"job-a": {"jobId": "job-a", "name": "A"}}
    # Session is still running: no "endedAt" field.
    job_sessions = {"job-a": [{"sessionId": "s-a", "sessionActions": [{"sessionActionIndex": 1}]}]}
    cats = _make_categorized_job_ids(updated={"job-a"})

    _update_checkpoint_jobs_list(checkpoint, download_candidate_jobs, cats, job_sessions)

    result = {job.job_id: job.session_ended_timestamp for job in checkpoint.jobs}
    assert result["job-a"] == t_saved, result


def test_filter_session_actions_tolerates_missing_manifests_key():
    """A session action without a 'manifests' key must not raise KeyError."""
    job_sessions = {
        "job-a": [
            {
                "sessionId": "s-a",
                "sessionActions": [{"sessionActionId": "sa-0"}],  # no "manifests" key
            }
        ]
    }
    download_candidate_jobs = {"job-a": {"jobId": "job-a", "name": "A"}}

    # Must not raise (previously raised KeyError on session_action["manifests"]).
    _filter_session_actions_without_manifests_from_job_sessions(
        job_sessions, download_candidate_jobs
    )
    # The action lacked any output manifests, so it is filtered out.
    assert job_sessions["job-a"][0].get("sessionActions", []) == []


def test_get_job_sessions_requeued_job_uses_eventual_consistency_window():
    """A requeued job reappears categorized as 'added' but carries a saved
    session_ended_timestamp. It must get the eventual-consistency window applied,
    like updated/completed jobs, so sessions near the requeue boundary aren't missed."""
    t0 = datetime(2025, 1, 1, tzinfo=timezone.utc)
    t_saved = datetime(2025, 1, 5, tzinfo=timezone.utc)

    prior_job = IncrementalDownloadJob({"jobId": "job-a"}, t_saved, {})
    checkpoint = IncrementalDownloadState(
        local_storage_profile_id=None, downloads_started_timestamp=t0, jobs=[prior_job]
    )
    download_candidate_jobs = {"job-a": {"jobId": "job-a", "name": "A"}}
    cats = _make_categorized_job_ids(added={"job-a"})

    captured_thresholds = {}

    def fake_retrieve_sessions_for_job(
        deadline_client, farm_id, queue_id, job_id, session_ended_threshold, output_job_sessions
    ):
        captured_thresholds[job_id] = session_ended_threshold

    with (
        patch.object(mod, "get_session_client", return_value=MagicMock()),
        patch.object(mod, "_retrieve_sessions_for_job", side_effect=fake_retrieve_sessions_for_job),
    ):
        _get_job_sessions(
            MagicMock(),  # boto3_session
            MagicMock(),  # boto3_session_for_s3
            MOCK_FARM_ID,
            {"queueId": MOCK_QUEUE_ID},
            {},  # checkpoint_job_session_completed_indexes
            cats,
            checkpoint,
            download_candidate_jobs,
        )

    expected = t_saved - timedelta(seconds=checkpoint.eventual_consistency_max_seconds)
    assert captured_thresholds["job-a"] == expected, captured_thresholds


def test_incremental_output_download_json_mode_emits_no_debug_lines(
    fresh_deadline_config, deadline_mock, checkpoint_dir
):
    """In --json mode the command must not emit raw DEBUG print() lines that would
    corrupt the JSON output stream."""
    mock_jobs = create_fake_job_list(1)
    mock_jobs[0]["name"] = "Mock Job"
    mock_jobs[0]["jobId"] = MOCK_JOB_ID
    mock_jobs[0]["taskRunStatus"] = "READY"
    mock_jobs[0]["taskRunStatusCounts"] = {"SUCCEEDED": 1, "READY": 1}
    mock_jobs[0]["attachments"] = {
        "manifests": [
            {"rootPath": "/", "rootPathFormat": "posix", "outputRelativeDirectories": ["."]}
        ],
        "fileSystem": "VIRTUAL",
    }
    del mock_jobs[0]["endedAt"]
    deadline_mock.search_jobs = mock_search_jobs_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, mock_jobs)
    deadline_mock.get_job = mock_get_job_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, mock_jobs)

    runner = CliRunner()
    with freeze_time(ISO_FREEZE_TIME):
        result = runner.invoke(
            main,
            [
                "queue",
                "sync-output",
                "--ignore-storage-profiles",
                "--json",
                "--farm-id",
                MOCK_FARM_ID,
                "--queue-id",
                MOCK_QUEUE_ID,
                "--checkpoint-dir",
                checkpoint_dir,
            ],
        )

    assert result.exit_code == 0, result.output
    assert "DEBUG" not in result.output, result.output


def _put_manifest_and_data_objects_in_s3(root_path: str) -> dict[str, bytes]:
    """Upload asset manifests and CAS data objects to the moto S3 bucket so that
    sync-output performs real S3 downloads. Returns {relative_path: content}.

    The keys follow the job attachments layout under the queue's rootPrefix
    ("MockRootPrefix" from the deadline_mock fixture's get_queue response):
    manifests at MockRootPrefix/Manifests/<outputManifestPath> and CAS data at
    MockRootPrefix/Data/<hash>.xxh128.
    """
    files = {
        "output/file1.txt": b"content of file one",
        "output/file2.txt": b"content of file two, a bit longer",
        "output/deeper/file3.txt": b"third file content",
    }
    hashes = {path: f"fakehash{i}" for i, path in enumerate(files)}

    def make_manifest(paths: list[str]) -> str:
        return AssetManifest(
            hash_alg=HashAlgorithm.XXH128,
            paths=[
                ManifestPath(path=path, hash=hashes[path], size=len(files[path]), mtime=1)
                for path in paths
            ],
            total_size=sum(len(files[path]) for path in paths),
        ).encode()

    s3 = boto3.client("s3", region_name="us-west-2")
    s3.put_object(
        Bucket=MOCK_BUCKET_NAME,
        Key="MockRootPrefix/Manifests/manifest_action_0_output.xxh128",
        Body=make_manifest(["output/file1.txt", "output/file2.txt"]).encode("utf-8"),
    )
    s3.put_object(
        Bucket=MOCK_BUCKET_NAME,
        Key="MockRootPrefix/Manifests/manifest_action_1_output.xxh128",
        Body=make_manifest(["output/deeper/file3.txt"]).encode("utf-8"),
    )
    for path, content in files.items():
        s3.put_object(
            Bucket=MOCK_BUCKET_NAME,
            Key=f"MockRootPrefix/Data/{hashes[path]}.xxh128",
            Body=content,
        )
    return files


def test_incremental_output_download_json_mode_with_real_s3_download(
    fresh_deadline_config, deadline_mock, checkpoint_dir, tmp_path
):
    """In --json mode, stdout must stay byte-clean even while sync-output performs
    real S3 file downloads (via moto) with the real ProgressTracker firing progress
    callbacks. A contrast run without --json verifies that the same download emits
    progress output, proving the callbacks fired and were suppressed rather than
    the download being skipped."""
    root_path = str(tmp_path / "job_root")

    mock_jobs = create_fake_job_list(1)
    mock_jobs[0]["name"] = "Mock Job"
    mock_jobs[0]["jobId"] = MOCK_JOB_ID
    mock_jobs[0]["taskRunStatus"] = "READY"
    mock_jobs[0]["taskRunStatusCounts"] = {"SUCCEEDED": 2, "READY": 1}
    mock_jobs[0]["attachments"] = {
        "manifests": [
            {
                "rootPath": root_path,
                "rootPathFormat": "posix",
                "outputRelativeDirectories": ["output"],
            }
        ],
        "fileSystem": "VIRTUAL",
    }
    del mock_jobs[0]["endedAt"]
    deadline_mock.search_jobs = mock_search_jobs_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, mock_jobs)
    deadline_mock.get_job = mock_get_job_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, mock_jobs)

    # One running session (no endedAt, so it passes the session threshold filter)
    # with two SUCCEEDED task-run session actions carrying output manifests.
    deadline_mock.list_sessions.return_value = {
        "sessions": [
            {
                "sessionId": MOCK_SESSION_ID,
                "fleetId": MOCK_FLEET_ID,
                "workerId": MOCK_WORKER_ID,
                "startedAt": "2025-05-26T11:40:00+00:00",
                "lifecycleStatus": "STARTED",
            }
        ]
    }
    deadline_mock.list_session_actions.return_value = {
        "sessionActions": [
            {
                "sessionActionId": MOCK_SESSION_ACTION_ID_1,
                "status": "SUCCEEDED",
                "startedAt": "2025-05-26T11:41:00+00:00",
                "endedAt": "2025-05-26T11:42:00+00:00",
                "definition": {"taskRun": {"taskId": "task-abc-0", "stepId": "step-abc"}},
                "manifests": [{"outputManifestPath": "manifest_action_0_output.xxh128"}],
            },
            {
                "sessionActionId": MOCK_SESSION_ACTION_ID_2,
                "status": "SUCCEEDED",
                "startedAt": "2025-05-26T11:43:00+00:00",
                "endedAt": "2025-05-26T11:44:00+00:00",
                "definition": {"taskRun": {"taskId": "task-abc-1", "stepId": "step-abc"}},
                "manifests": [{"outputManifestPath": "manifest_action_1_output.xxh128"}],
            },
        ]
    }

    files = _put_manifest_and_data_objects_in_s3(root_path)

    # RUN 1: --json mode, real downloads through moto S3.
    runner = CliRunner()
    with freeze_time(ISO_FREEZE_TIME):
        result = runner.invoke(
            main,
            [
                "queue",
                "sync-output",
                "--ignore-storage-profiles",
                "--json",
                "--farm-id",
                MOCK_FARM_ID,
                "--queue-id",
                MOCK_QUEUE_ID,
                "--checkpoint-dir",
                checkpoint_dir,
            ],
        )
    assert result.exit_code == 0, result.output

    # The files must have actually been downloaded from S3, byte-for-byte.
    for rel_path, content in files.items():
        local_file = os.path.join(root_path, *rel_path.split("/"))
        assert os.path.isfile(local_file), f"missing downloaded file: {local_file}"
        with open(local_file, "rb") as f:
            assert f.read() == content, rel_path

    # stdout must be byte-clean for scripting: the command currently emits no JSON
    # payload of its own, so any output at all is leakage. If a legitimate JSON
    # payload is added later, every line must still parse as JSON.
    for line in result.output.splitlines():
        if line.strip():
            json.loads(line)
    assert result.output == "", repr(result.output)

    # RUN 2 (contrast): the same download without --json emits the DEBUG lines and
    # the ProgressTracker's 100% progress message, proving the progress callbacks
    # fired during RUN 1 and were suppressed rather than the download not happening.
    contrast_checkpoint_dir = str(tmp_path / "contrast_checkpoint")
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
                contrast_checkpoint_dir,
            ],
        )
    assert result.exit_code == 0, result.output
    assert "DEBUG: Got" in result.output, result.output
    assert "Downloading 3 files from S3..." in result.output, result.output
    # The 100% progress callback message, e.g. "Downloaded 70 B / 70 B of 3 files (...)"
    assert "of 3 files" in result.output, result.output
    assert "Downloaded files: 3" in result.output, result.output
