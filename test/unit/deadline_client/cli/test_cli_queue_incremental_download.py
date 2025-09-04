# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for the CLI queue incremental output download command.
"""

import os
import sys
import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta

import botocore
from moto import mock_aws
from freezegun import freeze_time
from click.testing import CliRunner
from deadline.client.cli import main
import deadline.client
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
)
from deadline.job_attachments.models import StorageProfileOperatingSystemFamily

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


@pytest.fixture
def deadline_mock(deadline_telemetry_client_mock):
    """Create a mock boto3 session for all tests to use."""
    os.environ["AWS_ACCESS_KEY_ID"] = "ACCESSKEY"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-west-2"

    with mock_aws():
        deadline_magicmock = MagicMock()

        # See https://docs.getmoto.org/en/latest/docs/services/patching_other_services.html
        original_make_api_call = botocore.client.BaseClient._make_api_call

        def mock_make_api_call(self, operation_name, kwarg):
            service_name = self._service_model.service_name

            if service_name == "deadline":
                # Send the "GetQueue" operation, i.e. the get_queue call, to deadline_magicmock.GetQueue()
                return getattr(deadline_magicmock, operation_name)(**kwarg)

            # If we don't want to patch the API call
            return original_make_api_call(self, operation_name, kwarg)

        deadline_magicmock.GetQueue.return_value = {
            "queueId": MOCK_QUEUE_ID,
            "displayName": "Mock Queue",
            "jobAttachmentSettings": {
                "rootPrefix": "MockRootPrefix",
                "s3BucketName": "mock-s3-bucket",
            },
        }
        deadline_magicmock.ListSessions.return_value = {"sessions": []}
        deadline_magicmock.ListSessionActions.return_value = {"sessionActions": []}
        deadline_magicmock.AssumeQueueRoleForUser.return_value = {
            "credentials": {
                "accessKeyId": "ACCESSKEY",
                "secretAccessKey": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
                "sessionToken": "testing",
                "expiration": datetime.fromisoformat("2025-08-07T01:01:44+00:00"),
            }
        }

        with patch("botocore.client.BaseClient._make_api_call", new=mock_make_api_call):
            yield deadline_magicmock


@pytest.mark.skipif(
    sys.version_info < (3, 9), reason="Incremental output download requires Python >= 3.9"
)
def test_incremental_output_download_requires_queue_with_job_attachments(
    fresh_deadline_config, deadline_mock, checkpoint_dir
):
    # The response does not include the "jobAttachmentSettings" field
    deadline_mock.GetQueue.return_value = {
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
    deadline_mock.SearchJobs = mock_search_jobs_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, mock_jobs)
    deadline_mock.GetJob = mock_get_job_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, mock_jobs)

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
    deadline_mock.SearchJobs = mock_search_jobs_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, mock_jobs)
    deadline_mock.GetJob = mock_get_job_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, mock_jobs)

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

    deadline_mock.GetStorageProfileForQueue = mock_get_storage_profile_for_queue
    # Mock list_sessions to return one session
    deadline_mock.ListSessions.return_value = {
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
    deadline_mock.ListSessionActions.return_value = {
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
    deadline_mock.SearchJobs = mock_search_jobs_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, mock_jobs)
    deadline_mock.GetJob = mock_get_job_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, mock_jobs)

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
    deadline_mock.SearchJobs = mock_search_jobs_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, mock_jobs)
    deadline_mock.GetJob = mock_get_job_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, mock_jobs)

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
    deadline_mock.SearchJobs = mock_search_jobs_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, mock_jobs)
    deadline_mock.GetJob = mock_get_job_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, mock_jobs)

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
    deadline_mock.SearchJobs = mock_search_jobs_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, mock_jobs)
    deadline_mock.GetJob = mock_get_job_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, mock_jobs)

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
    deadline_mock.SearchJobs = mock_search_jobs_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, mock_jobs)
    deadline_mock.GetJob = mock_get_job_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, mock_jobs)

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
    deadline_mock.SearchJobs = mock_search_jobs_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, [mock_job])
    deadline_mock.GetJob = mock_get_job_for_set(MOCK_FARM_ID, MOCK_QUEUE_ID, [mock_job])

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
