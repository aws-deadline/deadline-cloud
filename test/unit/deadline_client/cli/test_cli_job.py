# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for the CLI job commands.
"""

import datetime
import json
import os
from typing import Dict, List
import pytest
from pathlib import Path
import sys
from unittest.mock import ANY, MagicMock, patch

import boto3  # type: ignore[import]
from botocore.exceptions import ClientError  # type: ignore[import]
from click.testing import CliRunner
from dateutil.tz import tzutc  # type: ignore[import]

from deadline.client import api, config
from deadline.client.cli import main
from deadline.client.cli._groups import job_group
from deadline.client.cli._groups.job_group import (
    _get_summary_of_files_to_download_message,
    _get_json_line,
    _get_download_summary_message,
)
from deadline.client.exceptions import DeadlineOperationError, DeadlineOperationTimedOut
from deadline.job_attachments.models import (
    FileConflictResolution,
    JobAttachmentS3Settings,
    PathFormat,
)
from deadline.job_attachments.progress_tracker import (
    DownloadSummaryStatistics,
)
from ..api.test_job_bundle_submission import (
    MOCK_GET_QUEUE_RESPONSE,
)
from ..shared_constants import (
    MOCK_FARM_ID,
    MOCK_JOB_ID,
    MOCK_QUEUE_ID,
    MOCK_FLEET_ID,
    MOCK_WORKER_ID,
)

MOCK_JOBS_LIST = [
    {
        "jobId": "job-aaf4cdf8aae242f58fb84c5bb19f199b",
        "name": "CLI Job",
        "taskRunStatus": "RUNNING",
        "lifecycleStatus": "SUCCEEDED",
        "createdBy": "b801f3c0-c071-70bc-b869-6804bc732408",
        "createdAt": datetime.datetime(2023, 1, 27, 7, 34, 41, tzinfo=tzutc()),
        "startedAt": datetime.datetime(2023, 1, 27, 7, 37, 53, tzinfo=tzutc()),
        "endedAt": datetime.datetime(2023, 1, 27, 7, 39, 17, tzinfo=tzutc()),
        "priority": 50,
    },
    {
        "jobId": "job-0d239749fa05435f90263b3a8be54144",
        "name": "CLI Job",
        "taskRunStatus": "COMPLETED",
        "lifecycleStatus": "SUCCEEDED",
        "createdBy": "b801f3c0-c071-70bc-b869-6804bc732408",
        "createdAt": datetime.datetime(2023, 1, 27, 7, 24, 22, tzinfo=tzutc()),
        "startedAt": datetime.datetime(2023, 1, 27, 7, 27, 6, tzinfo=tzutc()),
        "endedAt": datetime.datetime(2023, 1, 27, 7, 29, 51, tzinfo=tzutc()),
        "priority": 50,
    },
]

MOCK_SESSIONS_LIST = [
    {
        "sessionId": "session-1",
        "fleetId": MOCK_FLEET_ID,
        "workerId": MOCK_WORKER_ID,
        "startedAt": datetime.datetime(2023, 1, 27, 7, 24, 22, tzinfo=tzutc()),
        "lifecycleStatus": "ENDED",
        "endedAt": datetime.datetime(2023, 1, 27, 7, 25, 22, tzinfo=tzutc()),
    },
]

MOCK_SESSION_ACTIONS_LIST = [
    {
        "sessionActionId": "sessionaction-1-0",
        "status": "SUCCEEDED",
        "startedAt": datetime.datetime(2023, 1, 27, 7, 24, 45, tzinfo=tzutc()),
        "endedAt": datetime.datetime(2023, 1, 27, 7, 25, 15, tzinfo=tzutc()),
        "progressPercent": 100.0,
        "definition": {
            "taskRun": {
                "taskId": "task-0a0ac395f3ed4d61bda7019874b1f384-0",
                "stepId": "step-0a0ac395f3ed4d61bda7019874b1f384",
            }
        },
    },
]

MOCK_STEP = {
    "stepId": "step-0a0ac395f3ed4d61bda7019874b1f384",
    "name": "Step Name",
    "lifecycleStatus": "CREATE_COMPLETE",
    "taskRunStatus": "SUCCEEDED",
    "taskRunStatusCounts": {
        "PENDING": 0,
        "READY": 0,
        "RUNNING": 0,
        "ASSIGNED": 0,
        "STARTING": 0,
        "SCHEDULED": 0,
        "INTERRUPTING": 0,
        "SUSPENDED": 0,
        "CANCELED": 0,
        "FAILED": 0,
        "SUCCEEDED": 1,
    },
    "createdAt": datetime.datetime(2023, 1, 27, 7, 14, 41, tzinfo=tzutc()),
    "createdBy": "a4a874f8-10b1-70d6-e763-a0e3822893b0",
    "startedAt": datetime.datetime(2023, 1, 27, 7, 24, 45, tzinfo=tzutc()),
    "endedAt": datetime.datetime(2023, 1, 27, 7, 25, 15, tzinfo=tzutc()),
}

MOCK_TASK = {
    "taskId": "task-0a0ac395f3ed4d61bda7019874b1f384-2",
    "createdAt": datetime.datetime(2023, 1, 27, 7, 14, 41, tzinfo=tzutc()),
    "createdBy": "a4a874f8-10b1-70d6-e763-a0e3822893b0",
    "runStatus": "SUCCEEDED",
    "failureRetryCount": 0,
    "parameters": {},
    "startedAt": datetime.datetime(2023, 1, 27, 7, 24, 45, tzinfo=tzutc()),
    "endedAt": datetime.datetime(2023, 1, 27, 7, 25, 15, tzinfo=tzutc()),
    "latestSessionActionId": "sessionaction-1-0",
}

os.environ["AWS_ENDPOINT_URL_DEADLINE"] = "https://fake-endpoint"


def test_cli_job_list(fresh_deadline_config):
    """
    Confirm that the CLI interface prints out the expected list of
    jobs, given mock data.
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)

    with patch.object(api._session, "get_boto3_session") as session_mock:
        session_mock().client("deadline").search_jobs.return_value = {
            "jobs": MOCK_JOBS_LIST,
            "totalResults": 12,
            "itemOffset": len(MOCK_JOBS_LIST),
        }

        runner = CliRunner()
        result = runner.invoke(main, ["job", "list"])

        assert (
            result.output
            == """Displaying 2 of 12 Jobs starting at 0

- name: CLI Job
  jobId: job-aaf4cdf8aae242f58fb84c5bb19f199b
  taskRunStatus: RUNNING
  startedAt: 2023-01-27 07:37:53+00:00
  endedAt: 2023-01-27 07:39:17+00:00
  createdBy: b801f3c0-c071-70bc-b869-6804bc732408
  createdAt: 2023-01-27 07:34:41+00:00
- name: CLI Job
  jobId: job-0d239749fa05435f90263b3a8be54144
  taskRunStatus: COMPLETED
  startedAt: 2023-01-27 07:27:06+00:00
  endedAt: 2023-01-27 07:29:51+00:00
  createdBy: b801f3c0-c071-70bc-b869-6804bc732408
  createdAt: 2023-01-27 07:24:22+00:00

"""
        )
        assert result.exit_code == 0


def test_cli_job_list_explicit_farm_and_queue_id(fresh_deadline_config):
    """
    Confirm that the CLI interface prints out the expected list of
    jobs, given mock data.
    """
    with patch.object(api._session, "get_boto3_session") as session_mock:
        session_mock().client("deadline").search_jobs.return_value = {
            "jobs": MOCK_JOBS_LIST,
            "totalResults": 12,
            "itemOffset": len(MOCK_JOBS_LIST),
        }

        runner = CliRunner()
        result = runner.invoke(
            main,
            ["job", "list", "--farm-id", MOCK_FARM_ID, "--queue-id", MOCK_QUEUE_ID],
        )

        assert (
            result.output
            == """Displaying 2 of 12 Jobs starting at 0

- name: CLI Job
  jobId: job-aaf4cdf8aae242f58fb84c5bb19f199b
  taskRunStatus: RUNNING
  startedAt: 2023-01-27 07:37:53+00:00
  endedAt: 2023-01-27 07:39:17+00:00
  createdBy: b801f3c0-c071-70bc-b869-6804bc732408
  createdAt: 2023-01-27 07:34:41+00:00
- name: CLI Job
  jobId: job-0d239749fa05435f90263b3a8be54144
  taskRunStatus: COMPLETED
  startedAt: 2023-01-27 07:27:06+00:00
  endedAt: 2023-01-27 07:29:51+00:00
  createdBy: b801f3c0-c071-70bc-b869-6804bc732408
  createdAt: 2023-01-27 07:24:22+00:00

"""
        )
        assert result.exit_code == 0


def test_cli_job_list_override_profile(fresh_deadline_config):
    """
    Confirms that the --profile option overrides the option to boto3.Session.
    """
    # set the farm id for the overridden profile
    config.set_setting("defaults.aws_profile_name", "NonDefaultProfileName")
    config.set_setting("defaults.farm_id", "farm-overriddenid")
    config.set_setting("defaults.queue_id", "queue-overriddenid")
    config.set_setting("defaults.aws_profile_name", "DifferentProfileName")

    with patch.object(boto3, "Session") as session_mock:
        session_mock().client("deadline").search_jobs.return_value = {
            "jobs": MOCK_JOBS_LIST,
            "totalResults": 12,
            "nextPage": len(MOCK_JOBS_LIST),
        }
        session_mock.reset_mock()

        runner = CliRunner()
        result = runner.invoke(main, ["job", "list", "--profile", "NonDefaultProfileName"])

        assert result.exit_code == 0
        session_mock.assert_called_once_with(profile_name="NonDefaultProfileName")
        session_mock().client().search_jobs.assert_called_once_with(
            farmId="farm-overriddenid",
            queueIds=["queue-overriddenid"],
            itemOffset=0,
            pageSize=5,
            sortExpressions=[{"fieldSort": {"name": "CREATED_AT", "sortOrder": "DESCENDING"}}],
        )


def test_cli_job_list_no_farm_id(fresh_deadline_config):
    with patch.object(api._session, "get_boto3_session") as session_mock:
        session_mock().client("deadline").search_jobs.return_value = {
            "jobs": MOCK_JOBS_LIST,
            "totalResults": 12,
            "nextPage": len(MOCK_JOBS_LIST),
        }

        runner = CliRunner()
        result = runner.invoke(main, ["job", "list"])

        assert "Missing '--farm-id' or default Farm ID configuration" in result.output
        assert result.exit_code != 0


def test_cli_job_list_no_queue_id(fresh_deadline_config):
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)

    with patch.object(api._session, "get_boto3_session") as session_mock:
        session_mock().client("deadline").search_jobs.return_value = {
            "jobs": MOCK_JOBS_LIST,
            "totalResults": 12,
            "nextPage": len(MOCK_JOBS_LIST),
        }

        runner = CliRunner()
        result = runner.invoke(main, ["job", "list"])

        assert "Missing '--queue-id' or default Queue ID configuration" in result.output
        assert result.exit_code != 0


def test_cli_job_list_client_error(fresh_deadline_config):
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)

    with patch.object(api._session, "get_boto3_session") as session_mock:
        session_mock().client("deadline").search_jobs.side_effect = ClientError(
            {"Error": {"Message": "A botocore client error"}}, "client error"
        )

        runner = CliRunner()
        result = runner.invoke(main, ["job", "list"])

        assert "Failed to get Jobs" in result.output
        assert "A botocore client error" in result.output
        assert result.exit_code != 0


def test_cli_job_get(fresh_deadline_config):
    """
    Confirm that the CLI interface prints out the expected job, given mock data.
    """

    with patch.object(api._session, "get_boto3_session") as session_mock:
        session_mock().client("deadline").get_job.return_value = MOCK_JOBS_LIST[0]

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "job",
                "get",
                "--farm-id",
                MOCK_FARM_ID,
                "--queue-id",
                MOCK_QUEUE_ID,
                "--job-id",
                str(MOCK_JOBS_LIST[0]["jobId"]),
            ],
        )

        assert (
            result.output
            == """jobId: job-aaf4cdf8aae242f58fb84c5bb19f199b
name: CLI Job
taskRunStatus: RUNNING
lifecycleStatus: SUCCEEDED
createdBy: b801f3c0-c071-70bc-b869-6804bc732408
createdAt: 2023-01-27 07:34:41+00:00
startedAt: 2023-01-27 07:37:53+00:00
endedAt: 2023-01-27 07:39:17+00:00
priority: 50

"""
        )
        session_mock().client("deadline").get_job.assert_called_once_with(
            farmId=MOCK_FARM_ID, queueId=MOCK_QUEUE_ID, jobId=MOCK_JOBS_LIST[0]["jobId"]
        )
        assert result.exit_code == 0


def test_cli_job_download_output_stdout_with_only_required_input(
    fresh_deadline_config, tmp_path: Path
):
    """
    Tests whether the output messages printed to stdout match expected messages
    when `download-output` command is executed.
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)

    with patch.object(api, "get_boto3_client") as boto3_client_mock, patch.object(
        job_group, "OutputDownloader"
    ) as MockOutputDownloader, patch.object(
        job_group, "_get_conflicting_filenames", return_value=[]
    ), patch.object(job_group, "round", return_value=0), patch.object(
        api, "get_queue_user_boto3_session"
    ):
        mock_download = MagicMock()
        mock_download.return_value = DownloadSummaryStatistics(
            total_time=12,
            processed_files=3,
            processed_bytes=1024,
        )
        MockOutputDownloader.return_value.download_job_output = mock_download
        mock_root_path = "/root/path" if sys.platform != "win32" else "C:\\Users\\username"
        mock_files_list = ["outputs/file1.txt", "outputs/file2.txt", "outputs/file3.txt"]
        MockOutputDownloader.return_value.get_output_paths_by_root.side_effect = [
            {
                f"{mock_root_path}": mock_files_list,
                f"{mock_root_path}2": mock_files_list,
            },
            {
                f"{mock_root_path}": mock_files_list,
                f"{mock_root_path}2": mock_files_list,
            },
            {
                f"{mock_root_path}": mock_files_list,
                str(tmp_path): mock_files_list,
            },
        ]

        mock_host_path_format = PathFormat.get_host_path_format()

        boto3_client_mock().get_queue.side_effect = [MOCK_GET_QUEUE_RESPONSE]
        boto3_client_mock().get_job.return_value = {
            "name": "Mock Job",
            "attachments": {
                "manifests": [
                    {
                        "rootPath": f"{mock_root_path}",
                        "rootPathFormat": mock_host_path_format,
                        "outputRelativeDirectories": ["."],
                    },
                    {
                        "rootPath": f"{mock_root_path}2",
                        "rootPathFormat": mock_host_path_format,
                        "outputRelativeDirectories": ["."],
                    },
                ],
            },
        }

        runner = CliRunner()
        result = runner.invoke(
            main,
            ["job", "download-output", "--job-id", MOCK_JOB_ID, "--output", "verbose"],
            input=f"1\n{str(tmp_path)}\ny\n",
        )

        MockOutputDownloader.assert_called_once_with(
            s3_settings=JobAttachmentS3Settings(**MOCK_GET_QUEUE_RESPONSE["jobAttachmentSettings"]),  # type: ignore
            farm_id=MOCK_FARM_ID,
            queue_id=MOCK_QUEUE_ID,
            job_id=MOCK_JOB_ID,
            step_id=None,
            task_id=None,
            session=ANY,
        )

        path_separator = "/" if sys.platform != "win32" else "\\"

        assert (
            f"""Downloading output from Job 'Mock Job'

Summary of files to download:
    {mock_root_path}{path_separator}outputs (3 files)
    {mock_root_path}2{path_separator}outputs (3 files)

You are about to download files which may come from multiple root directories. Here are a list of the current root directories:
[0] {mock_root_path}
[1] {mock_root_path}2
> Please enter the index of root directory to edit, y to proceed without changes, or n to cancel the download (0, 1, y, n) [y]: 1
> Please enter the new root directory path, or press Enter to keep it unchanged [{mock_root_path}2]: {str(tmp_path)}

Summary of files to download:
    {mock_root_path}{path_separator}outputs (3 files)
    {str(tmp_path)}{path_separator}outputs (3 files)

You are about to download files which may come from multiple root directories. Here are a list of the current root directories:
[0] {mock_root_path}
[1] {str(tmp_path)}
> Please enter the index of root directory to edit, y to proceed without changes, or n to cancel the download (0, 1, y, n) [y]: y
"""
            in result.output
        )
        assert "Download Summary:" in result.output
        assert result.exit_code == 0


def test_cli_job_download_output_stdout_with_mismatching_path_format(
    fresh_deadline_config, tmp_path: Path
):
    """
    Tests that the `download-output` command handles cross-platform situations,
    where the output files of the job submitted on Windows need to be downloaded
    on non-Windows and vice versa, by verifying that the output messages printed
    to stdout match expected messages.
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)

    with patch.object(api, "get_boto3_client") as boto3_client_mock, patch.object(
        job_group, "OutputDownloader"
    ) as MockOutputDownloader, patch.object(
        job_group, "_get_conflicting_filenames", return_value=[]
    ), patch.object(job_group, "round", return_value=0), patch.object(
        api, "get_queue_user_boto3_session"
    ):
        mock_download = MagicMock()
        mock_download.return_value = DownloadSummaryStatistics(
            total_time=12,
            processed_files=3,
            processed_bytes=1024,
        )
        MockOutputDownloader.return_value.download_job_output = mock_download

        mock_root_path = "C:\\Users\\username" if sys.platform != "win32" else "/root/path"
        mock_files_list = ["outputs/file1.txt", "outputs/file2.txt", "outputs/file3.txt"]
        MockOutputDownloader.return_value.get_output_paths_by_root.side_effect = [
            {
                f"{mock_root_path}": mock_files_list,
            },
            {
                str(tmp_path): mock_files_list,
            },
            {
                str(tmp_path): mock_files_list,
            },
        ]

        # Get the opposite path format of the current operating system
        current_format = PathFormat.get_host_path_format()
        other_format = (
            PathFormat.WINDOWS if current_format == PathFormat.POSIX else PathFormat.POSIX
        )

        boto3_client_mock().get_queue.side_effect = [MOCK_GET_QUEUE_RESPONSE]
        boto3_client_mock().get_job.return_value = {
            "name": "Mock Job",
            "attachments": {
                "manifests": [
                    {
                        "rootPath": f"{mock_root_path}",
                        "rootPathFormat": other_format,
                        "outputRelativeDirectories": ["."],
                    },
                ],
            },
        }

        runner = CliRunner()
        result = runner.invoke(
            main,
            ["job", "download-output", "--job-id", MOCK_JOB_ID, "--output", "verbose"],
            input=f"{str(tmp_path)}\ny\n",
        )

        MockOutputDownloader.assert_called_once_with(
            s3_settings=JobAttachmentS3Settings(**MOCK_GET_QUEUE_RESPONSE["jobAttachmentSettings"]),  # type: ignore
            farm_id=MOCK_FARM_ID,
            queue_id=MOCK_QUEUE_ID,
            job_id=MOCK_JOB_ID,
            step_id=None,
            task_id=None,
            session=ANY,
        )

        path_separator = "/" if sys.platform != "win32" else "\\"

        assert (
            f"""Downloading output from Job 'Mock Job'
This root path format does not match the operating system you're using. Where would you like to save the files?
The location was {mock_root_path}, on {other_format[0].upper() + other_format[1:]}.
> Please enter a new root path: {str(tmp_path)}

Summary of files to download:
    {str(tmp_path)}{path_separator}outputs (3 files)

You are about to download files which may come from multiple root directories. Here are a list of the current root directories:
[0] {str(tmp_path)}
> Please enter the index of root directory to edit, y to proceed without changes, or n to cancel the download (0, y, n) [y]: y
"""
            in result.output
        )
        assert "Download Summary:" in result.output
        assert result.exit_code == 0


@pytest.mark.skipif(
    sys.platform != "win32",
    reason="This test is for testing Windows downloading job outputs located at a UNC root path.",
)
def test_cli_job_download_output_handles_unc_path_on_windows(fresh_deadline_config, tmp_path: Path):
    """
    This tests only runs on Windows OS.
    Executes the `download-output` command on a job that has a root path of UNC format,
    and verifies that the output messages printed to stdout match expected messages.
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)

    with patch.object(api, "get_boto3_client") as boto3_client_mock, patch.object(
        job_group, "OutputDownloader"
    ) as MockOutputDownloader, patch.object(
        job_group, "_get_conflicting_filenames", return_value=[]
    ), patch.object(job_group, "round", return_value=0), patch.object(
        api, "get_queue_user_boto3_session"
    ):
        mock_download = MagicMock()
        mock_download.return_value = DownloadSummaryStatistics(
            total_time=12,
            processed_files=3,
            processed_bytes=1024,
        )
        MockOutputDownloader.return_value.download_job_output = mock_download

        # UNC format (which refers to the same location as 'C:\Users\username')
        mock_root_path = "\\\\127.0.0.1\\c$\\Users\\username"
        mock_files_list = ["outputs/file1.txt", "outputs/file2.txt", "outputs/file3.txt"]
        MockOutputDownloader.return_value.get_output_paths_by_root.side_effect = [
            {
                f"{mock_root_path}": mock_files_list,
            },
            {
                f"{mock_root_path}": mock_files_list,
            },
            {
                str(tmp_path): mock_files_list,
            },
        ]

        boto3_client_mock().get_queue.side_effect = [MOCK_GET_QUEUE_RESPONSE]
        boto3_client_mock().get_job.return_value = {
            "name": "Mock Job",
            "attachments": {
                "manifests": [
                    {
                        "rootPath": f"{mock_root_path}",
                        "rootPathFormat": PathFormat.WINDOWS,
                        "outputRelativeDirectories": ["."],
                    },
                ],
            },
        }

        runner = CliRunner()
        result = runner.invoke(
            main,
            ["job", "download-output", "--job-id", MOCK_JOB_ID, "--output", "verbose"],
            input=f"0\n{str(tmp_path)}\ny\n",
        )

        MockOutputDownloader.assert_called_once_with(
            s3_settings=JobAttachmentS3Settings(**MOCK_GET_QUEUE_RESPONSE["jobAttachmentSettings"]),  # type: ignore
            farm_id=MOCK_FARM_ID,
            queue_id=MOCK_QUEUE_ID,
            job_id=MOCK_JOB_ID,
            step_id=None,
            task_id=None,
            session=ANY,
        )

        path_separator = "/" if sys.platform != "win32" else "\\"

        assert (
            f"""Downloading output from Job 'Mock Job'

Summary of files to download:
    {mock_root_path}{path_separator}outputs (3 files)

You are about to download files which may come from multiple root directories. Here are a list of the current root directories:
[0] {mock_root_path}
> Please enter the index of root directory to edit, y to proceed without changes, or n to cancel the download (0, y, n) [y]: 0
> Please enter the new root directory path, or press Enter to keep it unchanged [{mock_root_path}]: {str(tmp_path)}

Summary of files to download:
    {str(tmp_path)}{path_separator}outputs (3 files)

You are about to download files which may come from multiple root directories. Here are a list of the current root directories:
[0] {str(tmp_path)}
> Please enter the index of root directory to edit, y to proceed without changes, or n to cancel the download (0, y, n) [y]: y
"""
            in result.output
        )
        assert "Download Summary:" in result.output
        assert result.exit_code == 0, result.output


def test_cli_job_download_no_output_stdout(fresh_deadline_config, tmp_path: Path):
    """
    Tests whether the output messages printed to stdout match expected messages
    when executing download-output command for a job that don't have any output yet.
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)

    with patch.object(api, "get_boto3_client") as boto3_client_mock, patch.object(
        job_group, "OutputDownloader"
    ) as MockOutputDownloader, patch.object(
        job_group, "_get_conflicting_filenames", return_value=[]
    ), patch.object(job_group, "round", return_value=0), patch.object(
        api, "get_queue_user_boto3_session"
    ):
        mock_download = MagicMock()
        mock_download.return_value = DownloadSummaryStatistics(
            total_time=12,
            processed_files=3,
            processed_bytes=1024,
        )
        MockOutputDownloader.return_value.download_job_output = mock_download
        MockOutputDownloader.return_value.get_output_paths_by_root.return_value = {}

        mock_host_path_format_name = PathFormat.get_host_path_format_string()
        boto3_client_mock().get_job.return_value = {
            "name": "Mock Job",
            "attachments": {
                "manifests": [
                    {
                        "rootPath": "/root/path",
                        "rootPathFormat": PathFormat(mock_host_path_format_name),
                        "outputRelativeDirectories": ["."],
                    }
                ],
            },
        }
        boto3_client_mock().get_queue.side_effect = [MOCK_GET_QUEUE_RESPONSE]

        runner = CliRunner()
        result = runner.invoke(
            main,
            ["job", "download-output", "--job-id", MOCK_JOB_ID, "--output", "verbose"],
            input="",
        )

        MockOutputDownloader.assert_called_once_with(
            s3_settings=JobAttachmentS3Settings(**MOCK_GET_QUEUE_RESPONSE["jobAttachmentSettings"]),  # type: ignore
            farm_id=MOCK_FARM_ID,
            queue_id=MOCK_QUEUE_ID,
            job_id=MOCK_JOB_ID,
            step_id=None,
            task_id=None,
            session=ANY,
        )

        assert (
            """Downloading output from Job 'Mock Job'
There are no output files available for download at this moment. Please verify that the Job/Step/Task you are trying to download output from has completed successfully.
"""
            in result.output
        )
        assert result.exit_code == 0


def test_cli_job_download_output_stdout_with_json_format(
    fresh_deadline_config,
    tmp_path: Path,
):
    """
    Tests whether the output messages printed to stdout match expected messages
    when `download-output` command is executed with `--output json` option.
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)

    with patch.object(api, "get_boto3_client") as boto3_client_mock, patch.object(
        job_group, "OutputDownloader"
    ) as MockOutputDownloader, patch.object(job_group, "round", return_value=0), patch.object(
        job_group, "_get_conflicting_filenames", return_value=[]
    ), patch.object(job_group, "_assert_valid_path", return_value=None), patch.object(
        api, "get_queue_user_boto3_session"
    ):
        mock_download = MagicMock()
        mock_download.return_value = DownloadSummaryStatistics(
            total_time=12,
            processed_files=3,
            processed_bytes=1024,
        )
        MockOutputDownloader.return_value.download_job_output = mock_download
        mock_root_path = "/root/path" if sys.platform != "win32" else "C:\\Users\\username"
        mock_files_list = ["outputs/file1.txt", "outputs/file2.txt", "outputs/file3.txt"]
        MockOutputDownloader.return_value.get_output_paths_by_root.side_effect = [
            {
                f"{mock_root_path}": mock_files_list,
                f"{mock_root_path}2": mock_files_list,
            },
            {
                f"{mock_root_path}": mock_files_list,
                f"{mock_root_path}2": mock_files_list,
            },
            {
                f"{mock_root_path}": mock_files_list,
                str(tmp_path): mock_files_list,
            },
        ]

        mock_host_path_format = PathFormat.get_host_path_format()

        boto3_client_mock().get_queue.side_effect = [MOCK_GET_QUEUE_RESPONSE]
        boto3_client_mock().get_job.return_value = {
            "name": "Mock Job",
            "attachments": {
                "manifests": [
                    {
                        "rootPath": f"{mock_root_path}",
                        "rootPathFormat": mock_host_path_format,
                        "outputRelativeDirectories": ["."],
                    },
                    {
                        "rootPath": f"{mock_root_path}2",
                        "rootPathFormat": mock_host_path_format,
                        "outputRelativeDirectories": ["."],
                    },
                ],
            },
        }

        runner = CliRunner()
        result = runner.invoke(
            main,
            ["job", "download-output", "--job-id", MOCK_JOB_ID, "--output", "json"],
            input=json.dumps(
                {"messageType": "pathconfirm", "value": [mock_root_path, str(tmp_path)]}
            ),
        )

        MockOutputDownloader.assert_called_once_with(
            s3_settings=JobAttachmentS3Settings(**MOCK_GET_QUEUE_RESPONSE["jobAttachmentSettings"]),  # type: ignore
            farm_id=MOCK_FARM_ID,
            queue_id=MOCK_QUEUE_ID,
            job_id=MOCK_JOB_ID,
            step_id=None,
            task_id=None,
            session=ANY,
        )

        expected_json_title = json.dumps({"messageType": "title", "value": "Mock Job"})
        expected_json_presummary = json.dumps(
            {
                "messageType": "presummary",
                "value": {
                    mock_root_path: [
                        "outputs/file1.txt",
                        "outputs/file2.txt",
                        "outputs/file3.txt",
                    ],
                    f"{mock_root_path}2": [
                        "outputs/file1.txt",
                        "outputs/file2.txt",
                        "outputs/file3.txt",
                    ],
                },
            }
        )
        expected_json_path = json.dumps(
            {"messageType": "path", "value": [mock_root_path, f"{mock_root_path}2"]}
        )
        expected_json_pathconfirm = json.dumps(
            {"messageType": "pathconfirm", "value": [mock_root_path, str(tmp_path)]}
        )

        assert (
            f"{expected_json_title}\n{expected_json_presummary}\n{expected_json_path}\n {expected_json_pathconfirm}\n"
            in result.output
        )
        assert result.exit_code == 0


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="This is for testing with POSIX paths.",
)
@pytest.mark.parametrize(
    "output_paths_by_root, expected_result",
    [
        (
            {"/home/username/project01": ["renders/image1.png", "renders/image2.png"]},
            "\nSummary of files to download:\n    /home/username/project01/renders (2 files)\n",
        ),
        (
            {
                "/home/username/project01": ["renders/image1.png", "renders/image2.png"],
                "/home/username/project02": [
                    "renders/image1.png",
                    "renders/image2.png",
                    "renders/image3.png",
                ],
            },
            (
                "\nSummary of files to download:\n"
                "    /home/username/project01/renders (2 files)\n"
                "    /home/username/project02/renders (3 files)\n"
            ),
        ),
        (
            {
                "/home/username/project01": [
                    "renders/image1.png",
                    "renders/image2.png",
                    "videos/video.mov",
                ]
            },
            "\nSummary of files to download:\n    /home/username/project01 (3 files)\n",
        ),
        (
            {"C:/Users/username": ["renders/image1.png", "renders/image2.png"]},
            "\nSummary of files to download:\n    C:/Users/username/renders (2 files)\n",
        ),
    ],
)
def test_get_summary_of_files_to_download_message_posix(
    output_paths_by_root: Dict[str, List[str]],
    expected_result: str,
):
    """Tests if the _get_summary_of_files_to_download_message() returns expected string"""
    is_json_format = False
    assert (
        _get_summary_of_files_to_download_message(output_paths_by_root, is_json_format)
        == expected_result
    )


@pytest.mark.skipif(
    sys.platform != "win32",
    reason="This is for testing with Windows paths.",
)
@pytest.mark.parametrize(
    "output_paths_by_root, expected_result",
    [
        (
            {"C:/Users/username": ["renders/image1.png", "renders/image2.png"]},
            "\nSummary of files to download:\n    C:\\Users\\username\\renders (2 files)\n",
        )
    ],
)
def test_get_summary_of_files_to_download_message_windows(
    output_paths_by_root: Dict[str, List[str]],
    expected_result: str,
):
    """Tests if the _get_summary_of_files_to_download_message() returns expected string"""
    is_json_format = False
    assert (
        _get_summary_of_files_to_download_message(output_paths_by_root, is_json_format)
        == expected_result
    )


def test_cli_job_wait_succeeded(fresh_deadline_config):
    """
    Test that job wait command returns exit code 0 when job succeeds.
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config.set_setting("defaults.job_id", MOCK_JOB_ID)

    with patch.object(api, "wait_for_job_completion") as mock_wait, patch.object(
        api, "get_boto3_client"
    ) as boto3_client_mock:
        mock_wait.return_value = api.JobCompletionResult(
            status="SUCCEEDED", failed_tasks=[], elapsed_time=10.5
        )

        # Mock get_job to return job name
        boto3_client_mock().get_job.return_value = {"name": "Test Job Name"}

        runner = CliRunner()
        result = runner.invoke(main, ["job", "wait"])

        # Verify get_job was called to get job name
        boto3_client_mock().get_job.assert_called_once_with(
            farmId=MOCK_FARM_ID, queueId=MOCK_QUEUE_ID, jobId=MOCK_JOB_ID
        )

        assert "Job ID: " + MOCK_JOB_ID in result.output
        assert "Job Name: Test Job Name" in result.output
        assert "Job completed with status: SUCCEEDED" in result.output
        assert "Elapsed time: 10.5 seconds" in result.output
        assert "No failed tasks found." in result.output
        assert result.exit_code == 0


def test_cli_job_wait_timeout(fresh_deadline_config):
    """
    Test that job wait command returns exit code 1 when timeout occurs.
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config.set_setting("defaults.job_id", MOCK_JOB_ID)

    with patch.object(api, "wait_for_job_completion") as mock_wait, patch.object(
        api, "get_boto3_client"
    ) as boto3_client_mock:
        mock_wait.side_effect = DeadlineOperationTimedOut(
            "Timeout waiting for job job-123 to complete after 30.0 seconds"
        )

        # Mock get_job to return job name
        boto3_client_mock().get_job.return_value = {"name": "Test Job Name"}

        runner = CliRunner()
        result = runner.invoke(main, ["job", "wait"])

        assert "Timeout waiting for job" in result.output
        assert result.exit_code == 1


def test_cli_job_wait_failed(fresh_deadline_config):
    """
    Test that job wait command returns exit code 2 when job fails.
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config.set_setting("defaults.job_id", MOCK_JOB_ID)

    with patch.object(api, "wait_for_job_completion") as mock_wait, patch.object(
        api, "get_boto3_client"
    ) as boto3_client_mock:
        mock_wait.return_value = api.JobCompletionResult(
            status="FAILED",
            failed_tasks=[
                api.FailedTask(
                    step_id="step-123",
                    task_id="task-456",
                    step_name="Render Step",
                    parameters={"frame": {"int": 1}},
                    session_id="session-789",
                )
            ],
            elapsed_time=15.2,
        )

        # Mock get_job to return job name
        boto3_client_mock().get_job.return_value = {"name": "Test Job Name"}

        runner = CliRunner()
        result = runner.invoke(main, ["job", "wait"])

        assert "Job ID: " + MOCK_JOB_ID in result.output
        assert "Job Name: Test Job Name" in result.output
        assert "Job completed with status: FAILED" in result.output
        assert "Elapsed time: 15.2 seconds" in result.output
        assert "Found 1 failed tasks:" in result.output
        assert result.exit_code == 2


def test_cli_job_wait_canceled(fresh_deadline_config):
    """
    Test that job wait command returns exit code 3 when job is canceled.
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config.set_setting("defaults.job_id", MOCK_JOB_ID)

    with patch.object(api, "wait_for_job_completion") as mock_wait, patch.object(
        api, "get_boto3_client"
    ) as boto3_client_mock:
        mock_wait.return_value = api.JobCompletionResult(
            status="CANCELED", failed_tasks=[], elapsed_time=5.0
        )

        # Mock get_job to return job name
        boto3_client_mock().get_job.return_value = {"name": "Test Job Name"}

        runner = CliRunner()
        result = runner.invoke(main, ["job", "wait"])

        assert "Job ID: " + MOCK_JOB_ID in result.output
        assert "Job Name: Test Job Name" in result.output
        assert "Job completed with status: CANCELED" in result.output
        assert "Elapsed time: 5.0 seconds" in result.output
        assert "No failed tasks found." in result.output
        assert result.exit_code == 3


def test_cli_job_wait_archived(fresh_deadline_config):
    """
    Test that job wait command returns exit code 4 when job is archived.
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config.set_setting("defaults.job_id", MOCK_JOB_ID)

    with patch.object(api, "wait_for_job_completion") as mock_wait, patch.object(
        api, "get_boto3_client"
    ) as boto3_client_mock:
        mock_wait.return_value = api.JobCompletionResult(
            status="ARCHIVED", failed_tasks=[], elapsed_time=8.3
        )

        # Mock get_job to return job name
        boto3_client_mock().get_job.return_value = {"name": "Test Job Name"}

        runner = CliRunner()
        result = runner.invoke(main, ["job", "wait"])

        assert "Job ID: " + MOCK_JOB_ID in result.output
        assert "Job Name: Test Job Name" in result.output
        assert "Job completed with status: ARCHIVED" in result.output
        assert "Elapsed time: 8.3 seconds" in result.output
        assert "No failed tasks found." in result.output
        assert result.exit_code == 4


def test_cli_job_wait_not_compatible(fresh_deadline_config):
    """
    Test that job wait command returns exit code 5 when job is not compatible.
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config.set_setting("defaults.job_id", MOCK_JOB_ID)

    with patch.object(api, "wait_for_job_completion") as mock_wait, patch.object(
        api, "get_boto3_client"
    ) as boto3_client_mock:
        mock_wait.return_value = api.JobCompletionResult(
            status="NOT_COMPATIBLE", failed_tasks=[], elapsed_time=2.1
        )

        # Mock get_job to return job name
        boto3_client_mock().get_job.return_value = {"name": "Test Job Name"}

        runner = CliRunner()
        result = runner.invoke(main, ["job", "wait"])

        assert "Job ID: " + MOCK_JOB_ID in result.output
        assert "Job Name: Test Job Name" in result.output
        assert "Job completed with status: NOT_COMPATIBLE" in result.output
        assert "Elapsed time: 2.1 seconds" in result.output
        assert "No failed tasks found." in result.output
        assert result.exit_code == 5


def test_cli_job_wait_succeeded_with_failed_tasks_returns_exit_code_2(fresh_deadline_config):
    """
    Test that job wait command returns exit code 2 when there are failed tasks, even if status is SUCCEEDED.
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config.set_setting("defaults.job_id", MOCK_JOB_ID)

    with patch.object(api, "wait_for_job_completion") as mock_wait, patch.object(
        api, "get_boto3_client"
    ) as boto3_client_mock:
        mock_wait.return_value = api.JobCompletionResult(
            status="SUCCEEDED",
            failed_tasks=[
                api.FailedTask(
                    step_id="step-123",
                    task_id="task-456",
                    step_name="Render Step",
                    parameters={"frame": {"int": 1}},
                    session_id="session-789",
                )
            ],
            elapsed_time=12.0,
        )

        # Mock get_job to return job name
        boto3_client_mock().get_job.return_value = {"name": "Test Job Name"}

        runner = CliRunner()
        result = runner.invoke(main, ["job", "wait"])

        assert "Job ID: " + MOCK_JOB_ID in result.output
        assert "Job Name: Test Job Name" in result.output
        assert "Job completed with status: SUCCEEDED" in result.output
        assert "Found 1 failed tasks:" in result.output
        assert result.exit_code == 2


def test_cli_job_wait_json_output_succeeded(fresh_deadline_config):
    """
    Test that job wait command with JSON output returns exit code 0 when job succeeds.
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config.set_setting("defaults.job_id", MOCK_JOB_ID)

    with patch.object(api, "wait_for_job_completion") as mock_wait, patch.object(
        api, "get_boto3_client"
    ) as boto3_client_mock:
        mock_wait.return_value = api.JobCompletionResult(
            status="SUCCEEDED", failed_tasks=[], elapsed_time=10.5
        )

        # Mock get_job to return job name
        boto3_client_mock().get_job.return_value = {"name": "Test Job Name"}

        runner = CliRunner()
        result = runner.invoke(main, ["job", "wait", "--output", "json"])

        # Parse the JSON output
        output_data = json.loads(result.output)
        assert output_data["jobId"] == MOCK_JOB_ID
        assert output_data["jobName"] == "Test Job Name"
        assert output_data["status"] == "SUCCEEDED"
        assert output_data["elapsedTime"] == pytest.approx(10.5)
        assert output_data["failedTasks"] == []
        assert result.exit_code == 0


def test_cli_job_wait_json_output_failed(fresh_deadline_config):
    """
    Test that job wait command with JSON output returns exit code 2 when job fails.
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config.set_setting("defaults.job_id", MOCK_JOB_ID)

    with patch.object(api, "wait_for_job_completion") as mock_wait, patch.object(
        api, "get_boto3_client"
    ) as boto3_client_mock:
        mock_wait.return_value = api.JobCompletionResult(
            status="FAILED",
            failed_tasks=[
                api.FailedTask(
                    step_id="step-123",
                    task_id="task-456",
                    step_name="Render Step",
                    parameters={"frame": {"int": 1}},
                    session_id="session-789",
                )
            ],
            elapsed_time=15.2,
        )

        # Mock get_job to return job name
        boto3_client_mock().get_job.return_value = {"name": "Test Job Name"}

        runner = CliRunner()
        result = runner.invoke(main, ["job", "wait", "--output", "json"])

        # Parse the JSON output
        output_data = json.loads(result.output)
        assert output_data["jobId"] == MOCK_JOB_ID
        assert output_data["jobName"] == "Test Job Name"
        assert output_data["status"] == "FAILED"
        assert output_data["elapsedTime"] == pytest.approx(15.2)
        assert len(output_data["failedTasks"]) == 1
        assert output_data["failedTasks"][0]["stepId"] == "step-123"
        assert output_data["failedTasks"][0]["taskId"] == "task-456"
        assert output_data["failedTasks"][0]["stepName"] == "Render Step"
        assert output_data["failedTasks"][0]["sessionId"] == "session-789"
        assert result.exit_code == 2


def test_cli_job_wait_unknown_status_returns_exit_code_2(fresh_deadline_config):
    """
    Test that job wait command returns exit code 2 for unknown status.
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config.set_setting("defaults.job_id", MOCK_JOB_ID)

    with patch.object(api, "wait_for_job_completion") as mock_wait, patch.object(
        api, "get_boto3_client"
    ) as mock_get_client:
        mock_client = mock_get_client.return_value
        mock_client.get_job.return_value = {"jobId": MOCK_JOB_ID, "name": "Test Job"}

        mock_wait.return_value = api.JobCompletionResult(
            status="UNKNOWN_STATUS", failed_tasks=[], elapsed_time=3.0
        )

        runner = CliRunner()
        result = runner.invoke(main, ["job", "wait"])

        assert "Job ID: " + MOCK_JOB_ID in result.output
        assert "Job Name: Test Job" in result.output
        assert "Job completed with status: UNKNOWN_STATUS" in result.output
        assert result.exit_code == 2


def test_cli_job_wait_timeout_json_output(fresh_deadline_config):
    """
    Test that job wait command handles timeout correctly with JSON output.
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config.set_setting("defaults.job_id", MOCK_JOB_ID)

    with patch.object(api, "wait_for_job_completion") as mock_wait, patch.object(
        api, "get_boto3_client"
    ) as mock_get_client:
        mock_client = mock_get_client.return_value
        mock_client.get_job.return_value = {"jobId": MOCK_JOB_ID, "name": "Test Job"}

        mock_wait.side_effect = DeadlineOperationTimedOut(
            "Timeout waiting for job job-123 to complete after 30.0 seconds"
        )

        runner = CliRunner()
        result = runner.invoke(main, ["job", "wait", "--output", "json"])

        # Parse the JSON output
        output_data = json.loads(result.output)
        assert (
            output_data["error"] == "Timeout waiting for job job-123 to complete after 30.0 seconds"
        )
        assert output_data["timeout"] is True
        assert output_data["jobId"] == MOCK_JOB_ID
        assert output_data["jobName"] == "Test Job"
        assert result.exit_code == 1


def test_cli_job_wait_error_handling(fresh_deadline_config):
    """
    Test that job wait command handles non-timeout errors correctly.
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config.set_setting("defaults.job_id", MOCK_JOB_ID)

    with patch.object(api, "wait_for_job_completion") as mock_wait, patch.object(
        api, "get_boto3_client"
    ) as mock_get_client:
        mock_client = mock_get_client.return_value
        mock_client.get_job.return_value = {"jobId": MOCK_JOB_ID, "name": "Test Job"}

        mock_wait.side_effect = DeadlineOperationError("Test error message")

        runner = CliRunner()
        result = runner.invoke(main, ["job", "wait"])

        assert "Job ID: " + MOCK_JOB_ID in result.output
        assert "Job Name: Test Job" in result.output
        assert "Error waiting for job completion: Test error message" in result.output
        assert result.exit_code == 2


def test_cli_job_wait_error_handling_json_output(fresh_deadline_config):
    """
    Test that job wait command handles non-timeout errors correctly with JSON output.
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config.set_setting("defaults.job_id", MOCK_JOB_ID)

    with patch.object(api, "wait_for_job_completion") as mock_wait, patch.object(
        api, "get_boto3_client"
    ) as mock_get_client:
        mock_client = mock_get_client.return_value
        mock_client.get_job.return_value = {"jobId": MOCK_JOB_ID, "name": "Test Job"}

        mock_wait.side_effect = DeadlineOperationError("Test error message")

        runner = CliRunner()
        result = runner.invoke(main, ["job", "wait", "--output", "json"])

        # Parse the JSON output
        output_data = json.loads(result.output)
        assert output_data["error"] == "Test error message"
        assert output_data["jobId"] == MOCK_JOB_ID
        assert output_data["jobName"] == "Test Job"
        assert result.exit_code == 2


def test_cli_job_download_output_handle_web_url_with_optional_input(fresh_deadline_config):
    """
    Confirm that the CLI interface prints out the expected list of
    farms, given mock data.
    """
    config.set_setting("settings.auto_accept", "true")

    with patch.object(api, "get_boto3_client") as boto3_client_mock, patch.object(
        job_group, "OutputDownloader"
    ) as MockOutputDownloader, patch.object(job_group, "round", return_value=0), patch.object(
        api, "get_queue_user_boto3_session"
    ):
        mock_download = MagicMock()
        mock_download.return_value = DownloadSummaryStatistics(
            total_time=12,
            processed_files=3,
            processed_bytes=1024,
        )
        MockOutputDownloader.return_value.download_job_output = mock_download
        mock_host_path_format_name = PathFormat.get_host_path_format_string()

        boto3_client_mock().get_job.return_value = {
            "name": "Mock Job",
            "attachments": {
                "manifests": [
                    {
                        "rootPath": "/root/path",
                        "rootPathFormat": PathFormat(mock_host_path_format_name),
                        "outputRelativeDirectories": ["."],
                    },
                ],
            },
        }
        boto3_client_mock().get_queue.side_effect = [MOCK_GET_QUEUE_RESPONSE]

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "job",
                "download-output",
                "--farm-id",
                MOCK_FARM_ID,
                "--queue-id",
                MOCK_QUEUE_ID,
                "--job-id",
                MOCK_JOB_ID,
                "--step-id",
                "step-1",
                "--task-id",
                "task-2",
            ],
        )

        MockOutputDownloader.assert_called_once_with(
            s3_settings=JobAttachmentS3Settings(**MOCK_GET_QUEUE_RESPONSE["jobAttachmentSettings"]),  # type: ignore
            farm_id=MOCK_FARM_ID,
            queue_id=MOCK_QUEUE_ID,
            job_id=MOCK_JOB_ID,
            step_id="step-1",
            task_id="task-2",
            session=ANY,
        )
        mock_download.assert_called_once_with(
            file_conflict_resolution=FileConflictResolution.CREATE_COPY,
            on_downloading_files=ANY,
        )
        assert result.exit_code == 0


def test_cli_job_trace_schedule(fresh_deadline_config):
    """
    A very minimal sanity check of the trace-schedule CLI command.
    To test the function more thoroughly involves creating a mock
    set of APIs that return a coherent set of data based on the query
    IDs instead of single mocked returns as this test does.
    """

    with patch.object(api._session, "get_boto3_session") as session_mock:
        deadline_mock = session_mock().client("deadline")
        deadline_mock.get_job.return_value = MOCK_JOBS_LIST[0]
        deadline_mock.list_sessions.return_value = {"sessions": MOCK_SESSIONS_LIST}
        deadline_mock.list_session_actions.return_value = {
            "sessionActions": MOCK_SESSION_ACTIONS_LIST
        }
        deadline_mock.get_step.return_value = MOCK_STEP
        deadline_mock.get_task.return_value = MOCK_TASK

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "job",
                "trace-schedule",
                "--farm-id",
                MOCK_FARM_ID,
                "--queue-id",
                MOCK_QUEUE_ID,
                "--job-id",
                str(MOCK_JOBS_LIST[0]["jobId"]),
            ],
        )

        assert (
            result.output
            == """Getting the job...
Getting all the sessions for the job...
Getting all the session actions for the job...
Getting all the steps and tasks for the job...
Processing the trace data...

 ==== SUMMARY ====

Session Count: 1
Session Total Duration: 0:01:00
Session Action Count: 1
Session Action Total Duration: 0:00:30
Task Run Count: 1
Task Run Total Duration: 0:00:30 (50.0%)
Non-Task Run Count: 0
Non-Task Run Total Duration: 0:00:00 (0.0%)
Sync Job Attachments Count: 0
Sync Job Attachments Total Duration: 0:00:00 (0.0%)
Env Action Count: 0
Env Action Total Duration: 0:00:00 (0.0%)

Within-session Overhead Duration: 0:00:30 (50.0%)
Within-session Overhead Duration Per Action: 0:00:30
"""
        )
        assert result.exit_code == 0


@pytest.mark.usefixtures("fresh_deadline_config")
def test_cli_job_download_output_with_different_asset_root_path_format_than_job(tmp_path: Path):
    """
    Tests whether the output messages printed to stdout match expected messages
    when `download-output` command is executed.
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)

    with patch.object(api, "get_boto3_client") as boto3_client_mock, patch.object(
        job_group, "OutputDownloader"
    ) as MockOutputDownloader, patch.object(job_group, "round", return_value=0), patch.object(
        job_group, "_get_conflicting_filenames", return_value=[]
    ), patch.object(job_group, "_assert_valid_path", return_value=None), patch.object(
        api, "get_queue_user_boto3_session"
    ), patch.object(
        job_group.os.path,
        "expanduser",
        return_value=tmp_path,
    ) as mock_expanduser, patch.object(api._telemetry.TelemetryClient, "record_event", MagicMock()):
        mock_download = MagicMock()
        mock_download.return_value = DownloadSummaryStatistics(
            total_time=12,
            processed_files=3,
            processed_bytes=1024,
        )
        MockOutputDownloader.return_value.download_job_output = mock_download
        windows_root_path = "C:\\Users\\username"
        not_windows_root_path = "/root/path"
        mock_root_path = not_windows_root_path if sys.platform == "win32" else windows_root_path
        mock_files_list = ["outputs/file1.txt", "outputs/file2.txt", "outputs/file3.txt"]
        MockOutputDownloader.return_value.get_output_paths_by_root.side_effect = [
            {
                f"{mock_root_path}": mock_files_list,
            },
            {
                f"{tmp_path}": mock_files_list,
            },
        ]

        mock_host_path_format = PathFormat.POSIX if sys.platform == "win32" else PathFormat.WINDOWS

        boto3_client_mock().get_queue.side_effect = [MOCK_GET_QUEUE_RESPONSE]
        boto3_client_mock().get_job.return_value = {
            "name": "Mock Job",
            "attachments": {
                "manifests": [
                    {
                        "rootPath": f"{mock_root_path}",
                        "rootPathFormat": mock_host_path_format,
                        "outputRelativeDirectories": ["."],
                    },
                ],
            },
        }

        runner = CliRunner()
        result = runner.invoke(
            main,
            ["job", "download-output", "--job-id", MOCK_JOB_ID, "--output", "verbose"],
            input="~\ny\n",
        )

        MockOutputDownloader.assert_called_once_with(
            s3_settings=JobAttachmentS3Settings(**MOCK_GET_QUEUE_RESPONSE["jobAttachmentSettings"]),  # type: ignore
            farm_id=MOCK_FARM_ID,
            queue_id=MOCK_QUEUE_ID,
            job_id=MOCK_JOB_ID,
            step_id=None,
            task_id=None,
            session=ANY,
        )

        path_separator = "/" if sys.platform != "win32" else "\\"

        assert (
            f"""Downloading output from Job 'Mock Job'
This root path format does not match the operating system you're using. Where would you like to save the files?
The location was {not_windows_root_path if sys.platform == "win32" else windows_root_path}, on {"Posix" if sys.platform == "win32" else "Windows"}.
> Please enter a new root path: ~

Summary of files to download:
    {tmp_path}{path_separator}outputs (3 files)

You are about to download files which may come from multiple root directories. Here are a list of the current root directories:
[0] {tmp_path}
> Please enter the index of root directory to edit, y to proceed without changes, or n to cancel the download (0, y, n) [y]: y
"""
            in result.output
        )
        assert "Download Summary:" in result.output
        assert result.exit_code == 0
        mock_expanduser.assert_any_call("~")


class TestJsonLineHelpers:
    """Tests for JSON line helper functions."""

    def test_get_json_line_basic(self):
        """Test _get_json_line with basic parameters."""
        result = _get_json_line("test", "value")
        parsed = json.loads(result)

        assert parsed["messageType"] == "test"
        assert parsed["value"] == "value"
        assert len(parsed) == 2  # Only messageType and value

    def test_get_json_line_with_none_extra_properties(self):
        """Test _get_json_line with explicit None extra_properties."""
        result = _get_json_line("test", "value", extra_properties=None)
        parsed = json.loads(result)

        assert parsed["messageType"] == "test"
        assert parsed["value"] == "value"
        assert len(parsed) == 2  # Only messageType and value

    def test_get_json_line_with_kwargs(self):
        """Test _get_json_line with additional properties."""
        result = _get_json_line(
            "summary", "Downloaded 5 files", extra_properties={"fileCount": 5, "status": "complete"}
        )
        parsed = json.loads(result)

        assert parsed["messageType"] == "summary"
        assert parsed["value"] == "Downloaded 5 files"
        assert parsed["fileCount"] == 5
        assert parsed["status"] == "complete"

    def test_get_json_line_with_list_value(self):
        """Test _get_json_line with list value and extra properties."""
        result = _get_json_line("path", ["/path1", "/path2"], extra_properties={"count": 2})
        parsed = json.loads(result)

        assert parsed["messageType"] == "path"
        assert parsed["value"] == ["/path1", "/path2"]
        assert parsed["count"] == 2

    def test_get_download_summary_message_json_with_file_count(self):
        """Test _get_download_summary_message includes fileCount in JSON format."""
        from deadline.job_attachments.progress_tracker import DownloadSummaryStatistics

        # Create mock download summary
        summary = DownloadSummaryStatistics()
        summary.processed_files = 3
        summary.processed_bytes = 1024
        summary.total_time = 2.5
        summary.transfer_rate = 409.6
        summary.file_counts_by_root_directory = {"/downloads": 3}

        result = _get_download_summary_message(summary, is_json_format=True)
        parsed = json.loads(result)

        assert parsed["messageType"] == "summary"
        assert parsed["value"] == "Downloaded 3 files"
        assert parsed["fileCount"] == 3

    def test_get_download_summary_message_json_zero_files(self):
        """Test _get_download_summary_message with zero files."""
        from deadline.job_attachments.progress_tracker import DownloadSummaryStatistics

        summary = DownloadSummaryStatistics()
        summary.processed_files = 0

        result = _get_download_summary_message(summary, is_json_format=True)
        parsed = json.loads(result)

        assert parsed["messageType"] == "summary"
        assert parsed["value"] == "Downloaded 0 files"
        assert parsed["fileCount"] == 0

    def test_get_download_summary_message_non_json_unchanged(self):
        """Test _get_download_summary_message non-JSON format is unchanged."""
        from deadline.job_attachments.progress_tracker import DownloadSummaryStatistics

        summary = DownloadSummaryStatistics()
        summary.processed_files = 2
        summary.processed_bytes = 512
        summary.total_time = 1.0
        summary.transfer_rate = 512.0
        summary.file_counts_by_root_directory = {"/downloads": 2}

        result = _get_download_summary_message(summary, is_json_format=False)

        # Should be human-readable format, not JSON
        assert "Download Summary:" in result
        assert "Downloaded 2 files totaling" in result
        assert not result.startswith('{"messageType":')
