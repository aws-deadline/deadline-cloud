# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for the CLI job commands.
"""
import datetime
import json
import os
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
from deadline.job_attachments.models import (
    FileConflictResolution,
    JobAttachmentS3Settings,
    PathFormat,
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
    Tests whether the ouptut messages printed to stdout match expected messages
    when download-output command is executed.
    """
    with patch.object(api._session, "get_deadline_endpoint_url") as session_endpoint:
        session_endpoint.return_value = "some-endpoint-url"
        config.set_setting("defaults.farm_id", MOCK_FARM_ID)
        config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)

    with patch.object(api, "get_boto3_client") as boto3_client_mock, patch.object(
        job_group, "OutputDownloader"
    ) as MockOutputDownloader, patch.object(
        job_group, "_get_conflicting_filenames", return_value=[]
    ), patch.object(
        job_group, "round", return_value=0
    ), patch.object(
        api, "get_queue_user_boto3_session"
    ):
        mock_download = MagicMock()
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

        assert (
            f"""Downloading output from Job 'Mock Job'
Outputs will be downloaded to the following root paths:
[0] {mock_root_path}
[1] {mock_root_path}2
> Please enter a number of root path to edit, y to proceed, or n to cancel the download: (0, 1, y, n) [y]: 1
> Please enter a new path for the root directory [{mock_root_path}2]: {str(tmp_path)}
Outputs will be downloaded to the following root paths:
[0] {mock_root_path}
[1] {str(tmp_path)}
> Please enter a number of root path to edit, y to proceed, or n to cancel the download: (0, 1, y, n) [y]: y
"""
            in result.output
        )
        assert "Download Summary:" in result.output
        assert result.exit_code == 0


def test_cli_job_dowuload_output_stdout_with_json_format(
    fresh_deadline_config,
    tmp_path: Path,
):
    """
    Tests whether the ouptut messages printed to stdout match expected messages
    when download-output command is executed with `--output json` option.
    """
    with patch.object(api._session, "get_deadline_endpoint_url") as session_endpoint:
        session_endpoint.return_value = "fake-endpoint-url"
        config.set_setting("defaults.farm_id", MOCK_FARM_ID)
        config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)

        with patch.object(api, "get_boto3_client") as boto3_client_mock, patch.object(
            job_group, "OutputDownloader"
        ) as MockOutputDownloader, patch.object(job_group, "round", return_value=0), patch.object(
            job_group, "_get_conflicting_filenames", return_value=[]
        ), patch.object(
            job_group, "_assert_valid_path", return_value=None
        ), patch.object(
            api, "get_queue_user_boto3_session"
        ):
            mock_download = MagicMock()
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
            expected_json_path = json.dumps(
                {"messageType": "path", "value": [mock_root_path, f"{mock_root_path}2"]}
            )
            expected_json_pathconfirm = json.dumps(
                {"messageType": "pathconfirm", "value": [mock_root_path, str(tmp_path)]}
            )

            assert (
                f"{expected_json_title}\n{expected_json_path}\n {expected_json_pathconfirm}\n"
                in result.output
            )
            assert result.exit_code == 0


def test_cli_job_download_output_handle_web_url_with_optional_input(fresh_deadline_config):
    """
    Confirm that the CLI interface prints out the expected list of
    farms, given mock data.
    """
    with patch.object(api._session, "get_deadline_endpoint_url") as session_endpoint:
        session_endpoint.return_value = "fake-endpoint-url"

        with patch.object(api, "get_boto3_client") as boto3_client_mock, patch.object(
            job_group, "OutputDownloader"
        ) as MockOutputDownloader, patch.object(job_group, "round", return_value=0), patch.object(
            api, "get_queue_user_boto3_session"
        ):
            mock_download = MagicMock()
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
            "sessionactions": MOCK_SESSION_ACTIONS_LIST
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
