# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for the CLI handle-web-url command.
"""

import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Dict, List
from unittest.mock import ANY, MagicMock, call, patch

import pytest
from click.testing import CliRunner

from deadline.client import api
from deadline.client.config import set_setting
from deadline.client.cli import main
from deadline.client.cli._deadline_web_url import (
    parse_query_string,
    validate_id_format,
    validate_resource_ids,
)
from deadline.client.cli._groups import handle_web_url_command, job_group
from deadline.client.exceptions import DeadlineOperationError
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
    MOCK_SESSION_ACTION_ID,
    MOCK_STEP_ID,
    MOCK_TASK_ID,
    MOCK_PROFILE_NAME,
)


def test_parse_query_string():
    """
    A few successful test cases.
    """
    assert parse_query_string("ab-c=def&x=73&xyz=testing-value", ["ab-c", "x", "xyz"], []) == {
        "ab_c": "def",
        "x": "73",
        "xyz": "testing-value",
    }
    assert parse_query_string("a=b&c=d", ["a", "c", "f", "g"], ["a", "c"]) == {
        "a": "b",
        "c": "d",
    }


def test_parse_query_string_missing_required():
    """
    Tests with missing required parameters
    """
    with pytest.raises(DeadlineOperationError) as excinfo:
        parse_query_string(
            "a-1=b&c=d", ["a-1", "c", "missing-required", "g"], ["a-1", "c", "missing-required"]
        )
    assert "did not contain the required parameter" in str(excinfo)
    assert "missing-required" in str(excinfo)

    # The error message lists all the missing parameters
    with pytest.raises(DeadlineOperationError) as excinfo:
        parse_query_string(
            "a=b&c=d",
            ["a", "c", "missing-required", "also-not-here", "not-required"],
            ["a", "c", "missing-required", "also-not-here"],
        )
    assert "did not contain the required parameter" in str(excinfo)
    assert "missing-required" in str(excinfo)
    assert "also-not-here" in str(excinfo)
    assert "not-required" not in str(excinfo)


def test_parse_query_string_extra_parameters():
    """
    Tests with parameters that are not supposed to be there
    """
    with pytest.raises(DeadlineOperationError) as excinfo:
        parse_query_string("a=b&c=d&extra-parameter=3", ["a", "c"], ["a"])
    assert "contained unsupported parameter" in str(excinfo)
    assert "extra-parameter" in str(excinfo)

    # The error message lists all the extra parameters
    with pytest.raises(DeadlineOperationError) as excinfo:
        parse_query_string(
            "a=b&c=d&extra-parameter=3&more-too-much=100&acceptable-one=xyz",
            ["a", "c", "acceptable-one"],
            ["a"],
        )
    assert "contained unsupported parameter" in str(excinfo)
    assert "extra-parameter" in str(excinfo)
    assert "more-too-much" in str(excinfo)
    assert "acceptable-one" not in str(excinfo)


def test_parse_query_string_duplicate_parameters():
    """
    Tests with a repeated parameter
    """
    with pytest.raises(DeadlineOperationError) as excinfo:
        parse_query_string(
            "duplicated-param=b&c=d&duplicated-param=e", ["duplicated-param", "c"], ["c"]
        )
    assert "provided multiple times" in str(excinfo)
    assert "duplicated-param" in str(excinfo)


@pytest.mark.parametrize(
    "ids",
    [
        {
            "farm_id": MOCK_FARM_ID,
            "queue_id": MOCK_QUEUE_ID,
            "job_id": MOCK_JOB_ID,
            "step_id": MOCK_STEP_ID,
            "task_id": MOCK_TASK_ID,
        },
        {
            "farm_id": MOCK_FARM_ID,
            "queue_id": MOCK_QUEUE_ID,
            "job_id": MOCK_JOB_ID,
        },
        {
            "farm_id": MOCK_FARM_ID,
            "queue_id": MOCK_QUEUE_ID,
        },
        {
            "farm_id": MOCK_FARM_ID,
        },
    ],
)
def test_validate_resource_ids_successful(ids: Dict[str, str]):
    """
    A few successful test cases.
    """
    validate_resource_ids(ids)


@pytest.mark.parametrize(
    ("ids", "exception_message"),
    [
        ({"": ""}, 'The given resource ID "": "" has invalid format.'),
        (
            {"farm_id": "farm-123"},
            'The given resource ID "farm_id": "farm-123" has invalid format.',
        ),
        (
            {"farm_id": "farm-0123456789abcdefabcdefabcdefabc"},
            'The given resource ID "farm_id": "farm-0123456789abcdefabcdefabcdefabc" has invalid format.',
        ),
        (
            {"farm_id": "farm-0123456789abcdefabcdefabcdefabcdef"},
            'The given resource ID "farm_id": "farm-0123456789abcdefabcdefabcdefabcdef" has invalid format.',
        ),
        (
            {"farm_id": "0123456789abcdefabcdefabcdefabcd"},
            'The given resource ID "farm_id": "0123456789abcdefabcdefabcdefabcd" has invalid format.',
        ),
        (
            {"farm_id": "far-0123456789abcdefabcdefabcdefabcd"},
            'The given resource ID "farm_id": "far-0123456789abcdefabcdefabcdefabcd" has invalid format.',
        ),
        (
            {"farm_id": "-farm-0123456789abcdefabcdefabcdefabcd"},
            'The given resource ID "farm_id": "-farm-0123456789abcdefabcdefabcdefabcd" has invalid format.',
        ),
        (
            {"farm_id": "farm--0123456789abcdefabcdefabcdefabcd"},
            'The given resource ID "farm_id": "farm--0123456789abcdefabcdefabcdefabcd" has invalid format.',
        ),
        (
            {"farm_id": "farm-farm-0123456789abcdefabcdefabcdefabcd"},
            'The given resource ID "farm_id": "farm-farm-0123456789abcdefabcdefabcdefabcd" has invalid format.',
        ),
        (
            {"mission_id": "mission-0123456789abcdefabcdefabcdefabcd"},
            'The given resource ID "mission_id": "mission-0123456789abcdefabcdefabcdefabcd" has invalid format.',
        ),
        (
            {"farm_id": MOCK_QUEUE_ID},
            f'The given resource ID "farm_id": "{MOCK_QUEUE_ID}" has invalid format.',
        ),
        (
            {"farm_id": MOCK_FARM_ID, "queue_id": "queue-123"},
            'The given resource ID "queue_id": "queue-123" has invalid format.',
        ),
        (
            {"task_id": "task-0123456789abcdefabcdefabcdefabcd"},
            'The given resource ID "task_id": "task-0123456789abcdefabcdefabcdefabcd" has invalid format.',
        ),
    ],
)
def test_validate_resource_ids_failed(ids: Dict[str, str], exception_message: str):
    """
    Tests with invalid IDs.
    """
    with pytest.raises(DeadlineOperationError) as excinfo:
        validate_resource_ids(ids)
    assert exception_message in str(excinfo)


@pytest.mark.parametrize(
    ("resource_type", "full_id_str"),
    [
        ("farm", MOCK_FARM_ID),
        ("queue", MOCK_QUEUE_ID),
        ("job", MOCK_JOB_ID),
        ("step", MOCK_STEP_ID),
        ("task", MOCK_TASK_ID),
    ],
)
def test_validate_id_format_successful(resource_type: str, full_id_str: str):
    """
    A few successful test cases.
    """
    assert validate_id_format(resource_type, full_id_str)


@pytest.mark.parametrize(
    ("resource_type", "full_id_str"),
    [
        ("", ""),
        ("farm", ""),
        ("farm", "farm-123"),
        ("farm", "farm0123456789abcdefabcdefabcdefabcd"),
        ("farm", "farm--0123456789abcdefabcdefabcdefabcd"),
        ("farm", "farm-farm-0123456789abcdefabcdefabcdefabcd"),
        ("farm", "farm-0123456789abcdefabcdefabcdezxvzx"),
        ("farm", "farm-0123456789abcdefabcdefabcde!@#$%"),
        ("farm", "farm-0123456789abcdefabcdefabcdefabcd00000"),
        ("farm", "queue-0123456789abcdefabcdefabcdefabcd"),
        ("farmfarm", "farmfarm-0123456789abcdefabcdefabcdefabcd"),
        ("mission", "mission-0123456789abcdefabcdefabcdefabcd"),
        ("task", "task-0123456789abcdefabcdefabcdefabcd"),
        ("task", "task-0123456789abcdefabcdefabcdefabcd-00"),
        ("task", "task-0123456789abcdefabcdefabcdefabcd-12345678912345"),
    ],
)
def test_validate_id_format_failed(resource_type: str, full_id_str: str):
    """
    Tests with invalid IDs.
    """
    assert not validate_id_format(resource_type, full_id_str)


def test_cli_handle_web_url_download_output_only_required_input(fresh_deadline_config):
    """
    Confirm that the CLI interface prints out the expected list of
    farms, given mock data.
    """
    set_setting("settings.auto_accept", "true")

    with patch.object(api, "get_boto3_client") as boto3_client_mock, patch.object(
        job_group, "OutputDownloader"
    ) as MockOutputDownloader, patch.object(api, "get_queue_user_boto3_session"):
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
                    }
                ],
            },
        }
        boto3_client_mock().get_queue.side_effect = [MOCK_GET_QUEUE_RESPONSE]

        web_url = f"deadline://download-output?farm-id={MOCK_FARM_ID}&queue-id={MOCK_QUEUE_ID}&job-id={MOCK_JOB_ID}"

        runner = CliRunner()
        result = runner.invoke(main, ["handle-web-url", web_url])

        MockOutputDownloader.assert_called_once_with(
            s3_settings=JobAttachmentS3Settings(**MOCK_GET_QUEUE_RESPONSE["jobAttachmentSettings"]),  # type: ignore
            farm_id=MOCK_FARM_ID,
            queue_id=MOCK_QUEUE_ID,
            job_id=MOCK_JOB_ID,
            step_id=None,
            task_id=None,
            session_action_id=None,
            session=ANY,
        )
        mock_download.assert_called_once_with(
            file_conflict_resolution=FileConflictResolution.CREATE_COPY,
            on_downloading_files=ANY,
        )
        assert result.exit_code == 0, result.output


def test_cli_handle_web_url_download_output_with_optional_input(fresh_deadline_config):
    """
    Confirm that the CLI interface prints out the expected list of
    farms, given mock data.
    """
    set_setting("settings.auto_accept", "true")

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
                    }
                ],
            },
        }
        boto3_client_mock().get_queue.side_effect = [MOCK_GET_QUEUE_RESPONSE]

        web_url = (
            f"deadline://download-output?farm-id={MOCK_FARM_ID}&queue-id={MOCK_QUEUE_ID}&job-id={MOCK_JOB_ID}"
            + f"&step-id={MOCK_STEP_ID}&task-id={MOCK_TASK_ID}&profile={MOCK_PROFILE_NAME}"
        )

        boto3_client_mock().get_task.return_value = {
            "latestSessionActionId": MOCK_SESSION_ACTION_ID,
        }

        runner = CliRunner()
        result = runner.invoke(main, ["handle-web-url", web_url])
        assert result.exit_code == 0, result.output

        MockOutputDownloader.assert_called_once_with(
            s3_settings=JobAttachmentS3Settings(**MOCK_GET_QUEUE_RESPONSE["jobAttachmentSettings"]),  # type: ignore
            farm_id=MOCK_FARM_ID,
            queue_id=MOCK_QUEUE_ID,
            job_id=MOCK_JOB_ID,
            step_id=MOCK_STEP_ID,
            task_id=MOCK_TASK_ID,
            session_action_id=MOCK_SESSION_ACTION_ID,
            session=ANY,
        )
        mock_download.assert_called_once_with(
            file_conflict_resolution=FileConflictResolution.CREATE_COPY,
            on_downloading_files=ANY,
        )


def test_cli_handle_web_url_unsupported_protocol_scheme(fresh_deadline_config):
    """
    Tests that an error is returned when an unsupported url is passed to the handle-web-url command
    """
    runner = CliRunner()
    result = runner.invoke(main, ["handle-web-url", "https://sketchy-website.com"])

    assert "URL scheme https is not supported." in result.output
    assert result.exit_code != 0


def test_cli_handle_web_url_command_not_allowlisted(fresh_deadline_config):
    """
    Tests that a command that isn't explicitly allowlisted isn't ran.
    """
    runner = CliRunner()
    result = runner.invoke(main, ["handle-web-url", "deadline://config"])

    assert "Command config is not supported through handle-web-url." in result.output
    assert result.exit_code != 0


def test_handle_web_url_command_not_allowlisted_with_prompt(fresh_deadline_config):
    """
    Tests that a command that isn't explicitly allowlisted isn't ran. Test with a prompt for the exit.
    """
    runner = CliRunner()
    result = runner.invoke(
        main,
        ["handle-web-url", "deadline://config", "--prompt-when-complete"],
        input="\n",
    )

    assert "Command config is not supported through handle-web-url." in result.output
    assert result.exit_code != 0


@pytest.mark.parametrize(
    "url, missing_names",
    [
        ("deadline://download-output?queue-id=queue-2&job-id=job-3", ["farm-id"]),
        ("deadline://download-output?farm-id=farm-1&job-id=job-3", ["queue-id"]),
        ("deadline://download-output?farm-id=farm-1&queue-id=queue-2", ["job-id"]),
        ("deadline://download-output?farm-id=farm-1", ["queue-id", "job-id"]),
        ("deadline://download-output", ["farm-id", "job-id", "queue-id"]),
    ],
)
def test_handle_web_url_missing_required_args(
    fresh_deadline_config, url: str, missing_names: List[str]
):
    """
    Tests an error is returned when a required argument is missing.
    """
    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "handle-web-url",
            url,
        ],
    )

    assert "The URL query did not contain the required parameter(s)" in result.output
    for name in missing_names:
        assert name in result.output
    assert result.exit_code != 0


@pytest.mark.parametrize("install_option", ["--install", "--uninstall", "--all-users"])
def test_handle_web_url_incorrect_install_option(fresh_deadline_config, install_option):
    """
    Tests that the install/uninstall commands cannot be used with a URL
    """
    runner = CliRunner()
    result = runner.invoke(
        main,
        ["handle-web-url", "deadline://config", install_option],
    )

    assert (
        "The --install, --uninstall and --all-users options cannot be used with a provided URL."
        in result.output
    )
    assert result.exit_code != 0


def test_handle_web_url_both_install_and_uninstall(fresh_deadline_config):
    """
    Tests that install & uninstall cannot be used together
    """
    runner = CliRunner()
    result = runner.invoke(
        main,
        ["handle-web-url", "--install", "--uninstall"],
    )

    assert "Only one of the --install and --uninstall options may be provided." in result.output
    assert result.exit_code != 0


def test_handle_web_url_require_url_or_install_option(fresh_deadline_config):
    """
    Tests that install & uninstall cannot be used together
    """
    runner = CliRunner()
    result = runner.invoke(
        main,
        ["handle-web-url"],
    )

    assert "At least one of a URL, --install, or --uninstall must be provided." in result.output
    assert result.exit_code != 0


def test_cli_handle_web_url_install_frozen_exe(fresh_deadline_config, monkeypatch):
    """
    When running as a PyInstaller frozen binary, sys.argv[0] is already
    "deadline.exe". with_suffix(".exe") should be a no-op.
    """
    winreg_mock = MagicMock()
    monkeypatch.setitem(sys.modules, "winreg", winreg_mock)

    exe_path = r"C:\Program Files\DeadlineClient\deadline.exe"

    with patch.object(sys, "platform", "win32"), patch.object(sys, "argv", [exe_path]), patch(
        "deadline.client.cli._deadline_web_url.Path.resolve",
        return_value=Path(exe_path),
    ), patch("os.path.isfile", return_value=True) as isfile_mock:
        winreg_mock.HKEY_CURRENT_USER = "HKEY_CURRENT_USER"
        winreg_mock.REG_SZ = "REG_SZ"
        winreg_mock.CreateKeyEx.side_effect = ["FIRST_CREATED_KEY", "SECOND_CREATED_KEY"]

        from deadline.client.cli._deadline_web_url import install_deadline_web_url_handler

        install_deadline_web_url_handler(all_users=False)

        # with_suffix(".exe") is a no-op when already .exe
        isfile_mock.assert_called_once_with(exe_path)


def test_cli_handle_web_url_install_pip_console_script(fresh_deadline_config, monkeypatch):
    """
    When running from a pip/uv-installed console_script, sys.argv[0] is
    extensionless (e.g. "deadline"). The handler should append .exe.
    """
    winreg_mock = MagicMock()
    monkeypatch.setitem(sys.modules, "winreg", winreg_mock)

    script_path = r"C:\Scripts\deadline"

    with patch.object(sys, "platform", "win32"), patch.object(sys, "argv", [script_path]), patch(
        "deadline.client.cli._deadline_web_url.Path.resolve",
        return_value=Path(script_path),
    ), patch("os.path.isfile", return_value=True) as isfile_mock:
        winreg_mock.HKEY_CURRENT_USER = "HKEY_CURRENT_USER"
        winreg_mock.REG_SZ = "REG_SZ"
        winreg_mock.CreateKeyEx.side_effect = ["FIRST_CREATED_KEY", "SECOND_CREATED_KEY"]

        from deadline.client.cli._deadline_web_url import install_deadline_web_url_handler

        install_deadline_web_url_handler(all_users=False)

        # Should append .exe to the extensionless console_script path
        isfile_mock.assert_called_once_with(script_path + ".exe")


@pytest.mark.parametrize("install_command", ["install", "uninstall"])
@pytest.mark.parametrize("all_users", [True, False])
def test_cli_handle_web_url_install(fresh_deadline_config, install_command, all_users):
    """
    Confirm that the install command calls the implementation function
    """
    with patch.object(
        handle_web_url_command, f"{install_command}_deadline_web_url_handler"
    ) as mock_install:
        runner = CliRunner()
        cli_options = ["handle-web-url", f"--{install_command}"]
        if all_users:
            cli_options.append("--all-users")
        result = runner.invoke(main, cli_options)

        mock_install.assert_called_once_with(all_users=all_users)
        assert result.exit_code == 0


@pytest.mark.parametrize("all_users", [True, False])
def test_cli_handle_web_url_install_prints_success_message(fresh_deadline_config, all_users):
    """
    Confirm that --install prints a success message on completion.
    """
    with patch.object(handle_web_url_command, "install_deadline_web_url_handler"):
        runner = CliRunner()
        cli_options = ["handle-web-url", "--install"]
        if all_users:
            cli_options.append("--all-users")
        result = runner.invoke(main, cli_options)

        assert result.exit_code == 0
        assert "Web URL handler installed successfully." in result.output


@pytest.mark.parametrize("all_users", [True, False])
def test_cli_handle_web_url_uninstall_prints_success_message(fresh_deadline_config, all_users):
    """
    Confirm that --uninstall prints a success message on completion.
    """
    with patch.object(handle_web_url_command, "uninstall_deadline_web_url_handler"):
        runner = CliRunner()
        cli_options = ["handle-web-url", "--uninstall"]
        if all_users:
            cli_options.append("--all-users")
        result = runner.invoke(main, cli_options)

        assert result.exit_code == 0
        assert "Web URL handler uninstalled successfully." in result.output


def test_cli_handle_web_url_install_current_user_monkeypatched_windows(
    fresh_deadline_config, monkeypatch
):
    """
    Tests the registry installation calls for Windows URL handler installers.
    By monkeypatching, we can test this on any OS.

    This test is pretty inflexible, it verifies that the specific set of known-good winreg
    calls are made.
    """

    winreg_mock = MagicMock()
    monkeypatch.setitem(sys.modules, "winreg", winreg_mock)

    with patch.object(sys, "platform", "win32"), patch.object(os.path, "isfile") as isfile_mock:
        # Tell the handler that everything is a file, so it succeeds when it checks on argv[0]
        isfile_mock.return_value = True
        winreg_mock.HKEY_CURRENT_USER = "HKEY_CURRENT_USER"
        winreg_mock.HKEY_CLASSES_ROOT = "HKEY_CLASSES_ROOT"
        winreg_mock.REG_SZ = "REG_SZ"
        winreg_mock.OpenKeyEx.side_effect = ["FIRST_OPENED_KEY", "SECOND_OPENED_KEY"]
        winreg_mock.CreateKeyEx.side_effect = ["FIRST_CREATED_KEY", "SECOND_CREATED_KEY"]

        runner = CliRunner()
        result = runner.invoke(main, ["handle-web-url", "--install"])

        winreg_mock.assert_has_calls(
            [
                call.CreateKeyEx("HKEY_CURRENT_USER", "Software\\Classes\\deadline"),
                call.SetValueEx(
                    "FIRST_CREATED_KEY", None, 0, "REG_SZ", "URL:AWS Deadline Cloud Protocol"
                ),
                call.SetValueEx("FIRST_CREATED_KEY", "URL Protocol", 0, "REG_SZ", ""),
                call.CreateKeyEx("FIRST_CREATED_KEY", "shell\\open\\command"),
                call.SetValueEx(
                    "SECOND_CREATED_KEY",
                    None,
                    0,
                    "REG_SZ",
                    ANY,
                ),
                call.CloseKey("SECOND_CREATED_KEY"),
                call.CloseKey("FIRST_CREATED_KEY"),
            ]
        )
        assert "Web URL handler installed successfully." in result.output


def test_cli_handle_web_url_install_all_users_monkeypatched_windows(
    fresh_deadline_config, monkeypatch
):
    """
    Tests the registry installation calls for Windows URL handler installers.
    By monkeypatching, we can test this on any OS.

    This test is pretty inflexible, it verifies that the specific set of known-good winreg
    calls are made.
    """

    winreg_mock = MagicMock()
    monkeypatch.setitem(sys.modules, "winreg", winreg_mock)

    with patch.object(sys, "platform", "win32"), patch.object(os.path, "isfile") as isfile_mock:
        # Tell the handler that everything is a file, so it succeeds when it checks on argv[0]
        isfile_mock.return_value = True
        winreg_mock.HKEY_CURRENT_USER = "HKEY_CURRENT_USER"
        winreg_mock.HKEY_CLASSES_ROOT = "HKEY_CLASSES_ROOT"
        winreg_mock.REG_SZ = "REG_SZ"
        winreg_mock.OpenKeyEx.side_effect = ["FIRST_OPENED_KEY", "SECOND_OPENED_KEY"]
        winreg_mock.CreateKeyEx.side_effect = ["FIRST_CREATED_KEY", "SECOND_CREATED_KEY"]

        runner = CliRunner()
        result = runner.invoke(main, ["handle-web-url", "--install", "--all-users"])

        winreg_mock.assert_has_calls(
            [
                call.CreateKeyEx("HKEY_CLASSES_ROOT", "deadline"),
                call.SetValueEx(
                    "FIRST_CREATED_KEY", None, 0, "REG_SZ", "URL:AWS Deadline Cloud Protocol"
                ),
                call.SetValueEx("FIRST_CREATED_KEY", "URL Protocol", 0, "REG_SZ", ""),
                call.CreateKeyEx("FIRST_CREATED_KEY", "shell\\open\\command"),
                call.SetValueEx(
                    "SECOND_CREATED_KEY",
                    None,
                    0,
                    "REG_SZ",
                    ANY,
                ),
                call.CloseKey("SECOND_CREATED_KEY"),
                call.CloseKey("FIRST_CREATED_KEY"),
            ]
        )
        assert "Web URL handler installed successfully." in result.output


def test_cli_handle_web_url_uninstall_current_user_monkeypatched_windows(
    fresh_deadline_config, monkeypatch
):
    """
    Tests the registry installation calls for Windows URL handler installers.
    By monkeypatching, we can test this on any OS.

    This test is pretty inflexible, it verifies that the specific set of known-good winreg
    calls are made.
    """

    winreg_mock = MagicMock()
    monkeypatch.setitem(sys.modules, "winreg", winreg_mock)

    with patch.object(sys, "platform", "win32"), patch.object(os.path, "isfile") as isfile_mock:
        # Tell the handler that everything is a file, so it succeeds when it checks on argv[0]
        isfile_mock.return_value = True
        winreg_mock.HKEY_CURRENT_USER = "HKEY_CURRENT_USER"
        winreg_mock.HKEY_CLASSES_ROOT = "HKEY_CLASSES_ROOT"
        winreg_mock.REG_SZ = "REG_SZ"
        winreg_mock.OpenKeyEx.side_effect = ["FIRST_OPENED_KEY", "SECOND_OPENED_KEY"]
        winreg_mock.CreateKeyEx.side_effect = ["FIRST_CREATED_KEY", "SECOND_CREATED_KEY"]

        runner = CliRunner()
        result = runner.invoke(main, ["handle-web-url", "--uninstall"])

        winreg_mock.assert_has_calls(
            [
                call.OpenKeyEx("HKEY_CURRENT_USER", "Software\\Classes"),
                call.DeleteKeyEx("FIRST_OPENED_KEY", "deadline\\shell\\open\\command"),
                call.DeleteKeyEx("FIRST_OPENED_KEY", "deadline\\shell\\open"),
                call.DeleteKeyEx("FIRST_OPENED_KEY", "deadline\\shell"),
                call.DeleteKeyEx("FIRST_OPENED_KEY", "deadline"),
                call.CloseKey("FIRST_OPENED_KEY"),
            ]
        )
        assert "Web URL handler uninstalled successfully." in result.output


def test_cli_handle_web_url_uninstall_all_users_monkeypatched_windows(
    fresh_deadline_config, monkeypatch
):
    """
    Tests the registry installation calls for Windows URL handler installers.
    By monkeypatching, we can test this on any OS.

    This test is pretty inflexible, it verifies that the specific set of known-good winreg
    calls are made.
    """

    winreg_mock = MagicMock()
    monkeypatch.setitem(sys.modules, "winreg", winreg_mock)

    with patch.object(sys, "platform", "win32"), patch.object(os.path, "isfile") as isfile_mock:
        # Tell the handler that everything is a file, so it succeeds when it checks on argv[0]
        isfile_mock.return_value = True
        winreg_mock.HKEY_CURRENT_USER = "HKEY_CURRENT_USER"
        winreg_mock.HKEY_CLASSES_ROOT = "HKEY_CLASSES_ROOT"
        winreg_mock.REG_SZ = "REG_SZ"
        winreg_mock.OpenKeyEx.side_effect = ["FIRST_OPENED_KEY", "SECOND_OPENED_KEY"]
        winreg_mock.CreateKeyEx.side_effect = ["FIRST_CREATED_KEY", "SECOND_CREATED_KEY"]

        runner = CliRunner()
        result = runner.invoke(main, ["handle-web-url", "--uninstall", "--all-users"])

        winreg_mock.assert_has_calls(
            [
                call.DeleteKeyEx("HKEY_CLASSES_ROOT", "deadline\\shell\\open\\command"),
                call.DeleteKeyEx("HKEY_CLASSES_ROOT", "deadline\\shell\\open"),
                call.DeleteKeyEx("HKEY_CLASSES_ROOT", "deadline\\shell"),
                call.DeleteKeyEx("HKEY_CLASSES_ROOT", "deadline"),
            ]
        )
        assert "Web URL handler uninstalled successfully." in result.output


def test_linux_install_generates_valid_desktop_file(fresh_deadline_config, tmp_path):
    """
    Tests that the generated .desktop file on Linux has the expected contents.
    """
    entry_dir = tmp_path / "applications"
    entry_dir.mkdir()
    config_dir = tmp_path / "config"
    config_dir.mkdir()

    desktop_file_path = str(entry_dir / "deadline.desktop")

    with patch.object(sys, "platform", "linux"), patch.object(
        sys, "argv", ["/usr/bin/deadline"]
    ), patch.object(shutil, "which", return_value="/usr/bin/deadline"), patch.object(
        os.path,
        "expanduser",
        side_effect=lambda p: p.replace("~/.local/share", str(tmp_path)).replace(
            "~/.config", str(config_dir)
        ),
    ), patch.object(subprocess, "run"), patch.object(os, "makedirs"):
        from deadline.client.cli._deadline_web_url import install_deadline_web_url_handler

        install_deadline_web_url_handler(all_users=False)

    with open(desktop_file_path) as f:
        desktop_content = f.read()
    assert desktop_content == (
        "[Desktop Entry]\n"
        "Type=Application\n"
        "Name=deadline\n"
        "Exec=/usr/bin/deadline handle-web-url %u\n"
        "Terminal=true\n"
        "MimeType=x-scheme-handler/deadline\n"
    )


def test_linux_install_resolves_bare_command_via_shutil_which(fresh_deadline_config, tmp_path):
    """
    Tests that on Linux, when sys.argv[0] is a bare command name (e.g. 'deadline'),
    the install resolves the full path using shutil.which.
    """
    entry_dir = tmp_path / "applications"
    entry_dir.mkdir()
    config_dir = tmp_path / "config"
    config_dir.mkdir()

    desktop_file_path = str(entry_dir / "deadline.desktop")

    with patch.object(sys, "platform", "linux"), patch.object(
        sys, "argv", ["deadline"]
    ), patch.object(
        shutil,
        "which",
        side_effect=lambda cmd: (
            "/opt/deadline/bin/deadline" if cmd == "deadline" else "/usr/bin/" + cmd
        ),
    ), patch.object(
        os.path,
        "expanduser",
        side_effect=lambda p: p.replace("~/.local/share", str(tmp_path)).replace(
            "~/.config", str(config_dir)
        ),
    ), patch.object(subprocess, "run"), patch.object(os, "makedirs"):
        from deadline.client.cli._deadline_web_url import install_deadline_web_url_handler

        install_deadline_web_url_handler(all_users=False)

    with open(desktop_file_path) as f:
        desktop_content = f.read()
    assert "Exec=/opt/deadline/bin/deadline handle-web-url %u" in desktop_content


def test_linux_install_raises_when_command_not_found(fresh_deadline_config, tmp_path):
    """
    Tests that on Linux, when shutil.which cannot find the command,
    the install raises a DeadlineOperationError.
    """
    with patch.object(sys, "platform", "linux"), patch.object(
        sys, "argv", ["deadline"]
    ), patch.object(
        shutil, "which", side_effect=lambda cmd: None if cmd == "deadline" else "/usr/bin/" + cmd
    ):
        from deadline.client.cli._deadline_web_url import install_deadline_web_url_handler
        from deadline.client.exceptions import DeadlineOperationError

        with pytest.raises(DeadlineOperationError, match="could not find 'deadline' on PATH"):
            install_deadline_web_url_handler(all_users=False)
