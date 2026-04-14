# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for the CLI job bundle commands.
"""

import os
import sys
import json
from typing import Generator
from unittest.mock import ANY, patch, call, MagicMock

import boto3  # type: ignore[import]
from click.testing import CliRunner
import pytest

import deadline.client.ui
from deadline.client import config
import deadline.client.api as api_module
from deadline.client.cli import main
from deadline.job_attachments.models import JobAttachmentsFileSystem
from deadline.job_attachments.upload import S3AssetManager
from deadline.client.dataclasses import SubmitterInfo

from ..api.test_job_bundle_submission import (
    MOCK_FARM_ID,
    MOCK_JOB_TEMPLATE_CASES,
    MOCK_PARAMETERS_CASES,
    MOCK_QUEUE_ID,
    get_minimal_json_job_template,
)

from ..testing_utilities import (
    MOCK_CREATE_JOB_RESPONSE,
    MOCK_GET_JOB_RESPONSE,
)


@pytest.fixture
def mock_job_bundle_submitter() -> Generator[MagicMock, None, None]:
    mock_job_bundle_submitter = MagicMock()
    with patch.dict(
        sys.modules, {"deadline.client.ui.job_bundle_submitter": mock_job_bundle_submitter}
    ):
        yield mock_job_bundle_submitter


def test_cli_bundle_submit_simple_json_template(
    fresh_deadline_config, deadline_mock, temp_job_bundle_dir
):
    """
    Confirm that CLI bundle submit makes the right create_job call from a simple JSON template.
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)

    deadline_mock.create_job.return_value = MOCK_CREATE_JOB_RESPONSE
    deadline_mock.get_job.return_value = MOCK_GET_JOB_RESPONSE

    # Write a JSON template
    with open(os.path.join(temp_job_bundle_dir, "template.json"), "w", encoding="utf8") as f:
        f.write(MOCK_JOB_TEMPLATE_CASES["MINIMAL_JSON"][1])

    # Write out some parameters
    with open(
        os.path.join(temp_job_bundle_dir, "parameter_values.yaml"),
        "w",
        encoding="utf8",
    ) as f:
        f.write(MOCK_PARAMETERS_CASES["TEMPLATE_ONLY_JSON"][1])

    runner = CliRunner()
    result = runner.invoke(main, ["bundle", "submit", temp_job_bundle_dir])

    deadline_mock.create_job.assert_called_with(
        farmId=MOCK_FARM_ID,
        queueId=MOCK_QUEUE_ID,
        parameters=MOCK_PARAMETERS_CASES["TEMPLATE_ONLY_JSON"][2]["parameters"],  # type: ignore
        template=MOCK_JOB_TEMPLATE_CASES["MINIMAL_JSON"][1],
        templateType="JSON",
        priority=50,
    )
    assert "Submitting to Queue: Mock Queue" in result.output, result.output
    assert f"Submitted job bundle:\n   {temp_job_bundle_dir}\n" in result.output, result.output
    assert MOCK_CREATE_JOB_RESPONSE["jobId"] in result.output, result.output
    assert MOCK_GET_JOB_RESPONSE["lifecycleStatusMessage"] in result.output, result.output
    assert result.exit_code == 0, result.output


def test_cli_bundle_explicit_parameters(fresh_deadline_config, temp_job_bundle_dir):
    """
    Confirm that --profile, --farm-id, and --queue-id get passed in from the CLI.

    This test mocks the Session object instead of using moto, to confirm calls with an
    AWS profile that doesn't exist in the config.
    """

    # Write a JSON template
    with open(os.path.join(temp_job_bundle_dir, "template.json"), "w", encoding="utf8") as f:
        f.write(MOCK_JOB_TEMPLATE_CASES["MINIMAL_JSON"][1])

    with patch.object(boto3, "Session") as session_mock:
        session_mock().client().create_job.return_value = MOCK_CREATE_JOB_RESPONSE
        session_mock().client().get_job.return_value = MOCK_GET_JOB_RESPONSE

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "bundle",
                "submit",
                temp_job_bundle_dir,
                "--profile",
                "NonDefaultProfileName",
                "--farm-id",
                MOCK_FARM_ID,
                "--queue-id",
                MOCK_QUEUE_ID,
            ],
        )

    session_mock.assert_called_with(profile_name="NonDefaultProfileName", botocore_session=ANY)
    session_mock().client().create_job.assert_called_once_with(
        farmId=MOCK_FARM_ID,
        queueId=MOCK_QUEUE_ID,
        template=ANY,
        templateType="JSON",
        priority=50,
    )

    assert temp_job_bundle_dir in result.output, result.output
    assert MOCK_CREATE_JOB_RESPONSE["jobId"] in result.output, result.output
    assert MOCK_GET_JOB_RESPONSE["lifecycleStatusMessage"] in result.output, result.output
    assert result.exit_code == 0, result.output


def test_cli_bundle_priority_retries(fresh_deadline_config, deadline_mock, temp_job_bundle_dir):
    """
    Confirm that --priority, --max-failed-tasks-count, --max_worker_count and --max-retries-per-task get passed in from the CLI.
    """
    # Write a JSON template
    with open(os.path.join(temp_job_bundle_dir, "template.json"), "w", encoding="utf8") as f:
        f.write(MOCK_JOB_TEMPLATE_CASES["MINIMAL_JSON"][1])

    deadline_mock.create_job.return_value = MOCK_CREATE_JOB_RESPONSE
    deadline_mock.get_job.return_value = MOCK_GET_JOB_RESPONSE

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "bundle",
            "submit",
            temp_job_bundle_dir,
            "--farm-id",
            MOCK_FARM_ID,
            "--queue-id",
            MOCK_QUEUE_ID,
            "--priority",
            "25",
            "--max-failed-tasks-count",
            "12",
            "--max-retries-per-task",
            "4",
            "--max-worker-count",
            "123",
        ],
    )

    assert temp_job_bundle_dir in result.output
    assert MOCK_CREATE_JOB_RESPONSE["jobId"] in result.output
    assert MOCK_GET_JOB_RESPONSE["lifecycleStatusMessage"] in result.output
    deadline_mock.create_job.assert_called_once_with(
        farmId=MOCK_FARM_ID,
        queueId=MOCK_QUEUE_ID,
        template=ANY,
        templateType="JSON",
        priority=25,
        maxFailedTasksCount=12,
        maxRetriesPerTask=4,
        maxWorkerCount=123,
    )
    assert result.exit_code == 0


def test_cli_bundle_job_name(fresh_deadline_config, deadline_mock, temp_job_bundle_dir):
    """
    Confirm that --name sets the job name in the template.
    """
    # Write a JSON template
    with open(os.path.join(temp_job_bundle_dir, "template.json"), "w", encoding="utf8") as f:
        f.write(MOCK_JOB_TEMPLATE_CASES["MINIMAL_JSON"][1])

    deadline_mock.create_job.return_value = MOCK_CREATE_JOB_RESPONSE
    deadline_mock.get_job.return_value = MOCK_GET_JOB_RESPONSE

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "bundle",
            "submit",
            temp_job_bundle_dir,
            "--farm-id",
            MOCK_FARM_ID,
            "--queue-id",
            MOCK_QUEUE_ID,
            "--name",
            "Replacement Name For The Job",
        ],
    )

    assert temp_job_bundle_dir in result.output
    assert MOCK_CREATE_JOB_RESPONSE["jobId"] in result.output
    assert MOCK_GET_JOB_RESPONSE["lifecycleStatusMessage"] in result.output
    deadline_mock.create_job.assert_called_once_with(
        farmId=MOCK_FARM_ID,
        queueId=MOCK_QUEUE_ID,
        template=get_minimal_json_job_template("Replacement Name For The Job"),
        templateType="JSON",
        priority=50,
    )
    assert result.exit_code == 0


def test_cli_bundle_storage_profile_id(fresh_deadline_config, deadline_mock, temp_job_bundle_dir):
    """
    Confirm that --storage-profile-id sets the ID that the job is submitted with, but does not
    change the value of storage profile saved to the configuration file.
    """
    PRE_STORAGE_PROFILE_ID = "sp-11223344556677889900abbccddeeff"
    CLI_STORAGE_PROFILE_ID = "sp-0000000000000000000000000000000"

    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)

    # Set the storage profile ID in the config; as someone may have by using `deadline config set`
    config.set_setting("settings.storage_profile_id", PRE_STORAGE_PROFILE_ID)

    # Write a JSON template
    with open(os.path.join(temp_job_bundle_dir, "template.json"), "w", encoding="utf8") as f:
        f.write(MOCK_JOB_TEMPLATE_CASES["MINIMAL_JSON"][1])

    deadline_mock.create_job.return_value = MOCK_CREATE_JOB_RESPONSE
    deadline_mock.get_job.return_value = MOCK_GET_JOB_RESPONSE

    runner = CliRunner()
    with patch.object(api_module, "get_storage_profile_for_queue"):
        result = runner.invoke(
            main,
            [
                "bundle",
                "submit",
                temp_job_bundle_dir,
                "--storage-profile-id",
                CLI_STORAGE_PROFILE_ID,
            ],
        )

    assert temp_job_bundle_dir in result.output
    assert MOCK_CREATE_JOB_RESPONSE["jobId"] in result.output
    assert MOCK_GET_JOB_RESPONSE["lifecycleStatusMessage"] in result.output
    deadline_mock.create_job.assert_called_once_with(
        farmId=MOCK_FARM_ID,
        queueId=MOCK_QUEUE_ID,
        template=ANY,
        templateType="JSON",
        priority=50,
        storageProfileId=CLI_STORAGE_PROFILE_ID,
    )
    assert result.exit_code == 0
    # Force a re-load from disk of the config object
    with patch.object(config.config_file, "_should_read_config", return_value=True):
        assert config.get_setting("settings.storage_profile_id") == PRE_STORAGE_PROFILE_ID


@pytest.mark.parametrize("loading_method", [e.value for e in JobAttachmentsFileSystem] + [None])
def test_cli_bundle_asset_load_method(
    fresh_deadline_config, deadline_mock, temp_job_bundle_dir, loading_method
):
    """
    Verify that asset loading method set on CLI are passed to the CreateJob call.

    The job attachments S3 bucket is a moto mock, so the verified calls exercise the relevant code for that.
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config.set_setting("settings.auto_accept", "true")

    # Write a JSON template
    with open(os.path.join(temp_job_bundle_dir, "template.json"), "w", encoding="utf8") as f:
        f.write(MOCK_JOB_TEMPLATE_CASES["MINIMAL_JSON"][1])

    # Write out some parameters
    with open(
        os.path.join(temp_job_bundle_dir, "parameter_values.yaml"),
        "w",
        encoding="utf8",
    ) as f:
        f.write(MOCK_PARAMETERS_CASES["TEMPLATE_ONLY_JSON"][1])

    # Write out the temp directory as an attachment
    with open(
        os.path.join(temp_job_bundle_dir, "asset_references.json"),
        "w",
        encoding="utf8",
    ) as f:
        data = {
            "assetReferences": {
                "inputs": {
                    "directories": [temp_job_bundle_dir],
                    "filenames": [],
                },
                "outputs": {"directories": [temp_job_bundle_dir]},
            }
        }
        json.dump(data, f)

    deadline_mock.create_job.return_value = MOCK_CREATE_JOB_RESPONSE
    deadline_mock.get_job.return_value = MOCK_GET_JOB_RESPONSE

    params = ["bundle", "submit", temp_job_bundle_dir]

    # None case represents not setting the parameter
    if loading_method is not None:
        params += ["--job-attachments-file-system", loading_method]

    runner = CliRunner()
    result = runner.invoke(main, params)

    expected_loading_method = (
        loading_method
        if loading_method is not None
        else config.get_setting("defaults.job_attachments_file_system")
    )

    assert temp_job_bundle_dir in result.output, result.output
    deadline_mock.create_job.assert_called_once_with(
        farmId=MOCK_FARM_ID,
        queueId=MOCK_QUEUE_ID,
        parameters=MOCK_PARAMETERS_CASES["TEMPLATE_ONLY_JSON"][2]["parameters"],  # type: ignore
        template=MOCK_JOB_TEMPLATE_CASES["MINIMAL_JSON"][1],
        templateType="JSON",
        attachments={
            "fileSystem": expected_loading_method,
            "manifests": [
                {
                    "rootPath": temp_job_bundle_dir,
                    "rootPathFormat": "windows" if os.name == "nt" else "posix",
                    "inputManifestPath": ANY,
                    "inputManifestHash": ANY,
                    "outputRelativeDirectories": ["."],
                }
            ],
        },
        priority=50,
    )
    assert MOCK_CREATE_JOB_RESPONSE["jobId"] in result.output, result.output
    assert MOCK_GET_JOB_RESPONSE["lifecycleStatusMessage"] in result.output, result.output
    assert result.exit_code == 0, result.output


def test_cli_bundle_job_parameter_from_cli(
    fresh_deadline_config, deadline_mock, temp_job_bundle_dir
):
    """
    Verify that job parameters specified at the CLI are passed to the CreateJob call
    """
    # Write a JSON template
    with open(os.path.join(temp_job_bundle_dir, "template.json"), "w", encoding="utf8") as f:
        f.write(MOCK_JOB_TEMPLATE_CASES["MINIMAL_JSON"][1])

    deadline_mock.create_job.return_value = MOCK_CREATE_JOB_RESPONSE
    deadline_mock.get_job.return_value = MOCK_GET_JOB_RESPONSE

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "bundle",
            "submit",
            temp_job_bundle_dir,
            "--farm-id",
            MOCK_FARM_ID,
            "--queue-id",
            MOCK_QUEUE_ID,
            "--parameter",
            "sceneFile=/path/to/scenefile",
            "--parameter",
            "priority=90",
            "--priority",
            "45",
            "--submitter-name",
            "MyDCC",
        ],
    )

    deadline_mock.create_job.assert_called_once_with(
        farmId=MOCK_FARM_ID,
        queueId=MOCK_QUEUE_ID,
        template=ANY,
        templateType="JSON",
        parameters={
            "sceneFile": {"string": "/path/to/scenefile"},
            "priority": {"int": "90"},
        },
        priority=45,
    )

    deadline_mock.get_deadline_cloud_library_telemetry_client.return_value.record_event.assert_any_call(
        event_type="com.amazon.rum.deadline.submission",
        event_details={"submitter_name": "MyDCC"},
        from_gui=False,
    )

    assert result.exit_code == 0, result.output


def test_cli_bundle_empty_job_parameter_from_cli(
    fresh_deadline_config, deadline_mock, temp_job_bundle_dir
):
    """
    Verify that an empty job parameter specified at the CLI are passed to the CreateJob call
    """
    # Write a JSON template
    with open(os.path.join(temp_job_bundle_dir, "template.json"), "w", encoding="utf8") as f:
        f.write(MOCK_JOB_TEMPLATE_CASES["MINIMAL_JSON"][1])

    deadline_mock.create_job.return_value = MOCK_CREATE_JOB_RESPONSE
    deadline_mock.get_job.return_value = MOCK_GET_JOB_RESPONSE

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "bundle",
            "submit",
            temp_job_bundle_dir,
            "--farm-id",
            MOCK_FARM_ID,
            "--queue-id",
            MOCK_QUEUE_ID,
            "--parameter",
            "sceneFile=",
        ],
    )

    assert deadline_mock.create_job.mock_calls == [
        call(
            farmId=MOCK_FARM_ID,
            queueId=MOCK_QUEUE_ID,
            template=ANY,
            templateType="JSON",
            parameters={
                "sceneFile": {"string": ""},
            },
            priority=50,
        )
    ], result.output

    assert result.exit_code == 0, result.output


def test_cli_bundle_job_parameter_with_equals_from_cli(
    fresh_deadline_config, deadline_mock, temp_job_bundle_dir
):
    """
    Verify that a job parameter value with an '=' in it is passed correctly to the CreateJob call
    """
    # Write a JSON template
    with open(os.path.join(temp_job_bundle_dir, "template.json"), "w", encoding="utf8") as f:
        f.write(MOCK_JOB_TEMPLATE_CASES["MINIMAL_JSON"][1])

    deadline_mock.create_job.return_value = MOCK_CREATE_JOB_RESPONSE
    deadline_mock.get_job.return_value = MOCK_GET_JOB_RESPONSE

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "bundle",
            "submit",
            temp_job_bundle_dir,
            "--farm-id",
            MOCK_FARM_ID,
            "--queue-id",
            MOCK_QUEUE_ID,
            "--parameter",
            "sceneFile=this=is=a=test",
        ],
    )

    deadline_mock.create_job.assert_called_once_with(
        farmId=MOCK_FARM_ID,
        queueId=MOCK_QUEUE_ID,
        template=ANY,
        templateType="JSON",
        parameters={
            "sceneFile": {"string": "this=is=a=test"},
        },
        priority=50,
    )

    assert result.exit_code == 0, result.output


def test_cli_bundle_invalid_job_parameter(
    fresh_deadline_config, deadline_mock, temp_job_bundle_dir
):
    """
    Verify that a badly formatted parameter value (without "Key=Value") throws an error
    """
    # Write a JSON template
    with open(os.path.join(temp_job_bundle_dir, "template.json"), "w", encoding="utf8") as f:
        f.write(MOCK_JOB_TEMPLATE_CASES["MINIMAL_JSON"][1])

    deadline_mock.create_job.return_value = MOCK_CREATE_JOB_RESPONSE
    deadline_mock.get_job.return_value = MOCK_GET_JOB_RESPONSE

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "bundle",
            "submit",
            temp_job_bundle_dir,
            "--farm-id",
            MOCK_FARM_ID,
            "--queue-id",
            MOCK_QUEUE_ID,
            "--parameter",
            "BadParam",
        ],
    )

    assert 'Parameters must be provided in the format "ParamName=Value"' in result.output
    assert result.exit_code == 2


def test_cli_bundle_invalid_job_parameter_name(
    fresh_deadline_config, deadline_mock, temp_job_bundle_dir
):
    """
    Verify that a non-identifier parameter name raises an error.
    """
    # Write a JSON template
    with open(os.path.join(temp_job_bundle_dir, "template.json"), "w", encoding="utf8") as f:
        f.write(MOCK_JOB_TEMPLATE_CASES["MINIMAL_JSON"][1])

    deadline_mock.create_job.return_value = MOCK_CREATE_JOB_RESPONSE
    deadline_mock.get_job.return_value = MOCK_GET_JOB_RESPONSE

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "bundle",
            "submit",
            temp_job_bundle_dir,
            "--farm-id",
            MOCK_FARM_ID,
            "--queue-id",
            MOCK_QUEUE_ID,
            "--parameter",
            "Param*Name=Value",
        ],
    )

    assert "Parameter names must be alphanumeric Open Job Description identifiers." in result.output
    assert result.exit_code == 2


def test_cli_bundle_accept_upload_confirmation(
    fresh_deadline_config, deadline_mock, temp_job_bundle_dir
):
    """
    Verify that when the user accepts the job attachments upload confirmation
    that CreateJob is called properly still.
    """

    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config.set_setting("settings.auto_accept", "false")

    # Write a JSON template
    with open(os.path.join(temp_job_bundle_dir, "template.json"), "w", encoding="utf8") as f:
        f.write(MOCK_JOB_TEMPLATE_CASES["MINIMAL_JSON"][1])
    # Write a single asset path
    with open(
        os.path.join(temp_job_bundle_dir, "asset_references.yaml"), "w", encoding="utf8"
    ) as f:
        data = {
            "assetReferences": {
                "inputs": {
                    "directories": [temp_job_bundle_dir],
                    "filenames": [],
                },
                "outputs": {"directories": [temp_job_bundle_dir]},
            }
        }
        json.dump(data, f)

    deadline_mock.create_job.return_value = MOCK_CREATE_JOB_RESPONSE
    deadline_mock.get_job.return_value = MOCK_GET_JOB_RESPONSE

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "bundle",
            "submit",
            temp_job_bundle_dir,
            "--farm-id",
            MOCK_FARM_ID,
            "--queue-id",
            MOCK_QUEUE_ID,
        ],
        input="y",
    )

    deadline_mock.create_job.assert_called_with(
        farmId=MOCK_FARM_ID,
        queueId=MOCK_QUEUE_ID,
        template=MOCK_JOB_TEMPLATE_CASES["MINIMAL_JSON"][1],
        templateType="JSON",
        attachments=ANY,
        priority=50,
    )
    assert result.exit_code == 0, result.output


def test_cli_bundle_reject_upload_confirmation(
    fresh_deadline_config, deadline_mock, temp_job_bundle_dir
):
    """
    Verify that when the user rejects the job attachments upload confirmation
    that no further action is taken after that point, and that a failure CLI exit code results.
    """

    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config.set_setting("settings.auto_accept", "false")

    # Write a JSON template
    with open(os.path.join(temp_job_bundle_dir, "template.json"), "w", encoding="utf8") as f:
        f.write(MOCK_JOB_TEMPLATE_CASES["MINIMAL_JSON"][1])
    # Write a single asset path
    with open(
        os.path.join(temp_job_bundle_dir, "asset_references.yaml"), "w", encoding="utf8"
    ) as f:
        data = {
            "assetReferences": {
                "inputs": {
                    "directories": [temp_job_bundle_dir],
                    "filenames": [],
                },
                "outputs": {"directories": [temp_job_bundle_dir]},
            }
        }
        json.dump(data, f)

    deadline_mock.create_job.return_value = MOCK_CREATE_JOB_RESPONSE
    deadline_mock.get_job.return_value = MOCK_GET_JOB_RESPONSE

    with patch.object(S3AssetManager, "upload_assets") as mock_upload_assets:
        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "bundle",
                "submit",
                temp_job_bundle_dir,
                "--farm-id",
                MOCK_FARM_ID,
                "--queue-id",
                MOCK_QUEUE_ID,
            ],
            input="n",
        )

        mock_upload_assets.assert_not_called()
        assert result.exit_code == 1


@patch.object(deadline.client.ui, "gui_context_for_cli")
def test_gui_submit_submitter_name(_mock_context, mock_job_bundle_submitter):
    """
    Verify that the DEPRECATED --submitter-name arg gets passed through correctly
    """

    runner = CliRunner()
    runner.invoke(
        main,
        ["bundle", "gui-submit", "--browse", "--submitter-name", "MyDCC"],
    )
    _args, kwargs = mock_job_bundle_submitter.show_job_bundle_submitter.call_args
    assert kwargs["submitter_info"] == SubmitterInfo(submitter_name="MyDCC")


@patch.object(deadline.client.ui, "gui_context_for_cli")
def test_gui_submit_name(_mock_context, mock_job_bundle_submitter):
    """
    Verify that --name gets passed through to show_job_bundle_submitter.
    """
    runner = CliRunner()
    runner.invoke(
        main,
        ["bundle", "gui-submit", "--browse", "--name", "My Custom Job Name"],
    )
    _args, kwargs = mock_job_bundle_submitter.show_job_bundle_submitter.call_args
    assert kwargs["name"] == "My Custom Job Name"


@patch.object(deadline.client.ui, "gui_context_for_cli")
def test_gui_submit_name_default(_mock_context, mock_job_bundle_submitter):
    """
    Verify that when --name is not provided, None is passed through.
    """
    runner = CliRunner()
    runner.invoke(
        main,
        ["bundle", "gui-submit", "--browse"],
    )
    _args, kwargs = mock_job_bundle_submitter.show_job_bundle_submitter.call_args
    assert kwargs["name"] is None


def test_bundle_submit_with_target_task_run_status(
    fresh_deadline_config, deadline_mock, temp_job_bundle_dir
):
    """
    Test that the --target-task-run-status CLI option is passed through correctly.
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)

    # Write a JSON template
    with open(os.path.join(temp_job_bundle_dir, "template.json"), "w", encoding="utf8") as f:
        f.write(MOCK_JOB_TEMPLATE_CASES["MINIMAL_JSON"][1])

    deadline_mock.create_job.return_value = MOCK_CREATE_JOB_RESPONSE
    deadline_mock.get_job.return_value = MOCK_GET_JOB_RESPONSE

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "bundle",
            "submit",
            "--target-task-run-status",
            "SUSPENDED",
            temp_job_bundle_dir,
        ],
    )

    assert result.exit_code == 0
    deadline_mock.create_job.assert_called_once()
    _, kwargs = deadline_mock.create_job.call_args
    assert kwargs["targetTaskRunStatus"] == "SUSPENDED"


def test_bundle_submit_without_target_task_run_status(
    fresh_deadline_config, deadline_mock, temp_job_bundle_dir
):
    """
    Test that when --target-task-run-status is not specified, None is passed.
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)

    # Write a JSON template
    with open(os.path.join(temp_job_bundle_dir, "template.json"), "w", encoding="utf8") as f:
        f.write(MOCK_JOB_TEMPLATE_CASES["MINIMAL_JSON"][1])

    deadline_mock.create_job.return_value = MOCK_CREATE_JOB_RESPONSE
    deadline_mock.get_job.return_value = MOCK_GET_JOB_RESPONSE

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "bundle",
            "submit",
            temp_job_bundle_dir,
        ],
    )

    assert result.exit_code == 0
    deadline_mock.create_job.assert_called_once()
    _, kwargs = deadline_mock.create_job.call_args
    assert "targetTaskRunStatus" not in kwargs


@patch.object(deadline.client.ui, "gui_context_for_cli")
def test_bundle_gui_submit_submitter_info_key_value_pairs(_mock_context, mock_job_bundle_submitter):
    """
    Test that --submitter-info works with multiple key=value pairs.
    """

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "bundle",
            "gui-submit",
            "--browse",
            "--submitter-info",
            "submitter_name=Maya",
            "--submitter-info",
            "host_application_name=Maya",
            "--submitter-info",
            "host_application_version=2024",
        ],
    )

    assert result.exit_code == 0, result.output
    _args, kwargs = mock_job_bundle_submitter.show_job_bundle_submitter.call_args
    submitter_info = kwargs["submitter_info"]
    assert submitter_info is not None
    assert submitter_info.submitter_name == "Maya"
    assert submitter_info.host_application_name == "Maya"
    assert submitter_info.host_application_version == "2024"
    assert submitter_info.submitter_package_name is None
    assert submitter_info.submitter_package_version is None
    assert submitter_info.additional_info is None


@patch.object(deadline.client.ui, "gui_context_for_cli")
def test_bundle_gui_submit_submitter_info_inline_json(_mock_context, mock_job_bundle_submitter):
    """
    Test that --submitter-info works with inline JSON.
    """

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "bundle",
            "gui-submit",
            "--browse",
            "--submitter-info",
            '{"submitter_name": "Blender", "host_application_name": "Blender", "host_application_version": "4.0", "additional_info": {"render_engine": "Cycles", "some_versions": ["1.0", "1.1", "1.2"]}}',
        ],
    )

    assert result.exit_code == 0, result.output
    _args, kwargs = mock_job_bundle_submitter.show_job_bundle_submitter.call_args
    submitter_info = kwargs["submitter_info"]
    assert submitter_info is not None
    assert submitter_info.submitter_name == "Blender"
    assert submitter_info.host_application_name == "Blender"
    assert submitter_info.host_application_version == "4.0"
    assert submitter_info.submitter_package_name is None
    assert submitter_info.submitter_package_version is None
    assert submitter_info.additional_info == {
        "render_engine": "Cycles",
        "some_versions": ["1.0", "1.1", "1.2"],
    }


@patch.object(deadline.client.ui, "gui_context_for_cli")
def test_bundle_gui_submit_submitter_info_missing_submitter_name(
    _mock_context, mock_job_bundle_submitter
):
    """
    Test that --submitter-info without submitter_name shows an error.
    """

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "bundle",
            "gui-submit",
            "--browse",
            "--submitter-info",
            "host_application_name=Maya",
        ],
    )

    assert result.exit_code != 0
    assert "submitter_name is required" in result.output


@patch.object(deadline.client.ui, "gui_context_for_cli")
def test_bundle_gui_submit_submitter_info_unknown_field_key_value(
    _mock_context, mock_job_bundle_submitter
):
    """
    Test that --submitter-info with an unknown field in key=value format shows an error.
    """

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "bundle",
            "gui-submit",
            "--browse",
            "--submitter-info",
            "submitter_name=Maya",
            "--submitter-info",
            "unknown_field=value",
        ],
    )

    assert result.exit_code != 0
    assert "Unknown field" in result.output


@patch.object(deadline.client.ui, "gui_context_for_cli")
def test_bundle_gui_submit_submitter_info_unknown_field_json(
    _mock_context, mock_job_bundle_submitter
):
    """
    Test that --submitter-info with an unknown field in JSON format shows an error.
    """

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "bundle",
            "gui-submit",
            "--browse",
            "--submitter-info",
            '{"submitter_name": "Maya", "invalid_key": "value"}',
        ],
    )

    assert result.exit_code != 0
    assert "Unknown field" in result.output


@patch.object(deadline.client.ui, "gui_context_for_cli")
def test_bundle_gui_submit_submitter_info_invalid_json(_mock_context, mock_job_bundle_submitter):
    """
    Test that invalid JSON in --submitter-info shows an error.
    """

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "bundle",
            "gui-submit",
            "--browse",
            "--submitter-info",
            '{"submitter_name": "Test", invalid}',
        ],
    )

    assert result.exit_code != 0
    assert "not formatted correctly" in result.output


@patch.object(deadline.client.ui, "gui_context_for_cli")
def test_bundle_gui_submit_submitter_info_invalid_key_value_format(
    _mock_context, mock_job_bundle_submitter
):
    """
    Test that invalid key=value format (missing equals sign) shows an error.
    """

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "bundle",
            "gui-submit",
            "--browse",
            "--submitter-info",
            "invalid_format_no_equals",
        ],
    )

    assert result.exit_code != 0
    assert "not formatted correctly" in result.output


@patch.object(deadline.client.ui, "gui_context_for_cli")
def test_bundle_gui_submit_submitter_info_multiple_json_allowed(
    _mock_context, mock_job_bundle_submitter
):
    """
    Test that multiple JSON objects are allowed and merged.
    """

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "bundle",
            "gui-submit",
            "--browse",
            "--submitter-info",
            '{"submitter_name": "Test1"}',
            "--submitter-info",
            '{"host_application_name": "Test2"}',
        ],
    )

    assert result.exit_code == 0, result.output
    _args, kwargs = mock_job_bundle_submitter.show_job_bundle_submitter.call_args
    submitter_info = kwargs["submitter_info"]
    assert submitter_info is not None
    assert submitter_info.submitter_name == "Test1"
    assert submitter_info.host_application_name == "Test2"


@patch.object(deadline.client.ui, "gui_context_for_cli")
def test_bundle_gui_submit_submitter_name_overrides_submitter_info(
    _mock_context, mock_job_bundle_submitter
):
    """
    Test that when both --submitter-name and --submitter-info are provided,
    --submitter-name takes precedence and a deprecation warning is shown.
    """

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "bundle",
            "gui-submit",
            "--browse",
            "--submitter-name",
            "DeprecatedName",
            "--submitter-info",
            "submitter_name=NewName",
            "--submitter-info",
            "host_application_name=Maya",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "DeprecationWarning: The option --submitter-name is deprecated" in result.output

    _args, kwargs = mock_job_bundle_submitter.show_job_bundle_submitter.call_args
    submitter_info = kwargs["submitter_info"]
    assert submitter_info is not None
    assert submitter_info.submitter_name == "DeprecatedName"
    assert submitter_info.host_application_name == "Maya"


@patch.object(deadline.client.ui, "gui_context_for_cli")
def test_bundle_gui_submit_submitter_info_json_file(
    _mock_context, tmp_path, mock_job_bundle_submitter
):
    """
    Test that --submitter-info works with a JSON file path.
    """

    # Create a temporary JSON file
    json_file = tmp_path / "submitter.json"
    json_data = {
        "submitter_name": "Maya",
        "host_application_name": "Maya",
        "host_application_version": "2024",
        "additional_info": {"render_engine": "Arnold", "plugins": ["mtoa", "redshift"]},
    }
    json_file.write_text(json.dumps(json_data))

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "bundle",
            "gui-submit",
            "--browse",
            "--submitter-info",
            f"file://{json_file}",
        ],
    )

    assert result.exit_code == 0, result.output
    _args, kwargs = mock_job_bundle_submitter.show_job_bundle_submitter.call_args
    submitter_info = kwargs["submitter_info"]
    assert submitter_info is not None
    assert submitter_info.submitter_name == "Maya"
    assert submitter_info.host_application_name == "Maya"
    assert submitter_info.host_application_version == "2024"
    assert submitter_info.additional_info == {
        "render_engine": "Arnold",
        "plugins": ["mtoa", "redshift"],
    }


@patch.object(deadline.client.ui, "gui_context_for_cli")
def test_bundle_gui_submit_submitter_info_yaml_file(
    _mock_context, tmp_path, mock_job_bundle_submitter
):
    """
    Test that --submitter-info works with a YAML file path.
    """

    # Create a temporary YAML file
    yaml_file = tmp_path / "submitter.yaml"
    yaml_data = """
submitter_name: Blender
host_application_name: Blender
host_application_version: "4.0"
additional_info:
  render_engine: Cycles
  samples: 128
"""
    yaml_file.write_text(yaml_data)

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "bundle",
            "gui-submit",
            "--browse",
            "--submitter-info",
            f"file://{yaml_file}",
        ],
    )

    assert result.exit_code == 0, result.output
    _args, kwargs = mock_job_bundle_submitter.show_job_bundle_submitter.call_args
    submitter_info = kwargs["submitter_info"]
    assert submitter_info is not None
    assert submitter_info.submitter_name == "Blender"
    assert submitter_info.host_application_name == "Blender"
    assert submitter_info.host_application_version == "4.0"
    assert submitter_info.additional_info == {"render_engine": "Cycles", "samples": 128}


@patch.object(deadline.client.ui, "gui_context_for_cli")
def test_bundle_gui_submit_submitter_info_file_not_found(_mock_context, mock_job_bundle_submitter):
    """
    Test that --submitter-info with a non-existent file shows an error.
    """

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "bundle",
            "gui-submit",
            "--browse",
            "--submitter-info",
            "file:///nonexistent/file.json",
        ],
    )

    assert result.exit_code != 0
    assert "does not exist" in result.output


@patch.object(deadline.client.ui, "gui_context_for_cli")
def test_bundle_gui_submit_submitter_info_txt_file_as_yaml(
    _mock_context, tmp_path, mock_job_bundle_submitter
):
    """
    Test that --submitter-info with a .txt file is treated as YAML.
    """

    # Create a temporary file with .txt extension containing valid YAML
    txt_file = tmp_path / "submitter.txt"
    txt_file.write_text("submitter_name: Test\nhost_application_name: Maya")

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "bundle",
            "gui-submit",
            "--browse",
            "--submitter-info",
            f"file://{txt_file}",
        ],
    )

    assert result.exit_code == 0, result.output
    _args, kwargs = mock_job_bundle_submitter.show_job_bundle_submitter.call_args
    submitter_info = kwargs["submitter_info"]
    assert submitter_info is not None
    assert submitter_info.submitter_name == "Test"
    assert submitter_info.host_application_name == "Maya"


@patch.object(deadline.client.ui, "gui_context_for_cli")
def test_bundle_gui_submit_submitter_info_yml_extension(
    _mock_context, tmp_path, mock_job_bundle_submitter
):
    """
    Test that --submitter-info works with .yml extension.
    """

    # Create a temporary YAML file with .yml extension
    yml_file = tmp_path / "submitter.yml"
    yml_data = """
submitter_name: TestApp
host_application_name: Houdini
host_application_version: "19.5"
"""
    yml_file.write_text(yml_data)

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "bundle",
            "gui-submit",
            "--browse",
            "--submitter-info",
            f"file://{yml_file}",
        ],
    )

    assert result.exit_code == 0, result.output
    _args, kwargs = mock_job_bundle_submitter.show_job_bundle_submitter.call_args
    submitter_info = kwargs["submitter_info"]
    assert submitter_info is not None
    assert submitter_info.submitter_name == "TestApp"
    assert submitter_info.host_application_name == "Houdini"
    assert submitter_info.host_application_version == "19.5"


@patch.object(deadline.client.ui, "gui_context_for_cli")
def test_bundle_gui_submit_submitter_info_all_formats_combined(
    _mock_context, tmp_path, mock_job_bundle_submitter
):
    """
    Test combining all three formats: file + JSON + key=value with precedence.
    """

    # Create a temporary JSON file
    json_file = tmp_path / "submitter.json"
    json_data = {
        "submitter_name": "FromFile",
        "host_application_name": "FromFile",
        "host_application_version": "1.0",
        "additional_info": {"source": "file"},
    }
    json_file.write_text(json.dumps(json_data))

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "bundle",
            "gui-submit",
            "--browse",
            "--submitter-info",
            f"file://{json_file}",
            "--submitter-info",
            '{"host_application_name": "FromJSON", "submitter_package_name": "FromJSON"}',
            "--submitter-info",
            "host_application_version=FromKeyValue",
            "--submitter-info",
            "submitter_package_version=1.2.3",
        ],
    )

    assert result.exit_code == 0, result.output
    _args, kwargs = mock_job_bundle_submitter.show_job_bundle_submitter.call_args
    submitter_info = kwargs["submitter_info"]
    assert submitter_info is not None
    # Later values should override earlier ones
    assert submitter_info.submitter_name == "FromFile"  # Only in file
    assert submitter_info.host_application_name == "FromJSON"  # JSON overrides file
    assert submitter_info.host_application_version == "FromKeyValue"  # Key=value overrides JSON
    assert submitter_info.submitter_package_name == "FromJSON"  # Only in JSON
    assert submitter_info.submitter_package_version == "1.2.3"  # Only in key=value
    assert submitter_info.additional_info == {"source": "file"}  # Only in file


@patch.object(deadline.client.ui, "gui_context_for_cli")
def test_bundle_gui_submit_submitter_info_invalid_json_file(
    _mock_context, tmp_path, mock_job_bundle_submitter
):
    """
    Test that --submitter-info with an invalid JSON file shows an error.
    """

    # Create a temporary file with invalid JSON
    json_file = tmp_path / "invalid.json"
    json_file.write_text('{"submitter_name": "Test", invalid}')

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "bundle",
            "gui-submit",
            "--browse",
            "--submitter-info",
            f"file://{json_file}",
        ],
    )

    assert result.exit_code != 0
    assert "is formatted incorrectly" in result.output


@patch.object(deadline.client.ui, "gui_context_for_cli")
def test_bundle_gui_submit_submitter_info_invalid_yaml_file(
    _mock_context, tmp_path, mock_job_bundle_submitter
):
    """
    Test that --submitter-info with an invalid YAML file shows an error.
    """

    # Create a temporary file with invalid YAML
    yaml_file = tmp_path / "invalid.yaml"
    yaml_file.write_text('submitter_name: "Test"\n  invalid: yaml: structure')

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "bundle",
            "gui-submit",
            "--browse",
            "--submitter-info",
            f"file://{yaml_file}",
        ],
    )

    assert result.exit_code != 0
    assert "is formatted incorrectly" in result.output


@patch.object(deadline.client.ui, "gui_context_for_cli")
def test_bundle_gui_submit_submitter_info_file_unknown_field(
    _mock_context, tmp_path, mock_job_bundle_submitter
):
    """
    Test that --submitter-info with unknown fields in a file shows an error.
    """

    # Create a temporary JSON file with unknown field
    json_file = tmp_path / "submitter.json"
    json_data = {"submitter_name": "Test", "unknown_field": "value"}
    json_file.write_text(json.dumps(json_data))

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "bundle",
            "gui-submit",
            "--browse",
            "--submitter-info",
            f"file://{json_file}",
        ],
    )

    assert result.exit_code != 0
    assert "Unknown field" in result.output


@patch.object(deadline.client.ui, "gui_context_for_cli")
def test_bundle_gui_submit_submitter_info_file_missing_submitter_name(
    _mock_context, tmp_path, mock_job_bundle_submitter
):
    """
    Test that --submitter-info with a file missing submitter_name shows an error.
    """

    # Create a temporary JSON file without submitter_name
    json_file = tmp_path / "submitter.json"
    json_data = {"host_application_name": "Maya"}
    json_file.write_text(json.dumps(json_data))

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "bundle",
            "gui-submit",
            "--browse",
            "--submitter-info",
            f"file://{json_file}",
        ],
    )

    assert result.exit_code != 0
    assert "submitter_name is required" in result.output
