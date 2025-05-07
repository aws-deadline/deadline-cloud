# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for the CLI job bundle commands.
"""

import os
import sys
import tempfile
import json
from unittest.mock import ANY, patch, Mock, call

import boto3  # type: ignore[import]
from click.testing import CliRunner
import pytest

from deadline.client import config
import deadline.client.api as api_module
from deadline.client.cli import main
from deadline.job_attachments.models import JobAttachmentsFileSystem

from ..api.test_job_bundle_submission import (
    MOCK_FARM_ID,
    MOCK_JOB_TEMPLATE_CASES,
    MOCK_PARAMETERS_CASES,
    MOCK_QUEUE_ID,
    get_minimal_json_job_template,
)

from ..testing_utilities import (
    patch_calls_for_create_job_from_job_bundle,
    MOCK_CREATE_JOB_RESPONSE,
    MOCK_GET_JOB_RESPONSE,
)

os.environ["AWS_ENDPOINT_URL_DEADLINE"] = "https://fake-endpoint"


def test_cli_bundle_submit(fresh_deadline_config, temp_job_bundle_dir):
    """
    Confirm that the CLI interface calls the proper functions with the right
    arguments on the way to calling CreateJob.
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)

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

    with patch_calls_for_create_job_from_job_bundle() as mock:
        runner = CliRunner()
        result = runner.invoke(main, ["bundle", "submit", temp_job_bundle_dir])

        mock.get_boto3_client().create_job.assert_called_with(
            farmId=MOCK_FARM_ID,
            queueId=MOCK_QUEUE_ID,
            parameters=MOCK_PARAMETERS_CASES["TEMPLATE_ONLY_JSON"][2]["parameters"],  # type: ignore
            template=MOCK_JOB_TEMPLATE_CASES["MINIMAL_JSON"][1],
            templateType="JSON",
            priority=50,
        )
        assert temp_job_bundle_dir in result.output, result.output
        assert MOCK_CREATE_JOB_RESPONSE["jobId"] in result.output, result.output
        assert MOCK_GET_JOB_RESPONSE["lifecycleStatusMessage"] in result.output, result.output
        assert result.exit_code == 0, result.output


def test_cli_bundle_explicit_parameters(fresh_deadline_config):
    """
    Confirm that --profile, --farm-id, and --queue-id get passed in from the CLI.
    """
    # Use a temporary directory for the job bundle
    with tempfile.TemporaryDirectory() as tmpdir, patch.object(boto3, "Session") as session_mock:
        session_mock().client("deadline").create_job.return_value = MOCK_CREATE_JOB_RESPONSE
        session_mock().client("deadline").get_job.return_value = MOCK_GET_JOB_RESPONSE
        session_mock.reset_mock()

        # Write a JSON template
        with open(os.path.join(tmpdir, "template.json"), "w", encoding="utf8") as f:
            f.write(MOCK_JOB_TEMPLATE_CASES["MINIMAL_JSON"][1])

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "bundle",
                "submit",
                tmpdir,
                "--profile",
                "NonDefaultProfileName",
                "--farm-id",
                MOCK_FARM_ID,
                "--queue-id",
                MOCK_QUEUE_ID,
            ],
        )

    session_mock.assert_called_with(profile_name="NonDefaultProfileName")
    session_mock().client().create_job.assert_called_once_with(
        farmId=MOCK_FARM_ID,
        queueId=MOCK_QUEUE_ID,
        template=ANY,
        templateType="JSON",
        priority=50,
    )

    assert tmpdir in result.output
    assert MOCK_CREATE_JOB_RESPONSE["jobId"] in result.output
    assert MOCK_GET_JOB_RESPONSE["lifecycleStatusMessage"] in result.output
    assert result.exit_code == 0


def test_cli_bundle_priority_retries(fresh_deadline_config):
    """
    Confirm that --priority, --max-failed-tasks-count, --max_worker_count and --max-retries-per-task get passed in from the CLI.
    """
    # Use a temporary directory for the job bundle
    with tempfile.TemporaryDirectory() as tmpdir, patch.object(boto3, "Session") as session_mock:
        session_mock().client("deadline").create_job.return_value = MOCK_CREATE_JOB_RESPONSE
        session_mock().client("deadline").get_job.return_value = MOCK_GET_JOB_RESPONSE
        session_mock.reset_mock()

        # Write a JSON template
        with open(os.path.join(tmpdir, "template.json"), "w", encoding="utf8") as f:
            f.write(MOCK_JOB_TEMPLATE_CASES["MINIMAL_JSON"][1])

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "bundle",
                "submit",
                tmpdir,
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

    assert tmpdir in result.output
    assert MOCK_CREATE_JOB_RESPONSE["jobId"] in result.output
    assert MOCK_GET_JOB_RESPONSE["lifecycleStatusMessage"] in result.output
    session_mock().client().create_job.assert_called_once_with(
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


def test_cli_bundle_job_name(fresh_deadline_config):
    """
    Confirm that --name sets the job name in the template.
    """
    # Use a temporary directory for the job bundle
    with tempfile.TemporaryDirectory() as tmpdir, patch.object(boto3, "Session") as session_mock:
        session_mock().client("deadline").create_job.return_value = MOCK_CREATE_JOB_RESPONSE
        session_mock().client("deadline").get_job.return_value = MOCK_GET_JOB_RESPONSE
        session_mock.reset_mock()

        # Write a JSON template
        with open(os.path.join(tmpdir, "template.json"), "w", encoding="utf8") as f:
            f.write(MOCK_JOB_TEMPLATE_CASES["MINIMAL_JSON"][1])

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "bundle",
                "submit",
                tmpdir,
                "--farm-id",
                MOCK_FARM_ID,
                "--queue-id",
                MOCK_QUEUE_ID,
                "--name",
                "Replacement Name For The Job",
            ],
        )

    assert tmpdir in result.output
    assert MOCK_CREATE_JOB_RESPONSE["jobId"] in result.output
    assert MOCK_GET_JOB_RESPONSE["lifecycleStatusMessage"] in result.output
    session_mock().client().create_job.assert_called_once_with(
        farmId=MOCK_FARM_ID,
        queueId=MOCK_QUEUE_ID,
        template=get_minimal_json_job_template("Replacement Name For The Job"),
        templateType="JSON",
        priority=50,
    )
    assert result.exit_code == 0


def test_cli_bundle_storage_profile_id(fresh_deadline_config):
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

    # Use a temporary directory for the job bundle
    with tempfile.TemporaryDirectory() as tmpdir, patch.object(boto3, "Session") as session_mock:
        session_mock().client("deadline").create_job.return_value = MOCK_CREATE_JOB_RESPONSE
        session_mock().client("deadline").get_job.return_value = MOCK_GET_JOB_RESPONSE
        session_mock.reset_mock()

        # Write a JSON template
        with open(os.path.join(tmpdir, "template.json"), "w", encoding="utf8") as f:
            f.write(MOCK_JOB_TEMPLATE_CASES["MINIMAL_JSON"][1])

        runner = CliRunner()
        with patch.object(api_module, "get_storage_profile_for_queue"):
            result = runner.invoke(
                main,
                ["bundle", "submit", tmpdir, "--storage-profile-id", CLI_STORAGE_PROFILE_ID],
            )

    assert tmpdir in result.output
    assert MOCK_CREATE_JOB_RESPONSE["jobId"] in result.output
    assert MOCK_GET_JOB_RESPONSE["lifecycleStatusMessage"] in result.output
    session_mock().client().create_job.assert_called_once_with(
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
def test_cli_bundle_asset_load_method(fresh_deadline_config, temp_job_bundle_dir, loading_method):
    """
    Verify that asset loading method set on CLI are passed to the CreateJob call
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

    attachment_mock = Mock()
    attachment_mock.total_bytes = 0
    attachment_mock.total_files.return_value = 0

    with patch_calls_for_create_job_from_job_bundle() as mock:
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
        assert mock.get_boto3_client().create_job.mock_calls == [
            call(
                farmId=MOCK_FARM_ID,
                queueId=MOCK_QUEUE_ID,
                parameters=MOCK_PARAMETERS_CASES["TEMPLATE_ONLY_JSON"][2]["parameters"],  # type: ignore
                template=MOCK_JOB_TEMPLATE_CASES["MINIMAL_JSON"][1],
                templateType="JSON",
                attachments={
                    "fileSystem": expected_loading_method,
                    "manifests": [
                        {
                            "rootPath": "/mnt/root/path1",
                            "rootPathFormat": "posix",
                            "inputManifestPath": "mock-manifest",
                            "inputManifestHash": "mock-manifest-hash",
                            "outputRelativeDirectories": ["."],
                        }
                    ],
                },
                priority=50,
            )
        ], result.output
        assert MOCK_CREATE_JOB_RESPONSE["jobId"] in result.output, result.output
        assert MOCK_GET_JOB_RESPONSE["lifecycleStatusMessage"] in result.output, result.output
        assert result.exit_code == 0, result.output


def test_cli_bundle_job_parameter_from_cli(fresh_deadline_config):
    """
    Verify that job parameters specified at the CLI are passed to the CreateJob call
    """
    # Use a temporary directory for the job bundle
    with tempfile.TemporaryDirectory() as tmpdir, patch_calls_for_create_job_from_job_bundle() as mock:
        # Write a JSON template
        with open(os.path.join(tmpdir, "template.json"), "w", encoding="utf8") as f:
            f.write(MOCK_JOB_TEMPLATE_CASES["MINIMAL_JSON"][1])

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "bundle",
                "submit",
                tmpdir,
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

        mock.get_boto3_client().create_job.assert_called_once_with(
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

        mock.get_deadline_cloud_library_telemetry_client.return_value.record_event.assert_any_call(
            event_type="com.amazon.rum.deadline.submission",
            event_details={"submitter_name": "MyDCC"},
            from_gui=False,
        )

        assert result.exit_code == 0


def test_cli_bundle_empty_job_parameter_from_cli(fresh_deadline_config):
    """
    Verify that an empty job parameter specified at the CLI are passed to the CreateJob call
    """
    # Use a temporary directory for the job bundle
    with tempfile.TemporaryDirectory() as tmpdir, patch_calls_for_create_job_from_job_bundle() as mock:
        # Write a JSON template
        with open(os.path.join(tmpdir, "template.json"), "w", encoding="utf8") as f:
            f.write(MOCK_JOB_TEMPLATE_CASES["MINIMAL_JSON"][1])

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "bundle",
                "submit",
                tmpdir,
                "--farm-id",
                MOCK_FARM_ID,
                "--queue-id",
                MOCK_QUEUE_ID,
                "--parameter",
                "sceneFile=",
            ],
        )

        assert mock.get_boto3_client().create_job.mock_calls == [
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


def test_cli_bundle_job_parameter_with_equals_from_cli(fresh_deadline_config):
    """
    Verify that a job parameter value with an '=' in it is passed correctly to the CreateJob call
    """
    # Use a temporary directory for the job bundle
    with tempfile.TemporaryDirectory() as tmpdir, patch_calls_for_create_job_from_job_bundle() as mock:
        # Write a JSON template
        with open(os.path.join(tmpdir, "template.json"), "w", encoding="utf8") as f:
            f.write(MOCK_JOB_TEMPLATE_CASES["MINIMAL_JSON"][1])

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "bundle",
                "submit",
                tmpdir,
                "--farm-id",
                MOCK_FARM_ID,
                "--queue-id",
                MOCK_QUEUE_ID,
                "--parameter",
                "sceneFile=this=is=a=test",
            ],
        )

        mock.get_boto3_client().create_job.assert_called_once_with(
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


def test_cli_bundle_invalid_job_parameter(fresh_deadline_config):
    """
    Verify that a badly formatted parameter value (without "Key=Value") throws an error
    """
    # Use a temporary directory for the job bundle
    with tempfile.TemporaryDirectory() as tmpdir, patch_calls_for_create_job_from_job_bundle():
        # Write a JSON template
        with open(os.path.join(tmpdir, "template.json"), "w", encoding="utf8") as f:
            f.write(MOCK_JOB_TEMPLATE_CASES["MINIMAL_JSON"][1])

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "bundle",
                "submit",
                tmpdir,
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


def test_cli_bundle_invalid_job_parameter_name(fresh_deadline_config):
    """
    Verify that a non-identifier parameter name raises an error.
    """
    # Use a temporary directory for the job bundle
    with tempfile.TemporaryDirectory() as tmpdir, patch_calls_for_create_job_from_job_bundle():
        # Write a JSON template
        with open(os.path.join(tmpdir, "template.json"), "w", encoding="utf8") as f:
            f.write(MOCK_JOB_TEMPLATE_CASES["MINIMAL_JSON"][1])

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "bundle",
                "submit",
                tmpdir,
                "--farm-id",
                MOCK_FARM_ID,
                "--queue-id",
                MOCK_QUEUE_ID,
                "--parameter",
                "Param*Name=Value",
            ],
        )

        assert (
            "Parameter names must be alphanumeric Open Job Description identifiers."
            in result.output
        )
        assert result.exit_code == 2


def test_cli_bundle_accept_upload_confirmation(fresh_deadline_config, temp_job_bundle_dir):
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

    with patch_calls_for_create_job_from_job_bundle() as mock:
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

        mock.get_boto3_client().create_job.assert_called_with(
            farmId=MOCK_FARM_ID,
            queueId=MOCK_QUEUE_ID,
            template=MOCK_JOB_TEMPLATE_CASES["MINIMAL_JSON"][1],
            templateType="JSON",
            attachments=ANY,
            priority=50,
        )
        assert result.exit_code == 0, result.output


def test_cli_bundle_reject_upload_confirmation(fresh_deadline_config, temp_job_bundle_dir):
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

    with patch_calls_for_create_job_from_job_bundle() as mock:
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

        mock.upload_assets.assert_not_called()
        assert result.exit_code == 1


@patch("deadline.client.ui.gui_context_for_cli")
def test_gui_submit_submitter_name(_mock_context):
    """
    Verify that the --submitter-name arg gets passed through correctly
    """

    # Unconventional mocking pattern because of how the function is imported in code
    mock_job_bundle_submitter = Mock()
    sys.modules["deadline.client.ui.job_bundle_submitter"] = mock_job_bundle_submitter
    mock_job_bundle_submitter.show_job_bundle_submitter

    runner = CliRunner()
    runner.invoke(
        main,
        ["bundle", "gui-submit", "--browse", "--submitter-name", "MyDCC"],
    )
    _args, kwargs = mock_job_bundle_submitter.show_job_bundle_submitter.call_args
    assert kwargs["submitter_name"] == "MyDCC"
