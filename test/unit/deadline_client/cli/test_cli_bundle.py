# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for the CLI job bundle commands.
"""
import os
import tempfile
import json
from unittest.mock import ANY, patch, Mock

import boto3  # type: ignore[import]
from click.testing import CliRunner
import pytest

from deadline.client import config
from deadline.client.cli import main
from deadline.client.cli._groups import bundle_group
from deadline.client.config.config_file import set_setting
from deadline.job_attachments.models import AssetLoadingMethod
from deadline.job_attachments.progress_tracker import SummaryStatistics

from ..api.test_job_bundle_submission import (
    MOCK_CREATE_JOB_RESPONSE,
    MOCK_GET_JOB_RESPONSE,
    MOCK_FARM_ID,
    MOCK_JOB_TEMPLATE_CASES,
    MOCK_PARAMETERS_CASES,
    MOCK_QUEUE_ID,
)


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

    with patch.object(bundle_group, "get_boto3_client") as get_boto3_client_mock, patch.object(
        bundle_group, "_hash_attachments", return_value=[]
    ), patch.object(bundle_group, "get_queue_boto3_session"), patch.object(
        bundle_group, "_upload_attachments"
    ), patch.object(
        bundle_group.api, "get_telemetry_client"
    ):
        get_boto3_client_mock().create_job.return_value = MOCK_CREATE_JOB_RESPONSE
        get_boto3_client_mock().get_job.return_value = MOCK_GET_JOB_RESPONSE

        runner = CliRunner()
        result = runner.invoke(main, ["bundle", "submit", temp_job_bundle_dir])

        get_boto3_client_mock().create_job.assert_called_with(
            farmId=MOCK_FARM_ID,
            queueId=MOCK_QUEUE_ID,
            parameters=MOCK_PARAMETERS_CASES["TEMPLATE_ONLY_JSON"][2]["parameters"],  # type: ignore
            template=MOCK_JOB_TEMPLATE_CASES["MINIMAL_JSON"][1],
            templateType="JSON",
        )
        assert temp_job_bundle_dir in result.output
        assert MOCK_CREATE_JOB_RESPONSE["jobId"] in result.output
        assert MOCK_GET_JOB_RESPONSE["lifecycleStatusMessage"] in result.output
        assert result.exit_code == 0


def test_cli_bundle_explicit_parameters(fresh_deadline_config):
    """
    Confirm that --profile, --farm-id, and --queue-id get passed in from the CLI.
    """
    # Use a temporary directory for the job bundle
    with patch(
        "deadline.client.api._session.DeadlineClient._get_deadline_api_input_shape"
    ) as input_shape_mock:
        input_shape_mock.return_value = {}
        with tempfile.TemporaryDirectory() as tmpdir, patch.object(
            boto3, "Session"
        ) as session_mock:
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

        session_mock.assert_called_once_with(profile_name="NonDefaultProfileName")
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


@pytest.mark.parametrize("loading_method", [e.value for e in AssetLoadingMethod] + [None])
def test_cli_bundle_asset_load_method(fresh_deadline_config, temp_job_bundle_dir, loading_method):
    """
    Verify that asset loading method set on CLI are passed to the CreateJob call
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

    with patch.object(bundle_group, "get_boto3_client") as get_boto3_client_mock, patch.object(
        bundle_group, "_hash_attachments", return_value=(attachment_mock, {})
    ), patch.object(bundle_group, "_upload_attachments", return_value={}), patch.object(
        bundle_group.api, "get_boto3_session"
    ), patch.object(
        bundle_group, "get_queue_boto3_session"
    ), patch.object(
        bundle_group.api, "get_telemetry_client"
    ):
        get_boto3_client_mock().create_job.return_value = MOCK_CREATE_JOB_RESPONSE
        get_boto3_client_mock().get_job.return_value = MOCK_GET_JOB_RESPONSE
        get_boto3_client_mock().get_queue.return_value = {
            "displayName": "Test Queue",
            "jobAttachmentSettings": {"s3BucketName": "mock", "rootPrefix": "root"},
        }

        params = ["bundle", "submit", temp_job_bundle_dir]

        # None case represents not setting the parameter
        if loading_method is not None:
            params += ["--asset-loading-method", loading_method]

        runner = CliRunner()
        result = runner.invoke(main, params)

        expected_loading_method = (
            loading_method
            if loading_method is not None
            else config.get_setting("defaults.job_attachments_file_system")
        )

        get_boto3_client_mock().create_job.assert_called_with(
            farmId=MOCK_FARM_ID,
            queueId=MOCK_QUEUE_ID,
            parameters=MOCK_PARAMETERS_CASES["TEMPLATE_ONLY_JSON"][2]["parameters"],  # type: ignore
            template=MOCK_JOB_TEMPLATE_CASES["MINIMAL_JSON"][1],
            templateType="JSON",
            attachments={"assetLoadingMethod": expected_loading_method},
        )
        assert temp_job_bundle_dir in result.output
        assert MOCK_CREATE_JOB_RESPONSE["jobId"] in result.output
        assert MOCK_GET_JOB_RESPONSE["lifecycleStatusMessage"] in result.output
        assert result.exit_code == 0


def test_cli_bundle_job_parameter_from_cli(fresh_deadline_config):
    """
    Verify that job parameters specified at the CLI are passed to the CreateJob call
    """
    # Use a temporary directory for the job bundle
    with patch(
        "deadline.client.api._session.DeadlineClient._get_deadline_api_input_shape"
    ) as input_shape_mock:
        input_shape_mock.return_value = {}
        with tempfile.TemporaryDirectory() as tmpdir, patch.object(
            boto3, "Session"
        ) as session_mock:
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
                    "--parameter",
                    "sceneFile=/path/to/scenefile",
                    "--parameter",
                    "priority=90",
                ],
            )

            session_mock().client().create_job.assert_called_once_with(
                farmId=MOCK_FARM_ID,
                queueId=MOCK_QUEUE_ID,
                template=ANY,
                templateType="JSON",
                parameters={
                    "sceneFile": {"string": "/path/to/scenefile"},
                    "priority": {"int": "90"},
                },
                priority=50,
            )

            assert result.exit_code == 0


def test_cli_bundle_invalid_job_paramter(fresh_deadline_config):
    """
    Verify that a badly formatted parameter value (without "Key=Value") throws an error
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
                "--parameter",
                "BadParam",
            ],
        )

        assert 'Parameters must be provided in the format "Key=Value"' in result.output
        assert result.exit_code == 2


def test_cli_bundle_accept_upload_confirmation(fresh_deadline_config, temp_job_bundle_dir):
    """
    Verify that when the user accepts the job attachments upload confirmation
    that CreateJob is called properly still.
    """

    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)

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

    with patch.object(bundle_group, "get_boto3_client") as get_boto3_client_mock, patch.object(
        bundle_group, "_hash_attachments", return_value=[SummaryStatistics(), "test"]
    ), patch.object(bundle_group, "_upload_attachments"), patch.object(
        bundle_group.api, "get_boto3_session"
    ), patch.object(
        bundle_group, "get_queue_boto3_session"
    ), patch.object(
        bundle_group.api, "get_telemetry_client"
    ):
        get_boto3_client_mock().create_job.return_value = MOCK_CREATE_JOB_RESPONSE
        get_boto3_client_mock().get_job.return_value = MOCK_GET_JOB_RESPONSE
        get_boto3_client_mock().get_queue.return_value = {
            "displayName": "Test Queue",
            "jobAttachmentSettings": {"s3BucketName": "mock", "rootPrefix": "root"},
        }

        set_setting("settings.auto_accept", "false")
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

        get_boto3_client_mock().create_job.assert_called_with(
            farmId=MOCK_FARM_ID,
            queueId=MOCK_QUEUE_ID,
            template=MOCK_JOB_TEMPLATE_CASES["MINIMAL_JSON"][1],
            templateType="JSON",
            attachments=ANY,
        )
        assert result.exit_code == 0


def test_cli_bundle_reject_upload_confirmation(fresh_deadline_config, temp_job_bundle_dir):
    """
    Verify that when the user rejects the job attachments upload confirmation
    that no further action is taken after that point.
    """

    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)

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

    with patch.object(bundle_group, "get_boto3_client") as get_boto3_client_mock, patch.object(
        bundle_group, "_hash_attachments", return_value=[SummaryStatistics(), "test"]
    ), patch.object(bundle_group, "_upload_attachments") as upload_attachments_mock, patch.object(
        bundle_group.api, "get_boto3_session"
    ), patch.object(
        bundle_group, "get_queue_boto3_session"
    ), patch.object(
        bundle_group.api, "get_telemetry_client"
    ):
        get_boto3_client_mock().get_queue.return_value = {
            "displayName": "Test Queue",
            "jobAttachmentSettings": {"s3BucketName": "mock", "rootPrefix": "root"},
        }
        set_setting("settings.auto_accept", "false")
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

        upload_attachments_mock.assert_not_called()
        assert result.exit_code == 0
