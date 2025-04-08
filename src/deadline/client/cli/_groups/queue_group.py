# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
All the `deadline queue` commands.
"""

import click
import json
import time
from botocore.exceptions import ClientError  # type: ignore[import]

from ... import api
from ...config import config_file
from ...exceptions import DeadlineOperationError
from .._common import _apply_cli_options_to_config, _cli_object_repr, _handle_error
from deadline.job_attachments.models import (
    FileConflictResolution,
)
from .click_logger import ClickLogger
from deadline.client.api import _queue_apis
from configparser import ConfigParser
from typing import Optional
import boto3
import os


@click.group(name="queue")
@_handle_error
def cli_queue():
    """
    Commands for queues.
    """


@cli_queue.command(name="list")
@click.option("--profile", help="The AWS profile to use.")
@click.option("--farm-id", help="The farm to use.")
@_handle_error
def queue_list(**args):
    """
    Lists the available queues.
    """
    # Get a temporary config object with the standard options handled
    config = _apply_cli_options_to_config(required_options={"farm_id"}, **args)

    farm_id = config_file.get_setting("defaults.farm_id", config=config)

    try:
        response = api.list_queues(farmId=farm_id, config=config)
    except ClientError as exc:
        raise DeadlineOperationError(f"Failed to get Queues from Deadline:\n{exc}") from exc

    # Select which fields to print and in which order
    structured_queue_list = [
        {field: queue[field] for field in ["queueId", "displayName"]}
        for queue in response["queues"]
    ]

    click.echo(_cli_object_repr(structured_queue_list))


@cli_queue.command(name="export-credentials")
@click.option("--queue-id", help="The queue ID to use.")
@click.option("--farm-id", help="The farm ID to use.")
@click.option(
    "--mode",
    type=click.Choice(["USER", "READ"], case_sensitive=False),
    default="USER",
    help="The type of queue role to assume (default: USER)",
)
@click.option(
    "--output-format",
    type=click.Choice(["credentials_process"], case_sensitive=False),
    default="credentials_process",
    help="Format of the output (default: credentials_process)",
)
@click.option("--profile", help="The AWS profile to use.")
@_handle_error
def queue_export_credentials(mode, output_format, **args):
    """
    Export queue credentials in a format compatible with AWS SDK credentials_process.
    """
    start_time = time.time()
    is_success = True
    error_type = None

    # Get a temporary config object with the standard options handled
    config = _apply_cli_options_to_config(required_options={"farm_id", "queue_id"}, **args)

    queue_id: str = config_file.get_setting("defaults.queue_id", config=config)
    farm_id: str = config_file.get_setting("defaults.farm_id", config=config)

    try:
        # Call the appropriate API based on mode
        if mode.upper() == "USER":
            response = api.assume_queue_role_for_user(
                farmId=farm_id, queueId=queue_id, config=config
            )
        elif mode.upper() == "READ":
            response = api.assume_queue_role_for_read(
                farmId=farm_id, queueId=queue_id, config=config
            )
        else:
            is_success = False
            error_type = "InvalidMode"
            raise DeadlineOperationError(f"Invalid mode: {mode}")

        # Format the response according to the AWS SDK credentials_process format
        if output_format.lower() == "credentials_process":
            credentials = response["credentials"]
            formatted_credentials = {
                "Version": 1,
                "AccessKeyId": credentials["accessKeyId"],
                "SecretAccessKey": credentials["secretAccessKey"],
                "SessionToken": credentials["sessionToken"],
                "Expiration": credentials["expiration"].isoformat(),
            }
            click.echo(json.dumps(formatted_credentials, indent=2))
        else:
            is_success = False
            error_type = "InvalidOutputFormat"
            raise DeadlineOperationError(f"Invalid output format: {output_format}")

    except ClientError as exc:
        is_success = False
        error_type = "ClientError"
        if "AccessDenied" in str(exc):
            raise DeadlineOperationError(
                f"Insufficient permissions to assume the requested queue role: {exc}"
            ) from exc
        elif "UnrecognizedClientException" in str(exc):
            raise DeadlineOperationError(
                f"Authentication failed. Please run 'deadline auth login' or check your AWS credentials: {exc}"
            ) from exc
        else:
            raise DeadlineOperationError(
                f"Failed to get credentials from AWS Deadline Cloud:\n{exc}"
            ) from exc
    finally:
        # Record telemetry
        duration_ms = int((time.time() - start_time) * 1000)
        api._telemetry.get_deadline_cloud_library_telemetry_client().record_event(
            "com.amazon.rum.deadline.queue_export_credentials",
            {
                "mode": mode,
                "queue_id": queue_id,
                "output_format": output_format,
                "is_success": is_success,
                "error_type": error_type if not is_success else None,
                "duration_ms": duration_ms,
            },
        )


@cli_queue.command(name="paramdefs")
@click.option("--profile", help="The AWS profile to use.")
@click.option("--farm-id", help="The farm to use.")
@click.option("--queue-id", help="The queue to use.")
@_handle_error
def queue_paramdefs(**args):
    """
    Lists a Queue's Parameters Definitions.
    """
    # Get a temporary config object with the standard options handled
    config = _apply_cli_options_to_config(required_options={"farm_id", "queue_id"}, **args)

    farm_id = config_file.get_setting("defaults.farm_id", config=config)
    queue_id = config_file.get_setting("defaults.queue_id", config=config)

    try:
        response = api.get_queue_parameter_definitions(farmId=farm_id, queueId=queue_id)
    except ClientError as exc:
        raise DeadlineOperationError(
            f"Failed to get Queue Parameter Definitions from Deadline:\n{exc}"
        ) from exc

    click.echo(_cli_object_repr(response))


@cli_queue.command(name="get")
@click.option("--profile", help="The AWS profile to use.")
@click.option("--farm-id", help="The farm to use.")
@click.option("--queue-id", help="The queue to use.")
@_handle_error
def queue_get(**args):
    """
    Get the details of a queue.

    If Queue ID is not provided, returns the configured default Queue.
    """
    # Get a temporary config object with the standard options handled
    config = _apply_cli_options_to_config(required_options={"farm_id", "queue_id"}, **args)

    farm_id = config_file.get_setting("defaults.farm_id", config=config)
    queue_id = config_file.get_setting("defaults.queue_id", config=config)

    deadline = api.get_boto3_client("deadline", config=config)
    response = deadline.get_queue(farmId=farm_id, queueId=queue_id)
    response.pop("ResponseMetadata", None)

    click.echo(_cli_object_repr(response))


if os.environ.get("ENABLE_INCREMENTAL_OUTPUT_DOWNLOAD") is not None:

    @cli_queue.command(
        name="incremental-output-download",
        help="BETA - Download Job Output data incrementally for all jobs running on a queue as session actions finish.\n"
        "The command bootstraps once using a bootstrap lookback specified in minutes and\n"
        "continues downloading from the last saved progress thereafter until bootstrap is forced.\n"
        "[NOTE] This command is still WIP and partially implemented right now",
    )
    @click.option("--farm-id", help="The AWS Deadline Cloud Farm to use.")
    @click.option("--queue-id", help="The AWS Deadline Cloud Queue to use.")
    @click.option("--path-mapping-rules", help="Path to a file with the path mapping rules to use.")
    @click.option(
        "--json", default=None, is_flag=True, help="Output is printed as JSON for scripting."
    )
    @click.option(
        "--bootstrap-lookback-in-minutes",
        default=0,
        help="Downloads outputs for job-session-actions that have been completed since these many\n"
        "minutes at bootstrap. Default value is 0 minutes.",
    )
    @click.option(
        "--saved-progress-checkpoint-location",
        help="Proceed downloading from previous progress file at this location, if it exists.\n"
        "If parameter not provided or file does not exist,\n"
        "the download will start from the provided bootstrap lookback in minutes or its default value. \n",
        required=True,
    )
    @click.option(
        "--force-bootstrap",
        is_flag=True,
        help="Ignores the previous download progress and forces command to start from the bootstrap \n"
        "lookback period specified in minutes.\n"
        "Default value is False.",
        default=False,
    )
    @click.option(
        "--conflict-resolution",
        type=click.Choice(
            [
                FileConflictResolution.SKIP.name,
                FileConflictResolution.OVERWRITE.name,
                FileConflictResolution.CREATE_COPY.name,
            ],
            case_sensitive=False,
        ),
        default=FileConflictResolution.OVERWRITE.name,
        help="How to handle downloads if an output file already exists:\n"
        "CREATE_COPY (default): Download the file with a new name, appending '(1)' to the end\n"
        "SKIP: Do not download the file\n"
        "OVERWRITE: Download and replace the existing file.\n"
        "Default behaviour is to OVERWRITE.",
    )
    @_handle_error
    def incremental_output_download(
        path_mapping_rules: str,
        json: bool,
        bootstrap_lookback_in_minutes: Optional[int],
        saved_progress_checkpoint_location: str,
        force_bootstrap: bool,
        **args,
    ):
        """
        Download Job Output data incrementally for all jobs running on a queue as session actions finish.
        The command bootstraps once using a bootstrap lookback specified in minutes and
        continues downloading from the last saved progress thereafter until bootstrap is forced

        :param path_mapping_rules: path mapping rules for cross OS path mapping
        :param json: whether output is printed as JSON for scripting
        :param bootstrap_lookback_in_minutes: Downloads outputs for job-session-actions that have been completed
        since these many minutes at bootstrap. Default value is 0 minutes.
        :param saved_progress_checkpoint_location: location of the download progress file
        :param force_bootstrap: force bootstrap and ignore current download progress. Default value is False.
        :param args:
        :return:
        """
        logger: ClickLogger = ClickLogger(is_json=json)
        logger.echo("processing " + args.__str__())

        logger.echo("Started incremental download....")

        try:
            # Validate file path inputs for downloading outputs incrementally.
            _queue_apis._validate_file_inputs_for_incremental_output_download(
                saved_progress_checkpoint_location=saved_progress_checkpoint_location,
                path_mapping_rules=path_mapping_rules,
            )
        except RuntimeError as e:
            logger.echo(f"Download failed due to error: {e}")
            return

        # Get a temporary config object with the standard options handled
        config: Optional[ConfigParser] = _apply_cli_options_to_config(
            required_options={"farm_id", "queue_id"}, **args
        )

        farm_id = config_file.get_setting("defaults.farm_id", config=config)
        queue_id = config_file.get_setting("defaults.queue_id", config=config)

        boto3_session: boto3.Session = api.get_boto3_session(config=config)

        # Call the incremental output download api
        _queue_apis._incremental_output_download(
            boto3_session=boto3_session,
            farm_id=farm_id,
            queue_id=queue_id,
            saved_progress_checkpoint_location=saved_progress_checkpoint_location,
            bootstrap_lookback_in_minutes=bootstrap_lookback_in_minutes,
            force_bootstrap=force_bootstrap,
            path_mapping_rules=path_mapping_rules,
            logger=logger,
        )
