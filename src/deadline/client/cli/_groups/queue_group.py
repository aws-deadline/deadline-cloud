# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
All the `deadline queue` commands.
"""

import click
from botocore.exceptions import ClientError  # type: ignore[import]

from ... import api
from ...config import config_file
from ...exceptions import DeadlineOperationError
from .._common import _apply_cli_options_to_config, _cli_object_repr, _handle_error


@click.group(name="queue")
@_handle_error
def cli_queue():
    """
    Commands for AWS Deadline Cloud Queues.
    """


@cli_queue.command(name="list")
@click.option("--profile", help="The AWS profile to use.")
@click.option("--farm-id", help="The AWS Deadline Cloud Farm to use.")
@_handle_error
def queue_list(**args):
    """
    Lists the available Queues in AWS Deadline Cloud.
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


@cli_queue.command(name="paramdefs")
@click.option("--profile", help="The AWS profile to use.")
@click.option("--farm-id", help="The AWS Deadline Cloud Farm to use.")
@click.option("--queue-id", help="The AWS Deadline Cloud Queue to use.")
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
@click.option("--farm-id", help="The AWS Deadline Cloud Farm to use.")
@click.option("--queue-id", help="The AWS Deadline Cloud Queue to use.")
@_handle_error
def queue_get(**args):
    """
    Get the details of an AWS Deadline Cloud Queue.

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
