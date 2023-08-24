# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
All the `deadline farm` commands.
"""

import click
from botocore.exceptions import ClientError  # type: ignore[import]

from ... import api
from ...config import config_file
from ...exceptions import DeadlineOperationError
from .._common import apply_cli_options_to_config, cli_object_repr, handle_error


@click.group(name="farm")
@handle_error
def cli_farm():
    """
    Commands to work with Amazon Deadline Cloud Farm resources.
    """


@cli_farm.command(name="list")
@click.option("--profile", help="The AWS profile to use.")
@handle_error
def farm_list(**args):
    """
    Lists the available Farms in Amazon Deadline Cloud.
    """
    # Get a temporary config object with the standard options handled
    config = apply_cli_options_to_config(**args)

    try:
        response = api.list_farms(config=config)
    except ClientError as exc:
        raise DeadlineOperationError(f"Failed to get Farms from Deadline:\n{exc}") from exc

    # Select which fields to print and in which order
    structured_farm_list = [
        {field: farm[field] for field in ["farmId", "displayName"]} for farm in response["farms"]
    ]

    click.echo(cli_object_repr(structured_farm_list))


@cli_farm.command(name="get")
@click.option("--profile", help="The AWS profile to use.")
@click.option("--farm-id", help="The Amazon Deadline Cloud Farm to use.")
@handle_error
def farm_get(**args):
    """
    Get the details of a Amazon Deadline Cloud farm.

    If farm ID is not provided, returns the configured default farm.
    """
    # Get a temporary config object with the standard options handled
    config = apply_cli_options_to_config(required_options={"farm_id"}, **args)

    farm_id = config_file.get_setting("defaults.farm_id", config=config)

    deadline = api.get_boto3_client("deadline", config=config)
    response = deadline.get_farm(farmId=farm_id)
    response.pop("ResponseMetadata", None)

    click.echo(cli_object_repr(response))
