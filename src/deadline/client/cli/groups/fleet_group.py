# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
All the `deadline fleet` commands.
"""

import click
from botocore.exceptions import ClientError  # type: ignore[import]

from ... import api
from ...config import config_file
from ...exceptions import DeadlineOperationError
from .._common import apply_cli_options_to_config, cli_object_repr, handle_error


@click.group(name="fleet")
@handle_error
def cli_fleet():
    """
    Commands to work with Amazon Deadline Cloud Fleet resources.
    """


@cli_fleet.command(name="list")
@click.option("--profile", help="The AWS profile to use.")
@click.option("--farm-id", help="The Amazon Deadline Cloud Farm to use.")
@handle_error
def fleet_list(**args):
    """
    Lists the available Fleets in Amazon Deadline Cloud.
    """
    # Get a temporary config object with the standard options handled
    config = apply_cli_options_to_config(required_options={"farm_id"}, **args)

    farm_id = config_file.get_setting("defaults.farm_id", config=config)

    try:
        response = api.list_fleets(farmId=farm_id, config=config)
    except ClientError as exc:
        raise DeadlineOperationError(f"Failed to get Fleets from Deadline:\n{exc}") from exc

    # Select which fields to print and in which order
    structured_fleet_list = [
        {field: fleet[field] for field in ["fleetId", "displayName"]}
        for fleet in response["fleets"]
    ]

    click.echo(cli_object_repr(structured_fleet_list))


@cli_fleet.command(name="get")
@click.option("--profile", help="The AWS profile to use.")
@click.option("--farm-id", help="The Amazon Deadline Cloud Farm to use.")
@click.option("--fleet-id", help="The Amazon Deadline Cloud Fleet to use.", required=True)
@handle_error
def fleet_get(fleet_id, **args):
    """
    Get the details of a Amazon Deadline Cloud Fleet.
    """
    # Get a temporary config object with the standard options handled
    config = apply_cli_options_to_config(required_options={"farm_id"}, **args)

    farm_id = config_file.get_setting("defaults.farm_id", config=config)

    deadline = api.get_boto3_client("deadline", config=config)
    response = deadline.get_fleet(farmId=farm_id, fleetId=fleet_id)
    response.pop("ResponseMetadata", None)

    click.echo(cli_object_repr(response))
