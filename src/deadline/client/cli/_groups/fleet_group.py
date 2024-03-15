# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
All the `deadline fleet` commands.
"""

import click
from botocore.exceptions import ClientError  # type: ignore[import]

from ... import api
from ...config import config_file
from ...exceptions import DeadlineOperationError
from .._common import _apply_cli_options_to_config, _cli_object_repr, _handle_error


@click.group(name="fleet")
@_handle_error
def cli_fleet():
    """
    Commands to work with AWS Deadline Cloud Fleet resources.
    """


@cli_fleet.command(name="list")
@click.option("--profile", help="The AWS profile to use.")
@click.option("--farm-id", help="The AWS Deadline Cloud Farm to use.")
@_handle_error
def fleet_list(**args):
    """
    Lists the available Fleets in AWS Deadline Cloud.
    """
    # Get a temporary config object with the standard options handled
    config = _apply_cli_options_to_config(required_options={"farm_id"}, **args)

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

    click.echo(_cli_object_repr(structured_fleet_list))


@cli_fleet.command(name="get")
@click.option("--profile", help="The AWS profile to use.")
@click.option("--farm-id", help="The AWS Deadline Cloud Farm to use.")
@click.option("--fleet-id", help="The AWS Deadline Cloud Fleet to use.")
@click.option("--queue-id", help="If provided, gets all Fleets associated with the Queue.")
@_handle_error
def fleet_get(fleet_id, queue_id, **args):
    """
    Get the details of an AWS Deadline Cloud Fleet.
    """
    if fleet_id and queue_id:
        raise DeadlineOperationError(
            "Only one of the --fleet-id and --queue-id options may be provided."
        )

    # Get a temporary config object with the standard options handled
    config = _apply_cli_options_to_config(required_options={"farm_id"}, **args)

    farm_id = config_file.get_setting("defaults.farm_id", config=config)
    if not fleet_id:
        queue_id = config_file.get_setting("defaults.queue_id", config=config)
        if not queue_id:
            raise click.UsageError(
                "Missing '--fleet-id', '--queue-id', or default Queue ID configuration"
            )

    deadline = api.get_boto3_client("deadline", config=config)

    if fleet_id:
        response = deadline.get_fleet(farmId=farm_id, fleetId=fleet_id)
        response.pop("ResponseMetadata", None)

        click.echo(_cli_object_repr(response))
    else:
        response = deadline.get_queue(farmId=farm_id, queueId=queue_id)
        queue_name = response["displayName"]

        response = api._list_apis._call_paginated_deadline_list_api(
            deadline.list_queue_fleet_associations,
            "queueFleetAssociations",
            farmId=farm_id,
            queueId=queue_id,
        )
        response.pop("ResponseMetadata", None)
        qfa_list = response["queueFleetAssociations"]

        click.echo(
            f"Showing all fleets ({len(qfa_list)} total) associated with queue: {queue_name}"
        )
        for qfa in qfa_list:
            response = deadline.get_fleet(farmId=farm_id, fleetId=qfa["fleetId"])
            response.pop("ResponseMetadata", None)
            response["queueFleetAssociationStatus"] = qfa["status"]

            click.echo("")
            click.echo(_cli_object_repr(response))
