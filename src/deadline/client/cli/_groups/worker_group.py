# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
All the `deadline worker` commands.
"""

import click
from botocore.exceptions import ClientError  # type: ignore[import]

from ... import api
from ...config import config_file
from ...exceptions import DeadlineOperationError
from .._common import _apply_cli_options_to_config, _cli_object_repr, _handle_error


@click.group(name="worker")
@_handle_error
def cli_worker():
    """
    Commands to work with AWS Deadline Cloud Workers.
    """


@cli_worker.command(name="list")
@click.option("--profile", help="The AWS profile to use.")
@click.option("--farm-id", help="The AWS Deadline Cloud Farm to use.")
@click.option("--fleet-id", help="The AWS Deadline Cloud Fleet to use.", required=True)
@click.option("--page-size", default=5, help="The number of items shown in the page.")
@click.option("--item-offset", default=0, help="The starting offset of the items.")
@_handle_error
def worker_list(page_size, item_offset, fleet_id, **args):
    """
    Lists the Workers in an AWS Deadline Cloud Fleet.
    """
    # Get a temporary config object with the standard options handled
    config = _apply_cli_options_to_config(required_options={"farm_id"}, **args)

    farm_id = config_file.get_setting("defaults.farm_id", config=config)

    deadline = api.get_boto3_client("deadline", config=config)
    try:
        response = deadline.search_workers(
            farmId=farm_id, fleetIds=[fleet_id], itemOffset=item_offset, pageSize=page_size
        )
    except ClientError as exc:
        raise DeadlineOperationError(f"Failed to get Workers from Deadline:\n{exc}") from exc

    total_results = response["totalResults"]

    # Select which fields to print and in which order
    structured_worker_list = [
        {field: worker[field] for field in ["workerId", "status", "createdAt"]}
        for worker in response["workers"]
    ]

    click.echo(
        f"Displaying {len(structured_worker_list)} of {total_results} workers starting at {item_offset}"
    )
    click.echo()
    click.echo(_cli_object_repr(structured_worker_list))


@cli_worker.command(name="get")
@click.option("--profile", help="The AWS profile to use.")
@click.option("--farm-id", help="The AWS Deadline Cloud Farm to use.")
@click.option("--fleet-id", help="The AWS Deadline Cloud Fleet to use.", required=True)
@click.option("--worker-id", help="The AWS Deadline Cloud Worker to get.", required=True)
@_handle_error
def worker_get(fleet_id, worker_id, **args):
    """
    Get the details of an AWS Deadline Cloud worker.
    """
    # Get a temporary config object with the standard options handled
    config = _apply_cli_options_to_config(required_options={"farm_id"}, **args)

    farm_id = config_file.get_setting("defaults.farm_id", config=config)

    deadline = api.get_boto3_client("deadline", config=config)
    response = deadline.get_worker(farmId=farm_id, fleetId=fleet_id, workerId=worker_id)
    response.pop("ResponseMetadata", None)

    click.echo(_cli_object_repr(response))
