# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
All the `deadline asset` commands:
    * snapshot
    * upload
    * diff
    * download
"""

import click

from .._common import _handle_error


@click.group(name="asset")
@_handle_error
def cli_asset():
    """
    Commands to work with AWS Deadline Cloud Job Attachments.
    """


@cli_asset.command(name="snapshot")
@click.option("--root-dir", help="The root directory to snapshot. ")
@click.option("--manifest-out", help="Destination path to directory where manifest is created. ")
@click.option(
    "--recursive",
    "-r",
    help="Flag to recursively snapshot subdirectories. ",
    is_flag=True,
    show_default=True,
    default=False,
)
@_handle_error
def asset_snapshot(**args):
    """
    Creates manifest of files specified root directory.
    """
    click.echo("snapshot taken")


@cli_asset.command(name="upload")
@click.option(
    "--manifest", help="The path to manifest folder of the directory specified for upload. "
)
@click.option("--farm-id", help="The AWS Deadline Cloud Farm to use. ")
@click.option("--queue-id", help="The AWS Deadline Cloud Queue to use. ")
@click.option(
    "--update",
    help="Flag to update manifest before upload. ",
    is_flag=True,
    show_default=True,
    default=False,
)
@_handle_error
def asset_upload(**args):
    """
    Uploads the assets in the provided manifest file to S3.
    """
    click.echo("upload done")


@cli_asset.command(name="diff")
@click.option("--root-dir", help="The root directory to compare changes to. ")
@click.option(
    "--manifest", help="The path to manifest folder of the directory to show changes of. "
)
@click.option(
    "--format",
    help="Pretty prints diff information with easy to read formatting. ",
    is_flag=True,
    show_default=True,
    default=False,
)
@_handle_error
def asset_diff(**args):
    """
    Check file differences of a directory since last snapshot.

    TODO: show example of diff output
    """
    click.echo("diff shown")


@cli_asset.command(name="download")
@click.option("--farm-id", help="The AWS Deadline Cloud Farm to use.")
@click.option("--queue-id", help="The AWS Deadline Cloud Queue to use.")
@click.option("--job-id", help="The AWS Deadline Cloud Job to get. ")
@_handle_error
def asset_download(**args):
    """
    Downloads input manifest of previously submitted job.
    """
    click.echo("download complete")
