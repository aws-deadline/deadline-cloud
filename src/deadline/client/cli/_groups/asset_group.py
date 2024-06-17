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
@_handle_error
def asset_snapshot(**args):
    """
    Creates manifest of files specified root directory.
    """
    click.echo("snapshot taken")


@cli_asset.command(name="upload")
@click.option("--manifest", help="The manifest of files to be uploaded. ")
@_handle_error
def asset_upload(**args):
    """
    Uploads the assets in the provided manifest file to S3.
    """
    click.echo("upload done")


@cli_asset.command(name="diff")
@click.option("--manifest", help="The manifest of working directory to show changes of. ")
@_handle_error
def asset_diff(**args):
    """
    Check file differences of a directory since last snapshot.

    TODO: show example of diff output
    """
    click.echo("diff shown")


@cli_asset.command(name="download")
@click.option("--job-id", help="The job ID chosen to download input manifest from. ")
@_handle_error
def asset_download(**args):
    """
    Downloads input manifest of previously submitted job.
    """
    click.echo("download complete")
