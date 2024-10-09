# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
All the `deadline manifest` commands:
    * snapshot
    * upload
    * diff
    * download
"""
from __future__ import annotations

import dataclasses
import os
from typing import List
import click

from deadline.job_attachments.api.manifest import (
    _manifest_snapshot,
)

from ...exceptions import NonValidInputError
from .._common import _handle_error
from .click_logger import ClickLogger


@click.group(name="manifest")
@_handle_error
def cli_manifest():
    """
    Commands to work with AWS Deadline Cloud Job Attachments.
    """


@cli_manifest.command(
    name="snapshot",
    help="BETA - Generates a snapshot of files in a directory root as a Job Attachment Manifest.",
)
@click.option("--root", required=True, help="The root directory to snapshot. ")
@click.option(
    "-d",
    "--destination",
    default=None,
    help="Destination directory where manifest is created. Defaults to the manifest root directory.",
)
@click.option(
    "-n",
    "--name",
    default=None,
    help="Name of the manifest. A timestamp is added YYYY-MM-DD-HH-MM-SS for versioning.",
)
@click.option(
    "-i",
    "--include",
    default=None,
    help="Glob syntax of files and directories to include in the manifest. Can be provided multiple times.",
)
@click.option(
    "-e",
    "--exclude",
    default=None,
    help="Glob syntax of files and directories to exclude in the manifest. Can be provided multiple times.",
    multiple=True,
)
@click.option(
    "-ie",
    "--include-exclude-config",
    default=None,
    help="Include and exclude config of files and directories to include and exclude. Can be a json file or json string.",
    multiple=True,
)
@click.option("--diff", default=None, help="File Path to Asset Manifest to diff against.")
@click.option("--json", default=None, is_flag=True, help="Output is printed as JSON for scripting.")
@_handle_error
def manifest_snapshot(
    root: str,
    destination: str,
    name: str,
    include: List[str],
    exclude: List[str],
    include_exclude_config: str,
    diff: str,
    json: bool,
    **args,
):
    """
    Creates manifest of files specified by root directory.
    """
    logger: ClickLogger = ClickLogger(is_json=json)
    if not os.path.isdir(root):
        raise NonValidInputError(f"Specified root directory {root} does not exist.")

    if destination and not os.path.isdir(destination):
        raise NonValidInputError(f"Specified destination directory {destination} does not exist.")
    elif destination is None:
        destination = root
        logger.echo(f"Manifest creation path defaulted to {root} \n")

    manifest_out = _manifest_snapshot(
        root=root,
        destination=destination,
        name=name,
        include=include,
        exclude=exclude,
        include_exclude_config=include_exclude_config,
        diff=diff,
        logger=logger,
    )
    if manifest_out:
        logger.json(dataclasses.asdict(manifest_out))


@cli_manifest.command(
    name="diff",
    help="BETA - Compute the file difference of a root directory against an existing manifest for new, modified or deleted files.",
)
@click.option("--root", help="The root directory to compare changes to.")
@click.option(
    "--manifest",
    required=True,
    help="The path to manifest file to diff against.",
)
@click.option(
    "-i",
    "--include",
    default=None,
    help="Glob syntax of files and directories to include in the manifest. Can be provided multiple times.",
    multiple=True,
)
@click.option(
    "-e",
    "--exclude",
    default=None,
    help="Glob syntax of files and directories to exclude in the manifest. Can be provided multiple times.",
    multiple=True,
)
@click.option(
    "-ie",
    "--include-exclude-config",
    default=None,
    help="Include and exclude config of files and directories to include and exclude. Can be a json file or json string.",
)
@click.option("--json", default=None, is_flag=True, help="Output is printed as JSON for scripting.")
@_handle_error
def manifest_diff(
    root: str,
    manifest: str,
    include: List[str],
    exclude: List[str],
    include_exclude_config: str,
    json: bool,
    **args,
):
    """
    Check file differences between a directory and specified manifest.
    """
    raise NotImplementedError("This CLI is being implemented.")


@cli_manifest.command(
    name="download",
    help="BETA - Download Job Attachment Manifests for a Job, or Step including dependencies.",
)
@click.argument("download_dir")
@click.option("--profile", help="The AWS profile to use.")
@click.option("--job-id", required=True, help="The AWS Deadline Cloud Job to get. ")
@click.option("--step-id", help="The AWS Deadline Cloud Step to get. ")
@click.option("--farm-id", help="The AWS Deadline Cloud Farm to use. ")
@click.option("--queue-id", help="The AWS Deadline Cloud Queue to use. ")
@click.option(
    "--json", default=None, is_flag=True, help="Output is printed as JSON for scripting. "
)
@_handle_error
def manifest_download(
    download_dir: str,
    job_id: str,
    step_id: str,
    json: bool,
    **args,
):
    """
    Downloads input manifest of previously submitted job.
    """
    raise NotImplementedError("This CLI is being implemented.")


@cli_manifest.command(
    name="upload",
    help="BETA - Uploads a job attachment manifest file to a Content Addressable Storage's Manifest store. If calling via --s3-cas-path, it is recommended to use with --profile for a specific AWS profile with CAS S3 bucket access.",
)
@click.argument("manifest_file")
@click.option("--profile", help="The AWS profile to use.")
@click.option("--s3-cas-path", help="The path to the Content Addressable Storage root.")
@click.option(
    "--farm-id", help="The AWS Deadline Cloud Farm to use. Alternative to using --s3-cas-path."
)
@click.option(
    "--queue-id", help="The AWS Deadline Cloud Queue to use. Alternative to using --s3-cas-path."
)
@click.option("--json", default=None, is_flag=True, help="Output is printed as JSON for scripting.")
@_handle_error
def manifest_upload(
    manifest_file: str,
    s3_cas_path: str,
    json: bool,
    **args,
):
    raise NotImplementedError("This CLI is being implemented.")
