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
from typing import Any
from urllib.parse import urlparse

import boto3
import click

from deadline.client import api
from deadline.job_attachments.api.manifest import (
    _manifest_diff,
    _manifest_download,
    _manifest_snapshot,
    _manifest_upload,
)

from ...config import config_file
from ...exceptions import NonValidInputError
from .._common import _apply_cli_options_to_config, _handle_error
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
    "--destination",
    default=None,
    help="Destination directory where manifest is created. Defaults to the manifest root directory.",
)
@click.option(
    "--name",
    default=None,
    help="Name of the manifest. A timestamp is added YYYY-MM-DD-HH-MM-SS for versioning.",
)
@click.option(
    "--glob",
    default=None,
    help="Glob include and exclude of directory and file regex to include in the manifest.",
)
@click.option("--diff", default=None, help="File Path to Asset Manifest to diff against.")
@click.option("--json", default=None, is_flag=True, help="Output is printed as JSON for scripting")
@_handle_error
def manifest_snapshot(
    root: str, destination: str, name: str, glob: str, diff: str, json: bool, **args
):
    """
    Creates manifest of files specified by root directory.
    """
    logger: ClickLogger = ClickLogger(is_json=json)
    if not os.path.isdir(root):
        raise NonValidInputError(f"Specified root directory {root} does not exist. ")

    if destination and not os.path.isdir(destination):
        raise NonValidInputError(f"Specified destination directory {destination} does not exist. ")
    elif destination is None:
        destination = root
        logger.echo(f"Manifest creation path defaulted to {root} \n")

    manifest_out = _manifest_snapshot(
        root=root, destination=destination, name=name, glob=glob, diff=diff, logger=logger
    )
    if manifest_out:
        logger.json(dataclasses.asdict(manifest_out))


@cli_manifest.command(
    name="diff", help="BETA - Compute a directory root diff of new, modified or deleted files."
)
@click.option("--root", help="The root directory to compare changes to. ")
@click.option(
    "--manifest",
    required=True,
    help="The path to manifest file to diff against.",
)
@click.option(
    "--glob",
    default=None,
    help="Glob regex to find include and exclude directories and files to include in the manifest.",
)
@click.option("--json", default=None, is_flag=True, help="Output is printed as JSON for scripting")
@_handle_error
def manifest_diff(root: str, manifest: str, glob: str, json: bool, **args):
    """
    Check file differences between a directory and specified manifest.
    """
    logger: ClickLogger = ClickLogger(is_json=json)
    if not os.path.isfile(manifest):
        raise NonValidInputError(f"Specified manifest file {manifest} does not exist. ")

    if not os.path.isdir(root):
        raise NonValidInputError(f"Specified root directory {root} does not exist. ")

    differences = _manifest_diff(
        manifest=manifest, root=root, glob=glob, logger=logger, pretty_print=not json
    )
    logger.json(dataclasses.asdict(differences), indent=4)


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
@click.option("--json", default=None, is_flag=True, help="Output is printed as JSON for scripting")
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
    logger: ClickLogger = ClickLogger(is_json=json)
    if not os.path.isdir(download_dir):
        raise NonValidInputError(f"Specified destination directory {download_dir} does not exist. ")

    # setup config
    config = _apply_cli_options_to_config(required_options={"farm_id", "queue_id"}, **args)
    queue_id: str = config_file.get_setting("defaults.queue_id", config=config)
    farm_id: str = config_file.get_setting("defaults.farm_id", config=config)

    boto3_session: boto3.Session = api.get_boto3_session(config=config)

    output = _manifest_download(
        download_dir=download_dir,
        farm_id=farm_id,
        queue_id=queue_id,
        job_id=job_id,
        step_id=step_id,
        boto3_session=boto3_session,
        logger=logger,
    )
    logger.json(dataclasses.asdict(output))


@cli_manifest.command(
    name="upload",
    help="BETA - Uploads a job attachment manifest file to a Content Addressable Storage's Manifest store. If calling via --cas-path, it is recommended to use with --profile for a specific AWS profile with CAS S3 bucket access.",
)
@click.argument("manifest_file")
@click.option("--profile", help="The AWS profile to use.")
@click.option("--cas-path", help="The path to the Content Addressable Storage root.")
@click.option(
    "--farm-id", help="The AWS Deadline Cloud Farm to use. Alternative to using --cas-path."
)
@click.option(
    "--queue-id", help="The AWS Deadline Cloud Queue to use. Alternative to using --cas-path."
)
@click.option("--json", default=None, is_flag=True, help="Output is printed as JSON for scripting")
@_handle_error
def manifest_upload(
    manifest_file: str,
    cas_path: str,
    json: bool,
    **args,
):
    # Input checking.
    if not manifest_file or not os.path.isfile(manifest_file):
        raise NonValidInputError(f"Specified manifest {manifest_file} does not exist. ")

    # Where will we upload the manifest to?
    required: set[str] = set()
    if not cas_path:
        required = {"farm_id", "queue_id"}

    config = _apply_cli_options_to_config(required_options=required, **args)

    # Logger
    logger: ClickLogger = ClickLogger(is_json=json)

    # Upload settings:
    metadata: dict[str, Any] = {"Metadata": {}}
    metadata["Metadata"]["file-system-location-name"] = manifest_file

    bucket_name: str = ""
    manifest_path: str = ""
    session: boto3.Session = api.get_boto3_session(config=config)
    if not cas_path:
        farm_id = config_file.get_setting("defaults.farm_id", config=config)
        queue_id = config_file.get_setting("defaults.queue_id", config=config)

        deadline = api.get_boto3_client("deadline", config=config)
        queue = deadline.get_queue(
            farmId=farm_id,
            queueId=queue_id,
        )
        bucket_name = queue["jobAttachmentSettings"]["s3BucketName"]
        manifest_path = queue["jobAttachmentSettings"]["rootPrefix"]

        # IF we supplied a farm and queue, use the queue credentials.
        session = api.get_queue_user_boto3_session(
            deadline=deadline,
            config=config,
            farm_id=farm_id,
            queue_id=queue_id,
            queue_display_name=queue["displayName"],
        )

    else:
        # Self supplied cas path.
        url_fragments = urlparse(cas_path)
        bucket_name = url_fragments.netloc
        manifest_path = url_fragments.path

    logger.echo(f"Uploading Manifest to {bucket_name} {manifest_path}")
    _manifest_upload(manifest_file, bucket_name, manifest_path, metadata, session, logger)
    logger.echo("Uploading successful!")
