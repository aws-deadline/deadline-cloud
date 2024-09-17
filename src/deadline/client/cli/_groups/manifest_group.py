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
import datetime
import os
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlparse

import boto3
import click
from botocore.client import BaseClient

from deadline.client import api
from deadline.job_attachments._diff import compare_manifest
from deadline.job_attachments._glob import _process_glob_inputs
from deadline.job_attachments._utils import _glob_paths
from deadline.job_attachments.api.manifest import (
    _manifest_diff,
    _manifest_download,
    _manifest_upload,
)
from deadline.job_attachments.asset_manifests.base_manifest import (
    BaseManifestPath,
)
from deadline.job_attachments.asset_manifests.decode import decode_manifest
from deadline.job_attachments.models import (
    GlobConfig,
    JobAttachmentS3Settings,
)
from deadline.job_attachments.upload import FileStatus, S3AssetManager

from ...config import config_file
from ...exceptions import NonValidInputError
from .._common import _apply_cli_options_to_config, _handle_error, _ProgressBarCallbackManager
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
    help="Destination directory where manifest is created. Defaults to the root directory.",
)
@click.option(
    "--name",
    default=None,
    help="Name of the manifest. A time stamp is added YYYY-MM-DD-HH-MM-SS for versioning.",
)
@click.option(
    "--glob",
    default=None,
    help="Glob include and exclude of directory and file regex to include in the manifest.",
)
@click.option("--diff", default=None, help="Asset Manifest to diff against.")
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

    # Get all files in the root.
    glob_config: GlobConfig = _process_glob_inputs(glob)
    inputs = _glob_paths(root, include=glob_config.include_glob, exclude=glob_config.exclude_glob)

    # Placeholder Asset Manager
    asset_manager = S3AssetManager(
        farm_id=" ", queue_id=" ", job_attachment_settings=JobAttachmentS3Settings(" ", " ")
    )

    hash_callback_manager: Optional[_ProgressBarCallbackManager] = None
    if not json:
        hash_callback_manager = _ProgressBarCallbackManager(length=100, label="Hashing Attachments")

    upload_group = asset_manager.prepare_paths_for_upload(
        input_paths=inputs, output_paths=[root], referenced_paths=[]
    )
    assert len(upload_group.asset_groups) == 1

    if upload_group.asset_groups:
        _, manifests = api.hash_attachments(
            asset_manager=asset_manager,
            asset_groups=upload_group.asset_groups,
            total_input_files=upload_group.total_input_files,
            total_input_bytes=upload_group.total_input_bytes,
            print_function_callback=logger.echo,
            hashing_progress_callback=hash_callback_manager.callback if not json else None,  # type: ignore[union-attr]
        )

    if not manifests or len(manifests) == 0:
        logger.echo("No manifest generated")
        logger.json({})
        return

    # This is a hard failure, we are snapshotting 1 directory.
    assert len(manifests) == 1
    output_manifest = manifests[0].asset_manifest
    assert output_manifest

    # If this is a diff manifest, load the supplied manifest file.
    if diff:
        # Parse local manifest
        with open(diff) as source_diff:
            source_manifest_str = source_diff.read()
            source_manifest = decode_manifest(source_manifest_str)

        # Get the differences
        changed_paths: list[str] = []
        differences: list[tuple[FileStatus, BaseManifestPath]] = compare_manifest(
            source_manifest, output_manifest
        )
        for diff_item in differences:
            if diff_item[0] == FileStatus.MODIFIED or diff_item[0] == FileStatus.NEW:
                full_diff_path = f"{root}/{diff_item[1].path}"
                changed_paths.append(full_diff_path)
                logger.echo(f"Found difference at: {full_diff_path}, Status: {diff_item[0]}")

        # Since the files are already hashed, we can easily re-use has_attachments to remake a diff manifest.
        diff_group = asset_manager.prepare_paths_for_upload(
            input_paths=changed_paths, output_paths=[root], referenced_paths=[]
        )
        _, diff_manifests = api.hash_attachments(
            asset_manager=asset_manager,
            asset_groups=diff_group.asset_groups,
            total_input_files=diff_group.total_input_files,
            total_input_bytes=diff_group.total_input_bytes,
            print_function_callback=logger.echo,
            hashing_progress_callback=hash_callback_manager.callback if not json else None,  # type: ignore[union-attr]
        )
        output_manifest = diff_manifests[0].asset_manifest

    # Write created manifest into local file, at the specified location at destination
    if output_manifest is not None:

        timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
        manifest_name = name if name else root.replace("/", "_")
        manifest_name = manifest_name[1:] if manifest_name[0] == "_" else manifest_name
        manifest_name = f"{manifest_name}-{timestamp}.manifest"

        local_manifest_file = Path(destination, manifest_name)
        local_manifest_file.parent.mkdir(parents=True, exist_ok=True)
        with open(local_manifest_file, "w") as file:
            file.write(output_manifest.encode())

        # Output results.
        logger.echo(f"Manifest Generated at {destination}{manifest_name}\n")
        json_output: dict = {"manifest": f"{destination}{manifest_name}"}
        logger.json(json_output)
    else:
        # No manifest generated.
        logger.echo("No manifest generated")
        logger.json({})


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
    help="Glob include and exclude of directory and file regex to include in the manifest.",
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

    deadline: BaseClient = api.get_boto3_client("deadline", config=config)

    output = _manifest_download(
        download_dir=download_dir,
        farm_id=farm_id,
        queue_id=queue_id,
        job_id=job_id,
        step_id=step_id,
        deadline=deadline,
        config=config,
        logger=logger,
    )
    logger.json(dataclasses.asdict(output))


@cli_manifest.command(
    name="upload",
    help="BETA - Uploads a job attachment manifest file to a Content Addressable Storage's Manifest store.",
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
    session: boto3.Session = api.get_boto3_session()
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
