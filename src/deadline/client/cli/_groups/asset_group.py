# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
All the `deadline asset` commands:
    * snapshot
    * upload
    * diff
    * download
"""

import os
from pathlib import Path

import click

from deadline.client import api
from deadline.job_attachments.upload import S3AssetManager, S3AssetUploader
from deadline.job_attachments.models import JobAttachmentS3Settings

from .._common import _apply_cli_options_to_config, _handle_error, _ProgressBarCallbackManager
from ...exceptions import NonValidInputError

IGNORE_FILE: str = "manifests"


@click.group(name="asset")
@_handle_error
def cli_asset():
    """
    Commands to work with AWS Deadline Cloud Job Attachments.
    """


@cli_asset.command(name="snapshot")
@click.option("--root-dir", required=True, help="The root directory to snapshot. ")
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
def asset_snapshot(root_dir, manifest_out, recursive, **args):
    """
    Creates manifest of files specified root directory.
    """
    root_dir_basename = os.path.basename(root_dir) + "_"

    if not os.path.isdir(root_dir):
        misconfigured_directories_msg = f"Specified root directory {root_dir} does not exist. "
        raise NonValidInputError(misconfigured_directories_msg)

    if manifest_out and not os.path.isdir(manifest_out):
        misconfigured_directories_msg = (
            f"Specified destination directory {manifest_out} does not exist. "
        )
        raise NonValidInputError(misconfigured_directories_msg)
    elif manifest_out is None:
        manifest_out = root_dir

    inputs = []
    for root, dirs, files in os.walk(root_dir):
        if os.path.basename(root).endswith("_manifests"):
            continue
        for file in files:
            file_full_path = str(os.path.join(root, file))
            inputs.append(file_full_path)
        if not recursive:
            break

    # Placeholder Asset Manager
    asset_manager = S3AssetManager(
        farm_id=" ", queue_id=" ", job_attachment_settings=JobAttachmentS3Settings(" ", " ")
    )
    asset_uploader = S3AssetUploader()
    hash_callback_manager = _ProgressBarCallbackManager(length=100, label="Hashing Attachments")

    upload_group = asset_manager.prepare_paths_for_upload(
        input_paths=inputs, output_paths=[root_dir], referenced_paths=[]
    )
    if upload_group.asset_groups:
        _, manifests = api.hash_attachments(
            asset_manager=asset_manager,
            asset_groups=upload_group.asset_groups,
            total_input_files=upload_group.total_input_files,
            total_input_bytes=upload_group.total_input_bytes,
            print_function_callback=click.echo,
            hashing_progress_callback=hash_callback_manager.callback,
        )

    # Write created manifest into local file, at the specified location at manifest_out
    for asset_root_manifests in manifests:
        if asset_root_manifests.asset_manifest is None:
            continue
        source_root = Path(asset_root_manifests.root_path)
        file_system_location_name = asset_root_manifests.file_system_location_name
        (_, _, manifest_name) = asset_uploader._gather_upload_metadata(
            asset_root_manifests.asset_manifest, source_root, file_system_location_name
        )
        asset_uploader._write_local_input_manifest(
            manifest_out, manifest_name, asset_root_manifests.asset_manifest, root_dir_basename
        )


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
