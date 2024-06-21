# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
All the `deadline asset` commands:
    * snapshot
    * upload
    * diff
    * download
"""

import click
import os
from pathlib import Path

from .._common import _handle_error
from ...exceptions import NonValidInputError
from deadline.job_attachments.asset_manifests import hash_data
from deadline.job_attachments.upload import S3AssetManager, S3AssetUploader
from deadline.job_attachments.models import JobAttachmentS3Settings

@click.group(name="asset")
@_handle_error
def cli_asset():
    """
    Commands to work with AWS Deadline Cloud Job Attachments.
    """

@cli_asset.command(name="snapshot")
@click.option("--root-dir", help="The root directory to snapshot. ")
@click.option("--r", help="Flag to recursively snapshot subdirectories. ", is_flag=True, show_default=True, default=False)
@_handle_error
def asset_snapshot(r, **args):
    """
    Creates manifest of files specified root directory.
    """
    root_dir = args.pop("root_dir")

    # refactor
    if os.path.isdir(root_dir):
        inputs = []
        for root, dirs, files in os.walk(root_dir):
            print("root: ", root)
            
            if os.path.basename(root) == "manifests":
                continue

            #hashing attachments progress callback?
                #hashing_progress_callback

            for file in files:
                file_full_path = str(os.path.join(root, file))
                if _is_hidden_file(file_full_path):
                    continue
                print("file: ", file)
                inputs.append(file_full_path)

            if not r:
                break

        # Placeholder Asset Manager
        asset_manager = S3AssetManager(
            farm_id=" ",
            queue_id=" ",
            job_attachment_settings=JobAttachmentS3Settings(" "," ")
        )

        upload_group = asset_manager.prepare_paths_for_upload(inputs, [root_dir], [])
        (_, manifests) = asset_manager.hash_assets_and_create_manifest(
        upload_group.asset_groups, upload_group.total_input_files, upload_group.total_input_bytes
        )

        # Write created manifest into local file, at the specified location root_dir
        for asset_root_manifests in manifests:
            # refactor
            hash_alg = asset_root_manifests.asset_manifest.get_default_hash_alg()
            source_root = Path(asset_root_manifests.root_path)
            file_system_location_name = asset_root_manifests.file_system_location_name

            #refactor
            manifest_name_prefix = hash_data(
                f"{file_system_location_name or ''}{str(source_root)}".encode(), hash_alg
            )
            manifest_name = f"{manifest_name_prefix}_input"

            local_manifest_file = Path(root_dir, "manifests", manifest_name)

            local_manifest_file.parent.mkdir(parents=True, exist_ok=True)
            with open(local_manifest_file, "w") as file:
                file.write(asset_root_manifests.asset_manifest.encode())

    else:
        misconfigured_directories_msg = (f"Specified root directory {root_dir} does not exist. ")
        raise NonValidInputError(misconfigured_directories_msg)


    #click.echo(manifests)


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


def _is_hidden_file(filepath):
    """
    Checks for hidden files in directory, depending on OS
    """
    if os.name == 'nt':  # Windows
        return os.stat(filepath).st_file_attributes & stat.FILE_ATTRIBUTE_HIDDEN
    else:  # Unix-based
        return os.path.basename(filepath).startswith('.')