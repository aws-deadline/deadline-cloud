# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
All the `deadline asset` commands:
    * snapshot
    * upload
    * diff
    * download
"""
from __future__ import annotations

import os
from pathlib import Path
import concurrent.futures
from typing import List
import logging
import glob

import click

from deadline.client import api
from deadline.job_attachments.upload import FileStatus, S3AssetManager, S3AssetUploader
from deadline.job_attachments.models import (
    JobAttachmentS3Settings,
    AssetRootManifest,
)
from deadline.job_attachments.asset_manifests.decode import decode_manifest
from deadline.job_attachments.asset_manifests.base_manifest import BaseAssetManifest
from deadline.job_attachments.caches import HashCache

from .._common import _apply_cli_options_to_config, _handle_error, _ProgressBarCallbackManager
from ...exceptions import NonValidInputError, ManifestOutdatedError
from ...config import get_setting, config_file
import boto3
from botocore.client import BaseClient


@click.group(name="asset")
@_handle_error
def cli_asset():
    """
    Commands to work with AWS Deadline Cloud Job Attachments.
    """


@cli_asset.command(name="snapshot")
@click.option("--root-dir", required=True, help="The root directory to snapshot. ")
@click.option(
    "--manifest-out", default=None, help="Destination path to directory where manifest is created. "
)
@click.option(
    "--recursive",
    "-r",
    help="Flag to recursively snapshot subdirectories. ",
    is_flag=True,
    show_default=True,
    default=False,
)
@_handle_error
def asset_snapshot(root_dir: str, manifest_out: str, recursive: bool, **args):
    """
    Creates manifest of files specified root directory.
    """
    if not os.path.isdir(root_dir):
        raise NonValidInputError(f"Specified root directory {root_dir} does not exist. ")

    if manifest_out and not os.path.isdir(manifest_out):
        raise NonValidInputError(f"Specified destination directory {manifest_out} does not exist. ")
    elif manifest_out is None:
        manifest_out = root_dir
        click.echo(f"Manifest creation path defaulted to {root_dir} \n")

    inputs = []
    for root, dirs, files in os.walk(root_dir):
        inputs.extend([str(os.path.join(root, file)) for file in files])
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
            manifest=asset_root_manifests.asset_manifest,
            source_root=source_root,
            file_system_location_name=file_system_location_name,
        )
        asset_uploader._write_local_input_manifest(
            manifest_write_dir=manifest_out,
            manifest_name=manifest_name,
            manifest=asset_root_manifests.asset_manifest,
            root_dir_name=os.path.basename(root_dir),
        )

    click.echo(f"Manifest created at {manifest_out}\n")


@cli_asset.command(name="upload")
@click.option(
    "--root-dir",
    help="The root directory of assets to upload. Defaults to the parent directory of --manifest-dir if not specified. ",
)
@click.option(
    "--manifest-dir",
    required=True,
    help="The path to manifest folder of the directory specified for upload. ",
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
def asset_upload(root_dir: str, manifest_dir: str, update: bool, **args):
    """
    Uploads the assets in the provided manifest file to S3.
    """

    if not os.path.isdir(manifest_dir):
        raise NonValidInputError(f"Specified manifest directory {manifest_dir} does not exist. ")

    if root_dir is None:
        asset_root_dir = os.path.dirname(manifest_dir)
    else:
        if not os.path.isdir(root_dir):
            raise NonValidInputError(f"Specified root directory {root_dir} does not exist. ")
        asset_root_dir = root_dir

    config = _apply_cli_options_to_config(required_options={"farm_id", "queue_id"}, **args)
    upload_callback_manager: _ProgressBarCallbackManager = _ProgressBarCallbackManager(
        length=100, label="Uploading Attachments"
    )

    deadline: BaseClient = api.get_boto3_client("deadline", config=config)
    queue_id: str = get_setting("defaults.queue_id", config=config)
    farm_id: str = get_setting("defaults.farm_id", config=config)

    queue: dict = deadline.get_queue(
        farmId=farm_id,
        queueId=queue_id,
    )

    # assume queue role - session permissions
    queue_role_session: boto3.Session = api.get_queue_user_boto3_session(
        deadline=deadline,
        config=config,
        farm_id=farm_id,
        queue_id=queue_id,
        queue_display_name=queue["displayName"],
    )

    asset_manager: S3AssetManager = S3AssetManager(
        farm_id=farm_id,
        queue_id=queue_id,
        job_attachment_settings=JobAttachmentS3Settings(**queue["jobAttachmentSettings"]),
        session=queue_role_session,
    )

    asset_uploader: S3AssetUploader = S3AssetUploader()

    # read local manifest into BaseAssetManifest object
    asset_manifest: BaseAssetManifest = read_local_manifest(manifest=manifest_dir)
    clear_S3_mapping(manifest=manifest_dir)

    if asset_manifest is None:
        raise NonValidInputError(
            f"Specified manifest directory {manifest_dir} does contain valid manifest input file. "
        )

    asset_root_manifest: AssetRootManifest = AssetRootManifest(
        root_path=asset_root_dir,
        asset_manifest=asset_manifest,
    )

    manifest_changes: List[tuple] = diff_manifest(
        asset_manager=asset_manager,
        asset_root_manifest=asset_root_manifest,
        manifest=manifest_dir,
        update=update,
    )

    # if there are modified files, will either auto --update manifest or prompt user of file discrepancy
    if len(manifest_changes) > 0:
        if update:
            asset_root_manifest.asset_manifest = update_manifest(
                manifest=manifest_dir, new_or_modified_paths=manifest_changes
            )
            click.echo(f"Manifest information updated: {len(manifest_changes)} files updated. \n")
        else:
            raise ManifestOutdatedError(
                f"Manifest contents in {manifest_dir} are outdated; versioning does not match local files in {asset_root_dir}. Please run with --update to fix current files. \n"
            )

    attachment_settings: dict = api.upload_attachments(
        asset_manager=asset_manager,
        manifests=[asset_root_manifest],
        print_function_callback=click.echo,
        upload_progress_callback=upload_callback_manager.callback,
    )

    full_manifest_key: str = attachment_settings["manifests"][0]["inputManifestPath"]
    manifest_name = os.path.basename(full_manifest_key)
    manifest_dir_name = os.path.basename(manifest_dir)
    asset_uploader._write_local_manifest_s3_mapping(
        manifest_write_dir=asset_root_dir,
        manifest_name=manifest_name,
        full_manifest_key=full_manifest_key,
        manifest_dir_name=manifest_dir_name,
    )

    click.echo(f"Upload of {asset_root_dir} complete. \n")


@cli_asset.command(name="diff")
@click.option("--root-dir", help="The root directory to compare changes to. ")
@click.option(
    "--manifest-dir",
    required=True,
    help="The path to manifest folder of the directory to show changes of. ",
)
@click.option(
    "--raw",
    help="Outputs the raw JSON info of files and their changed statuses. ",
    is_flag=True,
    show_default=True,
    default=False,
)
@_handle_error
def asset_diff(root_dir: str, manifest_dir: str, raw: bool, **args):
    """
    Check file differences of a directory since last snapshot, specified by manifest.
    """
    if not os.path.isdir(manifest_dir):
        raise NonValidInputError(f"Specified manifest directory {manifest_dir} does not exist. ")

    if root_dir is None:
        asset_root_dir = os.path.dirname(manifest_dir)
    else:
        if not os.path.isdir(root_dir):
            raise NonValidInputError(f"Specified root directory {root_dir} does not exist. ")
        asset_root_dir = root_dir

    asset_manager = S3AssetManager(
        farm_id=" ", queue_id=" ", job_attachment_settings=JobAttachmentS3Settings(" ", " ")
    )

    # get inputs of directory
    input_paths = []
    for root, dirs, files in os.walk(asset_root_dir):
        for filename in files:
            file_path = os.path.join(root, filename)
            input_paths.append(Path(file_path))

    # hash and create manifest of local directory
    cache_config = config_file.get_cache_directory()
    with HashCache(cache_config) as hash_cache:
        directory_manifest_object = asset_manager._create_manifest_file(
            input_paths=input_paths, root_path=asset_root_dir, hash_cache=hash_cache
        )

    # parse local manifest
    local_manifest_object: BaseAssetManifest = read_local_manifest(manifest=manifest_dir)

    # compare manifests
    differences: List[tuple] = compare_manifest(
        reference_manifest=local_manifest_object, compare_manifest=directory_manifest_object
    )

    if raw:
        click.echo(f"\nFile Diffs: {differences}")
    else:
        click.echo(f"\n{asset_root_dir}")
        pretty_print(file_status_list=differences)


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


def read_local_manifest(manifest: str) -> BaseAssetManifest:
    """
    Read manifests specified by filepath to manifest folder, returns BaseAssetManifest Object
    """
    input_files = glob.glob(os.path.join(manifest, "*_input"))

    if not input_files:
        raise ValueError(f"No manifest files found in {manifest}")
    elif len(input_files) >= 2:
        raise NonValidInputError(
            f"Multiple input manifest files are not supported, found: {input_files}."
        )

    manifest_file_path = input_files[0]

    with open(manifest_file_path, "r") as input_file:
        manifest_data_str = input_file.read()
        asset_manifest = decode_manifest(manifest_data_str)

        return asset_manifest


def clear_S3_mapping(manifest: str):
    """
    Clears manifest_s3_mapping file contents if it previously exists.
    """
    for filename in os.listdir(manifest):
        if filename.endswith("manifest_s3_mapping"):
            # if S3 mapping already exists, clear contents
            filepath = os.path.join(manifest, filename)
            with open(filepath, "w") as _:
                pass


def diff_manifest(
    asset_manager: S3AssetManager,
    asset_root_manifest: AssetRootManifest,
    manifest: str,
    update: bool,
) -> List[tuple]:
    """
    Gets the file paths in specified manifest if the contents of file have changed since its last snapshot.
    """
    manifest_dir_name = os.path.basename(manifest)
    root_path = asset_root_manifest.root_path
    input_paths: List[Path] = []

    asset_manifest = asset_root_manifest.asset_manifest
    if asset_manifest is None:
        raise NonValidInputError("Manifest object not found, please check input manifest. ")

    for base_manifest_path in asset_manifest.paths:
        if base_manifest_path.path.startswith(manifest_dir_name):
            # skip the manifest folder, or else every upload will need an update after a previous change
            continue
        input_paths.append(Path(root_path, base_manifest_path.path))

    return find_file_with_status(
        asset_manager=asset_manager,
        input_paths=input_paths,
        root_path=root_path,
        update=update,
        statuses=[FileStatus.NEW, FileStatus.MODIFIED],
    )


def find_file_with_status(
    asset_manager: S3AssetManager,
    input_paths: List[Path],
    root_path: str,
    update: bool,
    statuses: List[FileStatus],
) -> List[tuple]:
    """
    Checks a manifest file, compares it to specified root directory or manifest of files with the local hash cache, and finds files that match the specified statuses.
    Returns a list of tuples containing the file information, and its corresponding file status.
    """
    cache_config = config_file.get_cache_directory()

    with HashCache(cache_config) as hash_cache:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = {
                executor.submit(
                    asset_manager._process_input_path,
                    path=path,
                    root_path=root_path,
                    hash_cache=hash_cache,
                    update=update,
                ): path
                for path in input_paths
            }
            status_paths: List[tuple] = []
            for future in concurrent.futures.as_completed(futures):
                (file_status, _, manifestPath) = future.result()
                if file_status in statuses:
                    status_paths.append((file_status, manifestPath))

            return status_paths


def update_manifest(manifest: str, new_or_modified_paths: List[tuple]) -> BaseAssetManifest:
    """
    Updates the local manifest file to reflect modified or new files
    """
    input_files = glob.glob(os.path.join(manifest, "*_input"))

    if not input_files:
        raise ValueError(f"No manifest files found in {manifest}")
    elif len(input_files) >= 2:
        raise NonValidInputError(
            f"Multiple input manifest files are not supported, found: {input_files}."
        )

    manifest_file_path = input_files[0]

    with open(manifest_file_path, "r") as manifest_file:
        manifest_data_str = manifest_file.read()
        local_base_asset_manifest = decode_manifest(manifest_data_str)

    # maps paths of local to optimize updating of manifest entries
    manifest_info_dict = {
        base_manifest_path.path: base_manifest_path
        for base_manifest_path in local_base_asset_manifest.paths
    }

    for _, base_asset_manifest in new_or_modified_paths:
        if base_asset_manifest.path in manifest_info_dict:
            # Update the hash_value of the existing object
            manifest_info_dict[base_asset_manifest.path].hash = base_asset_manifest.hash
        else:
            # Add the new object if it doesn't exist
            manifest_info_dict[base_asset_manifest.path] = base_asset_manifest

    # write to local manifest
    updated_path_list = list(manifest_info_dict.values())
    local_base_asset_manifest.paths = updated_path_list
    with open(manifest_file_path, "w") as manifest_file:
        manifest_file.write(local_base_asset_manifest.encode())

    return local_base_asset_manifest


def compare_manifest(
    reference_manifest: BaseAssetManifest, compare_manifest: BaseAssetManifest
) -> List[(tuple)]:
    """
    Compares two manifests, reference_manifest acting as the base, and compare_manifest acting as manifest with changes.
    Returns a list of FileStatus and BaseManifestPath

    """
    reference_dict = {
        manifest_path.path: manifest_path for manifest_path in reference_manifest.paths
    }
    compare_dict = {manifest_path.path: manifest_path for manifest_path in compare_manifest.paths}

    differences = []

    # Find new files
    for file_path, manifest_path in compare_dict.items():
        if file_path not in reference_dict:
            differences.append((FileStatus.NEW, manifest_path))
        else:
            if reference_dict[file_path].hash != manifest_path.hash:
                differences.append((FileStatus.MODIFIED, manifest_path))
            else:
                differences.append((FileStatus.UNCHANGED, manifest_path))

    # Find deleted files
    for file_path, manifest_path in reference_dict.items():
        if file_path not in compare_dict:
            differences.append((FileStatus.DELETED, manifest_path))

    return differences


def pretty_print(file_status_list: List[(tuple)]):
    """
    Prints to command line a formatted file tree structure with corresponding file statuses
    """

    # ASCII characters for the tree structure
    PIPE = "│"
    HORIZONTAL = "──"
    ELBOW = "└"
    TEE = "├"
    SPACE = "    "

    # ANSI escape sequences for colors
    COLORS = {
        "MODIFIED": "\033[93m",  # yellow
        "NEW": "\033[92m",  # green
        "DELETED": "\033[91m",  # red
        "UNCHANGED": "\033[90m",  # grey
        "RESET": "\033[0m",  # base color
        "DIRECTORY": "\033[80m",  # grey
    }

    # Tooltips:
    TOOLTIPS = {
        FileStatus.NEW: " +",  # added files
        FileStatus.DELETED: " -",  # deleted files
        FileStatus.MODIFIED: " M",  # modified files
        FileStatus.UNCHANGED: "",  # unchanged files
    }

    class ColorFormatter(logging.Formatter):
        def format(self, record):
            message = super().format(record)
            return f"{message}"

    # Configure logger
    formatter = ColorFormatter("")
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger = logging.getLogger(__name__)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    def print_tree(directory_tree, prefix=""):
        sorted_entries = sorted(directory_tree.items())

        for i, (entry, subtree) in enumerate(sorted_entries, start=1):
            is_last_entry = i == len(sorted_entries)
            symbol = ELBOW + HORIZONTAL if is_last_entry else TEE + HORIZONTAL
            is_dir = isinstance(subtree, dict)
            color = COLORS["DIRECTORY"] if is_dir else COLORS[subtree.name]
            tooltip = TOOLTIPS[FileStatus.UNCHANGED] if is_dir else TOOLTIPS[subtree]

            message = f"{prefix}{symbol}{color}{entry}{tooltip}{COLORS['RESET']}{os.path.sep if is_dir else ''}"
            logger.info(message)

            if is_dir:
                new_prefix = prefix + (SPACE if is_last_entry else PIPE + SPACE)
                print_tree(subtree, new_prefix)

        if not directory_tree:
            symbol = ELBOW + HORIZONTAL
            message = f"{prefix}{symbol}{COLORS['UNCHANGED']}. {COLORS['RESET']}"
            logger.info(message)

    def build_directory_tree(file_status_list: List[tuple]) -> dict[str, dict]:
        directory_tree: dict = {}

        def add_to_tree(path, status):
            parts = path.split(os.path.sep)
            current_level = directory_tree
            for i, part in enumerate(parts):
                if i == len(parts) - 1:
                    current_level[part] = status
                else:
                    current_level = current_level.setdefault(part, {})

        for status, manifest_path in file_status_list:
            add_to_tree(manifest_path.path, status)
        return directory_tree

    directory_tree = build_directory_tree(file_status_list)
    print_tree(directory_tree)
    logger.info("")
