# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
All the `deadline manifest` commands:
    * snapshot
    * upload
    * diff
    * download
"""
from __future__ import annotations

import concurrent.futures
import dataclasses
import datetime
import logging
import os
from pathlib import Path
from typing import Any, List, Optional
from urllib.parse import urlparse

import boto3
import click
from botocore.client import BaseClient

from deadline.client import api
from deadline.job_attachments._glob import _process_glob_inputs
from deadline.job_attachments._utils import _glob_paths
from deadline.job_attachments.api.manifest import _manifest_download, _manifest_upload
from deadline.job_attachments.asset_manifests.base_manifest import (
    BaseAssetManifest,
    BaseManifestPath,
)
from deadline.job_attachments.asset_manifests.decode import decode_manifest
from deadline.job_attachments.caches import HashCache
from deadline.job_attachments.models import (
    AssetRootManifest,
    GlobConfig,
    JobAttachmentS3Settings,
    ManifestDiff,
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

    asset_manager = S3AssetManager(
        farm_id=" ", queue_id=" ", job_attachment_settings=JobAttachmentS3Settings(" ", " ")
    )

    # get inputs of directory
    glob_config: GlobConfig = _process_glob_inputs(glob)
    input_files = _glob_paths(
        root, include=glob_config.include_glob, exclude=glob_config.exclude_glob
    )
    input_paths = [Path(p) for p in input_files]

    # hash and create manifest of local directory
    cache_config = config_file.get_cache_directory()
    with HashCache(cache_config) as hash_cache:
        directory_manifest_object = asset_manager._create_manifest_file(
            input_paths=input_paths, root_path=root, hash_cache=hash_cache
        )

    # parse local manifest
    local_manifest_object: BaseAssetManifest
    with open(manifest) as input_file:
        manifest_data_str = input_file.read()
        local_manifest_object = decode_manifest(manifest_data_str)

    # compare manifests
    differences: List[tuple[FileStatus, BaseManifestPath]] = compare_manifest(
        reference_manifest=local_manifest_object, compare_manifest=directory_manifest_object
    )

    if json:
        output: ManifestDiff = ManifestDiff()

        for item in differences:
            if item[0] == FileStatus.MODIFIED:
                output.modified.append(item[1].path)
            elif item[0] == FileStatus.NEW:
                output.new.append(item[1].path)
            elif item[0] == FileStatus.DELETED:
                output.deleted.append(item[1].path)

        logger.json(dataclasses.asdict(output), indent=4)
    else:
        logger.echo(f"\n{root}")
        pretty_print(file_status_list=differences)


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
        elif reference_dict[file_path].hash != manifest_path.hash:
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
