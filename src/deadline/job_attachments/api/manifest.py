# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import datetime
import os
from pathlib import Path
from typing import List, Optional, Tuple

from deadline.client.cli._groups.click_logger import ClickLogger
from deadline.job_attachments._diff import _fast_file_list_to_manifest_diff, compare_manifest
from deadline.job_attachments._glob import _process_glob_inputs, _glob_paths
from deadline.job_attachments.asset_manifests._create_manifest import (
    _create_manifest_for_single_root,
)
from deadline.job_attachments.asset_manifests.base_manifest import (
    BaseAssetManifest,
    BaseManifestPath,
)
from deadline.client.config import config_file
from deadline.job_attachments.asset_manifests.decode import decode_manifest
from deadline.job_attachments.asset_manifests.hash_algorithms import hash_data
from deadline.job_attachments.caches.hash_cache import HashCache
from deadline.job_attachments.models import (
    FileStatus,
    GlobConfig,
    ManifestDiff,
    ManifestSnapshot,
    default_glob_all,
)
from deadline.job_attachments.upload import S3AssetManager

"""
APIs here should be business logic only. It should perform one thing, and one thing well. 
It should use basic primitives like S3 upload, download, boto3 APIs.
These APIs should be boto3 session agnostic and a specific Boto3 Credential to use.
"""


def _glob_files(
    root: str,
    include: Optional[List[str]] = None,
    exclude: Optional[List[str]] = None,
    include_exclude_config: Optional[str] = None,
) -> List[str]:
    """
    :param include: Include glob to look for files to add to the manifest.
    :param exclude: Exclude glob to exclude files from the manifest.
    :param include_exclude_config: Config JSON or file containeing input and exclude config.
    :returns: All files matching the include and exclude expressions.
    """

    # Get all files in the root.
    glob_config: GlobConfig
    if include or exclude:
        include = include if include is not None else default_glob_all()
        exclude = exclude if exclude is not None else []
        glob_config = GlobConfig(include_glob=include, exclude_glob=exclude)
    elif include_exclude_config:
        glob_config = _process_glob_inputs(include_exclude_config)
    else:
        # Default, include all.
        glob_config = GlobConfig()

    input_files = _glob_paths(
        root, include=glob_config.include_glob, exclude=glob_config.exclude_glob
    )
    return input_files


def _manifest_snapshot(
    root: str,
    destination: str,
    name: str,
    include: Optional[List[str]] = None,
    exclude: Optional[List[str]] = None,
    include_exclude_config: Optional[str] = None,
    diff: Optional[str] = None,
    force_rehash: bool = False,
    logger: ClickLogger = ClickLogger(False),
) -> Optional[ManifestSnapshot]:

    # Get all files in the root.
    glob_config: GlobConfig
    if include or exclude:
        include = include if include is not None else default_glob_all()
        exclude = exclude if exclude is not None else []
        glob_config = GlobConfig(include_glob=include, exclude_glob=exclude)
    elif include_exclude_config:
        glob_config = _process_glob_inputs(include_exclude_config)
    else:
        # Default, include all.
        glob_config = GlobConfig()

    current_files = _glob_paths(
        root, include=glob_config.include_glob, exclude=glob_config.exclude_glob
    )

    # Compute the output manifest immediately and hash.
    if not diff:
        output_manifest = _create_manifest_for_single_root(
            files=current_files, root=root, logger=logger
        )
        if not output_manifest:
            return None

    # If this is a diff manifest, load the supplied manifest file.
    else:
        # Parse local manifest
        with open(diff) as source_diff:
            source_manifest_str = source_diff.read()
            source_manifest = decode_manifest(source_manifest_str)

        # Get the differences
        changed_paths: List[str] = []

        # Fast comparison using time stamps and sizes.
        if not force_rehash:
            diff_list: List[Tuple[str, FileStatus]] = _fast_file_list_to_manifest_diff(
                root=root,
                current_files=current_files,
                diff_manifest=source_manifest,
                logger=logger,
                return_root_relative_path=False,
            )
            for diff_file in diff_list:
                # Add all new and modified
                if diff_file[1] != FileStatus.DELETED:
                    changed_paths.append(diff_file[0])
        else:
            # In "slow / thorough" mode, we check by hash, which is definitive.
            output_manifest = _create_manifest_for_single_root(
                files=current_files, root=root, logger=logger
            )
            if not output_manifest:
                return None
            differences: List[Tuple[FileStatus, BaseManifestPath]] = compare_manifest(
                source_manifest, output_manifest
            )
            for diff_item in differences:
                if diff_item[0] == FileStatus.MODIFIED or diff_item[0] == FileStatus.NEW:
                    full_diff_path = f"{root}/{diff_item[1].path}"
                    changed_paths.append(full_diff_path)
                    logger.echo(f"Found difference at: {full_diff_path}, Status: {diff_item[0]}")

        # If there were no files diffed, return None, there was nothing to snapshot.
        if len(changed_paths) == 0:
            return None

        # Since the files are already hashed, we can easily re-use has_attachments to remake a diff manifest.
        output_manifest = _create_manifest_for_single_root(
            files=changed_paths, root=root, logger=logger
        )
        if not output_manifest:
            return None

    # Write created manifest into local file, at the specified location at destination
    if output_manifest is not None:
        # Encode the root path as
        root_hash: str = hash_data(root.encode("utf-8"), output_manifest.get_default_hash_alg())
        timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
        manifest_name = name if name else root.replace("/", "_")
        manifest_name = manifest_name[1:] if manifest_name[0] == "_" else manifest_name
        manifest_name = f"{manifest_name}-{root_hash}-{timestamp}.manifest"

        local_manifest_file = os.path.join(destination, manifest_name)
        os.makedirs(os.path.dirname(local_manifest_file), exist_ok=True)
        with open(local_manifest_file, "w") as file:
            file.write(output_manifest.encode())

        # Output results.
        logger.echo(f"Manifest Generated at {local_manifest_file}\n")
        return ManifestSnapshot(manifest=local_manifest_file)
    else:
        # No manifest generated.
        logger.echo("No manifest generated")
        return None


def _manifest_diff(
    manifest: str,
    root: str,
    include: Optional[List[str]] = None,
    exclude: Optional[List[str]] = None,
    include_exclude_config: Optional[str] = None,
    force_rehash=False,
    logger: ClickLogger = ClickLogger(False),
) -> ManifestDiff:
    """
    BETA API - This API is still evolving but will be made public in the near future.
    API to diff a manifest root with a previously snapshotted manifest.
    :param manifest: Manifest file path to compare against.
    :param root: Root directory to generate the manifest fileset.
    :param include: Include glob to look for files to add to the manifest.
    :param exclude: Exclude glob to exclude files from the manifest.
    :param include_exclude_config: Config JSON or file containeing input and exclude config.
    :param logger: Click Logger instance to print to CLI as test or JSON.
    :returns: ManifestDiff object containing all new changed, deleted files.
    """

    # Find all files matching our regex
    input_files = _glob_files(
        root=root, include=include, exclude=exclude, include_exclude_config=include_exclude_config
    )
    input_paths = [Path(p) for p in input_files]

    # Placeholder Asset Manager
    asset_manager = S3AssetManager()

    # parse the given manifest to compare against.
    local_manifest_object: BaseAssetManifest
    with open(manifest) as input_file:
        manifest_data_str = input_file.read()
        local_manifest_object = decode_manifest(manifest_data_str)

    output: ManifestDiff = ManifestDiff()

    # Helper function to update output datastructure.
    def process_output(status: FileStatus, path: str, output_diff: ManifestDiff):
        if status == FileStatus.MODIFIED:
            output_diff.modified.append(path)
        elif status == FileStatus.NEW:
            output_diff.new.append(path)
        elif status == FileStatus.DELETED:
            output_diff.deleted.append(path)

    if force_rehash:
        # hash and create manifest of local directory
        cache_config = config_file.get_cache_directory()
        with HashCache(cache_config) as hash_cache:
            directory_manifest_object = asset_manager._create_manifest_file(
                input_paths=input_paths, root_path=root, hash_cache=hash_cache
            )

        # Hash based compare manifests.
        differences: List[Tuple[FileStatus, BaseManifestPath]] = compare_manifest(
            reference_manifest=local_manifest_object, compare_manifest=directory_manifest_object
        )
        # Map to output datastructure.
        for item in differences:
            process_output(item[0], item[1].path, output)

    else:
        # File based comparisons.
        fast_diff: List[Tuple[str, FileStatus]] = _fast_file_list_to_manifest_diff(
            root=root, current_files=input_files, diff_manifest=local_manifest_object, logger=logger
        )
        for fast_diff_item in fast_diff:
            process_output(fast_diff_item[1], fast_diff_item[0], output)

    return output
