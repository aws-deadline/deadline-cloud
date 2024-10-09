# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
import concurrent.futures

import os
from pathlib import Path, PurePosixPath
from typing import Dict, List, Tuple
from deadline.client.cli._groups.click_logger import ClickLogger
from deadline.client.config import config_file
from deadline.client.exceptions import NonValidInputError
from deadline.job_attachments.asset_manifests.base_manifest import (
    BaseAssetManifest,
    BaseManifestPath,
)
from deadline.job_attachments.caches.hash_cache import HashCache
from deadline.job_attachments.models import AssetRootManifest, FileStatus
from deadline.job_attachments.upload import S3AssetManager


def diff_manifest(
    asset_manager: S3AssetManager,
    asset_root_manifest: AssetRootManifest,
    manifest: str,
    update: bool,
) -> List[(Tuple[FileStatus, BaseManifestPath])]:
    """
    Gets the file paths in specified manifest if the contents of file have changed since its last snapshot.
    Returns a list of FileStatus and BaseManifestPath
    """
    manifest_dir_name: str = os.path.basename(manifest)
    root_path: str = asset_root_manifest.root_path
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
) -> List[(Tuple[FileStatus, BaseManifestPath])]:
    """
    Checks a manifest file, compares it to specified root directory or manifest of files with the local hash cache, and finds files that match the specified statuses.
    Returns a list of tuples containing the file information, and its corresponding file status.
    """
    cache_config: str = config_file.get_cache_directory()

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
) -> List[(Tuple[FileStatus, BaseManifestPath])]:
    """
    Compares two manifests, reference_manifest acting as the base, and compare_manifest acting as manifest with changes.
    Returns a list of FileStatus and BaseManifestPath
    """
    reference_dict: Dict[str, BaseManifestPath] = {
        manifest_path.path: manifest_path for manifest_path in reference_manifest.paths
    }
    compare_dict: Dict[str, BaseManifestPath] = {
        manifest_path.path: manifest_path for manifest_path in compare_manifest.paths
    }

    differences: List[(Tuple[FileStatus, BaseManifestPath])] = []

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


def _fast_file_list_to_manifest_diff(
    root: str, current_files: List[str], diff_manifest: BaseAssetManifest, logger: ClickLogger
) -> List[str]:
    """
    Perform a fast difference of the current list of files to a previous manifest to diff against using time stamps and file sizes.
    :param root: Root folder of files to diff against.
    :param current_files: List of files to compare with.
    :param diff_manifest: Manifest containing files to diff against.
    :return List[str]: List of files that are new, or modified.
    """
    changed_paths: List[str] = []
    input_files_map: Dict[str, BaseManifestPath] = {}
    for input_file in diff_manifest.paths:
        # Normalize paths so we can compare different OSes
        normalized_path = os.path.normpath(input_file.path)
        input_files_map[normalized_path] = input_file

    # Iterate for each file that we found in glob.
    for local_file in current_files:
        # Get the file's time stamp and size. We want to compare both.
        # From enabling CRT, sometimes timestamp update can fail.
        local_file_path = Path(local_file)
        file_stat = local_file_path.stat()

        # Compare the glob against the relative path we store in the manifest.
        root_relative_path = str(PurePosixPath(*local_file_path.relative_to(root).parts))
        if root_relative_path not in input_files_map:
            # This is a new file
            logger.echo(f"Found difference at: {root_relative_path}, Status: FileStatus.NEW")
            changed_paths.append(local_file)
        else:
            # This is a modified file, compare with manifest relative timestamp.
            input_file = input_files_map[root_relative_path]
            # Check file size first as it is easier to test. Usually modified files will also have size diff.
            if file_stat.st_size != input_file.size:
                changed_paths.append(local_file)
                logger.echo(
                    f"Found size difference at: {root_relative_path}, Status: FileStatus.MODIFIED"
                )
            elif int(file_stat.st_mtime_ns // 1000) != input_file.mtime:
                changed_paths.append(local_file)
                logger.echo(
                    f"Found time difference at: {root_relative_path}, Status: FileStatus.MODIFIED"
                )
    return changed_paths
