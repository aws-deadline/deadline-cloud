# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
import concurrent.futures

import os
from pathlib import Path
from typing import Dict, List, Tuple
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
