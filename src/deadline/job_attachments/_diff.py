# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
import concurrent.futures

import logging
import os
from pathlib import Path
from typing import List
from deadline.client.config import config_file
from deadline.client.exceptions import NonValidInputError
from deadline.job_attachments.asset_manifests.base_manifest import BaseAssetManifest
from deadline.job_attachments.caches.hash_cache import HashCache
from deadline.job_attachments.models import AssetRootManifest
from deadline.job_attachments.upload import FileStatus, S3AssetManager


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


def pretty_print_cli(file_status_list: List[(tuple)]):
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
