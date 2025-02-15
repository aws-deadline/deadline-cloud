# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
import concurrent.futures

import logging
import os
from pathlib import Path, PurePosixPath
from typing import Dict, List, Tuple
from math import trunc
from deadline.client.cli._groups.click_logger import ClickLogger
from deadline.client.config import config_file
from deadline.client.exceptions import NonValidInputError
from deadline.job_attachments.asset_manifests.base_manifest import (
    BaseAssetManifest,
    BaseManifestPath,
)
from deadline.job_attachments.caches.hash_cache import HashCache
from deadline.job_attachments.models import AssetRootManifest, FileStatus, ManifestDiff
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
    root: str,
    current_files: List[str],
    diff_manifest: BaseAssetManifest,
    logger: ClickLogger,
    return_root_relative_path: bool = True,
) -> List[Tuple[str, FileStatus]]:
    """
    Perform a fast difference of the current list of files to a previous manifest to diff against using time stamps and file sizes.
    :param root: Root folder of files to diff against.
    :param current_files: List of files to compare with.
    :param diff_manifest: Manifest containing files to diff against.
    :param return_root_relative_path: File Path to return, either relative to root or full.
    :param logger: logger.
    :return List[Tuple[str, FileStatus]]: List of Tuple containing the file path and FileStatus pair.
    """

    # Select either relative or absolut path for results.
    def select_path(full_path: str, relative_path: str, return_root_relative_path: bool):
        return relative_path if return_root_relative_path else full_path

    changed_paths: List[Tuple[str, FileStatus]] = []
    input_files_map: Dict[str, BaseManifestPath] = {}
    for input_file in diff_manifest.paths:
        # Normalize paths so we can compare different OSes
        normalized_path = Path(os.path.normpath(input_file.path)).as_posix()
        input_files_map[normalized_path] = input_file

    # Iterate for each file that we found in glob.
    root_relative_paths: List[str] = []
    for local_file in current_files:
        # Get the file's time stamp and size. We want to compare both.
        # From enabling CRT, sometimes timestamp update can fail.
        local_file_path = Path(local_file)
        file_stat = local_file_path.stat()

        # Compare the glob against the relative path we store in the manifest.
        # Save it to a list so we can look for deleted files.
        root_relative_path = str(PurePosixPath(*local_file_path.relative_to(root).parts).as_posix())
        root_relative_paths.append(root_relative_path)

        return_path = select_path(
            full_path=local_file,
            relative_path=root_relative_path,
            return_root_relative_path=return_root_relative_path,
        )
        if root_relative_path not in input_files_map:
            # This is a new file
            logger.echo(f"Found difference at: {root_relative_path}, Status: FileStatus.NEW")
            changed_paths.append((return_path, FileStatus.NEW))
        else:
            # This is a modified file, compare with manifest relative timestamp.
            input_file = input_files_map[root_relative_path]
            # Check file size first as it is easier to test. Usually modified files will also have size diff.
            if file_stat.st_size != input_file.size:
                changed_paths.append((return_path, FileStatus.MODIFIED))
                logger.echo(
                    f"Found size difference at: {root_relative_path}, Status: FileStatus.MODIFIED"
                )
            # Check file mtime, allow 1 microsecond diff to prevent false positive
            # utime set from microsecond to nanosecond conversion could create 1 microsecond diff upon division
            elif abs(trunc(file_stat.st_mtime_ns / 1000) - input_file.mtime) > 1:
                changed_paths.append((return_path, FileStatus.MODIFIED))
                logger.echo(
                    f"Found time difference at: {root_relative_path}, Status: FileStatus.MODIFIED"
                )

    # Find deleted files. Manifest store files in relative form.
    for manifest_file_path in diff_manifest.paths:
        if manifest_file_path.path not in root_relative_paths:
            full_path = os.path.join(root, manifest_file_path.path)
            return_path = select_path(
                full_path=full_path,
                relative_path=manifest_file_path.path,
                return_root_relative_path=return_root_relative_path,
            )
            changed_paths.append((return_path, FileStatus.DELETED))
    return changed_paths


def pretty_print_cli(root: str, all_files: List[str], manifest_diff: ManifestDiff):
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

    def get_file_status(file: str, manifest_diff: ManifestDiff):
        print(file)
        if file in manifest_diff.new:
            return FileStatus.NEW
        elif file in manifest_diff.modified:
            return FileStatus.MODIFIED
        elif file in manifest_diff.deleted:
            return FileStatus.DELETED
        else:
            # Default, not in any diff list.
            return FileStatus.UNCHANGED

    def build_directory_tree(all_files: List[str]) -> Dict[str, dict]:
        directory_tree: dict = {}

        def add_to_tree(path, status):
            parts = str(path).split(os.path.sep)
            current_level = directory_tree
            for i, part in enumerate(parts):
                if i == len(parts) - 1:
                    current_level[part] = status
                else:
                    current_level = current_level.setdefault(part, {})

        for file in all_files:
            print(f"{file} {root}")
            relative_path = str(Path(file).relative_to(root))
            add_to_tree(
                relative_path,
                get_file_status(relative_path, manifest_diff),
            )
        return directory_tree

    directory_tree = build_directory_tree(all_files)
    print_tree(directory_tree)
    logger.info("")
