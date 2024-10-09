# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import datetime
import os
from typing import List, Optional, Tuple

from deadline.client.cli._groups.click_logger import ClickLogger
from deadline.job_attachments._diff import _fast_file_list_to_manifest_diff, compare_manifest
from deadline.job_attachments._glob import _process_glob_inputs, _glob_paths
from deadline.job_attachments.asset_manifests._create_manifest import (
    _create_manifest_for_single_root,
)
from deadline.job_attachments.asset_manifests.base_manifest import (
    BaseManifestPath,
)
from deadline.job_attachments.asset_manifests.decode import decode_manifest
from deadline.job_attachments.asset_manifests.hash_algorithms import hash_data
from deadline.job_attachments.models import (
    FileStatus,
    GlobConfig,
    ManifestSnapshot,
    default_glob_all,
)

"""
APIs here should be business logic only. It should perform one thing, and one thing well. 
It should use basic primitives like S3 upload, download, boto3 APIs.
These APIs should be boto3 session agnostic and a specific Boto3 Credential to use.
"""


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
            changed_paths = _fast_file_list_to_manifest_diff(
                root, current_files, source_manifest, logger
            )
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
