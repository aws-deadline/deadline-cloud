# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import datetime
import os
from typing import List, Optional, Tuple


from deadline.client.api._job_attachment import _hash_attachments
from deadline.client.cli._common import _ProgressBarCallbackManager
from deadline.client.cli._groups.click_logger import ClickLogger
from deadline.job_attachments._diff import compare_manifest
from deadline.job_attachments._glob import _process_glob_inputs, _glob_paths
from deadline.job_attachments.asset_manifests.base_manifest import (
    BaseManifestPath,
)
from deadline.job_attachments.asset_manifests.decode import decode_manifest
from deadline.job_attachments.exceptions import ManifestCreationException
from deadline.job_attachments.models import (
    FileStatus,
    GlobConfig,
    JobAttachmentS3Settings,
    ManifestSnapshot,
    default_glob_all,
)
from deadline.job_attachments.upload import S3AssetManager

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

    inputs = _glob_paths(root, include=glob_config.include_glob, exclude=glob_config.exclude_glob)

    # Placeholder Asset Manager
    asset_manager = S3AssetManager(
        farm_id=" ", queue_id=" ", job_attachment_settings=JobAttachmentS3Settings(" ", " ")
    )

    hash_callback_manager = _ProgressBarCallbackManager(length=100, label="Hashing Attachments")

    upload_group = asset_manager.prepare_paths_for_upload(
        input_paths=inputs, output_paths=[root], referenced_paths=[]
    )
    # We only provided 1 root path, so output should only have 1 group.
    assert len(upload_group.asset_groups) == 1

    if upload_group.asset_groups:
        _, manifests = _hash_attachments(
            asset_manager=asset_manager,
            asset_groups=upload_group.asset_groups,
            total_input_files=upload_group.total_input_files,
            total_input_bytes=upload_group.total_input_bytes,
            print_function_callback=logger.echo,
            hashing_progress_callback=hash_callback_manager.callback,
        )

    if not manifests or len(manifests) == 0:
        logger.echo("No manifest generated")
        return None

    # This is a hard failure, we are snapshotting 1 directory.
    assert len(manifests) == 1
    output_manifest = manifests[0].asset_manifest
    if output_manifest is None:
        raise ManifestCreationException()

    # If this is a diff manifest, load the supplied manifest file.
    if diff:
        # Parse local manifest
        with open(diff) as source_diff:
            source_manifest_str = source_diff.read()
            source_manifest = decode_manifest(source_manifest_str)

        # Get the differences
        changed_paths: List[str] = []
        differences: List[Tuple[FileStatus, BaseManifestPath]] = compare_manifest(
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
        _, diff_manifests = _hash_attachments(
            asset_manager=asset_manager,
            asset_groups=diff_group.asset_groups,
            total_input_files=diff_group.total_input_files,
            total_input_bytes=diff_group.total_input_bytes,
            print_function_callback=logger.echo,
            hashing_progress_callback=hash_callback_manager.callback,
        )
        output_manifest = diff_manifests[0].asset_manifest

    # Write created manifest into local file, at the specified location at destination
    if output_manifest is not None:

        timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
        manifest_name = name if name else root.replace("/", "_")
        manifest_name = manifest_name[1:] if manifest_name[0] == "_" else manifest_name
        manifest_name = f"{manifest_name}-{timestamp}.manifest"

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
