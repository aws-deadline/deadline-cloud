# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from typing import List, Optional

from deadline.client.api._job_attachment import _hash_attachments
from deadline.client.cli._common import _ProgressBarCallbackManager
from deadline.client.cli._groups.click_logger import ClickLogger
from deadline.job_attachments.asset_manifests.base_manifest import BaseAssetManifest
from deadline.job_attachments.exceptions import ManifestCreationException
from deadline.job_attachments.upload import S3AssetManager


def _create_manifest_for_single_root(
    files: List[str],
    root: str,
    logger: ClickLogger,
) -> Optional[BaseAssetManifest]:
    """
    Shared logic to create a manifest file from a single root.
    :param files: Input files to create a manifest with.
    :param root: Asset root of the files.
    :param logger: Click logger for stdout.
    :return
    """
    # Placeholder Asset Manager
    asset_manager = S3AssetManager()

    hash_callback_manager = _ProgressBarCallbackManager(length=100, label="Hashing Attachments")

    upload_group = asset_manager.prepare_paths_for_upload(
        input_paths=files, output_paths=[root], referenced_paths=[]
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
            hashing_progress_callback=(
                hash_callback_manager.callback if not logger.is_json() else None
            ),
        )

    if not manifests or len(manifests) == 0:
        logger.echo("No manifest generated")
        return None
    else:
        # This is a hard failure, we are snapshotting 1 directory.
        assert len(manifests) == 1

        output_manifest = manifests[0].asset_manifest
        if output_manifest is None:
            raise ManifestCreationException()

        # Return the generated manifest.
        return output_manifest
