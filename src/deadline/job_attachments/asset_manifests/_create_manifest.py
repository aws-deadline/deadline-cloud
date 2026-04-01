# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from typing import Any, Callable, List, Optional

from deadline.job_attachments.asset_manifests.base_manifest import BaseAssetManifest
from deadline.job_attachments.progress_tracker import ProgressReportMetadata
from deadline.job_attachments.upload import S3AssetManager, SummaryStatistics

from ...job_attachments.api._hashing import _hash_attachments


def _create_manifest_for_single_root(
    *,
    files: List[str],
    root: str,
    print_function_callback: Callable[[Any], None] = lambda msg: None,
    hashing_progress_callback: Optional[Callable[[ProgressReportMetadata], bool]] = None,
    telemetry_callback: Optional[Callable[[SummaryStatistics], None]] = None,
    hash_cache_dir: Optional[str] = None,
) -> Optional[BaseAssetManifest]:
    """
    Shared logic to create a manifest file from a single root.
    :param files: Input files to create a manifest with.
    :param root: Asset root of the files.
    :param print_function_callback: Callback for printing status messages.
    :param hashing_progress_callback: Optional callback for hashing progress updates.
    :param telemetry_callback: Optional callback for hashing telemetry reporting.
    :param hash_cache_dir: Optional directory for the hash cache.
    :return: The generated manifest, or None if no manifest was generated.
    """
    asset_manager = S3AssetManager()

    upload_group = asset_manager.prepare_paths_for_upload(
        input_paths=files, output_paths=[root], referenced_paths=[]
    )
    # We only provided 1 root path, so output should only have 1 group.
    assert len(upload_group.asset_groups) == 1

    manifests = None
    if upload_group.asset_groups:
        _, manifests = _hash_attachments(
            asset_manager=asset_manager,
            asset_groups=upload_group.asset_groups,
            total_input_files=upload_group.total_input_files,
            total_input_bytes=upload_group.total_input_bytes,
            print_function_callback=print_function_callback,
            hashing_progress_callback=hashing_progress_callback,
            hash_cache_dir=hash_cache_dir,
            telemetry_callback=telemetry_callback,
        )

    if not manifests or len(manifests) == 0:
        print_function_callback("No manifest generated")
        return None
    else:
        # This is a hard failure, we are snapshotting 1 directory.
        assert len(manifests) == 1

        # Return the generated manifest.
        return manifests[0].asset_manifest
