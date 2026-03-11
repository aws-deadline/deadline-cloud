# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import textwrap

from typing import Any, Optional, List, Callable, Tuple

from deadline.job_attachments.models import (
    AssetRootGroup,
    AssetRootManifest,
)
from deadline.job_attachments.progress_tracker import (
    ProgressReportMetadata,
    ProgressStatus,
)
from deadline.job_attachments.upload import S3AssetManager, SummaryStatistics


def _hash_attachments(
    *,
    asset_manager: S3AssetManager,
    asset_groups: List[AssetRootGroup],
    total_input_files: int,
    total_input_bytes: int,
    print_function_callback: Callable[[str], None] = lambda msg: None,
    hashing_progress_callback: Optional[Callable[[Any], bool]] = None,
    hash_cache_dir: Optional[str] = None,
    telemetry_callback: Optional[Callable[[SummaryStatistics], None]] = None,
) -> Tuple[SummaryStatistics, List[AssetRootManifest]]:
    """
    Starts the job attachments hashing and returns a list of the asset manifests of the hashed files.
    Provides callbacks for:
      * Printing output
      * Hashing progress reporting
      * Sending hashing telemetry
    """

    def _default_update_hash_progress(hashing_metadata: ProgressReportMetadata) -> bool:
        return True

    if not hashing_progress_callback:
        hashing_progress_callback = _default_update_hash_progress

    hashing_summary, manifests = asset_manager.hash_assets_and_create_manifest(
        asset_groups=asset_groups,
        total_input_files=total_input_files,
        total_input_bytes=total_input_bytes,
        hash_cache_dir=hash_cache_dir,
        on_preparing_to_submit=hashing_progress_callback,
    )
    if telemetry_callback:
        telemetry_callback(hashing_summary)
    if hashing_summary.total_files > 0:
        print_function_callback("Hashing Summary:")
        print_function_callback(textwrap.indent(str(hashing_summary), "    "))
    else:
        # Ensure to call the callback once if no files were processed
        hashing_progress_callback(
            ProgressReportMetadata(
                status=ProgressStatus.PREPARING_IN_PROGRESS,
                progress=100,
                transferRate=0,
                progressMessage="No files to hash",
                processedFiles=0,
            )
        )

    return hashing_summary, manifests
