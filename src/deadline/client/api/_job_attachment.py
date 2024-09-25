# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from deadline.client import api
from deadline.client.config import config_file
from deadline.job_attachments.models import AssetRootGroup, AssetRootManifest
from deadline.job_attachments.upload import S3AssetManager, SummaryStatistics


import textwrap
from configparser import ConfigParser
from typing import Callable, Dict, List, Optional, Tuple


def _hash_attachments(
    asset_manager: S3AssetManager,
    asset_groups: List[AssetRootGroup],
    total_input_files: int,
    total_input_bytes: int,
    print_function_callback: Callable = lambda msg: None,
    hashing_progress_callback: Optional[Callable] = None,
    config: Optional[ConfigParser] = None,
) -> Tuple[SummaryStatistics, List[AssetRootManifest]]:
    """
    Starts the job attachments hashing and handles the progress reporting
    callback. Returns a list of the asset manifests of the hashed files.
    """

    def _default_update_hash_progress(hashing_metadata: Dict[str, str]) -> bool:
        return True

    if not hashing_progress_callback:
        hashing_progress_callback = _default_update_hash_progress

    hashing_summary, manifests = asset_manager.hash_assets_and_create_manifest(
        asset_groups=asset_groups,
        total_input_files=total_input_files,
        total_input_bytes=total_input_bytes,
        hash_cache_dir=config_file.get_cache_directory(),
        on_preparing_to_submit=hashing_progress_callback,
    )
    api.get_deadline_cloud_library_telemetry_client(config=config).record_hashing_summary(
        hashing_summary
    )
    print_function_callback("Hashing Summary:")
    print_function_callback(textwrap.indent(str(hashing_summary), "    "))

    return hashing_summary, manifests
