# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import os

from contextlib import ExitStack
from typing import List, Dict

from deadline.client.exceptions import NonValidInputError
from deadline.job_attachments.asset_manifests.base_manifest import BaseAssetManifest
from deadline.job_attachments.asset_manifests.decode import decode_manifest


def _read_manifests(manifest_paths: List[str]) -> Dict[str, BaseAssetManifest]:
    """
    Read in manfiests from the given file path list, and produce file name to manifest mapping.

    Args:
        manifest_paths (List[str]): List of file paths to manifest file.

    Raises:
        NonValidInputError: Raise when any of the file is not valid.

    Returns:
        Dict[str, BaseAssetManifest]: File name to encoded manifest mapping
    """

    if nonvalid_files := [manifest for manifest in manifest_paths if not os.path.isfile(manifest)]:
        raise NonValidInputError(f"Specified manifests {nonvalid_files} are not valid.")

    with ExitStack() as stack:
        file_name_manifest_dict: Dict[str, BaseAssetManifest] = {
            os.path.basename(file_path): decode_manifest(
                stack.enter_context(open(file_path)).read()
            )
            for file_path in manifest_paths
        }

    return file_name_manifest_dict
