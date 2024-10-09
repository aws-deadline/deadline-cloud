# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import os
import boto3
import json

from contextlib import ExitStack
from typing import Optional, List, Dict

from deadline.job_attachments.asset_manifests.base_manifest import BaseAssetManifest
from deadline.job_attachments.asset_manifests.decode import decode_manifest
from deadline.job_attachments.download import download_files_from_manifests
from deadline.job_attachments.models import JobAttachmentS3Settings, PathMappingRule
from deadline.job_attachments.progress_tracker import DownloadSummaryStatistics
from deadline.client.exceptions import NonValidInputError


def attachment_download(
    manifests: List[str],
    s3_root_path: str,
    boto3_session: boto3.Session,
    path_mapping_rules: Optional[str] = None,
):
    """
    BETA API - This API is still evolving but will be made public in the near future.

    API to download job attachments based on given list of manifests.
    If path mapping rules file is given, map to corresponding destinations.

    Args:
        manifests: File Path to the manifest file for upload.
        s3_root_path: S3 root path including bucket name and root prefix.
        boto_session: Boto3 session for interacting with customer s3.
        path_mapping_rules: Optional file path to a list of path mapping.
    """

    # path validation
    if not all([os.path.isfile(manifest) for manifest in manifests]):
        raise NonValidInputError(f"Specified manifests {manifests} contain invalid file.")

    parsed_mappings: List[PathMappingRule] = list()

    if path_mapping_rules:
        if not os.path.isfile(path_mapping_rules):
            raise NonValidInputError(
                f"Specified path mapping file {path_mapping_rules} is not valid."
            )

        with open(path_mapping_rules, encoding="utf8") as f:
            parsed_mappings = [PathMappingRule(**mapping) for mapping in json.load(f)]

    # Read in manifests
    merged_manifests_by_root: Dict[str, BaseAssetManifest] = dict()
    with ExitStack() as stack:
        for file_path in manifests:
            manifest: BaseAssetManifest = decode_manifest(
                stack.enter_context(open(file_path)).read()
            )

            # File name is supposed to be a hash of source path in path mapping, use that to determine destination
            # If it doesn't appear in path mapping or mapping doesn't exist, download to current directory instead
            file_name: str = os.path.basename(file_path)
            destination = next(
                (
                    rule.destination_path
                    for rule in parsed_mappings
                    if rule.get_hashed_source_path(manifest.get_default_hash_alg()) == file_name
                ),
                # Write to current directory partitioned by manifest name when no path mapping defined
                f"{os.getcwd()}/{file_name}",
            )
            print(f"local root is {destination}")
            merged_manifests_by_root[destination] = manifest

    # Given manifests and S3 bucket + root, downloads all files from a CAS in each manifest.
    s3_settings: JobAttachmentS3Settings = JobAttachmentS3Settings.from_root_path(s3_root_path)
    download_summary: DownloadSummaryStatistics = download_files_from_manifests(
        s3_bucket=s3_settings.s3BucketName,
        manifests_by_root=merged_manifests_by_root,
        cas_prefix=s3_settings.full_cas_prefix(),
        session=boto3_session,
    )
    print(f"Download summary \n{download_summary}")
