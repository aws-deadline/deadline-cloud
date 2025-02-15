# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import os
import boto3
import json

from typing import Optional, List, Dict
from pathlib import Path
from dataclasses import asdict

from deadline.job_attachments.api._utils import _read_manifests
from deadline.job_attachments.asset_manifests.base_manifest import BaseAssetManifest
from deadline.job_attachments.download import download_files_from_manifests
from deadline.job_attachments.models import JobAttachmentS3Settings, PathMappingRule
from deadline.job_attachments.progress_tracker import DownloadSummaryStatistics
from deadline.job_attachments.upload import S3AssetUploader
from deadline.client.cli._groups.click_logger import ClickLogger
from deadline.client.config import config_file
from deadline.client.exceptions import NonValidInputError


def attachment_download(
    manifests: List[str],
    s3_root_uri: str,
    boto3_session: boto3.Session,
    path_mapping_rules: Optional[str] = None,
    logger: ClickLogger = ClickLogger(False),
):
    """
    BETA API - This API is still evolving.

    API to download job attachments based on given list of manifests.
    If path mapping rules file is given, map to corresponding destinations.

    Args:
        manifests (List[str]): File Path to the manifest file for upload.
        s3_root_uri (str): S3 root uri including bucket name and root prefix.
        boto3_session (boto3.Session): Boto3 session for interacting with customer s3.
        path_mapping_rules (Optional[str], optional): Optional file path to a JSON file contains list of path mapping. Defaults to None.
        logger (ClickLogger, optional): Logger to provide visibility. Defaults to ClickLogger(False).

    Raises:
        NonValidInputError: raise when any of the input is not valid.
    """

    file_name_manifest_dict: Dict[str, BaseAssetManifest] = _read_manifests(
        manifest_paths=manifests
    )
    path_mapping_rule_list: List[PathMappingRule] = _process_path_mapping(
        path_mapping_rules=path_mapping_rules
    )

    merged_manifests_by_root: Dict[str, BaseAssetManifest] = dict()
    for file_name in file_name_manifest_dict:
        manifest: BaseAssetManifest = file_name_manifest_dict[file_name]
        # File name is supposed to be prefixed by a hash of source path in path mapping, use that to determine destination
        # If it doesn't appear in path mapping or mapping doesn't exist, download to current directory instead
        destination = next(
            (
                rule.destination_path
                for rule in path_mapping_rule_list
                if rule.get_hashed_source_path(manifest.get_default_hash_alg()) in file_name
            ),
            # Write to current directory partitioned by manifest name when no path mapping defined
            f"{os.getcwd()}/{file_name}",
        )
        # Assuming the manifest is already aggregated and correspond to a single destination
        if merged_manifests_by_root.get(destination):
            raise NonValidInputError(
                f"{destination} is already in use, one desination path maps to one manifest file only."
            )

        merged_manifests_by_root[destination] = manifest

    # Given manifests and S3 bucket + root, downloads all files from a CAS in each manifest.
    s3_settings: JobAttachmentS3Settings = JobAttachmentS3Settings.from_s3_root_uri(s3_root_uri)
    download_summary: DownloadSummaryStatistics = download_files_from_manifests(
        s3_bucket=s3_settings.s3BucketName,
        manifests_by_root=merged_manifests_by_root,
        cas_prefix=s3_settings.full_cas_prefix(),
        session=boto3_session,
    )
    logger.echo(download_summary)
    logger.json(asdict(download_summary.convert_to_summary_statistics()))


def attachment_upload(
    manifests: List[str],
    s3_root_uri: str,
    boto3_session: boto3.Session,
    root_dirs: List[str] = [],
    path_mapping_rules: Optional[str] = None,
    upload_manifest_path: Optional[str] = None,
    logger: ClickLogger = ClickLogger(False),
):
    """
    BETA API - This API is still evolving.

    API to upload job attachments based on given list of manifests and corresponding file directories.
    If path mapping rules file is given, map to corresponding destinations.

    Args:
        manifests (List[str]): File Path to the manifest file for upload.
        s3_root_uri (str): S3 root uri including bucket name and root prefix.
        boto3_session (boto3.Session): Boto3 session for interacting with customer s3.
        root_dirs (List[str]): List of root directories holding attachments. Defaults to empty.
        path_mapping_rules (Optional[str], optional): Optional file path to a JSON file contains list of path mapping. Defaults to None.
        upload_manifest_path (Optional[str], optional): Optional path prefix for uploading given manifests. Defaults to None.
        logger (ClickLogger, optional): Logger to provide visibility. Defaults to ClickLogger(False).

    Raises:
        NonValidInputError: raise when any of the input is not valid.
    """

    file_name_manifest_dict: Dict[str, BaseAssetManifest] = _read_manifests(
        manifest_paths=manifests
    )

    if bool(path_mapping_rules) == bool(root_dirs):
        raise NonValidInputError("One of path mapping rule and root dir must exist, and not both.")

    path_mapping_rule_list: List[PathMappingRule] = _process_path_mapping(
        path_mapping_rules=path_mapping_rules, root_dirs=root_dirs
    )

    s3_settings: JobAttachmentS3Settings = JobAttachmentS3Settings.from_s3_root_uri(s3_root_uri)
    asset_uploader: S3AssetUploader = S3AssetUploader(session=boto3_session)
    for file_name in file_name_manifest_dict:
        manifest: BaseAssetManifest = file_name_manifest_dict[file_name]

        # File name is supposed to be prefixed by a hash of source path in path mapping or provided root dirs
        rule: Optional[PathMappingRule] = next(
            # search in path mapping to determine source and destination
            (
                rule
                for rule in path_mapping_rule_list
                if rule.get_hashed_source_path(manifest.get_default_hash_alg()) in file_name
            ),
            None,
        )
        if not rule:
            raise NonValidInputError(
                f"No valid root defined for given manifest {file_name}, please check input root dirs and path mapping rule."
            )

        metadata = {"Metadata": {"asset-root": json.dumps(rule.source_path, ensure_ascii=True)}}
        # S3 metadata must be ASCII, so use either 'asset-root' or 'asset-root-json' depending
        # on whether the value is ASCII.
        try:
            # Add the 'asset-root' metadata if the path is ASCII
            rule.source_path.encode(encoding="ascii")
            metadata["Metadata"]["asset-root"] = rule.source_path
        except UnicodeEncodeError:
            # Add the 'asset-root-json' metadata encoded to ASCII as a JSON string
            metadata["Metadata"]["asset-root-json"] = json.dumps(
                rule.source_path, ensure_ascii=True
            )
        if rule.source_path_format:
            metadata["Metadata"]["file-system-location-name"] = rule.source_path_format

        # Uploads all files to a CAS in the manifest, optionally upload manifest file
        key, data = asset_uploader.upload_assets(
            job_attachment_settings=s3_settings,
            manifest=manifest,
            partial_manifest_prefix=upload_manifest_path,
            manifest_file_name=file_name,
            manifest_metadata=metadata,
            source_root=Path(rule.source_path),
            asset_root=Path(rule.destination_path),
            s3_check_cache_dir=config_file.get_cache_directory(),
        )
        logger.echo(
            f"Uploaded assets from {rule.destination_path}, to {s3_settings.to_s3_root_uri()}/Manifests/{key}, hashed data {data}"
        )


def _process_path_mapping(
    path_mapping_rules: Optional[str] = None, root_dirs: List[str] = []
) -> List[PathMappingRule]:
    """
    Process list of path mapping rules from the input path mapping file or root directories.

    Args:
        path_mapping_rules (Optional[str], optional): File path to path mapping rules. Defaults to None.
        root_dirs (List[str], optional): List of root directories path. Defaults to [].

    Raises:
        NonValidInputError: Raise if any of the path mapping rule file or root dirs are not valid.

    Returns:
        List[PathMappingRule]: List of processed PathMappingRule
    """

    path_mapping_rule_list: List[PathMappingRule] = list()

    if path_mapping_rules:
        if not os.path.isfile(path_mapping_rules):
            raise NonValidInputError(
                f"Specified path mapping file {path_mapping_rules} is not valid."
            )
        with open(path_mapping_rules, encoding="utf8") as f:
            data = json.load(f)
            if "path_mapping_rules" in data:
                data = data["path_mapping_rules"]

            assert isinstance(data, list), "Path mapping rules have to be a list of dict."
            path_mapping_rule_list.extend([PathMappingRule(**mapping) for mapping in data])

    if nonvalid_dirs := [root for root in root_dirs if not os.path.isdir(root)]:
        raise NonValidInputError(f"Specified root dir {nonvalid_dirs} are not valid.")

    path_mapping_rule_list.extend(
        PathMappingRule(source_path_format="", source_path=root, destination_path=root)
        for root in root_dirs
    )

    return path_mapping_rule_list
