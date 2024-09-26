# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
tests the deadline.client.api functions relating to storage profiles
"""

from unittest.mock import patch

import os
import pytest
import json

import deadline

from deadline.client import api
from deadline.client.api._attachment import attachment_download
from deadline.client.exceptions import NonValidInputError
from deadline.job_attachments.exceptions import MalformedAttachmentSettingError
from deadline.job_attachments.progress_tracker import DownloadSummaryStatistics
from deadline.job_attachments.asset_manifests.decode import decode_manifest
from deadline.job_attachments.asset_manifests.base_manifest import BaseAssetManifest

PATH_MAPPING = {
    "source_path_format": "posix",
    "source_path": "/local/home/test",
    "destination_path": "/local/home/test/output",
}
PATH_MAPPING_HASH = "4ab97c97c825551aaa963888278ef9ec"

MOCK_MANIFEST_CASE = {
    "unmapped_file_name": {
        "hashAlg": "xxh128",
        "manifestVersion": "2023-03-03",
        "paths": [
            {
                "hash": "19a71beb47d7cc2d654ac4637e680c88",
                "mtime": 1720199667787520,
                "path": "files/file2.txt",
                "size": 14,
            }
        ],
        "totalSize": 14,
    },
    PATH_MAPPING_HASH: {
        "hashAlg": "xxh128",
        "manifestVersion": "2023-03-03",
        "paths": [
            {
                "hash": "b03f20b08a76635964ab008a10cd20a8",
                "mtime": 1720199667787520,
                "path": "files/file1.txt",
                "size": 14,
            }
        ],
        "totalSize": 14,
    },
}


def test_attachment_download_single_to_mapped(temp_assets_dir):
    with patch.object(api._session, "get_boto3_session") as session_mock:
        with open(
            os.path.join(temp_assets_dir, PATH_MAPPING_HASH),
            "w",
            encoding="utf8",
        ) as f:
            json.dump(MOCK_MANIFEST_CASE[PATH_MAPPING_HASH], f)

        mapping_file_path = os.path.join(temp_assets_dir, "mapping")
        with open(mapping_file_path, "w", encoding="utf8") as f:
            json.dump([PATH_MAPPING], f)

        with patch(
            f"{deadline.__package__}.client.api._attachment.download_files_from_manifests",
            return_value=DownloadSummaryStatistics(),
        ) as mock_download_files_from_manifests:
            attachment_download(
                manifests=[os.path.join(temp_assets_dir, PATH_MAPPING_HASH)],
                s3_root_path="bucket/assetRoot",
                boto3_session=session_mock,
                path_mapping_rules=mapping_file_path,
            )

            mock_download_files_from_manifests.assert_called_once_with(
                s3_bucket="bucket",
                manifests_by_root={
                    PATH_MAPPING["destination_path"]: decode_manifest(
                        json.dumps(MOCK_MANIFEST_CASE[PATH_MAPPING_HASH])
                    ),
                },
                cas_prefix="assetRoot/Data",
                session=session_mock,
            )


@pytest.mark.parametrize("manifest_case_key", MOCK_MANIFEST_CASE.keys())
def test_attachment_download_single_to_current(temp_assets_dir, manifest_case_key):
    with patch.object(api._session, "get_boto3_session") as session_mock:
        with open(
            os.path.join(temp_assets_dir, manifest_case_key),
            "w",
            encoding="utf8",
        ) as f:
            json.dump(MOCK_MANIFEST_CASE[manifest_case_key], f)

        with patch(
            f"{deadline.__package__}.client.api._attachment.download_files_from_manifests",
            return_value=DownloadSummaryStatistics(),
        ) as mock_download_files_from_manifests:
            attachment_download(
                manifests=[os.path.join(temp_assets_dir, manifest_case_key)],
                s3_root_path="bucket/assetRoot",
                boto3_session=session_mock,
            )

            mock_download_files_from_manifests.assert_called_once_with(
                s3_bucket="bucket",
                manifests_by_root={
                    f"{os.getcwd()}/{manifest_case_key}": decode_manifest(
                        json.dumps(MOCK_MANIFEST_CASE[manifest_case_key])
                    ),
                },
                cas_prefix="assetRoot/Data",
                session=session_mock,
            )


def test_attachment_download_multiple_to_current(temp_assets_dir):
    with patch.object(api._session, "get_boto3_session") as session_mock:
        expected_merged: dict[str, BaseAssetManifest] = dict()

        for manifest_case_key in MOCK_MANIFEST_CASE.keys():
            expected_merged[f"{os.getcwd()}/{manifest_case_key}"] = decode_manifest(
                json.dumps(MOCK_MANIFEST_CASE[manifest_case_key])
            )
            with open(
                os.path.join(temp_assets_dir, manifest_case_key),
                "w",
                encoding="utf8",
            ) as f:
                json.dump(MOCK_MANIFEST_CASE[manifest_case_key], f)

        with patch(
            f"{deadline.__package__}.client.api._attachment.download_files_from_manifests",
            return_value=DownloadSummaryStatistics(),
        ) as mock_download_files_from_manifests:
            attachment_download(
                manifests=[os.path.join(temp_assets_dir, key) for key in MOCK_MANIFEST_CASE.keys()],
                s3_root_path="bucket/assetRoot",
                boto3_session=session_mock,
            )

            mock_download_files_from_manifests.assert_called_once_with(
                s3_bucket="bucket",
                manifests_by_root=expected_merged,
                cas_prefix="assetRoot/Data",
                session=session_mock,
            )


def test_attachment_download_invalid_input_manifests(fresh_deadline_config):
    with patch.object(api._session, "get_boto3_session") as session_mock:
        with pytest.raises(NonValidInputError):
            attachment_download(
                manifests=["file-not-found"],
                s3_root_path="bucket/root",
                boto3_session=session_mock,
            )


def test_attachment_download_invalid_input_path_mapping_rules(fresh_deadline_config):
    with patch.object(api._session, "get_boto3_session") as session_mock:
        with pytest.raises(NonValidInputError):
            attachment_download(
                manifests=[],
                s3_root_path="bucket/root",
                boto3_session=session_mock,
                path_mapping_rules="file-not-found",
            )


def test_attachment_download_invalid_input_s3_root_path(fresh_deadline_config):
    with patch.object(api._session, "get_boto3_session") as session_mock:
        with pytest.raises(MalformedAttachmentSettingError):
            attachment_download(
                manifests=[],
                s3_root_path="MalformedPath",
                boto3_session=session_mock,
            )
