# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Test the deadline.client.api functions relating to attachment
"""

from unittest.mock import patch
from typing import Dict, List
from pathlib import Path

import os
import pytest
import json

import deadline

from deadline.client import api
from deadline.client.config import config_file
from deadline.client.exceptions import NonValidInputError
from deadline.job_attachments import api as attachment_api
from deadline.job_attachments.exceptions import MalformedAttachmentSettingError
from deadline.job_attachments.progress_tracker import DownloadSummaryStatistics
from deadline.job_attachments.asset_manifests import HashAlgorithm, hash_data
from deadline.job_attachments.asset_manifests.decode import decode_manifest
from deadline.job_attachments.asset_manifests.base_manifest import BaseAssetManifest
from deadline.job_attachments.api.attachment import _process_path_mapping
from deadline.job_attachments.upload import S3AssetUploader
from deadline.job_attachments.models import JobAttachmentS3Settings, PathMappingRule

PATH_MAPPING = {
    "source_path_format": "posix",
    "source_path": "/local/home/test",
    "destination_path": "/local/home/test/output",
}

OPENJD_PATH_MAPPING = {
    "version": "pathmapping-1.0",
    "path_mapping_rules": [
        {
            "source_path_format": "posix",
            "source_path": "/local/home/test",
            "destination_path": "/local/home/test/output",
        }
    ],
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

TEST_S3_URI = "s3://bucket/root"


def test_process_path_mapping(temp_assets_dir):
    mapping_file_path = os.path.join(temp_assets_dir, "mapping")
    with open(mapping_file_path, "w", encoding="utf8") as f:
        json.dump([PATH_MAPPING], f)

    path_mappings: List[PathMappingRule] = _process_path_mapping(
        mapping_file_path, [temp_assets_dir]
    )
    assert len(path_mappings) == 2


def test_process_openjd_path_mapping(temp_assets_dir):
    mapping_file_path = os.path.join(temp_assets_dir, "mapping")
    with open(mapping_file_path, "w", encoding="utf8") as f:
        json.dump(OPENJD_PATH_MAPPING, f)

    path_mappings: List[PathMappingRule] = _process_path_mapping(
        mapping_file_path, [temp_assets_dir]
    )
    assert len(path_mappings) == 2


@pytest.fixture
def session_mock():
    with patch.object(api._session, "get_boto3_session") as session_mock:
        yield session_mock


class TestAttachmentDownload:

    @pytest.fixture
    def mock_download_files_from_manifests(self):
        with patch(
            f"{deadline.__package__}.job_attachments.api.attachment.download_files_from_manifests",
            return_value=DownloadSummaryStatistics(),
        ) as mock_download_files_from_manifests:
            yield mock_download_files_from_manifests

    def test_download_single_to_mapped_invalid_path_mapping(self, temp_assets_dir, session_mock):
        with open(
            os.path.join(temp_assets_dir, PATH_MAPPING_HASH),
            "w",
            encoding="utf8",
        ) as f:
            json.dump(MOCK_MANIFEST_CASE[PATH_MAPPING_HASH], f)

        mapping_file_path = os.path.join(temp_assets_dir, "mapping")
        with open(mapping_file_path, "w", encoding="utf8") as f:
            json.dump(PATH_MAPPING, f)

        with pytest.raises(
            AssertionError,
            match="Path mapping rules have to be a list of dict.",
        ):
            attachment_api.attachment_download(
                manifests=[os.path.join(temp_assets_dir, PATH_MAPPING_HASH)],
                s3_root_uri="s3://bucket/assetRoot",
                boto3_session=session_mock,
                path_mapping_rules=mapping_file_path,
            )

    def test_download_single_to_mapped(
        self, temp_assets_dir, session_mock, mock_download_files_from_manifests
    ):
        with open(
            os.path.join(temp_assets_dir, PATH_MAPPING_HASH),
            "w",
            encoding="utf8",
        ) as f:
            json.dump(MOCK_MANIFEST_CASE[PATH_MAPPING_HASH], f)

        mapping_file_path = os.path.join(temp_assets_dir, "mapping")
        with open(mapping_file_path, "w", encoding="utf8") as f:
            json.dump([PATH_MAPPING], f)

        attachment_api.attachment_download(
            manifests=[os.path.join(temp_assets_dir, PATH_MAPPING_HASH)],
            s3_root_uri="s3://bucket/assetRoot",
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
    def test_download_single_to_current(
        self, temp_assets_dir, session_mock, mock_download_files_from_manifests, manifest_case_key
    ):
        with open(
            os.path.join(temp_assets_dir, manifest_case_key),
            "w",
            encoding="utf8",
        ) as f:
            json.dump(MOCK_MANIFEST_CASE[manifest_case_key], f)

        attachment_api.attachment_download(
            manifests=[os.path.join(temp_assets_dir, manifest_case_key)],
            s3_root_uri="s3://bucket/assetRoot",
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

    def test_download_multiple_to_current(
        self, temp_assets_dir, session_mock, mock_download_files_from_manifests
    ):
        expected_merged: Dict[str, BaseAssetManifest] = dict()

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

        attachment_api.attachment_download(
            manifests=[os.path.join(temp_assets_dir, key) for key in MOCK_MANIFEST_CASE.keys()],
            s3_root_uri="s3://bucket/assetRoot",
            boto3_session=session_mock,
        )

        mock_download_files_from_manifests.assert_called_once_with(
            s3_bucket="bucket",
            manifests_by_root=expected_merged,
            cas_prefix="assetRoot/Data",
            session=session_mock,
        )

    def test_download_invalid_input_manifests(self, session_mock):
        with pytest.raises(NonValidInputError):
            attachment_api.attachment_download(
                manifests=["file-not-found"],
                s3_root_uri=TEST_S3_URI,
                boto3_session=session_mock,
            )

    def test_download_invalid_input_path_mapping_rules(self, session_mock):
        with pytest.raises(NonValidInputError):
            attachment_api.attachment_download(
                manifests=[],
                s3_root_uri=TEST_S3_URI,
                boto3_session=session_mock,
                path_mapping_rules="file-not-found",
            )

    def test_download_invalid_input_s3_root_uri(self, session_mock):
        with pytest.raises(MalformedAttachmentSettingError):
            attachment_api.attachment_download(
                manifests=[],
                s3_root_uri="MalformedPath",
                boto3_session=session_mock,
            )


class TestAttachmentUpload:

    @pytest.fixture
    def mock_upload_assets(self):
        with patch.object(
            S3AssetUploader, "upload_assets", return_value=("key", "data")
        ) as mock_upload_assets:
            yield mock_upload_assets

    def test_upload_invalid_input_manifests(self, session_mock):
        with pytest.raises(NonValidInputError):
            attachment_api.attachment_upload(
                manifests=["file-not-found"],
                s3_root_uri=TEST_S3_URI,
                boto3_session=session_mock,
            )

    def test_upload_invalid_input_path_mapping_rules(self, session_mock):
        with pytest.raises(NonValidInputError):
            attachment_api.attachment_upload(
                manifests=[],
                s3_root_uri=TEST_S3_URI,
                boto3_session=session_mock,
                path_mapping_rules="file-not-found",
            )

    def test_upload_invalid_input_s3_root_uri(self, temp_assets_dir, session_mock):
        with pytest.raises(MalformedAttachmentSettingError):
            attachment_api.attachment_upload(
                manifests=[],
                s3_root_uri="MalformedPath",
                root_dirs=[temp_assets_dir],
                boto3_session=session_mock,
            )

    def test_upload_single_from_mapped(self, temp_assets_dir, session_mock, mock_upload_assets):
        file_name: str = f"{PATH_MAPPING_HASH}.manifest"
        with open(
            os.path.join(temp_assets_dir, file_name),
            "w",
            encoding="utf8",
        ) as f:
            json.dump(MOCK_MANIFEST_CASE[PATH_MAPPING_HASH], f)

        mapping_file_path = os.path.join(temp_assets_dir, "mapping")
        with open(mapping_file_path, "w", encoding="utf8") as f:
            json.dump([PATH_MAPPING], f)

        attachment_api.attachment_upload(
            manifests=[os.path.join(temp_assets_dir, file_name)],
            s3_root_uri=TEST_S3_URI,
            boto3_session=session_mock,
            path_mapping_rules=mapping_file_path,
            upload_manifest_path="test",
        )

        mock_upload_assets.assert_called_once_with(
            job_attachment_settings=JobAttachmentS3Settings.from_s3_root_uri(TEST_S3_URI),
            manifest=decode_manifest(json.dumps(MOCK_MANIFEST_CASE[PATH_MAPPING_HASH])),
            partial_manifest_prefix="test",
            manifest_file_name=file_name,
            manifest_metadata={
                "Metadata": {
                    "asset-root": PATH_MAPPING["source_path"],
                    "file-system-location-name": PATH_MAPPING["source_path_format"],
                }
            },
            source_root=Path(PATH_MAPPING["source_path"]),
            asset_root=Path(PATH_MAPPING["destination_path"]),
            s3_check_cache_dir=config_file.get_cache_directory(),
        )

    @pytest.mark.parametrize("manifest_case_key", MOCK_MANIFEST_CASE.keys())
    def test_upload_single_map_from_root(
        self, temp_assets_dir, session_mock, mock_upload_assets, manifest_case_key
    ):
        file_name_prefix: str = hash_data(temp_assets_dir.encode("utf-8"), HashAlgorithm.XXH128)
        file_name: str = f"{file_name_prefix}_output"

        with open(
            os.path.join(temp_assets_dir, file_name),
            "w",
            encoding="utf8",
        ) as f:
            json.dump(MOCK_MANIFEST_CASE[manifest_case_key], f)

        attachment_api.attachment_upload(
            manifests=[os.path.join(temp_assets_dir, file_name)],
            s3_root_uri=TEST_S3_URI,
            boto3_session=session_mock,
            root_dirs=[temp_assets_dir],
            upload_manifest_path="test",
        )

        mock_upload_assets.assert_called_once_with(
            job_attachment_settings=JobAttachmentS3Settings.from_s3_root_uri(TEST_S3_URI),
            manifest=decode_manifest(json.dumps(MOCK_MANIFEST_CASE[manifest_case_key])),
            partial_manifest_prefix="test",
            manifest_file_name=file_name,
            manifest_metadata={"Metadata": {"asset-root": temp_assets_dir}},
            source_root=Path(temp_assets_dir),
            asset_root=Path(temp_assets_dir),
            s3_check_cache_dir=config_file.get_cache_directory(),
        )

    @pytest.mark.parametrize("manifest_case_key", MOCK_MANIFEST_CASE.keys())
    def test_upload_no_mapped_root(
        self, temp_assets_dir, session_mock, mock_upload_assets, manifest_case_key
    ):
        with open(
            os.path.join(temp_assets_dir, manifest_case_key),
            "w",
            encoding="utf8",
        ) as f:
            json.dump(MOCK_MANIFEST_CASE[manifest_case_key], f)

        # Test No valid root defined for given manifest
        with pytest.raises(NonValidInputError) as error:
            attachment_api.attachment_upload(
                manifests=[os.path.join(temp_assets_dir, manifest_case_key)],
                s3_root_uri="s3://bucket/assetRoot",
                root_dirs=[temp_assets_dir],
                boto3_session=session_mock,
            )

        assert f"No valid root defined for given manifest {manifest_case_key}" in str(error.value)

    def test_upload_no_root_dir_or_mapping(self, temp_assets_dir, session_mock):
        with pytest.raises(NonValidInputError) as error:
            attachment_api.attachment_upload(
                manifests=[],
                s3_root_uri="s3://bucketName/rootPrefix",
                boto3_session=session_mock,
            )

        assert str(error.value) == "One of path mapping rule and root dir must exist, and not both."

    def test_upload_both_root_dir_and_mapping(self, temp_assets_dir, session_mock):
        with pytest.raises(NonValidInputError) as error:
            attachment_api.attachment_upload(
                manifests=[],
                path_mapping_rules="fakefilepath",
                root_dirs=[temp_assets_dir],
                s3_root_uri="s3://bucketName/rootPrefix",
                boto3_session=session_mock,
            )

        assert str(error.value) == "One of path mapping rule and root dir must exist, and not both."
