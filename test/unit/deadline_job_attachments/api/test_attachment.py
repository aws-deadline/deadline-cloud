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
from deadline.job_attachments.api.attachment import (
    _attachment_download,
    _attachment_upload,
    _attachment_upload_single,
    _process_s3_metadata,
)
from deadline.job_attachments.exceptions import MalformedAttachmentSettingError
from deadline.job_attachments.progress_tracker import DownloadSummaryStatistics
from deadline.job_attachments.asset_manifests import HashAlgorithm, hash_data
from deadline.job_attachments.asset_manifests.decode import decode_manifest
from deadline.job_attachments.asset_manifests.base_manifest import BaseAssetManifest
from deadline.job_attachments.api.attachment import _process_path_mapping
from deadline.job_attachments.upload import S3AssetUploader
from deadline.job_attachments.models import (
    FileConflictResolution,
    JobAttachmentS3Settings,
    UploadManifestInfo,
    PathMappingRule,
)

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
            _attachment_download(
                manifests=[os.path.join(temp_assets_dir, PATH_MAPPING_HASH)],
                s3_root_uri="s3://bucket/assetRoot",
                boto3_session=session_mock,
                path_mapping_rules=mapping_file_path,
            )

    @pytest.mark.parametrize(
        "conflict_resolution",
        [
            FileConflictResolution.CREATE_COPY,
            FileConflictResolution.OVERWRITE,
            FileConflictResolution.SKIP,
            None,
        ],
    )
    def test_download_conflict_resolution(
        self, temp_assets_dir, session_mock, mock_download_files_from_manifests, conflict_resolution
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

        if conflict_resolution:
            _attachment_download(
                manifests=[os.path.join(temp_assets_dir, PATH_MAPPING_HASH)],
                s3_root_uri="s3://bucket/assetRoot",
                boto3_session=session_mock,
                path_mapping_rules=mapping_file_path,
                conflict_resolution=conflict_resolution,
            )
        else:
            _attachment_download(
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
            conflict_resolution=conflict_resolution
            if conflict_resolution
            else FileConflictResolution.CREATE_COPY,
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

        _attachment_download(
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
            conflict_resolution=FileConflictResolution.CREATE_COPY,
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

        _attachment_download(
            manifests=[os.path.join(temp_assets_dir, key) for key in MOCK_MANIFEST_CASE.keys()],
            s3_root_uri="s3://bucket/assetRoot",
            boto3_session=session_mock,
        )

        mock_download_files_from_manifests.assert_called_once_with(
            s3_bucket="bucket",
            manifests_by_root=expected_merged,
            cas_prefix="assetRoot/Data",
            session=session_mock,
            conflict_resolution=FileConflictResolution.CREATE_COPY,
        )

    def test_download_invalid_input_manifests(self, session_mock):
        with pytest.raises(NonValidInputError):
            _attachment_download(
                manifests=["file-not-found"],
                s3_root_uri=TEST_S3_URI,
                boto3_session=session_mock,
            )

    def test_download_invalid_input_path_mapping_rules(self, session_mock):
        with pytest.raises(NonValidInputError):
            _attachment_download(
                manifests=[],
                s3_root_uri=TEST_S3_URI,
                boto3_session=session_mock,
                path_mapping_rules="file-not-found",
            )

    def test_download_invalid_input_s3_root_uri(self, session_mock):
        with pytest.raises(MalformedAttachmentSettingError):
            _attachment_download(
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

    def test_upload_returns_manifest_info_list(self, temp_assets_dir, session_mock):
        """Test that _attachment_upload returns a list of UploadManifestInfo objects corresponding to the input manifests."""
        # Create a path mapping file with two rules
        path_mapping = [
            {
                "source_path_format": "posix",
                "source_path": "/local/home/test",
                "destination_path": "/local/home/test1/output",
            },
            {
                "source_path_format": "posix",
                "source_path": "/local/home/test2",
                "destination_path": "/local/home/test2/output",
            },
        ]
        path_mapping_file = os.path.join(temp_assets_dir, "path_mapping.json")
        with open(path_mapping_file, "w") as f:
            json.dump(path_mapping, f)

        # Create a manifest file that only has changes for the first asset root
        manifest_case_key = PATH_MAPPING_HASH
        file_name = f"{PATH_MAPPING_HASH}.manifest"
        with open(os.path.join(temp_assets_dir, file_name), "w") as f:
            json.dump(MOCK_MANIFEST_CASE[manifest_case_key], f)

        # Mock asset_uploader.upload_assets to return known values
        with patch(
            "deadline.job_attachments.upload.S3AssetUploader.upload_assets"
        ) as mock_upload_assets:
            mock_upload_assets.return_value = ("key1", "hash1")

            # Call _attachment_upload
            result = _attachment_upload(
                manifests=[os.path.join(temp_assets_dir, file_name)],
                s3_root_uri=TEST_S3_URI,
                boto3_session=session_mock,
                path_mapping_rules=path_mapping_file,
            )

            # Verify the result structure
            assert isinstance(result, list)
            assert len(result) == 1  # We only passed one manifest

            # Verify the UploadManifestInfo object has the correct values
            assert isinstance(result[0], UploadManifestInfo)
            assert result[0].output_manifest_path == "key1"
            assert result[0].output_manifest_hash == "hash1"
            assert result[0].source_path == "/local/home/test"

    def test_upload_invalid_input_manifests(self, session_mock):
        with pytest.raises(NonValidInputError):
            _attachment_upload(
                manifests=["file-not-found"],
                s3_root_uri=TEST_S3_URI,
                boto3_session=session_mock,
            )

    def test_upload_invalid_input_path_mapping_rules(self, session_mock):
        with pytest.raises(NonValidInputError):
            _attachment_upload(
                manifests=[],
                s3_root_uri=TEST_S3_URI,
                boto3_session=session_mock,
                path_mapping_rules="file-not-found",
            )

    def test_upload_invalid_input_s3_root_uri(self, temp_assets_dir, session_mock):
        with pytest.raises(MalformedAttachmentSettingError):
            _attachment_upload(
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

        _attachment_upload(
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

        _attachment_upload(
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
            _attachment_upload(
                manifests=[os.path.join(temp_assets_dir, manifest_case_key)],
                s3_root_uri="s3://bucket/assetRoot",
                root_dirs=[temp_assets_dir],
                boto3_session=session_mock,
            )

        assert f"No valid root defined for given manifest {manifest_case_key}" in str(error.value)

    def test_upload_no_root_dir_or_mapping(self, temp_assets_dir, session_mock):
        with pytest.raises(NonValidInputError) as error:
            _attachment_upload(
                manifests=[],
                s3_root_uri="s3://bucketName/rootPrefix",
                boto3_session=session_mock,
            )

        assert str(error.value) == "One of path mapping rule and root dir must exist, and not both."

    def test_upload_both_root_dir_and_mapping(self, temp_assets_dir, session_mock):
        with pytest.raises(NonValidInputError) as error:
            _attachment_upload(
                manifests=[],
                path_mapping_rules="fakefilepath",
                root_dirs=[temp_assets_dir],
                s3_root_uri="s3://bucketName/rootPrefix",
                boto3_session=session_mock,
            )

        assert str(error.value) == "One of path mapping rule and root dir must exist, and not both."


class TestAttachmentUploadSingle:
    @pytest.fixture
    def mock_upload_assets(self):
        with patch.object(
            S3AssetUploader, "upload_assets", return_value=("test_key", "test_hash")
        ) as mock_upload_assets:
            yield mock_upload_assets

    @pytest.fixture
    def sample_manifest(self):
        """Create a sample manifest object for testing"""
        return decode_manifest(json.dumps(MOCK_MANIFEST_CASE[PATH_MAPPING_HASH]))

    def test_upload_single_basic_functionality(
        self, session_mock, mock_upload_assets, sample_manifest
    ):
        """Test basic functionality of _attachment_upload_single"""
        source_root = Path("/test/source")
        upload_manifest_name = "test_manifest.json"
        upload_manifest_path = "test/path"

        result = _attachment_upload_single(
            manifest=sample_manifest,
            s3_settings=JobAttachmentS3Settings.from_s3_root_uri(TEST_S3_URI),
            boto3_session=session_mock,
            source_root=source_root,
            s3_upload_manifest_name=upload_manifest_name,
            s3_upload_manifest_path=upload_manifest_path,
        )

        # Verify the result
        assert isinstance(result, UploadManifestInfo)
        assert result.output_manifest_path == "test_key"
        assert result.output_manifest_hash == "test_hash"
        assert result.source_path == str(source_root)

        # Verify upload_assets was called with correct parameters
        mock_upload_assets.assert_called_once_with(
            job_attachment_settings=JobAttachmentS3Settings.from_s3_root_uri(TEST_S3_URI),
            manifest=sample_manifest,
            partial_manifest_prefix=upload_manifest_name,
            manifest_file_name=upload_manifest_name,
            manifest_metadata={"Metadata": {}},
            source_root=source_root,
            s3_check_cache_dir=config_file.get_cache_directory(),
        )

    def test_upload_single_with_ascii_metadata(
        self, session_mock, mock_upload_assets, sample_manifest
    ):
        """Test _attachment_upload_single with ASCII metadata"""
        source_root = Path("/test/source")
        upload_manifest_name = "test_manifest.json"
        upload_manifest_path = "test/path"
        metadata = {"project": "test_project", "user": "john_doe", "version": "1.0"}

        result = _attachment_upload_single(
            manifest=sample_manifest,
            s3_settings=JobAttachmentS3Settings.from_s3_root_uri(TEST_S3_URI),
            boto3_session=session_mock,
            source_root=source_root,
            s3_upload_manifest_name=upload_manifest_name,
            s3_upload_manifest_path=upload_manifest_path,
            s3_upload_manifest_metadata=metadata,
        )

        # Verify the result
        assert isinstance(result, UploadManifestInfo)
        assert result.output_manifest_path == "test_key"
        assert result.output_manifest_hash == "test_hash"
        assert result.source_path == str(source_root)

        # Verify metadata was processed correctly
        expected_metadata = {"Metadata": metadata}
        mock_upload_assets.assert_called_once()
        call_args = mock_upload_assets.call_args
        assert call_args[1]["manifest_metadata"] == expected_metadata

    def test_upload_single_with_non_ascii_metadata(
        self, session_mock, mock_upload_assets, sample_manifest
    ):
        """Test _attachment_upload_single with non-ASCII metadata values"""
        source_root = Path("/test/source")
        upload_manifest_name = "test_manifest.json"
        upload_manifest_path = "test/path"
        metadata = {
            "project": "test_project",  # ASCII
            "location": "café in Paris",  # Non-ASCII
            "user": "测试用户",  # Non-ASCII
        }

        result = _attachment_upload_single(
            manifest=sample_manifest,
            s3_settings=JobAttachmentS3Settings.from_s3_root_uri(TEST_S3_URI),
            boto3_session=session_mock,
            source_root=source_root,
            s3_upload_manifest_name=upload_manifest_name,
            s3_upload_manifest_path=upload_manifest_path,
            s3_upload_manifest_metadata=metadata,
        )

        # Verify the result
        assert isinstance(result, UploadManifestInfo)
        assert result.output_manifest_path == "test_key"
        assert result.output_manifest_hash == "test_hash"
        assert result.source_path == str(source_root)

        # Verify metadata was processed correctly with JSON encoding for non-ASCII values
        expected_metadata = {
            "Metadata": {
                "project": "test_project",
                "location-json": json.dumps("café in Paris", ensure_ascii=True),
                "user-json": json.dumps("测试用户", ensure_ascii=True),
            }
        }
        mock_upload_assets.assert_called_once()
        call_args = mock_upload_assets.call_args
        assert call_args[1]["manifest_metadata"] == expected_metadata

    def test_upload_single_with_non_ascii_metadata_key(self, session_mock, sample_manifest):
        """Test _attachment_upload_single with non-ASCII metadata key raises error"""
        source_root = Path("/test/source")
        upload_manifest_name = "test_manifest.json"
        upload_manifest_path = "test/path"
        metadata = {
            "用户名": "john_doe",  # Non-ASCII key
        }

        with pytest.raises(NonValidInputError) as exc_info:
            _attachment_upload_single(
                manifest=sample_manifest,
                s3_settings=JobAttachmentS3Settings.from_s3_root_uri(TEST_S3_URI),
                boto3_session=session_mock,
                source_root=source_root,
                s3_upload_manifest_name=upload_manifest_name,
                s3_upload_manifest_path=upload_manifest_path,
                s3_upload_manifest_metadata=metadata,
            )

        assert "contains non-ASCII characters" in str(exc_info.value)
        assert "S3 metadata keys must be ASCII-only" in str(exc_info.value)

    def test_upload_single_with_required_parameters_only(
        self, session_mock, mock_upload_assets, sample_manifest
    ):
        """Test _attachment_upload_single with only required parameters"""
        source_root = Path("/test/source")
        upload_manifest_name = "required_manifest.json"
        upload_manifest_path = "required/path"

        result = _attachment_upload_single(
            manifest=sample_manifest,
            s3_settings=JobAttachmentS3Settings.from_s3_root_uri(TEST_S3_URI),
            boto3_session=session_mock,
            source_root=source_root,
            s3_upload_manifest_name=upload_manifest_name,
            s3_upload_manifest_path=upload_manifest_path,
        )

        # Verify it works with required parameters
        assert isinstance(result, UploadManifestInfo)
        assert result.output_manifest_path == "test_key"
        assert result.output_manifest_hash == "test_hash"
        assert result.source_path == str(source_root)

        mock_upload_assets.assert_called_once()
        call_args = mock_upload_assets.call_args
        assert call_args[1]["partial_manifest_prefix"] == upload_manifest_name
        assert call_args[1]["manifest_file_name"] == upload_manifest_name
        assert call_args[1]["manifest_metadata"] == {"Metadata": {}}

    def test_upload_single_invalid_s3_uri(self, session_mock, sample_manifest):
        """Test _attachment_upload_single with invalid S3 URI"""
        source_root = Path("/test/source")
        upload_manifest_name = "test_manifest.json"
        upload_manifest_path = "test/path"

        with pytest.raises(MalformedAttachmentSettingError):
            _attachment_upload_single(
                manifest=sample_manifest,
                s3_settings=JobAttachmentS3Settings.from_s3_root_uri("invalid_uri"),
                boto3_session=session_mock,
                source_root=source_root,
                s3_upload_manifest_name=upload_manifest_name,
                s3_upload_manifest_path=upload_manifest_path,
            )


class TestProcessS3Metadata:
    def test_process_ascii_metadata(self):
        """Test _process_s3_metadata with ASCII-only metadata"""
        metadata = {"project": "test_project", "user": "john_doe", "version": "1.0"}

        result = _process_s3_metadata(metadata)

        # Should return unchanged for ASCII metadata
        assert result == metadata

    def test_process_non_ascii_values(self):
        """Test _process_s3_metadata with non-ASCII values"""
        metadata = {
            "project": "test_project",  # ASCII
            "location": "café in Paris",  # Non-ASCII
            "description": "测试项目",  # Non-ASCII
        }

        result = _process_s3_metadata(metadata)

        expected = {
            "project": "test_project",
            "location-json": json.dumps("café in Paris", ensure_ascii=True),
            "description-json": json.dumps("测试项目", ensure_ascii=True),
        }
        assert result == expected

    def test_process_non_ascii_key_raises_error(self):
        """Test _process_s3_metadata with non-ASCII key raises NonValidInputError"""
        metadata = {
            "用户名": "john_doe",  # Non-ASCII key
        }

        with pytest.raises(NonValidInputError) as exc_info:
            _process_s3_metadata(metadata)

        assert "contains non-ASCII characters" in str(exc_info.value)
        assert "S3 metadata keys must be ASCII-only" in str(exc_info.value)
        assert "Consider using ASCII alternatives" in str(exc_info.value)

    def test_process_mixed_metadata(self):
        """Test _process_s3_metadata with mixed ASCII and non-ASCII values"""
        metadata = {
            "ascii_key": "ascii_value",
            "unicode_key": "unicode_value_测试",
            "emoji_key": "value_with_emoji_🚀",
            "normal_key": "normal_value",
        }

        result = _process_s3_metadata(metadata)

        expected = {
            "ascii_key": "ascii_value",
            "unicode_key-json": json.dumps("unicode_value_测试", ensure_ascii=True),
            "emoji_key-json": json.dumps("value_with_emoji_🚀", ensure_ascii=True),
            "normal_key": "normal_value",
        }
        assert result == expected

    def test_process_empty_metadata(self):
        """Test _process_s3_metadata with empty metadata"""

        assert _process_s3_metadata({}) == {}

    def test_process_special_characters_in_values(self):
        """Test _process_s3_metadata with special characters that are ASCII"""
        metadata = {
            "symbols": "!@#$%^&*()_+-=[]{}|;:,.<>?",
            "quotes": 'single"double',
            "numbers": "12345",
        }

        result = _process_s3_metadata(metadata)

        # These should remain unchanged as they are ASCII
        assert result == metadata

    def test_json_encoding_format(self):
        """Test that JSON encoding produces expected ASCII-safe format"""
        metadata = {
            "unicode_text": "Hello 世界",
            "emoji": "🎉",
        }

        result = _process_s3_metadata(metadata)

        # Verify the JSON encoding is ASCII-safe
        unicode_json = result["unicode_text-json"]
        emoji_json = result["emoji-json"]

        # Should be valid JSON strings
        assert json.loads(unicode_json) == "Hello 世界"
        assert json.loads(emoji_json) == "🎉"

        # Should be ASCII-encodable
        unicode_json.encode("ascii")
        emoji_json.encode("ascii")
