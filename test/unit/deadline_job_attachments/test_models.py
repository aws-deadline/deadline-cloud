# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
from unittest.mock import patch
from dataclasses import asdict

from deadline.job_attachments.models import (
    PathFormat,
    StorageProfileOperatingSystemFamily,
    PathMappingRule,
    JobAttachmentS3Settings,
    ManifestSnapshot,
    ManifestProperties,
)
from deadline.job_attachments.asset_manifests.hash_algorithms import HashAlgorithm
from deadline.job_attachments.exceptions import MalformedAttachmentSettingError

import pytest
import json


class TestModels:
    @pytest.mark.parametrize(
        ("sys_os", "expected_output"),
        [("win32", "windows"), ("darwin", "posix"), ("linux", "posix")],
    )
    def test_get_host_path_format_string(self, sys_os: str, expected_output: str):
        """
        Tests that the expected OS string is returned
        """
        with patch("sys.platform", sys_os):
            assert PathFormat.get_host_path_format_string() == expected_output

    @pytest.mark.parametrize(
        ("input", "output"),
        [
            ("windows", StorageProfileOperatingSystemFamily.WINDOWS),
            ("WINDOWS", StorageProfileOperatingSystemFamily.WINDOWS),
            ("wInDoWs", StorageProfileOperatingSystemFamily.WINDOWS),
            ("linux", StorageProfileOperatingSystemFamily.LINUX),
            ("LINUX", StorageProfileOperatingSystemFamily.LINUX),
            ("LiNuX", StorageProfileOperatingSystemFamily.LINUX),
            ("macos", StorageProfileOperatingSystemFamily.MACOS),
            ("MACOS", StorageProfileOperatingSystemFamily.MACOS),
            ("maCOs", StorageProfileOperatingSystemFamily.MACOS),
        ],
    )
    def test_storage_profile_operating_system_family_case(
        self, input: str, output: StorageProfileOperatingSystemFamily
    ) -> None:
        """
        Tests that the correct enum types are created regardless of input string casing.
        """
        assert StorageProfileOperatingSystemFamily(input) == output

    @pytest.mark.parametrize(("input"), [("linuxx"), ("darwin"), ("oSx"), ("MSDOS")])
    def test_storage_profile_operating_system_raises_type_error(self, input):
        """
        Tests that a ValueError is raised when a non-valid string is given.
        I.e. our case-insensitivity isn't causing false-positives.
        """
        with pytest.raises(ValueError):
            StorageProfileOperatingSystemFamily(input)

    def test_path_mapping_rules(self):
        """
        Test rule construction and hashing the source attributes
        """
        path_mapping = PathMappingRule(
            source_path_format="posix",
            source_path="/tmp",
            destination_path="/local/home/test/output",
        )
        assert "a0271fe0c8b1c1f99b82b442cd878122" == path_mapping.get_hashed_source_path(
            HashAlgorithm.XXH128
        )


class TestJobAttachmentS3SettingsModel:
    @pytest.mark.parametrize(
        ("input", "output"),
        [
            ("s3BucketName/rootPrefix", JobAttachmentS3Settings("s3BucketName", "rootPrefix")),
            ("s3BucketName/root/Prefix", JobAttachmentS3Settings("s3BucketName", "root/Prefix")),
        ],
    )
    def test_job_attachment_setting_root_path(self, input: str, output: JobAttachmentS3Settings):
        """
        Test Job Attachment S3 Settings from and to S3 root path
        """
        assert output == JobAttachmentS3Settings.from_root_path(input)
        assert input == output.to_root_path()

    def test_job_attachment_setting_from_path_error(self):
        """
        Test Job Attachment S3 Settings from malformed S3 root path
        """
        with pytest.raises(MalformedAttachmentSettingError):
            JobAttachmentS3Settings.from_root_path("s3BucketOnly")

    @pytest.mark.parametrize(
        ("input", "output"),
        [
            ("s3://BucketName/rootPrefix", JobAttachmentS3Settings("BucketName", "rootPrefix")),
            ("s3://BucketName/root/Prefix", JobAttachmentS3Settings("BucketName", "root/Prefix")),
        ],
    )
    def test_job_attachment_setting_root_uri(self, input: str, output: JobAttachmentS3Settings):
        """
        Test Job Attachment S3 Settings from and to S3 root uri
        """
        assert output == JobAttachmentS3Settings.from_s3_root_uri(input)
        assert input == output.to_s3_root_uri()

    def test_job_attachment_setting_from_s3_root_uri_error(self):
        """
        Test Job Attachment S3 Settings from malformed S3 root uri
        """
        with pytest.raises(MalformedAttachmentSettingError):
            JobAttachmentS3Settings.from_s3_root_uri("s3://s3BucketOnly")

    def test_job_attachment_s3_settings_partial_session_action_manifest_prefix(self):
        """
        Test JobAttachmentS3Settings partial_session_action_manifest_prefix method
        """
        # Mock the _float_to_iso_datetime_string function to return a predictable value
        with patch(
            "deadline.job_attachments.models._float_to_iso_datetime_string",
            return_value="2025-05-22T22:17:03.409012Z",
        ):
            # Call the partial_session_action_manifest_prefix method
            result = JobAttachmentS3Settings.partial_session_action_manifest_prefix(
                farm_id="farm1",
                queue_id="queue1",
                job_id="job1",
                step_id="step1",
                task_id="task1",
                session_action_id="session1",
                time=1747952223.4090126,  # This is 2025-05-22T22:17:03.409012Z in timestamp
            )

            # Verify the result
            expected = "farm1/queue1/job1/step1/task1/2025-05-22T22:17:03.409012Z_session1"
            assert result == expected


class TestManifestSnapshotModel:
    """Tests for the ManifestSnapshot class"""

    def test_manifest_snapshot_creation(self):
        """
        Test ManifestSnapshot creation with required values
        """
        # Test with specific values
        snapshot = ManifestSnapshot(root="/path/to/root", manifest="manifest-path")
        assert snapshot.root == "/path/to/root"
        assert snapshot.manifest == "manifest-path"

    def test_manifest_snapshot_construct_from_json_missing_attribute(self):
        """
        Test ManifestSnapshot error when missing attribute
        """
        json_str = json.dumps({"manifest": "path/to/manifest"})
        assert isinstance(json_str, str)

        # Test deserialization
        with pytest.raises(TypeError):
            ManifestSnapshot(**json.loads(json_str))

    def test_manifest_snapshot_json_serialization_special_characters(self):
        """
        Test ManifestSnapshot serialization with special characters
        """
        # Test with paths containing special characters
        snapshot = ManifestSnapshot(
            root='/path/with spaces/and"quotes"/and\\backslashes',
            manifest="manifest-with-unicode-€-£-¥",
        )

        # Convert to JSON and back
        json_str = json.dumps(asdict(snapshot))
        data = json.loads(json_str)
        recreated = ManifestSnapshot(**data)

        # Verify the special characters are preserved
        assert recreated.root == '/path/with spaces/and"quotes"/and\\backslashes'
        assert recreated.manifest == "manifest-with-unicode-€-£-¥"


class TestManifestPropertiesModel:
    """Tests for the ManifestProperties class"""

    def test_from_dict_minimal_required_fields(self):
        """Test ManifestProperties.from_dict with only required fields"""
        data = {"rootPath": "/test/path", "rootPathFormat": "posix"}

        manifest_props = ManifestProperties.from_dict(data)

        assert manifest_props.rootPath == "/test/path"
        assert manifest_props.rootPathFormat == PathFormat.POSIX
        assert manifest_props.fileSystemLocationName is None
        assert manifest_props.inputManifestPath is None
        assert manifest_props.inputManifestHash is None
        assert manifest_props.outputRelativeDirectories is None

    def test_from_dict_all_fields_populated(self):
        """Test ManifestProperties.from_dict with all fields populated"""
        data = {
            "rootPath": "/test/path",
            "rootPathFormat": "windows",
            "fileSystemLocationName": "test-location",
            "inputManifestPath": "s3://bucket/manifest.json",
            "inputManifestHash": "abc123hash",
            "outputRelativeDirectories": ["output1", "output2", "subdir/output3"],
        }

        manifest_props = ManifestProperties.from_dict(data)

        assert manifest_props.rootPath == "/test/path"
        assert manifest_props.rootPathFormat == PathFormat.WINDOWS
        assert manifest_props.fileSystemLocationName == "test-location"
        assert manifest_props.inputManifestPath == "s3://bucket/manifest.json"
        assert manifest_props.inputManifestHash == "abc123hash"
        assert manifest_props.outputRelativeDirectories == ["output1", "output2", "subdir/output3"]

    @pytest.mark.parametrize(
        ("path_format", "expected_enum"),
        [
            ("posix", PathFormat.POSIX),
            ("windows", PathFormat.WINDOWS),
        ],
    )
    def test_from_dict_path_format_variations(self, path_format: str, expected_enum: PathFormat):
        """Test ManifestProperties.from_dict with different path format values"""
        data = {"rootPath": "/test/path", "rootPathFormat": path_format}

        manifest_props = ManifestProperties.from_dict(data)
        assert manifest_props.rootPathFormat == expected_enum

    def test_from_dict_missing_required_field_root_path(self):
        """Test ManifestProperties.from_dict raises KeyError when rootPath is missing"""
        data = {"rootPathFormat": "posix"}

        with pytest.raises(KeyError, match="rootPath"):
            ManifestProperties.from_dict(data)

    def test_from_dict_missing_required_field_root_path_format(self):
        """Test ManifestProperties.from_dict raises KeyError when rootPathFormat is missing"""
        data = {"rootPath": "/test/path"}

        with pytest.raises(KeyError, match="rootPathFormat"):
            ManifestProperties.from_dict(data)

    def test_from_dict_invalid_path_format(self):
        """Test ManifestProperties.from_dict raises ValueError for invalid path format"""
        invalid_root_path_format = "invalid_format"
        data = {"rootPath": "/test/path", "rootPathFormat": invalid_root_path_format}

        with pytest.raises(ValueError, match=invalid_root_path_format):
            ManifestProperties.from_dict(data)

    def test_from_dict_with_empty_optional_lists(self):
        """Test ManifestProperties.from_dict with empty lists for optional fields"""
        data = {
            "rootPath": "/test/path",
            "rootPathFormat": "posix",
            "outputRelativeDirectories": [],
        }

        manifest_props = ManifestProperties.from_dict(data)
        assert manifest_props.outputRelativeDirectories == []

    def test_from_dict_roundtrip_with_to_dict(self):
        """Test that from_dict and to_dict are inverse operations"""
        # Create a ManifestProperties instance with all fields
        original = ManifestProperties(
            rootPath="/original/path",
            rootPathFormat=PathFormat.POSIX,
            fileSystemLocationName="test-location",
            inputManifestPath="s3://bucket/manifest.json",
            inputManifestHash="hash123",
            outputRelativeDirectories=["out1", "out2"],
        )

        # Convert to dict and back
        data = original.to_dict()
        recreated = ManifestProperties.from_dict(data)

        # Verify they are equal
        assert recreated == original

    def test_from_dict_with_special_characters_in_paths(self):
        """Test ManifestProperties.from_dict with special characters in paths"""
        data = {
            "rootPath": '/path/with spaces/and"quotes"/and\\backslashes',
            "rootPathFormat": "posix",
            "fileSystemLocationName": "location-with-unicode-€-£-¥",
            "inputManifestPath": "s3://bucket-name/path with spaces/manifest.json",
            "outputRelativeDirectories": ["output with spaces", "output/with/slashes"],
        }

        manifest_props = ManifestProperties.from_dict(data)

        assert manifest_props.rootPath == '/path/with spaces/and"quotes"/and\\backslashes'
        assert manifest_props.fileSystemLocationName == "location-with-unicode-€-£-¥"
        assert manifest_props.inputManifestPath == "s3://bucket-name/path with spaces/manifest.json"
        assert manifest_props.outputRelativeDirectories == [
            "output with spaces",
            "output/with/slashes",
        ]

    def test_from_dict_with_none_values_in_optional_fields(self):
        """Test ManifestProperties.from_dict with explicit None values for optional fields"""
        data = {
            "rootPath": "/test/path",
            "rootPathFormat": "posix",
            "fileSystemLocationName": None,
            "inputManifestPath": None,
            "inputManifestHash": None,
            "outputRelativeDirectories": None,
        }

        manifest_props = ManifestProperties.from_dict(data)

        assert manifest_props.rootPath == "/test/path"
        assert manifest_props.rootPathFormat == PathFormat.POSIX
        assert manifest_props.fileSystemLocationName is None
        assert manifest_props.inputManifestPath is None
        assert manifest_props.inputManifestHash is None
        assert manifest_props.outputRelativeDirectories is None

    def test_as_output_metadata_ascii_path(self):
        """Test as_output_metadata with ASCII-only root path"""
        manifest_props = ManifestProperties(
            rootPath="/test/path",
            rootPathFormat=PathFormat.POSIX,
            fileSystemLocationName="test-location",
        )

        result = manifest_props.as_output_metadata()

        expected = {
            "Metadata": {"asset-root": "/test/path", "file-system-location-name": "test-location"}
        }
        assert result == expected

    def test_as_output_metadata_non_ascii_path(self):
        """Test as_output_metadata with non-ASCII root path"""
        manifest_props = ManifestProperties(
            rootPath="/test/café/测试", rootPathFormat=PathFormat.POSIX
        )

        result = manifest_props.as_output_metadata()

        expected = {
            "Metadata": {
                "asset-root": '"/test/caf\\u00e9/\\u6d4b\\u8bd5"',
                "asset-root-json": '"/test/caf\\u00e9/\\u6d4b\\u8bd5"',
            }
        }
        assert result == expected

    def test_as_output_metadata_no_file_system_location(self):
        """Test as_output_metadata without file system location name"""
        manifest_props = ManifestProperties(rootPath="/test/path", rootPathFormat=PathFormat.POSIX)

        result = manifest_props.as_output_metadata()

        expected = {"Metadata": {"asset-root": "/test/path"}}
        assert result == expected
