# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
from unittest.mock import patch
from dataclasses import asdict

from deadline.job_attachments.models import (
    PathFormat,
    StorageProfileOperatingSystemFamily,
    PathMappingRule,
    JobAttachmentS3Settings,
    ManifestSnapshot,
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
