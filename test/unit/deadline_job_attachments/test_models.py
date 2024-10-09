# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
from unittest.mock import patch

from deadline.job_attachments.models import (
    PathFormat,
    StorageProfileOperatingSystemFamily,
    PathMappingRule,
    JobAttachmentS3Settings,
)
from deadline.job_attachments.asset_manifests.hash_algorithms import HashAlgorithm
from deadline.job_attachments.exceptions import MalformedAttachmentSettingError

import pytest


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
            source_path="/local/home/test",
            destination_path="/loca/home/test/output",
        )
        assert "f59f1ffe44f9a64aa7330625e3b70447" == path_mapping.get_hashed_source(
            HashAlgorithm.XXH128
        )
        assert "4ab97c97c825551aaa963888278ef9ec" == path_mapping.get_hashed_source_path(
            HashAlgorithm.XXH128
        )

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
