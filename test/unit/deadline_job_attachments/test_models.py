# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
from unittest.mock import patch

from deadline.job_attachments.models import PathFormat, StorageProfileOperatingSystemFamily

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
