# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
from unittest.mock import patch

from deadline.job_attachments.models import PathFormat

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
