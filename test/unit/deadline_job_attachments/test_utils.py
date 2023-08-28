# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from unittest.mock import patch

import pytest

from deadline.job_attachments.utils import (
    get_deadline_formatted_os,
    get_default_hash_cache_db_file_dir,
    map_source_path_to_dest_path,
    OJIOToken,
)


class TestUtils:
    @pytest.mark.parametrize(
        ("sys_os", "expected_output"),
        [("win32", "windows"), ("darwin", "macos"), ("linux", "linux"), ("fakeos", "Unknown")],
    )
    def test_get_deadline_formatted_os(self, sys_os: str, expected_output: str):
        """
        Tests that the expected OS string is returned
        """
        with patch("sys.platform", sys_os):
            assert get_deadline_formatted_os() == expected_output

    @pytest.mark.parametrize(
        ("source_os", "dest_os", "given_path", "expected_path"),
        [
            ("windows", "windows", r"C:\my\windows\path", r"C:\my\windows\path"),
            ("windows", "linux", r"C:\my\windows\path", r"C:\/my/windows/path"),
            ("linux", "windows", "my/linux/path", r"my\linux\path"),
            ("linux", "linux", "my/linux/path", "my/linux/path"),
        ],
    )
    def test_map_source_path_to_dest_path(
        self, source_os: str, dest_os: str, given_path: str, expected_path: str
    ):
        """
        Tests that a given path is mapped correctly
        """
        path = map_source_path_to_dest_path(source_os, dest_os, given_path)
        assert str(path) == expected_path

    def test_get_default_hash_cache_db_file_dir_env_var_path_exists(self, tmpdir):
        """
        Tests that when an environment variable exists, it uses that path for the hash cache
        """
        expected_path = tmpdir.join(".deadline").join("job_attachments")
        with patch("os.environ.get", side_effect=[tmpdir]):
            assert get_default_hash_cache_db_file_dir() == expected_path

    @pytest.mark.parametrize(
        ("test_token", "serialized_token"),
        [("mytoken", "{{ mytoken }}")],
    )
    def test_OJIOToken_serializer(self, test_token: str, serialized_token: str):
        """
        Tests that the serializer works
        """
        token = OJIOToken(test_token)
        assert str(token) == serialized_token
