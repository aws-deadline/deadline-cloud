# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from pathlib import Path
import sys

import pytest

from deadline.job_attachments._utils import (
    _is_relative_to,
    _retry,
)


class TestUtils:
    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="This test is for paths in POSIX path format and will be skipped on Windows.",
    )
    @pytest.mark.parametrize(
        ("path1", "path2", "expected"),
        [
            ("/a/b/c", "/a/b", True),
            (Path("/a/b/c.txt"), "/a", True),
            ("a/b/c", "a/b", True),
            (Path("a/b/c.txt"), "a", True),
            ("/a/b/c", "a/b", False),
            ("a/b/c", "/a/b", False),
            ("/a/b/c", "/d", False),
            ("a/b/c", "b", False),
            ("a/b/c", "d", False),
        ],
    )
    def test_is_relative_to_on_posix(self, path1, path2, expected):
        """
        Tests if the is_relative_to() works correctly when using Posix paths.
        """
        assert _is_relative_to(path1, path2) == expected

    @pytest.mark.skipif(
        sys.platform != "win32",
        reason="This test is for paths in Windows path format and will be skipped on non-Windows.",
    )
    @pytest.mark.parametrize(
        ("path1", "path2", "expected"),
        [
            ("C:/a/b/c", "C:/a/b", True),
            (Path("C:/a/b/c.txt"), "C:/a", True),
            ("C:\\a\\b\\c", "C:\\a\\b", True),
            (Path("C:\\a\\b\\c.txt"), "C:\\a", True),
            ("a/b/c", "a/b", True),
            (Path("a/b/c.txt"), "a", True),
            ("C:/a/b/c", "a/b", False),
            ("a/b/c", "C:/a/b", False),
            ("C:/a/b/c", "C:/d", False),
            ("a/b/c", "b", False),
            ("a/b/c", "d", False),
        ],
    )
    def test_is_relative_to_on_windows(self, path1, path2, expected):
        """
        Tests if the is_relative_to() works correctly when using Windows paths.
        """
        assert _is_relative_to(path1, path2) == expected

    def test_retry(self):
        """
        Test a function that throws an exception is retried.
        """
        call_count = 0

        # Given
        @_retry(ExceptionToCheck=NotImplementedError, tries=2, delay=0.1, backoff=0.1)
        def test_bad_function():
            nonlocal call_count
            call_count = call_count + 1
            if call_count == 1:
                raise NotImplementedError()

        # When
        test_bad_function()

        # Then
        assert call_count == 2

    @pytest.mark.skipif(
        sys.platform != "win32",
        reason="This test is for Windows UNC paths and will be skipped on non-Windows.",
    )
    @pytest.mark.parametrize(
        ("path1", "path2", "expected"),
        [
            # Test UNC path prefix in path1
            (r"\\?\C:\a\b\c", r"C:\a\b", True),
            (r"\\?\C:\a\b\c.txt", r"C:\a", True),
            # Test UNC path prefix in path2
            (r"C:\a\b\c", r"\\?\C:\a\b", True),
            (r"C:\a\b\c.txt", r"\\?\C:\a", True),
            # Test UNC path prefix in both paths
            (r"\\?\C:\a\b\c", r"\\?\C:\a\b", True),
            (r"\\?\C:\a\b\c.txt", r"\\?\C:\a", True),
            # Test UNC path prefix with non-relative paths
            (r"\\?\C:\a\b\c", r"C:\d", False),
            (r"C:\a\b\c", r"\\?\C:\d", False),
            (r"\\?\C:\a\b\c", r"\\?\C:\d", False),
            # Test with very long paths (simulating the real-world case)
            (
                r"\\?\C:\ProgramData\Amazon\OpenJD\session-d25da5617419425c82f8f9bf9f1eb8b1ctnxk60v\assetroot-51bb83f9066635ca31ef\05_Unreal_5_5_v2\Content\Base_Materials\Grass\seamlessT\originals\95_artificial_green_grass_texture-seamless_hr\95_artificial_green_grass_texture-seamless_hr_bump.uasset",
                r"C:\ProgramData\Amazon\OpenJD\session-d25da5617419425c82f8f9bf9f1eb8b1ctnxk60v\assetroot-51bb83f9066635ca31ef",
                True,
            ),
            # Test with UNC path in root directory
            (
                r"C:\ProgramData\Amazon\OpenJD\session-d25da5617419425c82f8f9bf9f1eb8b1ctnxk60v\assetroot-51bb83f9066635ca31ef\05_Unreal_5_5_v2\Content\file.txt",
                r"\\?\C:\ProgramData\Amazon\OpenJD\session-d25da5617419425c82f8f9bf9f1eb8b1ctnxk60v\assetroot-51bb83f9066635ca31ef",
                True,
            ),
        ],
    )
    def test_is_relative_to_with_unc_paths(self, path1, path2, expected):
        """
        Tests if the is_relative_to() works correctly when using Windows UNC paths with \\?\ prefix.
        """
        assert _is_relative_to(path1, path2) == expected
