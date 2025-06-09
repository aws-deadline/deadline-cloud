# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from pathlib import Path
import sys

import pytest

from deadline.job_attachments._utils import (
    _normalize_windows_path,
    _is_relative_to,
    _retry,
)


class TestUtils:
    @pytest.mark.skipif(
        sys.platform != "win32",
        reason="This test is for paths in Windows format and will be skipped on non-Windows systems.",
    )
    @pytest.mark.parametrize(
        ("input_path", "expected"),
        [
            (r"\\?\C:\path\to\file.txt", Path(r"C:\path\to\file.txt")),
            (r"\\?\D:\another\long\path", Path(r"D:\another\long\path")),
            (r"C:\normal\path.txt", Path(r"C:\normal\path.txt")),
            (r"Z:\already\normal\path", Path(r"Z:\already\normal\path")),
        ],
    )
    def test_normalize_windows_path(self, input_path, expected):
        """
        Tests if _normalize_windows_path correctly strips the \\?\\ prefix
        from Windows extended-length paths.
        """
        assert _normalize_windows_path(Path(input_path)) == expected

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
            (
                "\\\\?\\C:\\path\\to\\a\\very\\long\\file\\path\\that\\exceeds\\the\\windows\\max\\path\\length\\for\\testing\\max\\file\\path\\error\\handling\\when\\comparing\\path\\relativity\\using\\job\\attachments",
                "C:\\path\\to\\",
                True,
            ),
            (
                "\\\\?\\C:\\path\\to\\a\\very\\long\\file\\path\\that\\exceeds\\the\\windows\\max\\path\\length\\for\\testing\\max\\file\\path\\error\\handling\\when\\comparing\\path\\relativity\\using\\job\\attachments",
                "C:\\path\\doesnt\\exist\\",
                False,
            ),
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
