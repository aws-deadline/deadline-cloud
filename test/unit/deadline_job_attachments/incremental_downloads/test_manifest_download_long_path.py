# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Tests for long path handling in incremental downloads."""

from __future__ import annotations

import sys
import tempfile
from collections import defaultdict
from pathlib import Path
from threading import Lock
from typing import DefaultDict
from unittest.mock import MagicMock, patch

import pytest

import deadline
from deadline.job_attachments._utils import (
    WINDOWS_MAX_PATH_LENGTH,
    TEMP_DOWNLOAD_ADDED_CHARS_LENGTH,
)
from deadline.job_attachments.asset_manifests.hash_algorithms import HashAlgorithm
from deadline.job_attachments.asset_manifests.v2023_03_03 import ManifestPath
from deadline.job_attachments.models import FileConflictResolution
from deadline.job_attachments._incremental_downloads._manifest_s3_downloads import (
    _download_file,
)


class TestIncrementalDownloadLongPath:
    """Tests for long path handling in incremental download operations."""

    def _test_create_copy_long_path_scenario(
        self, base_dir: Path, long_base_name: str, expect_unc_prefix: bool = False
    ) -> None:
        """
        Common test logic for CREATE_COPY long path scenarios in incremental downloads.
        Tests that original path is not long but copy becomes long.
        """
        original_file = Path(base_dir) / f"{long_base_name}.txt"
        copy_file = Path(base_dir) / f"{long_base_name} (1).txt"

        # Verify our test scenario is correct
        original_len = len(str(original_file)) + TEMP_DOWNLOAD_ADDED_CHARS_LENGTH
        copy_len = len(str(copy_file)) + TEMP_DOWNLOAD_ADDED_CHARS_LENGTH
        assert original_len < WINDOWS_MAX_PATH_LENGTH, (
            f"Original should NOT be long path: {original_len}"
        )
        assert copy_len >= WINDOWS_MAX_PATH_LENGTH, f"Copy should become long path: {copy_len}"

        # Create test file path object for the manifest - path should be absolute
        file_path = ManifestPath(
            path=str(original_file), hash="testhash", size=100, mtime=1234000000
        )

        # Mock S3 operations
        mock_s3_client = MagicMock()
        mock_boto3_session = MagicMock()
        mock_progress_tracker = MagicMock()
        mock_progress_tracker.continue_reporting = True

        mock_lock = Lock()
        mock_collision_dict: DefaultDict[str, int] = defaultdict(int)

        with patch(
            f"{deadline.__package__}.job_attachments._incremental_downloads._manifest_s3_downloads._download_file_with_get_object"
        ) as mock_download_get_object, patch(
            f"{deadline.__package__}.job_attachments._utils._is_windows_long_path_registry_enabled",
            return_value=False,  # Ensure UNC prefix is used for Windows
        ), patch(
            "pathlib.Path.is_file",
            return_value=True,  # Simulate that original file exists to force conflict
        ), patch(
            f"{deadline.__package__}.job_attachments._incremental_downloads._manifest_s3_downloads._get_new_copy_file_path",
            return_value=copy_file,
        ), patch("pathlib.Path.mkdir"), patch(
            f"{deadline.__package__}.job_attachments._incremental_downloads._manifest_s3_downloads.os.utime"
        ), patch(
            f"{deadline.__package__}.job_attachments._incremental_downloads._manifest_s3_downloads.os.path.getsize",
            return_value=100,  # Return the same size as file.size to pass validation
        ):
            # Call _download_file with CREATE_COPY resolution
            _download_file(
                file=file_path,
                hash_algorithm=HashAlgorithm.XXH128,
                collision_lock=mock_lock,
                collision_file_dict=mock_collision_dict,
                s3_bucket="test-bucket",
                cas_prefix="rootPrefix/Data",
                s3_client=mock_s3_client,
                boto3_session_for_s3=mock_boto3_session,
                progress_tracker=mock_progress_tracker,
                file_conflict_resolution=FileConflictResolution.CREATE_COPY,
            )

            # Verify the download was called
            download_calls = mock_download_get_object.call_args_list
            assert len(download_calls) == 1, "Should have made exactly one download call"

            download_call = download_calls[0]
            # Get local_file_path from kwargs
            local_file_path = download_call.kwargs.get("local_file_path", "")
            fileobj_path = str(local_file_path)

            # Platform-specific path format validation
            if expect_unc_prefix:
                # Windows: verify UNC prefix is used
                assert fileobj_path.startswith("\\\\?\\"), (
                    f"Copy file path should use UNC prefix for long paths, got: {fileobj_path}"
                )

                # Verify the underlying path length that triggered the conversion
                underlying_path = fileobj_path.replace("\\\\?\\", "")
                assert (
                    len(underlying_path) + TEMP_DOWNLOAD_ADDED_CHARS_LENGTH
                    >= WINDOWS_MAX_PATH_LENGTH
                ), (
                    f"The underlying path + temp chars should be at/over Windows limit: {len(underlying_path) + TEMP_DOWNLOAD_ADDED_CHARS_LENGTH}"
                )

                # Verify expected path components are present
                assert str(base_dir).lstrip("\\\\?\\") in fileobj_path, (
                    f"Path should contain base directory: {base_dir}"
                )
            else:
                # POSIX: verify no UNC prefix is used
                assert not fileobj_path.startswith("\\\\?\\"), (
                    f"POSIX systems should not use UNC prefix, got: {fileobj_path}"
                )

                # Verify the path is the expected copy path
                expected_copy_path = str(copy_file)
                assert fileobj_path == expected_copy_path, (
                    f"Should use normal path format on POSIX: expected {expected_copy_path}, got {fileobj_path}"
                )

                # Verify expected path components are present
                assert str(base_dir) in fileobj_path, (
                    f"Path should contain base directory: {base_dir}"
                )

            # Verify it contains the copy filename pattern
            assert f"{long_base_name} (1).txt" in fileobj_path, (
                "Should contain the copy filename pattern"
            )

    @pytest.mark.skipif(
        sys.platform != "win32",
        reason="This test is for Windows long path handling only.",
    )
    def test_download_file_create_copy_becomes_long_path_windows(self) -> None:
        """
        Test that when CREATE_COPY conflict resolution creates a filename that becomes a Windows long path,
        _download_file converts it to use the UNC prefix (\\?\\) format and successfully downloads the file.

        This tests the fix for GitHub issue #617 in the incremental downloads module.
        """
        # Create a path that's just under the Windows limit, but becomes long with " (1)"
        base_dir = Path("C:\\" + "a" * 100)  # Directory part
        long_base_name = "b" * 141  # Filename part - calculated to hit threshold

        # Use the common test logic with Windows-specific validation
        self._test_create_copy_long_path_scenario(base_dir, long_base_name, expect_unc_prefix=True)

    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="This test is for POSIX systems.",
    )
    @pytest.mark.parametrize(
        "dir_multiplier,filename_len",
        [
            (14, 85) if sys.platform == "darwin" else (22, 72),
        ],
    )
    def test_download_file_create_copy_long_path_posix(
        self, dir_multiplier: int, filename_len: int
    ) -> None:
        """
        Test that CREATE_COPY conflict resolution works correctly on POSIX systems
        with long filenames and actually downloads the file.

        The variables have been decided such that they cross the max path length of 260.
        They are different for MacOS and Linux because of different temp directory lengths.
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            nested_dir = tmp_path / ("longdir" * dir_multiplier)
            long_base_name = "a" * filename_len

            # Use the common test logic with POSIX-specific validation
            self._test_create_copy_long_path_scenario(
                nested_dir, long_base_name, expect_unc_prefix=False
            )
