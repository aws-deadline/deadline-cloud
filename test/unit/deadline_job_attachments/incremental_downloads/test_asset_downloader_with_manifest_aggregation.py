# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Tests for asset_downloader_with_manifest_aggregation.py."""

from __future__ import annotations

import os
import tempfile
import shutil
from typing import Dict, Any
from unittest.mock import MagicMock, patch
from datetime import datetime
from pathlib import Path

import boto3
import pytest

from deadline.job_attachments.asset_manifests.base_manifest import BaseAssetManifest
from deadline.job_attachments.asset_manifests.hash_algorithms import HashAlgorithm
from deadline.job_attachments.asset_manifests.versions import ManifestVersion
from deadline.job_attachments.models import FileConflictResolution
from deadline.job_attachments._incremental_downloads.asset_downloader_with_manifest_aggregation import (
    AssetDownloadFailedException,
    aggregate_manifest_and_download_outputs,
)

# Constants
TEST_FARM_ID = "farm-12345"
TEST_QUEUE_ID = "queue-12345"
TEST_JOB_ID = "job-12345"
TEST_STEP_ID = "step-12345"
TEST_TASK_ID = "task-12345"
TEST_SESSION_ACTION_ID = "sa-12345"
TEST_BUCKET_NAME = "test-bucket"
TEST_ROOT_PREFIX = "test-prefix"
TEST_REGION = "us-west-2"
TEST_MANIFEST_PATH_1 = "manifest1.json"
TEST_MANIFEST_PATH_2 = "manifest2.json"
TEST_CONTENT = "Test content"
TEST_ROOT1_CONTENT = "Root 1 content"
TEST_ROOT2_CONTENT = "Root 2 content"
TEST_ROOT1_PATH = "/test/root1"
TEST_ROOT2_PATH = "/test/root2"


# Create a test manifest class that implements the required abstract methods
class TestManifest(BaseAssetManifest):
    def __init__(self, paths):
        super().__init__(paths=paths, hash_alg=HashAlgorithm.XXH128)
        self.manifestVersion = ManifestVersion.v2023_03_03

    @classmethod
    def get_default_hash_alg(cls) -> HashAlgorithm:
        return HashAlgorithm.XXH128

    @classmethod
    def decode(cls, *, manifest_data: Dict[str, Any]) -> BaseAssetManifest:
        return cls([])

    def encode(self) -> str:
        return "{}"


class TestAssetDownloaderWithManifestAggregation:
    """Tests for asset_downloader_with_manifest_aggregation.py."""

    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for testing."""
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        # Clean up
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)

    @pytest.fixture
    def boto3_session(self):
        """Create a mock boto3 session with deadline client."""
        session = MagicMock(spec=boto3.Session)
        session.profile_name = "default"
        deadline_client = MagicMock()
        session.client.return_value = deadline_client

        # Set up the mock queue response
        deadline_client.get_queue.return_value = {
            "displayName": "test-queue",
            "jobAttachmentSettings": {
                "s3BucketName": TEST_BUCKET_NAME,
                "rootPrefix": TEST_ROOT_PREFIX,
            },
        }

        return session

    @pytest.fixture
    def queue_role_session(self):
        """Create a mock boto3 session for queue role."""
        session = MagicMock(spec=boto3.Session)
        return session

    @pytest.fixture
    def test_file(self, temp_dir):
        """Create a single test file for download verification."""
        test_file_path = os.path.join(temp_dir, "test_file.txt")
        with open(test_file_path, "w") as f:
            f.write(TEST_CONTENT)
        return test_file_path

    @pytest.fixture
    def test_manifest(self, test_file):
        """Create a test manifest with a single file."""
        return TestManifest([Path(test_file)])

    @pytest.fixture
    def multiple_test_files(self, temp_dir):
        """Create multiple test files for different roots."""
        root1_file = os.path.join(temp_dir, "root1_file.txt")
        with open(root1_file, "w") as f:
            f.write(TEST_ROOT1_CONTENT)

        root2_file = os.path.join(temp_dir, "root2_file.txt")
        with open(root2_file, "w") as f:
            f.write(TEST_ROOT2_CONTENT)

        return root1_file, root2_file

    @pytest.fixture
    def multiple_test_manifests(self, multiple_test_files):
        """Create test manifests with multiple files."""
        root1_file, root2_file = multiple_test_files
        return (TestManifest([Path(root1_file)]), TestManifest([Path(root2_file)]))

    @patch(
        "deadline.job_attachments._incremental_downloads.asset_downloader_with_manifest_aggregation._get_queue_user_boto3_session"
    )
    @patch(
        "deadline.job_attachments._incremental_downloads.asset_downloader_with_manifest_aggregation._get_output_manifest_files_by_asset_root_with_last_modified"
    )
    @patch(
        "deadline.job_attachments._incremental_downloads.asset_downloader_with_manifest_aggregation._merge_asset_manifests_sorted_asc_by_last_modified"
    )
    def test_aggregate_manifest_and_download_outputs_success(
        self,
        mock_merge_manifests,
        mock_get_manifests,
        mock_get_queue_session,
        boto3_session,
        queue_role_session,
        test_file,
        test_manifest,
        temp_dir,
    ):
        """Test successful manifest aggregation and download with actual file verification."""
        # Set up mocks
        mock_get_queue_session.return_value = queue_role_session
        mock_get_manifests.return_value = [(TEST_ROOT1_PATH, datetime.now(), test_manifest)]
        mock_merge_manifests.return_value = test_manifest

        # Create a destination directory for downloads
        download_dir = os.path.join(temp_dir, "downloads")
        os.makedirs(download_dir, exist_ok=True)

        # Mock the download function to actually copy the file
        def mock_download_files(
            boto3_session, file_name_manifest_dict, s3_root_uri, conflict_resolution
        ):
            for root, manifest in file_name_manifest_dict.items():
                for path in manifest.paths:
                    dest_path = os.path.join(download_dir, os.path.basename(path))
                    shutil.copy(path, dest_path)

        # Apply the mock to the download function
        with patch(
            "deadline.job_attachments._incremental_downloads.asset_downloader_with_manifest_aggregation._attachment_download_with_root_manifests",
            side_effect=mock_download_files,
        ) as mock_download:
            # Call the function under test
            aggregate_manifest_and_download_outputs(
                boto3_session=boto3_session,
                output_manifest_paths=[TEST_MANIFEST_PATH_1, TEST_MANIFEST_PATH_2],
                farm_id=TEST_FARM_ID,
                queue_id=TEST_QUEUE_ID,
                file_conflict_resolution=FileConflictResolution.OVERWRITE,
            )

            # Verify the download function was called
            mock_download.assert_called_once()

            # Verify the file was actually "downloaded" (copied to the destination)
            downloaded_file = os.path.join(download_dir, os.path.basename(test_file))
            assert os.path.exists(downloaded_file)

            # Verify the content of the downloaded file
            with open(downloaded_file, "r") as f:
                content = f.read()
                assert content == TEST_CONTENT

    @patch(
        "deadline.job_attachments._incremental_downloads.asset_downloader_with_manifest_aggregation._get_queue_user_boto3_session"
    )
    @patch(
        "deadline.job_attachments._incremental_downloads.asset_downloader_with_manifest_aggregation._get_output_manifest_files_by_asset_root_with_last_modified"
    )
    @patch(
        "deadline.job_attachments._incremental_downloads.asset_downloader_with_manifest_aggregation._merge_asset_manifests_sorted_asc_by_last_modified"
    )
    def test_aggregate_manifest_and_download_outputs_multiple_asset_roots(
        self,
        mock_merge_manifests,
        mock_get_manifests,
        mock_get_queue_session,
        boto3_session,
        queue_role_session,
        multiple_test_files,
        multiple_test_manifests,
        temp_dir,
    ):
        """Test with multiple asset roots with actual file verification."""
        # Unpack test files and manifests
        root1_file, root2_file = multiple_test_files
        test_manifest1, test_manifest2 = multiple_test_manifests

        # Set up mocks
        mock_get_queue_session.return_value = queue_role_session

        # Set up mock for getting manifests with multiple roots
        now = datetime.now()
        mock_get_manifests.return_value = [
            (TEST_ROOT1_PATH, now, test_manifest1),
            (TEST_ROOT2_PATH, now, test_manifest2),
        ]

        # Set up mock for merging manifests
        mock_merge_manifests.side_effect = [test_manifest1, test_manifest2]

        # Create destination directories for downloads
        download_dir1 = os.path.join(temp_dir, "downloads", "root1")
        download_dir2 = os.path.join(temp_dir, "downloads", "root2")
        os.makedirs(download_dir1, exist_ok=True)
        os.makedirs(download_dir2, exist_ok=True)

        # Mock the download function to actually copy the files to appropriate directories
        def mock_download_files(
            boto3_session, file_name_manifest_dict, s3_root_uri, conflict_resolution
        ):
            for root, manifest in file_name_manifest_dict.items():
                # Choose the appropriate download directory based on the root
                if root == TEST_ROOT1_PATH:
                    download_dir = download_dir1
                else:
                    download_dir = download_dir2

                for path in manifest.paths:
                    dest_path = os.path.join(download_dir, os.path.basename(path))
                    shutil.copy(path, dest_path)

        # Apply the mock to the download function
        with patch(
            "deadline.job_attachments._incremental_downloads.asset_downloader_with_manifest_aggregation._attachment_download_with_root_manifests",
            side_effect=mock_download_files,
        ) as mock_download:
            # Call the function under test
            aggregate_manifest_and_download_outputs(
                boto3_session=boto3_session,
                output_manifest_paths=[TEST_MANIFEST_PATH_1, TEST_MANIFEST_PATH_2],
                farm_id=TEST_FARM_ID,
                queue_id=TEST_QUEUE_ID,
                file_conflict_resolution=FileConflictResolution.OVERWRITE,
            )

            # Verify the download function was called
            mock_download.assert_called_once()

            # Verify the files were actually "downloaded" (copied to the destinations)
            downloaded_file1 = os.path.join(download_dir1, os.path.basename(root1_file))
            downloaded_file2 = os.path.join(download_dir2, os.path.basename(root2_file))

            assert os.path.exists(downloaded_file1)
            assert os.path.exists(downloaded_file2)

            # Verify the content of the downloaded files
            with open(downloaded_file1, "r") as f:
                content = f.read()
                assert content == TEST_ROOT1_CONTENT

            with open(downloaded_file2, "r") as f:
                content = f.read()
                assert content == TEST_ROOT2_CONTENT

    @patch(
        "deadline.job_attachments._incremental_downloads.asset_downloader_with_manifest_aggregation._get_queue_user_boto3_session"
    )
    @patch(
        "deadline.job_attachments._incremental_downloads.asset_downloader_with_manifest_aggregation._get_output_manifest_files_by_asset_root_with_last_modified"
    )
    def test_aggregate_manifest_and_download_outputs_empty_manifests(
        self,
        mock_get_manifests,
        mock_get_queue_session,
        boto3_session,
        queue_role_session,
        temp_dir,
    ):
        """Test when no manifests are found."""
        # Set up mocks
        mock_get_queue_session.return_value = queue_role_session
        mock_get_manifests.return_value = []

        # Create a download directory
        download_dir = os.path.join(temp_dir, "downloads")
        os.makedirs(download_dir, exist_ok=True)

        # Mock the download function to track if it's called
        mock_download = MagicMock()

        # Apply the mock to the download function
        with patch(
            "deadline.job_attachments._incremental_downloads.asset_downloader_with_manifest_aggregation._attachment_download_with_root_manifests",
            side_effect=mock_download,
        ):
            # Call the function under test with empty manifest paths
            aggregate_manifest_and_download_outputs(
                boto3_session=boto3_session,
                output_manifest_paths=[],
                farm_id=TEST_FARM_ID,
                queue_id=TEST_QUEUE_ID,
                file_conflict_resolution=FileConflictResolution.OVERWRITE,
            )

            # Verify the mocks were called correctly
            mock_get_queue_session.assert_called_once()
            mock_get_manifests.assert_called_once()

            # Verify the download function was NOT called since there are no manifests
            mock_download.assert_not_called()

            # Verify the download directory is empty (no files were downloaded)
            assert len(os.listdir(download_dir)) == 0

    @patch(
        "deadline.job_attachments._incremental_downloads.asset_downloader_with_manifest_aggregation._get_output_manifest_files_by_asset_root_with_last_modified"
    )
    def test_aggregate_manifest_and_download_outputs_exception(
        self, mock_get_manifests, boto3_session, temp_dir
    ):
        """Test exception handling."""
        # Set up the mock to raise an exception
        mock_get_manifests.side_effect = Exception("Test exception")

        # Create a download directory
        download_dir = os.path.join(temp_dir, "downloads")
        os.makedirs(download_dir, exist_ok=True)

        # Set up the mock queue response with valid data
        deadline_client = boto3_session.client.return_value
        deadline_client.get_queue.return_value = {
            "displayName": "test-queue",
            "jobAttachmentSettings": {
                "s3BucketName": TEST_BUCKET_NAME,
                "rootPrefix": TEST_ROOT_PREFIX,
            },
        }

        # Call the function under test and expect an AssetDownloadFailedException
        with pytest.raises(AssetDownloadFailedException):
            aggregate_manifest_and_download_outputs(
                boto3_session=boto3_session,
                output_manifest_paths=[TEST_MANIFEST_PATH_1, TEST_MANIFEST_PATH_2],
                farm_id=TEST_FARM_ID,
                queue_id=TEST_QUEUE_ID,
                file_conflict_resolution=FileConflictResolution.OVERWRITE,
            )

        # Verify no files were downloaded (directory should be empty)
        assert len(os.listdir(download_dir)) == 0
