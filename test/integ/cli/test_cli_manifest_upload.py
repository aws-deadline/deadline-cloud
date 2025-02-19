# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Integ tests for the CLI manifest upload commands.
"""

import math
import os
from pathlib import Path
from typing import Optional
import boto3
from click.testing import CliRunner
from deadline.client.cli._groups.manifest_group import cli_manifest
from deadline.job_attachments._aws.deadline import get_queue
from deadline.job_attachments._utils import (
    WINDOWS_MAX_PATH_LENGTH,
    _is_windows_long_path_registry_enabled,
)
from deadline.job_attachments.api.manifest import _manifest_snapshot
from deadline.job_attachments.models import ManifestSnapshot
import pytest
import tempfile
from botocore.exceptions import ClientError

from deadline.client.cli import main


TEST_FILE_CONTENT = "test file content"
TEST_SUB_DIR_FILE_CONTENT = "subdir file content"
TEST_ROOT_DIR_FILE_CONTENT = "root file content"

TEST_ROOT_FILE = "root_file.txt"
TEST_SUB_FILE = "subdir_file.txt"

TEST_ROOT_DIR = "root_dir"
TEST_MANIFEST_DIR = "manifest_dir"
TEST_SUB_DIR_1 = "subdir1"
TEST_SUB_DIR_2 = "subdir2"


@pytest.mark.integ
class TestManifestUpload:
    @pytest.fixture
    def temp_dir(self):
        with tempfile.TemporaryDirectory() as tmpdir_path:
            yield tmpdir_path

    def create_manifest_file(self, root_directory: str, destination_directory: str) -> str:
        """
        Create a test manifest file, and return the full path for testing.
        """

        # Given a snapshot file:
        test_file_name = "test_file"
        test_file = os.path.join(root_directory, test_file_name)
        os.makedirs(os.path.dirname(test_file), exist_ok=True)
        with open(test_file, "w") as f:
            f.write("testing123")

        # When
        manifest: Optional[ManifestSnapshot] = _manifest_snapshot(
            root=root_directory, destination=destination_directory, name="test"
        )

        # Then
        assert manifest is not None
        assert manifest.manifest is not None
        return manifest.manifest

    def test_manifest_upload(self, temp_dir: str):
        """
        Simple test to generate a manifest, and then call the upload CLI to upload to S3.
        The test verifies the manifest is uploaded by doing a S3 get call.
        """

        # Given a snapshot file:
        manifest_file = self.create_manifest_file(temp_dir, temp_dir)
        manifest_file_name = Path(manifest_file).name

        # Now that we have a manifest file, execute the CLI and upload it to S3
        # The manifest file name is unique, so it will not collide with prior test runs.
        s3_bucket = os.environ.get("JOB_ATTACHMENTS_BUCKET")
        runner = CliRunner()
        # Temporary, always add cli_manifest until launched.
        main.add_command(cli_manifest)
        result = runner.invoke(
            main,
            [
                "manifest",
                "upload",
                "--s3-cas-uri",
                f"s3://{s3_bucket}/DeadlineCloud",
                manifest_file,
            ],
        )
        assert result.exit_code == 0, f"Non-Zero exit code, CLI output {result.output}"

        # Then validate the Manifest file is uploaded to S3 by checking the file actually exists.
        manifest_s3_path = f"DeadlineCloud/Manifests/{manifest_file_name}"
        s3_client = boto3.client("s3")
        s3_client.head_object(Bucket=s3_bucket, Key=manifest_s3_path)

        # Cleanup.
        s3_client.delete_object(Bucket=s3_bucket, Key=manifest_s3_path)

    def test_manifest_upload_by_farm_queue(self, temp_dir: str):
        """
        Simple test to generate a manifest, and then call the upload CLI to upoad to S3.
        This test case uses --farm-id and --queue-id
        The test verifies the manifest is uploaded by doing a S3 get call.
        """

        # Given a snapshot file:
        manifest_file = self.create_manifest_file(temp_dir, temp_dir)
        manifest_file_name = Path(manifest_file).name

        # Input:
        farm_id = os.environ.get("FARM_ID", "")
        queue_id = os.environ.get("QUEUE_ID", "")

        # Now that we have a manifest file, execute the CLI and upload it to S3
        # The manifest file name is unique, so it will not collide with prior test runs.
        s3_bucket = os.environ.get("JOB_ATTACHMENTS_BUCKET", "")
        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "manifest",
                "upload",
                "--farm-id",
                farm_id,
                "--queue-id",
                queue_id,
                manifest_file,
            ],
        )
        assert result.exit_code == 0, f"Non-Zero exit code, CLI output {result.output}"

        # Then validate the Manifest file is uploaded to S3 by checking the file actually exists.
        root_prefix = get_queue(farm_id=farm_id, queue_id=queue_id).jobAttachmentSettings.rootPrefix  # type: ignore[union-attr]
        manifest_s3_path = f"{root_prefix}/Manifests/{manifest_file_name}"
        s3_client = boto3.client("s3")
        try:
            s3_client.head_object(Bucket=s3_bucket, Key=manifest_s3_path)
        except ClientError:
            pytest.fail(f"File not found at {s3_bucket}, {manifest_s3_path}")

        # Cleanup.
        s3_client.delete_object(Bucket=s3_bucket, Key=manifest_s3_path)

    @pytest.mark.skipif(
        _is_windows_long_path_registry_enabled(),
        reason="This test is related to Windows file path length limit, skipping this if os not Windows or if the long path registry is enabled",
    )
    def test_manifest_upload_over_windows_path_limit(self, tmp_path):
        """
        Tests that when a manifest is created over the windows path limit, it is able to be uploaded and there are no issues.
        """

        tmp_path_str = str(tmp_path)
        # Create a manifest directory that is almost as long as the Windows path length limit.
        manifest_directory_remaining_length: int = WINDOWS_MAX_PATH_LENGTH - len(tmp_path_str) - 30
        manifest_directory: str = os.path.join(
            tmp_path_str,
            *["path"]
            * math.floor(
                manifest_directory_remaining_length / 5
            ),  # Create a temp path for the manifest directory that barely does not exceed the windows path limit
        )

        manifest_file: str = self.create_manifest_file(
            os.path.join(tmp_path_str, "root"), manifest_directory
        )

        assert len(manifest_file) >= WINDOWS_MAX_PATH_LENGTH

        manifest_file_name: str = Path(manifest_file).name

        # Now that we have a manifest file, execute the CLI and upload it to S3
        # The manifest file name is unique, so it will not collide with prior test runs.
        s3_bucket = os.environ.get("JOB_ATTACHMENTS_BUCKET")
        runner = CliRunner()
        # Temporary, always add cli_manifest until launched.
        main.add_command(cli_manifest)
        result = runner.invoke(
            main,
            [
                "manifest",
                "upload",
                "--s3-cas-uri",
                f"s3://{s3_bucket}/DeadlineCloud",
                manifest_file,
            ],
        )
        assert result.exit_code == 0, f"Non-Zero exit code, CLI output {result.output}"

        # Then validate the Manifest file is uploaded to S3 by checking the file actually exists.
        manifest_s3_path = f"DeadlineCloud/Manifests/{manifest_file_name}"
        s3_client = boto3.client("s3")
        s3_client.head_object(Bucket=s3_bucket, Key=manifest_s3_path)

        # Cleanup.
        s3_client.delete_object(Bucket=s3_bucket, Key=manifest_s3_path)
