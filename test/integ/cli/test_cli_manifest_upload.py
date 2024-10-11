# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Integ tests for the CLI manifest upload commands.
"""
import os
from pathlib import Path
from typing import Optional
import boto3
from click.testing import CliRunner
from deadline.client.cli._groups.manifest_group import cli_manifest
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

    def create_manifest_file(self, temp_dir) -> str:
        """
        Create a test manifest file, and return the full path for testing.
        """

        # Given a snapshot file:
        test_file_name = "test_file"
        test_file = os.path.join(temp_dir, test_file_name)
        os.makedirs(os.path.dirname(test_file), exist_ok=True)
        with open(test_file, "w") as f:
            f.write("testing123")

        # When
        manifest: Optional[ManifestSnapshot] = _manifest_snapshot(
            root=temp_dir, destination=temp_dir, name="test"
        )

        # Then
        assert manifest is not None
        assert manifest.manifest is not None
        return manifest.manifest

    def test_manifest_upload(self, temp_dir):
        """
        Simple test to generate a manifest, and then call the upload CLI to upload to S3.
        The test verifies the manifest is uploaded by doing a S3 get call.
        """

        # Given a snapshot file:
        manifest_file = self.create_manifest_file(temp_dir)
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
        assert result.exit_code == 0, f"Non-Zeo exit code, CLI output {result.output}"

        # Then validate the Manifest file is uploaded to S3 by checking the file actually exists.
        manifest_s3_path = f"DeadlineCloud/Manifests/{manifest_file_name}"
        s3_client = boto3.client("s3")
        s3_client.head_object(Bucket=s3_bucket, Key=manifest_s3_path)

        # Cleanup.
        s3_client.delete_object(Bucket=s3_bucket, Key=manifest_s3_path)

    def test_manifest_upload_by_farm_queue(self, temp_dir):
        """
        Simple test to generate a manifest, and then call the upload CLI to upoad to S3.
        This test case uses --farm-id and --queue-id
        The test verifies the manifest is uploaded by doing a S3 get call.
        """

        # Given a snapshot file:
        manifest_file = self.create_manifest_file(temp_dir)
        manifest_file_name = Path(manifest_file).name

        # Now that we have a manifest file, execute the CLI and upload it to S3
        # The manifest file name is unique, so it will not collide with prior test runs.
        s3_bucket = os.environ.get("JOB_ATTACHMENTS_BUCKET", "")
        runner = CliRunner()
        # Temporary, always add cli_manifest until launched.
        main.add_command(cli_manifest)
        result = runner.invoke(
            main,
            [
                "manifest",
                "upload",
                "--farm-id",
                os.environ.get("FARM_ID", ""),
                "--queue-id",
                os.environ.get("QUEUE_ID", ""),
                manifest_file,
            ],
        )
        assert result.exit_code == 0, f"Non-Zeo exit code, CLI output {result.output}"

        # Then validate the Manifest file is uploaded to S3 by checking the file actually exists.
        manifest_s3_path = f"DeadlineCloud/Manifests/{manifest_file_name}"
        s3_client = boto3.client("s3")
        try:
            s3_client.head_object(Bucket=s3_bucket, Key=manifest_s3_path)
        except ClientError:
            pytest.fail(f"File not found at {s3_bucket}, {manifest_s3_path}")

        # Cleanup.
        s3_client.delete_object(Bucket=s3_bucket, Key=manifest_s3_path)
