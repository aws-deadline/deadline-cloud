# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.


import os
import tempfile
from unittest.mock import ANY, MagicMock, patch
import pytest

from deadline.client import api

from deadline.job_attachments.api.manifest import _manifest_upload


TEST_MANIFEST = '{"foo":"bar"}'
TEST_BUCKET_NAME = "s3://foobarbucket"
TEST_CAS_PREFIX = "in/a/galaxy"
TEST_KEY_PREFIX = "far/far/away"


class TestManifestUpload:

    @pytest.fixture
    def temp_dir(self):
        with tempfile.TemporaryDirectory() as tmpdir_path:
            yield tmpdir_path

    @pytest.fixture
    def mock_manifest_file(self, temp_dir) -> str:
        """
        Create a Mock manifest file saved to the temp dir.
        :return path to the test file.
        """
        path = os.path.join(temp_dir, "test.manifest")
        with open(path, "w") as manifest_file:
            manifest_file.write(TEST_MANIFEST)
        return path

    @patch("deadline.job_attachments.api.manifest.S3AssetUploader")
    def test_upload(self, mock_upload_assets: MagicMock, mock_manifest_file: str) -> None:
        """
        Upload is really simple. It is a pass through to S3AssetUploader. Make sure it is called correctly.
        """
        # Given
        boto_session = api.get_boto3_session()

        # When the API is called....
        _manifest_upload(
            manifest_file=mock_manifest_file,
            s3_bucket_name=TEST_BUCKET_NAME,
            s3_cas_prefix=TEST_CAS_PREFIX,
            boto_session=boto_session,
        )

        # Then
        mock_upload_assets.return_value.upload_bytes_to_s3.assert_called_once_with(
            bytes=ANY,
            bucket=TEST_BUCKET_NAME,
            key=TEST_CAS_PREFIX + "/Manifests/test.manifest",
            progress_handler=ANY,
            extra_args=ANY,
        )

    @patch("deadline.job_attachments.api.manifest.S3AssetUploader")
    def test_upload_with_prefix(
        self, mock_upload_assets: MagicMock, mock_manifest_file: str
    ) -> None:
        """
        Upload is really simple. It is a pass through to S3AssetUploader. Make sure it is called correctly with prefix
        """
        # Given
        boto_session = api.get_boto3_session()

        # When the API is called....
        _manifest_upload(
            manifest_file=mock_manifest_file,
            s3_bucket_name=TEST_BUCKET_NAME,
            s3_cas_prefix=TEST_CAS_PREFIX,
            s3_key_prefix=TEST_KEY_PREFIX,
            boto_session=boto_session,
        )

        # Then
        mock_upload_assets.return_value.upload_bytes_to_s3.assert_called_once_with(
            bytes=ANY,
            bucket=TEST_BUCKET_NAME,
            key=TEST_CAS_PREFIX + "/Manifests/" + TEST_KEY_PREFIX + "/test.manifest",
            progress_handler=ANY,
            extra_args=ANY,
        )
