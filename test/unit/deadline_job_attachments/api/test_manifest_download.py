# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import tempfile
from typing import List
from unittest.mock import MagicMock, patch
import pytest

from deadline.job_attachments.api.manifest import _manifest_download
from deadline.job_attachments.models import ManifestDownloadResponse


class TestManifestDownload:

    @pytest.fixture
    def temp_dir(self):
        with tempfile.TemporaryDirectory() as tmpdir_path:
            yield tmpdir_path

    @patch("deadline.job_attachments.api.manifest._get_queue_user_boto3_session")
    @patch("deadline.job_attachments.api.manifest.get_manifest_from_s3")
    @patch("deadline.job_attachments.api.manifest.get_output_manifests_by_asset_root")
    @pytest.mark.parametrize(
        "job_manifests,step_manifests",
        [
            pytest.param([], []),
            pytest.param([{"inputManifestPath": "s3://hello/world", "rootPath": "/some/root"}], []),
            pytest.param([], [{"stepId": "step-123456"}]),
            pytest.param(
                [{"inputManifestPath": "s3://hello/world", "rootPath": "/some/root"}],
                [{"stepId": "step-123456"}],
            ),
        ],
    )
    def test_download_job(
        self,
        mock_get_output_manifest: MagicMock,
        mock_get_manifest_from_s3: MagicMock,
        mock_queue_session: MagicMock,
        job_manifests: List,
        step_manifests: List,
        temp_dir: str,
    ) -> None:

        # This is heavily mocked, so return nothing. Integration tests tests full manifest merging.
        mock_get_manifest_from_s3.return_value = None
        mock_get_output_manifest.return_value = {}

        # Mock Boto
        mock_boto_session = MagicMock()

        # Mock Get Queue Credentials
        mock_queue_session.return_value = MagicMock()

        # Mock up Deadline.
        mock_deadline_client = MagicMock()
        mock_boto_session.client.return_value = mock_deadline_client

        # Mock the result of get_queue
        mock_deadline_client.get_queue.return_value = {
            "displayName": "queue",
            "jobAttachmentSettings": {"s3BucketName": "bucket", "rootPrefix": "root_prefix"},
        }
        # Mock the result of get_job
        mock_deadline_client.get_job.return_value = {
            "name": "Mock Job",
            "attachments": {
                "manifests": job_manifests,
            },
        }
        # Mock the result of list_step_dependencies
        mock_deadline_client.list_step_dependencies.return_value = {"dependencies": step_manifests}

        output: ManifestDownloadResponse = _manifest_download(
            download_dir=temp_dir,
            farm_id="farm-12345",
            queue_id="queue-12345",
            job_id="job-12345",
            boto3_session=mock_boto_session,
        )
        assert output is not None
