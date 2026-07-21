# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for argument plumbing in the `deadline attachment` CLI group:
  * `--conflict-resolution` must not trip the `_apply_cli_options_to_config`
    "not standard CLI options" RuntimeError guard in the config-defaults workflow.
  * `--s3-root-uri` must be honored independently of `--profile`.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from deadline.client.cli import main
from deadline.client.config import config_file
from deadline.client.cli._groups import attachment_group
from deadline.job_attachments.models import JobAttachmentS3Settings
from deadline.job_attachments.progress_tracker import DownloadSummaryStatistics
from ..shared_constants import MOCK_FARM_ID, MOCK_QUEUE_ID

MOCK_REGION = "eu-central-1"

MOCK_S3_SETTINGS = JobAttachmentS3Settings(s3BucketName="mock-bucket", rootPrefix="MockRootPrefix")


def _write_manifest(tmp_path):
    manifest_path = tmp_path / "abc123_manifest"
    manifest_path.write_text(
        json.dumps({"hashAlg": "xxh128", "manifestVersion": "2023-03-03", "paths": []})
    )
    return str(manifest_path)


@pytest.fixture
def configured_farm_region(fresh_deadline_config):
    config_file.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config_file.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config_file.set_setting("defaults.farm_region", MOCK_REGION)
    yield fresh_deadline_config


def test_attachment_download_config_defaults_no_conflict_resolution(
    configured_farm_region, tmp_path
):
    """
    C12: In the config-defaults workflow (no --profile, no --conflict-resolution on the
    CLI), the unset --conflict-resolution option must not survive into the
    `_apply_cli_options_to_config` args and trip the "not standard CLI options" RuntimeError.
    """
    manifest_path = _write_manifest(tmp_path)

    mock_queue = MagicMock()
    mock_queue.jobAttachmentSettings = MOCK_S3_SETTINGS

    with (
        patch.object(attachment_group.api, "get_boto3_session", return_value=MagicMock()),
        patch.object(attachment_group, "get_queue", return_value=mock_queue),
        patch.object(attachment_group, "get_session_client", return_value=MagicMock()),
        patch.object(
            attachment_group.api, "get_queue_user_boto3_session", return_value=MagicMock()
        ),
        patch.object(
            attachment_group,
            "_attachment_download",
            return_value=DownloadSummaryStatistics(),
        ),
    ):
        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "attachment",
                "download",
                "--manifests",
                manifest_path,
            ],
        )

    assert result.exit_code == 0, result.output
    assert "not standard AWS Deadline Cloud CLI options" not in result.output


def test_attachment_download_honors_s3_root_uri_without_profile(configured_farm_region, tmp_path):
    """
    Bug: --s3-root-uri must be honored even when --profile is not passed. It must not be
    overwritten by the queue's job-attachment settings.
    """
    manifest_path = _write_manifest(tmp_path)

    explicit_uri = "s3://my-explicit-bucket/my-explicit-prefix"

    mock_queue = MagicMock()
    mock_queue.jobAttachmentSettings = MOCK_S3_SETTINGS

    with (
        patch.object(attachment_group.api, "get_boto3_session", return_value=MagicMock()),
        patch.object(attachment_group, "get_queue", return_value=mock_queue),
        patch.object(attachment_group, "get_session_client", return_value=MagicMock()),
        patch.object(
            attachment_group.api, "get_queue_user_boto3_session", return_value=MagicMock()
        ),
        patch.object(
            attachment_group,
            "_attachment_download",
            return_value=DownloadSummaryStatistics(),
        ) as mock_download,
    ):
        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "attachment",
                "download",
                "--manifests",
                manifest_path,
                "--s3-root-uri",
                explicit_uri,
            ],
        )

    assert result.exit_code == 0, result.output
    mock_download.assert_called_once()
    assert mock_download.call_args.kwargs["s3_root_uri"] == explicit_uri


def test_attachment_upload_honors_s3_root_uri_without_profile(configured_farm_region, tmp_path):
    """
    Bug: --s3-root-uri must be honored on `upload` even when --profile is not passed. It
    must not be overwritten by the queue's job-attachment settings.
    """
    manifest_path = _write_manifest(tmp_path)

    explicit_uri = "s3://my-explicit-bucket/my-explicit-prefix"

    mock_queue = MagicMock()
    mock_queue.jobAttachmentSettings = MOCK_S3_SETTINGS

    with (
        patch.object(attachment_group.api, "get_boto3_session", return_value=MagicMock()),
        patch.object(attachment_group, "get_queue", return_value=mock_queue),
        patch.object(attachment_group, "get_session_client", return_value=MagicMock()),
        patch.object(
            attachment_group.api, "get_queue_user_boto3_session", return_value=MagicMock()
        ),
        patch.object(
            attachment_group, "_attachment_upload", return_value=MagicMock()
        ) as mock_upload,
    ):
        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "attachment",
                "upload",
                "--manifests",
                manifest_path,
                "--s3-root-uri",
                explicit_uri,
            ],
        )

    assert result.exit_code == 0, result.output
    mock_upload.assert_called_once()
    assert mock_upload.call_args.kwargs["s3_root_uri"] == explicit_uri
