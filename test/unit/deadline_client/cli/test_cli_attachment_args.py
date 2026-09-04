# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for argument plumbing in the `deadline attachment` CLI group:
  * `--conflict-resolution` must not trip the `_apply_cli_options_to_config`
    "not standard CLI options" RuntimeError guard in the config-defaults workflow.
  * `--s3-root-uri` must be honored independently of `--profile`, including when the
    queue has no jobAttachmentSettings of its own (while the no-URI/no-settings case
    must still fail with MissingJobAttachmentSettingsError).
  * `--conflict-resolution` must be threaded through to the download call.
  * End-to-end against moto S3 (real transfer code, queue-role credential path): the
    explicit --s3-root-uri bucket is the one actually read from / written to, not the
    queue's configured bucket.
"""

from __future__ import annotations

import json
import os
from unittest.mock import MagicMock, patch

import boto3
import pytest
from click.testing import CliRunner

from deadline.client.cli import main
from deadline.client.config import config_file
from deadline.client.cli._groups import attachment_group
from deadline.job_attachments.asset_manifests.hash_algorithms import HashAlgorithm, hash_data
from deadline.job_attachments.exceptions import MissingJobAttachmentSettingsError
from deadline.job_attachments.models import FileConflictResolution, JobAttachmentS3Settings
from deadline.job_attachments.progress_tracker import DownloadSummaryStatistics
from ..shared_constants import MOCK_BUCKET_NAME, MOCK_FARM_ID, MOCK_QUEUE_ID

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


def test_attachment_download_honors_s3_root_uri_when_queue_lacks_settings(
    configured_farm_region, tmp_path
):
    """
    Bug: an explicit --s3-root-uri must be usable even when the queue has no
    jobAttachmentSettings. The MissingJobAttachmentSettingsError guard only applies
    when falling back to the queue's settings.
    """
    manifest_path = _write_manifest(tmp_path)

    explicit_uri = "s3://my-explicit-bucket/my-explicit-prefix"

    mock_queue = MagicMock()
    mock_queue.jobAttachmentSettings = None

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


def test_attachment_download_missing_settings_and_no_uri_still_raises(
    configured_farm_region, tmp_path
):
    """
    Failure mode: when the caller does NOT pass --s3-root-uri and the queue has no
    jobAttachmentSettings, the command must still fail with
    MissingJobAttachmentSettingsError rather than proceeding with no S3 root.
    """
    manifest_path = _write_manifest(tmp_path)

    mock_queue = MagicMock()
    mock_queue.jobAttachmentSettings = None

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
            ],
        )

    assert result.exit_code != 0
    assert isinstance(result.exception, (MissingJobAttachmentSettingsError, SystemExit)) or (
        "has no attachment settings" in result.output
    )
    mock_download.assert_not_called()


def test_attachment_download_conflict_resolution_reaches_download_call(
    configured_farm_region, tmp_path
):
    """
    --conflict-resolution must be applied to the config and threaded through to the
    _attachment_download call, not just silently consumed.
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
                "--conflict-resolution",
                FileConflictResolution.SKIP.name,
            ],
        )

    assert result.exit_code == 0, result.output
    mock_download.assert_called_once()
    assert mock_download.call_args.kwargs["conflict_resolution"] == FileConflictResolution.SKIP


def test_attachment_upload_honors_s3_root_uri_when_queue_lacks_settings(
    configured_farm_region, tmp_path
):
    """
    Bug: an explicit --s3-root-uri must be usable on `upload` even when the queue has no
    jobAttachmentSettings. The MissingJobAttachmentSettingsError guard only applies when
    falling back to the queue's settings.
    """
    manifest_path = _write_manifest(tmp_path)

    explicit_uri = "s3://my-explicit-bucket/my-explicit-prefix"

    mock_queue = MagicMock()
    mock_queue.jobAttachmentSettings = None

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


# ─── End-to-end tests against moto S3 ────────────────────────────────────────
#
# These run the full CLI path with NO job-attachments internals mocked: the queue-role
# credential provider (AssumeQueueRoleForUser via the deadline_mock), the real manifest
# decode, and the real S3 transfer code all execute against moto's S3. Two buckets
# exist — the queue's configured bucket (MOCK_BUCKET_NAME, created by the deadline_mock
# fixture) and an "other" bucket named by --s3-root-uri — and the tests assert the
# transfer actually touches the OTHER bucket, not just that the URI was plumbed along.

OTHER_BUCKET = "explicit-other-bucket"
OTHER_PREFIX = "OtherPrefix"
QUEUE_ROOT_PREFIX = "MockRootPrefix"

FULL_GET_QUEUE_RESPONSE = {
    "queueId": MOCK_QUEUE_ID,
    "farmId": MOCK_FARM_ID,
    "displayName": "Mock Queue",
    "status": "IDLE",
    "defaultBudgetAction": "NONE",
    "jobAttachmentSettings": {
        "rootPrefix": QUEUE_ROOT_PREFIX,
        "s3BucketName": MOCK_BUCKET_NAME,
    },
}


@pytest.fixture
def moto_farm_config(fresh_deadline_config, deadline_mock):
    """Config-defaults workflow pointed at the moto-backed farm/queue in us-west-2."""
    config_file.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config_file.set_setting("defaults.queue_id", MOCK_QUEUE_ID)
    config_file.set_setting("defaults.farm_region", "us-west-2")
    # The job_attachments get_queue helper requires more response fields than the
    # conftest default provides.
    deadline_mock.get_queue.return_value = FULL_GET_QUEUE_RESPONSE
    s3 = boto3.client("s3", region_name="us-west-2")
    s3.create_bucket(
        Bucket=OTHER_BUCKET, CreateBucketConfiguration={"LocationConstraint": "us-west-2"}
    )
    yield s3


def _write_real_manifest(directory, file_rel_path: str, content: bytes, manifest_name: str) -> str:
    """Write a valid v2023-03-03 manifest for a single file with real xxh128 hashes."""
    file_hash = hash_data(content, HashAlgorithm.XXH128)
    manifest = {
        "hashAlg": "xxh128",
        "manifestVersion": "2023-03-03",
        "totalSize": len(content),
        "paths": [{"path": file_rel_path, "hash": file_hash, "size": len(content), "mtime": 1}],
    }
    manifest_path = os.path.join(directory, manifest_name)
    with open(manifest_path, "w", encoding="utf8") as f:
        json.dump(manifest, f)
    return manifest_path


def test_attachment_download_reads_from_explicit_bucket_moto(moto_farm_config, temp_cwd, tmp_path):
    """
    End-to-end: `attachment download --s3-root-uri s3://other-bucket/...` must GET the
    CAS object from the OTHER bucket. A decoy object with the same CAS key exists in the
    queue's configured bucket with different content; the downloaded bytes prove which
    bucket was read.
    """
    s3 = moto_farm_config
    content = b"explicit bucket content"
    decoy = b"WRONG: queue bucket content!!!"
    file_hash = hash_data(content, HashAlgorithm.XXH128)

    # CAS object in the explicitly-named bucket, decoy under the queue's settings.
    s3.put_object(Bucket=OTHER_BUCKET, Key=f"{OTHER_PREFIX}/Data/{file_hash}.xxh128", Body=content)
    s3.put_object(
        Bucket=MOCK_BUCKET_NAME, Key=f"{QUEUE_ROOT_PREFIX}/Data/{file_hash}.xxh128", Body=decoy
    )

    manifest_path = _write_real_manifest(str(tmp_path), "test_file.txt", content, "e2e_manifest")

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "attachment",
            "download",
            "--manifests",
            manifest_path,
            "--s3-root-uri",
            f"s3://{OTHER_BUCKET}/{OTHER_PREFIX}",
        ],
    )

    assert result.exit_code == 0, result.output
    # No path mapping: files land under <cwd>/<manifest file name>/
    downloaded = os.path.join(temp_cwd, "e2e_manifest", "test_file.txt")
    assert os.path.isfile(downloaded), result.output
    with open(downloaded, "rb") as f:
        assert f.read() == content  # not the queue-bucket decoy


def test_attachment_upload_writes_to_explicit_bucket_moto(moto_farm_config, tmp_path):
    """
    End-to-end: `attachment upload --s3-root-uri s3://other-bucket/...` must PUT the CAS
    object and manifest into the OTHER bucket and leave the queue's configured bucket
    untouched.
    """
    s3 = moto_farm_config
    content = b"asset file uploaded to the explicit bucket"
    file_hash = hash_data(content, HashAlgorithm.XXH128)

    root_dir = tmp_path / "asset_root"
    root_dir.mkdir()
    (root_dir / "asset_file.txt").write_bytes(content)

    # For --root-dirs, the manifest file name must contain the hash of the root path.
    root_hash = hash_data(str(root_dir).encode("utf-8"), HashAlgorithm.XXH128)
    manifest_path = _write_real_manifest(
        str(tmp_path), "asset_file.txt", content, f"{root_hash}_input"
    )

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "attachment",
            "upload",
            "--manifests",
            manifest_path,
            "--root-dirs",
            str(root_dir),
            "--s3-root-uri",
            f"s3://{OTHER_BUCKET}/{OTHER_PREFIX}",
        ],
    )

    assert result.exit_code == 0, result.output

    # The CAS object was written to the explicitly-named bucket...
    cas_key = f"{OTHER_PREFIX}/Data/{file_hash}.xxh128"
    body = s3.get_object(Bucket=OTHER_BUCKET, Key=cas_key)["Body"].read()
    assert body == content

    # ...and nothing was written to the queue's configured bucket.
    queue_objects = s3.list_objects_v2(Bucket=MOCK_BUCKET_NAME)
    assert queue_objects.get("KeyCount", 0) == 0, queue_objects.get("Contents")


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
