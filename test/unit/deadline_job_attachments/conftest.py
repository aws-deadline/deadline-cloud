# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Common fixtures for deadline tests.
"""
from __future__ import annotations
import dataclasses
import json
import os
import tempfile
from datetime import datetime
from io import BytesIO
from typing import Any, Callable, Generator

import pytest
from moto import mock_aws

from botocore.client import BaseClient  # noqa: E402 isort:skip

from deadline.job_attachments._aws import aws_clients  # noqa: E402 isort:skip
from deadline.job_attachments.asset_sync import AssetSync  # noqa: E402 isort:skip
from deadline.job_attachments.models import (  # noqa: E402 isort:skip
    JobAttachmentsFileSystem,
    Attachments,
    ManifestProperties,
    Job,
    JobAttachmentS3Settings,
    PathFormat,
    Queue,
)


@pytest.fixture(scope="function")
def temp_dir():
    """
    Fixture to provide a temporary directory.
    """

    with tempfile.TemporaryDirectory() as temp_dir:
        yield temp_dir


@pytest.fixture(scope="function")
def temp_assets_dir():
    """
    Fixture to provide a temporary directory for asset files.
    """

    with tempfile.TemporaryDirectory() as assets_dir:
        yield assets_dir


@pytest.fixture(scope="function", autouse=True)
def boto_config() -> Generator[None, None, None]:
    os.environ["AWS_ACCESS_KEY_ID"] = "ACCESSKEY"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-west-2"

    mock = mock_aws()
    mock.start()
    yield
    mock.stop()


@pytest.fixture(scope="function", name="s3")
def s3_fixture(boto_config) -> Generator[BaseClient, None, None]:
    """
    Fixture to get a mock S3 client.
    """
    yield aws_clients.get_s3_client()


@pytest.fixture(scope="function")
def create_s3_bucket(boto_config, s3) -> Callable[[str], None]:  # pylint: disable=invalid-name
    """
    Fixture that returns a function that creates moto S3 buckets.
    """

    def create_bucket(bucket_name):
        s3.create_bucket(
            Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "us-west-2"}
        )

    return create_bucket


@pytest.fixture(name="default_job_attachment_s3_settings")
def fixture_default_job_attachment_s3_settings():
    """
    Fixture returning default settings for an S3 bucket associated with a Queue
    """
    return JobAttachmentS3Settings(
        s3BucketName="test-bucket",
        rootPrefix="assetRoot",
    )


@pytest.fixture(name="default_attachments")
def fixture_default_attachments(farm_id, queue_id):
    """
    Fixture returning default settings for a Job
    """
    return Attachments(
        manifests=[
            ManifestProperties(
                rootPath="/tmp",
                rootPathFormat=PathFormat.POSIX,
                inputManifestPath=f"assetRoot/Manifests/{farm_id}/{queue_id}/Inputs/0000/manifest_input",
                inputManifestHash="manifesthash",
                outputRelativeDirectories=["test/outputs"],
            )
        ],
    )


@pytest.fixture(name="vfs_attachments")
def fixture_vfs_attachments():
    """
    Fixture returning default settings for a Job
    """
    return Attachments(
        manifests=[
            ManifestProperties(
                rootPath="/tmp",
                rootPathFormat=PathFormat.POSIX,
                inputManifestPath="manifest.json",
                inputManifestHash="manifesthash",
                outputRelativeDirectories=["test/outputs"],
            )
        ],
        fileSystem=JobAttachmentsFileSystem.VIRTUAL,
    )


@pytest.fixture(name="windows_attachments")
def fixture_windows_attachments():
    """
    Fixture returning default settings for a Job submitted on a Windows machine
    """
    return Attachments(
        manifests=[
            ManifestProperties(
                rootPath=r"C:\Users\temp",
                rootPathFormat=PathFormat.WINDOWS,
                inputManifestPath="manifest.json",
                inputManifestHash="manifesthash",
                outputRelativeDirectories=["test\\outputs"],
            )
        ],
    )


@pytest.fixture(name="attachments_no_inputs")
def fixture_attachments_no_required_assets():
    """
    Fixture returning Job settings with no required assets (inputs)
    """
    return Attachments(
        manifests=[
            ManifestProperties(
                rootPath="/tmp",
                rootPathFormat=PathFormat.POSIX,
                outputRelativeDirectories=["test/outputs"],
            )
        ],
    )


@pytest.fixture(name="default_asset_sync")
def fixture_default_asset_sync(farm_id: str):
    """
    Fixture returning a default AssetSync instance
    """
    return AssetSync(farm_id)


@pytest.fixture
def assert_manifest():
    """
    Assert that a manifest file in a mock s3 bucket matches what's expected.
    """

    def __inner_func__(bucket, manifest_location, expected_manifest):
        with BytesIO() as manifest:
            bucket.download_fileobj(manifest_location, manifest)
            manifest_json = json.loads(manifest.getvalue().decode("utf-8"))

            assert manifest_json == expected_manifest

    return __inner_func__


@pytest.fixture
def assert_canonical_manifest():
    """
    Assert that a canonical manifest file in a mock s3 bucket matches what's expected.
    """

    def __inner_func__(bucket, manifest_location: str, expected_manifest: str):
        with BytesIO() as manifest:
            bucket.download_fileobj(manifest_location, manifest)

            assert manifest.getvalue().decode("utf-8") == expected_manifest

    return __inner_func__


@pytest.fixture
def assert_expected_files_on_s3():
    """
    Assert that all provided files are in an S3 bucket.
    """

    def __inner_func__(bucket, expected_files):
        actual_files = set()

        for bucket_object in bucket.objects.all():
            actual_files.add(bucket_object.key)

        assert actual_files == expected_files

    return __inner_func__


@pytest.fixture
def variables():
    return {
        "frame": 1,
    }


@pytest.fixture
def default_manifest_str_v2023_03_03() -> str:
    manifest_file = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "data", "manifest_v2023_03_03.json")
    )
    with open(manifest_file) as f:
        return f.read()


@pytest.fixture
def farm_id():
    return "farm-1234567890abcdefghijklmnopqrstuv"


@pytest.fixture
def queue_id():
    return "queue-01234567890123456789012345678901"


@pytest.fixture
def job_id():
    return "job-01234567890123456789012345678901"


@pytest.fixture
def session_action_id():
    return "session-action-1"


@pytest.fixture(name="default_job")
def fixture_default_job(job_id, default_attachments):
    """
    Fixture returning a job that can be used for most tests.
    """
    return Job(
        jobId=job_id,
        attachments=default_attachments,
    )


@pytest.fixture(name="vfs_job")
def fixture_vfs_job(job_id, vfs_attachments):
    """
    Fixture returning a job that can be used for most tests.
    """
    return Job(
        jobId=job_id,
        attachments=vfs_attachments,
    )


@pytest.fixture(name="default_queue")
def fixture_default_queue(farm_id, queue_id, default_job_attachment_s3_settings):
    """
    Fixture returning a queue that can be used for most tests.
    """
    return Queue(
        displayName="queue_name",
        queueId=queue_id,
        farmId=farm_id,
        status="ENABLED",
        defaultBudgetAction="None",
        jobAttachmentSettings=default_job_attachment_s3_settings,
    )


@pytest.fixture(scope="function")
def create_get_queue_response(response_metadata) -> Callable[[Queue], dict[str, Any]]:
    """
    Fixture used to create get_queue responses
    """

    def _inner_func_(queue_info: Queue):
        response = dict(
            dataclasses.asdict(
                queue_info, dict_factory=lambda x: {k: v for (k, v) in x if v is not None}
            ),
            **response_metadata,
        )

        response["createdAt"] = datetime.strptime(
            "2023-07-13 14:35:26.123456", "%Y-%m-%d %H:%M:%S.%f"
        )
        response["createdBy"] = "job attachments tests"
        response["defaultBudgetAction"] = "None"

        return response

    return _inner_func_


@pytest.fixture(scope="function")
def create_get_job_response(response_metadata) -> Callable[[Job], dict[str, Any]]:
    """
    Fixture used to create get_job responses
    """

    def _inner_func_(job_info: Job):
        now = datetime.now()
        return dict(
            dataclasses.asdict(
                job_info, dict_factory=lambda x: {k: v for (k, v) in x if v is not None}
            ),
            **{
                "jobId": job_info.jobId,
                "createdAt": now,
                "lifecycleStatus": "READY",
                "createdBy": "CreatedBy",
                "taskRunStatusCounts": {"READY": 1},
                "priority": 50,
            },
            **response_metadata,
        )

    return _inner_func_


@pytest.fixture(name="response_metadata")
def fixture_response_metadata():
    """
    Fixture returning a ResponseMetadata to be included in get_queue, get_job response
    """
    return {
        "ResponseMetadata": {
            "RequestId": "abc123",
            "HTTPStatusCode": 200,
            "HostId": "abc123",
        }
    }


@pytest.fixture(name="test_manifest_one")
def fixture_test_manifest_one():
    return {
        "hashAlg": "xxh128",
        "manifestVersion": "2023-03-03",
        "paths": [
            {
                "hash": "a96ddfc33590cd7d2391f1972f66a72a",
                "mtime": 1111111111111111,
                "path": "a.txt",
                "size": 2,
            },
            {
                "hash": "b96ddfc33590cd7d2391f1972f66a72a",
                "mtime": 2222222222222222,
                "path": "b.txt",
                "size": 4,
            },
            {
                "hash": "c96ddfc33590cd7d2391f1972f66a72a",
                "mtime": 3333333333333333,
                "path": "c.txt",
                "size": 6,
            },
        ],
        "totalSize": 12,
    }


@pytest.fixture(name="test_manifest_two")
def fixture_test_manifest_two():
    return {
        "hashAlg": "xxh128",
        "manifestVersion": "2023-03-03",
        "paths": [
            {
                "hash": "a20ddfc33590cd7d2391f1972f66a72a",
                "mtime": 4444444444444444,
                "path": "a.txt",
                "size": 20,
            },
            {
                "hash": "d96ddfc33590cd7d2391f1972f66a72a",
                "mtime": 5555555555555555,
                "path": "d.txt",
                "size": 40,
            },
        ],
        "totalSize": 60,
    }


@pytest.fixture(name="merged_manifest")
def fixture_merged_manifest():
    return {
        "hashAlg": "xxh128",
        "manifestVersion": "2023-03-03",
        "paths": [
            {
                "hash": "a20ddfc33590cd7d2391f1972f66a72a",
                "mtime": 4444444444444444,
                "path": "a.txt",
                "size": 20,
            },
            {
                "hash": "b96ddfc33590cd7d2391f1972f66a72a",
                "mtime": 2222222222222222,
                "path": "b.txt",
                "size": 4,
            },
            {
                "hash": "c96ddfc33590cd7d2391f1972f66a72a",
                "mtime": 3333333333333333,
                "path": "c.txt",
                "size": 6,
            },
            {
                "hash": "d96ddfc33590cd7d2391f1972f66a72a",
                "mtime": 5555555555555555,
                "path": "d.txt",
                "size": 40,
            },
        ],
        "totalSize": 70,
    }


@pytest.fixture(name="really_big_manifest")
def fixture_really_big_manifest():
    return {
        "hashAlg": "xxh128",
        "manifestVersion": "2023-03-03",
        "paths": [
            {
                "hash": "a20ddfc33590cd7d2391f1972f66a72a",
                "mtime": 4444444444444444,
                "path": "a.txt",
                "size": 100000000000000000,  # 100 Petabytes
            },
            {
                "hash": "b96ddfc33590cd7d2391f1972f66a72a",
                "mtime": 2222222222222222,
                "path": "b.txt",
                "size": 200000000000000000,  # 200 Petabytes
            },
        ],
        "totalSize": 300000000000000000,
    }


def has_posix_target_user() -> bool:
    """Returns if the testing environment exported the env variables for doing
    cross-account posix target-user tests.
    """
    return (
        os.environ.get("DEADLINE_JOB_ATTACHMENT_TEST_SUDO_TARGET_USER") is not None
        and os.environ.get("DEADLINE_JOB_ATTACHMENT_TEST_SUDO_TARGET_GROUP") is not None
    )


def has_posix_disjoint_user() -> bool:
    """Returns if the testing environment exported the env variables for doing
    cross-account posix disjoint-user tests.
    """
    return (
        os.environ.get("DEADLINE_JOB_ATTACHMENT_TEST_SUDO_DISJOINT_USER") is not None
        and os.environ.get("DEADLINE_JOB_ATTACHMENT_TEST_SUDO_DISJOINT_GROUP") is not None
    )


@pytest.fixture(scope="function")
def posix_target_group() -> str:
    # Intentionally fail if the var is not defined.
    return os.environ["DEADLINE_JOB_ATTACHMENT_TEST_SUDO_TARGET_GROUP"]


@pytest.fixture(scope="function")
def posix_disjoint_group() -> str:
    # Intentionally fail if the var is not defined.
    return os.environ["DEADLINE_JOB_ATTACHMENT_TEST_SUDO_DISJOINT_GROUP"]


@pytest.fixture(scope="function")
def test_glob_folder() -> str:
    glob_data_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "data", "glob"))
    return glob_data_dir


@pytest.fixture(scope="function")
def glob_config_file() -> str:
    manifest_file = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "data", "glob_config.txt")
    )
    return manifest_file
