# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Tests for downloading files from the Job Attachment CAS."""
from __future__ import annotations

import os
import shutil

from collections import Counter
from dataclasses import dataclass, fields
from io import BytesIO
import json
from pathlib import Path
import sys
import tempfile
from typing import Any, Callable, List
from unittest.mock import MagicMock, call, patch, ANY

import boto3
from botocore.exceptions import BotoCoreError, ClientError, ReadTimeoutError
from botocore.stub import Stubber

import pytest
from moto import mock_aws

import deadline
from deadline.job_attachments.asset_manifests import HashAlgorithm
from deadline.job_attachments.asset_manifests.base_manifest import (
    BaseAssetManifest,
    BaseManifestPath as BaseManifestPath,
)
from deadline.job_attachments.asset_manifests.v2023_03_03 import (
    ManifestPath as ManifestPathv2023_03_03,
)
from deadline.job_attachments.asset_manifests.versions import ManifestVersion
from deadline.job_attachments.download import (
    OutputDownloader,
    download_file,
    download_file_with_s3_key,
    download_files_from_manifests,
    download_files_in_directory,
    get_job_input_output_paths_by_asset_root,
    get_job_input_paths_by_asset_root,
    get_job_output_paths_by_asset_root,
    get_manifest_from_s3,
    handle_existing_vfs,
    mount_vfs_from_manifests,
    merge_asset_manifests,
    _ensure_paths_within_directory,
    _get_asset_root_from_s3,
    _get_tasks_manifests_keys_from_s3,
    VFS_CACHE_REL_PATH_IN_SESSION,
    VFS_MANIFEST_FOLDER_IN_SESSION,
    VFS_MANIFEST_FOLDER_PERMISSIONS,
    VFS_LOGS_FOLDER_IN_SESSION,
)
from deadline.job_attachments.exceptions import (
    AssetSyncError,
    JobAttachmentsError,
    JobAttachmentsS3ClientError,
    MissingAssetRootError,
    PathOutsideDirectoryError,
)
from deadline.job_attachments.models import (
    Attachments,
    FileConflictResolution,
    Job,
    JobAttachmentS3Settings,
    ManifestPathGroup,
    Queue,
)
from deadline.job_attachments.progress_tracker import (
    DownloadSummaryStatistics,
    ProgressReportMetadata,
    ProgressStatus,
)
from deadline.job_attachments.asset_manifests.decode import decode_manifest

from deadline.job_attachments.os_file_permission import (
    PosixFileSystemPermissionSettings,
    WindowsFileSystemPermissionSettings,
    WindowsPermissionEnum,
)
from deadline.job_attachments._utils import _human_readable_file_size

from .conftest import has_posix_target_user, has_posix_disjoint_user
from ..conftest import is_windows_non_admin


@dataclass
class Manifest:
    prefix: str
    manifests: bytes


MANIFESTS_v2022_03_03: List[Manifest] = [
    Manifest(
        "job-1/step-1/task-1-1/session-action-9/manifest1v2023-03-03_output",
        b'{"hashAlg":"xxh128","manifestVersion":"2023-03-03",'
        b'"paths":[{"hash":"test1","mtime":1234000000,"path":"test1.txt","size":1},'
        b'{"hash":"test2","mtime":1234000000,"path":"test/test2.txt","size":1}],'
        b'"totalSize":2}',
    ),
    Manifest(
        "job-1/step-1/task-1-1/session-action-9/manifest2v2023-03-03_output",
        b'{"hashAlg":"xxh128","manifestVersion":"2023-03-03",'
        b'"paths":[{"hash":"test3","mtime":1234000000,"path":"test/test3.txt","size":1},'
        b'{"hash":"test4","mtime":1234000000,"path":"test4.txt","size":1}],'
        b'"totalSize":2}',
    ),
    Manifest(
        "job-1/step-1/task-1-1/session-action-1/manifest2v2023-03-03_output",
        b'{"hashAlg":"xxh128","manifestVersion":"2023-03-03",'
        b'"paths":[{"hash":"test3","mtime":1234000000,"path":"test/test33.txt","size":1},'
        b'{"hash":"test4","mtime":1234000000,"path":"test44.txt","size":1}],'
        b'"totalSize":2}',
    ),
    Manifest(
        "job-1/step-1/task-1-11/session-action-9/manifest7v2023-03-03_output",
        b'{"hashAlg":"xxh128","manifestVersion":"2023-03-03",'
        b'"paths":[{"hash":"test13","mtime":1234000000,"path":"test13.txt","size":1},'
        b'{"hash":"test14","mtime":1234000000,"path":"test/test14.txt","size":1}],'
        b'"totalSize":2}',
    ),
    Manifest(
        "job-1/step-1/task-1-2/session-action-9/manifest3v2023-03-03_output",
        b'{"hashAlg":"xxh128","manifestVersion":"2023-03-03",'
        b'"paths":[{"hash":"test5","mtime":1234000000,"path":"test5.txt","size":1},'
        b'{"hash":"test6","mtime":1234000000,"path":"test/test6.txt","size":1}],'
        b'"totalSize":2}',
    ),
    Manifest(
        "job-1/step-2/task-2-3/session-action-9/manifest4v2023-03-03_output",
        b'{"hashAlg":"xxh128","manifestVersion":"2023-03-03",'
        b'"paths":[{"hash":"test7","mtime":1234000000,"path":"test7.txt","size":1},'
        b'{"hash":"test8","mtime":1234000000,"path":"test/test8.txt","size":1}],'
        b'"totalSize":2}',
    ),
    Manifest(
        "job-1/step-2/task-2-3/session-action-9/manifest5v2023-03-03_output",
        b'{"hashAlg":"xxh128","manifestVersion":"2023-03-03",'
        b'"paths":[{"hash":"test9","mtime":1234000000,"path":"test/test9.txt","size":1},'
        b'{"hash":"test10","mtime":1234000000,"path":"test10.txt","size":1}],'
        b'"totalSize":2}',
    ),
    Manifest(
        "job-1/step-2/task-2-3/session-action-1/manifest5v2023-03-03_output",
        b'{"hashAlg":"xxh128","manifestVersion":"2023-03-03",'
        b'"paths":[{"hash":"test9","mtime":1234000000,"path":"test/test99.txt","size":1},'
        b'{"hash":"test100","mtime":1234000000,"path":"test10.txt","size":1}],'
        b'"totalSize":2}',
    ),
    Manifest(
        "job-1/step-2/task-2-4/session-action-9/manifest6v2023-03-03_output",
        b'{"hashAlg":"xxh128","manifestVersion":"2023-03-03",'
        b'"paths":[{"hash":"test11","mtime":1234000000,"path":"test11.txt","size":1},'
        b'{"hash":"test12","mtime":1234000000,"path":"test/test12.txt","size":1}],'
        b'"totalSize":2}',
    ),
]


MANIFEST_VERSION_TO_MANIFESTS: dict[ManifestVersion, List[Manifest]] = {
    ManifestVersion.v2023_03_03: MANIFESTS_v2022_03_03,
}


INPUT_ASSET_MANIFESTS_V2023_03_03: List[Manifest] = [
    Manifest(
        "Inputs/0000/manifest_input",
        b'{"hashAlg":"xxh128","manifestVersion":"2023-03-03",'
        b'"paths":['
        b'{"hash":"input1","mtime":1234000000,"path":"inputs/input1.txt","size":1},'
        b'{"hash":"input2","mtime":1234000000,"path":"inputs/subdir/input2.txt","size":1},'
        b'{"hash":"input3","mtime":1234000000,"path":"inputs/subdir/input3.txt","size":1},'
        b'{"hash":"input4","mtime":1234000000,"path":"inputs/subdir/subdir2/input4.txt","size":1},'
        b'{"hash":"input5","mtime":1234000000,"path":"inputs/input5.txt","size":1}],'
        b'"totalSize":5}',
    ),
]


MANIFEST_VERSION_TO_INPUT_ASSET_MANIFESTS: dict[ManifestVersion, List[Manifest]] = {
    ManifestVersion.v2023_03_03: INPUT_ASSET_MANIFESTS_V2023_03_03,
}


def assert_download_task_output(
    s3_settings: JobAttachmentS3Settings,
    farm_id,
    queue_id,
    tmp_path: Path,
    expected_files: dict[str, List[Path]],
    expected_total_bytes: int,
    manifest_version: ManifestVersion,
):
    """
    Assert that the expected files are downloaded when download_job_output is called with a task id.
    """
    with patch(
        f"{deadline.__package__}.job_attachments.download._get_asset_root_from_s3",
        return_value=str(tmp_path.resolve()),
    ):
        mock_on_downloading_files = MagicMock(return_value=True)

        output_downloader = OutputDownloader(
            s3_settings=s3_settings,
            farm_id=farm_id,
            queue_id=queue_id,
            job_id="job-1",
            step_id="step-1",
            task_id="task-1-1",
        )

        summary_statistics = output_downloader.download_job_output(
            on_downloading_files=mock_on_downloading_files,
        )

    # Ensure that only the expected files are there and no extras.
    expected_files_set = set().union(*expected_files.values())
    assert expected_files_set == set([path for path in tmp_path.glob("**/*") if path.is_file()])
    # Ensure that all the files from the 2023-03-03 manifest have had the correct mtime set.
    if manifest_version == ManifestVersion.v2023_03_03:
        assert all(path.stat().st_mtime == 1234 for path in tmp_path.glob("**/*") if path.is_file())

    assert_progress_tracker_values(
        manifest_version=manifest_version,
        summary_statistics=summary_statistics,
        expected_files=expected_files,
        expected_total_bytes=expected_total_bytes,
        mock_on_downloading_files=mock_on_downloading_files,
    )


def assert_download_step_output(
    s3_settings: JobAttachmentS3Settings,
    farm_id,
    queue_id,
    tmp_path: Path,
    expected_files: dict[str, List[Path]],
    expected_total_bytes: int,
    manifest_version: ManifestVersion,
):
    """
    Assert that the expected files are downloaded when download_job_output is called with a step id.
    """
    with patch(
        f"{deadline.__package__}.job_attachments.download._get_asset_root_from_s3",
        return_value=str(tmp_path.resolve()),
    ):
        mock_on_downloading_files = MagicMock(return_value=True)

        output_downloader = OutputDownloader(
            s3_settings=s3_settings,
            farm_id=farm_id,
            queue_id=queue_id,
            job_id="job-1",
            step_id="step-1",
            task_id=None,
        )

        summary_statistics = output_downloader.download_job_output(
            on_downloading_files=mock_on_downloading_files,
        )

    # Ensure that only the expected files are there and no extras.
    expected_files_set = set().union(*expected_files.values())
    assert expected_files_set == set([path for path in tmp_path.glob("**/*") if path.is_file()])
    # Ensure that all the files from the 2023-03-03 manifest have had the correct mtime set.
    if manifest_version == ManifestVersion.v2023_03_03:
        assert all(path.stat().st_mtime == 1234 for path in tmp_path.glob("**/*") if path.is_file())

    assert_progress_tracker_values(
        manifest_version=manifest_version,
        summary_statistics=summary_statistics,
        expected_files=expected_files,
        expected_total_bytes=expected_total_bytes,
        mock_on_downloading_files=mock_on_downloading_files,
    )


def assert_download_job_output(
    s3_settings: JobAttachmentS3Settings,
    farm_id,
    queue_id,
    tmp_path: Path,
    expected_files: dict[str, List[Path]],
    expected_total_bytes: int,
    manifest_version: ManifestVersion,
):
    """
    Assert that the expected files are downloaded when download_job_output is called.
    """
    with patch(
        f"{deadline.__package__}.job_attachments.download._get_asset_root_from_s3",
        return_value=str(tmp_path.resolve()),
    ):
        mock_on_downloading_files = MagicMock(return_value=True)

        output_downloader = OutputDownloader(
            s3_settings=s3_settings,
            farm_id=farm_id,
            queue_id=queue_id,
            job_id="job-1",
            step_id=None,
            task_id=None,
        )
        summary_statistics = output_downloader.download_job_output(
            on_downloading_files=mock_on_downloading_files,
        )

    # Ensure that only the expected files are there and no extras.
    expected_files_set = set().union(*expected_files.values())
    assert expected_files_set == set([path for path in tmp_path.glob("**/*") if path.is_file()])
    # Ensure that all the files from the 2023-03-03 manifest have had the correct mtime set.
    if manifest_version == ManifestVersion.v2023_03_03:
        assert all(path.stat().st_mtime == 1234 for path in tmp_path.glob("**/*") if path.is_file())

    assert_progress_tracker_values(
        manifest_version=manifest_version,
        summary_statistics=summary_statistics,
        expected_files=expected_files,
        expected_total_bytes=expected_total_bytes,
        mock_on_downloading_files=mock_on_downloading_files,
    )


def assert_download_files_in_directory(
    s3_settings: JobAttachmentS3Settings,
    attachments: Attachments,
    farm_id: str,
    queue_id: str,
    directory_path: str,
    tmp_path: Path,
    expected_files: dict[str, List[Path]],
    expected_total_bytes: int,
    manifest_version: ManifestVersion,
):
    """
    Assert that the expected files are downloaded when download_files_in_directory is called.
    """
    with patch(
        f"{deadline.__package__}.job_attachments.download._get_asset_root_from_s3",
        return_value=str(tmp_path.resolve()),
    ):
        mock_on_downloading_files = MagicMock(return_value=True)

        summary_statistics = download_files_in_directory(
            s3_settings=s3_settings,
            attachments=attachments,
            farm_id=farm_id,
            queue_id=queue_id,
            job_id="job-1",
            directory_path=directory_path,
            local_download_dir=str(tmp_path.resolve()),
            on_downloading_files=mock_on_downloading_files,
        )

    # Ensure that only the expected files are there and no extras.
    expected_files_set = set().union(*expected_files.values())
    assert expected_files_set == set([path for path in tmp_path.glob("**/*") if path.is_file()])

    assert_progress_tracker_values(
        manifest_version=manifest_version,
        summary_statistics=summary_statistics,
        expected_files=expected_files,
        expected_total_bytes=expected_total_bytes,
        mock_on_downloading_files=mock_on_downloading_files,
    )


def assert_progress_tracker_values(
    manifest_version: ManifestVersion,
    summary_statistics: DownloadSummaryStatistics,
    expected_files: dict[str, List[Path]],
    expected_total_bytes: int,
    mock_on_downloading_files: MagicMock,
):
    readable_total_input_bytes = _human_readable_file_size(expected_total_bytes)
    expected_files_set = set().union(*expected_files.values())
    file_counts_by_root_directory = {root: len(paths) for root, paths in expected_files.items()}

    if manifest_version == ManifestVersion.v2023_03_03:
        expected_progress_message_part = (
            f"Downloaded {readable_total_input_bytes} / {readable_total_input_bytes}"
            f" of {len(expected_files_set)} files (Transfer rate: "
        )
        expected_summary_statistics = DownloadSummaryStatistics(
            total_time=summary_statistics.total_time,
            total_files=len(expected_files_set),
            total_bytes=expected_total_bytes,
            processed_files=len(expected_files_set),
            processed_bytes=expected_total_bytes,
            skipped_files=0,
            skipped_bytes=0,
            transfer_rate=expected_total_bytes / summary_statistics.total_time,
            file_counts_by_root_directory=file_counts_by_root_directory,
        )
    else:
        # If the manifest version does not support `size` and `total_size` properties,
        # the progress is tracked in the number of files instead of bytes.
        expected_progress_message_part = (
            f"Downloaded {len(expected_files_set)}/{len(expected_files_set)} files"
        )
        expected_summary_statistics = DownloadSummaryStatistics(
            total_time=summary_statistics.total_time,
            total_files=len(expected_files_set),
            total_bytes=0,
            processed_files=len(expected_files_set),
            processed_bytes=0,
            skipped_files=0,
            skipped_bytes=0,
            transfer_rate=0.0,
            file_counts_by_root_directory=file_counts_by_root_directory,
        )

    actual_args, _ = mock_on_downloading_files.call_args
    actual_last_progress_report = actual_args[0]
    assert actual_last_progress_report.status == ProgressStatus.DOWNLOAD_IN_PROGRESS
    assert actual_last_progress_report.progress == 100.0
    assert expected_progress_message_part in actual_last_progress_report.progressMessage

    for attribute in fields(expected_summary_statistics):
        assert getattr(summary_statistics, attribute.name) == getattr(
            expected_summary_statistics, attribute.name
        )


def assert_download_job_output_with_task_id_and_no_step_id_throws_error(
    s3_settings: JobAttachmentS3Settings, farm_id, queue_id
):
    """
    Assert a JobAttachmentError is thrown when a task id is provided but step id is not.
    """
    with pytest.raises(JobAttachmentsError):
        mock_on_downloading_files = MagicMock(return_value=True)

        output_downloader = OutputDownloader(
            s3_settings=s3_settings,
            farm_id=farm_id,
            queue_id=queue_id,
            job_id="job-1",
            step_id=None,
            task_id="task-1-1",
        )
        output_downloader.download_job_output(
            on_downloading_files=mock_on_downloading_files,
        )


def assert_get_job_input_paths_by_asset_root(
    s3_settings: JobAttachmentS3Settings,
    attachments: Attachments,
    expected_files: dict[str, List[BaseManifestPath]],
    expected_total_bytes: int,
    manifest_version: ManifestVersion,
):
    """
    Assert that get_job_input_paths_by_asset_root returns a dict of (asset root, manifest path group) of all asset files.
    """
    with patch(
        f"{deadline.__package__}.job_attachments.download.get_job_output_paths_by_asset_root",
        return_value={
            "/tmp": ManifestPathGroup(
                total_bytes=100,
                files_by_hash_alg={
                    HashAlgorithm.XXH128: [
                        ManifestPathv2023_03_03(
                            path="outputs/output.txt", hash="outputhash", size=100, mtime=1234567
                        )
                    ],
                },
            )
        },
    ):
        paths_by_root = get_job_input_paths_by_asset_root(
            s3_settings=s3_settings,
            attachments=attachments,
        )
    assert len(paths_by_root) == len(expected_files)
    total_bytes = 0
    for root, path_group in paths_by_root.items():
        assert len(path_group.files_by_hash_alg) == 1  # assume only one hash alg
        assert path_group.files_by_hash_alg[HashAlgorithm.XXH128] == expected_files[root]
        total_bytes += path_group.total_bytes

    assert total_bytes == expected_total_bytes


def assert_get_job_output_paths_by_asset_root(
    s3_settings: JobAttachmentS3Settings,
    farm_id: str,
    queue_id: str,
    expected_files: dict[str, List[BaseManifestPath]],
    expected_total_bytes: int,
    manifest_version: ManifestVersion,
):
    """
    Assert that get_job_output_paths_by_asset_root returns a list of (hash, path) pairs of all output files.
    """
    with patch(
        f"{deadline.__package__}.job_attachments.download._get_asset_root_from_s3",
        return_value="/test",
    ):
        paths_by_root = get_job_output_paths_by_asset_root(
            s3_settings=s3_settings, farm_id=farm_id, queue_id=queue_id, job_id="job-1"
        )

    assert len(paths_by_root) == len(expected_files)
    total_bytes = 0
    for root, path_group in paths_by_root.items():
        assert len(path_group.files_by_hash_alg) == 1  # assume only one hash alg
        assert path_group.files_by_hash_alg[HashAlgorithm.XXH128] == expected_files[root]
        total_bytes += path_group.total_bytes

    assert total_bytes == expected_total_bytes


def assert_get_job_output_paths_by_asset_root_when_no_asset_root_throws_error(
    farm_id: str,
    queue_id: str,
    s3_settings: JobAttachmentS3Settings,
):
    """
    Assert that get_job_output_paths_by_asset_root raises MissingAssetRootError when fail to get manifest.
    """
    with patch(
        f"{deadline.__package__}.job_attachments.download._get_asset_root_from_s3",
        return_value=None,
    ), pytest.raises(MissingAssetRootError) as raised_err:
        get_job_output_paths_by_asset_root(s3_settings, farm_id, queue_id, "job-1")
    assert "Failed to get asset root from metadata of output manifest:" in str(raised_err.value)


def assert_get_job_input_output_paths_by_asset_root(
    s3_settings: JobAttachmentS3Settings,
    attachments: Attachments,
    farm_id: str,
    queue_id: str,
    expected_files: dict[str, List[BaseManifestPath]],
    expected_total_bytes: int,
    manifest_version: ManifestVersion,
):
    """
    Assert that get_job_input_output_paths_by_asset_root returns a list of (hash, path) pairs of all
    asset files and output files.
    """
    with patch(
        f"{deadline.__package__}.job_attachments.download._get_asset_root_from_s3",
        return_value="/tmp",
    ):
        paths_by_root = get_job_input_output_paths_by_asset_root(
            s3_settings, attachments, farm_id, queue_id, "job-1"
        )

    assert len(paths_by_root) == len(expected_files)
    total_bytes = 0
    for root, path_group in paths_by_root.items():
        assert len(path_group.files_by_hash_alg) == 1  # assume only one hash alg
        assert path_group.files_by_hash_alg[HashAlgorithm.XXH128] == expected_files[root]
        if manifest_version == ManifestVersion.v2023_03_03:
            total_bytes += path_group.total_bytes

    if manifest_version == ManifestVersion.v2023_03_03:
        assert total_bytes == expected_total_bytes


@pytest.mark.docker
@pytest.mark.parametrize("manifest_version", [ManifestVersion.v2023_03_03])
class TestFullDownload:
    """
    Tests for downloads from cas.
    """

    @pytest.fixture(autouse=True)
    def before_test(
        self,
        request,
        create_s3_bucket: Callable[[str], None],
        farm_id: str,
        queue_id: str,
        default_job_attachment_s3_settings: JobAttachmentS3Settings,
        default_queue: Queue,
        default_job: Job,
        create_get_queue_response: Callable[[Queue], dict[str, Any]],
        create_get_job_response: Callable[[Job], dict[str, Any]],
        manifest_version: ManifestVersion,
    ):
        """
        Setup the default queue and s3 bucket for all asset tests.
        Mark test with `no_setup` if you don't want this setup to run.
        """
        if "no_setup" in request.keywords:
            return

        self.job_attachment_settings = default_job_attachment_s3_settings
        self.queue = default_queue
        self.job = default_job
        self.queue_response = create_get_queue_response(self.queue)
        self.job_response = create_get_job_response(self.job)
        create_s3_bucket(default_job_attachment_s3_settings.s3BucketName)

        s3 = boto3.Session(region_name="us-west-2").resource("s3")  # pylint: disable=invalid-name
        bucket = s3.Bucket(self.job_attachment_settings.s3BucketName)

        for i in range(1, 15):
            bucket.upload_fileobj(
                BytesIO(b"a"),
                f"{self.job_attachment_settings.rootPrefix}/Data/test{i}.xxh128",
            )

        for i in range(1, 6):
            bucket.upload_fileobj(
                BytesIO(b"a"),
                f"{self.job_attachment_settings.rootPrefix}/Data/input{i}.xxh128",
            )

        for manifest in MANIFEST_VERSION_TO_MANIFESTS[manifest_version]:
            bucket.upload_fileobj(
                BytesIO(manifest.manifests),
                f"{self.job_attachment_settings.rootPrefix}/"
                f"Manifests/{farm_id}/{queue_id}/{manifest.prefix}",
            )

        for manifest in MANIFEST_VERSION_TO_INPUT_ASSET_MANIFESTS[manifest_version]:
            bucket.upload_fileobj(
                BytesIO(manifest.manifests),
                f"{self.job_attachment_settings.rootPrefix}/Manifests/{farm_id}/{queue_id}/{manifest.prefix}",
            )

        # Put random junk in the outputs prefix to make sure it isn't downloaded.
        bucket.upload_fileobj(
            BytesIO(b"a"),
            f"{self.job_attachment_settings.rootPrefix}/"
            f"Manifests/{farm_id}/{queue_id}/job-1/step-1/task-1-1/junk",
        )

        bucket.upload_fileobj(
            BytesIO(b"a"),
            f"{self.job_attachment_settings.rootPrefix}/"
            f"Manifests/{farm_id}/{queue_id}/job-1/step-1/junk.json",
        )

        bucket.upload_fileobj(
            BytesIO(b"a"),
            f"{self.job_attachment_settings.rootPrefix}/"
            f"Manifests/{farm_id}/{queue_id}/job-1/junk2.json",
        )

    INPUT_MANIFEST_PATHS_BY_ASSET_ROOT_v2023_03_03: list[BaseManifestPath] = [
        ManifestPathv2023_03_03(path="inputs/input1.txt", hash="input1", size=1, mtime=1234000000),
        ManifestPathv2023_03_03(
            path="inputs/subdir/input2.txt", hash="input2", size=1, mtime=1234000000
        ),
        ManifestPathv2023_03_03(
            path="inputs/subdir/input3.txt", hash="input3", size=1, mtime=1234000000
        ),
        ManifestPathv2023_03_03(
            path="inputs/subdir/subdir2/input4.txt", hash="input4", size=1, mtime=1234000000
        ),
        ManifestPathv2023_03_03(path="inputs/input5.txt", hash="input5", size=1, mtime=1234000000),
    ]
    INPUT_MANIFEST_VERSION_TO_ASSET_ROOT_PATHS: dict[ManifestVersion, list[BaseManifestPath]] = {
        ManifestVersion.v2023_03_03: INPUT_MANIFEST_PATHS_BY_ASSET_ROOT_v2023_03_03,
    }

    @mock_aws
    def test_get_job_input_paths_by_asset_root(self, manifest_version: ManifestVersion):
        assert self.job.attachments is not None
        assert_get_job_input_paths_by_asset_root(
            self.job_attachment_settings,
            self.job.attachments,
            {"/tmp": self.INPUT_MANIFEST_VERSION_TO_ASSET_ROOT_PATHS[manifest_version]},
            5,
            manifest_version,
        )

    MANIFEST_PATHS_BY_ASSET_ROOT_v2023_03_03: list[BaseManifestPath] = [
        ManifestPathv2023_03_03(path="test1.txt", hash="test1", size=1, mtime=1234000000),
        ManifestPathv2023_03_03(path="test/test2.txt", hash="test2", size=1, mtime=1234000000),
        ManifestPathv2023_03_03(path="test/test3.txt", hash="test3", size=1, mtime=1234000000),
        ManifestPathv2023_03_03(path="test4.txt", hash="test4", size=1, mtime=1234000000),
        ManifestPathv2023_03_03(path="test13.txt", hash="test13", size=1, mtime=1234000000),
        ManifestPathv2023_03_03(path="test/test14.txt", hash="test14", size=1, mtime=1234000000),
        ManifestPathv2023_03_03(path="test5.txt", hash="test5", size=1, mtime=1234000000),
        ManifestPathv2023_03_03(path="test/test6.txt", hash="test6", size=1, mtime=1234000000),
        ManifestPathv2023_03_03(path="test7.txt", hash="test7", size=1, mtime=1234000000),
        ManifestPathv2023_03_03(path="test/test8.txt", hash="test8", size=1, mtime=1234000000),
        ManifestPathv2023_03_03(path="test/test9.txt", hash="test9", size=1, mtime=1234000000),
        ManifestPathv2023_03_03(path="test10.txt", hash="test10", size=1, mtime=1234000000),
        ManifestPathv2023_03_03(path="test11.txt", hash="test11", size=1, mtime=1234000000),
        ManifestPathv2023_03_03(path="test/test12.txt", hash="test12", size=1, mtime=1234000000),
    ]

    MANIFEST_VERSION_TO_ASSET_ROOT_PATHS: dict[ManifestVersion, list[BaseManifestPath]] = {
        ManifestVersion.v2023_03_03: MANIFEST_PATHS_BY_ASSET_ROOT_v2023_03_03,
    }

    @mock_aws
    def test_get_job_output_paths_by_asset_root(
        self, farm_id, queue_id, manifest_version: ManifestVersion
    ):
        assert_get_job_output_paths_by_asset_root(
            self.job_attachment_settings,
            farm_id,
            queue_id,
            {"/test": self.MANIFEST_VERSION_TO_ASSET_ROOT_PATHS[manifest_version]},
            14,
            manifest_version,
        )

    @mock_aws
    def test_get_job_outputs_paths_by_asset_root_when_no_asset_root(self, farm_id, queue_id):
        assert_get_job_output_paths_by_asset_root_when_no_asset_root_throws_error(
            farm_id, queue_id, self.job_attachment_settings
        )

    @mock_aws
    def test_get_job_input_output_paths_by_asset_root(
        self, farm_id, queue_id, manifest_version: ManifestVersion
    ):
        assert self.job.attachments is not None
        assert_get_job_input_output_paths_by_asset_root(
            self.job_attachment_settings,
            self.job.attachments,
            farm_id,
            queue_id,
            {
                "/tmp": self.INPUT_MANIFEST_VERSION_TO_ASSET_ROOT_PATHS[manifest_version]
                + self.MANIFEST_VERSION_TO_ASSET_ROOT_PATHS[manifest_version],
            },
            19,
            manifest_version,
        )

    EXPECTED_DOWNLOAD_FILE_PATHS_RELATIVE = [
        "inputs/input1.txt",
        "inputs/subdir/input2.txt",
        "inputs/subdir/input3.txt",
        "inputs/subdir/subdir2/input4.txt",
        "inputs/input5.txt",
    ]

    TARGET_PERMISSION_CHANGE_PATHS_RELATIVE = [
        "inputs/input1.txt",
        ".",
        "inputs",
        "inputs/subdir/input2.txt",
        "inputs/subdir",
        "inputs/subdir/input3.txt",
        "inputs/subdir/subdir2/input4.txt",
        "inputs/subdir/subdir2",
        "inputs/input5.txt",
    ]

    @mock_aws
    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="This test is for testing file permission changes in Posix-based OS.",
    )
    def test_download_files_from_manifests_with_fs_permission_settings_posix(
        self,
        tmp_path: Path,
        manifest_version: ManifestVersion,
    ):
        """
        Tests whether the files listed in the given manifest are downloaded correctly from the
        S3 bucket. Also, verifies that the functions for changing file ownership and permissions
        (i.e., chown & chmod for POSIX) are correctly called with the given permission settings.
        """
        manifest_str = MANIFEST_VERSION_TO_INPUT_ASSET_MANIFESTS[manifest_version][
            0
        ].manifests.decode("utf-8")
        manifest = decode_manifest(manifest_str)
        manifests_by_root = {str(tmp_path): manifest}

        fs_permission_settings = PosixFileSystemPermissionSettings(
            os_user="test-user",
            os_group="test-group",
            dir_mode=0o20,
            file_mode=0o20,
        )

        mock_on_downloading_files = MagicMock(return_value=True)

        # IF
        with patch("shutil.chown") as mock_chown, patch("os.chmod") as mock_chmod:
            _ = download_files_from_manifests(
                s3_bucket=self.job_attachment_settings.s3BucketName,
                manifests_by_root=manifests_by_root,
                cas_prefix=self.job_attachment_settings.full_cas_prefix(),
                fs_permission_settings=fs_permission_settings,
                on_downloading_files=mock_on_downloading_files,
            )

        # THEN
        # Ensure that `chown` and `chmod` are properly called with the given permission settings
        # for the downloaded files (and directory) paths.
        expected_changed_paths = [
            tmp_path / rel_path for rel_path in self.TARGET_PERMISSION_CHANGE_PATHS_RELATIVE
        ]

        chown_expected_calls = [
            str(call(path, group="test-group")) for path in expected_changed_paths
        ]
        chown_actual_calls = [str(call_args) for call_args in mock_chown.call_args_list]
        assert Counter(chown_actual_calls) == Counter(chown_expected_calls)

        chmod_expected_calls = [
            str(call(path, path.stat().st_mode | 0o20)) for path in expected_changed_paths
        ]
        chmod_actual_calls = [str(call_args) for call_args in mock_chmod.call_args_list]
        assert Counter(chmod_actual_calls) == Counter(chmod_expected_calls)

        # Ensure that only the expected files are there and no extras.
        expected_files = [
            tmp_path / rel_path for rel_path in self.EXPECTED_DOWNLOAD_FILE_PATHS_RELATIVE
        ]
        assert set(expected_files) == set(
            [path for path in tmp_path.glob("**/*") if path.is_file()]
        )

    @mock_aws
    @pytest.mark.skipif(
        sys.platform != "win32",
        reason="This test is for testing file permission changes in Windows.",
    )
    def test_download_files_from_manifests_with_fs_permission_settings_windows(
        self,
        tmp_path: Path,
        manifest_version: ManifestVersion,
    ):
        """
        Tests whether the files listed in the given manifest are downloaded correctly from the
        S3 bucket. Also, verifies that the function for changing file ownership and permissions
        is correctly called with the given permission settings.
        """
        manifest_str = MANIFEST_VERSION_TO_INPUT_ASSET_MANIFESTS[manifest_version][
            0
        ].manifests.decode("utf-8")
        manifest = decode_manifest(manifest_str)
        manifests_by_root = {str(tmp_path): manifest}

        fs_permission_settings = WindowsFileSystemPermissionSettings(
            os_user="test-user",
            dir_mode=WindowsPermissionEnum.FULL_CONTROL,
            file_mode=WindowsPermissionEnum.FULL_CONTROL,
        )

        mock_on_downloading_files = MagicMock(return_value=True)

        # IF
        with patch(
            f"{deadline.__package__}.job_attachments.os_file_permission._change_permission_for_windows"
        ) as mock_change_permission:
            _ = download_files_from_manifests(
                s3_bucket=self.job_attachment_settings.s3BucketName,
                manifests_by_root=manifests_by_root,
                cas_prefix=self.job_attachment_settings.full_cas_prefix(),
                fs_permission_settings=fs_permission_settings,
                on_downloading_files=mock_on_downloading_files,
            )

        # THEN
        # Ensure that `_change_permission_for_windows` are properly called with the given
        # permission settings for the downloaded files (and directory) paths.
        expected_changed_paths = [
            tmp_path / rel_path for rel_path in self.TARGET_PERMISSION_CHANGE_PATHS_RELATIVE
        ]

        mock_change_permission_expected_calls = [
            str(
                call(
                    str(path),
                    "test-user",
                    WindowsPermissionEnum.FULL_CONTROL,
                )
            )
            for path in expected_changed_paths
        ]
        mock_change_permission_actual_calls = [
            str(call_args) for call_args in mock_change_permission.call_args_list
        ]
        assert Counter(mock_change_permission_actual_calls) == Counter(
            mock_change_permission_expected_calls
        )

        # Ensure that only the expected files are there and no extras.
        expected_files = [
            tmp_path / rel_path for rel_path in self.EXPECTED_DOWNLOAD_FILE_PATHS_RELATIVE
        ]
        assert set(expected_files) == set(
            [path for path in tmp_path.glob("**/*") if path.is_file()]
        )

    @mock_aws
    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="This test is for testing file permission changes in Posix-based OS.",
    )
    @pytest.mark.xfail(
        not (has_posix_target_user() and has_posix_disjoint_user()),
        reason="Must be running inside of the sudo_environment testing container.",
    )
    def test_download_files_from_manifests_have_correct_group_posix(
        self,
        tmp_path: Path,
        manifest_version: ManifestVersion,
        posix_target_group: str,
        posix_disjoint_group: str,
    ):
        """
        Tests whether the file system ownership and permissions of the downloaded files
        are correctly changed on POSIX-based environment.
        """
        import grp

        # Creates some files in the root directory that were not downloaded by Job Attachment.
        Path(tmp_path / "inputs/subdir/subdir2").mkdir(parents=True, exist_ok=True)
        random_paths = [
            tmp_path / "not_asset.txt",
            tmp_path / "inputs/not_asset.txt",
            tmp_path / "inputs/subdir/not_asset.txt",
            tmp_path / "inputs/subdir/subdir2/not_asset.txt",
        ]
        for path in random_paths:
            with open(str(path), "w") as f:
                f.write("I am a pre-existing file, not downloaded by Job Attachment.")

        manifest_str = MANIFEST_VERSION_TO_INPUT_ASSET_MANIFESTS[manifest_version][
            0
        ].manifests.decode("utf-8")
        manifest = decode_manifest(manifest_str)
        manifests_by_root = {str(tmp_path): manifest}

        fs_permission_settings = PosixFileSystemPermissionSettings(
            os_user="test-user",
            os_group=posix_target_group,
            dir_mode=0o20,
            file_mode=0o20,
        )

        mock_on_downloading_files = MagicMock(return_value=True)

        # IF
        _ = download_files_from_manifests(
            s3_bucket=self.job_attachment_settings.s3BucketName,
            manifests_by_root=manifests_by_root,
            cas_prefix=self.job_attachment_settings.full_cas_prefix(),
            fs_permission_settings=fs_permission_settings,
            on_downloading_files=mock_on_downloading_files,
        )

        # THEN
        expected_changed_paths = [
            tmp_path / rel_path for rel_path in self.TARGET_PERMISSION_CHANGE_PATHS_RELATIVE
        ]

        # Verify that the group ownership and permissions of files downloaded through Job Attachment
        # have been appropriately modified.
        # Also, confirm that a permission error occurs when attempting to change the group ownership
        # of those files to a group other than the target group.
        for path in expected_changed_paths:
            file_stat = os.stat(str(path))
            updated_mode = file_stat.st_mode
            assert updated_mode == updated_mode | 0o20

            updated_group_name = grp.getgrgid(file_stat.st_gid).gr_name  # type: ignore
            assert updated_group_name == posix_target_group

            with pytest.raises(PermissionError):
                shutil.chown(path, group=posix_disjoint_group)

        # For the files that were not downloaded through Job Attachment, confirm that the group ownership
        # has not been changed to the target group.
        for path in random_paths:
            group_name = grp.getgrgid(os.stat(str(path)).st_gid).gr_name  # type: ignore
            assert group_name != posix_target_group

    @mock_aws
    @pytest.mark.skipif(
        sys.platform != "win32",
        reason="This test is for testing file permission changes in Windows.",
    )
    def test_download_files_from_manifests_have_correct_group_windows(
        self,
        tmp_path: Path,
        manifest_version: ManifestVersion,
    ):
        """
        Tests whether the file system ownership and permissions of the downloaded files
        are correctly changed on Windows environment.
        """
        import win32security
        import ntsecuritycon

        # Creates some files in the root directory that were not downloaded by Job Attachment.
        Path(tmp_path / "inputs/subdir/subdir2").mkdir(parents=True, exist_ok=True)
        random_paths = [
            tmp_path / "not_asset.txt",
            tmp_path / "inputs/not_asset.txt",
            tmp_path / "inputs/subdir/not_asset.txt",
            tmp_path / "inputs/subdir/subdir2/not_asset.txt",
        ]
        for path in random_paths:
            with open(str(path), "w") as f:
                f.write("I am a pre-existing file, not downloaded by Job Attachment.")

        manifest_str = MANIFEST_VERSION_TO_INPUT_ASSET_MANIFESTS[manifest_version][
            0
        ].manifests.decode("utf-8")
        manifest = decode_manifest(manifest_str)
        manifests_by_root = {str(tmp_path): manifest}

        # Use a builtin user 'Guest', so we can expect it to exist on any Windows machine
        fs_permission_settings = WindowsFileSystemPermissionSettings(
            os_user="Guest",
            dir_mode=WindowsPermissionEnum.FULL_CONTROL,
            file_mode=WindowsPermissionEnum.FULL_CONTROL,
        )

        mock_on_downloading_files = MagicMock(return_value=True)

        # IF
        _ = download_files_from_manifests(
            s3_bucket=self.job_attachment_settings.s3BucketName,
            manifests_by_root=manifests_by_root,
            cas_prefix=self.job_attachment_settings.full_cas_prefix(),
            fs_permission_settings=fs_permission_settings,
            on_downloading_files=mock_on_downloading_files,
        )

        # THEN
        expected_changed_paths = [
            tmp_path / rel_path for rel_path in self.TARGET_PERMISSION_CHANGE_PATHS_RELATIVE
        ]

        # Verify that the user ownership and permissions of files downloaded through Job Attachment
        # have been appropriately modified.
        for path in expected_changed_paths:
            # Get the file's security information
            sd = win32security.GetFileSecurity(str(path), win32security.DACL_SECURITY_INFORMATION)
            # Get the discretionary access control list (DACL)
            dacl = sd.GetSecurityDescriptorDacl()
            # Get the permissions info from ACE
            permission_mapping: dict[str, int] = {}
            for ace_no in range(dacl.GetAceCount()):
                trustee_sid = dacl.GetAce(ace_no)[2]
                trustee_name, _, _ = win32security.LookupAccountSid(None, trustee_sid)
                if trustee_name:
                    trustee = {
                        "TrusteeForm": win32security.TRUSTEE_IS_SID,
                        "TrusteeType": win32security.TRUSTEE_IS_USER,
                        "Identifier": trustee_sid,
                    }
                    result = dacl.GetEffectiveRightsFromAcl(trustee)
                    permission_mapping[trustee_name] = result
            assert "Guest" in permission_mapping
            assert permission_mapping["Guest"] == ntsecuritycon.FILE_ALL_ACCESS

    @mock_aws
    def test_download_task_output(
        self, farm_id, queue_id, tmp_path: Path, manifest_version: ManifestVersion
    ):
        assert_download_task_output(
            self.job_attachment_settings,
            farm_id,
            queue_id,
            tmp_path,
            expected_files={
                str(tmp_path): [
                    tmp_path / "test1.txt",
                    tmp_path / "test" / "test2.txt",
                    tmp_path / "test" / "test3.txt",
                    tmp_path / "test4.txt",
                ]
            },
            expected_total_bytes=4,
            manifest_version=manifest_version,
        )

    @mock_aws
    def test_download_step_output(
        self, farm_id, queue_id, tmp_path: Path, manifest_version: ManifestVersion
    ):
        assert_download_step_output(
            self.job_attachment_settings,
            farm_id,
            queue_id,
            tmp_path,
            expected_files={
                str(tmp_path): [
                    tmp_path / "test1.txt",
                    tmp_path / "test" / "test2.txt",
                    tmp_path / "test" / "test3.txt",
                    tmp_path / "test4.txt",
                    tmp_path / "test13.txt",
                    tmp_path / "test" / "test14.txt",
                    tmp_path / "test5.txt",
                    tmp_path / "test" / "test6.txt",
                ]
            },
            expected_total_bytes=8,
            manifest_version=manifest_version,
        )

    @mock_aws
    def test_download_job_output(
        self, farm_id, queue_id, tmp_path: Path, manifest_version: ManifestVersion
    ):
        assert_download_job_output(
            self.job_attachment_settings,
            farm_id,
            queue_id,
            tmp_path,
            expected_files={
                str(tmp_path): [
                    tmp_path / "test1.txt",
                    tmp_path / "test" / "test2.txt",
                    tmp_path / "test" / "test3.txt",
                    tmp_path / "test4.txt",
                    tmp_path / "test13.txt",
                    tmp_path / "test" / "test14.txt",
                    tmp_path / "test5.txt",
                    tmp_path / "test" / "test6.txt",
                    tmp_path / "test7.txt",
                    tmp_path / "test" / "test8.txt",
                    tmp_path / "test" / "test9.txt",
                    tmp_path / "test10.txt",
                    tmp_path / "test11.txt",
                    tmp_path / "test" / "test12.txt",
                ]
            },
            expected_total_bytes=14,
            manifest_version=manifest_version,
        )

    @mock_aws
    def test_download_files_in_directory(
        self, farm_id, queue_id, tmp_path: Path, manifest_version: ManifestVersion
    ):
        assert self.job.attachments is not None
        assert_download_files_in_directory(
            self.job_attachment_settings,
            self.job.attachments,
            farm_id,
            queue_id,
            "test",
            tmp_path,
            expected_files={
                str(tmp_path): [
                    tmp_path / "test" / "test2.txt",
                    tmp_path / "test" / "test3.txt",
                    tmp_path / "test" / "test14.txt",
                    tmp_path / "test" / "test6.txt",
                    tmp_path / "test" / "test8.txt",
                    tmp_path / "test" / "test9.txt",
                    tmp_path / "test" / "test12.txt",
                ]
            },
            expected_total_bytes=7,
            manifest_version=manifest_version,
        )

    @mock_aws
    def test_OutputDownloader_get_output_paths_by_root(
        self,
        farm_id,
        queue_id,
        tmp_path: Path,
    ):
        with patch(
            f"{deadline.__package__}.job_attachments.download._get_asset_root_from_s3",
            return_value=str(tmp_path.resolve()),
        ):
            output_downloader = OutputDownloader(
                s3_settings=self.job_attachment_settings,
                farm_id=farm_id,
                queue_id=queue_id,
                job_id="job-1",
                step_id=None,
                task_id=None,
            )

        assert output_downloader.get_output_paths_by_root() == {
            str(tmp_path.resolve()): [
                "test/test12.txt",
                "test/test14.txt",
                "test/test2.txt",
                "test/test3.txt",
                "test/test6.txt",
                "test/test8.txt",
                "test/test9.txt",
                "test1.txt",
                "test10.txt",
                "test11.txt",
                "test13.txt",
                "test4.txt",
                "test5.txt",
                "test7.txt",
            ]
        }

    @mock_aws
    def test_OutputDownloader_set_root_path(self, farm_id, queue_id, tmp_path: Path):
        with patch(
            f"{deadline.__package__}.job_attachments.download._get_asset_root_from_s3",
            return_value=str(tmp_path.resolve()),
        ):
            output_downloader = OutputDownloader(
                s3_settings=self.job_attachment_settings,
                farm_id=farm_id,
                queue_id=queue_id,
                job_id="job-1",
                step_id=None,
                task_id=None,
            )

        new_root_path = "/new_root_path" if sys.platform != "win32" else "C:\\new_root_path"

        output_downloader.set_root_path(
            original_root=str(tmp_path.resolve()), new_root=new_root_path
        )

        assert output_downloader.get_output_paths_by_root() == {
            new_root_path: [
                "test/test12.txt",
                "test/test14.txt",
                "test/test2.txt",
                "test/test3.txt",
                "test/test6.txt",
                "test/test8.txt",
                "test/test9.txt",
                "test1.txt",
                "test10.txt",
                "test11.txt",
                "test13.txt",
                "test4.txt",
                "test5.txt",
                "test7.txt",
            ]
        }

    @pytest.mark.skipif(
        is_windows_non_admin(),
        reason="Windows requires Admin to create symlinks, skipping this test.",
    )
    @mock_aws
    def test_OutputDownloader_set_root_path_with_symlinks(self, farm_id, queue_id, tmp_path: Path):
        """
        Test that when a symlink path containing '..' is used as a new root. Without
        resolving the symlink target, the absolute path with ".." removed is stored.
        """
        with patch(
            f"{deadline.__package__}.job_attachments.download._get_asset_root_from_s3",
            return_value=str(tmp_path.resolve()),
        ):
            output_downloader = OutputDownloader(
                s3_settings=self.job_attachment_settings,
                farm_id=farm_id,
                queue_id=queue_id,
                job_id="job-1",
                step_id="step-1",
                task_id="task-1-1",
            )

        target_path = tmp_path / "target"
        target_path.mkdir()
        sym_path = tmp_path / "subfolder/../symlink_folder"
        sym_path.parent.mkdir(parents=True, exist_ok=True)
        sym_path.symlink_to(target_path, target_is_directory=True)
        output_downloader.set_root_path(
            original_root=str(tmp_path.resolve()), new_root=str(sym_path)
        )

        assert output_downloader.get_output_paths_by_root() == {
            str(tmp_path / "symlink_folder"): [
                "test/test2.txt",
                "test/test3.txt",
                "test1.txt",
                "test4.txt",
            ]
        }

    @mock_aws
    def test_OutputDownloader_set_root_path_wrong_root_throws_exception(
        self, farm_id, queue_id, tmp_path: Path
    ):
        """
        Assert a ValueError is thrown when given a non-existent root path.
        """
        with patch(
            f"{deadline.__package__}.job_attachments.download._get_asset_root_from_s3",
            return_value=str(tmp_path.resolve()),
        ):
            output_downloader = OutputDownloader(
                s3_settings=self.job_attachment_settings,
                farm_id=farm_id,
                queue_id=queue_id,
                job_id="job-1",
                step_id=None,
                task_id=None,
            )

        with pytest.raises(ValueError):
            output_downloader.set_root_path(original_root="/wrong_root", new_root="/new_root_path")

    @mock_aws
    def test_OutputDownloader_download_job_output_when_skip(
        self, farm_id, queue_id, tmp_path: Path
    ):
        """
        When path conflicts occur during file download and the resolution method is set to SKIP,
        test whether the files has actually been skipped.
        Note: This test relies on `st_ctime` for checking if a file has been skipped. On Linux,
        `st_ctime` represents the time of the last metadata change, but on Windows, it represents
        the file creation time. So the skipping verification is only available on Linux.
        """
        expected_files = [
            tmp_path / "test1.txt",
            tmp_path / "test" / "test2.txt",
            tmp_path / "test" / "test3.txt",
            tmp_path / "test4.txt",
            tmp_path / "test13.txt",
            tmp_path / "test" / "test14.txt",
            tmp_path / "test5.txt",
            tmp_path / "test" / "test6.txt",
            tmp_path / "test7.txt",
            tmp_path / "test" / "test8.txt",
            tmp_path / "test" / "test9.txt",
            tmp_path / "test10.txt",
            tmp_path / "test11.txt",
            tmp_path / "test" / "test12.txt",
        ]

        with patch(
            f"{deadline.__package__}.job_attachments.download._get_asset_root_from_s3",
            return_value=str(tmp_path.resolve()),
        ):
            output_downloader = OutputDownloader(
                s3_settings=self.job_attachment_settings,
                farm_id=farm_id,
                queue_id=queue_id,
                job_id="job-1",
                step_id=None,
                task_id=None,
            )

        # First download the files and check if the files are there.
        # (Ensure that only the expected files are there and no extras.)
        output_downloader.download_job_output()
        assert set(expected_files) == set(
            [path for path in tmp_path.glob("**/*") if path.is_file()]
        )
        # Record the last metadata modification times for each file.
        modified_time_before_second_trial = [path.stat().st_ctime for path in expected_files]
        # Re-download the files with the SKIP option.
        output_downloader.download_job_output(file_conflict_resolution=FileConflictResolution.SKIP)
        # Check that no additional files were added during the second download.
        assert set(expected_files) == set(
            [path for path in tmp_path.glob("**/*") if path.is_file()]
        )

        # (Test only on Linux system) Record the last metadata modification times again.
        # Since the second download with the SKIP option should have skipped the files,
        # the modification times should be the same before and after the second download.
        if sys.platform == "linux":
            modified_time_after_second_trial = [path.stat().st_ctime for path in expected_files]
            assert modified_time_before_second_trial == modified_time_after_second_trial

    @mock_aws
    def test_OutputDownloader_download_job_output_when_overwrite(
        self, farm_id, queue_id, tmp_path: Path
    ):
        """
        When path conflicts occur during file download and the resolution method is set to OVERWRITE,
        test whether the files has actually been overwritten.
        Note: This test relies on `st_ctime` for checking if a file has been overwritten. On Linux,
        `st_ctime` represents the time of the last metadata change, but on Windows, it represents
        the file creation time. So the overwriting verification is only available on Linux.
        """
        expected_files = [
            tmp_path / "test1.txt",
            tmp_path / "test" / "test2.txt",
            tmp_path / "test" / "test3.txt",
            tmp_path / "test4.txt",
            tmp_path / "test13.txt",
            tmp_path / "test" / "test14.txt",
            tmp_path / "test5.txt",
            tmp_path / "test" / "test6.txt",
            tmp_path / "test7.txt",
            tmp_path / "test" / "test8.txt",
            tmp_path / "test" / "test9.txt",
            tmp_path / "test10.txt",
            tmp_path / "test11.txt",
            tmp_path / "test" / "test12.txt",
        ]

        with patch(
            f"{deadline.__package__}.job_attachments.download._get_asset_root_from_s3",
            return_value=str(tmp_path.resolve()),
        ):
            output_downloader = OutputDownloader(
                s3_settings=self.job_attachment_settings,
                farm_id=farm_id,
                queue_id=queue_id,
                job_id="job-1",
                step_id=None,
                task_id=None,
            )

        # First download the files and check if the files are there.
        # (Ensure that only the expected files are there and no extras.)
        output_downloader.download_job_output()
        assert set(expected_files) == set(
            [path for path in tmp_path.glob("**/*") if path.is_file()]
        )
        # Record the last metadata modification times for each file.
        modified_time_before_overwrite = [path.stat().st_ctime for path in expected_files]
        # Re-download the files with the OVERWRITE option.
        output_downloader.download_job_output(
            file_conflict_resolution=FileConflictResolution.OVERWRITE
        )
        # Check that no additional files were added during the second download.
        assert set(expected_files) == set(
            [path for path in tmp_path.glob("**/*") if path.is_file()]
        )

        # (Test only on Linux system) Record the last metadata modification times again.
        # The modification times before and after the second download should be different.
        if sys.platform == "linux":
            modified_time_after_overwrite = [path.stat().st_ctime for path in expected_files]
            for time_before, time_after in zip(
                modified_time_before_overwrite, modified_time_after_overwrite
            ):
                assert time_before < time_after

    @mock_aws
    def test_OutputDownloader_download_job_output_when_create_copy(
        self, farm_id, queue_id, tmp_path: Path
    ):
        expected_files = [
            tmp_path / "test1.txt",
            tmp_path / "test" / "test2.txt",
            tmp_path / "test" / "test3.txt",
            tmp_path / "test4.txt",
            tmp_path / "test13.txt",
            tmp_path / "test" / "test14.txt",
            tmp_path / "test5.txt",
            tmp_path / "test" / "test6.txt",
            tmp_path / "test7.txt",
            tmp_path / "test" / "test8.txt",
            tmp_path / "test" / "test9.txt",
            tmp_path / "test10.txt",
            tmp_path / "test11.txt",
            tmp_path / "test" / "test12.txt",
        ]

        expected_files_after_create_copy = [
            tmp_path / "test1 (1).txt",
            tmp_path / "test" / "test2 (1).txt",
            tmp_path / "test" / "test3 (1).txt",
            tmp_path / "test4 (1).txt",
            tmp_path / "test13 (1).txt",
            tmp_path / "test" / "test14 (1).txt",
            tmp_path / "test5 (1).txt",
            tmp_path / "test" / "test6 (1).txt",
            tmp_path / "test7 (1).txt",
            tmp_path / "test" / "test8 (1).txt",
            tmp_path / "test" / "test9 (1).txt",
            tmp_path / "test10 (1).txt",
            tmp_path / "test11 (1).txt",
            tmp_path / "test" / "test12 (1).txt",
        ]

        expected_files_after_create_copy.extend(expected_files)

        with patch(
            f"{deadline.__package__}.job_attachments.download._get_asset_root_from_s3",
            return_value=str(tmp_path.resolve()),
        ):
            output_downloader = OutputDownloader(
                s3_settings=self.job_attachment_settings,
                farm_id=farm_id,
                queue_id=queue_id,
                job_id="job-1",
                step_id=None,
                task_id=None,
            )

        # First download the files and check if the files are there.
        output_downloader.download_job_output()
        assert set(expected_files) == set(
            [path for path in tmp_path.glob("**/*") if path.is_file()]
        )
        # Re-download the files with the CREATE_COPY option.
        output_downloader.download_job_output(
            file_conflict_resolution=FileConflictResolution.CREATE_COPY
        )
        assert set(expected_files_after_create_copy) == set(
            [path for path in tmp_path.glob("**/*") if path.is_file()]
        )

    @mock_aws
    def test_OutputDownloader_download_job_output_unknown_resolution_throws_exception(
        self, farm_id, queue_id, tmp_path: Path
    ):
        with patch(
            f"{deadline.__package__}.job_attachments.download._get_asset_root_from_s3",
            return_value=str(tmp_path.resolve()),
        ):
            output_downloader = OutputDownloader(
                s3_settings=self.job_attachment_settings,
                farm_id=farm_id,
                queue_id=queue_id,
                job_id="job-1",
                step_id=None,
                task_id=None,
            )

        output_downloader.download_job_output()
        with pytest.raises(ValueError):
            output_downloader.download_job_output(
                file_conflict_resolution=FileConflictResolution(99)
            )

    @mock_aws
    def test_OutputDownloader_download_job_output_to_new_asset_root(
        self, farm_id, queue_id, tmp_path: Path
    ):
        expected_files = [
            tmp_path / "test1.txt",
            tmp_path / "test" / "test2.txt",
            tmp_path / "test" / "test3.txt",
            tmp_path / "test4.txt",
            tmp_path / "test13.txt",
            tmp_path / "test" / "test14.txt",
            tmp_path / "test5.txt",
            tmp_path / "test" / "test6.txt",
            tmp_path / "test7.txt",
            tmp_path / "test" / "test8.txt",
            tmp_path / "test" / "test9.txt",
            tmp_path / "test10.txt",
            tmp_path / "test11.txt",
            tmp_path / "test" / "test12.txt",
        ]

        with patch(
            f"{deadline.__package__}.job_attachments.download._get_asset_root_from_s3",
            return_value="/test_root",
        ):
            output_downloader = OutputDownloader(
                s3_settings=self.job_attachment_settings,
                farm_id=farm_id,
                queue_id=queue_id,
                job_id="job-1",
                step_id=None,
                task_id=None,
            )

        output_downloader.set_root_path("/test_root", str(tmp_path.resolve()))
        output_downloader.download_job_output()
        assert set(expected_files) == set(
            [path for path in tmp_path.glob("**/*") if path.is_file()]
        )

    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="This test is for paths in POSIX path format and will be skipped on Windows.",
    )
    @pytest.mark.parametrize(
        "outputs_by_root",
        [
            {
                "/local/home": ManifestPathGroup(
                    total_bytes=1,
                    files_by_hash_alg={
                        HashAlgorithm.XXH128: [
                            ManifestPathv2023_03_03(
                                path="../inputs/input1.txt", hash="a", size=1, mtime=1
                            )
                        ],
                    },
                ),
            },
            {
                "/local/home": ManifestPathGroup(
                    total_bytes=1,
                    files_by_hash_alg={
                        HashAlgorithm.XXH128: [
                            ManifestPathv2023_03_03(
                                path="/inputs/input1.txt", hash="a", size=1, mtime=1
                            )
                        ],
                    },
                ),
            },
            {
                "home": ManifestPathGroup(
                    total_bytes=1,
                    files_by_hash_alg={
                        HashAlgorithm.XXH128: [
                            ManifestPathv2023_03_03(
                                path="/inputs/input1.txt", hash="a", size=1, mtime=1
                            )
                        ],
                    },
                ),
            },
            {
                "/local/home": ManifestPathGroup(
                    total_bytes=1,
                    files_by_hash_alg={
                        HashAlgorithm.XXH128: [
                            ManifestPathv2023_03_03(path="////", hash="a", size=1, mtime=1)
                        ],
                    },
                ),
            },
        ],
    )
    def test_OutputDownloader_download_job_output_posix_invalid_file_path_fails(
        self, farm_id, queue_id, outputs_by_root: dict[str, ManifestPathGroup]
    ):
        with patch(
            f"{deadline.__package__}.job_attachments.download.get_job_output_paths_by_asset_root",
            return_value=outputs_by_root,
        ):
            output_downloader = OutputDownloader(
                s3_settings=self.job_attachment_settings,
                farm_id=farm_id,
                queue_id=queue_id,
                job_id="job-1",
                step_id=None,
                task_id=None,
            )
        with patch(
            f"{deadline.__package__}.job_attachments.download.download_files", return_value=[]
        ), pytest.raises((PathOutsideDirectoryError, ValueError)):
            output_downloader.download_job_output()

    @pytest.mark.skipif(
        sys.platform != "win32",
        reason="This test is for paths in Windows path format and will be skipped on non-Windows.",
    )
    @pytest.mark.parametrize(
        "outputs_by_root",
        [
            {
                "C:/Users": ManifestPathGroup(
                    total_bytes=1,
                    files_by_hash_alg={
                        HashAlgorithm.XXH128: [
                            ManifestPathv2023_03_03(
                                path="../inputs/input1.txt", hash="a", size=1, mtime=1
                            )
                        ],
                    },
                ),
            },
            {
                "C:/Users": ManifestPathGroup(
                    total_bytes=1,
                    files_by_hash_alg={
                        HashAlgorithm.XXH128: [
                            ManifestPathv2023_03_03(
                                path="C:/inputs/input1.txt", hash="a", size=1, mtime=1
                            )
                        ],
                    },
                ),
            },
            {
                "/C:": ManifestPathGroup(
                    total_bytes=1,
                    files_by_hash_alg={
                        HashAlgorithm.XXH128: [
                            ManifestPathv2023_03_03(
                                path="inputs/input1.txt", hash="a", size=1, mtime=1
                            )
                        ],
                    },
                ),
            },
            {
                "C:/Users": ManifestPathGroup(
                    total_bytes=1,
                    files_by_hash_alg={
                        HashAlgorithm.XXH128: [
                            ManifestPathv2023_03_03(path="////", hash="a", size=1, mtime=1)
                        ],
                    },
                ),
            },
        ],
    )
    def test_OutputDownloader_download_job_output_windows_invalid_file_path_fails(
        self, farm_id, queue_id, outputs_by_root: dict[str, ManifestPathGroup]
    ):
        with patch(
            f"{deadline.__package__}.job_attachments.download.get_job_output_paths_by_asset_root",
            return_value=outputs_by_root,
        ):
            output_downloader = OutputDownloader(
                s3_settings=self.job_attachment_settings,
                farm_id=farm_id,
                queue_id=queue_id,
                job_id="job-1",
                step_id=None,
                task_id=None,
            )
        with patch(
            f"{deadline.__package__}.job_attachments.download.download_files", return_value=[]
        ), pytest.raises((PathOutsideDirectoryError, ValueError)):
            output_downloader.download_job_output()

    @mock_aws
    def test_get_asset_root_from_s3_error_message_on_not_found(self):
        """
        Test if the function raises the expected exception with a proper error message
        when S3 client's head_object returns a Not Found (404) error.
        """
        s3_client = boto3.client("s3")
        stubber = Stubber(s3_client)
        stubber.add_client_error(
            "head_object",
            service_error_code="NotFound",
            service_message="Not Found",
            http_status_code=404,
        )

        with stubber, patch(
            f"{deadline.__package__}.job_attachments.download.get_s3_client", return_value=s3_client
        ):
            with pytest.raises(JobAttachmentsS3ClientError) as err:
                _get_asset_root_from_s3("not/existed/test.txt", "test-bucket")
            assert isinstance(err.value.__cause__, ClientError)
            assert (
                err.value.__cause__.response["ResponseMetadata"]["HTTPStatusCode"] == 404  # type: ignore[attr-defined]
            )
            assert (
                "Error checking if object exists in bucket 'test-bucket', Target key or prefix: 'not/existed/test.txt', "
                "HTTP Status Code: 404, Not found. "
            ) in str(err.value)

    @mock_aws
    def test_get_asset_root_from_s3_error_message_on_timeout(self):
        """
        Test that the appropriate error is raised when a ReadTimeoutError occurs
        during an S3 client's head_object call.
        """
        mock_s3_client = MagicMock()
        mock_s3_client.head_object.side_effect = ReadTimeoutError(endpoint_url="test_url")

        with patch(
            f"{deadline.__package__}.job_attachments.download.get_s3_client",
            return_value=mock_s3_client,
        ):
            with pytest.raises(AssetSyncError) as exc:
                _get_asset_root_from_s3("test-key", "test-bucket")
            assert isinstance(exc.value.__cause__, BotoCoreError)
            assert (
                "An issue occurred with AWS service request while checking for the existence of an object in the S3 bucket: "
                'Read timeout on endpoint URL: "test_url"\n'
                "This could be due to temporary issues with AWS, internet connection, or your AWS credentials. "
                "Please verify your credentials and network connection. If the problem persists, try again later"
                " or contact support for further assistance."
            ) in str(exc.value)

    @mock_aws
    def test_get_manifest_from_s3_error_message_on_access_denied(self):
        """
        Test if the function raises the expected exception with a proper error message
        when S3 client's download_fileobj returns an Access Denied (403) error.
        """
        s3_client = boto3.client("s3")
        stubber = Stubber(s3_client)
        stubber.add_client_error(
            "head_object",
            service_error_code="AccessDenied",
            service_message="Access Denied",
            http_status_code=403,
        )

        with stubber, patch(
            f"{deadline.__package__}.job_attachments.download.get_s3_client", return_value=s3_client
        ):
            with pytest.raises(JobAttachmentsS3ClientError) as exc:
                get_manifest_from_s3("test-key", "test-bucket")
            assert isinstance(exc.value.__cause__, ClientError)
            assert (
                exc.value.__cause__.response["ResponseMetadata"]["HTTPStatusCode"] == 403  # type: ignore[attr-defined]
            )
            assert (
                "Error downloading binary file in bucket 'test-bucket', Target key or prefix: 'test-key', "
                "HTTP Status Code: 403, Forbidden or Access denied. "
            ) in str(exc.value)

    @mock_aws
    def test_get_manifest_from_s3_error_message_on_timeout(self):
        """
        Test that the appropriate error is raised when a ReadTimeoutError occurs
        during an S3 client's download_fileobj call.
        """
        mock_s3_client = MagicMock()
        mock_s3_client.download_fileobj.side_effect = ReadTimeoutError(endpoint_url="test_url")

        with patch(
            f"{deadline.__package__}.job_attachments.download.get_s3_client",
            return_value=mock_s3_client,
        ):
            with pytest.raises(AssetSyncError) as exc:
                get_manifest_from_s3("test-key", "test-bucket")
            assert isinstance(exc.value.__cause__, BotoCoreError)
            assert (
                "An issue occurred with AWS service request while downloading binary file: "
                'Read timeout on endpoint URL: "test_url"\n'
                "This could be due to temporary issues with AWS, internet connection, or your AWS credentials. "
                "Please verify your credentials and network connection. If the problem persists, try again later"
                " or contact support for further assistance."
            ) in str(exc.value)

    @mock_aws
    def test_get_tasks_manifests_keys_from_s3_error_message_on_access_denied(self):
        """
        Test if the function raises the expected exception with a proper error message
        when S3 client's list_objects_v2 returns an Access Denied (403) error.
        """
        s3_client = boto3.client("s3")
        stubber = Stubber(s3_client)
        stubber.add_client_error(
            "list_objects_v2",
            service_error_code="AccessDenied",
            service_message="Access Denied",
            http_status_code=403,
        )

        with stubber, patch(
            f"{deadline.__package__}.job_attachments.download.get_s3_client", return_value=s3_client
        ):
            with pytest.raises(JobAttachmentsS3ClientError) as exc:
                _get_tasks_manifests_keys_from_s3(
                    "assetRoot",
                    "test-bucket",
                )
            assert isinstance(exc.value.__cause__, ClientError)
            assert (
                exc.value.__cause__.response["ResponseMetadata"]["HTTPStatusCode"] == 403  # type: ignore[attr-defined]
            )
            assert (
                "Error listing bucket contents in bucket 'test-bucket', Target key or prefix: 'assetRoot', "
                "HTTP Status Code: 403, Forbidden or Access denied. "
            ) in str(exc.value)

    @mock_aws
    def test_get_tasks_manifests_keys_from_s3_error_message_on_timeout(self):
        """
        Test that the appropriate error is raised when S3 client's get_paginator call triggers
        a ReadTimeoutError while getting the keys of task output manifests from S3.
        """
        mock_s3_client = MagicMock()
        mock_s3_client.get_paginator.side_effect = ReadTimeoutError(endpoint_url="test_url")

        with patch(
            f"{deadline.__package__}.job_attachments.download.get_s3_client",
            return_value=mock_s3_client,
        ):
            with pytest.raises(AssetSyncError) as exc:
                _get_tasks_manifests_keys_from_s3(
                    "assetRoot",
                    "test-bucket",
                )
            assert isinstance(exc.value.__cause__, BotoCoreError)
            assert (
                "An issue occurred with AWS service request while listing bucket contents: "
                'Read timeout on endpoint URL: "test_url"\n'
                "This could be due to temporary issues with AWS, internet connection, or your AWS credentials. "
                "Please verify your credentials and network connection. If the problem persists, try again later"
                " or contact support for further assistance."
            ) in str(exc.value)

    @mock_aws
    def test_download_file_error_message_on_access_denied(self):
        """
        Test if the function raises the expected exception with a proper error message
        when S3 client's download_file returns an Access Denied (403) error.
        """
        s3_client = boto3.client("s3")
        stubber = Stubber(s3_client)
        stubber.add_client_error(
            "head_object",
            service_error_code="AccessDenied",
            service_message="Access Denied",
            http_status_code=403,
        )

        file_path = ManifestPathv2023_03_03(
            path="inputs/input1.txt", hash="input1", size=1, mtime=1234000000
        )

        with stubber, patch(
            f"{deadline.__package__}.job_attachments.download.get_s3_client", return_value=s3_client
        ), patch(f"{deadline.__package__}.job_attachments.download.Path.mkdir"):
            with pytest.raises(JobAttachmentsS3ClientError) as exc:
                download_file(
                    file_path,
                    HashAlgorithm.XXH128,
                    "/home/username/assets",
                    "test-bucket",
                    "rootPrefix/Data",
                    s3_client,
                )
            assert isinstance(exc.value.__cause__, ClientError)
            assert (
                exc.value.__cause__.response["ResponseMetadata"]["HTTPStatusCode"] == 403  # type: ignore[attr-defined]
            )
            assert (
                "Error downloading file in bucket 'test-bucket', Target key or prefix: 'rootPrefix/Data/input1.xxh128', "
                "HTTP Status Code: 403, Forbidden or Access denied. "
            ) in str(exc.value)
            failed_file_path = Path("/home/username/assets/inputs/input1.txt")
            assert (f"(Failed to download the file to {str(failed_file_path)})") in str(exc.value)

    @mock_aws
    def test_download_file_error_message_on_timeout(self):
        """
        Test that the appropriate error is raised when a ReadTimeoutError occurs
        during a transfer manager's download operation.
        """
        mock_s3_client = MagicMock()
        mock_future = MagicMock()
        mock_transfer_manager = MagicMock()
        mock_transfer_manager.download.return_value = mock_future
        mock_future.result.side_effect = ReadTimeoutError(endpoint_url="test_url")

        file_path = ManifestPathv2023_03_03(
            path="inputs/input1.txt", hash="input1", size=1, mtime=1234000000
        )

        with patch(
            f"{deadline.__package__}.job_attachments.download.get_s3_client",
            return_value=mock_s3_client,
        ), patch(
            f"{deadline.__package__}.job_attachments.download.get_s3_transfer_manager",
            return_value=mock_transfer_manager,
        ), patch(
            f"{deadline.__package__}.job_attachments.download.Path.mkdir"
        ):
            with pytest.raises(AssetSyncError) as exc:
                download_file(
                    file_path,
                    HashAlgorithm.XXH128,
                    "/home/username/assets",
                    "test-bucket",
                    "rootPrefix/Data",
                    mock_s3_client,
                )
            assert isinstance(exc.value.__cause__, BotoCoreError)
            assert (
                "An issue occurred with AWS service request while downloading file: "
                'Read timeout on endpoint URL: "test_url"\n'
                "This could be due to temporary issues with AWS, internet connection, or your AWS credentials. "
                "Please verify your credentials and network connection. If the problem persists, try again later"
                " or contact support for further assistance."
            ) in str(exc.value)

    @mock_aws
    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="This test is for Linux path only.",
    )
    def test_windows_long_path_exception_PosixOS(self):
        mock_s3_client = MagicMock()
        mock_future = MagicMock()
        mock_transfer_manager = MagicMock()
        mock_transfer_manager.download.return_value = mock_future
        mock_future.result.side_effect = Exception("Test exception")

        file_path = ManifestPathv2023_03_03(
            path="very/long/input/to/test/windows/max/file/path/for/error/handling/when/downloading/assest/from/job/attachment.txt",
            hash="path",
            size=1,
            mtime=1234000000,
        )

        local_path = "Users/path/to/a/very/long/file/path/that/exceeds/the/windows/max/path/length/for/testing/max/file/path/error/handling/when/download/or/syncing/assest/using/job/attachment"

        with patch(
            f"{deadline.__package__}.job_attachments.download.get_s3_client",
            return_value=mock_s3_client,
        ), patch(
            f"{deadline.__package__}.job_attachments.download.get_s3_transfer_manager",
            return_value=mock_transfer_manager,
        ), patch(
            f"{deadline.__package__}.job_attachments.download.Path.mkdir"
        ):
            with pytest.raises(AssetSyncError) as exc:
                download_file(
                    file_path,
                    HashAlgorithm.XXH128,
                    local_path,
                    "test-bucket",
                    "rootPrefix/Data",
                    mock_s3_client,
                )

        expected_message = "Test exception"
        assert str(exc.value) == expected_message

    @mock_aws
    @pytest.mark.skipif(
        sys.platform != "win32",
        reason="This test is for Windows path only.",
    )
    def test_windows_long_path_exception_WindowsOS(self):
        mock_s3_client = MagicMock()
        mock_future = MagicMock()
        mock_transfer_manager = MagicMock()
        mock_transfer_manager.download.return_value = mock_future
        mock_future.result.side_effect = Exception("Test exception")

        file_path = ManifestPathv2023_03_03(
            path="very/long/input/to/test/windows/max/file/path/for/error/handling/when/downloading/assest/from/job/attachment.txt",
            hash="path",
            size=1,
            mtime=1234000000,
        )

        local_path = "C:\\path\\to\\a\\very\\long\\file\\path\\that\\exceeds\\the\\windows\\max\\path\\length\\for\\testing\\max\\file\\path\\error\\handling\\when\\download\\or\\syncing\\assest\\using\\job\\attachment"

        with patch(
            f"{deadline.__package__}.job_attachments.download.get_s3_client",
            return_value=mock_s3_client,
        ), patch(
            f"{deadline.__package__}.job_attachments.download.get_s3_transfer_manager",
            return_value=mock_transfer_manager,
        ), patch(
            f"{deadline.__package__}.job_attachments.download.Path.mkdir"
        ):
            with pytest.raises(AssetSyncError) as exc:
                download_file(
                    file_path,
                    HashAlgorithm.XXH128,
                    local_path,
                    "test-bucket",
                    "rootPrefix/Data",
                    mock_s3_client,
                )

        expected_message = "Your file path is longer than what Windows allow.\nThis could be the error if you do not enable longer file path in Windows"
        assert str(exc.value) == expected_message

    @mock_aws
    @pytest.mark.skipif(
        sys.platform != "win32",
        reason="This test is for Windows path only.",
    )
    def test_windows_long_path_UNC_notation_WindowsOS(self):
        mock_s3_client = MagicMock()
        mock_future = MagicMock()
        mock_transfer_manager = MagicMock()
        mock_transfer_manager.download.return_value = mock_future
        mock_future.result.side_effect = Exception("Test exception")

        file_path = ManifestPathv2023_03_03(
            path="very/long/input/to/test/windows/max/file/path/for/error/handling/when/downloading/assest/from/job/attachment.txt",
            hash="path",
            size=1,
            mtime=1234000000,
        )

        local_path = "\\\\?\\C:\\path\\to\\a\\very\\long\\file\\path\\that\\exceeds\\the\\windows\\max\\path\\length\\for\\testing\\max\\file\\path\\error\\handling\\when\\download\\or\\syncing\\assest\\using\\job\\attachment"

        with patch(
            f"{deadline.__package__}.job_attachments.download.get_s3_client",
            return_value=mock_s3_client,
        ), patch(
            f"{deadline.__package__}.job_attachments.download.get_s3_transfer_manager",
            return_value=mock_transfer_manager,
        ), patch(
            f"{deadline.__package__}.job_attachments.download._is_windows_file_path_limit",
            return_value=False,
        ), patch(
            f"{deadline.__package__}.job_attachments.download.Path.mkdir"
        ):
            with pytest.raises(AssetSyncError) as exc:
                download_file(
                    file_path,
                    HashAlgorithm.XXH128,
                    local_path,
                    "test-bucket",
                    "rootPrefix/Data",
                    mock_s3_client,
                )

        expected_message = "Test exception\nUNC notation exist, but long path registry not enabled. Undefined error"
        assert str(exc.value) == expected_message

    @mock_aws
    @pytest.mark.skipif(
        sys.platform != "win32",
        reason="This test is for Windows path only.",
    )
    def test_windows_long_path_UNC_notation_and_registry_WindowsOS(self):
        mock_s3_client = MagicMock()
        mock_future = MagicMock()
        mock_transfer_manager = MagicMock()
        mock_transfer_manager.download.return_value = mock_future
        mock_future.result.side_effect = Exception("Test exception")

        file_path = ManifestPathv2023_03_03(
            path="very/long/input/to/test/windows/max/file/path/for/error/handling/when/downloading/assest/from/job/attachment.txt",
            hash="path",
            size=1,
            mtime=1234000000,
        )

        local_path = "\\\\?\\C:\\path\\to\\a\\very\\long\\file\\path\\that\\exceeds\\the\\windows\\max\\path\\length\\for\\testing\\max\\file\\path\\error\\handling\\when\\download\\or\\syncing\\assest\\using\\job\\attachment"

        with patch(
            f"{deadline.__package__}.job_attachments.download.get_s3_client",
            return_value=mock_s3_client,
        ), patch(
            f"{deadline.__package__}.job_attachments.download.get_s3_transfer_manager",
            return_value=mock_transfer_manager,
        ), patch(
            f"{deadline.__package__}.job_attachments.download._is_windows_file_path_limit",
            return_value=True,
        ), patch(
            f"{deadline.__package__}.job_attachments.download.Path.mkdir"
        ):
            with pytest.raises(AssetSyncError) as exc:
                download_file(
                    file_path,
                    HashAlgorithm.XXH128,
                    local_path,
                    "test-bucket",
                    "rootPrefix/Data",
                    mock_s3_client,
                )

        expected_message = "Test exception"
        assert str(exc.value) == expected_message


@pytest.mark.parametrize("manifest_version", [ManifestVersion.v2023_03_03])
class TestFullDownloadPrefixesWithSlashes:
    """
    Tests for downloads from cas when the queue prefixes are created.
    """

    @pytest.fixture(autouse=True)
    def before_test(
        self,
        request,
        create_s3_bucket: Callable[[str], None],
        farm_id: str,
        queue_id: str,
        default_queue: Queue,
        create_get_queue_response: Callable[[Queue], dict[str, Any]],
        manifest_version: ManifestVersion,
    ):
        """
        Setup the default queue and s3 bucket for all asset tests.
        Mark test with `no_setup` if you don't want this setup to run.
        """
        if "no_setup" in request.keywords:
            return

        self.queue = default_queue
        assert self.queue.jobAttachmentSettings
        self.queue.jobAttachmentSettings.rootPrefix = "test////////"
        self.queue_response = create_get_queue_response(self.queue)
        create_s3_bucket(self.queue.jobAttachmentSettings.s3BucketName)

        s3 = boto3.Session(region_name="us-west-2").resource("s3")  # pylint: disable=invalid-name
        bucket = s3.Bucket(self.queue.jobAttachmentSettings.s3BucketName)

        for i in range(1, 15):
            bucket.upload_fileobj(
                BytesIO(b"a"),
                f"{self.queue.jobAttachmentSettings.rootPrefix}/Data/test{i}.xxh128",
            )

        for manifest in MANIFEST_VERSION_TO_MANIFESTS[manifest_version]:
            bucket.upload_fileobj(
                BytesIO(manifest.manifests),
                f"{self.queue.jobAttachmentSettings.rootPrefix}/"
                f"Manifests/{farm_id}/{queue_id}/{manifest.prefix}",
            )

        # Put random junk in the outputs prefix to make sure it isn't downloaded.
        bucket.upload_fileobj(
            BytesIO(b"a"),
            f"{self.queue.jobAttachmentSettings.rootPrefix}/"
            f"Manifests/{farm_id}/{queue_id}/job-1/step-1/task-1-1/junk",
        )

        bucket.upload_fileobj(
            BytesIO(b"a"),
            f"{self.queue.jobAttachmentSettings.rootPrefix}/"
            f"Manifests/{farm_id}/{queue_id}/job-1/step-1/junk.json",
        )

        bucket.upload_fileobj(
            BytesIO(b"a"),
            f"{self.queue.jobAttachmentSettings.rootPrefix}/"
            f"Manifests/{farm_id}/{queue_id}/job-1/junk2.json",
        )

    @mock_aws
    def test_download_task_output_prefixes_with_slashes(
        self, farm_id, queue_id, tmp_path: Path, manifest_version: ManifestVersion
    ):
        assert self.queue.jobAttachmentSettings
        assert_download_task_output(
            self.queue.jobAttachmentSettings,
            farm_id,
            queue_id,
            tmp_path,
            expected_files={
                str(tmp_path): [
                    tmp_path / "test1.txt",
                    tmp_path / "test" / "test2.txt",
                    tmp_path / "test" / "test3.txt",
                    tmp_path / "test4.txt",
                ]
            },
            expected_total_bytes=4,
            manifest_version=manifest_version,
        )

    @mock_aws
    def test_download_step_prefixes_with_slashes(
        self, farm_id, queue_id, tmp_path: Path, manifest_version: ManifestVersion
    ):
        assert self.queue.jobAttachmentSettings
        assert_download_step_output(
            self.queue.jobAttachmentSettings,
            farm_id,
            queue_id,
            tmp_path,
            expected_files={
                str(tmp_path): [
                    tmp_path / "test1.txt",
                    tmp_path / "test" / "test2.txt",
                    tmp_path / "test" / "test3.txt",
                    tmp_path / "test4.txt",
                    tmp_path / "test13.txt",
                    tmp_path / "test" / "test14.txt",
                    tmp_path / "test5.txt",
                    tmp_path / "test" / "test6.txt",
                ]
            },
            expected_total_bytes=8,
            manifest_version=manifest_version,
        )

    @mock_aws
    def test_download_job_prefixes_with_slashes(
        self, farm_id, queue_id, tmp_path: Path, manifest_version: ManifestVersion
    ):
        assert self.queue.jobAttachmentSettings
        assert_download_job_output(
            self.queue.jobAttachmentSettings,
            farm_id,
            queue_id,
            tmp_path,
            expected_files={
                str(tmp_path): [
                    tmp_path / "test1.txt",
                    tmp_path / "test" / "test2.txt",
                    tmp_path / "test" / "test3.txt",
                    tmp_path / "test4.txt",
                    tmp_path / "test13.txt",
                    tmp_path / "test" / "test14.txt",
                    tmp_path / "test5.txt",
                    tmp_path / "test" / "test6.txt",
                    tmp_path / "test7.txt",
                    tmp_path / "test" / "test8.txt",
                    tmp_path / "test" / "test9.txt",
                    tmp_path / "test10.txt",
                    tmp_path / "test11.txt",
                    tmp_path / "test" / "test12.txt",
                ]
            },
            expected_total_bytes=14,
            manifest_version=manifest_version,
        )


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="This test is for paths in POSIX path format and will be skipped on Windows.",
)
@pytest.mark.parametrize(
    "root_path, output_paths",
    [
        ("/local/home", ["test.png"]),
        ("/local/home", ["outputs/test.png"]),
        ("/local/home", ["../home/outputs/test.png"]),
        ("/local/home/documents/..", ["outputs/test.png"]),
        ("/local/home/documents/..", ["../home/outputs/test.png"]),
        ("/////local/home", ["test.png"]),
    ],
)
def test_ensure_paths_within_directory_posix_no_error(root_path: str, output_paths: list[str]):
    _ensure_paths_within_directory(root_path, output_paths)


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="This test is for paths in POSIX path format and will be skipped on Windows.",
)
@pytest.mark.parametrize(
    "root_path, output_paths",
    [
        ("/local/home", ["../test.png"]),
        ("/local/home", ["outputs/../../test.png"]),
        ("/local/home", ["../home/../outputs/test.png"]),
        ("/local/home", ["/outputs/test.png"]),
        ("local", ["local/outputs/test.png"]),
        ("C:/Users", ["outputs/test.png"]),
        ("", ["outputs/test.png"]),
    ],
)
def test_ensure_paths_within_directory_posix_raises_error(root_path: str, output_paths: list[str]):
    with pytest.raises((PathOutsideDirectoryError, ValueError)):
        _ensure_paths_within_directory(root_path, output_paths)


@pytest.mark.skipif(
    sys.platform != "win32",
    reason="This test is for paths in Windows path format and will be skipped on non-Windows.",
)
@pytest.mark.parametrize(
    "root_path, output_paths",
    [
        ("C:/Users", ["test.png"]),
        ("C:/Users", ["outputs/test.png"]),
        ("C:/Users", ["../Users/outputs/test.png"]),
        ("C:/Users/Temp/..", ["outputs/test.png"]),
        ("C:/Users/Temp/..", ["../Users/outputs/test.png"]),
    ],
)
def test_ensure_paths_within_directory_windows_no_error(root_path: str, output_paths: list[str]):
    _ensure_paths_within_directory(root_path, output_paths)


@pytest.mark.skipif(
    sys.platform != "win32",
    reason="This test is for paths in Windows path format and will be skipped on non-Windows.",
)
@pytest.mark.parametrize(
    "root_path, output_paths",
    [
        ("C:/Users", ["../test.png"]),
        ("C:/Users", ["test.png", "../test.png"]),
        ("C:/Users", ["outputs/../../test.png"]),
        ("C:/Users", ["../home/../outputs/test.png"]),
        ("C:/Users", ["C:/Temp/outputs/test.png"]),
        (":/Users", ["outputs/test.png"]),
        ("/Users", ["outputs/test.png"]),
        ("/local/home", ["outputs/test.png"]),
        ("", ["outputs/test.png"]),
    ],
)
def test_ensure_paths_within_directory_windows_raises_error(
    root_path: str, output_paths: list[str]
):
    with pytest.raises((PathOutsideDirectoryError, ValueError)):
        _ensure_paths_within_directory(root_path, output_paths)


def test_merge_asset_manifests(
    test_manifest_one: dict, test_manifest_two: dict, merged_manifest: dict
):
    """
    Test that merging two manifests correctly overlays the 2nd on top of the 1st
    """
    manifests = [
        decode_manifest(json.dumps(test_manifest_one)),
        decode_manifest(json.dumps(test_manifest_two)),
    ]

    actual_merged_manifest = merge_asset_manifests(manifests)

    assert decode_manifest(json.dumps(merged_manifest)) == actual_merged_manifest


def test_merge_asset_manifests_empty():
    """
    Test that merging an empty list returns None
    """
    assert merge_asset_manifests([]) is None


def test_merge_asset_manifest_single(test_manifest_one: dict):
    """
    Test that merging a single manifest returns the same manifest
    """
    manifest = decode_manifest(json.dumps(test_manifest_one))
    actual_merged_manifest = merge_asset_manifests([manifest])

    assert actual_merged_manifest == manifest


def on_downloading_files(progress: ProgressReportMetadata) -> bool:
    return True


def test_download_files_from_manifests(
    test_manifest_one: dict,
    test_manifest_two: dict,
):
    manifests: list[BaseAssetManifest] = [
        decode_manifest(json.dumps(test_manifest_one)),
        decode_manifest(json.dumps(test_manifest_two)),
    ]

    merged_manifest = merge_asset_manifests(manifests)

    assert merged_manifest

    downloaded_files: list[str] = []

    def download_file(*args):
        nonlocal downloaded_files
        downloaded_files.append(args[0].path)
        return (40, Path(args[0].path))

    with patch(
        f"{deadline.__package__}.job_attachments.download.download_file", side_effect=download_file
    ), patch(f"{deadline.__package__}.job_attachments.download.get_s3_client"):
        download_files_from_manifests(
            s3_bucket="s3_settings.s3BucketName",
            manifests_by_root={"/test": merged_manifest},
            cas_prefix="s3_settings.full_cas_prefix()",
            session=boto3.Session(region_name="us-west-2"),
            on_downloading_files=on_downloading_files,
        )

    assert sorted(downloaded_files) == ["a.txt", "b.txt", "c.txt", "d.txt"]


def test_handle_existing_vfs_no_mount_returns(test_manifest_one: dict):
    """
    Test that handling an existing manifest for a non existent mount returns the manifest
    """
    manifest = decode_manifest(json.dumps(test_manifest_one))
    with patch(
        f"{deadline.__package__}.job_attachments.download.VFSProcessManager.is_mount",
        return_value=False,
    ) as mock_is_mount:
        result_manifest = handle_existing_vfs(
            manifest, Path("/some/session/dir"), "/not/a/mount", "test-user"
        )
        mock_is_mount.assert_called_once_with("/not/a/mount")
    assert manifest == result_manifest


def test_handle_existing_vfs_success(
    test_manifest_one: dict, test_manifest_two: dict, merged_manifest: dict
):
    """
    Test that handling an existing manifest for a mount which exists attempts to merge the manifests and
    shut down the mount
    """
    manifest_one = decode_manifest(json.dumps(test_manifest_one))
    manifest_two = decode_manifest(json.dumps(test_manifest_two))
    merged_decoded = decode_manifest(json.dumps(merged_manifest))
    session_path = Path("/some/session/dir")
    with patch(
        f"{deadline.__package__}.job_attachments.download.VFSProcessManager.is_mount",
        return_value=True,
    ) as mock_is_mount, patch(
        f"{deadline.__package__}.job_attachments.download.VFSProcessManager.get_manifest_path_for_mount",
        return_value="/some/manifest/path",
    ) as mock_get_manifest_path, patch(
        f"{deadline.__package__}.job_attachments.download._read_manifest_file",
        return_value=manifest_one,
    ) as mock_decode_manifest, patch(
        f"{deadline.__package__}.job_attachments.download.VFSProcessManager.kill_process_at_mount",
    ) as mock_kill_process:
        result_manifest = handle_existing_vfs(
            manifest_two, session_path, "/some/mount", "test-user"
        )
        mock_is_mount.assert_called_once_with("/some/mount")
        mock_get_manifest_path.assert_called_once_with(
            session_dir=session_path, mount_point="/some/mount"
        )
        mock_decode_manifest.assert_called_once_with("/some/manifest/path")
        mock_kill_process.assert_called_once_with(
            session_dir=session_path, mount_point="/some/mount", os_user="test-user"
        )
    assert result_manifest == merged_decoded


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="This VFS test is currently not valid for windows - VFS is a linux only feature currently.",
)
def test_mount_vfs_from_manifests(
    test_manifest_one: dict, test_manifest_two: dict, merged_manifest: dict
):
    """
    Test that handling an existing manifest for a mount which exists attempts to merge the manifests and
    shut down the mount
    """
    manifest_one = decode_manifest(json.dumps(test_manifest_one))
    manifest_two = decode_manifest(json.dumps(test_manifest_two))
    merged_decoded = decode_manifest(json.dumps(merged_manifest))
    temp_dir = tempfile.TemporaryDirectory()
    temp_dir_path = Path(temp_dir.name)
    manifests_by_root = {"/some/root/one": manifest_one, "/some/root/two": manifest_two}
    fs_permissions = PosixFileSystemPermissionSettings("test-user", "test-group", 0o31, 0o66)
    manifest_permissions = PosixFileSystemPermissionSettings(
        fs_permissions.os_user,
        fs_permissions.os_group,
        VFS_MANIFEST_FOLDER_PERMISSIONS.dir_mode,
        VFS_MANIFEST_FOLDER_PERMISSIONS.file_mode,
    )

    cache_path = temp_dir_path / VFS_CACHE_REL_PATH_IN_SESSION
    manifest_path = temp_dir_path / VFS_MANIFEST_FOLDER_IN_SESSION
    logs_path = temp_dir_path / VFS_LOGS_FOLDER_IN_SESSION

    with patch(
        f"{deadline.__package__}.job_attachments.download._set_fs_group",
    ) as mock_set_vs_group, patch(
        f"{deadline.__package__}.job_attachments.download.handle_existing_vfs",
        return_value=merged_decoded,
    ) as mock_handle_existing, patch(
        f"{deadline.__package__}.job_attachments.download._write_manifest_to_temp_file",
    ) as mock_write_manifest, patch(
        f"{deadline.__package__}.job_attachments.download.VFSProcessManager.start",
    ) as mock_vfs_start:
        mount_vfs_from_manifests(
            "test-bucket",
            manifests_by_root,
            boto3_session=boto3.Session(region_name="us-west-2"),
            session_dir=temp_dir_path,
            os_env_vars={},
            fs_permission_settings=fs_permissions,
            cas_prefix="cas/test",
        )
        # Were the cache and manifest folders created
        assert os.path.isdir(cache_path)
        assert os.path.isdir(manifest_path)

        #
        # Did we attempt to assign the expected permissions
        mock_set_vs_group.assert_has_calls(
            [
                call([str(cache_path / "cas/test")], str(cache_path), fs_permissions),
                call([str(manifest_path)], str(manifest_path), manifest_permissions),
                call([str(logs_path)], str(logs_path), fs_permissions),
            ]
        )

        mock_handle_existing.assert_has_calls(
            [
                call(
                    manifest=manifest_one,
                    session_dir=temp_dir_path,
                    mount_point="/some/root/one",
                    os_user="test-user",
                ),
                call(
                    manifest=manifest_two,
                    session_dir=temp_dir_path,
                    mount_point="/some/root/two",
                    os_user="test-user",
                ),
            ]
        )

        mock_write_manifest.assert_has_calls(
            [call(merged_decoded, dir=manifest_path), call(merged_decoded, dir=manifest_path)]
        )
        mock_vfs_start.assert_has_calls(
            [call(session_dir=temp_dir_path), call(session_dir=temp_dir_path)]
        )


@pytest.mark.parametrize(
    "file_conflict_resolution",
    [
        FileConflictResolution.OVERWRITE,
        FileConflictResolution.CREATE_COPY,
        FileConflictResolution.SKIP,
    ],
)
def test_download_file_with_s3_key(tmp_path, file_conflict_resolution):
    s3_bucket = "test-bucket"
    s3_key = "test-key"
    local_file_path = tmp_path / "test-file.txt"
    file_bytes = 1024

    mock_transfer_manager = MagicMock()

    if file_conflict_resolution == FileConflictResolution.SKIP:
        local_file_path.touch()
        mock_transfer_manager.download.return_value = None
    else:
        mock_future = MagicMock()
        mock_transfer_manager.download.return_value = mock_future

    with patch(
        "deadline.job_attachments.download.get_account_id", return_value="YOUR_AWS_ACCOUNT_ID"
    ):
        future = download_file_with_s3_key(
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            transfer_manager=mock_transfer_manager,
            local_file_name=local_file_path,
            file_bytes=file_bytes,
            file_conflict_resolution=file_conflict_resolution,
        )

        # Make sure download funciton isn't called and returns None when SKIP
        if file_conflict_resolution == FileConflictResolution.SKIP:
            assert future is None
            mock_transfer_manager.download.assert_not_called()
        else:
            assert future is mock_future
            mock_transfer_manager.download.assert_called_once_with(
                bucket=s3_bucket,
                key=s3_key,
                fileobj=str(local_file_path),
                extra_args={"ExpectedBucketOwner": "YOUR_AWS_ACCOUNT_ID"},
                subscribers=[ANY],
            )

            # Check if the file name is modified for CREATE_COPY resolution
            if file_conflict_resolution == FileConflictResolution.CREATE_COPY:
                assert local_file_path.name.startswith("test-file")
                assert local_file_path.name.endswith(".txt")
