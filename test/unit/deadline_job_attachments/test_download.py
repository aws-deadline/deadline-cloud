# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Tests for downloading files from the Job Attachment CAS."""
from __future__ import annotations

from dataclasses import dataclass, fields
from io import BytesIO
import json
from pathlib import Path
import sys
from typing import Any, Callable, List
from unittest.mock import MagicMock, patch

import boto3
from botocore.exceptions import ClientError
from botocore.stub import Stubber

import pytest
from moto import mock_sts

import deadline
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
    download_files_from_manifests,
    download_files_in_directory,
    get_job_input_output_paths_by_asset_root,
    get_job_input_paths_by_asset_root,
    get_job_output_paths_by_asset_root,
    get_manifest_from_s3,
    merge_asset_manifests,
    _ensure_paths_within_directory,
    _get_asset_root_from_s3,
    _get_tasks_manifests_keys_from_s3,
)
from deadline.job_attachments.exceptions import (
    JobAttachmentsError,
    JobAttachmentsS3ClientError,
    MissingAssetRootError,
    PathOutsideDirectoryError,
)
from deadline.job_attachments.models import Job, JobAttachmentS3Settings, Attachments, Queue
from deadline.job_attachments.progress_tracker import (
    DownloadSummaryStatistics,
    ProgressReportMetadata,
    ProgressStatus,
)
from deadline.job_attachments._utils import FileConflictResolution, _human_readable_file_size
from deadline.job_attachments.asset_manifests.decode import decode_manifest


@dataclass
class Manifest:
    prefix: str
    manifests: bytes


MANIFESTS_v2022_03_03: List[Manifest] = [
    Manifest(
        "job-1/step-1/task-1-1/session-action-9/manifest1v2023-03-03_output.xxh128",
        b'{"hashAlg":"xxh128","manifestVersion":"2023-03-03",'
        b'"paths":[{"hash":"test1","mtime":1234000000,"path":"test1.txt","size":1},'
        b'{"hash":"test2","mtime":1234000000,"path":"test/test2.txt","size":1}],'
        b'"totalSize":2}',
    ),
    Manifest(
        "job-1/step-1/task-1-1/session-action-9/manifest2v2023-03-03_output.xxh128",
        b'{"hashAlg":"xxh128","manifestVersion":"2023-03-03",'
        b'"paths":[{"hash":"test3","mtime":1234000000,"path":"test/test3.txt","size":1},'
        b'{"hash":"test4","mtime":1234000000,"path":"test4.txt","size":1}],'
        b'"totalSize":2}',
    ),
    Manifest(
        "job-1/step-1/task-1-1/session-action-1/manifest2v2023-03-03_output.xxh128",
        b'{"hashAlg":"xxh128","manifestVersion":"2023-03-03",'
        b'"paths":[{"hash":"test3","mtime":1234000000,"path":"test/test33.txt","size":1},'
        b'{"hash":"test4","mtime":1234000000,"path":"test44.txt","size":1}],'
        b'"totalSize":2}',
    ),
    Manifest(
        "job-1/step-1/task-1-11/session-action-9/manifest7v2023-03-03_output.xxh128",
        b'{"hashAlg":"xxh128","manifestVersion":"2023-03-03",'
        b'"paths":[{"hash":"test13","mtime":1234000000,"path":"test13.txt","size":1},'
        b'{"hash":"test14","mtime":1234000000,"path":"test/test14.txt","size":1}],'
        b'"totalSize":2}',
    ),
    Manifest(
        "job-1/step-1/task-1-2/session-action-9/manifest3v2023-03-03_output.xxh128",
        b'{"hashAlg":"xxh128","manifestVersion":"2023-03-03",'
        b'"paths":[{"hash":"test5","mtime":1234000000,"path":"test5.txt","size":1},'
        b'{"hash":"test6","mtime":1234000000,"path":"test/test6.txt","size":1}],'
        b'"totalSize":2}',
    ),
    Manifest(
        "job-1/step-2/task-2-3/session-action-9/manifest4v2023-03-03_output.xxh128",
        b'{"hashAlg":"xxh128","manifestVersion":"2023-03-03",'
        b'"paths":[{"hash":"test7","mtime":1234000000,"path":"test7.txt","size":1},'
        b'{"hash":"test8","mtime":1234000000,"path":"test/test8.txt","size":1}],'
        b'"totalSize":2}',
    ),
    Manifest(
        "job-1/step-2/task-2-3/session-action-9/manifest5v2023-03-03_output.xxh128",
        b'{"hashAlg":"xxh128","manifestVersion":"2023-03-03",'
        b'"paths":[{"hash":"test9","mtime":1234000000,"path":"test/test9.txt","size":1},'
        b'{"hash":"test10","mtime":1234000000,"path":"test10.txt","size":1}],'
        b'"totalSize":2}',
    ),
    Manifest(
        "job-1/step-2/task-2-3/session-action-1/manifest5v2023-03-03_output.xxh128",
        b'{"hashAlg":"xxh128","manifestVersion":"2023-03-03",'
        b'"paths":[{"hash":"test9","mtime":1234000000,"path":"test/test99.txt","size":1},'
        b'{"hash":"test100","mtime":1234000000,"path":"test10.txt","size":1}],'
        b'"totalSize":2}',
    ),
    Manifest(
        "job-1/step-2/task-2-4/session-action-9/manifest6v2023-03-03_output.xxh128",
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
        "Inputs/0000/manifest_input.xxh128",
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
        expected_last_progress_report = ProgressReportMetadata(
            status=ProgressStatus.DOWNLOAD_IN_PROGRESS,
            progress=100.0,
            progressMessage=f"Downloaded {readable_total_input_bytes} / {readable_total_input_bytes}"
            + f" of {len(expected_files_set)} files",
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
        expected_last_progress_report = ProgressReportMetadata(
            status=ProgressStatus.DOWNLOAD_IN_PROGRESS,
            progress=100.0,
            progressMessage=f"Downloaded {len(expected_files_set)}/{len(expected_files_set)} files",
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

    mock_on_downloading_files.assert_called_with(expected_last_progress_report)
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
    Assert that get_job_input_paths_by_asset_root returns a list of (hash, path) pairs of all asset files.
    """
    with patch(
        f"{deadline.__package__}.job_attachments.download.get_job_output_paths_by_asset_root",
        return_value=(
            0,
            {
                "/tmp": [
                    ManifestPathv2023_03_03(
                        path="outputs/output.txt", hash="outputhash", size=100, mtime=1234567
                    )
                ]
            },
        ),
    ):
        (total_bytes, assets) = get_job_input_paths_by_asset_root(
            s3_settings=s3_settings,
            attachments=attachments,
        )
    assert assets == expected_files

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
        (total_bytes, outputs) = get_job_output_paths_by_asset_root(
            s3_settings=s3_settings, farm_id=farm_id, queue_id=queue_id, job_id="job-1"
        )
    assert outputs == expected_files

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
        (total_bytes, paths_hashes) = get_job_input_output_paths_by_asset_root(
            s3_settings, attachments, farm_id, queue_id, "job-1"
        )
    assert paths_hashes == expected_files

    if manifest_version == ManifestVersion.v2023_03_03:
        assert total_bytes == expected_total_bytes


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
                f"{self.job_attachment_settings.rootPrefix}/Data/test{i}",
            )

        for i in range(1, 6):
            bucket.upload_fileobj(
                BytesIO(b"a"),
                f"{self.job_attachment_settings.rootPrefix}/Data/input{i}",
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

    @mock_sts
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

    @mock_sts
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

    @mock_sts
    def test_get_job_outputs_paths_by_asset_root_when_no_asset_root(self, farm_id, queue_id):
        assert_get_job_output_paths_by_asset_root_when_no_asset_root_throws_error(
            farm_id, queue_id, self.job_attachment_settings
        )

    @mock_sts
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

    @mock_sts
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

    @mock_sts
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

    @mock_sts
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

    @mock_sts
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

    @mock_sts
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
                "test1.txt",
                "test/test2.txt",
                "test/test3.txt",
                "test4.txt",
                "test13.txt",
                "test/test14.txt",
                "test5.txt",
                "test/test6.txt",
                "test7.txt",
                "test/test8.txt",
                "test/test9.txt",
                "test10.txt",
                "test11.txt",
                "test/test12.txt",
            ]
        }

    @mock_sts
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
                "test1.txt",
                "test/test2.txt",
                "test/test3.txt",
                "test4.txt",
                "test13.txt",
                "test/test14.txt",
                "test5.txt",
                "test/test6.txt",
                "test7.txt",
                "test/test8.txt",
                "test/test9.txt",
                "test10.txt",
                "test11.txt",
                "test/test12.txt",
            ]
        }

    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="Windows requires Admin to create symlinks, skipping this test.",
    )
    @mock_sts
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
                "test1.txt",
                "test/test2.txt",
                "test/test3.txt",
                "test4.txt",
            ]
        }

    @mock_sts
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

    @mock_sts
    def test_OutputDownloader_download_job_output_when_skip(
        self, farm_id, queue_id, tmp_path: Path
    ):
        """
        When path conflicts occur during file download and the resolution method is set to SKIP,
        test whether the files has actually been skipped.
        Note: This test relies on `st_ctime` for checking if a file has been skipped. On Linux,
        `st_ctime` represents the time of the last metadta change, but on Windows, it represents
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

    @mock_sts
    def test_OutputDownloader_download_job_output_when_overwrite(
        self, farm_id, queue_id, tmp_path: Path
    ):
        """
        When path conflicts occur during file download and the resolution method is set to OVERWRITE,
        test whether the files has actually been overwritten.
        Note: This test relies on `st_ctime` for checking if a file has been overwritten. On Linux,
        `st_ctime` represents the time of the last metadta change, but on Windows, it represents
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

    @mock_sts
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

    @mock_sts
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

    @mock_sts
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
                "/local/home": [
                    ManifestPathv2023_03_03(path="../inputs/input1.txt", hash="a", size=1, mtime=1),
                ]
            },
            {
                "/local/home": [
                    ManifestPathv2023_03_03(path="/inputs/input1.txt", hash="a", size=1, mtime=1),
                ]
            },
            {
                "home": [
                    ManifestPathv2023_03_03(path="inputs/input1.txt", hash="a", size=1, mtime=1),
                ]
            },
            {
                "/local/home": [
                    ManifestPathv2023_03_03(path="////", hash="a", size=1, mtime=1),
                ]
            },
        ],
    )
    def test_OutputDownloader_download_job_output_posix_invalid_file_path_fails(
        self, farm_id, queue_id, outputs_by_root: dict[str, list[BaseManifestPath]]
    ):
        with patch(
            f"{deadline.__package__}.job_attachments.download.get_job_output_paths_by_asset_root",
            return_value=(1, outputs_by_root),
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
                "C:/Users": [
                    ManifestPathv2023_03_03(path="../inputs/input1.txt", hash="a", size=1, mtime=1),
                ]
            },
            {
                "/C:": [
                    ManifestPathv2023_03_03(path="inputs/input1.txt", hash="a", size=1, mtime=1),
                ]
            },
            {
                "C:/Users": [
                    ManifestPathv2023_03_03(path="////", hash="a", size=1, mtime=1),
                ]
            },
        ],
    )
    def test_OutputDownloader_download_job_output_windows_invalid_file_path_fails(
        self, farm_id, queue_id, outputs_by_root: dict[str, list[BaseManifestPath]]
    ):
        with patch(
            f"{deadline.__package__}.job_attachments.download.get_job_output_paths_by_asset_root",
            return_value=(1, outputs_by_root),
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

    @mock_sts
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
            with pytest.raises(JobAttachmentsS3ClientError) as exc:
                _get_asset_root_from_s3("not/existsed/test.txt", "my-bucket")
                assert isinstance(exc.value.__cause__, ClientError)
                assert (
                    exc.value.__cause__.response["ResponseMetadata"]["HTTPStatusCode"] == 404  # type: ignore[attr-defined]
                )
                assert (
                    "Error checking if object exists in bucket 'my-bucket'. Target key or prefix: 'not/existsed/test.txt'. "
                    "HTTP Status Code: 404 Not found. "
                ) in str(exc.value)

    @mock_sts
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
                get_manifest_from_s3("test-key", "my-bucket")
                assert isinstance(exc.value.__cause__, ClientError)
                assert (
                    exc.value.__cause__.response["ResponseMetadata"]["HTTPStatusCode"] == 403  # type: ignore[attr-defined]
                )
                assert (
                    "Error downloading binary file in bucket 'my-bucket'. Target key or prefix: 'test-key'. "
                    "HTTP Status Code: 403 Forbidden or Access denied. "
                ) in str(exc.value)

    @mock_sts
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
                    "test_prefix",
                    "my-bucket",
                )
                assert isinstance(exc.value.__cause__, ClientError)
                assert (
                    exc.value.__cause__.response["ResponseMetadata"]["HTTPStatusCode"] == 403  # type: ignore[attr-defined]
                )
                assert (
                    "Error listing bucket contents in bucket 'my-bucket'. Target key or prefix: 'test_prefix'. "
                    "HTTP Status Code: 403 Forbidden or Access denied. "
                ) in str(exc.value)

    @mock_sts
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
                    file_path, "/home/username/assets", "my-bucket", "rootPrefix/Data", s3_client
                )
                assert isinstance(exc.value.__cause__, ClientError)
                assert (
                    exc.value.__cause__.response["ResponseMetadata"]["HTTPStatusCode"] == 403  # type: ignore[attr-defined]
                )
                assert (
                    "Error downloading file in bucket 'my-bucket'. Target key or prefix: 'rootPrefix/Data/relative_file_path'. "
                    "HTTP Status Code: 403 Forbidden or Access denied. "
                ) in str(exc.value)


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
                f"{self.queue.jobAttachmentSettings.rootPrefix}/Data/test{i}",
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

    @mock_sts
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

    @mock_sts
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

    @mock_sts
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


def test_merge_asset_manifests_different_hashes(
    test_manifest_one: dict, test_manifest_two: dict, merged_manifest: dict
):
    """
    Test that merging two manifests with different hash algorithms raises an exception
    """
    manifests: list[BaseAssetManifest] = [
        decode_manifest(json.dumps(test_manifest_one)),
        decode_manifest(json.dumps(test_manifest_two)),
    ]

    manifests[1].hashAlg = "crc"

    with pytest.raises(NotImplementedError):
        merge_asset_manifests(manifests)


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
    ):
        download_files_from_manifests(
            s3_bucket="s3_settings.s3BucketName",
            manifests_by_root={"/test": merged_manifest},
            cas_prefix="s3_settings.full_cas_prefix()",
            session=boto3.Session(region_name="us-west-2"),
            on_downloading_files=on_downloading_files,
        )

    assert downloaded_files == ["a.txt", "b.txt", "c.txt", "d.txt"]
