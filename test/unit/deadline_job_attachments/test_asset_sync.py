# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Tests for the Asset Synching class for task-level attachments."""

import json
from logging import getLogger
import os
import shutil
from math import trunc
from pathlib import Path
from typing import Optional, Dict
from unittest.mock import ANY, MagicMock, patch

import boto3
import pytest
from moto import mock_aws

import deadline
from deadline.job_attachments.asset_manifests.decode import decode_manifest
from deadline.job_attachments.asset_sync import AssetSync
from deadline.job_attachments.os_file_permission import PosixFileSystemPermissionSettings

from deadline.job_attachments.exceptions import (
    AssetSyncError,
    VFSExecutableMissingError,
    JobAttachmentsS3ClientError,
    VFSOSUserNotSetError,
)
from deadline.job_attachments.models import (
    Attachments,
    Job,
    JobAttachmentsFileSystem,
    JobAttachmentS3Settings,
    ManifestProperties,
    PathFormat,
    Queue,
)
from deadline.job_attachments.progress_tracker import (
    DownloadSummaryStatistics,
    ProgressStatus,
    SummaryStatistics,
)
from deadline.job_attachments._utils import _human_readable_file_size
from ..conftest import is_windows_non_admin


class TestAssetSync:
    @pytest.fixture(autouse=True)
    def before_test(
        self,
        request,
        create_s3_bucket,
        default_job_attachment_s3_settings: JobAttachmentS3Settings,
        default_asset_sync: AssetSync,
    ):
        """
        Setup the default queue and s3 bucket for all asset tests.
        Mark test with `no_setup` if you don't want this setup to run.
        """
        if "no_setup" in request.keywords:
            return

        create_s3_bucket(bucket_name=default_job_attachment_s3_settings.s3BucketName)
        self.default_asset_sync = default_asset_sync

    @pytest.fixture
    def client(self) -> MagicMock:
        return MagicMock()

    @pytest.fixture
    def asset_sync(self, farm_id: str, client: MagicMock) -> AssetSync:
        asset_sync = AssetSync(farm_id)
        asset_sync.s3_uploader._s3 = client
        return asset_sync

    @pytest.mark.parametrize(
        ("file_size", "expected_output"),
        [
            (1000000000000000000, "1000.0 PB"),
            (89234597823492938, "89.23 PB"),
            (1000000000000001, "1.0 PB"),
            (1000000000000000, "1.0 PB"),
            (999999999999999, "1.0 PB"),
            (999995000000000, "1.0 PB"),
            (999994000000000, "999.99 TB"),
            (8934587945678, "8.93 TB"),
            (1000000000001, "1.0 TB"),
            (1000000000000, "1.0 TB"),
            (999999999999, "1.0 TB"),
            (999995000000, "1.0 TB"),
            (999994000000, "999.99 GB"),
            (83748237582, "83.75 GB"),
            (1000000001, "1.0 GB"),
            (1000000000, "1.0 GB"),
            (999999999, "1.0 GB"),
            (999995000, "1.0 GB"),
            (999994000, "999.99 MB"),
            (500229150, "500.23 MB"),
            (1000001, "1.0 MB"),
            (1000000, "1.0 MB"),
            (999999, "1.0 MB"),
            (999995, "1.0 MB"),
            (999994, "999.99 KB"),
            (96771, "96.77 KB"),
            (1001, "1.0 KB"),
            (1000, "1.0 KB"),
            (999, "999.0 B"),
            (934, "934.0 B"),
        ],
    )
    def test_human_readable_file_size(self, file_size: int, expected_output: str) -> None:
        """
        Test that given a file size in bytes, the expected human readable file size is output.
        """
        assert _human_readable_file_size(file_size) == expected_output

    def test_sync_inputs_no_inputs_successful(
        self,
        tmp_path: Path,
        default_queue: Queue,
        default_job: Job,
        attachments_no_inputs: Attachments,
    ):
        """Asserts that sync_inputs is successful when no required assets exist for the Job"""
        # GIVEN
        default_job.attachments = attachments_no_inputs
        session_dir = str(tmp_path)
        dest_dir = "assetroot-27bggh78dd2b568ab123"
        local_root = str(Path(session_dir) / dest_dir)

        # WHEN
        with patch(
            f"{deadline.__package__}.job_attachments.asset_sync.download_files_from_manifests",
            side_effect=[DownloadSummaryStatistics()],
        ), patch(
            f"{deadline.__package__}.job_attachments.asset_sync._get_unique_dest_dir_name",
            side_effect=[dest_dir],
        ):
            mock_on_downloading_files = MagicMock(return_value=True)

            (summary_statistics, result_pathmap_rules) = self.default_asset_sync.sync_inputs(
                default_queue.jobAttachmentSettings,
                attachments_no_inputs,
                default_queue.queueId,
                default_job.jobId,
                tmp_path,
                on_downloading_files=mock_on_downloading_files,
            )

            # THEN
            expected_source_path_format = (
                "windows"
                if default_job.attachments.manifests[0].rootPathFormat == PathFormat.WINDOWS
                else "posix"
            )
            assert result_pathmap_rules == [
                {
                    "source_path_format": expected_source_path_format,
                    "source_path": default_job.attachments.manifests[0].rootPath,
                    "destination_path": local_root,
                }
            ]

            expected_summary_statistics = SummaryStatistics(
                total_time=summary_statistics.total_time,
                total_files=0,
                total_bytes=0,
                processed_files=0,
                processed_bytes=0,
                skipped_files=0,
                skipped_bytes=0,
                transfer_rate=0.0,
            )
            assert summary_statistics == expected_summary_statistics

    @pytest.mark.parametrize(
        ("job_fixture_name"),
        [
            ("default_job"),
            ("vfs_job"),
        ],
    )
    @pytest.mark.parametrize(
        ("s3_settings_fixture_name"),
        [
            ("default_job_attachment_s3_settings"),
        ],
    )
    def test_sync_inputs_successful(
        self,
        tmp_path: Path,
        default_queue: Queue,
        job_fixture_name: str,
        s3_settings_fixture_name: str,
        test_manifest_one: dict,
        request: pytest.FixtureRequest,
    ):
        """Asserts that a valid manifest can be processed to download attachments from S3"""
        # GIVEN
        job: Job = request.getfixturevalue(job_fixture_name)
        s3_settings: JobAttachmentS3Settings = request.getfixturevalue(s3_settings_fixture_name)
        default_queue.jobAttachmentSettings = s3_settings
        session_dir = str(tmp_path)
        dest_dir = "assetroot-27bggh78dd2b568ab123"
        local_root = str(Path(session_dir) / dest_dir)
        test_manifest = decode_manifest(json.dumps(test_manifest_one))
        test_fs_permission_settings: PosixFileSystemPermissionSettings = (
            PosixFileSystemPermissionSettings(
                os_user="test-user",
                os_group="test-group",
                dir_mode=0o20,
                file_mode=0o20,
            )
        )
        os_env_vars: Dict[str, str] = {"AWS_PROFILE": "test-profile"}
        assert job.attachments

        # WHEN
        with patch(
            f"{deadline.__package__}.job_attachments.asset_sync.get_manifest_from_s3",
            return_value=test_manifest,
        ), patch(
            f"{deadline.__package__}.job_attachments.asset_sync.download_files_from_manifests",
            side_effect=[DownloadSummaryStatistics()],
        ), patch(
            f"{deadline.__package__}.job_attachments.asset_sync._get_unique_dest_dir_name",
            side_effect=[dest_dir],
        ), patch(
            f"{deadline.__package__}.job_attachments.asset_sync.mount_vfs_from_manifests"
        ), patch(
            f"{deadline.__package__}.job_attachments.asset_sync.VFSProcessManager.find_vfs"
        ), patch.object(
            Path, "stat", MagicMock(st_mtime_ns=1234512345123451)
        ):
            mock_on_downloading_files = MagicMock(return_value=True)

            (_, result_pathmap_rules) = self.default_asset_sync.sync_inputs(
                s3_settings,
                job.attachments,
                default_queue.queueId,
                job.jobId,
                tmp_path,
                on_downloading_files=mock_on_downloading_files,
                fs_permission_settings=test_fs_permission_settings,
                os_env_vars=os_env_vars,
            )

            # THEN
            expected_source_path_format = (
                "windows"
                if job.attachments.manifests[0].rootPathFormat == PathFormat.WINDOWS
                else "posix"
            )
            assert result_pathmap_rules == [
                {
                    "source_path_format": expected_source_path_format,
                    "source_path": job.attachments.manifests[0].rootPath,
                    "destination_path": local_root,
                }
            ]

    @pytest.mark.parametrize(
        ("job_fixture_name"),
        [
            ("default_job"),
        ],
    )
    @pytest.mark.parametrize(
        ("s3_settings_fixture_name"),
        [
            ("default_job_attachment_s3_settings"),
        ],
    )
    def test_sync_inputs_404_error(
        self,
        tmp_path: Path,
        default_queue: Queue,
        job_fixture_name: str,
        s3_settings_fixture_name: str,
        test_manifest_one: dict,
        request: pytest.FixtureRequest,
    ):
        """Asserts that a specific error message is raised when getting 404 errors synching inputs"""
        # GIVEN
        download_exception = JobAttachmentsS3ClientError(
            action="get-object",
            status_code=404,
            bucket_name="test bucket",
            key_or_prefix="test-key.xxh128",
            message="File not found",
        )
        job: Job = request.getfixturevalue(job_fixture_name)
        test_manifest = decode_manifest(json.dumps(test_manifest_one))
        s3_settings: JobAttachmentS3Settings = request.getfixturevalue(s3_settings_fixture_name)
        default_queue.jobAttachmentSettings = s3_settings
        dest_dir = "assetroot-27bggh78dd2b568ab123"
        assert job.attachments

        # WHEN
        with patch(
            f"{deadline.__package__}.job_attachments.asset_sync.get_manifest_from_s3",
            return_value=test_manifest,
        ), patch(
            f"{deadline.__package__}.job_attachments.asset_sync._get_unique_dest_dir_name",
            side_effect=[dest_dir],
        ), patch(
            f"{deadline.__package__}.job_attachments.asset_sync.download_files_from_manifests",
            side_effect=download_exception,
        ):
            with pytest.raises(JobAttachmentsS3ClientError) as excinfo:
                self.default_asset_sync.sync_inputs(
                    s3_settings,
                    job.attachments,
                    default_queue.queueId,
                    job.jobId,
                    tmp_path,
                )

        # THEN
        assert "usually located in the home directory (~/.deadline/cache/s3_check_cache.db)" in str(
            excinfo
        )

    @pytest.mark.parametrize(
        ("s3_settings_fixture_name"),
        [
            ("default_job_attachment_s3_settings"),
        ],
    )
    def test_sync_inputs_with_step_dependencies(
        self,
        tmp_path: Path,
        default_queue: Queue,
        default_job: Job,
        s3_settings_fixture_name: str,
        test_manifest_one: dict,
        request: pytest.FixtureRequest,
    ):
        """Asserts that input syncing is done correctly when step dependencies are provided."""
        # GIVEN
        s3_settings: JobAttachmentS3Settings = request.getfixturevalue(s3_settings_fixture_name)
        default_queue.jobAttachmentSettings = s3_settings
        session_dir = str(tmp_path)
        dest_dir = "assetroot-27bggh78dd2b568ab123"
        local_root = str(Path(session_dir) / dest_dir)
        test_manifest = decode_manifest(json.dumps(test_manifest_one))
        assert default_job.attachments

        step_output_root = "/home/outputs_roots"
        step_dest_dir = "assetroot-8a7d189e9c17186fb88b"

        # WHEN
        with patch(
            f"{deadline.__package__}.job_attachments.asset_sync.get_manifest_from_s3",
            return_value=test_manifest,
        ), patch(
            f"{deadline.__package__}.job_attachments.asset_sync.download_files_from_manifests",
            side_effect=[DownloadSummaryStatistics()],
        ), patch(
            f"{deadline.__package__}.job_attachments.asset_sync._get_unique_dest_dir_name",
            side_effect=[dest_dir, step_dest_dir],
        ), patch(
            f"{deadline.__package__}.job_attachments.asset_sync.get_output_manifests_by_asset_root",
            side_effect=[{step_output_root: {}}],
        ), patch.object(
            Path, "stat", MagicMock(st_mtime_ns=1234512345123451)
        ):
            mock_on_downloading_files = MagicMock(return_value=True)

            (_, result_pathmap_rules) = self.default_asset_sync.sync_inputs(
                s3_settings,
                default_job.attachments,
                default_queue.queueId,
                default_job.jobId,
                tmp_path,
                step_dependencies=["step-1"],
                on_downloading_files=mock_on_downloading_files,
            )

            # THEN
            expected_source_path_format = (
                "windows"
                if default_job.attachments.manifests[0].rootPathFormat == PathFormat.WINDOWS
                else "posix"
            )
            assert result_pathmap_rules == [
                {
                    "source_path_format": expected_source_path_format,
                    "source_path": default_job.attachments.manifests[0].rootPath,
                    "destination_path": local_root,
                },
            ]

    @pytest.mark.parametrize(
        ("s3_settings_fixture_name"),
        [
            ("default_job_attachment_s3_settings"),
        ],
    )
    def test_sync_inputs_with_step_dependencies_same_root_vfs_on_posix(
        self,
        tmp_path: Path,
        default_queue: Queue,
        vfs_job: Job,
        s3_settings_fixture_name: str,
        test_manifest_one: dict,
        test_manifest_two: dict,
        request: pytest.FixtureRequest,
    ):
        """Asserts that input syncing is done correctly when step dependencies are provided."""
        # GIVEN
        job = vfs_job
        s3_settings: JobAttachmentS3Settings = request.getfixturevalue(s3_settings_fixture_name)
        default_queue.jobAttachmentSettings = s3_settings
        session_dir = str(tmp_path)
        dest_dir = "assetroot-27bggh78dd2b568ab123"
        local_root = str(Path(session_dir) / dest_dir)
        test_fs_permission_settings: PosixFileSystemPermissionSettings = (
            PosixFileSystemPermissionSettings(
                os_user="test-user",
                os_group="test-group",
                dir_mode=0o20,
                file_mode=0o20,
            )
        )
        os_env_vars: Dict[str, str] = {"AWS_PROFILE": "test-profile"}
        assert job.attachments

        test_manifest = decode_manifest(json.dumps(test_manifest_two))

        # WHEN
        with patch(
            f"{deadline.__package__}.job_attachments.asset_sync.get_manifest_from_s3",
            return_value=json.dumps(test_manifest_one),
        ), patch(
            f"{deadline.__package__}.job_attachments.asset_sync.download_files_from_manifests",
            side_effect=[DownloadSummaryStatistics()],
        ), patch(
            f"{deadline.__package__}.job_attachments.asset_sync._get_unique_dest_dir_name",
            return_value=dest_dir,
        ), patch(
            f"{deadline.__package__}.job_attachments.asset_sync.get_output_manifests_by_asset_root",
            return_value={"tmp/": [(test_manifest, "hello")]},
        ), patch(
            f"{deadline.__package__}.job_attachments.asset_sync.merge_asset_manifests",
        ) as merge_manifests_mock, patch(
            f"{deadline.__package__}.job_attachments.asset_sync.AssetSync._ensure_disk_capacity",
        ) as disk_capacity_mock, patch(
            f"{deadline.__package__}.job_attachments.download._write_manifest_to_temp_file",
            return_value="tmp_manifest",
        ), patch(
            "sys.platform", "linux"
        ), patch(
            f"{deadline.__package__}.job_attachments.asset_sync.mount_vfs_from_manifests"
        ), patch(
            f"{deadline.__package__}.job_attachments.asset_sync.VFSProcessManager.find_vfs"
        ):
            mock_on_downloading_files = MagicMock(return_value=True)

            (_, result_pathmap_rules) = self.default_asset_sync.sync_inputs(
                s3_settings,
                job.attachments,
                default_queue.queueId,
                job.jobId,
                tmp_path,
                step_dependencies=["step-1"],
                on_downloading_files=mock_on_downloading_files,
                fs_permission_settings=test_fs_permission_settings,
                os_env_vars=os_env_vars,
            )

            # THEN
            merge_manifests_mock.assert_called()
            disk_capacity_mock.assert_not_called()
            expected_source_path_format = (
                "windows"
                if job.attachments.manifests[0].rootPathFormat == PathFormat.WINDOWS
                else "posix"
            )

            assert result_pathmap_rules == [
                {
                    "source_path_format": expected_source_path_format,
                    "source_path": job.attachments.manifests[0].rootPath,
                    "destination_path": local_root,
                },
            ]

    @pytest.mark.parametrize(
        ("job_fixture_name"),
        [
            ("default_job"),
        ],
    )
    @pytest.mark.parametrize(
        ("s3_settings_fixture_name"),
        [
            ("default_job_attachment_s3_settings"),
        ],
    )
    def test_sync_inputs_no_space_left(
        self,
        tmp_path: Path,
        default_queue: Queue,
        job_fixture_name: str,
        s3_settings_fixture_name: str,
        really_big_manifest: dict,
        request: pytest.FixtureRequest,
    ):
        """Asserts that an AssetSyncError is thrown if there is not enough space left on the device to download all inputs."""
        # GIVEN
        job: Job = request.getfixturevalue(job_fixture_name)
        s3_settings: JobAttachmentS3Settings = request.getfixturevalue(s3_settings_fixture_name)
        default_queue.jobAttachmentSettings = s3_settings
        dest_dir = "assetroot-27bggh78dd2b568ab123"
        test_manifest = decode_manifest(json.dumps(really_big_manifest))
        test_fs_permission_settings: PosixFileSystemPermissionSettings = (
            PosixFileSystemPermissionSettings(
                os_user="test-user",
                os_group="test-group",
                dir_mode=0o20,
                file_mode=0o20,
            )
        )
        os_env_vars: Dict[str, str] = {"AWS_PROFILE": "test-profile"}
        assert job.attachments

        # WHEN
        with patch(
            f"{deadline.__package__}.job_attachments.asset_sync.get_manifest_from_s3",
            return_value=test_manifest,
        ), patch(
            f"{deadline.__package__}.job_attachments.asset_sync.download_files_from_manifests",
            side_effect=[DownloadSummaryStatistics()],
        ), patch(
            f"{deadline.__package__}.job_attachments.asset_sync._get_unique_dest_dir_name",
            side_effect=[dest_dir],
        ), patch.object(
            Path, "stat", MagicMock(st_mtime_ns=1234512345123451)
        ):
            mock_on_downloading_files = MagicMock(return_value=True)

            with pytest.raises(AssetSyncError) as ase:
                self.default_asset_sync.sync_inputs(
                    s3_settings,
                    job.attachments,
                    default_queue.queueId,
                    job.jobId,
                    tmp_path,
                    on_downloading_files=mock_on_downloading_files,
                    fs_permission_settings=test_fs_permission_settings,
                    os_env_vars=os_env_vars,
                )

            # THEN
            assert (
                "Total file size required for download (300.0 PB) is larger than available disk space"
                in str(ase)
            )

    @mock_aws
    @pytest.mark.parametrize(
        (
            "s3_settings_fixture_name",
            "attachments_fixture_name",
            "expected_cas_prefix",
            "expected_output_prefix",
        ),
        [
            (
                "default_job_attachment_s3_settings",
                "default_attachments",
                "assetRoot/Data/",
                "assetRoot/Manifests/farm-1234567890abcdefghijklmnopqrstuv/queue-01234567890123456789012345678901/job-01234567890123456789012345678901/test_step4/test_task4/2023-07-13T14:35:26.123456Z_session-action-1/",
            ),
            (
                "default_job_attachment_s3_settings",
                "windows_attachments",
                "assetRoot/Data/",
                "assetRoot/Manifests/farm-1234567890abcdefghijklmnopqrstuv/queue-01234567890123456789012345678901/job-01234567890123456789012345678901/test_step4/test_task4/2023-07-13T14:35:26.123456Z_session-action-1/",
            ),
        ],
    )
    def test_sync_outputs(
        self,
        tmp_path: Path,
        default_queue: Queue,
        default_job: Job,
        session_action_id: str,
        s3_settings_fixture_name: str,
        attachments_fixture_name: str,
        expected_cas_prefix: str,
        expected_output_prefix: str,
        request: pytest.FixtureRequest,
        assert_expected_files_on_s3,
        assert_canonical_manifest,
    ):
        """
        Test that output files get uploaded to the CAS, skipping upload for files that are already in the CAS,
        and tests that an output manifest is uploaded to the Output prefix.
        """
        # GIVEN
        s3_settings: JobAttachmentS3Settings = request.getfixturevalue(s3_settings_fixture_name)
        attachments: Attachments = request.getfixturevalue(attachments_fixture_name)
        default_queue.jobAttachmentSettings = s3_settings
        default_job.attachments = attachments
        root_path = str(tmp_path)
        local_root = Path(f"{root_path}/assetroot-15addf56bb1a568df964")
        test_step = "test_step4"
        test_task = "test_task4"

        expected_output_root = Path(local_root).joinpath("test/outputs")
        expected_file_path = Path(expected_output_root).joinpath("test.txt")
        expected_sub_file_path = Path(expected_output_root).joinpath("inner_dir/test2.txt")

        expected_file_rel_path = "test/outputs/test.txt"
        expected_sub_file_rel_path = "test/outputs/inner_dir/test2.txt"

        # Add the files to S3
        s3 = boto3.Session(region_name="us-west-2").resource("s3")  # pylint: disable=invalid-name
        bucket = s3.Bucket(s3_settings.s3BucketName)
        bucket.put_object(
            Key=f"{expected_cas_prefix}hash1.xxh128",
            Body="a",
        )
        expected_metadata = s3.meta.client.head_object(
            Bucket=s3_settings.s3BucketName, Key=f"{expected_cas_prefix}hash1.xxh128"
        )

        # WHEN
        with patch(
            f"{deadline.__package__}.job_attachments.asset_sync.hash_file",
            side_effect=["hash1", "hash2"],
        ), patch(
            f"{deadline.__package__}.job_attachments.asset_sync.hash_data", side_effect=["hash3"]
        ), patch(
            f"{deadline.__package__}.job_attachments.asset_sync._get_unique_dest_dir_name",
            side_effect=[local_root],
        ), patch(
            f"{deadline.__package__}.job_attachments.asset_sync._float_to_iso_datetime_string",
            side_effect=["2023-07-13T14:35:26.123456Z"],
        ):
            mock_on_uploading_files = MagicMock(return_value=True)

            try:
                # Need to test having multiple files and subdirectories with files
                Path(expected_file_path).parent.mkdir(parents=True, exist_ok=True)
                with open(expected_file_path, "w") as test_file:
                    test_file.write("Test Output\n")
                Path(expected_sub_file_path).parent.mkdir(parents=True, exist_ok=True)
                with open(expected_sub_file_path, "w") as test_file:
                    test_file.write("Test Sub-Output\n")

                expected_processed_bytes = expected_sub_file_path.resolve().stat().st_size
                expected_skipped_bytes = expected_file_path.resolve().stat().st_size
                expected_total_bytes = expected_processed_bytes + expected_skipped_bytes
                expected_file_mtime = trunc(expected_file_path.stat().st_mtime_ns // 1000)
                expected_sub_file_mtime = trunc(expected_sub_file_path.stat().st_mtime_ns // 1000)

                # Actually run the test
                summary_statistics = self.default_asset_sync.sync_outputs(
                    s3_settings=s3_settings,
                    attachments=attachments,
                    queue_id=default_queue.queueId,
                    job_id=default_job.jobId,
                    step_id=test_step,
                    task_id=test_task,
                    session_action_id=session_action_id,
                    start_time=1234.56,
                    session_dir=tmp_path,
                    on_uploading_files=mock_on_uploading_files,
                )
            finally:
                # Need to clean up after
                if local_root.exists():
                    shutil.rmtree(local_root)

            # THEN
            actual_metadata = s3.meta.client.head_object(
                Bucket=s3_settings.s3BucketName, Key=f"{expected_cas_prefix}hash1.xxh128"
            )
            assert actual_metadata["LastModified"] == expected_metadata["LastModified"]
            assert_expected_files_on_s3(
                bucket,
                expected_files={
                    f"{expected_cas_prefix}hash1.xxh128",
                    f"{expected_cas_prefix}hash2.xxh128",
                    f"{expected_output_prefix}hash3_output",
                },
            )

            assert_canonical_manifest(
                bucket,
                f"{expected_output_prefix}hash3_output",
                expected_manifest='{"hashAlg":"xxh128","manifestVersion":"2023-03-03",'
                f'"paths":[{{"hash":"hash2","mtime":{expected_sub_file_mtime},"path":"{expected_sub_file_rel_path}",'
                f'"size":{expected_processed_bytes}}},'
                f'{{"hash":"hash1","mtime":{expected_file_mtime},"path":"{expected_file_rel_path}",'
                f'"size":{expected_skipped_bytes}}}],'
                f'"totalSize":{expected_total_bytes}}}',
            )

            readable_total_input_bytes = _human_readable_file_size(expected_total_bytes)

            expected_summary_statistics = SummaryStatistics(
                total_time=summary_statistics.total_time,
                total_files=2,
                total_bytes=expected_total_bytes,
                processed_files=1,
                processed_bytes=expected_processed_bytes,
                skipped_files=1,
                skipped_bytes=expected_skipped_bytes,
                transfer_rate=expected_processed_bytes / summary_statistics.total_time,
            )

            actual_args, _ = mock_on_uploading_files.call_args
            actual_last_progress_report = actual_args[0]
            assert actual_last_progress_report.status == ProgressStatus.UPLOAD_IN_PROGRESS
            assert actual_last_progress_report.progress == 100.0
            assert (
                f"Uploaded {readable_total_input_bytes} / {readable_total_input_bytes} of 2 files (Transfer rate: "
                in actual_last_progress_report.progressMessage
            )

            assert summary_statistics == expected_summary_statistics

    @pytest.mark.parametrize(
        "file_path, directory_path, expected",
        [
            (Path("/path/to/directory/file.txt"), Path("/path/to/directory"), True),
            (Path("/path/to/another/directory/file.txt"), Path("/path/to/directory"), False),
            (Path("/path/to/directory/subdirectory/file.txt"), Path("/path/to/directory"), True),
            (Path("/path/to/directory/file.txt"), Path("/"), True),
            (Path("/path/to/directory/../file.txt"), Path("/path/to"), True),
            (Path("directory/file.txt"), Path("directory"), True),
        ],
    )
    def test_is_file_within_directory(self, file_path, directory_path, expected):
        assert (
            self.default_asset_sync._is_file_within_directory(file_path, directory_path) == expected
        )

    @pytest.mark.skipif(
        is_windows_non_admin(),
        reason="Windows requires Admin to create symlinks, skipping this test.",
    )
    def test_is_file_within_directory_with_symlink(self, tmp_path: Path):
        """
        Test the `_is_file_within_directory` method when dealing with symbolic links.
        Ensures that it correctly identifies whether the target file of the given
        symlink is within the specified directory or not.
        """
        tmp_dir = tmp_path / "tmp_dir"
        tmp_dir.mkdir()

        # Create a file inside the directory
        inside_file_path = tmp_dir / "file.txt"
        inside_file_path.touch()
        # Create a file outside the directory
        outside_file_path = tmp_path / "outside_file.txt"
        outside_file_path.touch()

        # Create a symlink that points to a file inside the directory
        symlink_path_inside = tmp_dir / "symlink_inside.txt"
        os.symlink(inside_file_path, symlink_path_inside)
        # Create a symlink that points to a file outside the directory
        symlink_path_outside = tmp_dir / "symlink_outside.txt"
        os.symlink(outside_file_path, symlink_path_outside)

        assert symlink_path_inside.is_symlink()
        assert symlink_path_outside.is_symlink()
        assert (
            self.default_asset_sync._is_file_within_directory(symlink_path_inside, tmp_dir) is True
        )
        assert (
            self.default_asset_sync._is_file_within_directory(symlink_path_outside, tmp_dir)
            is False
        )

    @pytest.mark.parametrize(
        ("job", "expected_settings"),
        [(Job(jobId="job-98765567890123456789012345678901"), None), (None, None)],
    )
    def test_get_attachments_not_found_return_none(
        self, job: Job, expected_settings: Optional[Attachments]
    ):
        """Tests that get_attachments returns the expected result if Job or settings are None"""
        with patch(f"{deadline.__package__}.job_attachments.asset_sync.get_job", side_effect=[job]):
            actual = self.default_asset_sync.get_attachments("test-farm", "test-queue", "test-job")
            assert actual == expected_settings

    def test_get_attachments_successful(
        self, default_job: Job, default_attachments: Optional[Attachments]
    ):
        """Tests that get_attachments returns the expected result"""
        with patch(
            f"{deadline.__package__}.job_attachments.asset_sync.get_job", side_effect=[default_job]
        ):
            actual = self.default_asset_sync.get_attachments(
                "test-farm", "test-queue", default_job.jobId
            )
            assert actual == default_attachments

    @pytest.mark.parametrize(
        ("queue", "expected_settings"),
        [
            (
                Queue(
                    queueId="queue-98765567890123456789012345678901",
                    displayName="test-queue",
                    farmId="test-farm",
                    status="test",
                    defaultBudgetAction="NONE",
                ),
                None,
            ),
            (None, None),
        ],
    )
    def test_get_s3_settings_not_found_return_none(
        self, queue: Queue, expected_settings: Optional[JobAttachmentS3Settings]
    ):
        """Tests that get_s3_settings returns the expected result if Queue or S3 settings are None"""
        with patch(
            f"{deadline.__package__}.job_attachments.asset_sync.get_queue", side_effect=[queue]
        ):
            actual = self.default_asset_sync.get_s3_settings("test-farm", "test-queue")
            assert actual == expected_settings

    def test_get_s3_settings_successful(
        self,
        default_queue: Queue,
        default_job_attachment_s3_settings: Optional[JobAttachmentS3Settings],
    ):
        """Tests that get_s3_settings returns the expected result"""
        with patch(
            f"{deadline.__package__}.job_attachments.asset_sync.get_queue",
            side_effect=[default_queue],
        ):
            actual = self.default_asset_sync.get_s3_settings("test-farm", default_queue.queueId)
            assert actual == default_job_attachment_s3_settings

    def test_sync_inputs_with_storage_profiles_path_mapping_rules(
        self,
        default_queue: Queue,
        default_job: Job,
        test_manifest_one: dict,
        tmp_path: Path,
    ):
        """Tests when a non-empty `storage_profiles_path_mapping_rules` is passed to `sync_inputs`.
        Check that, for input manifests with an `fileSystemLocationName`, if the root path
        corresponding to it exists in the `storage_profiles_path_mapping_rules`, the download
        is attempted to the correct destination path."""
        # GIVEN
        default_job.attachments = Attachments(
            manifests=[
                ManifestProperties(
                    rootPath="/tmp",
                    rootPathFormat=PathFormat.POSIX,
                    inputManifestPath="manifest_input",
                    inputManifestHash="manifesthash",
                    outputRelativeDirectories=["test/outputs"],
                ),
                ManifestProperties(
                    fileSystemLocationName="Movie 1",
                    rootPath="/home/user/movie1",
                    rootPathFormat=PathFormat.POSIX,
                    inputManifestPath="manifest-movie1_input",
                    inputManifestHash="manifestmovie1hash",
                    outputRelativeDirectories=["test/outputs"],
                ),
            ],
        )
        test_manifest = decode_manifest(json.dumps(test_manifest_one))
        dest_dir = "assetroot-27bggh78dd2b568ab123"
        local_root = str(tmp_path.joinpath(dest_dir))

        storage_profiles_path_mapping_rules = {
            "/home/user/movie1": "/tmp/movie1",
        }

        # WHEN
        with patch(
            f"{deadline.__package__}.job_attachments.asset_sync.get_manifest_from_s3",
            return_value=test_manifest,
        ), patch(
            f"{deadline.__package__}.job_attachments.asset_sync.download_files_from_manifests",
            return_value=DownloadSummaryStatistics(),
        ) as mock_download_files_from_manifests, patch(
            f"{deadline.__package__}.job_attachments.asset_sync._get_unique_dest_dir_name",
            side_effect=[dest_dir],
        ), patch.object(
            Path, "stat", MagicMock(st_mtime_ns=1234512345123451)
        ):
            mock_on_downloading_files = MagicMock(return_value=True)

            (summary_statistics, result_pathmap_rules) = self.default_asset_sync.sync_inputs(
                s3_settings=default_queue.jobAttachmentSettings,
                attachments=default_job.attachments,
                queue_id=default_queue.queueId,
                job_id=default_job.jobId,
                session_dir=tmp_path,
                storage_profiles_path_mapping_rules=storage_profiles_path_mapping_rules,
                on_downloading_files=mock_on_downloading_files,
            )

            # THEN
            assert result_pathmap_rules == [
                {
                    "source_path_format": "posix",
                    "source_path": default_job.attachments.manifests[0].rootPath,
                    "destination_path": local_root,
                }
            ]

            mock_download_files_from_manifests.assert_called_once_with(
                s3_bucket="test-bucket",
                manifests_by_root={
                    f"{local_root}": test_manifest,
                    "/tmp/movie1": test_manifest,
                },
                cas_prefix="assetRoot/Data",
                fs_permission_settings=None,
                session=ANY,
                on_downloading_files=mock_on_downloading_files,
                logger=getLogger("deadline.job_attachments"),
            )

    @pytest.mark.parametrize(
        ("job_fixture_name"),
        [
            ("default_job"),
            ("vfs_job"),
        ],
    )
    @pytest.mark.parametrize(
        ("s3_settings_fixture_name"),
        [
            ("default_job_attachment_s3_settings"),
        ],
    )
    def test_sync_inputs_successful_using_vfs_fallback(
        self,
        tmp_path: Path,
        default_queue: Queue,
        job_fixture_name: str,
        s3_settings_fixture_name: str,
        test_manifest_one: dict,
        request: pytest.FixtureRequest,
    ):
        """Asserts that a valid manifest can be processed to download attachments from S3"""
        # GIVEN
        job: Job = request.getfixturevalue(job_fixture_name)
        s3_settings: JobAttachmentS3Settings = request.getfixturevalue(s3_settings_fixture_name)
        default_queue.jobAttachmentSettings = s3_settings
        session_dir = str(tmp_path)
        dest_dir = "assetroot-27bggh78dd2b568ab123"
        local_root = str(Path(session_dir) / dest_dir)
        test_manifest = decode_manifest(json.dumps(test_manifest_one))
        assert job.attachments

        # WHEN
        with patch(
            f"{deadline.__package__}.job_attachments.asset_sync.get_manifest_from_s3",
            return_value=test_manifest,
        ), patch(
            f"{deadline.__package__}.job_attachments.asset_sync.download_files_from_manifests",
            side_effect=[DownloadSummaryStatistics()],
        ), patch(
            f"{deadline.__package__}.job_attachments.asset_sync._get_unique_dest_dir_name",
            side_effect=[dest_dir],
        ), patch(
            f"{deadline.__package__}.job_attachments.asset_sync.VFSProcessManager.find_vfs",
            side_effect=VFSExecutableMissingError,
        ), patch(
            f"{deadline.__package__}.job_attachments.asset_sync.mount_vfs_from_manifests"
        ) as mock_mount_vfs, patch(
            "sys.platform", "linux"
        ), patch.object(
            Path, "stat", MagicMock(st_mtime_ns=1234512345123451)
        ):
            mock_on_downloading_files = MagicMock(return_value=True)

            (_, result_pathmap_rules) = self.default_asset_sync.sync_inputs(
                s3_settings,
                job.attachments,
                default_queue.queueId,
                job.jobId,
                tmp_path,
                on_downloading_files=mock_on_downloading_files,
            )

            # THEN
            expected_source_path_format = (
                "windows"
                if job.attachments.manifests[0].rootPathFormat == PathFormat.WINDOWS
                else "posix"
            )
            assert result_pathmap_rules == [
                {
                    "source_path_format": expected_source_path_format,
                    "source_path": job.attachments.manifests[0].rootPath,
                    "destination_path": local_root,
                }
            ]
            mock_mount_vfs.assert_not_called()

    def test_cleanup_session_vfs_terminate_called(self, tmp_path):
        with patch(
            f"{deadline.__package__}.job_attachments.asset_sync.VFSProcessManager.find_vfs",
        ) as mock_find_vfs, patch(
            f"{deadline.__package__}.job_attachments.asset_sync.VFSProcessManager.kill_all_processes",
        ):
            self.default_asset_sync.cleanup_session(
                session_dir=tmp_path,
                file_system=JobAttachmentsFileSystem.COPIED,
                os_user="test-user",
            )

            mock_find_vfs.assert_not_called()

            self.default_asset_sync.cleanup_session(
                session_dir=tmp_path,
                file_system=JobAttachmentsFileSystem.VIRTUAL,
                os_user="test-user",
            )

            mock_find_vfs.assert_called_once()

    def test_cleanup_session_virtual_witout_os_user_raises(self, tmp_path):
        self.default_asset_sync.cleanup_session(
            session_dir=tmp_path,
            file_system=JobAttachmentsFileSystem.COPIED,
        )

        with pytest.raises(VFSOSUserNotSetError):
            self.default_asset_sync.cleanup_session(
                session_dir=tmp_path,
                file_system=JobAttachmentsFileSystem.VIRTUAL,
            )

    def test_attachment_sync_inputs_no_inputs_successful(
        self,
        tmp_path: Path,
        default_queue: Queue,
        default_job: Job,
        attachments_no_inputs: Attachments,
    ):
        """Asserts that sync_inputs is successful when no required assets exist for the Job"""
        # GIVEN
        default_job.attachments = attachments_no_inputs
        session_dir = str(tmp_path)
        dest_dir = "assetroot-27bggh78dd2b568ab123"
        local_root = str(Path(session_dir) / dest_dir)
        # WHEN
        with patch(
            f"{deadline.__package__}.job_attachments.asset_sync.download_files_from_manifests",
            side_effect=[DownloadSummaryStatistics()],
        ), patch(
            f"{deadline.__package__}.job_attachments.asset_sync._get_unique_dest_dir_name",
            side_effect=[dest_dir],
        ):
            mock_on_downloading_files = MagicMock(return_value=True)

            (summary_statistics, result_pathmap_rules) = (
                self.default_asset_sync.attachment_sync_inputs(
                    default_queue.jobAttachmentSettings,
                    attachments_no_inputs,
                    default_queue.queueId,
                    default_job.jobId,
                    tmp_path,
                    on_downloading_files=mock_on_downloading_files,
                )
            )

            # THEN
            expected_source_path_format = (
                "windows"
                if default_job.attachments.manifests[0].rootPathFormat == PathFormat.WINDOWS
                else "posix"
            )
            assert result_pathmap_rules == [
                {
                    "source_path_format": expected_source_path_format,
                    "source_path": default_job.attachments.manifests[0].rootPath,
                    "destination_path": local_root,
                }
            ]
            expected_summary_statistics = SummaryStatistics(
                total_time=summary_statistics.total_time,
                total_files=0,
                total_bytes=0,
                processed_files=0,
                processed_bytes=0,
                skipped_files=0,
                skipped_bytes=0,
                transfer_rate=0.0,
            )
            assert summary_statistics == expected_summary_statistics

    @pytest.mark.parametrize(
        ("job_fixture_name"),
        [
            ("default_job"),
            ("vfs_job"),
        ],
    )
    @pytest.mark.parametrize(
        ("s3_settings_fixture_name"),
        [
            ("default_job_attachment_s3_settings"),
        ],
    )
    def test_attachment_sync_inputs_successful(
        self,
        tmp_path: Path,
        default_queue: Queue,
        job_fixture_name: str,
        s3_settings_fixture_name: str,
        test_manifest_one: dict,
        request: pytest.FixtureRequest,
    ):
        """Asserts that a valid manifest can be processed to download attachments from S3"""
        # GIVEN
        job: Job = request.getfixturevalue(job_fixture_name)
        s3_settings: JobAttachmentS3Settings = request.getfixturevalue(s3_settings_fixture_name)
        default_queue.jobAttachmentSettings = s3_settings
        session_dir = str(tmp_path)
        dest_dir = "assetroot-27bggh78dd2b568ab123"
        local_root = str(Path(session_dir) / dest_dir)
        test_manifest = decode_manifest(json.dumps(test_manifest_one))
        test_fs_permission_settings: PosixFileSystemPermissionSettings = (
            PosixFileSystemPermissionSettings(
                os_user="test-user",
                os_group="test-group",
                dir_mode=0o20,
                file_mode=0o20,
            )
        )
        os_env_vars: Dict[str, str] = {"AWS_PROFILE": "test-profile"}
        assert job.attachments
        # WHEN
        with patch(
            f"{deadline.__package__}.job_attachments.asset_sync.get_manifest_from_s3",
            return_value=test_manifest,
        ), patch(
            f"{deadline.__package__}.job_attachments.asset_sync.download_files_from_manifests",
            side_effect=[DownloadSummaryStatistics()],
        ), patch(
            f"{deadline.__package__}.job_attachments.asset_sync._get_unique_dest_dir_name",
            side_effect=[dest_dir],
        ), patch(
            f"{deadline.__package__}.job_attachments.asset_sync.mount_vfs_from_manifests"
        ), patch(
            f"{deadline.__package__}.job_attachments.asset_sync.VFSProcessManager.find_vfs"
        ), patch.object(
            Path, "stat", MagicMock(st_mtime_ns=1234512345123451)
        ):
            mock_on_downloading_files = MagicMock(return_value=True)

            (_, result_pathmap_rules) = self.default_asset_sync.attachment_sync_inputs(
                s3_settings,
                job.attachments,
                default_queue.queueId,
                job.jobId,
                tmp_path,
                on_downloading_files=mock_on_downloading_files,
                fs_permission_settings=test_fs_permission_settings,
                os_env_vars=os_env_vars,
            )
            # THEN
            expected_source_path_format = (
                "windows"
                if job.attachments.manifests[0].rootPathFormat == PathFormat.WINDOWS
                else "posix"
            )
            assert result_pathmap_rules == [
                {
                    "source_path_format": expected_source_path_format,
                    "source_path": job.attachments.manifests[0].rootPath,
                    "destination_path": local_root,
                }
            ]

    @pytest.mark.parametrize(
        ("job_fixture_name"),
        [
            ("default_job"),
        ],
    )
    @pytest.mark.parametrize(
        ("s3_settings_fixture_name"),
        [
            ("default_job_attachment_s3_settings"),
        ],
    )
    def test_attachment_sync_inputs_404_error(
        self,
        tmp_path: Path,
        default_queue: Queue,
        job_fixture_name: str,
        s3_settings_fixture_name: str,
        test_manifest_one: dict,
        request: pytest.FixtureRequest,
    ):
        """Asserts that a specific error message is raised when getting 404 errors synching inputs"""
        # GIVEN
        download_exception = JobAttachmentsS3ClientError(
            action="get-object",
            status_code=404,
            bucket_name="test bucket",
            key_or_prefix="test-key.xxh128",
            message="File not found",
        )
        job: Job = request.getfixturevalue(job_fixture_name)
        test_manifest = decode_manifest(json.dumps(test_manifest_one))
        s3_settings: JobAttachmentS3Settings = request.getfixturevalue(s3_settings_fixture_name)
        default_queue.jobAttachmentSettings = s3_settings
        dest_dir = "assetroot-27bggh78dd2b568ab123"
        assert job.attachments
        # WHEN
        with patch(
            f"{deadline.__package__}.job_attachments.asset_sync.get_manifest_from_s3",
            return_value=test_manifest,
        ), patch(
            f"{deadline.__package__}.job_attachments.asset_sync._get_unique_dest_dir_name",
            side_effect=[dest_dir],
        ), patch(
            f"{deadline.__package__}.job_attachments.asset_sync.download_files_from_manifests",
            side_effect=download_exception,
        ):
            with pytest.raises(JobAttachmentsS3ClientError) as excinfo:
                self.default_asset_sync.attachment_sync_inputs(
                    s3_settings,
                    job.attachments,
                    default_queue.queueId,
                    job.jobId,
                    tmp_path,
                )
        # THEN
        assert "usually located in the home directory (~/.deadline/cache/s3_check_cache.db)" in str(
            excinfo
        )

    @pytest.mark.parametrize(
        ("s3_settings_fixture_name"),
        [
            ("default_job_attachment_s3_settings"),
        ],
    )
    def test_attachment_sync_inputs_with_step_dependencies(
        self,
        tmp_path: Path,
        default_queue: Queue,
        default_job: Job,
        s3_settings_fixture_name: str,
        test_manifest_one: dict,
        request: pytest.FixtureRequest,
    ):
        """Asserts that input syncing is done correctly when step dependencies are provided."""
        # GIVEN
        s3_settings: JobAttachmentS3Settings = request.getfixturevalue(s3_settings_fixture_name)
        default_queue.jobAttachmentSettings = s3_settings
        session_dir = str(tmp_path)
        dest_dir = "assetroot-27bggh78dd2b568ab123"
        local_root = str(Path(session_dir) / dest_dir)
        test_manifest = decode_manifest(json.dumps(test_manifest_one))
        assert default_job.attachments
        step_output_root = "/home/outputs_roots"
        step_dest_dir = "assetroot-8a7d189e9c17186fb88b"
        # WHEN
        with patch(
            f"{deadline.__package__}.job_attachments.asset_sync.get_manifest_from_s3",
            return_value=test_manifest,
        ), patch(
            f"{deadline.__package__}.job_attachments.asset_sync.download_files_from_manifests",
            side_effect=[DownloadSummaryStatistics()],
        ), patch(
            f"{deadline.__package__}.job_attachments.asset_sync._get_unique_dest_dir_name",
            side_effect=[dest_dir, step_dest_dir],
        ), patch(
            f"{deadline.__package__}.job_attachments.asset_sync.get_output_manifests_by_asset_root",
            side_effect=[{step_output_root: {}}],
        ), patch.object(
            Path, "stat", MagicMock(st_mtime_ns=1234512345123451)
        ):
            mock_on_downloading_files = MagicMock(return_value=True)

            (_, result_pathmap_rules) = self.default_asset_sync.attachment_sync_inputs(
                s3_settings,
                default_job.attachments,
                default_queue.queueId,
                default_job.jobId,
                tmp_path,
                step_dependencies=["step-1"],
                on_downloading_files=mock_on_downloading_files,
            )
            # THEN
            expected_source_path_format = (
                "windows"
                if default_job.attachments.manifests[0].rootPathFormat == PathFormat.WINDOWS
                else "posix"
            )
            assert result_pathmap_rules == [
                {
                    "source_path_format": expected_source_path_format,
                    "source_path": default_job.attachments.manifests[0].rootPath,
                    "destination_path": local_root,
                },
            ]

    @pytest.mark.parametrize(
        ("s3_settings_fixture_name"),
        [
            ("default_job_attachment_s3_settings"),
        ],
    )
    def test_attachment_sync_inputs_with_step_dependencies_same_root_vfs_on_posix(
        self,
        tmp_path: Path,
        default_queue: Queue,
        vfs_job: Job,
        s3_settings_fixture_name: str,
        test_manifest_one: dict,
        test_manifest_two: dict,
        request: pytest.FixtureRequest,
    ):
        """Asserts that input syncing is done correctly when step dependencies are provided."""
        # GIVEN
        job = vfs_job
        s3_settings: JobAttachmentS3Settings = request.getfixturevalue(s3_settings_fixture_name)
        default_queue.jobAttachmentSettings = s3_settings
        session_dir = str(tmp_path)
        dest_dir = "assetroot-27bggh78dd2b568ab123"
        local_root = str(Path(session_dir) / dest_dir)
        test_fs_permission_settings: PosixFileSystemPermissionSettings = (
            PosixFileSystemPermissionSettings(
                os_user="test-user",
                os_group="test-group",
                dir_mode=0o20,
                file_mode=0o20,
            )
        )
        os_env_vars: Dict[str, str] = {"AWS_PROFILE": "test-profile"}
        assert job.attachments
        test_manifest = decode_manifest(json.dumps(test_manifest_two))
        # WHEN
        with patch(
            f"{deadline.__package__}.job_attachments.asset_sync.get_manifest_from_s3",
            return_value=json.dumps(test_manifest_one),
        ), patch(
            f"{deadline.__package__}.job_attachments.asset_sync.download_files_from_manifests",
            side_effect=[DownloadSummaryStatistics()],
        ), patch(
            f"{deadline.__package__}.job_attachments.asset_sync._get_unique_dest_dir_name",
            return_value=dest_dir,
        ), patch(
            f"{deadline.__package__}.job_attachments.asset_sync.get_output_manifests_by_asset_root",
            return_value={"tmp/": [(test_manifest, "hello")]},
        ), patch(
            f"{deadline.__package__}.job_attachments.asset_sync.merge_asset_manifests",
        ) as merge_manifests_mock, patch(
            f"{deadline.__package__}.job_attachments.asset_sync.AssetSync._ensure_disk_capacity",
        ) as disk_capacity_mock, patch(
            f"{deadline.__package__}.job_attachments.download._write_manifest_to_temp_file",
            return_value="tmp_manifest",
        ), patch(
            "sys.platform", "linux"
        ), patch(
            f"{deadline.__package__}.job_attachments.asset_sync.mount_vfs_from_manifests"
        ), patch(
            f"{deadline.__package__}.job_attachments.asset_sync.VFSProcessManager.find_vfs"
        ):
            mock_on_downloading_files = MagicMock(return_value=True)

            (_, result_pathmap_rules) = self.default_asset_sync.attachment_sync_inputs(
                s3_settings,
                job.attachments,
                default_queue.queueId,
                job.jobId,
                tmp_path,
                step_dependencies=["step-1"],
                on_downloading_files=mock_on_downloading_files,
                fs_permission_settings=test_fs_permission_settings,
                os_env_vars=os_env_vars,
            )
            # THEN
            merge_manifests_mock.assert_called()
            disk_capacity_mock.assert_not_called()
            expected_source_path_format = (
                "windows"
                if job.attachments.manifests[0].rootPathFormat == PathFormat.WINDOWS
                else "posix"
            )
            assert result_pathmap_rules == [
                {
                    "source_path_format": expected_source_path_format,
                    "source_path": job.attachments.manifests[0].rootPath,
                    "destination_path": local_root,
                },
            ]

    @pytest.mark.parametrize(
        ("job_fixture_name"),
        [
            ("default_job"),
        ],
    )
    @pytest.mark.parametrize(
        ("s3_settings_fixture_name"),
        [
            ("default_job_attachment_s3_settings"),
        ],
    )
    def test_attachment_sync_inputs_no_space_left(
        self,
        tmp_path: Path,
        default_queue: Queue,
        job_fixture_name: str,
        s3_settings_fixture_name: str,
        really_big_manifest: dict,
        request: pytest.FixtureRequest,
    ):
        """Asserts that an AssetSyncError is thrown if there is not enough space left on the device to download all inputs."""
        # GIVEN
        job: Job = request.getfixturevalue(job_fixture_name)
        s3_settings: JobAttachmentS3Settings = request.getfixturevalue(s3_settings_fixture_name)
        default_queue.jobAttachmentSettings = s3_settings
        dest_dir = "assetroot-27bggh78dd2b568ab123"
        test_manifest = decode_manifest(json.dumps(really_big_manifest))
        test_fs_permission_settings: PosixFileSystemPermissionSettings = (
            PosixFileSystemPermissionSettings(
                os_user="test-user",
                os_group="test-group",
                dir_mode=0o20,
                file_mode=0o20,
            )
        )
        os_env_vars: Dict[str, str] = {"AWS_PROFILE": "test-profile"}
        assert job.attachments
        # WHEN
        with patch(
            f"{deadline.__package__}.job_attachments.asset_sync.get_manifest_from_s3",
            return_value=test_manifest,
        ), patch(
            f"{deadline.__package__}.job_attachments.asset_sync.download_files_from_manifests",
            side_effect=[DownloadSummaryStatistics()],
        ), patch(
            f"{deadline.__package__}.job_attachments.asset_sync._get_unique_dest_dir_name",
            side_effect=[dest_dir],
        ), patch.object(
            Path, "stat", MagicMock(st_mtime_ns=1234512345123451)
        ):
            mock_on_downloading_files = MagicMock(return_value=True)

            with pytest.raises(AssetSyncError) as ase:
                self.default_asset_sync.attachment_sync_inputs(
                    s3_settings,
                    job.attachments,
                    default_queue.queueId,
                    job.jobId,
                    tmp_path,
                    on_downloading_files=mock_on_downloading_files,
                    fs_permission_settings=test_fs_permission_settings,
                    os_env_vars=os_env_vars,
                )

            # THEN
            assert (
                "Total file size required for download (300.0 PB) is larger than available disk space"
                in str(ase)
            )

    def test_aggregate_asset_root_manifests_and_write(
        self,
        default_queue: Queue,
        default_job: Job,
        default_job_attachment_s3_settings: JobAttachmentS3Settings,
        test_manifest_one: dict,
        tmp_path: Path,
    ):
        test_manifest = decode_manifest(json.dumps(test_manifest_one))
        dest_dir = "assetroot"

        default_job.attachments = Attachments(
            manifests=[
                ManifestProperties(
                    rootPath="/root/tmp",
                    rootPathFormat=PathFormat.POSIX,
                    inputManifestPath="manifest_input",
                    inputManifestHash="manifesthash",
                    outputRelativeDirectories=["test/outputs"],
                ),
                ManifestProperties(
                    fileSystemLocationName="Movie 1",
                    rootPath="/home/user/movie1",
                    rootPathFormat=PathFormat.POSIX,
                    inputManifestPath="manifest-movie1_input",
                    inputManifestHash="manifestmovie1hash",
                    outputRelativeDirectories=["test/outputs"],
                ),
            ],
        )
        manifest_count = len(default_job.attachments.manifests)
        storage_profiles_path_mapping_rules = {
            "/home/user/movie1": "/root/tmp/movie1",
        }
        path_write_local_input_manifest = tmp_path.joinpath("manifest/hash_manifest")

        with patch(
            f"{deadline.__package__}.job_attachments.asset_sync.get_manifest_from_s3",
            return_value=test_manifest,
        ) as mock_get_manifest_from_s3, patch(
            f"{deadline.__package__}.job_attachments.asset_sync.merge_asset_manifests",
            return_value=test_manifest,
        ) as mock_merge_asset_manifests, patch(
            f"{deadline.__package__}.job_attachments.asset_sync._get_unique_dest_dir_name",
            side_effect=[dest_dir],
        ), patch(
            f"{deadline.__package__}.job_attachments.asset_sync.S3AssetUploader._write_local_input_manifest",
            return_value=path_write_local_input_manifest,
        ) as mock__write_local_input_manifest:

            merged_manifests_by_root = self.default_asset_sync._aggregate_asset_root_manifests(
                session_dir=tmp_path,
                s3_settings=default_job_attachment_s3_settings,
                queue_id=default_queue.queueId,
                job_id=default_job.jobId,
                attachments=default_job.attachments,
                dynamic_mapping_rules=self.default_asset_sync.generate_dynamic_path_mapping(
                    session_dir=tmp_path, attachments=default_job.attachments
                ),
                storage_profiles_path_mapping_rules=storage_profiles_path_mapping_rules,
            )
            assert mock_merge_asset_manifests.call_count == manifest_count
            assert mock_get_manifest_from_s3.call_count == manifest_count

            manifest_paths_by_root = self.default_asset_sync._check_and_write_local_manifests(
                merged_manifests_by_root=merged_manifests_by_root,
                manifest_write_dir=str(tmp_path),
                manifest_name_suffix="test",
            )
            assert mock__write_local_input_manifest.call_count == manifest_count
            assert len(self.default_asset_sync._local_root_to_src_map) == len(
                manifest_paths_by_root
            )
            assert len(manifest_paths_by_root) == manifest_count
            assert "/root/tmp/movie1" in manifest_paths_by_root
            assert "test" in manifest_paths_by_root["/root/tmp/movie1"]
            assert str(tmp_path.joinpath(dest_dir)) in manifest_paths_by_root

    def test_attachment_sync_inputs_with_storage_profiles_path_mapping_rules(
        self,
        default_queue: Queue,
        default_job: Job,
        test_manifest_one: dict,
        tmp_path: Path,
    ):
        """Tests when a non-empty `storage_profiles_path_mapping_rules` is passed to `sync_inputs`.
        Check that, for input manifests with an `fileSystemLocationName`, if the root path
        corresponding to it exists in the `storage_profiles_path_mapping_rules`, the download
        is attempted to the correct destination path."""
        # GIVEN
        default_job.attachments = Attachments(
            manifests=[
                ManifestProperties(
                    rootPath="/tmp",
                    rootPathFormat=PathFormat.POSIX,
                    inputManifestPath="manifest_input",
                    inputManifestHash="manifesthash",
                    outputRelativeDirectories=["test/outputs"],
                ),
                ManifestProperties(
                    fileSystemLocationName="Movie 1",
                    rootPath="/home/user/movie1",
                    rootPathFormat=PathFormat.POSIX,
                    inputManifestPath="manifest-movie1_input",
                    inputManifestHash="manifestmovie1hash",
                    outputRelativeDirectories=["test/outputs"],
                ),
            ],
        )
        test_manifest = decode_manifest(json.dumps(test_manifest_one))
        dest_dir = "assetroot-27bggh78dd2b568ab123"
        local_root = str(tmp_path.joinpath(dest_dir))

        storage_profiles_path_mapping_rules = {
            "/home/user/movie1": "/tmp/movie1",
        }

        # WHEN
        with patch(
            f"{deadline.__package__}.job_attachments.asset_sync.get_manifest_from_s3",
            return_value=test_manifest,
        ), patch(
            f"{deadline.__package__}.job_attachments.asset_sync.download_files_from_manifests",
            return_value=DownloadSummaryStatistics(),
        ) as mock_download_files_from_manifests, patch(
            f"{deadline.__package__}.job_attachments.asset_sync._get_unique_dest_dir_name",
            side_effect=[dest_dir],
        ), patch.object(
            Path, "stat", MagicMock(st_mtime_ns=1234512345123451)
        ):
            mock_on_downloading_files = MagicMock(return_value=True)

            (summary_statistics, result_pathmap_rules) = (
                self.default_asset_sync.attachment_sync_inputs(
                    s3_settings=default_queue.jobAttachmentSettings,
                    attachments=default_job.attachments,
                    queue_id=default_queue.queueId,
                    job_id=default_job.jobId,
                    session_dir=tmp_path,
                    storage_profiles_path_mapping_rules=storage_profiles_path_mapping_rules,
                    on_downloading_files=mock_on_downloading_files,
                )
            )

            # THEN
            assert result_pathmap_rules == [
                {
                    "source_path_format": "posix",
                    "source_path": default_job.attachments.manifests[0].rootPath,
                    "destination_path": local_root,
                }
            ]

            mock_download_files_from_manifests.assert_called_once_with(
                s3_bucket="test-bucket",
                manifests_by_root={
                    f"{local_root}": test_manifest,
                    "/tmp/movie1": test_manifest,
                },
                cas_prefix="assetRoot/Data",
                fs_permission_settings=None,
                session=ANY,
                on_downloading_files=mock_on_downloading_files,
                logger=getLogger("deadline.job_attachments"),
            )

    @pytest.mark.parametrize(
        ("job_fixture_name"),
        [
            ("default_job"),
            ("vfs_job"),
        ],
    )
    @pytest.mark.parametrize(
        ("s3_settings_fixture_name"),
        [
            ("default_job_attachment_s3_settings"),
        ],
    )
    def test_attachment_sync_inputs_successful_using_vfs_fallback(
        self,
        tmp_path: Path,
        default_queue: Queue,
        job_fixture_name: str,
        s3_settings_fixture_name: str,
        test_manifest_one: dict,
        request: pytest.FixtureRequest,
    ):
        """Asserts that a valid manifest can be processed to download attachments from S3"""
        # GIVEN
        job: Job = request.getfixturevalue(job_fixture_name)
        s3_settings: JobAttachmentS3Settings = request.getfixturevalue(s3_settings_fixture_name)
        default_queue.jobAttachmentSettings = s3_settings
        session_dir = str(tmp_path)
        dest_dir = "assetroot-27bggh78dd2b568ab123"
        local_root = str(Path(session_dir) / dest_dir)
        test_manifest = decode_manifest(json.dumps(test_manifest_one))
        assert job.attachments

        # WHEN
        with patch(
            f"{deadline.__package__}.job_attachments.asset_sync.get_manifest_from_s3",
            return_value=test_manifest,
        ), patch(
            f"{deadline.__package__}.job_attachments.asset_sync.download_files_from_manifests",
            side_effect=[DownloadSummaryStatistics()],
        ), patch(
            f"{deadline.__package__}.job_attachments.asset_sync._get_unique_dest_dir_name",
            side_effect=[dest_dir],
        ), patch(
            f"{deadline.__package__}.job_attachments.asset_sync.VFSProcessManager.find_vfs",
            side_effect=VFSExecutableMissingError,
        ), patch(
            f"{deadline.__package__}.job_attachments.asset_sync.mount_vfs_from_manifests"
        ) as mock_mount_vfs, patch(
            "sys.platform", "linux"
        ), patch.object(
            Path, "stat", MagicMock(st_mtime_ns=1234512345123451)
        ):
            mock_on_downloading_files = MagicMock(return_value=True)

            (_, result_pathmap_rules) = self.default_asset_sync.attachment_sync_inputs(
                s3_settings,
                job.attachments,
                default_queue.queueId,
                job.jobId,
                tmp_path,
                on_downloading_files=mock_on_downloading_files,
            )

            # THEN
            expected_source_path_format = (
                "windows"
                if job.attachments.manifests[0].rootPathFormat == PathFormat.WINDOWS
                else "posix"
            )
            assert result_pathmap_rules == [
                {
                    "source_path_format": expected_source_path_format,
                    "source_path": job.attachments.manifests[0].rootPath,
                    "destination_path": local_root,
                }
            ]
            mock_mount_vfs.assert_not_called()
