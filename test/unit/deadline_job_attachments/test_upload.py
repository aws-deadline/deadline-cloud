# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests related to the uploading of assets.
"""

import os
import sys
from copy import deepcopy
from datetime import datetime
from io import BytesIO
from logging import DEBUG, INFO
from pathlib import Path
from typing import Dict, List, Set, Tuple
from unittest.mock import MagicMock, patch

import boto3
import py.path
import pytest
from botocore.exceptions import BotoCoreError, ClientError, ReadTimeoutError
from botocore.stub import Stubber
from moto import mock_aws

import deadline
from deadline.client import config
from deadline.job_attachments.asset_manifests import (
    BaseManifestModel,
    BaseManifestPath,
    HashAlgorithm,
    ManifestVersion,
)
from deadline.job_attachments.caches import HashCacheEntry, S3CheckCacheEntry
from deadline.job_attachments.exceptions import (
    AssetSyncError,
    JobAttachmentsS3ClientError,
    MisconfiguredInputsError,
    MissingS3BucketError,
    MissingS3RootPrefixError,
)
from deadline.job_attachments.models import (
    AssetRootGroup,
    Attachments,
    FileSystemLocation,
    FileSystemLocationType,
    ManifestProperties,
    JobAttachmentS3Settings,
    StorageProfileOperatingSystemFamily,
    PathFormat,
    StorageProfile,
)
from deadline.job_attachments.asset_manifests.v2023_03_03 import AssetManifest

from deadline.job_attachments.progress_tracker import (
    ProgressStatus,
    SummaryStatistics,
)
from deadline.job_attachments.upload import FileStatus, S3AssetManager, S3AssetUploader
from deadline.job_attachments._utils import _human_readable_file_size
from ..conftest import is_windows_non_admin


class TestUpload:
    """
    Tests for handling uploading assets.
    """

    @pytest.fixture(autouse=True)
    def before_test(
        self, request, create_s3_bucket, default_job_attachment_s3_settings: JobAttachmentS3Settings
    ):
        """
        Setup the default queue and s3 bucket for all asset tests.
        Mark test with `no_setup` if you don't want this setup to run.
        """
        if "no_setup" in request.keywords:
            return

        self.job_attachment_s3_settings = default_job_attachment_s3_settings
        create_s3_bucket(bucket_name=default_job_attachment_s3_settings.s3BucketName)

    @mock_aws
    @pytest.mark.parametrize(
        "manifest_version,expected_manifest",
        [
            (
                ManifestVersion.v2023_03_03,
                '{"hashAlg":"xxh128","manifestVersion":"2023-03-03",'
                '"paths":[{"hash":"d","mtime":1234000000,"path":"meta.txt","size":1},'
                '{"hash":"a","mtime":1234000000,"path":"scene/maya.ma","size":1},'
                '{"hash":"c","mtime":1234000000,"path":"textures/normals/normal.png","size":1},'
                '{"hash":"b","mtime":1234000000,"path":"textures/texture.png","size":1}],"totalSize":4}',
            ),
        ],
    )
    def test_asset_management(
        self,
        tmpdir: py.path.local,
        farm_id,
        queue_id,
        default_job_attachment_s3_settings,
        assert_canonical_manifest,
        assert_expected_files_on_s3,
        caplog,
        manifest_version: ManifestVersion,
        expected_manifest: str,
    ):
        """
        Test that the correct files get uploaded to S3 and the asset manifest
        is as expected when there are multiple input and output files.
        """
        # Given
        asset_root = str(tmpdir)

        scene_file = tmpdir.mkdir("scene").join("maya.ma")
        scene_file.write("a")
        os.utime(scene_file, (1234, 1234))

        texture_file = tmpdir.mkdir("textures").join("texture.png")
        texture_file.write("b")
        os.utime(texture_file, (1234, 1234))

        normal_file = tmpdir.join("textures").mkdir("normals").join("normal.png")
        normal_file.write("c")
        os.utime(normal_file, (1234, 1234))

        meta_file = tmpdir.join("meta.txt")
        meta_file.write("d")
        os.utime(meta_file, (1234, 1234))

        cache_dir = tmpdir.mkdir("cache")
        output_dir1 = tmpdir.join("outputs")
        output_dir2 = tmpdir.join("outputs").join("textures")

        history_dir = tmpdir.join("history")
        expected_manifest_file = history_dir.join("manifests").join("e_input")
        expected_mapping_file = history_dir.join("manifests").join("manifest_s3_mapping")
        expected_mapping_contents = f"{{'local_file': 'e_input', 's3_key': '{default_job_attachment_s3_settings.rootPrefix}/Manifests/{farm_id}/{queue_id}/Inputs/0000/e_input'}}\n"
        assert not os.path.exists(history_dir)
        assert not os.path.exists(expected_manifest_file)
        assert not os.path.exists(expected_mapping_file)

        expected_total_input_bytes = (
            scene_file.size() + texture_file.size() + normal_file.size() + meta_file.size()
        )

        with patch(
            f"{deadline.__package__}.job_attachments.upload.PathFormat.get_host_path_format",
            return_value=PathFormat.POSIX,
        ), patch(
            f"{deadline.__package__}.job_attachments.upload.hash_data",
            side_effect=["e", "manifesthash"],
        ), patch(
            f"{deadline.__package__}.job_attachments.upload.hash_file",
            side_effect={
                str(scene_file): "a",
                str(texture_file): "b",
                str(normal_file): "c",
                str(meta_file): "d",
            }.get,
        ), patch(
            f"{deadline.__package__}.job_attachments.models._generate_random_guid",
            return_value="0000",
        ):
            caplog.set_level(DEBUG)

            mock_on_preparing_to_submit = MagicMock(return_value=True)
            mock_on_uploading_assets = MagicMock(return_value=True)

            asset_manager = S3AssetManager(
                farm_id=farm_id,
                queue_id=queue_id,
                job_attachment_settings=self.job_attachment_s3_settings,
                asset_manifest_version=manifest_version,
            )

            # When
            upload_group = asset_manager.prepare_paths_for_upload(
                input_paths=[
                    str(scene_file),
                    str(texture_file),
                    str(normal_file),
                    str(meta_file),
                    str(meta_file),
                    "",
                ],
                output_paths=[
                    str(asset_root),
                    str(output_dir1),
                    str(output_dir2),
                    str(output_dir2),
                    "",
                ],
                referenced_paths=[],
            )
            (
                hash_summary_statistics,
                asset_root_manifests,
            ) = asset_manager.hash_assets_and_create_manifest(
                asset_groups=upload_group.asset_groups,
                total_input_files=upload_group.total_input_files,
                total_input_bytes=upload_group.total_input_bytes,
                hash_cache_dir=str(cache_dir),
                on_preparing_to_submit=mock_on_preparing_to_submit,
            )

            (upload_summary_statistics, attachments) = asset_manager.upload_assets(
                manifests=asset_root_manifests,
                on_uploading_assets=mock_on_uploading_assets,
                s3_check_cache_dir=str(cache_dir),
                manifest_write_dir=str(history_dir),
            )

            # Then
            expected_attachments = Attachments(
                manifests=[
                    ManifestProperties(
                        rootPath=asset_root,
                        rootPathFormat=PathFormat.POSIX,
                        inputManifestPath=f"{farm_id}/{queue_id}/Inputs/0000/e_input",
                        inputManifestHash="manifesthash",
                        outputRelativeDirectories=[
                            ".",
                            "outputs",
                            os.path.join("outputs", "textures"),
                        ],
                    )
                ],
            )

            assert attachments == expected_attachments
            assert attachments.to_dict() == {  # type: ignore
                "fileSystem": "COPIED",
                "manifests": [
                    {
                        "rootPath": f"{asset_root}",
                        "rootPathFormat": PathFormat("posix").value,
                        "inputManifestPath": f"{farm_id}/{queue_id}/Inputs/0000/e_input",
                        "inputManifestHash": "manifesthash",
                        "outputRelativeDirectories": [
                            ".",
                            "outputs",
                            os.path.join("outputs", "textures"),
                        ],
                    }
                ],
            }

            assert f"assetRoot/Manifests/{farm_id}/{queue_id}/Inputs/0000/e_input" in caplog.text

            # Ensure we wrote our manifest file locally
            assert os.path.exists(expected_manifest_file)
            assert os.path.isfile(expected_manifest_file)
            assert os.path.exists(expected_mapping_file)
            assert os.path.isfile(expected_mapping_file)
            with open(expected_mapping_file, "r") as mapping_file:
                actual_contents = mapping_file.read()
            assert actual_contents == expected_mapping_contents

            assert_progress_report_last_callback(
                num_input_files=4,
                expected_total_input_bytes=expected_total_input_bytes,
                on_preparing_to_submit=mock_on_preparing_to_submit,
                on_uploading_assets=mock_on_uploading_assets,
            )

            assert_progress_report_summary_statistics(
                actual_summary_statistics=hash_summary_statistics,
                processed_files=4,
                processed_bytes=expected_total_input_bytes,
                skipped_files=0,
                skipped_bytes=0,
            )

            assert_progress_report_summary_statistics(
                actual_summary_statistics=upload_summary_statistics,
                processed_files=4,
                processed_bytes=expected_total_input_bytes,
                skipped_files=0,
                skipped_bytes=0,
            )

            s3 = boto3.Session(region_name="us-west-2").resource(
                "s3"
            )  # pylint: disable=invalid-name
            bucket = s3.Bucket(self.job_attachment_s3_settings.s3BucketName)

            assert_expected_files_on_s3(
                bucket,
                expected_files={
                    f"assetRoot/Manifests/{farm_id}/{queue_id}/Inputs/0000/e_input",
                    f"{self.job_attachment_s3_settings.full_cas_prefix()}/a.xxh128",
                    f"{self.job_attachment_s3_settings.full_cas_prefix()}/b.xxh128",
                    f"{self.job_attachment_s3_settings.full_cas_prefix()}/c.xxh128",
                    f"{self.job_attachment_s3_settings.full_cas_prefix()}/d.xxh128",
                },
            )

            assert_canonical_manifest(
                bucket,
                f"assetRoot/Manifests/{farm_id}/{queue_id}/Inputs/0000/e_input",
                expected_manifest=expected_manifest,
            )

    @mock_aws
    @pytest.mark.skipif(
        sys.platform != "win32",
        reason="Requires Windows to test resolving paths completely with multiple drives",
    )
    @pytest.mark.parametrize(
        "manifest_version,expected_manifest",
        [
            (
                ManifestVersion.v2023_03_03,
                '{"hashAlg":"xxh128","manifestVersion":"2023-03-03",'
                '"paths":[{"hash":"a","mtime":1234000000,"path":"input.txt","size":1}],"totalSize":1}',
            ),
        ],
    )
    def test_asset_management_windows_multi_root(
        self,
        tmpdir,
        farm_id,
        queue_id,
        assert_canonical_manifest,
        assert_expected_files_on_s3,
        caplog,
        manifest_version,
        expected_manifest,
    ):
        """
        Test that the correct files get uploaded to S3 and the asset manifest
        is as expected when there are multiple input and output files.
        """
        # Given
        root_c = tmpdir.mkdir("c-drive-inputs")
        input_c = root_c.join("input.txt")
        input_c.write("a")
        os.utime(input_c, (1234, 1234))
        root_d = r"D:\my\awesome"
        input_d = r"D:\my\awesome\input2.txt"  # doesn't exist, shouldn't get included
        output_d = r"D:\my\awesome\outputdir"
        cache_dir = tmpdir.mkdir("cache")

        with patch(
            f"{deadline.__package__}.job_attachments.upload.hash_data",
            side_effect=["b", "manifesthash"],
        ), patch(
            f"{deadline.__package__}.job_attachments.upload.hash_file",
            side_effect={str(input_c): "a"}.get,
        ), patch(
            f"{deadline.__package__}.job_attachments.models._generate_random_guid",
            return_value="0000",
        ):
            caplog.set_level(DEBUG)

            mock_on_preparing_to_submit = MagicMock(return_value=True)
            mock_on_uploading_assets = MagicMock(return_value=True)

            asset_manager = S3AssetManager(
                farm_id=farm_id,
                queue_id=queue_id,
                job_attachment_settings=self.job_attachment_s3_settings,
                asset_manifest_version=manifest_version,
            )

            # When
            upload_group = asset_manager.prepare_paths_for_upload(
                input_paths=[input_c, input_d],
                output_paths=[output_d],
                referenced_paths=[],
            )
            (
                hash_summary_statistics,
                asset_root_manifests,
            ) = asset_manager.hash_assets_and_create_manifest(
                asset_groups=upload_group.asset_groups,
                total_input_files=upload_group.total_input_files,
                total_input_bytes=upload_group.total_input_bytes,
                hash_cache_dir=cache_dir,
                on_preparing_to_submit=mock_on_preparing_to_submit,
            )

            (upload_summary_statistics, attachments) = asset_manager.upload_assets(
                manifests=asset_root_manifests,
                on_uploading_assets=mock_on_uploading_assets,
                s3_check_cache_dir=cache_dir,
            )

            # Then
            expected_attachments = Attachments(
                manifests=[
                    ManifestProperties(
                        rootPath=root_c,
                        rootPathFormat=PathFormat.WINDOWS,
                        inputManifestPath=f"{farm_id}/{queue_id}/Inputs/0000/b_input",
                        inputManifestHash="manifesthash",
                        outputRelativeDirectories=[],
                    ),
                    ManifestProperties(
                        rootPath=root_d,
                        rootPathFormat=PathFormat.WINDOWS,
                        outputRelativeDirectories=["outputdir"],
                    ),
                ],
            )
            expected_total_input_bytes = input_c.size()

            assert attachments == expected_attachments
            assert attachments.to_dict() == {  # type: ignore
                "fileSystem": "COPIED",
                "manifests": [
                    {
                        "rootPath": f"{root_c}",
                        "rootPathFormat": PathFormat("windows").value,
                        "inputManifestPath": f"{farm_id}/{queue_id}/Inputs/0000/b_input",
                        "inputManifestHash": "manifesthash",
                    },
                    {
                        "rootPath": f"{root_d}",
                        "rootPathFormat": PathFormat("windows").value,
                        "outputRelativeDirectories": [
                            "outputdir",
                        ],
                    },
                ],
            }

            assert f"assetRoot/Manifests/{farm_id}/{queue_id}/Inputs/0000/b_input" in caplog.text

            assert_progress_report_last_callback(
                num_input_files=1,
                expected_total_input_bytes=expected_total_input_bytes,
                on_preparing_to_submit=mock_on_preparing_to_submit,
                on_uploading_assets=mock_on_uploading_assets,
            )

            assert_progress_report_summary_statistics(
                actual_summary_statistics=hash_summary_statistics,
                processed_files=1,
                processed_bytes=expected_total_input_bytes,
                skipped_files=0,
                skipped_bytes=0,
            )

            assert_progress_report_summary_statistics(
                actual_summary_statistics=upload_summary_statistics,
                processed_files=1,
                processed_bytes=expected_total_input_bytes,
                skipped_files=0,
                skipped_bytes=0,
            )

            s3 = boto3.Session(region_name="us-west-2").resource(
                "s3"
            )  # pylint: disable=invalid-name
            bucket = s3.Bucket(self.job_attachment_s3_settings.s3BucketName)

            assert_expected_files_on_s3(
                bucket,
                expected_files={
                    f"{self.job_attachment_s3_settings.rootPrefix}/Manifests/{farm_id}/{queue_id}/Inputs/0000/b_input",
                    f"{self.job_attachment_s3_settings.full_cas_prefix()}/a.xxh128",
                },
            )

            assert_canonical_manifest(
                bucket,
                f"assetRoot/Manifests/{farm_id}/{queue_id}/Inputs/0000/b_input",
                expected_manifest=expected_manifest,
            )

    @mock_aws
    @pytest.mark.parametrize(
        "num_input_files",
        [
            1,
            100,
            200,
        ],
    )
    @pytest.mark.parametrize(
        "manifest_version",
        [
            ManifestVersion.v2023_03_03,
        ],
    )
    def test_asset_management_many_inputs(
        self,
        tmpdir,
        farm_id,
        queue_id,
        assert_canonical_manifest,
        assert_expected_files_on_s3,
        caplog,
        manifest_version: ManifestVersion,
        num_input_files: int,
    ):
        """
        Test that the correct files get uploaded to S3 and the asset manifest
        is as expected when there are multiple input and output files.
        """
        # Given
        asset_root = str(tmpdir)

        asset_manager = S3AssetManager(
            farm_id=farm_id,
            queue_id=queue_id,
            job_attachment_settings=self.job_attachment_s3_settings,
            asset_manifest_version=manifest_version,
        )

        with patch(
            f"{deadline.__package__}.job_attachments.upload.PathFormat.get_host_path_format",
            return_value=PathFormat.POSIX,
        ), patch(
            f"{deadline.__package__}.job_attachments.upload.hash_data",
            side_effect=["c", "manifesthash"],
        ), patch(
            f"{deadline.__package__}.job_attachments.upload.hash_file",
            side_effect=[str(i) for i in range(num_input_files)],
        ), patch(
            f"{deadline.__package__}.job_attachments.models._generate_random_guid",
            return_value="0000",
        ):
            caplog.set_level(DEBUG)

            mock_on_preparing_to_submit = MagicMock(return_value=True)
            mock_on_uploading_assets = MagicMock(return_value=True)

            input_files = []
            expected_total_input_bytes = 0
            test_dir = tmpdir.mkdir("large_submit")
            for i in range(num_input_files):
                test_file = test_dir.join(f"test{i}.txt")
                test_file.write(f"test {i}")
                expected_total_input_bytes += test_file.size()
                input_files.append(test_file)

            cache_dir = tmpdir.mkdir("cache")

            # When
            upload_group = asset_manager.prepare_paths_for_upload(
                input_paths=input_files,
                output_paths=[str(Path(asset_root).joinpath("outputs"))],
                referenced_paths=[],
            )
            (
                hash_summary_statistics,
                asset_root_manifests,
            ) = asset_manager.hash_assets_and_create_manifest(
                asset_groups=upload_group.asset_groups,
                total_input_files=upload_group.total_input_files,
                total_input_bytes=upload_group.total_input_bytes,
                hash_cache_dir=cache_dir,
                on_preparing_to_submit=mock_on_preparing_to_submit,
            )

            (upload_summary_statistics, attachments) = asset_manager.upload_assets(
                manifests=asset_root_manifests,
                on_uploading_assets=mock_on_uploading_assets,
                s3_check_cache_dir=cache_dir,
            )

            # Then
            expected_attachments = Attachments(
                manifests=[
                    ManifestProperties(
                        rootPath=asset_root,
                        rootPathFormat=PathFormat.POSIX,
                        inputManifestPath=f"{farm_id}/{queue_id}/Inputs/0000/c_input",
                        inputManifestHash="manifesthash",
                        outputRelativeDirectories=["outputs"],
                    )
                ],
            )

            assert attachments == expected_attachments
            assert attachments.to_dict() == {  # type: ignore
                "fileSystem": "COPIED",
                "manifests": [
                    {
                        "rootPath": f"{asset_root}",
                        "rootPathFormat": PathFormat("posix").value,
                        "inputManifestPath": f"{farm_id}/{queue_id}/Inputs/0000/c_input",
                        "inputManifestHash": "manifesthash",
                        "outputRelativeDirectories": ["outputs"],
                    }
                ],
            }

            assert f"assetRoot/Manifests/{farm_id}/{queue_id}/Inputs/0000/c_input" in caplog.text

            assert_progress_report_last_callback(
                num_input_files=num_input_files,
                expected_total_input_bytes=expected_total_input_bytes,
                on_preparing_to_submit=mock_on_preparing_to_submit,
                on_uploading_assets=mock_on_uploading_assets,
            )

            assert_progress_report_summary_statistics(
                actual_summary_statistics=hash_summary_statistics,
                processed_files=num_input_files,
                processed_bytes=expected_total_input_bytes,
                skipped_files=0,
                skipped_bytes=0,
            )

            assert_progress_report_summary_statistics(
                actual_summary_statistics=upload_summary_statistics,
                processed_files=num_input_files,
                processed_bytes=expected_total_input_bytes,
                skipped_files=0,
                skipped_bytes=0,
            )

            s3 = boto3.Session(region_name="us-west-2").resource(
                "s3"
            )  # pylint: disable=invalid-name
            bucket = s3.Bucket(self.job_attachment_s3_settings.s3BucketName)

            expected_files = set(
                [
                    f"{self.job_attachment_s3_settings.full_cas_prefix()}/{i}.xxh128"
                    for i in range(num_input_files)
                ]
            )
            expected_files.add(
                f"assetRoot/Manifests/{farm_id}/{queue_id}/Inputs/0000/c_input",
            )
            assert_expected_files_on_s3(bucket, expected_files=expected_files)

    @mock_aws
    @pytest.mark.parametrize(
        "num_input_files",
        [
            1,
            100,
            200,
        ],
    )
    @pytest.mark.parametrize(
        "manifest_version",
        [
            ManifestVersion.v2023_03_03,
        ],
    )
    def test_asset_management_many_inputs_with_same_hash(
        self,
        tmpdir,
        farm_id,
        queue_id,
        manifest_version: ManifestVersion,
        num_input_files: int,
    ):
        """
        Test that the asset management can handle many input files with the same hash.
        If files with different paths have the same content (and thus the same hash),
        they should be counted as skipped files.
        """
        asset_root = str(tmpdir)

        asset_manager = S3AssetManager(
            farm_id=farm_id,
            queue_id=queue_id,
            job_attachment_settings=self.job_attachment_s3_settings,
            asset_manifest_version=manifest_version,
        )
        # Change the number of thread workers to 1 to get consistent tests
        asset_manager.asset_uploader.num_upload_workers = 1

        # Given
        with patch(
            f"{deadline.__package__}.job_attachments.upload.PathFormat.get_host_path_format",
            return_value=PathFormat.POSIX,
        ), patch(
            f"{deadline.__package__}.job_attachments.upload.hash_data",
            side_effect=["c", "manifesthash"],
        ), patch(
            f"{deadline.__package__}.job_attachments.upload.hash_file",
            side_effect=lambda *args, **kwargs: "samehash",
        ):
            mock_on_preparing_to_submit = MagicMock(return_value=True)
            mock_on_uploading_assets = MagicMock(return_value=True)

            input_files = []
            expected_total_input_bytes = 0
            test_dir = tmpdir.mkdir("large_submit")
            for i in range(num_input_files):
                test_file = test_dir.join(f"test{i}.txt")
                test_file.write("same content")
                expected_total_input_bytes += test_file.size()
                input_files.append(test_file)
            expected_total_downloaded_bytes = test_file.size()

            cache_dir = tmpdir.mkdir("cache")

            # When
            upload_group = asset_manager.prepare_paths_for_upload(
                input_paths=input_files,
                output_paths=[str(Path(asset_root).joinpath("outputs"))],
                referenced_paths=[],
            )
            (
                hash_summary_statistics,
                asset_root_manifests,
            ) = asset_manager.hash_assets_and_create_manifest(
                asset_groups=upload_group.asset_groups,
                total_input_files=upload_group.total_input_files,
                total_input_bytes=upload_group.total_input_bytes,
                hash_cache_dir=cache_dir,
                on_preparing_to_submit=mock_on_preparing_to_submit,
            )

            (upload_summary_statistics, _) = asset_manager.upload_assets(
                manifests=asset_root_manifests,
                on_uploading_assets=mock_on_uploading_assets,
                s3_check_cache_dir=cache_dir,
            )

            # Then
            assert_progress_report_last_callback(
                num_input_files=num_input_files,
                expected_total_input_bytes=expected_total_input_bytes,
                on_preparing_to_submit=mock_on_preparing_to_submit,
                on_uploading_assets=mock_on_uploading_assets,
            )

            assert_progress_report_summary_statistics(
                actual_summary_statistics=hash_summary_statistics,
                processed_files=num_input_files,
                processed_bytes=expected_total_input_bytes,
                skipped_files=0,
                skipped_bytes=0,
            )

            assert_progress_report_summary_statistics(
                actual_summary_statistics=upload_summary_statistics,
                processed_files=1,
                processed_bytes=expected_total_downloaded_bytes,
                skipped_files=num_input_files - 1,
                skipped_bytes=expected_total_input_bytes - expected_total_downloaded_bytes,
            )

    @mock_aws
    @pytest.mark.parametrize(
        "manifest_version",
        [
            ManifestVersion.v2023_03_03,
        ],
    )
    def test_asset_management_no_outputs_inputs_already_uploaded(
        self,
        tmpdir,
        farm_id,
        queue_id,
        assert_expected_files_on_s3,
        caplog,
        manifest_version: ManifestVersion,
    ):
        """
        Test the input files that have already been uploaded to S3 are skipped.
        """
        already_uploaded_file = tmpdir.mkdir("scene").join("maya_scene.ma")
        already_uploaded_file.write("cool scene with lots of spheres")

        not_yet_uploaded_file = tmpdir.mkdir("textures").join("cool_texture.png")
        not_yet_uploaded_file.write("the best texture you've ever seen")

        expected_total_skipped_bytes = already_uploaded_file.size()
        expected_total_uploaded_bytes = not_yet_uploaded_file.size()
        expected_total_input_bytes = expected_total_skipped_bytes + expected_total_uploaded_bytes

        def mock_hash_file(file_path: str, hash_alg: HashAlgorithm):
            if file_path == already_uploaded_file:
                return "existinghash"
            elif file_path == not_yet_uploaded_file:
                return "somethingnew"

        # Given
        with patch(
            f"{deadline.__package__}.job_attachments.upload.PathFormat.get_host_path_format",
            return_value=PathFormat.POSIX,
        ), patch(
            f"{deadline.__package__}.job_attachments.upload.hash_data",
            side_effect=["manifest", "manifesthash"],
        ), patch(
            f"{deadline.__package__}.job_attachments.upload.PathFormat.get_host_path_format",
            return_value=PathFormat.POSIX,
        ), patch(
            f"{deadline.__package__}.job_attachments.upload.hash_file", side_effect=mock_hash_file
        ), patch(
            f"{deadline.__package__}.job_attachments.models._generate_random_guid",
            return_value="0000",
        ):
            s3 = boto3.Session(region_name="us-west-2").resource(
                "s3"
            )  # pylint: disable=invalid-name
            bucket = s3.Bucket(self.job_attachment_s3_settings.s3BucketName)

            caplog.set_level(DEBUG)

            mock_on_preparing_to_submit = MagicMock(return_value=True)
            mock_on_uploading_assets = MagicMock(return_value=True)

            asset_manager = S3AssetManager(
                farm_id=farm_id,
                queue_id=queue_id,
                job_attachment_settings=self.job_attachment_s3_settings,
                asset_manifest_version=manifest_version,
            )

            # mock pre-uploading the file
            bucket.put_object(
                Key=f"{self.job_attachment_s3_settings.full_cas_prefix()}/existinghash.xxh128",
                Body="a",
            )

            cache_dir = tmpdir.mkdir("cache")

            # When
            upload_group = asset_manager.prepare_paths_for_upload(
                input_paths=[already_uploaded_file, not_yet_uploaded_file],
                output_paths=[],
                referenced_paths=[],
            )
            (
                hash_summary_statistics,
                asset_root_manifests,
            ) = asset_manager.hash_assets_and_create_manifest(
                asset_groups=upload_group.asset_groups,
                total_input_files=upload_group.total_input_files,
                total_input_bytes=upload_group.total_input_bytes,
                hash_cache_dir=cache_dir,
                on_preparing_to_submit=mock_on_preparing_to_submit,
            )

            (upload_summary_statistics, _) = asset_manager.upload_assets(
                manifests=asset_root_manifests,
                on_uploading_assets=mock_on_uploading_assets,
                s3_check_cache_dir=cache_dir,
            )

            # Then
            assert "maya_scene.ma because it has already been uploaded to s3" in caplog.text
            assert (
                f"assetRoot/Manifests/{farm_id}/{queue_id}/Inputs/0000/manifest_input"
                in caplog.text
            )

            assert_progress_report_last_callback(
                num_input_files=2,
                expected_total_input_bytes=expected_total_input_bytes,
                on_preparing_to_submit=mock_on_preparing_to_submit,
                on_uploading_assets=mock_on_uploading_assets,
            )

            assert_progress_report_summary_statistics(
                actual_summary_statistics=hash_summary_statistics,
                processed_files=2,
                processed_bytes=expected_total_input_bytes,
                skipped_files=0,
                skipped_bytes=0,
            )

            assert_progress_report_summary_statistics(
                actual_summary_statistics=upload_summary_statistics,
                processed_files=1,
                processed_bytes=expected_total_uploaded_bytes,
                skipped_files=1,
                skipped_bytes=expected_total_skipped_bytes,
            )

            assert_expected_files_on_s3(
                bucket,
                expected_files={
                    f"{self.job_attachment_s3_settings.rootPrefix}/Manifests/{farm_id}/{queue_id}/Inputs/0000/manifest_input",
                    f"{self.job_attachment_s3_settings.full_cas_prefix()}/existinghash.xxh128",
                    f"{self.job_attachment_s3_settings.full_cas_prefix()}/somethingnew.xxh128",
                },
            )

    @mock_aws
    @pytest.mark.parametrize(
        "num_input_files",
        [
            1,
            100,
            200,
        ],
    )
    @pytest.mark.parametrize(
        "manifest_version",
        [
            ManifestVersion.v2023_03_03,
        ],
    )
    def test_asset_management_no_outputs_large_number_of_inputs_already_uploaded(
        self,
        tmpdir,
        farm_id,
        queue_id,
        assert_expected_files_on_s3,
        caplog,
        manifest_version: ManifestVersion,
        num_input_files: int,
    ):
        """
        Test the input files that have already been uploaded to S3 are skipped.
        """
        # Given
        asset_manager = S3AssetManager(
            farm_id=farm_id,
            queue_id=queue_id,
            job_attachment_settings=self.job_attachment_s3_settings,
            asset_manifest_version=manifest_version,
        )

        with patch(
            f"{deadline.__package__}.job_attachments.upload.PathFormat.get_host_path_format",
            return_value=PathFormat.POSIX,
        ), patch(
            f"{deadline.__package__}.job_attachments.upload.hash_data",
            side_effect=["manifesto", "manifesthash"],
        ), patch(
            f"{deadline.__package__}.job_attachments.upload.PathFormat.get_host_path_format",
            return_value=PathFormat.POSIX,
        ), patch(
            f"{deadline.__package__}.job_attachments.upload.hash_file",
            side_effect=[str(i) for i in range(num_input_files)],
        ), patch(
            f"{deadline.__package__}.job_attachments.models._generate_random_guid",
            return_value="0000",
        ):
            s3 = boto3.Session(region_name="us-west-2").resource(
                "s3"
            )  # pylint: disable=invalid-name
            bucket = s3.Bucket(self.job_attachment_s3_settings.s3BucketName)

            caplog.set_level(DEBUG)

            mock_on_preparing_to_submit = MagicMock(return_value=True)
            mock_on_uploading_assets = MagicMock(return_value=True)

            input_files = []
            expected_total_input_bytes = 0
            test_dir = tmpdir.mkdir("large_submit")
            for i in range(num_input_files):
                test_file = test_dir.join(f"test{i}.txt")
                test_file.write(f"test {i}")
                expected_total_input_bytes += test_file.size()
                input_files.append(test_file)
                # mock pre-uploading the file
                bucket.put_object(
                    Key=f"{self.job_attachment_s3_settings.full_cas_prefix()}/{i}.xxh128",
                    Body=f"test {i}",
                )

            not_yet_uploaded_file = tmpdir.mkdir("textures").join("texture.png")
            not_yet_uploaded_file.write("b")

            cache_dir = tmpdir.mkdir("cache")

            # When
            upload_group = asset_manager.prepare_paths_for_upload(
                input_paths=input_files,
                output_paths=[],
                referenced_paths=[],
            )
            (
                hash_summary_statistics,
                asset_root_manifests,
            ) = asset_manager.hash_assets_and_create_manifest(
                asset_groups=upload_group.asset_groups,
                total_input_files=upload_group.total_input_files,
                total_input_bytes=upload_group.total_input_bytes,
                hash_cache_dir=cache_dir,
                on_preparing_to_submit=mock_on_preparing_to_submit,
            )

            (upload_summary_statistics, _) = asset_manager.upload_assets(
                manifests=asset_root_manifests,
                on_uploading_assets=mock_on_uploading_assets,
                s3_check_cache_dir=cache_dir,
            )

            # Then
            assert (
                f"assetRoot/Manifests/{farm_id}/{queue_id}/Inputs/0000/manifesto_input"
                in caplog.text
            )

            assert_progress_report_last_callback(
                num_input_files=num_input_files,
                expected_total_input_bytes=expected_total_input_bytes,
                on_preparing_to_submit=mock_on_preparing_to_submit,
                on_uploading_assets=mock_on_uploading_assets,
            )

            assert_progress_report_summary_statistics(
                actual_summary_statistics=hash_summary_statistics,
                processed_files=num_input_files,
                processed_bytes=expected_total_input_bytes,
                skipped_files=0,
                skipped_bytes=0,
            )

            assert_progress_report_summary_statistics(
                actual_summary_statistics=upload_summary_statistics,
                processed_files=0,
                processed_bytes=0,
                skipped_files=num_input_files,
                skipped_bytes=expected_total_input_bytes,
            )

            expected_files = set(
                [
                    f"{self.job_attachment_s3_settings.full_cas_prefix()}/{i}.xxh128"
                    for i in range(num_input_files)
                ]
            )
            expected_files.add(
                f"assetRoot/Manifests/{farm_id}/{queue_id}/Inputs/0000/manifesto_input",
            )
            assert_expected_files_on_s3(bucket, expected_files=expected_files)

    @mock_aws
    @pytest.mark.parametrize(
        "manifest_version",
        [
            ManifestVersion.v2023_03_03,
        ],
    )
    def test_asset_management_no_inputs(
        self,
        tmpdir,
        farm_id,
        queue_id,
        assert_canonical_manifest,
        assert_expected_files_on_s3,
        caplog,
        manifest_version: ManifestVersion,
    ):
        """
        Test that only the manifest file gets uploaded to S3 and the asset manifest is as expected
        when there are no input files and multiple output files.
        """
        output_dir = str(tmpdir.join("outputs"))

        # Given
        with patch(
            f"{deadline.__package__}.job_attachments.upload.PathFormat.get_host_path_format",
            return_value=PathFormat.POSIX,
        ), patch(
            f"{deadline.__package__}.job_attachments.upload.hash_data",
            side_effect=["a", "manifesthash"],
        ), patch(
            f"{deadline.__package__}.job_attachments.models._generate_random_guid",
            return_value="0000",
        ):
            mock_on_preparing_to_submit = MagicMock(return_value=True)
            mock_on_uploading_assets = MagicMock(return_value=True)

            asset_manager = S3AssetManager(
                farm_id=farm_id,
                queue_id=queue_id,
                job_attachment_settings=self.job_attachment_s3_settings,
                asset_manifest_version=manifest_version,
            )

            cache_dir = tmpdir.mkdir("cache")

            # When
            upload_group = asset_manager.prepare_paths_for_upload(
                input_paths=[],
                output_paths=[output_dir],
                referenced_paths=[],
            )
            (
                hash_summary_statistics,
                asset_root_manifests,
            ) = asset_manager.hash_assets_and_create_manifest(
                asset_groups=upload_group.asset_groups,
                total_input_files=upload_group.total_input_files,
                total_input_bytes=upload_group.total_input_bytes,
                hash_cache_dir=cache_dir,
                on_preparing_to_submit=mock_on_preparing_to_submit,
            )

            (upload_summary_statistics, attachments) = asset_manager.upload_assets(
                manifests=asset_root_manifests,
                on_uploading_assets=mock_on_uploading_assets,
                s3_check_cache_dir=cache_dir,
            )

            # Then
            expected_attachments = Attachments(
                manifests=[
                    ManifestProperties(
                        rootPath=output_dir,
                        rootPathFormat=PathFormat.POSIX,
                        outputRelativeDirectories=["."],
                    )
                ],
            )

            assert attachments == expected_attachments
            assert attachments.to_dict() == {  # type: ignore
                "fileSystem": "COPIED",
                "manifests": [
                    {
                        "rootPath": f"{output_dir}",
                        "rootPathFormat": PathFormat("posix").value,
                        "outputRelativeDirectories": ["."],
                    }
                ],
            }

            assert_progress_report_summary_statistics(
                actual_summary_statistics=hash_summary_statistics,
                processed_files=0,
                processed_bytes=0,
                skipped_files=0,
                skipped_bytes=0,
            )

            assert_progress_report_summary_statistics(
                actual_summary_statistics=upload_summary_statistics,
                processed_files=0,
                processed_bytes=0,
                skipped_files=0,
                skipped_bytes=0,
            )

    @pytest.mark.parametrize(
        "manifest_version",
        [
            ManifestVersion.v2023_03_03,
        ],
    )
    def test_asset_management_no_s3_bucket_set(
        self,
        farm_id,
        queue_id,
        manifest_version: ManifestVersion,
    ):
        """
        Test that the appropriate error is raised when no s3 bucket is provided.
        """
        missing_s3_job_attachment_settings = deepcopy(self.job_attachment_s3_settings)

        del missing_s3_job_attachment_settings.s3BucketName

        with pytest.raises(AttributeError):
            S3AssetManager(
                farm_id=farm_id,
                queue_id=queue_id,
                job_attachment_settings=missing_s3_job_attachment_settings,
                asset_manifest_version=manifest_version,
            )

    @pytest.mark.parametrize(
        "manifest_version",
        [
            ManifestVersion.v2023_03_03,
        ],
    )
    def test_asset_management_with_s3_bucket_empty(
        self,
        farm_id,
        queue_id,
        manifest_version: ManifestVersion,
    ):
        """
        Test that the appropriate error is raised when no s3 bucket is provided.
        """
        s3_job_attachment_settings_with_s3_bucket_empty = deepcopy(self.job_attachment_s3_settings)
        s3_job_attachment_settings_with_s3_bucket_empty.s3BucketName = ""

        with pytest.raises(MissingS3BucketError):
            S3AssetManager(
                farm_id=farm_id,
                queue_id=queue_id,
                job_attachment_settings=s3_job_attachment_settings_with_s3_bucket_empty,
                asset_manifest_version=manifest_version,
            )

    @pytest.mark.parametrize(
        "manifest_version",
        [
            ManifestVersion.v2023_03_03,
        ],
    )
    def test_asset_management_no_s3_root_prefix_set(
        self,
        farm_id,
        queue_id,
        manifest_version: ManifestVersion,
    ):
        """
        Test that the appropriate error is raised when no s3 root prefix is provided.
        """
        missing_s3_job_attachment_settings = deepcopy(self.job_attachment_s3_settings)

        del missing_s3_job_attachment_settings.rootPrefix

        with pytest.raises(AttributeError):
            S3AssetManager(
                farm_id=farm_id,
                queue_id=queue_id,
                job_attachment_settings=missing_s3_job_attachment_settings,
                asset_manifest_version=manifest_version,
            )

    @pytest.mark.parametrize(
        "manifest_version",
        [
            ManifestVersion.v2023_03_03,
        ],
    )
    def test_asset_management_with_root_prefix_empty(
        self,
        farm_id,
        queue_id,
        manifest_version: ManifestVersion,
    ):
        """
        Test that the appropriate error is raised when no s3 bucket is provided.
        """
        s3_job_attachment_settings_with_root_prefix_empty = deepcopy(
            self.job_attachment_s3_settings
        )
        s3_job_attachment_settings_with_root_prefix_empty.rootPrefix = ""

        with pytest.raises(MissingS3RootPrefixError):
            S3AssetManager(
                farm_id=farm_id,
                queue_id=queue_id,
                job_attachment_settings=s3_job_attachment_settings_with_root_prefix_empty,
                asset_manifest_version=manifest_version,
            )

    def test_asset_management_manifest_version_not_implemented(self, farm_id, queue_id, tmpdir):
        """
        Test that the appropriate error is raised when the library doesn't support an asset manifest version.
        """
        with patch(
            f"{deadline.__package__}.job_attachments.upload.ManifestModelRegistry.get_manifest_model",
            return_value=BaseManifestModel,
        ):
            with pytest.raises(
                NotImplementedError,
                match=r"Creation of manifest version (ManifestVersion.)?UNDEFINED is not supported.",
            ):
                asset_manager = S3AssetManager(
                    farm_id=farm_id,
                    queue_id=queue_id,
                    job_attachment_settings=self.job_attachment_s3_settings,
                    asset_manifest_version=ManifestVersion.UNDEFINED,
                )
                cache_dir = tmpdir.mkdir("cache")
                test_file = tmpdir.join("test.txt")
                test_file.write("test")
                upload_group = asset_manager.prepare_paths_for_upload(
                    input_paths=[test_file],
                    output_paths=[],
                    referenced_paths=[],
                )
                asset_manager.hash_assets_and_create_manifest(
                    upload_group.asset_groups,
                    upload_group.total_input_files,
                    upload_group.total_input_bytes,
                    hash_cache_dir=cache_dir,
                )

    def test_asset_uploader_constructor(self, fresh_deadline_config):
        """
        Test that when the asset uploader is created, the instance variables are correctly set.
        """
        uploader = S3AssetUploader()
        assert uploader.num_upload_workers == 5
        assert uploader.small_file_threshold == 20 * 8 * (1024**2)

    def test_asset_uploader_constructor_with_non_integer_config_settings(
        self, fresh_deadline_config
    ):
        """
        Tests that when the asset uploader is created with non-integer config settings, an AssetSyncError is raised.
        """
        config.set_setting("settings.s3_max_pool_connections", "!@#$")
        with pytest.raises(AssetSyncError) as err:
            _ = S3AssetUploader()
        assert isinstance(err.value.__cause__, ValueError)
        assert "Failed to parse configuration settings." in str(err.value)

    @pytest.mark.parametrize(
        "setting_name, nonvalid_value, expected_error_msg",
        [
            pytest.param(
                "s3_max_pool_connections",
                "-100",
                "'s3_max_pool_connections' (-100) must be positive integer.",
                id="s3_max_pool_connections value is negative.",
            ),
            pytest.param(
                "s3_max_pool_connections",
                "0",
                "'s3_max_pool_connections' (0) must be positive integer.",
                id="s3_max_pool_connections value is 0.",
            ),
            pytest.param(
                "s3_max_pool_connections",
                "some string",
                "Failed to parse configuration settings. Please ensure that the following settings in the config file are integers",
                id="s3_max_pool_connections value is not a number.",
            ),
            pytest.param(
                "small_file_threshold_multiplier",
                "-12",
                "'small_file_threshold_multiplier' (-12) must be positive integer.",
                id="small_file_threshold_multiplier value is negative.",
            ),
            pytest.param(
                "small_file_threshold_multiplier",
                "some string",
                "Failed to parse configuration settings. Please ensure that the following settings in the config file are integers",
                id="small_file_threshold_multiplier value is not a number.",
            ),
        ],
    )
    def test_asset_uploader_constructor_with_nonvalid_config_settings(
        self, setting_name, nonvalid_value, expected_error_msg, fresh_deadline_config
    ):
        """
        Tests that when the asset uploader is created with nonvalid config settings, an AssetSyncError is raised.
        """
        config.set_setting(f"settings.{setting_name}", nonvalid_value)
        with pytest.raises(AssetSyncError) as err:
            _ = S3AssetUploader()
        assert expected_error_msg in str(err.value)

    @mock_aws
    def test_file_already_uploaded_bucket_in_different_account(self):
        """
        Test that the appropriate error is raised when checking if a file has already been uploaded, but the bucket
        is in an account that is different from the uploader's account.
        """
        s3 = boto3.client("s3")
        stubber = Stubber(s3)
        stubber.add_client_error(
            "head_object",
            service_error_code="AccessDenied",
            service_message="Access Denied",
            http_status_code=403,
        )

        uploader = S3AssetUploader()

        uploader._s3 = s3

        with stubber:
            with pytest.raises(JobAttachmentsS3ClientError) as err:
                uploader.file_already_uploaded(
                    self.job_attachment_s3_settings.s3BucketName, "test_key"
                )
            assert isinstance(err.value.__cause__, ClientError)
            assert (
                err.value.__cause__.response["ResponseMetadata"]["HTTPStatusCode"] == 403  # type: ignore[attr-defined]
            )
            assert (
                "Error checking if object exists in bucket 'test-bucket', Target key or prefix: 'test_key', "
                "HTTP Status Code: 403, Access denied. Ensure that the bucket is in the account 123456789012, "
                "and your AWS IAM Role or User has the 's3:ListBucket' permission for this bucket."
            ) in str(err.value)

    @mock_aws
    def test_file_already_uploaded_timeout(self):
        """
        Test that the appropriate error is raised when a ReadTimeoutError occurs
        during an S3 request to check file existence in an S3 bucket.
        """
        mock_s3_client = MagicMock()
        mock_s3_client.head_object.side_effect = ReadTimeoutError(endpoint_url="test_url")

        uploader = S3AssetUploader()
        uploader._s3 = mock_s3_client

        with pytest.raises(AssetSyncError) as err:
            uploader.file_already_uploaded(self.job_attachment_s3_settings.s3BucketName, "test_key")
        assert isinstance(err.value.__cause__, BotoCoreError)
        assert (
            "An issue occurred with AWS service request while checking for the existence of an object in the S3 bucket: "
            'Read timeout on endpoint URL: "test_url"\n'
            "This could be due to temporary issues with AWS, internet connection, or your AWS credentials. "
            "Please verify your credentials and network connection. If the problem persists, try again later"
            " or contact support for further assistance."
        ) in str(err.value)

    @mock_aws
    def test_upload_bytes_to_s3_bucket_in_different_account(self):
        """
        Test that the appropriate error is raised when uploading bytes, but the bucket
        is in an account that is different from the uploader's account.
        """
        s3 = boto3.client("s3")
        stubber = Stubber(s3)

        # This is the error that's surfaced when a bucket is in a different account than expected.
        stubber.add_client_error(
            "put_object",
            service_error_code="AccessDenied",
            service_message="Access Denied",
            http_status_code=403,
        )

        uploader = S3AssetUploader()

        uploader._s3 = s3

        with stubber:
            with pytest.raises(JobAttachmentsS3ClientError) as err:
                uploader.upload_bytes_to_s3(
                    BytesIO(), self.job_attachment_s3_settings.s3BucketName, "test_key"
                )
            assert isinstance(err.value.__cause__, ClientError)
            assert (
                err.value.__cause__.response["ResponseMetadata"]["HTTPStatusCode"] == 403  # type: ignore[attr-defined]
            )
            assert (
                "Error uploading binary file in bucket 'test-bucket', Target key or prefix: 'test_key', "
                "HTTP Status Code: 403, Forbidden or Access denied. "
            ) in str(err.value)

    @mock_aws
    def test_upload_bytes_to_s3_timeout(self):
        """
        Test that the appropriate error is raised when a ReadTimeoutError occurs
        during an S3 request to upload a binary file to an S3 bucket.
        """
        mock_s3_client = MagicMock()
        mock_s3_client.upload_fileobj.side_effect = ReadTimeoutError(endpoint_url="test_url")

        uploader = S3AssetUploader()
        uploader._s3 = mock_s3_client

        with pytest.raises(AssetSyncError) as err:
            uploader.upload_bytes_to_s3(
                BytesIO(), self.job_attachment_s3_settings.s3BucketName, "test_key"
            )
        assert isinstance(err.value.__cause__, BotoCoreError)
        assert (
            "An issue occurred with AWS service request while uploading binary file: "
            'Read timeout on endpoint URL: "test_url"\n'
            "This could be due to temporary issues with AWS, internet connection, or your AWS credentials. "
            "Please verify your credentials and network connection. If the problem persists, try again later"
            " or contact support for further assistance."
        ) in str(err.value)

    @mock_aws
    def test_upload_file_to_s3_bucket_in_different_account(self, tmp_path: Path):
        """
        Test that the appropriate error is raised when uploading files, but the bucket
        is in an account that is different from the uploader's account.
        """
        s3 = boto3.client("s3")
        stubber = Stubber(s3)

        # This is the error that's surfaced when a bucket is in a different account than expected.
        stubber.add_client_error(
            "put_object",
            service_error_code="AccessDenied",
            service_message="Access Denied",
            http_status_code=403,
        )

        uploader = S3AssetUploader()

        uploader._s3 = s3

        file = tmp_path / "test_file"
        file.write_text("")

        with stubber:
            with pytest.raises(JobAttachmentsS3ClientError) as err:
                uploader.upload_file_to_s3(
                    file, self.job_attachment_s3_settings.s3BucketName, "test_key"
                )
            assert isinstance(err.value.__cause__, ClientError)
            assert (
                err.value.__cause__.response["ResponseMetadata"]["HTTPStatusCode"] == 403  # type: ignore[attr-defined]
            )
            assert (
                "Error uploading file in bucket 'test-bucket', Target key or prefix: 'test_key', "
                "HTTP Status Code: 403, Forbidden or Access denied. "
            ) in str(err.value)
            assert (f"(Failed to upload {str(file)})") in str(err.value)

    @mock_aws
    def test_upload_file_to_s3_bucket_has_kms_permissions_error(self, tmp_path: Path):
        """
        Test that the appropriate error is raised when uploading files, but the bucket
        is encrypted with a KMS key and the user doesn't have access to the key.
        """
        s3 = boto3.client("s3")
        stubber = Stubber(s3)

        # This is the error that's surfaced when a bucket is in a different account than expected.
        stubber.add_client_error(
            "put_object",
            service_error_code="AccessDenied",
            service_message="An error occurred (AccessDenied) when calling the PutObject operation: User: arn:aws:sts::<account>:assumed-role/<role> is not authorized to perform: kms:GenerateDataKey on resource: arn:aws:kms:us-west-2:<account>:key/<key-id> because no identity-based policy allows the kms:GenerateDataKey action",
            http_status_code=403,
        )

        uploader = S3AssetUploader()

        uploader._s3 = s3

        file = tmp_path / "test_file"
        file.write_text("")

        with stubber:
            with pytest.raises(JobAttachmentsS3ClientError) as err:
                uploader.upload_file_to_s3(
                    file, self.job_attachment_s3_settings.s3BucketName, "test_key"
                )
            assert isinstance(err.value.__cause__, ClientError)
            assert (
                err.value.__cause__.response["ResponseMetadata"]["HTTPStatusCode"] == 403  # type: ignore[attr-defined]
            )
            assert (
                "If a customer-managed KMS key is set, confirm that your AWS IAM Role or "
                "User has the 'kms:GenerateDataKey' and 'kms:DescribeKey' permissions for the key used to encrypt the bucket."
            ) in str(err.value)
            assert (f"(Failed to upload {str(file)})") in str(err.value)

    @mock_aws
    def test_upload_file_to_s3_timeout(self, tmp_path: Path):
        """
        Test that the appropriate error is raised when a ReadTimeoutError occurs
        during an S3 request to upload a file to an S3 bucket.
        """
        mock_future = MagicMock()
        mock_transfer_manager = MagicMock()
        mock_transfer_manager.upload.return_value = mock_future
        mock_future.result.side_effect = ReadTimeoutError(endpoint_url="test_url")

        s3 = boto3.client("s3")
        uploader = S3AssetUploader()
        uploader._s3 = s3

        file = tmp_path / "test_file"
        file.write_text("")

        with patch(
            f"{deadline.__package__}.job_attachments.upload.get_s3_transfer_manager",
            return_value=mock_transfer_manager,
        ):
            with pytest.raises(AssetSyncError) as err:
                uploader.upload_file_to_s3(
                    file, self.job_attachment_s3_settings.s3BucketName, "test_key"
                )
            assert isinstance(err.value.__cause__, BotoCoreError)
            assert (
                "An issue occurred with AWS service request while uploading file: "
                'Read timeout on endpoint URL: "test_url"\n'
                "This could be due to temporary issues with AWS, internet connection, or your AWS credentials. "
                "Please verify your credentials and network connection. If the problem persists, try again later"
                " or contact support for further assistance."
            ) in str(err.value)

    @pytest.mark.parametrize(
        "manifest_version",
        [
            ManifestVersion.v2023_03_03,
        ],
    )
    def test_process_input_path_cached_file_is_updated(
        self, farm_id, queue_id, tmpdir, manifest_version: ManifestVersion
    ):
        """
        Test that a file that exists in the hash cache, but has been modified, will be hashed again.
        """
        # GIVEN
        root_dir = tmpdir.mkdir("root")
        test_file = root_dir.join("test.txt")
        test_file.write("test")
        file_time = os.stat(test_file).st_mtime
        expected_entry = HashCacheEntry(
            test_file, HashAlgorithm.XXH128, "b", str(datetime.fromtimestamp((file_time)))
        )

        # WHEN
        test_entry = HashCacheEntry(test_file, HashAlgorithm.XXH128, "a", "123.45")
        hash_cache = MagicMock()
        hash_cache.get_entry.return_value = test_entry

        with patch(f"{deadline.__package__}.job_attachments.upload.hash_file", side_effect=["b"]):
            asset_manager = S3AssetManager(
                farm_id=farm_id,
                queue_id=queue_id,
                job_attachment_settings=self.job_attachment_s3_settings,
                asset_manifest_version=manifest_version,
            )

            (is_hashed, _, man_path) = asset_manager._process_input_path(
                Path(test_file), root_dir, hash_cache
            )

            # THEN
            assert is_hashed == FileStatus.NEW or is_hashed == FileStatus.MODIFIED
            assert man_path.path == "test.txt"
            assert man_path.hash == "b"
            hash_cache.put_entry.assert_called_with(expected_entry)

    def test_process_input_path_skip_file_already_in_hash_cache(self, farm_id, queue_id, tmpdir):
        """
        Test the input files that already exists in the hash cache are skipped hashing.
        """
        # GIVEN
        root_dir = tmpdir.mkdir("root")
        test_file = root_dir.join("test.txt")
        test_file.write("test")
        file_time = str(datetime.fromtimestamp(os.stat(test_file).st_mtime))
        file_bytes = test_file.size()

        # WHEN
        test_entry = HashCacheEntry(test_file, HashAlgorithm.XXH128, "a", file_time)
        hash_cache = MagicMock()
        hash_cache.get_entry.return_value = test_entry

        with patch(f"{deadline.__package__}.job_attachments.upload.hash_file", side_effect=["a"]):
            asset_manager = S3AssetManager(
                farm_id=farm_id,
                queue_id=queue_id,
                job_attachment_settings=self.job_attachment_s3_settings,
                asset_manifest_version=ManifestVersion.v2023_03_03,
            )

            (is_hashed, size, man_path) = asset_manager._process_input_path(
                Path(test_file), root_dir, hash_cache
            )
            _ = asset_manager._create_manifest_file(
                [Path(test_file)], root_dir, hash_cache=hash_cache
            )

            # THEN
            assert is_hashed == FileStatus.UNCHANGED
            assert size == file_bytes
            assert man_path.path == "test.txt"
            assert man_path.hash == "a"
            hash_cache.put_entry.assert_not_called()

    @mock_aws
    def test_asset_management_misconfigured_inputs(self, farm_id, queue_id, tmpdir):
        """
        Ensure that when directories are classified as files the submission is prevented with a MisconfiguredInputsError.
        """
        asset_root = str(tmpdir)

        # GIVEN
        scene_file = tmpdir.mkdir("scene").join("maya.ma")
        scene_file.write("a")
        input_not_exist = "/texture/that/doesnt/exist.anywhere"
        directory_as_file = str(Path(scene_file).parent)

        with patch(
            f"{deadline.__package__}.job_attachments.upload.PathFormat.get_host_path_format",
            return_value=PathFormat.POSIX,
        ), patch(
            f"{deadline.__package__}.job_attachments.upload.hash_data",
            side_effect=["c", "manifesthash"],
        ), patch(
            f"{deadline.__package__}.job_attachments.upload.hash_file", side_effect=["a"]
        ):
            asset_manager = S3AssetManager(
                farm_id=farm_id,
                queue_id=queue_id,
                job_attachment_settings=self.job_attachment_s3_settings,
                asset_manifest_version=ManifestVersion.v2023_03_03,
            )

            # WHEN / THEN
            with pytest.raises(MisconfiguredInputsError, match="scene"):
                asset_manager.prepare_paths_for_upload(
                    input_paths=[input_not_exist, directory_as_file, scene_file],
                    output_paths=[str(Path(asset_root).joinpath("outputs"))],
                    referenced_paths=[],
                )

    @mock_aws
    def test_asset_management_input_not_exists(self, farm_id, queue_id, tmpdir, caplog):
        """Test that input paths that do not exist are added to referenced files."""
        asset_root = str(tmpdir)

        # GIVEN
        scene_file = tmpdir.mkdir("scene").join("maya.ma")
        scene_file.write("a")
        input_not_exist = tmpdir.join("/texture/that/does/notexist.anywhere")

        cache_dir = tmpdir.mkdir("cache")

        expected_total_input_bytes = scene_file.size()

        with patch(
            f"{deadline.__package__}.job_attachments.upload.PathFormat.get_host_path_format",
            return_value=PathFormat.POSIX,
        ), patch(
            f"{deadline.__package__}.job_attachments.upload.hash_data",
            side_effect=["c", "manifesthash"],
        ), patch(
            f"{deadline.__package__}.job_attachments.upload.hash_file", side_effect=["a"]
        ):
            caplog.set_level(INFO)

            mock_on_preparing_to_submit = MagicMock(return_value=True)
            mock_on_uploading_assets = MagicMock(return_value=True)

            asset_manager = S3AssetManager(
                farm_id=farm_id,
                queue_id=queue_id,
                job_attachment_settings=self.job_attachment_s3_settings,
                asset_manifest_version=ManifestVersion.v2023_03_03,
            )

            # When
            upload_group = asset_manager.prepare_paths_for_upload(
                input_paths=[input_not_exist, scene_file],
                output_paths=[str(Path(asset_root).joinpath("outputs"))],
                referenced_paths=[],
            )
            (
                hash_summary_statistics,
                asset_root_manifests,
            ) = asset_manager.hash_assets_and_create_manifest(
                asset_groups=upload_group.asset_groups,
                total_input_files=upload_group.total_input_files,
                total_input_bytes=upload_group.total_input_bytes,
                hash_cache_dir=cache_dir,
                on_preparing_to_submit=mock_on_preparing_to_submit,
            )

            (upload_summary_statistics, _) = asset_manager.upload_assets(
                manifests=asset_root_manifests,
                on_uploading_assets=mock_on_uploading_assets,
                s3_check_cache_dir=cache_dir,
            )

            # Then
            assert "notexist.anywhere' does not exist. Adding to referenced paths." in caplog.text
            assert len(upload_group.asset_groups) == 1
            assert len(upload_group.asset_groups[0].references) == 1
            assert Path(input_not_exist) in upload_group.asset_groups[0].references

            assert_progress_report_last_callback(
                num_input_files=1,
                expected_total_input_bytes=expected_total_input_bytes,
                on_preparing_to_submit=mock_on_preparing_to_submit,
                on_uploading_assets=mock_on_uploading_assets,
            )

            assert_progress_report_summary_statistics(
                actual_summary_statistics=hash_summary_statistics,
                processed_files=1,
                processed_bytes=expected_total_input_bytes,
                skipped_files=0,
                skipped_bytes=0,
            )

            assert_progress_report_summary_statistics(
                actual_summary_statistics=upload_summary_statistics,
                processed_files=1,
                processed_bytes=expected_total_input_bytes,
                skipped_files=0,
                skipped_bytes=0,
            )

    @mock_aws
    def test_asset_management_input_not_exists_require_fails(self, farm_id, queue_id, tmpdir):
        """Test that input paths that do not exist raise a MisconfiguredInputsError if the `require_paths_exist` flag is true."""
        asset_root = str(tmpdir)

        # GIVEN
        scene_file = tmpdir.mkdir("scene").join("maya.ma")
        scene_file.write("a")
        input_not_exist = "/texture/that/does/notexist.anywhere"

        with patch(
            f"{deadline.__package__}.job_attachments.upload.PathFormat.get_host_path_format",
            return_value=PathFormat.POSIX,
        ), patch(
            f"{deadline.__package__}.job_attachments.upload.hash_data",
            side_effect=["c", "manifesthash"],
        ), patch(
            f"{deadline.__package__}.job_attachments.upload.hash_file", side_effect=["a"]
        ):
            asset_manager = S3AssetManager(
                farm_id=farm_id,
                queue_id=queue_id,
                job_attachment_settings=self.job_attachment_s3_settings,
                asset_manifest_version=ManifestVersion.v2023_03_03,
            )

            # When
            with pytest.raises(MisconfiguredInputsError, match="Missing input files") as execinfo:
                asset_manager.prepare_paths_for_upload(
                    input_paths=[input_not_exist, scene_file],
                    output_paths=[str(Path(asset_root).joinpath("outputs"))],
                    referenced_paths=[],
                    require_paths_exist=True,
                )

            assert "notexist.anywhere" in str(execinfo)

    @mock_aws
    @pytest.mark.parametrize(
        "manifest_version,expected_manifest",
        [
            (
                ManifestVersion.v2023_03_03,
                '{"hashAlg":"xxh128","manifestVersion":"2023-03-03",'
                '"paths":[{"hash":"a","mtime":1234000000,"path":"sym_ip_test.txt","size":1}],"totalSize":1}',
            ),
        ],
    )
    @pytest.mark.skipif(
        is_windows_non_admin(),
        reason="Windows requires Admin to create symlinks, skipping this test.",
    )
    def test_manage_assets_with_symlinks(
        self,
        tmpdir: py.path.local,
        farm_id,
        queue_id,
        assert_canonical_manifest,
        assert_expected_files_on_s3,
        manifest_version: ManifestVersion,
        expected_manifest: str,
    ):
        """
        Test that symlink paths that contain '..' expand the full path without
        resolving the symlink target, but also hash the symlink target and not
        the link.

        /tmp/source_folder/test.txt
            /symlink-folder
        """
        # Given
        test_file = tmpdir.mkdir("source_folder").join("test.txt")
        test_file.write("a")

        expected_total_input_bytes = test_file.size()
        os.utime(test_file, (1234, 1234))

        source_path = Path(tmpdir.join("source_folder"))
        symlink_input_path = Path(
            tmpdir.mkdir("symlink_folder").join("sub_folder").join("..").join("sym_ip_test.txt")
        )
        symlink_input_path.symlink_to(str(test_file))
        symlink_output_path = Path(
            tmpdir.join("symlink_folder").join("sub_folder").join("..").join("sym_op_test_dir")
        )
        symlink_output_path.symlink_to(source_path, target_is_directory=True)

        cache_dir = tmpdir.mkdir("cache")

        # WHEN
        with patch(
            f"{deadline.__package__}.job_attachments.upload.PathFormat.get_host_path_format",
            return_value=PathFormat.POSIX,
        ), patch(
            f"{deadline.__package__}.job_attachments.upload.hash_data",
            side_effect=["manifest", "manifesthash"],
        ), patch(
            f"{deadline.__package__}.job_attachments.upload.hash_file", side_effect=["a"]
        ), patch(
            f"{deadline.__package__}.job_attachments.models._generate_random_guid",
            return_value="0000",
        ):
            mock_on_preparing_to_submit = MagicMock(return_value=True)
            mock_on_uploading_assets = MagicMock(return_value=True)

            asset_manager = S3AssetManager(
                farm_id=farm_id,
                queue_id=queue_id,
                job_attachment_settings=self.job_attachment_s3_settings,
                asset_manifest_version=manifest_version,
            )

            upload_group = asset_manager.prepare_paths_for_upload(
                input_paths=[str(symlink_input_path)],
                output_paths=[str(symlink_output_path)],
                referenced_paths=[],
            )
            (
                hash_summary_statistics,
                asset_root_manifests,
            ) = asset_manager.hash_assets_and_create_manifest(
                asset_groups=upload_group.asset_groups,
                total_input_files=upload_group.total_input_files,
                total_input_bytes=upload_group.total_input_bytes,
                hash_cache_dir=str(cache_dir),
                on_preparing_to_submit=mock_on_preparing_to_submit,
            )

            (upload_summary_statistics, attachments) = asset_manager.upload_assets(
                manifests=asset_root_manifests,
                on_uploading_assets=mock_on_uploading_assets,
                s3_check_cache_dir=str(cache_dir),
            )

            # THEN
            expected_root = str(tmpdir.join("symlink_folder"))
            expected_attachments = Attachments(
                manifests=[
                    ManifestProperties(
                        rootPath=expected_root,
                        rootPathFormat=PathFormat.POSIX,
                        inputManifestPath=f"{farm_id}/{queue_id}/Inputs/0000/manifest_input",
                        inputManifestHash="manifesthash",
                        outputRelativeDirectories=["sym_op_test_dir"],
                    )
                ],
            )

            assert attachments == expected_attachments

            assert_progress_report_last_callback(
                num_input_files=1,
                expected_total_input_bytes=expected_total_input_bytes,
                on_preparing_to_submit=mock_on_preparing_to_submit,
                on_uploading_assets=mock_on_uploading_assets,
            )

            assert_progress_report_summary_statistics(
                actual_summary_statistics=hash_summary_statistics,
                processed_files=1,
                processed_bytes=expected_total_input_bytes,
                skipped_files=0,
                skipped_bytes=0,
            )

            assert_progress_report_summary_statistics(
                actual_summary_statistics=upload_summary_statistics,
                processed_files=1,
                processed_bytes=expected_total_input_bytes,
                skipped_files=0,
                skipped_bytes=0,
            )

            s3 = boto3.Session(region_name="us-west-2").resource(
                "s3"
            )  # pylint: disable=invalid-name
            bucket = s3.Bucket(self.job_attachment_s3_settings.s3BucketName)

            assert_expected_files_on_s3(
                bucket,
                expected_files={
                    f"assetRoot/Manifests/{farm_id}/{queue_id}/Inputs/0000/manifest_input",
                    f"{self.job_attachment_s3_settings.full_cas_prefix()}/a.xxh128",
                },
            )

            assert_canonical_manifest(
                bucket,
                f"assetRoot/Manifests/{farm_id}/{queue_id}/Inputs/0000/manifest_input",
                expected_manifest=expected_manifest,
            )

    @pytest.mark.parametrize(
        "mock_file_system_locations, expected_result",
        [
            (
                [],
                ({}, {}),
            ),
            (
                [
                    FileSystemLocation(
                        name="location-1",
                        type=FileSystemLocationType.LOCAL,
                        path="C:\\User\\Movie1",
                    ),
                ],
                ({"C:\\User\\Movie1": "location-1"}, {}),
            ),
            (
                [
                    FileSystemLocation(
                        name="location-1",
                        type=FileSystemLocationType.SHARED,
                        path="/mnt/shared/movie1",
                    ),
                ],
                ({}, {"/mnt/shared/movie1": "location-1"}),
            ),
            (
                [
                    FileSystemLocation(
                        name="location-1",
                        type=FileSystemLocationType.LOCAL,
                        path="C:\\User\\Movie1",
                    ),
                    FileSystemLocation(
                        name="location-2",
                        type=FileSystemLocationType.LOCAL,
                        path="/home/user1/movie1",
                    ),
                    FileSystemLocation(
                        name="location-3",
                        type=FileSystemLocationType.SHARED,
                        path="/mnt/shared/movie1",
                    ),
                    FileSystemLocation(
                        name="location-4",
                        type=FileSystemLocationType.SHARED,
                        path="/mnt/shared/etc",
                    ),
                ],
                (
                    {"C:\\User\\Movie1": "location-1", "/home/user1/movie1": "location-2"},
                    {"/mnt/shared/movie1": "location-3", "/mnt/shared/etc": "location-4"},
                ),
            ),
        ],
    )
    def test_get_file_system_locations_by_type(
        self,
        farm_id: str,
        queue_id: str,
        mock_file_system_locations: List[FileSystemLocation],
        expected_result: Tuple[Dict[str, str], Dict[str, str]],
    ):
        mock_storage_profile_for_queue = StorageProfile(
            storageProfileId="sp-0123456789",
            displayName="Storage profile 1",
            osFamily=StorageProfileOperatingSystemFamily.WINDOWS,
            fileSystemLocations=mock_file_system_locations,
        )

        asset_manager = S3AssetManager(
            farm_id=farm_id,
            queue_id=queue_id,
            job_attachment_settings=self.job_attachment_s3_settings,
        )

        result = asset_manager._get_file_system_locations_by_type(
            storage_profile_for_queue=mock_storage_profile_for_queue
        )

        assert result == expected_result

    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="This test is for paths in POSIX path format and will be skipped on Windows.",
    )
    @patch.object(Path, "exists", return_value=True)
    @pytest.mark.parametrize(
        "input_paths, output_paths, referenced_paths, local_type_locations, shared_type_locations, expected_result",
        [
            (
                set(),  # input paths
                set(),  # output paths
                set(),  # referenced paths
                {},  # File System Location (LOCAL type)
                {},  # File System Location (SHARED type)
                [],
            ),
            (
                {
                    "/home/username/DOCS/inputs/input1.txt",
                    "/HOME/username/DOCS/inputs/input2.txt",
                },  # input paths
                {"/home/username/docs/outputs"},  # output paths
                set(),  # referenced paths
                {},  # File System Location (LOCAL type)
                {},  # File System Location (SHARED type)
                [
                    AssetRootGroup(
                        root_path="/",
                        inputs={
                            Path("/home/username/DOCS/inputs/input1.txt"),
                            Path("/HOME/username/DOCS/inputs/input2.txt"),
                        },
                        outputs={
                            Path("/home/username/docs/outputs"),
                        },
                    ),
                ],
            ),
            (
                {"/home/username/docs/inputs/input1.txt"},  # input paths
                {"/home/username/docs/outputs"},  # output paths
                set(),  # referenced paths
                {"/home/username/movie1": "Movie 1 - Local"},  # File System Location (LOCAL type)
                {},  # File System Location (SHARED type)
                [
                    AssetRootGroup(
                        root_path="/home/username/docs",
                        inputs={
                            Path("/home/username/docs/inputs/input1.txt"),
                        },
                        outputs={
                            Path("/home/username/docs/outputs"),
                        },
                    ),
                ],
            ),
            (
                {"/home/username/movie1/inputs/input1.txt"},  # input paths
                {"/home/username/movie1/outputs"},  # output paths
                set(),  # referenced paths
                {"/home/username/movie1": "Movie 1 - Local"},  # File System Location (LOCAL type)
                {},  # File System Location (SHARED type)
                [
                    AssetRootGroup(
                        file_system_location_name="Movie 1 - Local",
                        root_path="/home/username/movie1",
                        inputs={
                            Path("/home/username/movie1/inputs/input1.txt"),
                        },
                        outputs={
                            Path("/home/username/movie1/outputs"),
                        },
                    ),
                ],
            ),
            (
                {"/mnt/shared/movie1/something.txt"},  # input paths
                {"/home/username/movie1/outputs"},  # output paths
                set(),  # referenced paths
                {"/home/username/movie1": "Movie 1 - Local"},  # File System Location (LOCAL type)
                {"/mnt/shared/movie1": "Movie 1 - Shared"},  # File System Location (SHARED type)
                [
                    AssetRootGroup(
                        file_system_location_name="Movie 1 - Local",
                        root_path="/home/username/movie1/outputs",
                        inputs=set(),
                        outputs={
                            Path("/home/username/movie1/outputs"),
                        },
                    ),
                ],
            ),
            (
                {
                    "/home/username/movie1/inputs/input1.txt",
                    "/home/username/movie1/inputs/input2.txt",
                    "/home/username/docs/doc1.txt",
                    "/home/username/docs/doc2.txt",
                    "/home/username/extra1.txt",
                    "/mnt/shared/movie1/something.txt",
                },  # input paths
                {
                    "/home/username/movie1/outputs1",
                    "/home/username/movie1/outputs2",
                },  # output paths
                {"/home/username/movie1/outputs1/referenced/path"},  # referenced paths
                {"/home/username/movie1": "Movie 1 - Local"},  # File System Location (LOCAL type)
                {"/mnt/shared/movie1": "Movie 1 - Shared"},  # File System Location (SHARED type)
                [
                    AssetRootGroup(
                        file_system_location_name="Movie 1 - Local",
                        root_path="/home/username/movie1",
                        inputs={
                            Path("/home/username/movie1/inputs/input1.txt"),
                            Path("/home/username/movie1/inputs/input2.txt"),
                        },
                        outputs={
                            Path("/home/username/movie1/outputs1"),
                            Path("/home/username/movie1/outputs2"),
                        },
                        references={Path("/home/username/movie1/outputs1/referenced/path")},
                    ),
                    AssetRootGroup(
                        root_path="/home/username",
                        inputs={
                            Path("/home/username/docs/doc1.txt"),
                            Path("/home/username/docs/doc2.txt"),
                            Path("/home/username/extra1.txt"),
                        },
                        outputs=set(),
                    ),
                ],
            ),
        ],
    )
    def test_get_asset_groups(
        self,
        farm_id: str,
        queue_id: str,
        input_paths: Set[str],
        output_paths: Set[str],
        referenced_paths: Set[str],
        local_type_locations: Dict[str, str],
        shared_type_locations: Dict[str, str],
        expected_result: List[AssetRootGroup],
    ):
        asset_manager = S3AssetManager(
            farm_id=farm_id,
            queue_id=queue_id,
            job_attachment_settings=self.job_attachment_s3_settings,
        )
        result = asset_manager._get_asset_groups(
            input_paths,
            output_paths,
            referenced_paths,
            local_type_locations,
            shared_type_locations,
        )

        sorted_result = sorted(result, key=lambda x: x.root_path)
        sorted_expected_result = sorted(expected_result, key=lambda x: x.root_path)

        assert sorted_result == sorted_expected_result

    @pytest.mark.skipif(
        sys.platform != "win32",
        reason="This test is for paths in Windows path format and will be skipped on POSIX-based system.",
    )
    @patch.object(Path, "exists", return_value=True)
    @pytest.mark.parametrize(
        "input_paths, output_paths, referenced_paths, local_type_locations, shared_type_locations, expected_result",
        [
            (
                set(),  # input paths
                set(),  # output paths
                set(),  # referenced paths
                {},  # File System Location (LOCAL type)
                {},  # File System Location (SHARED type)
                [],
            ),
            (
                {"d:\\USERNAME\\DOCS\\inputs\\input1.txt"},  # input paths
                {"D:\\username\\docs\\outputs"},  # output paths
                set(),  # referenced paths
                {},  # File System Location (LOCAL type)
                {},  # File System Location (SHARED type)
                [
                    AssetRootGroup(
                        root_path="D:\\username\\docs",
                        inputs={
                            Path("d:\\USERNAME\\DOCS\\inputs\\input1.txt"),
                        },
                        outputs={
                            Path("D:\\username\\docs\\outputs"),
                        },
                    ),
                ],
            ),
            (
                {"D:\\username\\docs\\inputs\\input1.txt"},  # input paths
                {"d:\\USERNAME\\DOCS\\outputs"},  # output paths
                set(),  # referenced paths
                {},  # File System Location (LOCAL type)
                {},  # File System Location (SHARED type)
                [
                    AssetRootGroup(
                        root_path="D:\\username\\docs",
                        inputs={
                            Path("D:\\username\\docs\\inputs\\input1.txt"),
                        },
                        outputs={
                            Path("d:\\USERNAME\\DOCS\\outputs"),
                        },
                    ),
                ],
            ),
            (
                {"C:\\username\\docs\\inputs\\input1.txt"},  # input paths
                {"C:\\username\\docs\\outputs"},  # output paths
                set(),  # referenced paths
                {"C:\\username\\movie1": "Movie 1 - Local"},  # File System Location (LOCAL type)
                {},  # File System Location (SHARED type)
                [
                    AssetRootGroup(
                        root_path="C:\\username\\docs",
                        inputs={
                            Path("C:\\username\\docs\\inputs\\input1.txt"),
                        },
                        outputs={
                            Path("C:\\username\\docs\\outputs"),
                        },
                    ),
                ],
            ),
            (
                {"C:\\username\\movie1\\inputs\\input1.txt"},  # input paths
                {"C:\\username\\movie1\\outputs"},  # output paths
                set(),  # referenced paths
                {"C:\\username\\movie1": "Movie 1 - Local"},  # File System Location (LOCAL type)
                {},  # File System Location (SHARED type)
                [
                    AssetRootGroup(
                        file_system_location_name="Movie 1 - Local",
                        root_path="C:\\username\\movie1",
                        inputs={
                            Path("C:\\username\\movie1\\inputs\\input1.txt"),
                        },
                        outputs={
                            Path("C:\\username\\movie1\\outputs"),
                        },
                    ),
                ],
            ),
            (
                {"X:\\mnt\\shared\\movie1\\something.txt"},  # input paths
                {"C:\\username\\movie1\\outputs"},  # output paths
                set(),  # referenced paths
                {"C:\\username\\movie1": "Movie 1 - Local"},  # File System Location (LOCAL type)
                {
                    "X:\\mnt\\shared\\movie1": "Movie 1 - Shared"
                },  # File System Location (SHARED type)
                [
                    AssetRootGroup(
                        file_system_location_name="Movie 1 - Local",
                        root_path="C:\\username\\movie1\\outputs",
                        inputs=set(),
                        outputs={
                            Path("C:\\username\\movie1\\outputs"),
                        },
                    ),
                ],
            ),
            (
                {
                    "C:\\username\\movie1\\inputs\\input1.txt",
                    "C:\\username\\movie1\\inputs\\input2.txt",
                    "C:\\username\\docs\\doc1.txt",
                    "C:\\username\\docs\\doc2.txt",
                    "C:\\username\\extra1.txt",
                    "X:\\mnt\\shared\\movie1\\something.txt",
                },  # input paths
                {
                    "C:\\username\\movie1\\outputs1",
                    "C:\\username\\movie1\\outputs2",
                },  # output paths
                {"C:\\username\\movie1\\outputs1\\referenced\\path"},  # referenced paths
                {"C:\\username\\movie1": "Movie 1 - Local"},  # File System Location (LOCAL type)
                {
                    "X:\\mnt\\shared\\movie1": "Movie 1 - Shared"
                },  # File System Location (SHARED type)
                [
                    AssetRootGroup(
                        file_system_location_name="Movie 1 - Local",
                        root_path="C:\\username\\movie1",
                        inputs={
                            Path("C:\\username\\movie1\\inputs\\input1.txt"),
                            Path("C:\\username\\movie1\\inputs\\input2.txt"),
                        },
                        outputs={
                            Path("C:\\username\\movie1\\outputs1"),
                            Path("C:\\username\\movie1\\outputs2"),
                        },
                        references={Path("C:\\username\\movie1\\outputs1\\referenced\\path")},
                    ),
                    AssetRootGroup(
                        root_path="C:\\username",
                        inputs={
                            Path("C:\\username\\docs\\doc1.txt"),
                            Path("C:\\username\\docs\\doc2.txt"),
                            Path("C:\\username\\extra1.txt"),
                        },
                        outputs=set(),
                    ),
                ],
            ),
        ],
    )
    def test_get_asset_groups_for_windows(
        self,
        farm_id: str,
        queue_id: str,
        input_paths: Set[str],
        output_paths: Set[str],
        referenced_paths: Set[str],
        local_type_locations: Dict[str, str],
        shared_type_locations: Dict[str, str],
        expected_result: List[AssetRootGroup],
    ):
        asset_manager = S3AssetManager(
            farm_id=farm_id,
            queue_id=queue_id,
            job_attachment_settings=self.job_attachment_s3_settings,
        )
        result = asset_manager._get_asset_groups(
            input_paths,
            output_paths,
            referenced_paths,
            local_type_locations,
            shared_type_locations,
        )

        sorted_result = sorted(result, key=lambda x: x.root_path)
        sorted_expected_result = sorted(expected_result, key=lambda x: x.root_path)

        assert len(sorted_result) == len(sorted_expected_result)
        for i in range(len(sorted_result)):
            assert sorted_result[i].root_path.upper() == sorted_expected_result[i].root_path.upper()
            assert sorted_result[i].inputs == sorted_expected_result[i].inputs
            assert sorted_result[i].outputs == sorted_expected_result[i].outputs

    @pytest.mark.skipif(
        sys.platform != "win32",
        reason="This test is for paths in Windows path format and will be skipped on POSIX-based system.",
    )
    @patch.object(Path, "exists", return_value=True)
    def test_get_asset_groups_for_windows_case_insensitive(
        self,
        farm_id: str,
        queue_id: str,
    ):
        """
        Tests that the asset manager can handle Windows paths and ignore case.
        (Verifies if two paths are treated as the same when they differ only in letter case.)
        """
        asset_manager = S3AssetManager(
            farm_id=farm_id,
            queue_id=queue_id,
            job_attachment_settings=self.job_attachment_s3_settings,
        )

        input_paths = {
            "C:\\username\\docs\\inputs\\input1.txt",
            "C:\\username\\DOCS\\inputs\\input1.txt",
        }
        output_paths = {"C:\\username\\docs\\outputs"}

        result = asset_manager._get_asset_groups(
            input_paths,
            output_paths,
            referenced_paths=set(),
            local_type_locations={},
            shared_type_locations={},
        )

        assert result[0].root_path == "C:\\username\\docs" or "C:\\username\\DOCS"
        assert result[0].inputs == {Path("C:\\username\\docs\\inputs\\input1.txt")} or {
            Path("C:\\username\\DOCS\\inputs\\input1.txt")
        }
        assert result[0].outputs == {Path("C:\\username\\docs\\outputs")}

    @pytest.mark.parametrize(
        "input_files, size_threshold, expected_queues",
        [
            (
                [],
                100 * (1024**2),  # 100 MB
                ([], []),
            ),
            (
                [
                    BaseManifestPath(path="", hash="", size=10 * (1024**2), mtime=1),
                    BaseManifestPath(path="", hash="", size=100 * (1024**2), mtime=1),
                    BaseManifestPath(path="", hash="", size=1000 * (1024**2), mtime=1),
                ],
                100 * (1024**2),  # 100 MB
                (
                    [
                        BaseManifestPath(path="", hash="", size=10 * (1024**2), mtime=1),
                        BaseManifestPath(path="", hash="", size=100 * (1024**2), mtime=1),
                    ],
                    [
                        BaseManifestPath(path="", hash="", size=1000 * (1024**2), mtime=1),
                    ],
                ),
            ),
            (
                [
                    BaseManifestPath(path="", hash="", size=10 * (1024**2), mtime=1),
                    BaseManifestPath(path="", hash="", size=100 * (1024**2), mtime=1),
                ],
                800 * (1024**2),  # 800 MB
                (
                    [
                        BaseManifestPath(path="", hash="", size=10 * (1024**2), mtime=1),
                        BaseManifestPath(path="", hash="", size=100 * (1024**2), mtime=1),
                    ],
                    [],
                ),
            ),
        ],
    )
    def test_separate_files_by_size(
        self,
        input_files: List[BaseManifestPath],
        size_threshold: int,
        expected_queues: Tuple[List[BaseManifestPath], List[BaseManifestPath]],
    ):
        """
        Tests that a helper method `_separate_files_by_size` is working as expected.
        """
        a3_asset_uploader = S3AssetUploader()
        actual_queues = a3_asset_uploader._separate_files_by_size(
            files_to_upload=input_files,
            size_threshold=size_threshold,
        )
        assert actual_queues == expected_queues

    @mock_aws
    @pytest.mark.parametrize(
        "manifest_version",
        [
            ManifestVersion.v2023_03_03,
        ],
    )
    def test_upload_object_to_cas_skips_upload_with_cache(
        self, tmpdir, farm_id, queue_id, manifest_version, default_job_attachment_s3_settings
    ):
        """
        Tests that objects are not uploaded to S3 if there is a corresponding entry in the S3CheckCache
        """
        # Given
        asset_root = tmpdir.mkdir("test-root")
        test_file = asset_root.join("test-file.txt")
        test_file.write("stuff")
        asset_manager = S3AssetManager(
            farm_id=farm_id,
            queue_id=queue_id,
            job_attachment_settings=self.job_attachment_s3_settings,
            asset_manifest_version=manifest_version,
        )
        s3_key = f"{default_job_attachment_s3_settings.s3BucketName}/prefix/test-hash.xxh128"
        test_entry = S3CheckCacheEntry(s3_key, "123.45")
        s3_cache = MagicMock()
        s3_cache.get_entry.return_value = test_entry

        # When
        with patch.object(
            asset_manager.asset_uploader,
            "_get_current_timestamp",
            side_effect=["345.67"],
        ):
            (is_uploaded, file_size) = asset_manager.asset_uploader.upload_object_to_cas(
                file=BaseManifestPath(path="test-file.txt", hash="test-hash", size=5, mtime=1),
                hash_algorithm=HashAlgorithm.XXH128,
                s3_bucket=default_job_attachment_s3_settings.s3BucketName,
                source_root=Path(asset_root),
                s3_cas_prefix="prefix",
                s3_check_cache=s3_cache,
            )

            # Then
            assert not is_uploaded
            assert file_size == 5
            s3_cache.put_entry.assert_not_called()

    def test_open_non_symlink_file_binary(self, tmp_path: Path):
        temp_file = tmp_path / "temp_file.txt"
        temp_file.write_text("this is test file")

        a3_asset_uploader = S3AssetUploader()
        with a3_asset_uploader._open_non_symlink_file_binary(str(temp_file)) as file_obj:
            assert file_obj is not None
            assert file_obj.read() == b"this is test file"

    def test_open_non_symlink_file_binary_posix_fail(self, tmp_path: Path, caplog):
        caplog.set_level(DEBUG)

        # IF
        target_file = tmp_path / "target_file.txt"
        target_file.write_text(("This is target"))
        symlink_path = tmp_path / "symlink"
        os.symlink(target_file, symlink_path)

        # WHEN
        a3_asset_uploader = S3AssetUploader()
        with a3_asset_uploader._open_non_symlink_file_binary(str(symlink_path)) as file_obj:
            # THEN
            assert file_obj is None
            assert (
                f"Failed to open file. The following file will be skipped: {symlink_path}"
                in caplog.text
            )
            if hasattr(os, "O_NOFOLLOW") is False:
                # Windows or other platforms that don't support O_NOFOLLOW
                assert "Mismatch between path and its final path" in caplog.text
            else:
                # Posix
                assert "Too many levels of symbolic links:" in caplog.text

    @mock_aws
    @pytest.mark.parametrize(
        "manifest_version",
        [
            ManifestVersion.v2023_03_03,
        ],
    )
    def test_upload_object_to_cas_adds_cache_entry(
        self,
        tmpdir,
        farm_id,
        queue_id,
        manifest_version,
        default_job_attachment_s3_settings,
        assert_expected_files_on_s3,
    ):
        """
        Tests that when an object is added to the CAS, an S3 cache entry is added.
        """
        # Given
        asset_root = tmpdir.mkdir("test-root")
        test_file = asset_root.join("test-file.txt")
        test_file.write("stuff")
        asset_manager = S3AssetManager(
            farm_id=farm_id,
            queue_id=queue_id,
            job_attachment_settings=self.job_attachment_s3_settings,
            asset_manifest_version=manifest_version,
        )
        s3_key = f"{default_job_attachment_s3_settings.s3BucketName}/prefix/test-hash.xxh128"
        s3_cache = MagicMock()
        s3_cache.get_entry.return_value = None
        expected_new_entry = S3CheckCacheEntry(s3_key, "345.67")

        # When
        with patch.object(
            asset_manager.asset_uploader,
            "_get_current_timestamp",
            side_effect=["345.67"],
        ):
            (is_uploaded, file_size) = asset_manager.asset_uploader.upload_object_to_cas(
                file=BaseManifestPath(path="test-file.txt", hash="test-hash", size=5, mtime=1),
                hash_algorithm=HashAlgorithm.XXH128,
                s3_bucket=default_job_attachment_s3_settings.s3BucketName,
                source_root=Path(asset_root),
                s3_cas_prefix="prefix",
                s3_check_cache=s3_cache,
            )

            # Then
            assert is_uploaded
            assert file_size == 5
            s3_cache.put_entry.assert_called_once_with(expected_new_entry)

            s3 = boto3.Session(region_name="us-west-2").resource(
                "s3"
            )  # pylint: disable=invalid-name
            bucket = s3.Bucket(self.job_attachment_s3_settings.s3BucketName)

            assert_expected_files_on_s3(
                bucket,
                expected_files={"prefix/test-hash.xxh128"},
            )

    def test_gather_upload_metadata(self):
        # Given
        manifest = AssetManifest(
            hash_alg=HashAlgorithm("xxh128"),
            total_size=10,
            paths=[
                BaseManifestPath(path="output_file", hash="a", size=1, mtime=167907934333848),
                BaseManifestPath(
                    path="output/nested_output_file", hash="b", size=1, mtime=1479079344833848
                ),
            ],
        )
        # When
        (hash_alg, _, manifest_name) = S3AssetUploader._gather_upload_metadata(
            manifest, Path("mocksourcerootpath"), "suffix"
        )
        # Then
        assert hash_alg == HashAlgorithm.XXH128
        assert manifest_name == "73addc7c69ddec53bd8d9df653add3c4_suffix"


def assert_progress_report_last_callback(
    num_input_files: int,
    expected_total_input_bytes: int,
    on_preparing_to_submit: MagicMock,
    on_uploading_assets: MagicMock,
):
    """
    Assert that the argument of the last callback (when the progress is 100%) is as expected.
    """
    readable_total_input_bytes = _human_readable_file_size(expected_total_input_bytes)
    actual_args, _ = on_preparing_to_submit.call_args
    actual_last_hashing_progress_report = actual_args[0]
    assert actual_last_hashing_progress_report.status == ProgressStatus.PREPARING_IN_PROGRESS
    assert actual_last_hashing_progress_report.progress == 100.0
    assert (
        f"Processed {readable_total_input_bytes} / {readable_total_input_bytes}"
        f" of {num_input_files} file{'' if num_input_files == 1 else 's'}"
        " (Hashing speed: "
    ) in actual_last_hashing_progress_report.progressMessage

    actual_args, _ = on_uploading_assets.call_args
    actual_last_upload_progress_report = actual_args[0]
    assert actual_last_upload_progress_report.status == ProgressStatus.UPLOAD_IN_PROGRESS
    assert actual_last_upload_progress_report.progress == 100.0
    assert (
        f"Uploaded {readable_total_input_bytes} / {readable_total_input_bytes}"
        f" of {num_input_files} file{'' if num_input_files == 1 else 's'}"
        " (Transfer rate: "
    ) in actual_last_upload_progress_report.progressMessage


def assert_progress_report_summary_statistics(
    actual_summary_statistics: SummaryStatistics,
    processed_files: int,
    processed_bytes: int,
    skipped_files: int,
    skipped_bytes: int,
):
    """
    Assert that the reported summary statistics are as expected.
    """
    expected_summary_statistics = SummaryStatistics(
        total_time=actual_summary_statistics.total_time,
        total_files=processed_files + skipped_files,
        total_bytes=processed_bytes + skipped_bytes,
        processed_files=processed_files,
        processed_bytes=processed_bytes,
        skipped_files=skipped_files,
        skipped_bytes=skipped_bytes,
        transfer_rate=processed_bytes / actual_summary_statistics.total_time,
    )
    assert actual_summary_statistics == expected_summary_statistics
